// BlockingPool and the blocking ops on real threads.

#include <gtest/gtest.h>

#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>
#include <vector>

#include "fibers/blocking/blocking_pool.hpp"
#include "fibers/blocking/ops.hpp"
#include "fibers/thread_pool/thread_pool.hpp"
#include "test_util.hpp"

namespace blocking = fibers::blocking;
using blocking::BlockingPool;
using blocking::BlockingPoolOptions;
using blocking::IoResult;
using blocking::Sink;
using blocking::Task;
using namespace blocking::test;

namespace {

std::chrono::microseconds process_cpu() {
    rusage usage{};
    ::getrusage(RUSAGE_SELF, &usage);
    auto tv = [](const timeval& t) { return std::chrono::seconds(t.tv_sec) + std::chrono::microseconds(t.tv_usec); };
    return tv(usage.ru_utime) + tv(usage.ru_stime);
}

}  // namespace

TEST(BlockingPool, RunsTasks) {
    BlockingPool<> pool({.num_threads = 4, .queue_capacity = 64});
    Counts counts;
    constexpr int kTasks = 2000;
    for (int i = 0; i < kTasks; ++i) {
        Task task = counts.task();
        while (!pool.try_submit(std::move(task))) std::this_thread::yield();
    }
    ASSERT_TRUE(wait_for([&] { return counts.ran.load() == kTasks; }));
    pool.stop();
    EXPECT_EQ(counts.ran.load(), kTasks);
    EXPECT_EQ(counts.dropped.load(), 0);
}

TEST(BlockingPool, EmptyTaskIsAcceptedAndIgnored) {
    BlockingPool<> pool({.num_threads = 1});
    EXPECT_TRUE(pool.try_submit(Task{}));
    EXPECT_EQ(pool.queued(), 0u);
}

TEST(BlockingPool, AtMostNumThreadsRunConcurrently) {
    constexpr std::size_t kThreads = 4;
    BlockingPool<> pool({.num_threads = kThreads});
    struct Probe {
        std::atomic<int> running{0};
        std::atomic<int> max_running{0};
        std::atomic<int> done{0};
    } probe;
    auto make = [&] {
        return Task(&probe, [](void* m) noexcept {
            auto* p = static_cast<Probe*>(m);
            const int now = p->running.fetch_add(1) + 1;
            int seen = p->max_running.load();
            while (now > seen && !p->max_running.compare_exchange_weak(seen, now)) {
            }
            std::this_thread::sleep_for(20ms);  // "blocked in the kernel"
            p->running.fetch_sub(1);
            p->done.fetch_add(1);
        }, nullptr);
    };
    constexpr int kTasks = 16;
    for (int i = 0; i < kTasks; ++i) {
        ASSERT_TRUE(pool.try_submit(make()));
    }
    ASSERT_TRUE(wait_for([&] { return probe.done.load() == kTasks; }));
    EXPECT_EQ(probe.max_running.load(), static_cast<int>(kThreads));
}

TEST(BlockingPool, IdleThreadsSleep) {
    BlockingPool<> pool({.num_threads = 8});
    ASSERT_TRUE(wait_for([&] { return pool.sleeping_threads() == 8; }));
    const auto sleeps_before = pool.sleeps();
    const auto cpu_before = process_cpu();
    std::this_thread::sleep_for(200ms);
    const auto cpu = process_cpu() - cpu_before;
    const auto sleeps = pool.sleeps() - sleeps_before;
    // 8 sleeping threads (and this one): well under 1% of one core. 5 ms
    // leaves room for sanitizer background threads.
    EXPECT_LT(cpu.count(), 5000) << "process CPU over 200 ms idle: " << cpu.count() << " us";
    EXPECT_LE(sleeps, 2u) << "idle threads kept waking up";
    EXPECT_EQ(pool.sleeping_threads(), 8u);
}

TEST(BlockingPool, QueueFullReportsFalseAndCallerKeepsTask) {
    Gate gate, gate2;  // outlive the pool (its threads run them)
    BlockingPool<> pool({.num_threads = 1, .queue_capacity = 4});
    ASSERT_TRUE(pool.try_submit(gate.task()));
    ASSERT_TRUE(wait_for([&] { return gate.entered.load(); }));

    Counts counts;
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(pool.try_submit(counts.task()));
    }
    Task extra = counts.task();
    EXPECT_FALSE(pool.try_submit(std::move(extra)));
    EXPECT_TRUE(static_cast<bool>(extra)) << "refused task must stay with the caller";
    EXPECT_EQ(counts.dropped.load(), 0);

    Task batch[3] = {counts.task(), counts.task(), counts.task()};
    EXPECT_EQ(pool.try_submit_batch(batch, 3), 0u);
    for (auto& t : batch) EXPECT_TRUE(static_cast<bool>(t));

    gate.open();
    ASSERT_TRUE(wait_for([&] { return counts.ran.load() == 4; }));
    // Room again: the refused task goes in now.
    EXPECT_TRUE(pool.try_submit(std::move(extra)));
    ASSERT_TRUE(wait_for([&] { return counts.ran.load() == 5; }));

    // Partial batch: a blocked thread, capacity 4, six tasks -> four accepted.
    ASSERT_TRUE(pool.try_submit(gate2.task()));
    ASSERT_TRUE(wait_for([&] { return gate2.entered.load(); }));
    Task six[6] = {counts.task(), counts.task(), counts.task(), counts.task(), counts.task(), counts.task()};
    EXPECT_EQ(pool.try_submit_batch(six, 6), 4u);
    for (int i = 0; i < 4; ++i) EXPECT_FALSE(static_cast<bool>(six[i]));
    for (int i = 4; i < 6; ++i) EXPECT_TRUE(static_cast<bool>(six[i]));
    gate2.open();
    ASSERT_TRUE(wait_for([&] { return counts.ran.load() == 9; }));
    for (auto& t : batch) t.reset();
    for (int i = 4; i < 6; ++i) six[i].reset();
    pool.stop();
    EXPECT_EQ(counts.ran.load(), 9);
    EXPECT_EQ(counts.dropped.load(), 5);  // the ones we reset ourselves
}

TEST(BlockingPool, BatchWakesSleepingThreads) {
    Gate gates[3];
    BlockingPool<> pool({.num_threads = 4});
    ASSERT_TRUE(wait_for([&] { return pool.sleeping_threads() == 4; }));
    Task tasks[3] = {gates[0].task(), gates[1].task(), gates[2].task()};
    ASSERT_EQ(pool.try_submit_batch(tasks, 3), 3u);
    // Three different threads must pick them up (each one blocks its thread).
    for (auto& g : gates) ASSERT_TRUE(wait_for([&] { return g.entered.load(); }));
    for (auto& g : gates) g.open();
    pool.stop();  // the gate tasks are still finishing: join before the gates go
}

TEST(BlockingPool, StopRunsCurrentTaskAndDropsQueuedOnesOnce) {
    Gate gate;
    BlockingPool<> pool({.num_threads = 1, .queue_capacity = 64});
    ASSERT_TRUE(pool.try_submit(gate.task()));
    ASSERT_TRUE(wait_for([&] { return gate.entered.load(); }));

    constexpr int kQueued = 10;
    Counts per_task[kQueued];
    for (auto& c : per_task) ASSERT_TRUE(pool.try_submit(c.task()));

    std::thread stopper([&] { pool.stop(); });
    ASSERT_TRUE(wait_for([&] { return pool.stop_requested(); }));
    Counts late;
    Task late_task = late.task();
    EXPECT_FALSE(pool.try_submit(std::move(late_task))) << "no submits after stop";
    EXPECT_TRUE(static_cast<bool>(late_task));
    gate.open();  // the running task finishes, then the thread exits
    stopper.join();

    EXPECT_EQ(gate.ran.load(), 1);
    for (auto& c : per_task) {
        EXPECT_EQ(c.ran.load(), 0);
        EXPECT_EQ(c.dropped.load(), 1);
    }
    pool.stop();  // idempotent
    for (auto& c : per_task) EXPECT_EQ(c.dropped.load(), 1);
    EXPECT_EQ(pool.queued(), 0u);
    late_task.reset();
    EXPECT_EQ(late.dropped.load(), 1);
}

TEST(BlockingPool, StopWhileAllThreadsSleep) {
    BlockingPool<> pool({.num_threads = 8});
    ASSERT_TRUE(wait_for([&] { return pool.sleeping_threads() == 8; }));
    const auto start = std::chrono::steady_clock::now();
    pool.stop();
    EXPECT_LT(std::chrono::steady_clock::now() - start, 2s);
}

TEST(BlockingPool, ConcurrentStopsAreSerialised) {
    BlockingPool<> pool({.num_threads = 4});
    std::vector<std::thread> stoppers;
    for (int i = 0; i < 4; ++i) stoppers.emplace_back([&] { pool.stop(); });
    for (auto& t : stoppers) t.join();
    EXPECT_TRUE(pool.stop_requested());
}

TEST(BlockingPool, SubmitFromPoolThread) {
    BlockingPool<> pool({.num_threads = 2});
    struct Chain {
        BlockingPool<>* pool;
        std::atomic<int> hops{0};
        static void hop(void* m) noexcept {
            auto* c = static_cast<Chain*>(m);
            if (c->hops.fetch_add(1) + 1 < 100) {
                if (!c->pool->try_submit(Task(c, &hop, nullptr))) std::abort();
            }
        }
    } chain{&pool};
    ASSERT_TRUE(pool.try_submit(Task(&chain, &Chain::hop, nullptr)));
    ASSERT_TRUE(wait_for([&] { return chain.hops.load() == 100; }));
}

// ---------------------------------------------------------------------------
// Ops: continuation to a real ThreadPool.

namespace {

using MainPool = fibers::thread_pool::ThreadPool<>;

struct Continuation {
    MainPool* main = nullptr;
    std::atomic<bool> done{false};
    std::atomic<bool> on_main_worker{false};

    Task task() {
        return Task(this, [](void* m) noexcept {
            auto* c = static_cast<Continuation*>(m);
            c->on_main_worker.store(c->main->in_worker_thread());
            c->done.store(true);
        }, nullptr);
    }
};

}  // namespace

TEST(BlockingOps, ContinuationRunsOnTheMainPool) {
    MainPool main({.num_workers = 2});
    BlockingPool<> pool({.num_threads = 2});
    std::atomic<bool> call_on_main_worker{true};
    blocking::CallOp op([&] {
        call_on_main_worker.store(main.in_worker_thread());
        return IoResult::success(42);
    });
    Continuation k{&main};
    op.set_continuation(k.task(), Sink::of(main));
    ASSERT_TRUE(blocking::try_submit_op(pool, op));
    ASSERT_TRUE(wait_for([&] { return k.done.load(); }));
    EXPECT_FALSE(call_on_main_worker.load()) << "the blocking call must not run on a main-pool worker";
    EXPECT_TRUE(k.on_main_worker.load()) << "the continuation must run on a main-pool worker";
    EXPECT_EQ(op.result().value, 42);
    EXPECT_TRUE(op.result().ok());
}

TEST(BlockingOps, VoidCallOpAndInlineSink) {
    BlockingPool<> pool({.num_threads = 1});
    int side_effect = 0;
    blocking::CallOp op([&] { side_effect = 7; });
    Counts k;
    op.set_continuation(k.task(), Sink::run_inline());
    ASSERT_TRUE(blocking::try_submit_op(pool, op));
    ASSERT_TRUE(wait_for([&] { return k.ran.load() == 1; }));
    EXPECT_EQ(side_effect, 7);
    EXPECT_EQ(op.result().value, 0);
}

TEST(BlockingOps, RefusedOpKeepsItsContinuation) {
    BlockingPool<> pool({.num_threads = 1, .queue_capacity = 1});
    pool.stop();
    blocking::CallOp op([] {});
    Counts k;
    op.set_continuation(k.task(), Sink::run_inline());
    EXPECT_FALSE(blocking::try_submit_op(pool, op));
    EXPECT_EQ(k.dropped.load(), 0);
    Task back = op.take_continuation();
    EXPECT_TRUE(static_cast<bool>(back));
}

TEST(BlockingOps, DroppedOpDropsItsContinuation) {
    Counts k;
    blocking::CallOp op([] {});
    {
        Gate gate;
        BlockingPool<> pool({.num_threads = 1, .queue_capacity = 4});
        ASSERT_TRUE(pool.try_submit(gate.task()));
        ASSERT_TRUE(wait_for([&] { return gate.entered.load(); }));
        op.set_continuation(k.task(), Sink::run_inline());
        ASSERT_TRUE(blocking::try_submit_op(pool, op));
        std::thread stopper([&] { pool.stop(); });
        ASSERT_TRUE(wait_for([&] { return pool.stop_requested(); }));
        gate.open();
        stopper.join();
    }
    EXPECT_EQ(k.ran.load(), 0);
    EXPECT_EQ(k.dropped.load(), 1);
}

TEST(BlockingOps, PwriteFsyncPreadOnATempFile) {
    MainPool main({.num_workers = 2});
    BlockingPool<> pool({.num_threads = 2});
    TempFile file;

    char data[8192];
    for (std::size_t i = 0; i < sizeof(data); ++i) data[i] = static_cast<char>('a' + i % 26);

    auto run = [&](blocking::BlockingOp& op) {
        Continuation k{&main};
        op.set_continuation(k.task(), Sink::of(main));
        ASSERT_TRUE(blocking::try_submit_op(pool, op));
        ASSERT_TRUE(wait_for([&] { return k.done.load(); }));
        EXPECT_TRUE(k.on_main_worker.load());
    };

    blocking::PwriteOp write(file.fd, data, sizeof(data), 4096);
    run(write);
    ASSERT_TRUE(write.result().ok()) << std::strerror(write.result().error);
    EXPECT_EQ(write.result().value, static_cast<ssize_t>(sizeof(data)));

    blocking::FsyncOp sync(file.fd);
    run(sync);
    EXPECT_TRUE(sync.result().ok()) << std::strerror(sync.result().error);
    blocking::FsyncOp datasync(file.fd, /*datasync=*/true);
    run(datasync);
    EXPECT_TRUE(datasync.result().ok());

    char back[8192] = {};
    blocking::PreadOp read(file.fd, back, sizeof(back), 4096);
    run(read);
    ASSERT_TRUE(read.result().ok());
    EXPECT_EQ(read.result().value, static_cast<ssize_t>(sizeof(back)));
    EXPECT_EQ(std::memcmp(back, data, sizeof(data)), 0);

    // Short read at EOF, then EOF.
    blocking::PreadOp tail(file.fd, back, sizeof(back), 4096 + 8000);
    run(tail);
    EXPECT_EQ(tail.result().value, 192);
    blocking::PreadOp eof(file.fd, back, sizeof(back), 1 << 20);
    run(eof);
    EXPECT_EQ(eof.result().value, 0);

    // A prefix already transferred (as the nowait path leaves it).
    std::memset(back, 0, sizeof(back));
    std::memcpy(back, data, 100);
    blocking::PreadOp rest(file.fd, back, sizeof(back), 4096);
    rest.transferred = 100;
    run(rest);
    EXPECT_EQ(rest.result().value, static_cast<ssize_t>(sizeof(back)));
    EXPECT_EQ(std::memcmp(back, data, sizeof(data)), 0);

    // Errors carry errno.
    blocking::PreadOp bad(-1, back, sizeof(back), 0);
    run(bad);
    EXPECT_EQ(bad.result().value, -1);
    EXPECT_EQ(bad.result().error, EBADF);
    blocking::FsyncOp bad_sync(-1);
    run(bad_sync);
    EXPECT_EQ(bad_sync.result().error, EBADF);
}
