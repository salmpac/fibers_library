// BlockingPool and the blocking ops on real threads.

#include <gtest/gtest.h>

#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "fibers/blocking/blocking_pool.hpp"
#include "fibers/blocking/ops.hpp"
#include "fibers/thread_pool/thread_pool.hpp"
#include "test_util.hpp"

#if defined(__SANITIZE_THREAD__) || defined(__SANITIZE_ADDRESS__)
#define BLOCKING_TEST_SANITIZER 1
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer) || __has_feature(address_sanitizer)
#define BLOCKING_TEST_SANITIZER 1
#endif
#endif
#ifndef BLOCKING_TEST_SANITIZER
#define BLOCKING_TEST_SANITIZER 0
#endif

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

// Resident set size in bytes (/proc/self/statm).
std::size_t rss_bytes() {
    std::FILE* f = std::fopen("/proc/self/statm", "r");
    if (f == nullptr) return 0;
    unsigned long size = 0, resident = 0;
    const int n = std::fscanf(f, "%lu %lu", &size, &resident);
    std::fclose(f);
    return n == 2 ? resident * static_cast<std::size_t>(::sysconf(_SC_PAGESIZE)) : 0;
}

std::int64_t mono_ns() {
    timespec ts{};
    ::clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<std::int64_t>(ts.tv_sec) * 1'000'000'000 + ts.tv_nsec;
}

bool all_asleep(const BlockingPool<>& pool) { return pool.sleeping_threads() == pool.num_threads(); }

}  // namespace

TEST(BlockingPool, DefaultIsTwoThreads) {
    BlockingPool<> pool;
    EXPECT_EQ(pool.num_threads(), 2u);
    EXPECT_EQ(BlockingPoolOptions{}.num_threads, 2u);
    BlockingPool<> one({.num_threads = 0});
    EXPECT_EQ(one.num_threads(), 1u);
}

TEST(BlockingPool, RunsEveryOpExactlyOnce) {
    constexpr int kOps = 2000;
    auto ops = std::make_unique<CountingOp[]>(kOps);
    std::atomic<int> total{0};
    {
        BlockingPool<> pool({.num_threads = 4});
        for (int i = 0; i < kOps; ++i) {
            ops[i].total = &total;
            pool.submit(ops[i]);
        }
        ASSERT_TRUE(wait_for([&] { return total.load() == kOps; }));
    }
    for (int i = 0; i < kOps; ++i) ASSERT_EQ(ops[i].executed.load(), 1) << i;
}

// Unbounded and allocation-free: 1M ops from 8 threads are queued while both
// pool threads are blocked; the process's resident memory does not grow
// (the ops are the queue's nodes), then every op runs exactly once.
TEST(BlockingPool, UnboundedOneMillionOpsFromEightThreadsMemoryStaysFlat) {
    constexpr int kSubmitters = 8;
    constexpr int kPerSubmitter = 1 << 17;
    constexpr int kOps = kSubmitters * kPerSubmitter;  // 1'048'576
    auto ops = std::make_unique<CountingOp[]>(kOps);   // constructed: all pages touched
    std::atomic<int> total{0};
    for (int i = 0; i < kOps; ++i) ops[i].total = &total;

    GateOp gates[2];
    BlockingPool<> pool({.num_threads = 2});
    for (auto& g : gates) pool.submit(g);
    for (auto& g : gates) ASSERT_TRUE(wait_for([&] { return g.entered.load(); }));

    std::atomic<int> ready{0}, done{0};
    std::atomic<bool> start{false}, exit{false};
    std::vector<std::thread> submitters;
    for (int s = 0; s < kSubmitters; ++s) {
        submitters.emplace_back([&, s] {
            ready.fetch_add(1);
            while (!start.load()) std::this_thread::yield();
            for (int i = s * kPerSubmitter; i < (s + 1) * kPerSubmitter; ++i) pool.submit(ops[i]);
            done.fetch_add(1);
            while (!exit.load()) std::this_thread::sleep_for(100us);
        });
    }
    ASSERT_TRUE(wait_for([&] { return ready.load() == kSubmitters; }));
    const std::size_t rss_before = rss_bytes();
    start.store(true);
    const bool all_done = wait_for([&] { return done.load() == kSubmitters; });
    const std::size_t rss_after = rss_bytes();
    exit.store(true);
    for (auto& t : submitters) t.join();
    ASSERT_TRUE(all_done);

    EXPECT_EQ(total.load(), 0) << "both pool threads are blocked: everything is still queued";
    const long growth = static_cast<long>(rss_after) - static_cast<long>(rss_before);
    std::printf("RSS %zu -> %zu KiB (%+ld KiB) with %d ops queued (ops array %zu KiB)\n", rss_before / 1024,
                rss_after / 1024, growth / 1024, kOps, sizeof(CountingOp) * kOps / 1024);
    // A queue storing a 24-byte Task per op would need 24 MiB here. Not
    // checked under sanitizers: their runtimes grow on their own (TSan keeps
    // metadata per atomic it sees accessed: ~650 MiB here; ASan ~10-20 MiB).
#if BLOCKING_TEST_SANITIZER
    std::printf("sanitizer build: RSS growth not checked\n");
#else
    EXPECT_LT(growth, 4L << 20) << "memory grew while queueing";
#endif

    for (auto& g : gates) g.open();
    ASSERT_TRUE(wait_for([&] { return total.load() == kOps; }));
    pool.stop();
    int bad = 0;
    for (int i = 0; i < kOps; ++i) bad += ops[i].executed.load() != 1 ? 1 : 0;
    EXPECT_EQ(bad, 0) << "ops not run exactly once";
    for (auto& g : gates) EXPECT_EQ(g.ran.load(), 1);
}

TEST(BlockingPool, AtMostNumThreadsRunConcurrently) {
    for (const std::size_t threads : {std::size_t{2}, std::size_t{4}}) {
        struct Probe {
            std::atomic<int> running{0};
            std::atomic<int> max_running{0};
            std::atomic<int> done{0};
        } probe;
        struct SleepOp final : blocking::Op<SleepOp> {
            Probe* probe = nullptr;
            IoResult execute() noexcept {
                const int now = probe->running.fetch_add(1) + 1;
                int seen = probe->max_running.load();
                while (now > seen && !probe->max_running.compare_exchange_weak(seen, now)) {
                }
                std::this_thread::sleep_for(20ms);  // "blocked in the kernel"
                probe->running.fetch_sub(1);
                probe->done.fetch_add(1);
                return IoResult{};
            }
        };
        constexpr int kOps = 16;
        SleepOp ops[kOps];
        BlockingPool<> pool({.num_threads = threads});
        for (auto& op : ops) {
            op.probe = &probe;
            pool.submit(op);
        }
        ASSERT_TRUE(wait_for([&] { return probe.done.load() == kOps; }));
        EXPECT_EQ(probe.max_running.load(), static_cast<int>(threads));
    }
}

TEST(BlockingPool, IdleThreadsSleep) {
    for (const std::size_t threads : {std::size_t{2}, std::size_t{8}}) {
        BlockingPool<> pool({.num_threads = threads});
        ASSERT_TRUE(wait_for([&] { return all_asleep(pool); }));
        const auto sleeps_before = pool.sleeps();
        const auto cpu_before = process_cpu();
        std::this_thread::sleep_for(200ms);
        const auto cpu = process_cpu() - cpu_before;
        const auto sleeps = pool.sleeps() - sleeps_before;
        // Sleeping threads (and this one): well under 1% of one core. 5 ms
        // leaves room for sanitizer background threads.
        EXPECT_LT(cpu.count(), 5000) << "process CPU over 200 ms idle: " << cpu.count() << " us";
        EXPECT_EQ(sleeps, 0u) << "idle threads kept waking up";
        EXPECT_EQ(pool.sleeping_threads(), threads);
    }
}

// One wake per push: one op into a pool whose threads all sleep wakes one
// thread, not all of them.
TEST(BlockingPool, OneOpWakesOneThread) {
    BlockingPool<> pool({.num_threads = 4});
    for (int round = 0; round < 20; ++round) {
        ASSERT_TRUE(wait_for([&] { return all_asleep(pool); }));
        const auto sleeps_before = pool.sleeps();
        Counts k;  // completion signal: the pool is done with the op once it runs
        CountingOp op;
        op.set_continuation(k.task(), Sink::run_inline());
        pool.submit(op);
        ASSERT_TRUE(wait_for([&] { return k.ran.load() == 1; }));
        ASSERT_TRUE(wait_for([&] { return all_asleep(pool); }));
        EXPECT_EQ(pool.sleeps() - sleeps_before, 1u) << "more than one thread woke for one op";
    }
}

// n ops into n sleeping threads wake n different threads.
TEST(BlockingPool, EachPushWakesAnotherThread) {
    constexpr std::size_t kThreads = 3;
    BlockingPool<> pool({.num_threads = kThreads});
    ASSERT_TRUE(wait_for([&] { return all_asleep(pool); }));
    GateOp gates[kThreads];
    for (auto& g : gates) pool.submit(g);
    // Each gate blocks its thread: all three must be picked up concurrently.
    for (auto& g : gates) ASSERT_TRUE(wait_for([&] { return g.entered.load(); }));
    for (auto& g : gates) g.open();
    pool.stop();  // join before the gates go
}

TEST(BlockingPool, WakeLatencyAfterIdle) {
    struct StampOp final : blocking::Op<StampOp> {
        std::atomic<std::int64_t> at{0};
        IoResult execute() noexcept {
            at.store(mono_ns());
            return IoResult{};
        }
    };
    BlockingPool<> pool;
    std::vector<std::int64_t> latencies;
    for (int i = 0; i < 20; ++i) {
        ASSERT_TRUE(wait_for([&] { return all_asleep(pool); }));
        std::this_thread::sleep_for(5ms);
        Counts k;
        StampOp op;
        op.set_continuation(k.task(), Sink::run_inline());
        const std::int64_t submitted = mono_ns();
        pool.submit(op);
        ASSERT_TRUE(wait_for([&] { return k.ran.load() == 1; }));
        latencies.push_back(op.at.load() - submitted);
    }
    std::sort(latencies.begin(), latencies.end());
    std::printf("wake latency after idle: median %lld us, max %lld us\n",
                static_cast<long long>(latencies[latencies.size() / 2] / 1000),
                static_cast<long long>(latencies.back() / 1000));
    EXPECT_LT(latencies[latencies.size() / 2], 50'000'000) << "median wake latency over 50 ms";
}

TEST(BlockingPool, StopRunsCurrentOpAndDropsQueuedOnesOnce) {
    GateOp gate;
    constexpr int kQueued = 10;
    CountingOp ops[kQueued];
    Counts k[kQueued];
    BlockingPool<> pool({.num_threads = 1});
    pool.submit(gate);
    ASSERT_TRUE(wait_for([&] { return gate.entered.load(); }));
    for (int i = 0; i < kQueued; ++i) {
        ops[i].set_continuation(k[i].task(), Sink::run_inline());
        pool.submit(ops[i]);
    }

    std::thread stopper([&] { pool.stop(); });
    ASSERT_TRUE(wait_for([&] { return pool.stop_requested(); }));
    gate.open();  // the running op finishes, then the thread exits
    stopper.join();

    EXPECT_EQ(gate.ran.load(), 1);
    for (int i = 0; i < kQueued; ++i) {
        EXPECT_EQ(ops[i].executed.load(), 0);
        EXPECT_EQ(k[i].ran.load(), 0);
        EXPECT_EQ(k[i].dropped.load(), 1);
    }
    pool.stop();  // idempotent
    for (auto& c : k) EXPECT_EQ(c.dropped.load(), 1);
}

TEST(BlockingPool, SubmitAfterStopDropsTheOpBeforeReturning) {
    BlockingPool<> pool({.num_threads = 2});
    pool.stop();
    CountingOp op;
    Counts k;
    op.set_continuation(k.task(), Sink::run_inline());
    pool.submit(op);
    EXPECT_EQ(k.dropped.load(), 1);
    EXPECT_EQ(k.ran.load(), 0);
    EXPECT_EQ(op.executed.load(), 0);
    // The op is ours again: resubmitting drops it again.
    op.set_continuation(k.task(), Sink::run_inline());
    pool.submit(op);
    EXPECT_EQ(k.dropped.load(), 2);
}

TEST(BlockingPool, StopWhileAllThreadsSleep) {
    BlockingPool<> pool({.num_threads = 8});
    ASSERT_TRUE(wait_for([&] { return all_asleep(pool); }));
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

// Submitters race with stop(): every op is run (continuation delivered) or
// dropped, exactly once, and nothing stays behind in the queue.
TEST(BlockingPool, StopRacesSubmitters) {
    constexpr int kSubmitters = 4;
    constexpr int kPer = 2000;
    constexpr int kOps = kSubmitters * kPer;
    auto ops = std::make_unique<CountingOp[]>(kOps);
    auto k = std::make_unique<Counts[]>(kOps);
    {
        BlockingPool<> pool({.num_threads = 2});
        std::atomic<int> started{0};
        std::vector<std::thread> submitters;
        for (int s = 0; s < kSubmitters; ++s) {
            submitters.emplace_back([&, s] {
                started.fetch_add(1);
                for (int i = s * kPer; i < (s + 1) * kPer; ++i) {
                    ops[i].set_continuation(k[i].task(), Sink::run_inline());
                    pool.submit(ops[i]);
                }
            });
        }
        ASSERT_TRUE(wait_for([&] { return started.load() == kSubmitters; }));
        pool.stop();
        for (auto& t : submitters) t.join();
    }
    int ran = 0, dropped = 0, bad = 0;
    for (int i = 0; i < kOps; ++i) {
        const int r = k[i].ran.load(), d = k[i].dropped.load();
        ran += r;
        dropped += d;
        bad += (r + d != 1 || r != ops[i].executed.load()) ? 1 : 0;
    }
    std::printf("ran %d, dropped %d\n", ran, dropped);
    EXPECT_EQ(bad, 0);
    EXPECT_EQ(ran + dropped, kOps);
}

TEST(BlockingPool, SubmitFromPoolThread) {
    BlockingPool<> pool({.num_threads = 2});
    struct Chain final : blocking::Op<Chain> {
        BlockingPool<>* pool = nullptr;
        std::atomic<int> hops{0};
        IoResult execute() noexcept { return IoResult{}; }
    } chain;
    // The continuation (run inline on the pool thread) resubmits the op: the
    // op is free again once its continuation runs.
    struct Resubmit {
        Chain* chain;
        static void run(void* m) noexcept {
            auto* self = static_cast<Resubmit*>(m);
            if (self->chain->hops.fetch_add(1) + 1 < 100) {
                self->chain->set_continuation(Task(self, &run, nullptr), Sink::run_inline());
                self->chain->pool->submit(*self->chain);
            }
        }
    } resubmit{&chain};
    chain.pool = &pool;
    chain.set_continuation(Task(&resubmit, &Resubmit::run, nullptr), Sink::run_inline());
    pool.submit(chain);
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
    BlockingPool<> pool;
    std::atomic<bool> call_on_main_worker{true};
    blocking::CallOp op([&] {
        call_on_main_worker.store(main.in_worker_thread());
        return IoResult::success(42);
    });
    Continuation k{&main};
    op.set_continuation(k.task(), Sink::of(main));
    pool.submit(op);
    ASSERT_TRUE(wait_for([&] { return k.done.load(); }));
    EXPECT_FALSE(call_on_main_worker.load()) << "the blocking call must not run on a main-pool worker";
    EXPECT_TRUE(k.on_main_worker.load()) << "the continuation must run on a main-pool worker";
    EXPECT_EQ(op.result().value, 42);
    EXPECT_TRUE(op.result().ok());
}

// The fiber flow: a main-pool task offloads, the continuation comes back to
// the main pool, many times over.
TEST(BlockingOps, ManyRoundTripsThroughTheMainPool) {
    MainPool main({.num_workers = 2});
    BlockingPool<> pool;
    struct Probe {
        MainPool* main;
        BlockingPool<>* pool;
        blocking::CallOp<IoResult (*)()> op{[] { return IoResult::success(1); }};
        std::atomic<int> round_trips{0};
        static void start(void* m) noexcept {  // on a main worker
            auto* p = static_cast<Probe*>(m);
            p->op.set_continuation(Task(p, &finish, nullptr), Sink::of(*p->main));
            p->pool->submit(p->op);
        }
        static void finish(void* m) noexcept {  // back on a main worker
            auto* p = static_cast<Probe*>(m);
            if (p->round_trips.fetch_add(1) + 1 < 1000) start(m);
        }
    } probe{&main, &pool};
    main.submit(Task(&probe, &Probe::start, nullptr));
    ASSERT_TRUE(wait_for([&] { return probe.round_trips.load() == 1000; }));
}

TEST(BlockingOps, VoidCallOpAndInlineSink) {
    BlockingPool<> pool({.num_threads = 1});
    int side_effect = 0;
    blocking::CallOp op([&] { side_effect = 7; });
    Counts k;
    op.set_continuation(k.task(), Sink::run_inline());
    pool.submit(op);
    ASSERT_TRUE(wait_for([&] { return k.ran.load() == 1; }));
    EXPECT_EQ(side_effect, 7);
    EXPECT_EQ(op.result().value, 0);
}

TEST(BlockingOps, FireAndForget) {
    std::atomic<int> calls{0};
    blocking::CallOp op([&] { calls.fetch_add(1); });  // outlives the pool: no completion signal
    BlockingPool<> pool({.num_threads = 1});
    pool.submit(op);  // no continuation
    ASSERT_TRUE(wait_for([&] { return calls.load() == 1; }));
}

TEST(BlockingOps, PwriteFsyncPreadOnATempFile) {
    MainPool main({.num_workers = 2});
    BlockingPool<> pool;
    TempFile file;
    ASSERT_TRUE(file.ok());

    char data[8192];
    for (std::size_t i = 0; i < sizeof(data); ++i) data[i] = static_cast<char>('a' + i % 26);

    auto run = [&](blocking::BlockingOp& op) {
        Continuation k{&main};
        op.set_continuation(k.task(), Sink::of(main));
        pool.submit(op);
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

    // A pwrite with a prefix done.
    blocking::PwriteOp rest_write(file.fd, data, 300, 0);
    rest_write.transferred = 200;
    run(rest_write);
    EXPECT_EQ(rest_write.result().value, 300);

    // Errors carry errno.
    blocking::PreadOp bad(-1, back, sizeof(back), 0);
    run(bad);
    EXPECT_EQ(bad.result().value, -1);
    EXPECT_EQ(bad.result().error, EBADF);
    blocking::FsyncOp bad_sync(-1);
    run(bad_sync);
    EXPECT_EQ(bad_sync.result().error, EBADF);
}
