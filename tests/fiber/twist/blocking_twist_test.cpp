// this_fiber::blocking under Twist: the offload-and-park handshake between a fiber
// (TwistSync, on a ThreadPool<TwistSync> worker) and a BlockingPool<TwistSync>
// thread that completes the call, possibly while the fiber is still on its
// stack (wake -> NOTIFIED) or after it parked (wake -> PARKED -> QUEUED ->
// resumed on any worker), and the drop path when the pool stops.
//
// TWIST_SIM:    each scenario is explored with the Random and PCT schedulers.
// TWIST_FAULTY: each scenario runs several times on real threads with fault
//               injection (clang-twist-faulty-tsan adds TSan).
//
// What the handshake hands over lives in twist::assist::Shared cells, whose
// accesses Twist checks for happens-before:
//   fiber -> pool thread: what the callable reads (op publication: the
//                         queue link's release / acquire);
//   pool thread -> fiber: what the callable writes and its result (the wake:
//                         NOTIFIED release / failing park CAS acquire, or
//                         PARKED -> QUEUED and the ThreadPool queue).

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>

#include <twist/assist/assert.hpp>
#include <twist/assist/shared.hpp>
#include <twist/cross.hpp>
#include <twist/ed/std/atomic.hpp>
#include <twist/ed/std/thread.hpp>

#if defined(__TWIST_SIM__)
#include <twist/sim.hpp>
#endif

#include "fibers/blocking/blocking_pool.hpp"
#include "fibers/context/twist_context.hpp"
#include "fibers/fiber/blocking.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/stack/stack_pool.hpp"
#include "fibers/sync/twist_sync.hpp"
#include "fibers/thread_pool/thread_pool.hpp"

namespace fiber = fibers::fiber;
namespace this_fiber = fibers::fiber::this_fiber;
namespace tp = fibers::thread_pool;
using fibers::sync::TwistSync;

namespace {

using Counter = twist::ed::std::atomic<std::size_t>;

std::size_t env_or(const char* name, std::size_t fallback) {
    const char* value = std::getenv(name);
    return value != nullptr ? static_cast<std::size_t>(std::strtoull(value, nullptr, 10)) : fallback;
}

// Env: FIBERS_TWIST_RUNS (runs per scheduler / repetitions), FIBERS_TWIST_SEED.
void check(const std::function<void()>& body, std::size_t sim_runs = 5'000) {
#if defined(__TWIST_SIM__)
    const std::size_t runs = env_or("FIBERS_TWIST_RUNS", sim_runs);
    const std::uint64_t seed = env_or("FIBERS_TWIST_SEED", 42);
    {
        twist::sim::sched::RandomScheduler random{{.seed = seed, .max_runs = runs}};
        const auto explored = twist::sim::Explore(random, body);
        if (explored.found) {
            twist::sim::Print(body, explored.found->schedule);
            FAIL() << "Random scheduler, after " << explored.sims << " runs:\n"
                   << explored.found->result.std_err;
        }
    }
    {
        twist::sim::sched::PctScheduler pct{{.depth = 3, .seed = seed + 1, .max_runs = runs}};
        const auto explored = twist::sim::Explore(pct, body);
        if (explored.found) {
            twist::sim::Print(body, explored.found->schedule);
            FAIL() << "PCT scheduler, after " << explored.sims << " runs:\n"
                   << explored.found->result.std_err;
        }
    }
#else
    static_cast<void>(sim_runs);
    const std::size_t runs = env_or("FIBERS_TWIST_RUNS", 50);
    for (std::size_t i = 0; i < runs; ++i) {
        twist::cross::Run(body);
    }
#endif
}

// Bounded waits: a lost wakeup must fail the run, not hang the simulator.
#if defined(__TWIST_SIM__)
#define FIBERS_WAIT_BOUNDED(condition, message)          \
    for (std::size_t step_ = 0; !(condition); ++step_) { \
        TWIST_ASSERT_M(step_ < 20'000, message);         \
        twist::ed::std::this_thread::yield();            \
    }
#else
#define FIBERS_WAIT_BOUNDED(condition, message)                                                  \
    for (const auto deadline_ = std::chrono::steady_clock::now() + std::chrono::seconds(30);    \
         !(condition);) {                                                                        \
        TWIST_ASSERT_M(std::chrono::steady_clock::now() < deadline_, message);                   \
        twist::ed::std::this_thread::yield();                                                    \
    }
#endif

constexpr std::size_t kStackSize = 256 * 1024;

tp::ThreadPoolOptions tiny(std::size_t workers) {
    return tp::ThreadPoolOptions{
        .num_workers = workers,
        .main_box_capacity = 2,
        .mail_box_shards = 1,
        .mail_box_shard_capacity = 2,
        .batch_size = 2,
        .mail_box_check_interval = 2,
        .max_steal_backoff = 4,
        .idle_spins_before_park = 8,
    };
}

// Fiber stacks mapped once, outside the simulation, handed out in a fixed
// order (see tests/fiber/twist: addresses must be the same in every run).
constexpr std::size_t kTestStacks = 8;
fibers::stack::Stack g_stacks[kTestStacks];

void map_test_stacks() {
    for (auto& stack : g_stacks) {
        if (stack.empty()) {
            stack = fibers::stack::Stack::allocate(kStackSize);
        }
    }
}

class TestStackPool {
public:
    fibers::stack::Stack* acquire() noexcept {
        for (std::size_t i = 0; i < kTestStacks; ++i) {
            if (!used_[i].exchange(true, std::memory_order_acquire)) {
                return &g_stacks[i];
            }
        }
        fibers::fail::fail_fast("test: out of fiber stacks");
    }

    void release(fibers::stack::Stack* stack) noexcept {
        used_[static_cast<std::size_t>(stack - g_stacks)].store(false, std::memory_order_release);
    }

private:
    twist::ed::std::atomic<bool> used_[kTestStacks] = {false, false, false, false, false, false, false, false};
};

// What this_fiber::blocking needs: a ThreadPool, a StackPool and a BlockingPool.
class BlockingScheduler {
public:
    using SyncPolicy = TwistSync;

    BlockingScheduler(std::size_t workers, std::size_t blocking_threads)
        : pool_(tiny(workers)), blocking_({.num_threads = blocking_threads}) {}

    // Workers first (they may be inside a submit() into the blocking pool),
    // then the blocking threads.
    void stop() noexcept {
        pool_.stop();
        blocking_.stop();
    }

    tp::ThreadPool<TwistSync>& pool() noexcept { return pool_; }
    TestStackPool& stacks() noexcept { return stacks_; }
    fibers::blocking::BlockingPool<TwistSync>& blocking() noexcept { return blocking_; }

private:
    TestStackPool stacks_;
    tp::ThreadPool<TwistSync> pool_;
    fibers::blocking::BlockingPool<TwistSync> blocking_;
};

static_assert(fiber::BlockingScheduler<BlockingScheduler>);

// Two fibers offload two calls each to one blocking thread, on two workers.
// Each call reads what its fiber wrote before offloading and writes a cell
// the fiber reads after the wake; the result travels through the op. The
// pool thread's completion races with the fiber's switch-out.
TEST(BlockingTwist, Handshake) {
    map_test_stacks();
    check([] {
        constexpr int kFibers = 2;
        constexpr int kRounds = 2;
        Counter done{0};
        twist::assist::Shared<int> to_call[kFibers] = {0, 0};
        twist::assist::Shared<int> from_call[kFibers] = {0, 0};
        BlockingScheduler scheduler(2, 1);
        for (int f = 0; f < kFibers; ++f) {
            fiber::go(scheduler, [&, f]() noexcept {
                for (int i = 1; i <= kRounds; ++i) {
                    to_call[f].Write(i);
                    const int r = this_fiber::blocking<BlockingScheduler>([&, f, i]() noexcept {
                        TWIST_ASSERT_M(to_call[f].Read() == i, "fiber's write visible to the call");
                        from_call[f].Write(i * 10);
                        return i * 100 + f;
                    });
                    TWIST_ASSERT_M(r == i * 100 + f, "the call's result");
                    TWIST_ASSERT_M(from_call[f].Read() == i * 10, "the call's write visible after the wake");
                }
                done.fetch_add(1);
            });
        }
        FIBERS_WAIT_BOUNDED(done.load() == kFibers, "offloading fiber never woken (lost wakeup)");
        scheduler.stop();
    });
}

// Two blocking threads, one worker: completions of two fibers' calls race
// with each other and with the worker switching between the fibers.
TEST(BlockingTwist, TwoBlockingThreadsOneWorker) {
    map_test_stacks();
    check([] {
        Counter done{0};
        twist::assist::Shared<int> cell[2] = {0, 0};
        BlockingScheduler scheduler(1, 2);
        for (int f = 0; f < 2; ++f) {
            fiber::go(scheduler, [&, f]() noexcept {
                this_fiber::blocking<BlockingScheduler>([&, f]() noexcept { cell[f].Write(f + 1); });
                TWIST_ASSERT_M(cell[f].Read() == f + 1, "void call's write visible after the wake");
                done.fetch_add(1);
            });
        }
        FIBERS_WAIT_BOUNDED(done.load() == 2, "offloading fiber never woken (lost wakeup)");
        scheduler.stop();
    });
}

// stop() of the blocking pool races with the offload: the call runs on the
// pool thread, or is dropped (by stop()'s drain on this thread, or by the
// fiber's own submit after stop) and then runs inline on the worker. Either
// way exactly once, and the fiber gets its result.
TEST(BlockingTwist, RacingWithBlockingPoolStop) {
    map_test_stacks();
    check([] {
        Counter done{0};
        Counter calls{0};
        twist::assist::Shared<int> cell{0};
        BlockingScheduler scheduler(1, 1);
        fiber::go(scheduler, [&]() noexcept {
            const int r = this_fiber::blocking<BlockingScheduler>([&]() noexcept {
                calls.fetch_add(1);
                cell.Write(7);
                return 5;
            });
            TWIST_ASSERT_M(r == 5, "the call's result");
            TWIST_ASSERT_M(cell.Read() == 7, "the call's write visible");
            done.fetch_add(1);
        });
        scheduler.blocking().stop();
        FIBERS_WAIT_BOUNDED(done.load() == 1, "fiber left parked after the pool stopped");
        TWIST_ASSERT_M(calls.load() == 1, "the call ran exactly once");
        scheduler.pool().stop();
    });
}

}  // namespace
