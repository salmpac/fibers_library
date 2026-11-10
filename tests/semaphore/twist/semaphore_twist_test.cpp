// SemaphoreCore (plain threads, plain nodes) and the fiber Semaphore
// (MiniScheduler; plain threads through its ThreadWaiter path) under Twist.
//
// TWIST_SIM:    each scenario is explored with the Random and PCT schedulers
//               of the deterministic simulator.
// TWIST_FAULTY: each scenario runs several times on real threads with fault
//               injection (clang-twist-faulty-tsan adds TSan).
//
// What the protocol must hand over lives in twist::assist::Shared cells,
// whose accesses Twist checks for happens-before:
//   release(n) -> every acquire that obtained one of its permits (payload
//   cells written before the release, read after the acquire);
//   holder -> next holder (a cell incremented under a 1-permit semaphore);
//   waiter -> the granting thread (a cell written before the node is
//   queued, read inside the node's wake: stands for the node's fields).
// Each memory order of the core is needed by at least one of these checks
// (see the weakening table in the commit that added these tests).

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

#include "fibers/context/twist_context.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/semaphore.hpp"
#include "fibers/stack/stack.hpp"
#include "fibers/sync/twist_sync.hpp"
#include "fibers/thread_pool/thread_pool.hpp"
#include "fibers/wait/semaphore_core.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fiber = fibers::fiber;
namespace tp = fibers::thread_pool;
using fibers::sync::TwistSync;
using fibers::wait::SemaphoreNode;
using fibers::wait::WaitNode;

namespace {

using Core = fibers::wait::SemaphoreCore<TwistSync>;
using Counter = twist::ed::std::atomic<std::size_t>;

std::size_t env_or(const char* name, std::size_t fallback) {
    const char* value = std::getenv(name);
    return value != nullptr ? static_cast<std::size_t>(std::strtoull(value, nullptr, 10)) : fallback;
}

// Env: FIBERS_TWIST_RUNS (runs per scheduler / repetitions), FIBERS_TWIST_SEED.
void check(const std::function<void()>& body, std::size_t sim_runs = 2'000) {
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

// Bounded waits: a lost wakeup must fail the run, not hang the simulator
// (steps under the simulator, wall time on real threads).
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

// ------------------------------------------------------------ plain threads

// A thread-side waiter: `mine` is written before the node is queued and read
// by the wake (on the granting thread); the wake then releases `woken`,
// which the waiting thread spins on.
struct ThreadWaiter {
    SemaphoreNode sn;
    twist::assist::Shared<int> mine{0};
    twist::ed::std::atomic<int> woken{0};

    ThreadWaiter() noexcept { sn.node = WaitNode{nullptr, this, &ThreadWaiter::wake}; }

    static void wake(void* self) noexcept {
        auto* w = static_cast<ThreadWaiter*>(self);
        TWIST_ASSERT_M(w->mine.Read() == 1, "waiter's writes visible to the granting thread");
        TWIST_ASSERT_M(w->woken.fetch_add(1, std::memory_order_release) == 0, "woken twice");
    }

    // n permits, by enqueue + wake if they are not free.
    void acquire(Core& sem, std::uint64_t n) {
        mine.Write(1);
        if (!sem.acquire_or_enqueue(sn, n)) {
            FIBERS_WAIT_BOUNDED(woken.load(std::memory_order_acquire) != 0, "waiter never granted (lost wakeup)");
        }
    }
};

// One release(2) races with a queued acquire(1) and a try_acquire(1) loop:
// fast path vs fast path, the locked attempt, the hand-off and the leftover
// taken on the fast path after the queue empties. Both acquirers see the
// releaser's write.
TEST(SemaphoreTwist, ReleaseVsAcquireAndTryAcquire) {
    check([] {
        Core sem(0);
        twist::assist::Shared<int> payload{0};
        ThreadWaiter waiter;
        twist::ed::std::thread releaser([&] {
            payload.Write(1);
            sem.release(2);
        });
        twist::ed::std::thread trier([&] {
            FIBERS_WAIT_BOUNDED(sem.try_acquire(1), "try_acquire never succeeded (permit lost)");
            TWIST_ASSERT_M(payload.Read() == 1, "releaser's write visible after try_acquire");
        });
        waiter.acquire(sem, 1);
        TWIST_ASSERT_M(payload.Read() == 1, "releaser's write visible after acquire");
        releaser.join();
        trier.join();
        TWIST_ASSERT_M(sem.available_approx() == 0 && !sem.has_waiters_approx(), "both permits taken");
    });
}

// A 2-permit acquire collects two release(1)s from two threads (either may
// come before the waiter is queued, on the fast path); it sees both writes.
TEST(SemaphoreTwist, MultiPermitCollectsTwoReleases) {
    check([] {
        Core sem(0);
        twist::assist::Shared<int> cells[2] = {0, 0};
        ThreadWaiter waiter;
        twist::ed::std::thread releasers[2] = {
            twist::ed::std::thread([&] {
                cells[0].Write(1);
                sem.release(1);
            }),
            twist::ed::std::thread([&] {
                cells[1].Write(1);
                sem.release(1);
            }),
        };
        waiter.acquire(sem, 2);
        for (auto& cell : cells) {
            TWIST_ASSERT_M(cell.Read() == 1, "every releaser's write visible after acquire(2)");
        }
        for (auto& t : releasers) {
            t.join();
        }
        TWIST_ASSERT_M(sem.available_approx() == 0, "both permits taken");
    });
}

// A 1-permit semaphore as a lock for three threads (one of them asks with
// try_acquire): a plain cell incremented by each holder never races.
TEST(SemaphoreTwist, OnePermitMutualExclusion) {
    check([] {
        Core sem(1);
        twist::assist::Shared<int> counter{0};
        ThreadWaiter waiters[2];
        auto holder = [&](int i) {
            return [&, i] {
                waiters[i].acquire(sem, 1);
                counter.Write(counter.Read() + 1);
                sem.release(1);
            };
        };
        twist::ed::std::thread threads[2] = {twist::ed::std::thread(holder(0)), twist::ed::std::thread(holder(1))};
        FIBERS_WAIT_BOUNDED(sem.try_acquire(1), "try_acquire never succeeded");
        counter.Write(counter.Read() + 1);
        sem.release(1);
        for (auto& t : threads) {
            t.join();
        }
        TWIST_ASSERT_M(counter.Read() == 3, "three increments");
        TWIST_ASSERT_M(sem.available_approx() == 1, "the permit is back");
    });
}

// A 2-permit and a 1-permit waiter against three release(1)s from one
// thread: whichever queues first is served first (a 2 at the head holds the
// 1 behind it while permits accumulate); both are granted, both see the
// releaser's write.
TEST(SemaphoreTwist, HeadOfLineHandOff) {
    check([] {
        Core sem(0);
        twist::assist::Shared<int> payload{0};
        ThreadWaiter big;
        ThreadWaiter small;
        Counter order{0};
        int big_pos = -1;
        int small_pos = -1;
        twist::ed::std::thread big_thread([&] {
            big.acquire(sem, 2);
            big_pos = static_cast<int>(order.fetch_add(1));
            TWIST_ASSERT_M(payload.Read() == 1, "releaser's write visible to the big waiter");
        });
        twist::ed::std::thread small_thread([&] {
            small.acquire(sem, 1);
            small_pos = static_cast<int>(order.fetch_add(1));
            TWIST_ASSERT_M(payload.Read() == 1, "releaser's write visible to the small waiter");
        });
        payload.Write(1);
        sem.release(1);
        sem.release(1);
        sem.release(1);
        big_thread.join();
        small_thread.join();
        TWIST_ASSERT_M(big_pos >= 0 && small_pos >= 0, "both granted");
        TWIST_ASSERT_M(sem.available_approx() == 0, "all three permits taken");
        TWIST_ASSERT_M(sem.waiters_approx() == 0, "nobody queued");
    });
}

// waiters_approx(): the queue's length, kept under the lock. Two threads
// queue; the releaser waits until the counter alone says both are queued,
// then each release(1) hands one permit over and the count it reads right
// after its own release is exact. A third thread reads it concurrently: it
// never exceeds the number of waiters.
TEST(SemaphoreTwist, WaitersCount) {
    check([] {
        Core sem(0);
        ThreadWaiter waiters[2];
        Counter granted{0};
        twist::ed::std::thread threads[2] = {
            twist::ed::std::thread([&] {
                waiters[0].acquire(sem, 1);
                granted.fetch_add(1);
            }),
            twist::ed::std::thread([&] {
                waiters[1].acquire(sem, 1);
                granted.fetch_add(1);
            }),
        };
        twist::ed::std::thread reader([&] {
            for (int i = 0; i < 3; ++i) {
                TWIST_ASSERT_M(sem.waiters_approx() <= 2, "more waiters counted than exist");
            }
        });
        FIBERS_WAIT_BOUNDED(sem.waiters_approx() == 2, "waiters never counted");
        TWIST_ASSERT_M(sem.has_waiters_approx(), "WAITERS set with a nonempty queue");
        sem.release(1);
        TWIST_ASSERT_M(sem.waiters_approx() == 1, "one waiter granted, one left");
        sem.release(1);
        TWIST_ASSERT_M(sem.waiters_approx() == 0, "both granted");
        for (auto& t : threads) {
            t.join();
        }
        reader.join();
        TWIST_ASSERT_M(granted.load() == 2 && sem.available_approx() == 0, "both permits taken");
    });
}

// ------------------------------------------------------------------ fibers

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

// Fiber stacks mapped once, OUTSIDE the simulation, handed out in a fixed
// order (see tests/fiber/twist/fiber_twist_test.cpp: the simulator's
// determinism check digests atomic values, and fiber addresses travel
// through atomics).
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

// What a fiber needs from a scheduler, and nothing else.
class MiniScheduler {
public:
    using SyncPolicy = TwistSync;

    explicit MiniScheduler(std::size_t workers) : pool_(tiny(workers)) {}

    tp::ThreadPool<TwistSync>& pool() noexcept { return pool_; }
    TestStackPool& stacks() noexcept { return stacks_; }

private:
    TestStackPool stacks_;  // outlives the pool's queues
    tp::ThreadPool<TwistSync> pool_;
};

static_assert(fiber::FiberScheduler<MiniScheduler>);

using Semaphore = fiber::BasicSemaphore<MiniScheduler>;
using SemaphoreGuard = fiber::BasicSemaphoreGuard<MiniScheduler>;

// Three fibers on two workers share one permit (guards); the parked ones
// are granted by the releasing fiber (NOTIFIED or PARKED wake).
TEST(SemaphoreTwist, FibersShareOnePermit) {
    map_test_stacks();
    check([] {
        constexpr int kFibers = 3;
        Counter done{0};
        twist::assist::Shared<int> counter{0};
        MiniScheduler scheduler(2);
        Semaphore sem(1);
        for (int f = 0; f < kFibers; ++f) {
            fiber::go(scheduler, [&]() noexcept {
                {
                    SemaphoreGuard guard(sem);
                    counter.Write(counter.Read() + 1);
                }
                done.fetch_add(1);
            });
        }
        FIBERS_WAIT_BOUNDED(done.load() == kFibers, "fiber never granted (lost wakeup)");
        TWIST_ASSERT_M(counter.Read() == kFibers, "every increment");
        scheduler.pool().stop();
    });
}

// A fiber acquires 2 permits released by a plain thread and by another
// fiber; it sees both writes.
TEST(SemaphoreTwist, FiberMultiPermitFromThreadAndFiber) {
    map_test_stacks();
    check([] {
        Counter done{0};
        twist::assist::Shared<int> cells[2] = {0, 0};
        MiniScheduler scheduler(2);
        Semaphore sem(0);
        fiber::go(scheduler, [&]() noexcept {
            sem.acquire(2);
            for (auto& cell : cells) {
                TWIST_ASSERT_M(cell.Read() == 1, "every releaser's write visible after acquire(2)");
            }
            done.fetch_add(1);
        });
        fiber::go(scheduler, [&]() noexcept {
            cells[1].Write(1);
            sem.release();
        });
        twist::ed::std::thread releaser([&] {
            cells[0].Write(1);
            sem.release();
        });
        releaser.join();
        FIBERS_WAIT_BOUNDED(done.load() == 1, "fiber never granted (lost wakeup)");
        scheduler.pool().stop();
    });
}

// A plain thread blocks in acquire() (ThreadWaiter) until a fiber releases.
TEST(SemaphoreTwist, ThreadAcquiresFromFiber) {
    map_test_stacks();
    check([] {
        twist::assist::Shared<int> payload{0};
        MiniScheduler scheduler(1);
        Semaphore sem(0);
        fiber::go(scheduler, [&]() noexcept {
            payload.Write(1);
            sem.release();
        });
        sem.acquire();  // not a fiber: blocks this thread
        TWIST_ASSERT_M(payload.Read() == 1, "fiber's write visible after acquire");
        scheduler.pool().stop();
    });
}

}  // namespace
