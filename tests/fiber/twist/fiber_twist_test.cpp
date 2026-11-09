// Fibers over TwistSync under Twist: the fiber state machine (park / wake,
// NOTIFIED, migration through the ThreadPool), fiber::Mutex and its MutexCore.
//
// TWIST_SIM:    each scenario is explored with the Random and PCT schedulers
//               of the deterministic simulator.
// TWIST_FAULTY: each scenario runs several times on real threads with fault
//               injection (clang-twist-faulty-tsan adds TSan).
//
// Data the protocols must hand over lives in twist::assist::Shared cells,
// whose accesses Twist checks for happens-before: a missing release /
// acquire in the state word or the mutex shows up as a race on a cell.
//
// Most scenarios run on MiniScheduler (a ThreadPool and a StackPool: all a
// fiber needs, see fiber::FiberScheduler), which keeps the simulated thread
// count small; one runs on the full BasicScheduler with FakeBackend pollers
// (sleep_for through a PollerPool).

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
#include "fibers/fiber/mutex.hpp"
#include "fibers/poller/fake_backend.hpp"
#include "fibers/poller/poller_pool.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/stack/stack_pool.hpp"
#include "fibers/sync/twist_sync.hpp"
#include "fibers/thread_pool/thread_pool.hpp"
#include "fibers/wait/mutex_core.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fiber = fibers::fiber;
namespace tp = fibers::thread_pool;
using fibers::sync::TwistSync;

namespace {

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

// Twist's own frames (and the simulator's stack reserve) run on fiber stacks.
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

// Fiber stacks for the scenarios: mapped once, OUTSIDE the simulation
// (map_test_stacks() before check()), and handed out in a fixed order. The
// simulator checks that runs are deterministic, digesting the values of
// atomic loads, and fiber addresses travel through atomics (the pool's
// queues, the mutex word, wake slots). Stacks mmap'ed inside a run (as
// StackPool does) land at different addresses in different runs, because
// the simulator's determinism check kills runs midway, leaking mappings.
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

// MiniScheduler plus what this_fiber::sleep_* use: a PollerPool of
// FakeBackend pollers (simulated time), now() and the Timer type.
class SleepScheduler {
public:
    using SyncPolicy = TwistSync;
    using PollerPool = fibers::poller::PollerPool<fibers::poller::FakeBackend<TwistSync>, TwistSync>;
    using Timer = PollerPool::Timer;

    SleepScheduler(std::size_t workers, std::size_t pollers)
        : pool_(tiny(workers)),
          pollers_(fibers::poller::TaskSink::of(pool_), {.num_pollers = pollers}) {
        pollers_.start();
    }

    ~SleepScheduler() { stop(); }

    void stop() noexcept {
        pollers_.stop();
        pool_.stop();
    }

    tp::ThreadPool<TwistSync>& pool() noexcept { return pool_; }
    TestStackPool& stacks() noexcept { return stacks_; }
    PollerPool& pollers() noexcept { return pollers_; }
    fibers::poller::TimePoint now() noexcept { return pollers_.at(0).now(); }

private:
    TestStackPool stacks_;
    tp::ThreadPool<TwistSync> pool_;
    PollerPool pollers_;
};

using Fiber = fiber::BasicFiber<MiniScheduler>;
using Mutex = fiber::BasicMutex<MiniScheduler>;
namespace this_fiber = fiber::this_fiber;

// A fiber parks after publishing itself; an outside thread wakes it. The
// wake races with the park: it lands either before the trampoline's CAS
// (NOTIFIED: resumed on the same worker) or after (PARKED: rescheduled
// through the pool, maybe on another worker). Either way the waker's write
// must be visible to the fiber, and the fiber's write to the waker.
TEST(FiberTwist, ParkWakeFromThread) {
    map_test_stacks();
    check([] {
        constexpr int kRounds = 2;
        Counter done{0};
        twist::ed::std::atomic<Fiber*> published{nullptr};
        twist::assist::Shared<int> to_waker{0};
        twist::assist::Shared<int> to_fiber{0};
        // Written by the fiber after it published itself: only the park CAS
        // (release) -> wake CAS (acquire) -> queue chain orders it before
        // the read after a resume on another worker.
        twist::assist::Shared<int> own{0};
        MiniScheduler scheduler(2);
        fiber::go(scheduler, [&]() noexcept {
            for (int i = 1; i <= kRounds; ++i) {
                to_waker.Write(i);
                published.store(this_fiber::self<MiniScheduler>().get(), std::memory_order_release);
                own.Write(i);
                this_fiber::park<MiniScheduler>();
                TWIST_ASSERT_M(to_fiber.Read() == i, "waker's write visible after wake");
                TWIST_ASSERT_M(own.Read() == i, "own write visible after migration");
            }
            done.fetch_add(1);
        });
        twist::ed::std::thread waker([&] {
            for (int i = 1; i <= kRounds; ++i) {
                Fiber* f = nullptr;
                FIBERS_WAIT_BOUNDED((f = published.exchange(nullptr, std::memory_order_acquire)) != nullptr,
                                    "fiber never published itself");
                TWIST_ASSERT_M(to_waker.Read() == i, "fiber's write visible to the waker");
                to_fiber.Write(i);
                f->wake();
            }
        });
        waker.join();
        FIBERS_WAIT_BOUNDED(done.load() == 1, "fiber never finished (lost wakeup)");
        scheduler.pool().stop();
    });
}

// Two fibers hand control to each other: each wakes the other, then parks.
// Every wake targets a fiber that may not have left its stack yet.
TEST(FiberTwist, PingPongBetweenFibers) {
    map_test_stacks();
    check([] {
        constexpr int kRounds = 3;
        Counter done{0};
        twist::ed::std::atomic<Fiber*> slot[2] = {nullptr, nullptr};
        twist::assist::Shared<int> ball{0};
        MiniScheduler scheduler(2);
        auto player = [&](int me) {
            return [&, me]() noexcept {
                Fiber* self = this_fiber::self<MiniScheduler>().get();
                slot[me].store(self, std::memory_order_release);
                for (int i = 0; i < kRounds; ++i) {
                    if (me == 0 || i > 0) {
                        // Wait for our turn: the ball is handed over by wake.
                        this_fiber::park<MiniScheduler>();
                    }
                    ball.Write(ball.Read() + 1);
                    Fiber* other = nullptr;
                    FIBERS_WAIT_BOUNDED((other = slot[1 - me].load(std::memory_order_acquire)) != nullptr,
                                        "partner never started");
                    if (!(me == 0 && i == kRounds - 1)) {
                        other->wake();
                    }
                }
                done.fetch_add(1);
            };
        };
        // Player 0 parks first; player 1 serves once 0 has published itself.
        fiber::go(scheduler, player(0));
        FIBERS_WAIT_BOUNDED(slot[0].load() != nullptr, "player 0 never started");
        fiber::go(scheduler, player(1));
        FIBERS_WAIT_BOUNDED(done.load() == 2, "lost wakeup between fibers");
        scheduler.pool().stop();
        TWIST_ASSERT_M(ball.Read() == 2 * kRounds, "every hit counted");
    });
}

// Mutex: a plain Shared counter under the lock, one yield inside the
// critical section so that waiters really park and get woken.
TEST(FiberTwist, MutexCounter) {
    map_test_stacks();
    check([] {
        constexpr int kFibers = 3;
        constexpr int kIncrements = 2;
        Counter done{0};
        Mutex mutex(0);  // park at once
        twist::assist::Shared<int> counter{0};
        MiniScheduler scheduler(2);
        for (int f = 0; f < kFibers; ++f) {
            fiber::go(scheduler, [&]() noexcept {
                for (int k = 0; k < kIncrements; ++k) {
                    mutex.lock();
                    const int value = counter.Read();
                    this_fiber::yield<MiniScheduler>();
                    counter.Write(value + 1);
                    mutex.unlock();
                }
                done.fetch_add(1);
            });
        }
        FIBERS_WAIT_BOUNDED(done.load() == kFibers, "lost wakeup in Mutex");
        scheduler.pool().stop();
        TWIST_ASSERT_M(counter.Read() == kFibers * kIncrements, "no lost update");
    });
}

// Mutex without yields: short critical sections, barging, try_lock, a few
// spins before parking.
TEST(FiberTwist, MutexBargingTryLock) {
    map_test_stacks();
    check([] {
        constexpr int kFibers = 3;
        Counter done{0};
        Mutex mutex(4);  // spin a little before parking
        twist::assist::Shared<int> counter{0};
        MiniScheduler scheduler(2);
        for (int f = 0; f < kFibers; ++f) {
            fiber::go(scheduler, [&, f]() noexcept {
                for (int k = 0; k < 2; ++k) {
                    if ((f + k) % 2 == 0 && mutex.try_lock()) {
                        counter.Write(counter.Read() + 1);
                        mutex.unlock();
                        continue;
                    }
                    mutex.lock();
                    counter.Write(counter.Read() + 1);
                    mutex.unlock();
                }
                done.fetch_add(1);
            });
        }
        FIBERS_WAIT_BOUNDED(done.load() == kFibers, "lost wakeup in Mutex");
        scheduler.pool().stop();
        TWIST_ASSERT_M(counter.Read() == 2 * kFibers, "no lost update");
    });
}

// MutexCore alone, on threads, for WOKEN: a barging thread (try_lock only)
// overtakes woken waiters, which then re-enqueue. While a woken waiter has
// not made its next attempt unlock() wakes nobody, so at most one wake is
// in flight; the attempt (lock or re-enqueue) must re-enable waking, or a
// waiter hangs (lost wakeup).
TEST(FiberTwist, MutexCoreWokenWhileBarging) {
    check([] {
        using Core = fibers::wait::MutexCore<TwistSync>;
        constexpr int kRounds = 2;
        struct Waiter {
            twist::ed::std::atomic<bool> woken{false};
            Counter* in_flight = nullptr;

            static void wake(void* self) noexcept {
                auto* waiter = static_cast<Waiter*>(self);
                TWIST_ASSERT_M(waiter->in_flight->fetch_add(1) == 0, "two woken waiters in flight");
                waiter->woken.store(true, std::memory_order_release);
            }
        };
        Core core;
        Counter in_flight{0};
        twist::assist::Shared<int> counter{0};
        auto waiter_thread = [&] {
            Waiter waiter;
            waiter.in_flight = &in_flight;
            fibers::wait::WaitNode node{nullptr, &waiter, &Waiter::wake};
            for (int k = 0; k < kRounds; ++k) {
                for (bool woken = false; !core.lock_or_enqueue(node, woken); woken = true) {
                    FIBERS_WAIT_BOUNDED(waiter.woken.load(std::memory_order_acquire), "lost wakeup in MutexCore");
                    waiter.woken.store(false, std::memory_order_relaxed);
                    in_flight.fetch_sub(1);
                }
                counter.Write(counter.Read() + 1);
                core.unlock();
            }
        };
        twist::ed::std::thread first(waiter_thread);
        twist::ed::std::thread second(waiter_thread);
        for (int k = 0; k < kRounds; ++k) {
            FIBERS_WAIT_BOUNDED(core.try_lock(), "barger never got the lock");
            counter.Write(counter.Read() + 1);
            core.unlock();
        }
        first.join();
        second.join();
        TWIST_ASSERT_M(counter.Read() == 3 * kRounds, "no lost update");
    });
}

// sleep_for through a PollerPool of two FakeBackend pollers (simulated
// time): the timer goes to one of them, its callback wakes the fiber, maybe
// while the fiber is still on its stack (NOTIFIED).
TEST(FiberTwist, SleepThroughPollerPool) {
    map_test_stacks();
    check(
        [] {
            Counter done{0};
            twist::assist::Shared<int> cell{0};
            SleepScheduler scheduler(2, 2);
            for (int f = 0; f < 2; ++f) {
                fiber::go(scheduler, [&, f]() noexcept {
                    const auto start = scheduler.now();
                    const auto duration = std::chrono::milliseconds(f);  // 0 ms: fires at once
                    this_fiber::sleep_for<SleepScheduler>(duration);
                    TWIST_ASSERT_M(scheduler.now() >= start + duration, "woke up early");
                    if (f == 1) {
                        cell.Write(1);
                    }
                    done.fetch_add(1);
                });
            }
            FIBERS_WAIT_BOUNDED(done.load() == 2, "sleeping fiber never woken");
            scheduler.stop();
            TWIST_ASSERT_M(cell.Read() == 1, "fiber's write visible after join");
        },
        500);
}

#if !defined(__TWIST_SIM__)
// The full Scheduler (its own StackPool, TaskMemoryPool, BlockingPool) under
// fault injection only: StackPool maps stacks inside the run (see above).
using TwistScheduler = fibers::sched::BasicScheduler<TwistSync, fibers::poller::FakeBackend<TwistSync>>;

TEST(FiberTwist, FullSchedulerMutexAndSleep) {
    check([] {
        Counter done{0};
        twist::assist::Shared<int> counter{0};
        fibers::sched::SchedulerOptions options;
        options.pool = tiny(2);
        options.pollers.num_pollers = 2;
        options.blocking.num_threads = 1;
        options.stacks = {.stack_size = kStackSize, .max_stacks = 16, .shards = 2, .thread_cache_capacity = 0};
        options.task_memory.initial_blocks = 4;
        TwistScheduler scheduler(options);
        fiber::BasicMutex<TwistScheduler> mutex;
        for (int f = 0; f < 3; ++f) {
            fiber::go(scheduler, [&, f]() noexcept {
                this_fiber::sleep_for<TwistScheduler>(std::chrono::milliseconds(f % 2));
                std::lock_guard guard(mutex);
                counter.Write(counter.Read() + 1);
                scheduler.submit([&]() noexcept { done.fetch_add(1); });
            });
        }
        FIBERS_WAIT_BOUNDED(done.load() == 3, "fiber or callback lost");
        scheduler.stop();
        TWIST_ASSERT_M(counter.Read() == 3, "no lost update");
    });
}
#endif

}  // namespace
