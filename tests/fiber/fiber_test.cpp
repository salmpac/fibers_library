// Scheduler, TaskMemoryPool, stackful fibers, Mutex, sleep (StdSync).
//
// Fibers never use gtest assertions themselves: they record into atomics
// that the test thread checks after joining them (test-local Latch).

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/mutex.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/sched/task_memory_pool.hpp"
#include "fibers/wait/mutex_core.hpp"

namespace fiber = fibers::fiber;
namespace sched = fibers::sched;
namespace this_fiber = fibers::fiber::this_fiber;
using fibers::fiber::Fiber;
using fibers::sched::Scheduler;
using namespace std::chrono_literals;

namespace {

// The running thread's id, re-read after every switch: get_id() is
// pthread_self(), declared const by glibc, so a direct call in a fiber could
// be hoisted across a suspend (see coro/coroutine.hpp).
[[gnu::noinline]] std::thread::id current_thread_id() {
    asm volatile("" ::: "memory");
    return std::this_thread::get_id();
}

// Joins fibers from a non-fiber thread: the last count_down() wakes wait().
// Declare it BEFORE the scheduler, so the workers (that may still be inside
// count_down's notify) are joined before it goes away.
class Latch {
public:
    explicit Latch(std::int64_t count) : count_(count) {}

    void count_down(std::int64_t n = 1) noexcept {
        if (count_.fetch_sub(n, std::memory_order_acq_rel) == n) {
            count_.notify_all();
        }
    }

    // false on timeout (the test fails instead of hanging).
    bool wait(std::chrono::milliseconds timeout = 30s) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        for (;;) {
            if (count_.load(std::memory_order_acquire) == 0) {
                return true;
            }
            if (std::chrono::steady_clock::now() > deadline) {
                return false;
            }
            std::this_thread::sleep_for(100us);
        }
    }

    std::int64_t remaining() const noexcept { return count_.load(); }

private:
    std::atomic<std::int64_t> count_;
};

// Two pollers everywhere: sleep_for's timers go round robin over them.
sched::SchedulerOptions options(std::size_t workers, std::size_t pollers = 2) {
    sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = pollers;
    o.blocking.num_threads = 1;
    o.stacks.max_stacks = 4096;
    return o;
}

// ------------------------------------------------------------- TaskMemoryPool

TEST(TaskMemoryPool, SizeClasses) {
    using Pool = sched::TaskMemoryPool<>;
    static_assert(Pool::block_size_for(1) == 64);
    static_assert(Pool::block_size_for(64) == 64);
    static_assert(Pool::block_size_for(65) == 128);
    static_assert(Pool::block_size_for(256) == 256);
    static_assert(Pool::block_size_for(512) == 512);
    static_assert(Pool::block_size_for(513) == 0);
    Pool pool({.initial_blocks = 4});
    void* a = pool.acquire<128>();
    void* b = pool.acquire<128>();
    EXPECT_NE(a, b);
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(a) % Pool::kBlockAlign, 0u);
    pool.release<128>(a);
    pool.release<128>(b);
    EXPECT_GE(pool.capacity<128>(), 4u);
}

struct DtorCounter {
    std::atomic<int>* destroyed;
    std::atomic<int>* calls;
    DtorCounter(std::atomic<int>* d, std::atomic<int>* c) noexcept : destroyed(d), calls(c) {}
    DtorCounter(DtorCounter&& other) noexcept : destroyed(other.destroyed), calls(other.calls) {
        other.destroyed = nullptr;
    }
    ~DtorCounter() {
        if (destroyed != nullptr) {
            destroyed->fetch_add(1);
        }
    }
    void operator()() noexcept { calls->fetch_add(1); }
};

TEST(Scheduler, SubmitCallbacks) {
    constexpr int kTasks = 10'000;
    Latch done(kTasks);
    std::atomic<int> destroyed{0};
    std::atomic<int> calls{0};
    {
        Scheduler scheduler(options(4));
        for (int i = 0; i < kTasks; ++i) {
            scheduler.submit([&done, counter = DtorCounter(&destroyed, &calls)]() mutable noexcept {
                counter();
                done.count_down();
            });
        }
        ASSERT_TRUE(done.wait());
    }
    EXPECT_EQ(calls.load(), kTasks);
    EXPECT_EQ(destroyed.load(), kTasks);  // every closure destroyed after its call
}

TEST(Scheduler, CallbackSizeClassesAndHeader) {
    Scheduler scheduler(options(1));
    char big[300] = {};
    auto small = []() noexcept {};
    auto large = [big]() noexcept { static_cast<void>(big); };
    fibers::thread_pool::Task t1 = scheduler.make_task(small);
    fibers::thread_pool::Task t2 = scheduler.make_task(large);
    const auto r1 = t1.release();
    const auto r2 = t2.release();
    // TaskHeader at the start of every memo.
    EXPECT_EQ(static_cast<Scheduler::TaskHeader*>(r1.memo)->scheduler, &scheduler);
    EXPECT_EQ(static_cast<Scheduler::TaskHeader*>(r1.memo)->fiber, nullptr);
    EXPECT_EQ(static_cast<Scheduler::TaskHeader*>(r2.memo)->scheduler, &scheduler);
    fibers::thread_pool::Task::from_raw(r1).reset();  // drop: closure destroyed, block back
    fibers::thread_pool::Task::from_raw(r2).reset();
    static_assert(sizeof(fibers::thread_pool::Task) == 24);
}

TEST(Scheduler, QueuedCallbacksDroppedOnDestruction) {
    std::atomic<int> destroyed{0};
    std::atomic<int> calls{0};
    {
        Scheduler scheduler(options(2));
        scheduler.stop();
        for (int i = 0; i < 100; ++i) {
            scheduler.submit(DtorCounter(&destroyed, &calls));
        }
    }
    EXPECT_EQ(calls.load(), 0);
    EXPECT_EQ(destroyed.load(), 100);
}

// ------------------------------------------------------------------ fibers

TEST(Fiber, GoRunsToCompletion) {
    Latch done(1);
    std::atomic<int> value{0};
    Scheduler scheduler(options(2));
    fiber::go(scheduler, [&]() noexcept {
        value.store(42);
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(value.load(), 42);
}

TEST(Fiber, ClosureDestroyedOnCompletion) {
    std::atomic<int> destroyed{0};
    std::atomic<int> calls{0};
    Latch done(1);
    {
        Scheduler scheduler(options(1));
        fiber::go(scheduler, [&done, counter = DtorCounter(&destroyed, &calls)]() mutable noexcept {
            counter();
            done.count_down();
        });
        ASSERT_TRUE(done.wait());
    }
    EXPECT_EQ(calls.load(), 1);
    EXPECT_EQ(destroyed.load(), 1);
}

TEST(Fiber, NeverStartedFiberDroppedOnDestruction) {
    std::atomic<int> destroyed{0};
    std::atomic<int> calls{0};
    {
        Scheduler scheduler(options(1));
        scheduler.stop();
        fiber::go(scheduler, DtorCounter(&destroyed, &calls));
        EXPECT_EQ(scheduler.stacks().mapped_stacks(), 1u);
    }
    EXPECT_EQ(calls.load(), 0);
    EXPECT_EQ(destroyed.load(), 1);
}

TEST(Fiber, InFiberAndHeader) {
    Latch done(1);
    std::atomic<bool> in{false};
    std::atomic<bool> header_ok{false};
    EXPECT_FALSE(this_fiber::in_fiber());
    Scheduler scheduler(options(1));
    fiber::go(scheduler, [&]() noexcept {
        in.store(this_fiber::in_fiber());
        Fiber* self = this_fiber::self().get();
        header_ok.store(self->header().scheduler == &scheduler && self->header().fiber == self &&
                        static_cast<const void*>(&self->header()) == static_cast<const void*>(self));
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_TRUE(in.load());
    EXPECT_TRUE(header_ok.load());
}

TEST(Fiber, YieldInterleavesOnOneWorker) {
    constexpr int kSteps = 50;
    Latch done(2);
    std::vector<int> trace;  // one worker: no data race
    trace.reserve(2 * kSteps);
    Scheduler scheduler(options(1));
    // Spawn both from inside one fiber, so both are queued before either runs.
    fiber::go(scheduler, [&]() noexcept {
        for (int id = 0; id < 2; ++id) {
            fiber::go([&, id]() noexcept {
                for (int i = 0; i < kSteps; ++i) {
                    trace.push_back(id);
                    this_fiber::yield();
                }
                done.count_down();
            });
        }
    });
    ASSERT_TRUE(done.wait());
    ASSERT_EQ(trace.size(), static_cast<std::size_t>(2 * kSteps));
    for (std::size_t i = 0; i < trace.size(); ++i) {
        EXPECT_EQ(trace[i], static_cast<int>(i % 2)) << "at " << i;
    }
}

TEST(Fiber, GoFromInsideFiber) {
    constexpr int kChildren = 100;
    Latch done(kChildren);
    std::atomic<int> sum{0};
    Scheduler scheduler(options(3));
    fiber::go(scheduler, [&]() noexcept {
        for (int i = 1; i <= kChildren; ++i) {
            fiber::go([&, i]() noexcept {
                sum.fetch_add(i);
                done.count_down();
            });
        }
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(sum.load(), kChildren * (kChildren + 1) / 2);
}

TEST(Fiber, ManyFibersManyWorkers) {
    constexpr int kFibers = 2'000;
    constexpr int kYields = 20;
    Latch done(kFibers);
    std::atomic<std::int64_t> steps{0};
    Scheduler scheduler(options(4));
    for (int i = 0; i < kFibers; ++i) {
        fiber::go(scheduler, [&]() noexcept {
            for (int y = 0; y < kYields; ++y) {
                steps.fetch_add(1, std::memory_order_relaxed);
                this_fiber::yield();
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(steps.load(), std::int64_t{kFibers} * kYields);
    // Stacks are reused: far fewer than kFibers were ever mapped.
    EXPECT_LE(scheduler.stacks().mapped_stacks(), scheduler.stacks().max_stacks());
}

// Woken by an external thread each time, after a pause in which the idle
// workers park: an external submit then goes to the next worker of the
// submitting thread's round robin, so the fiber hops between threads. (A
// worker that is still spinning would be preferred, hence the pause and the
// loop until a second thread is seen.) Also a regression test for
// ThreadPool::pick_target: with every worker parked and at most kTargetScan
// workers, its scan used to advance the round robin by exactly the worker
// count, so every external submit went to the same worker.
TEST(Fiber, MigratesBetweenThreads) {
    constexpr int kMaxHops = 500;
    Latch done(1);
    std::atomic<Fiber*> parked{nullptr};
    std::atomic<bool> finished{false};
    std::set<std::thread::id> seen;  // only the fiber touches it
    std::atomic<std::size_t> distinct{0};
    Scheduler scheduler(options(4));
    fiber::go(scheduler, [&]() noexcept {
        seen.insert(current_thread_id());
        for (int i = 0; i < kMaxHops && seen.size() < 2; ++i) {
            Fiber* self = this_fiber::self().get();
            parked.store(self, std::memory_order_release);
            this_fiber::park();
            seen.insert(current_thread_id());
        }
        distinct.store(seen.size());
        finished.store(true, std::memory_order_release);
        done.count_down();
    });
    while (!finished.load(std::memory_order_acquire)) {
        Fiber* f = parked.exchange(nullptr, std::memory_order_acquire);
        if (f == nullptr) {
            std::this_thread::yield();
            continue;
        }
        std::this_thread::sleep_for(2ms);  // let the workers park
        f->wake();
    }
    ASSERT_TRUE(done.wait());
    EXPECT_GT(distinct.load(), 1u);
}

// The NOTIFIED path, deterministically: the fiber publishes itself and stays
// on its stack until the waker's wake() has returned, so the wake finds
// RUNNING and only sets NOTIFIED; the trampoline's park CAS fails and resumes
// the fiber at once.
TEST(Fiber, WakeWhileStillRunning) {
    constexpr int kRounds = 100;
    Latch done(1);
    std::atomic<Fiber*> published{nullptr};
    std::atomic<bool> woken{false};
    std::atomic<int> notified_seen{0};
    std::atomic<int> resumed{0};
    Scheduler scheduler(options(2));
    fiber::go(scheduler, [&]() noexcept {
        for (int i = 0; i < kRounds; ++i) {
            woken.store(false, std::memory_order_relaxed);
            published.store(this_fiber::self().get(), std::memory_order_release);
            while (!woken.load(std::memory_order_acquire)) {
                fibers::sync::cpu_relax();
            }
            this_fiber::park();  // returns at once: the wake already happened
            resumed.fetch_add(1);
        }
        done.count_down();
    });
    for (int i = 0; i < kRounds; ++i) {
        Fiber* f = nullptr;
        while ((f = published.exchange(nullptr, std::memory_order_acquire)) == nullptr) {
            std::this_thread::yield();
        }
        f->wake();
        if (f->state_approx() == (Fiber::kRunning | Fiber::kNotified)) {
            notified_seen.fetch_add(1);
        }
        woken.store(true, std::memory_order_release);
    }
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(resumed.load(), kRounds);
    EXPECT_EQ(notified_seen.load(), kRounds);
}

TEST(FiberDeathTest, DoubleWake) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            std::atomic<Fiber*> published{nullptr};
            std::atomic<bool> release{false};
            Scheduler scheduler(options(1));
            fiber::go(scheduler, [&]() noexcept {
                published.store(this_fiber::self().get());
                while (!release.load()) {
                    fibers::sync::cpu_relax();
                }
                this_fiber::park();
            });
            Fiber* f = nullptr;
            while ((f = published.load()) == nullptr) {
                std::this_thread::yield();
            }
            f->wake();  // RUNNING -> NOTIFIED
            f->wake();  // NOTIFIED: double wake
        },
        "double wake");
}

#ifndef NDEBUG
// Debug builds check the "published fibers park" rule on yield.
TEST(FiberDeathTest, YieldWithPendingWake) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            std::atomic<Fiber*> published{nullptr};
            std::atomic<bool> release{false};
            Scheduler scheduler(options(1));
            fiber::go(scheduler, [&]() noexcept {
                published.store(this_fiber::self().get());
                while (!release.load()) {
                    fibers::sync::cpu_relax();
                }
                this_fiber::yield();  // wrong: must park
            });
            Fiber* f = nullptr;
            while ((f = published.load()) == nullptr) {
                std::this_thread::yield();
            }
            f->wake();
            release.store(true);
            std::this_thread::sleep_for(10s);
        },
        "pending wake");
}
#endif

TEST(FiberDeathTest, YieldOutsideFiber) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(this_fiber::yield(), "outside a fiber");
}

// ------------------------------------------------------------------- Mutex

TEST(Mutex, CounterUnderContention) {
    constexpr int kFibers = 64;
    constexpr int kIncrements = 2'000;
    Latch done(kFibers);
    fiber::Mutex mutex;
    std::int64_t counter = 0;  // guarded by mutex
    std::atomic<int> inside{0};
    std::atomic<bool> overlap{false};
    Scheduler scheduler(options(4));
    for (int i = 0; i < kFibers; ++i) {
        fiber::go(scheduler, [&, i]() noexcept {
            for (int k = 0; k < kIncrements; ++k) {
                std::lock_guard guard(mutex);
                if (inside.fetch_add(1, std::memory_order_relaxed) != 0) {
                    overlap.store(true);
                }
                ++counter;
                if ((k + i) % 64 == 0) {
                    this_fiber::yield();  // hold the lock across a switch: others park
                }
                inside.fetch_sub(1, std::memory_order_relaxed);
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait(60s));
    EXPECT_FALSE(overlap.load());
    std::lock_guard guard(mutex);  // try_lock path from a thread: free now
    EXPECT_EQ(counter, std::int64_t{kFibers} * kIncrements);
}

// Lost wakeups would leave fibers parked forever (the latch times out).
// Short critical sections with yields outside them: fibers keep barging
// past woken ones, which must re-queue and still get through.
TEST(Mutex, BargingNoLostWakeups) {
    constexpr int kRounds = 20;
    constexpr int kFibers = 200;
    constexpr int kIterations = 200;
    for (int round = 0; round < kRounds; ++round) {
        Latch done(kFibers);
        fiber::Mutex mutex;
        std::int64_t counter = 0;
        Scheduler scheduler(options(4));
        for (int i = 0; i < kFibers; ++i) {
            fiber::go(scheduler, [&, i]() noexcept {
                for (int k = 0; k < kIterations; ++k) {
                    mutex.lock();
                    ++counter;
                    mutex.unlock();
                    if ((k + i) % 7 == 0) {
                        this_fiber::yield();
                    }
                }
                done.count_down();
            });
        }
        ASSERT_TRUE(done.wait(60s)) << "round " << round << ": " << done.remaining() << " fibers stuck";
        EXPECT_TRUE(mutex.try_lock());
        EXPECT_EQ(counter, std::int64_t{kFibers} * kIterations);
        mutex.unlock();
    }
}

TEST(Mutex, TryLockAndUniqueLock) {
    Latch done(1);
    std::atomic<int> result{0};
    fiber::Mutex mutex;
    Scheduler scheduler(options(2));
    fiber::go(scheduler, [&]() noexcept {
        int r = 0;
        {
            std::unique_lock lock(mutex);
            r += mutex.try_lock() ? 100 : 1;  // held: fails
            lock.unlock();
            r += mutex.try_lock() ? 10 : 1000;  // free: succeeds
            mutex.unlock();
        }
        result.store(r);
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(result.load(), 11);
}

// The waiter's FIFO order within a taken batch: with one worker and the
// holder parked on the lock-holding side, waiters queue up in arrival order
// and each unlock wakes the oldest.
TEST(Mutex, WakesInArrivalOrderOnOneWorker) {
    constexpr int kWaiters = 8;
    Latch done(kWaiters + 1);
    std::vector<int> order;  // under the mutex
    fiber::Mutex mutex;
    Scheduler scheduler(options(1));
    fiber::go(scheduler, [&]() noexcept {
        mutex.lock();
        for (int i = 0; i < kWaiters; ++i) {
            fiber::go([&, i]() noexcept {
                std::lock_guard guard(mutex);
                order.push_back(i);
                done.count_down();
            });
        }
        // Let all waiters run up to their park (each yield = one queue pass).
        for (int i = 0; i < 2 * kWaiters; ++i) {
            this_fiber::yield();
        }
        mutex.unlock();
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    ASSERT_EQ(order.size(), static_cast<std::size_t>(kWaiters));
    for (int i = 0; i < kWaiters; ++i) {
        EXPECT_EQ(order[static_cast<std::size_t>(i)], i);
    }
}

TEST(FiberDeathTest, MutexLockOutsideFiberWhenContended) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            fiber::Mutex mutex;
            mutex.lock();
            mutex.lock();
        },
        "outside a fiber");
}

// MutexCore over plain WaitNodes, no fibers: the front-end-agnostic core.
TEST(MutexCore, NodesWokenOnePerUnlockInFifoOrder) {
    fibers::wait::MutexCore<> core;
    std::vector<int> woken;
    struct Waiter {
        std::vector<int>* log;
        int id;
        static void wake(void* self) noexcept {
            auto* w = static_cast<Waiter*>(self);
            w->log->push_back(w->id);
        }
    };
    ASSERT_TRUE(core.try_lock());
    Waiter waiters[3] = {{&woken, 0}, {&woken, 1}, {&woken, 2}};
    fibers::wait::WaitNode nodes[3];
    for (int i = 0; i < 3; ++i) {
        nodes[i] = {nullptr, &waiters[i], &Waiter::wake};
        EXPECT_FALSE(core.lock_or_enqueue(nodes[i]));
    }
    core.unlock();  // takes the batch, wakes 0, unlocked
    EXPECT_EQ(woken, std::vector<int>({0}));
    EXPECT_FALSE(core.locked_approx());
    EXPECT_TRUE(core.lock_or_enqueue(nodes[0]));  // woken one gets the lock
    core.unlock();                               // wakes 1 from private
    EXPECT_EQ(woken, std::vector<int>({0, 1}));
    EXPECT_TRUE(core.try_lock());  // barging: someone else got there first
    EXPECT_FALSE(core.lock_or_enqueue(nodes[1]));  // 1 re-queues (stack)
    core.unlock();                                 // private: 2 first
    EXPECT_EQ(woken, std::vector<int>({0, 1, 2}));
    EXPECT_TRUE(core.lock_or_enqueue(nodes[2]));
    core.unlock();  // private empty: takes the stack (1)
    EXPECT_EQ(woken, std::vector<int>({0, 1, 2, 1}));
    EXPECT_TRUE(core.lock_or_enqueue(nodes[1]));
    core.unlock();
    EXPECT_FALSE(core.locked_approx());
}

// ------------------------------------------------------------------- sleep

TEST(Sleep, WakesInDeadlineOrderAndNotEarly) {
    constexpr int kFibers = 10;
    Latch done(kFibers);
    std::atomic<int> sequence{0};
    std::atomic<int> order[kFibers];
    std::atomic<bool> early{false};
    Scheduler scheduler(options(4));
    // Spawned in reverse deadline order.
    for (int i = kFibers - 1; i >= 0; --i) {
        fiber::go(scheduler, [&, i]() noexcept {
            const auto start = scheduler.now();
            const auto duration = std::chrono::milliseconds(20 * (i + 1));
            this_fiber::sleep_for(duration);
            if (scheduler.now() < start + duration) {
                early.store(true);
            }
            order[i].store(sequence.fetch_add(1));
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait());
    EXPECT_FALSE(early.load());
    for (int i = 0; i < kFibers; ++i) {
        EXPECT_EQ(order[i].load(), i);
    }
}

TEST(Sleep, ZeroAndPastDeadline) {
    constexpr int kFibers = 100;
    Latch done(kFibers);
    Scheduler scheduler(options(4));
    for (int i = 0; i < kFibers; ++i) {
        fiber::go(scheduler, [&, i]() noexcept {
            for (int k = 0; k < 20; ++k) {
                if ((i + k) % 2 == 0) {
                    this_fiber::sleep_for(0ns);
                } else {
                    this_fiber::sleep_until(scheduler.now() - 1s);
                }
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait());
    // Round robin over both pollers.
    EXPECT_GT(scheduler.pollers().at(0).stats().timers_fired, 0u);
    EXPECT_GT(scheduler.pollers().at(1).stats().timers_fired, 0u);
}

// The timer fires while the fiber is still on its stack (it waits for the
// callback before parking): NOTIFIED through the poller's TaskBatch path,
// i.e. what Fiber's own timer callback does (prepare_wake + out.push).
TEST(Sleep, TimerFiresWhileFiberStillRunning) {
    constexpr int kRounds = 50;
    struct Ctx {
        Fiber* fiber = nullptr;
        std::atomic<bool> fired{false};
        std::atomic<bool> was_notified{false};
    };
    Latch done(1);
    std::atomic<int> notified{0};
    Scheduler scheduler(options(2));
    fiber::go(scheduler, [&]() noexcept {
        for (int i = 0; i < kRounds; ++i) {
            Ctx ctx;
            ctx.fiber = this_fiber::self().get();
            Scheduler::Timer timer(&ctx, [](void* memo, fibers::poller::TimerEvent,
                                                    fibers::poller::TaskBatch& out) noexcept {
                auto* c = static_cast<Ctx*>(memo);
                Fiber* f = c->fiber;
                const bool schedule = f->prepare_wake();
                c->was_notified.store(!schedule && f->state_approx() == (Fiber::kRunning | Fiber::kNotified));
                c->fired.store(true, std::memory_order_release);
                if (schedule) {
                    out.push(f->task());
                }
            });
            timer.deadline = scheduler.now() - 1ms;  // already due
            scheduler.pollers().schedule(timer);
            while (!ctx.fired.load(std::memory_order_acquire)) {
                fibers::sync::cpu_relax();
            }
            this_fiber::park();
            if (ctx.was_notified.load()) {
                notified.fetch_add(1);
            }
        }
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(notified.load(), kRounds);
}

}  // namespace
