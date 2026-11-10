// SemaphoreCore over plain nodes, and its stackful front-end fiber::Semaphore
// / SemaphoreGuard (StdSync).
//
// Fibers never use gtest assertions themselves: they record into atomics
// (or plain data ordered by the primitive under test) that the test thread
// checks after joining them (test-local Latch).

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/semaphore.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/wait/semaphore_core.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fiber = fibers::fiber;
namespace sched = fibers::sched;
namespace this_fiber = fibers::fiber::this_fiber;
using fibers::sched::Scheduler;
using fibers::wait::SemaphoreCore;
using fibers::wait::SemaphoreNode;
using fibers::wait::WaitNode;
using namespace std::chrono_literals;

namespace {

// Joins fibers from a non-fiber thread. Declare it BEFORE the scheduler, so
// the workers (that may still be inside count_down) are joined first.
class Latch {
public:
    explicit Latch(std::int64_t count) : count_(count) {}

    void count_down(std::int64_t n = 1) noexcept { count_.fetch_sub(n, std::memory_order_acq_rel); }

    // false on timeout (the test fails instead of hanging).
    bool wait(std::chrono::milliseconds timeout = 60s) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (count_.load(std::memory_order_acquire) != 0) {
            if (std::chrono::steady_clock::now() > deadline) {
                return false;
            }
            std::this_thread::sleep_for(100us);
        }
        return true;
    }

    std::int64_t remaining() const noexcept { return count_.load(); }

private:
    std::atomic<std::int64_t> count_;
};

sched::SchedulerOptions options(std::size_t workers, std::size_t pollers = 1) {
    sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = pollers;
    o.blocking.num_threads = 1;
    o.stacks.stack_size = 64 * 1024;
    o.stacks.max_stacks = 8192;
    return o;
}

template <class Pred>
bool eventually(Pred pred, std::chrono::milliseconds timeout = 30s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(50us);
    }
    return true;
}

// A plain waiter for the core: counts its wakes, logs its id, and poisons
// its node's next inside the wake (as if the node disappeared with a
// resumed fiber): release() must have read next before calling it.
struct Waiter {
    SemaphoreNode sn;
    int id = 0;
    int wakes = 0;
    std::vector<int>* log = nullptr;

    explicit Waiter(int i = 0, std::vector<int>* l = nullptr) noexcept : id(i), log(l) {
        sn.node = WaitNode{nullptr, this, &Waiter::wake};
    }

    static void wake(void* self) noexcept {
        auto* w = static_cast<Waiter*>(self);
        ++w->wakes;
        if (w->log != nullptr) {
            w->log->push_back(w->id);
        }
        w->sn.node.next = reinterpret_cast<WaitNode*>(std::uintptr_t{0xdead0});
    }
};

// ---------------------------------------------------------------- SemaphoreCore

TEST(SemaphoreCore, TryAcquireAndRelease) {
    SemaphoreCore<> sem(3);
    EXPECT_EQ(sem.available_approx(), 3u);
    EXPECT_TRUE(sem.try_acquire(2));
    EXPECT_FALSE(sem.try_acquire(2));
    EXPECT_TRUE(sem.try_acquire());
    EXPECT_FALSE(sem.try_acquire());
    EXPECT_EQ(sem.available_approx(), 0u);
    sem.release(5);  // more than was ever there: just a higher count
    EXPECT_EQ(sem.available_approx(), 5u);
    EXPECT_TRUE(sem.try_acquire(5));
    EXPECT_FALSE(sem.has_waiters_approx());
}

TEST(SemaphoreCore, ZeroPermits) {
    SemaphoreCore<> sem(0);
    EXPECT_TRUE(sem.try_acquire(0));
    Waiter w;
    EXPECT_TRUE(sem.acquire_or_enqueue(w.sn, 0));
    sem.release(0);
    EXPECT_EQ(sem.available_approx(), 0u);
    // Also while somebody waits: zero never queues.
    Waiter blocked;
    EXPECT_FALSE(sem.acquire_or_enqueue(blocked.sn, 1));
    EXPECT_TRUE(sem.try_acquire(0));
    EXPECT_TRUE(sem.acquire_or_enqueue(w.sn, 0));
    sem.release(1);
    EXPECT_EQ(blocked.wakes, 1);
    EXPECT_EQ(w.wakes, 0);
}

TEST(SemaphoreCore, AcquireOrEnqueueTakesFreePermits) {
    SemaphoreCore<> sem(2);
    Waiter w;
    EXPECT_TRUE(sem.acquire_or_enqueue(w.sn, 2));
    EXPECT_EQ(sem.available_approx(), 0u);
    EXPECT_EQ(w.wakes, 0);
}

// release(n) hands permits to several waiters at once, in arrival order,
// and keeps the rest.
TEST(SemaphoreCore, ReleaseWakesSeveralInOrder) {
    constexpr int kWaiters = 8;
    SemaphoreCore<> sem(0);
    std::vector<int> log;
    std::vector<std::unique_ptr<Waiter>> waiters;
    for (int i = 0; i < kWaiters; ++i) {
        waiters.push_back(std::make_unique<Waiter>(i, &log));
        EXPECT_FALSE(sem.acquire_or_enqueue(waiters.back()->sn, 1));
    }
    EXPECT_TRUE(sem.has_waiters_approx());
    sem.release(3);
    EXPECT_EQ(log, (std::vector<int>{0, 1, 2}));
    EXPECT_TRUE(sem.has_waiters_approx());
    sem.release(kWaiters);  // 5 more waiters, 3 permits left over
    ASSERT_EQ(log.size(), static_cast<std::size_t>(kWaiters));
    for (int i = 0; i < kWaiters; ++i) {
        EXPECT_EQ(log[static_cast<std::size_t>(i)], i);
        EXPECT_EQ(waiters[static_cast<std::size_t>(i)]->wakes, 1);
    }
    EXPECT_FALSE(sem.has_waiters_approx());
    EXPECT_EQ(sem.available_approx(), 3u);
    EXPECT_TRUE(sem.try_acquire(3));
}

// A large request at the head holds smaller ones behind it; permits
// accumulate for it; nobody overtakes the queue, try_acquire() included.
TEST(SemaphoreCore, HeadOfLineMultiPermit) {
    SemaphoreCore<> sem(1);
    std::vector<int> log;
    Waiter big(1, &log);
    Waiter small(2, &log);
    Waiter later(3, &log);
    EXPECT_FALSE(sem.acquire_or_enqueue(big.sn, 3));  // 1 free, needs 3
    EXPECT_FALSE(sem.acquire_or_enqueue(small.sn, 1));  // 1 free, but queued behind big
    EXPECT_FALSE(sem.try_acquire(1));                   // no barging
    EXPECT_EQ(sem.available_approx(), 1u);
    sem.release(1);
    EXPECT_TRUE(log.empty());
    EXPECT_EQ(sem.available_approx(), 2u);
    EXPECT_FALSE(sem.acquire_or_enqueue(later.sn, 1));
    sem.release(1);  // big gets 3; small and later still wait (0 free)
    EXPECT_EQ(log, (std::vector<int>{1}));
    EXPECT_EQ(sem.available_approx(), 0u);
    sem.release(2);  // both
    EXPECT_EQ(log, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(sem.available_approx(), 0u);
    EXPECT_FALSE(sem.has_waiters_approx());
}

// Grants stop at the first waiter that does not fit, even if a later one
// would (FIFO, no skipping).
TEST(SemaphoreCore, NoSkippingPastTheHead) {
    SemaphoreCore<> sem(0);
    std::vector<int> log;
    Waiter a(1, &log);
    Waiter b(2, &log);
    Waiter c(3, &log);
    EXPECT_FALSE(sem.acquire_or_enqueue(a.sn, 1));
    EXPECT_FALSE(sem.acquire_or_enqueue(b.sn, 4));
    EXPECT_FALSE(sem.acquire_or_enqueue(c.sn, 1));
    sem.release(2);
    EXPECT_EQ(log, (std::vector<int>{1}));  // c fits but waits behind b
    EXPECT_EQ(sem.available_approx(), 1u);
    sem.release(4);
    EXPECT_EQ(log, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(sem.available_approx(), 0u);
}

// A node may be queued again after its grant (a fiber acquiring in a loop).
TEST(SemaphoreCore, NodeReuse) {
    SemaphoreCore<> sem(0);
    Waiter w;
    for (int i = 0; i < 100; ++i) {
        EXPECT_FALSE(sem.acquire_or_enqueue(w.sn, 2));
        sem.release(1);
        EXPECT_EQ(w.wakes, i);
        sem.release(1);
        EXPECT_EQ(w.wakes, i + 1);
    }
    EXPECT_EQ(sem.available_approx(), 0u);
}

TEST(SemaphoreCoreDeathTest, DestroyedWithWaiters) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            Waiter w;
            {
                SemaphoreCore<> sem(0);
                static_cast<void>(sem.acquire_or_enqueue(w.sn, 1));
            }
        },
        "semaphore that has waiters");
}

TEST(SemaphoreCoreDeathTest, Overflow) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            SemaphoreCore<> sem(SemaphoreCore<>::kMaxPermits);
            sem.release(1);
        },
        "overflow");
    EXPECT_DEATH(
        {
            SemaphoreCore<> sem(0);
            sem.release(SemaphoreCore<>::kMaxPermits + 1);
        },
        "too large");
}

// Plain threads on the core, plain nodes (the wake sets a flag the waiter
// thread spins on). Mutual exclusion of K slots, every hand-off seen.
TEST(SemaphoreCore, ThreadsStress) {
    constexpr int kThreads = 6;
    constexpr int kRounds = 3000;
    constexpr std::uint64_t kSlots = 2;
    struct ThreadNode {
        SemaphoreNode sn;
        std::atomic<int> wakes{0};
        ThreadNode() noexcept { sn.node = WaitNode{nullptr, this, &ThreadNode::wake}; }
        static void wake(void* self) noexcept {
            static_cast<ThreadNode*>(self)->wakes.fetch_add(1, std::memory_order_release);
        }
    };
    SemaphoreCore<> sem(kSlots);
    std::atomic<int> inside{0};
    std::atomic<int> violations{0};
    std::uint64_t counter = 0;  // touched only by holders of BOTH permits
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            ThreadNode node;
            for (int i = 0; i < kRounds; ++i) {
                const std::uint64_t n = (t + i) % 3 == 0 ? 2 : 1;
                const int before = node.wakes.load(std::memory_order_relaxed);
                if (!sem.acquire_or_enqueue(node.sn, n)) {
                    while (node.wakes.load(std::memory_order_acquire) == before) {
                        std::this_thread::yield();
                    }
                }
                if (inside.fetch_add(static_cast<int>(n)) + static_cast<int>(n) > static_cast<int>(kSlots)) {
                    violations.fetch_add(1);
                }
                if (n == 2) {
                    ++counter;
                }
                inside.fetch_sub(static_cast<int>(n));
                sem.release(n);
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(violations.load(), 0);
    EXPECT_EQ(sem.available_approx(), kSlots);
    EXPECT_FALSE(sem.has_waiters_approx());
    std::uint64_t expected = 0;
    for (int t = 0; t < kThreads; ++t) {
        for (int i = 0; i < kRounds; ++i) {
            expected += (t + i) % 3 == 0 ? 1 : 0;
        }
    }
    EXPECT_EQ(counter, expected);
}

// ------------------------------------------------------------ fiber::Semaphore

// Many fibers x workers on K slots: never more than K inside, everybody
// finishes, the count returns to K.
TEST(Semaphore, ContentionManyFibers) {
    constexpr int kFibers = 512;
    constexpr int kRounds = 200;
    constexpr int kSlots = 3;
    Latch done(kFibers);
    fiber::Semaphore sem(kSlots);
    std::atomic<int> inside{0};
    std::atomic<int> max_inside{0};
    std::atomic<int> violations{0};
    Scheduler scheduler(options(4));
    for (int f = 0; f < kFibers; ++f) {
        fiber::go(scheduler, [&, f]() noexcept {
            for (int i = 0; i < kRounds; ++i) {
                sem.acquire();
                const int now = inside.fetch_add(1) + 1;
                if (now > kSlots) {
                    violations.fetch_add(1);
                }
                int seen = max_inside.load(std::memory_order_relaxed);
                while (now > seen && !max_inside.compare_exchange_weak(seen, now)) {
                }
                if ((f + i) % 7 == 0) {
                    this_fiber::yield();  // hold the slot across a reschedule
                }
                inside.fetch_sub(1);
                sem.release();
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait()) << done.remaining() << " fibers stuck";
    EXPECT_EQ(violations.load(), 0);
    EXPECT_LE(max_inside.load(), kSlots);
    EXPECT_EQ(sem.available_approx(), static_cast<std::uint64_t>(kSlots));
    EXPECT_FALSE(sem.has_waiters_approx());
}

// Mixed 1..4-permit requests on a 4-permit semaphore: the sum held never
// exceeds 4 and nobody gets stuck (FIFO: a 4 cannot be starved by 1s).
TEST(Semaphore, MultiPermitMixed) {
    constexpr int kFibers = 128;
    constexpr int kRounds = 200;
    constexpr int kCapacity = 4;
    Latch done(kFibers);
    fiber::Semaphore sem(kCapacity);
    std::atomic<int> held{0};
    std::atomic<int> violations{0};
    Scheduler scheduler(options(4));
    for (int f = 0; f < kFibers; ++f) {
        fiber::go(scheduler, [&, f]() noexcept {
            for (int i = 0; i < kRounds; ++i) {
                const int n = (f * 7 + i) % kCapacity + 1;
                fiber::SemaphoreGuard guard(sem, static_cast<std::uint64_t>(n));
                if (held.fetch_add(n) + n > kCapacity) {
                    violations.fetch_add(1);
                }
                if (i % 3 == 0) {
                    this_fiber::yield();
                }
                held.fetch_sub(n);
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait()) << done.remaining() << " fibers stuck";
    EXPECT_EQ(violations.load(), 0);
    EXPECT_EQ(sem.available_approx(), static_cast<std::uint64_t>(kCapacity));
}

// A fiber asking for all permits while single-permit fibers churn through
// them nonstop: with barging it could wait forever; FIFO bounds it by the
// single-permit holders ahead of it.
TEST(Semaphore, MultiPermitNotStarvedBySingles) {
    constexpr int kSingles = 16;
    constexpr int kCapacity = 4;
    constexpr int kBigRounds = 200;
    Latch done(kSingles + 1);
    fiber::Semaphore sem(kCapacity);
    std::atomic<bool> big_done{false};
    std::atomic<int> big_rounds{0};
    std::atomic<std::uint64_t> single_rounds{0};
    Scheduler scheduler(options(4));
    for (int f = 0; f < kSingles; ++f) {
        fiber::go(scheduler, [&]() noexcept {
            // Bounded, so a starved big fiber fails the test instead of
            // spinning forever.
            for (int i = 0; i < 200'000 && !big_done.load(std::memory_order_relaxed); ++i) {
                sem.acquire(1);
                this_fiber::yield();
                sem.release(1);
                single_rounds.fetch_add(1, std::memory_order_relaxed);
            }
            done.count_down();
        });
    }
    fiber::go(scheduler, [&]() noexcept {
        for (int r = 0; r < kBigRounds; ++r) {
            sem.acquire(kCapacity);
            big_rounds.fetch_add(1);
            sem.release(kCapacity);
            this_fiber::yield();
        }
        big_done.store(true);
        done.count_down();
    });
    ASSERT_TRUE(done.wait()) << done.remaining() << " fibers stuck";
    EXPECT_TRUE(big_done.load());
    EXPECT_EQ(big_rounds.load(), kBigRounds);
    EXPECT_GT(single_rounds.load(), 0u);
}

// One release(n) from a plain thread wakes n parked fibers.
TEST(Semaphore, ReleaseNWakesSeveralFibers) {
    constexpr int kWaiters = 64;
    Latch done(kWaiters);
    fiber::Semaphore sem(0);
    int payload = 0;  // written before release(), read after acquire()
    std::atomic<int> bad{0};
    Scheduler scheduler(options(4));
    for (int i = 0; i < kWaiters; ++i) {
        fiber::go(scheduler, [&]() noexcept {
            sem.acquire();
            if (payload != 99) {
                bad.fetch_add(1);
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(eventually([&] { return sem.has_waiters_approx(); }));
    std::this_thread::sleep_for(20ms);  // let most of them park
    payload = 99;
    sem.release(kWaiters / 2);
    sem.release(kWaiters / 2);
    ASSERT_TRUE(done.wait()) << done.remaining() << " fibers stuck";
    EXPECT_EQ(bad.load(), 0);
    EXPECT_EQ(sem.available_approx(), 0u);
}

// A multi-permit acquire collects permits released one at a time by
// several fibers, and sees all of their writes.
TEST(Semaphore, MultiPermitCollectsReleases) {
    constexpr int kReleasers = 16;
    constexpr int kRounds = 200;
    Latch done(1);
    std::atomic<int> bad{0};
    Scheduler scheduler(options(4));
    fiber::go(scheduler, [&]() noexcept {
        for (int r = 0; r < kRounds; ++r) {
            fiber::Semaphore sem(0);  // on the waiter's stack: dies right after acquire
            int cells[kReleasers] = {};
            for (int i = 0; i < kReleasers; ++i) {
                fiber::go([&sem, &cells, i, r]() noexcept {
                    cells[i] = r + 1;
                    sem.release();
                });
            }
            sem.acquire(kReleasers);
            for (int c : cells) {
                if (c != r + 1) {
                    bad.fetch_add(1);
                }
            }
        }
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(bad.load(), 0);
}

// The latch pattern with the semaphore on the heap: freed by the waiter the
// moment acquire(N) returns while releasers may still be returning from
// release() (ASan: no release may touch it after the hand-off).
TEST(Semaphore, DestroyedRightAfterAcquireReturns) {
    constexpr int kReleasers = 8;
    constexpr int kRounds = 300;
    Latch done(1);
    Scheduler scheduler(options(4));
    fiber::go(scheduler, [&]() noexcept {
        for (int r = 0; r < kRounds; ++r) {
            auto sem = std::make_unique<fiber::Semaphore>(0);
            fiber::Semaphore* raw = sem.get();
            for (int i = 0; i < kReleasers; ++i) {
                fiber::go([raw]() noexcept { raw->release(); });
            }
            sem->acquire(kReleasers);
            sem.reset();
        }
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
}

TEST(Semaphore, TryAcquireFromFibersAndThreads) {
    constexpr int kFibers = 64;
    constexpr int kRounds = 500;
    Latch done(kFibers);
    fiber::Semaphore sem(2);
    std::atomic<int> inside{0};
    std::atomic<int> violations{0};
    std::atomic<std::uint64_t> taken{0};
    Scheduler scheduler(options(4));
    auto body = [&]() noexcept {
        for (int i = 0; i < kRounds; ++i) {
            if (sem.try_acquire()) {
                taken.fetch_add(1, std::memory_order_relaxed);
                if (inside.fetch_add(1) + 1 > 2) {
                    violations.fetch_add(1);
                }
                inside.fetch_sub(1);
                sem.release();
            }
        }
    };
    for (int f = 0; f < kFibers; ++f) {
        fiber::go(scheduler, [&]() noexcept {
            body();
            done.count_down();
        });
    }
    std::thread plain(body);  // try_acquire / release from a plain thread
    plain.join();
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(violations.load(), 0);
    EXPECT_GT(taken.load(), 0u);
    EXPECT_EQ(sem.available_approx(), 2u);
}

// acquire() outside a fiber blocks the thread (ThreadWaiter) until fibers
// release: a producer thread throttled by permits.
TEST(Semaphore, PlainThreadAcquireBlocksUntilFibersRelease) {
    constexpr int kItems = 2000;
    constexpr int kWindow = 8;
    Latch done(kItems);
    fiber::Semaphore window(kWindow);
    std::atomic<int> in_flight{0};
    std::atomic<int> violations{0};
    Scheduler scheduler(options(2));
    for (int i = 0; i < kItems; ++i) {
        window.acquire();  // the main thread: blocks while kWindow items are in flight
        if (in_flight.fetch_add(1) + 1 > kWindow) {
            violations.fetch_add(1);
        }
        fiber::go(scheduler, [&]() noexcept {
            this_fiber::yield();
            in_flight.fetch_sub(1);
            window.release();
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(violations.load(), 0);
    window.acquire(kWindow);  // all back
    EXPECT_EQ(window.available_approx(), 0u);
}

TEST(Semaphore, GuardScopesMovesAndModes) {
    fiber::Semaphore sem(5);
    {
        fiber::SemaphoreGuard g(sem, 2);
        EXPECT_TRUE(g.owns());
        EXPECT_EQ(g.permits(), 2u);
        EXPECT_EQ(sem.available_approx(), 3u);
        fiber::SemaphoreGuard moved(std::move(g));
        EXPECT_FALSE(g.owns());  // NOLINT(bugprone-use-after-move): moved-from is empty
        EXPECT_EQ(g.permits(), 0u);
        EXPECT_EQ(sem.available_approx(), 3u);
        fiber::SemaphoreGuard assigned;
        EXPECT_FALSE(assigned);
        assigned = std::move(moved);
        EXPECT_TRUE(assigned);
        EXPECT_EQ(sem.available_approx(), 3u);
    }
    EXPECT_EQ(sem.available_approx(), 5u);
    {
        fiber::SemaphoreGuard t(sem, 6, std::try_to_lock);
        EXPECT_FALSE(t);
        EXPECT_EQ(sem.available_approx(), 5u);
        fiber::SemaphoreGuard ok(sem, 5, std::try_to_lock);
        EXPECT_TRUE(ok);
        EXPECT_EQ(sem.available_approx(), 0u);
        ok.reset();
        EXPECT_EQ(sem.available_approx(), 5u);
        ok.reset();  // no-op
        EXPECT_EQ(sem.available_approx(), 5u);
    }
    {
        ASSERT_TRUE(sem.try_acquire(3));
        fiber::SemaphoreGuard adopted(sem, 3, std::adopt_lock);
        EXPECT_EQ(sem.available_approx(), 2u);
        fiber::SemaphoreGuard other(sem, 1);
        other = std::move(adopted);  // releases other's 1, takes over 3
        EXPECT_EQ(sem.available_approx(), 2u);
        EXPECT_EQ(other.disown(), 3u);
        EXPECT_FALSE(other);
    }
    EXPECT_EQ(sem.available_approx(), 2u);  // the disowned 3 are still out
    sem.release(3);
    EXPECT_EQ(sem.available_approx(), 5u);
}

// A guard taken in one fiber and released in another (permits are not
// owned by a fiber).
TEST(Semaphore, GuardMovedBetweenFibers) {
    Latch done(1);
    fiber::Semaphore sem(1);
    std::atomic<int> order{0};
    Scheduler scheduler(options(2));
    fiber::go(scheduler, [&]() noexcept {
        fiber::SemaphoreGuard g(sem);
        fiber::Semaphore handed(0);
        fiber::go([&handed, &order, g = std::move(g)]() mutable noexcept {
            order.store(1);
            g.reset();  // releases the permit taken by the parent
            handed.release();
        });
        sem.acquire();  // waits for the child's release
        handed.acquire();
        if (order.load() == 1) {
            order.store(2);
        }
        sem.release();
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(order.load(), 2);
    EXPECT_EQ(sem.available_approx(), 1u);
}

}  // namespace
