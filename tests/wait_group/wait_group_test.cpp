// EventCore / WaitGroupCore over plain WaitNodes, and their stackful
// front-ends fiber::Event / fiber::WaitGroup (StdSync).
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
#include <thread>
#include <vector>

#include "fibers/fiber/event.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/wait_group.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/sync/spin_lock.hpp"
#include "fibers/wait/event_core.hpp"
#include "fibers/wait/wait_group_core.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fiber = fibers::fiber;
namespace sched = fibers::sched;
namespace this_fiber = fibers::fiber::this_fiber;
using fibers::fiber::Fiber;
using fibers::sched::Scheduler;
using fibers::wait::EventCore;
using fibers::wait::WaitGroupCore;
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
    bool wait(std::chrono::milliseconds timeout = 30s) {
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

sched::SchedulerOptions options(std::size_t workers, std::size_t pollers = 2) {
    sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = pollers;
    o.blocking.num_threads = 1;
    o.stacks.stack_size = 64 * 1024;
    o.stacks.max_stacks = 8192;
    return o;
}

// A plain waiter for the cores: counts its wakes, and poisons its node's
// next inside the wake (as if the node disappeared with a resumed fiber):
// set() must have read next before calling it.
struct Waiter {
    WaitNode node;
    int id = 0;
    int wakes = 0;
    std::vector<int>* log = nullptr;

    Waiter() noexcept { node = WaitNode{nullptr, this, &Waiter::wake}; }

    static void wake(void* self) noexcept {
        auto* w = static_cast<Waiter*>(self);
        ++w->wakes;
        if (w->log != nullptr) {
            w->log->push_back(w->id);
        }
        w->node.next = reinterpret_cast<WaitNode*>(std::uintptr_t{0xdead0});
    }
};

// ------------------------------------------------------------------ EventCore

TEST(EventCore, SetWakesEveryWaiterExactlyOnce) {
    constexpr int kWaiters = 16;
    EventCore<> event;
    std::vector<int> log;
    Waiter waiters[kWaiters];
    EXPECT_FALSE(event.is_set());
    EXPECT_FALSE(event.has_waiters_approx());
    for (int i = 0; i < kWaiters; ++i) {
        waiters[i].id = i;
        waiters[i].log = &log;
        EXPECT_FALSE(event.wait_or_enqueue(waiters[i].node));
    }
    EXPECT_TRUE(event.has_waiters_approx());
    EXPECT_EQ(log.size(), 0u);
    EXPECT_TRUE(event.set());
    EXPECT_TRUE(event.is_set());
    EXPECT_FALSE(event.has_waiters_approx());
    ASSERT_EQ(log.size(), static_cast<std::size_t>(kWaiters));
    std::vector<bool> seen(kWaiters, false);
    for (int id : log) {
        EXPECT_FALSE(seen[static_cast<std::size_t>(id)]);
        seen[static_cast<std::size_t>(id)] = true;
    }
    for (const Waiter& w : waiters) {
        EXPECT_EQ(w.wakes, 1);
    }
}

TEST(EventCore, LateWaiterReturnsAtOnceAndSetIsIdempotent) {
    EventCore<> event;
    Waiter early;
    Waiter late;
    EXPECT_FALSE(event.wait_or_enqueue(early.node));
    EXPECT_TRUE(event.set());
    EXPECT_FALSE(event.set());  // already set: no-op, nobody woken again
    EXPECT_TRUE(event.wait_or_enqueue(late.node));
    EXPECT_EQ(early.wakes, 1);
    EXPECT_EQ(late.wakes, 0);
    EXPECT_TRUE(event.is_set());
}

TEST(EventCore, SetWithoutWaiters) {
    EventCore<> event;
    EXPECT_TRUE(event.set());
    EXPECT_TRUE(event.is_set());
    Waiter w;
    EXPECT_TRUE(event.wait_or_enqueue(w.node));
    EXPECT_EQ(w.wakes, 0);
}

TEST(EventCoreDeathTest, DestroyedWithWaiters) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            Waiter w;
            {
                EventCore<> event;
                static_cast<void>(event.wait_or_enqueue(w.node));
            }
        },
        "event that has waiters");
}

// -------------------------------------------------------------- WaitGroupCore

TEST(WaitGroupCore, WaitOnZeroReturnsAtOnce) {
    WaitGroupCore<> wg;
    Waiter w;
    EXPECT_TRUE(wg.wait_or_enqueue(w.node));
    EXPECT_TRUE(wg.wait_or_enqueue(w.node));  // again: still finished
    EXPECT_TRUE(wg.try_wait());
    EXPECT_EQ(w.wakes, 0);
}

TEST(WaitGroupCore, CountCrossesZeroBeforeWait) {
    WaitGroupCore<> wg;
    for (int round = 0; round < 100; ++round) {
        wg.add(3);
        wg.done();
        wg.add();
        wg.done();
        wg.done();
        wg.done();  // zero again, nobody waits: nothing finishes
        EXPECT_EQ(wg.count_approx(), 0u);
        EXPECT_FALSE(wg.joining_approx());
    }
    wg.add(2);
    Waiter a;
    Waiter b;
    EXPECT_FALSE(wg.wait_or_enqueue(a.node));
    EXPECT_FALSE(wg.wait_or_enqueue(b.node));
    EXPECT_FALSE(wg.try_wait());
    wg.add(1);  // while JOINING, covered by a unit: allowed
    wg.done();
    wg.done();
    EXPECT_EQ(a.wakes, 0);
    EXPECT_EQ(b.wakes, 0);
    wg.done();  // the last one: finishes, wakes both
    EXPECT_EQ(a.wakes, 1);
    EXPECT_EQ(b.wakes, 1);
    Waiter late;
    EXPECT_TRUE(wg.wait_or_enqueue(late.node));
    EXPECT_EQ(late.wakes, 0);
    wg.add(0);  // no-op, also after the group finished
}

TEST(WaitGroupCore, LastDoneBeforeWait) {
    WaitGroupCore<> wg;
    wg.add(2);
    wg.done();
    wg.done();
    Waiter w;
    EXPECT_TRUE(wg.wait_or_enqueue(w.node));  // never set the event, never needed it
    EXPECT_EQ(w.wakes, 0);
}

TEST(WaitGroupCoreDeathTest, AddAfterFinished) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            WaitGroupCore<> wg;
            wg.add(1);
            Waiter w;
            static_cast<void>(wg.wait_or_enqueue(w.node));
            wg.done();
            wg.add(1);
        },
        "after the group finished");
    EXPECT_DEATH(
        {
            WaitGroupCore<> wg;
            static_cast<void>(wg.try_wait());
            wg.add(1);
        },
        "after the group finished");
}

TEST(WaitGroupCoreDeathTest, Underflow) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            WaitGroupCore<> wg;
            wg.add(1);
            wg.done();
            wg.done();
        },
        "without a matching add");
}

TEST(WaitGroupCoreDeathTest, NegativeAdd) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            WaitGroupCore<> wg;
            wg.add(2);
            wg.add(-1);
        },
        "negative");
}

TEST(WaitGroupCoreDeathTest, Overflow) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            WaitGroupCore<> wg;
            wg.add(1);
            wg.add(static_cast<std::int64_t>(WaitGroupCore<>::kMaxCount));
        },
        "overflow");
}

TEST(WaitGroupCoreDeathTest, DestroyedWithWaiters) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            Waiter w;
            {
                WaitGroupCore<> wg;
                wg.add(1);
                static_cast<void>(wg.wait_or_enqueue(w.node));
            }
        },
        "event that has waiters");
}

// Concurrent threads on the cores, plain nodes: the wake sets a flag the
// waiter thread spins on. Every waiter returns, every data write is seen.
TEST(WaitGroupCore, ThreadsStress) {
    constexpr int kRounds = 300;
    constexpr int kWorkers = 4;
    constexpr int kWaiters = 3;
    struct ThreadWaiter {
        WaitNode node;
        std::atomic<int> wakes{0};
        ThreadWaiter() noexcept { node = WaitNode{nullptr, this, &ThreadWaiter::wake}; }
        static void wake(void* self) noexcept {
            static_cast<ThreadWaiter*>(self)->wakes.fetch_add(1, std::memory_order_release);
        }
    };
    for (int round = 0; round < kRounds; ++round) {
        WaitGroupCore<> wg;
        int data[kWorkers] = {};
        ThreadWaiter waiters[kWaiters];
        std::atomic<int> bad{0};
        wg.add(kWorkers);
        std::vector<std::thread> threads;
        for (int i = 0; i < kWorkers; ++i) {
            threads.emplace_back([&, i] {
                data[i] = round + 1;
                wg.done();
            });
        }
        for (int i = 0; i < kWaiters; ++i) {
            threads.emplace_back([&, i] {
                if (!wg.wait_or_enqueue(waiters[i].node)) {
                    while (waiters[i].wakes.load(std::memory_order_acquire) == 0) {
                        std::this_thread::yield();
                    }
                }
                for (int k = 0; k < kWorkers; ++k) {
                    if (data[k] != round + 1) {
                        bad.fetch_add(1);
                    }
                }
            });
        }
        for (auto& t : threads) {
            t.join();
        }
        EXPECT_EQ(bad.load(), 0);
        for (const auto& w : waiters) {
            EXPECT_LE(w.wakes.load(), 1);
        }
    }
}

// ---------------------------------------------------------------- fiber::Event

TEST(Event, ManyWaitersSetFromThread) {
    constexpr int kWaiters = 1000;
    constexpr int kLate = 100;
    Latch done(kWaiters + kLate);
    fiber::Event event;
    int payload = 0;  // written before set(), read after wait()
    std::atomic<int> bad{0};
    std::atomic<int> started{0};
    Scheduler scheduler(options(4));
    for (int i = 0; i < kWaiters; ++i) {
        fiber::go(scheduler, [&]() noexcept {
            started.fetch_add(1, std::memory_order_relaxed);
            event.wait();
            if (payload != 42) {
                bad.fetch_add(1);
            }
            done.count_down();
        });
    }
    while (started.load() < kWaiters / 2) {
        std::this_thread::yield();
    }
    payload = 42;
    EXPECT_TRUE(event.set());
    EXPECT_FALSE(event.set());
    for (int i = 0; i < kLate; ++i) {
        fiber::go(scheduler, [&]() noexcept {
            event.wait();  // already set: returns at once
            if (payload != 42) {
                bad.fetch_add(1);
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait()) << done.remaining() << " fibers stuck";
    EXPECT_EQ(bad.load(), 0);
    event.wait();  // set: fine outside a fiber
}

TEST(Event, SetFromPollerTimer) {
    constexpr int kWaiters = 64;
    struct Ctx {
        fiber::Event event;
        int payload = 0;
    };
    Ctx ctx;
    Latch done(kWaiters);
    std::atomic<int> bad{0};
    // Declared before the scheduler: the pollers are gone before the timer.
    Scheduler::Timer timer(&ctx, [](void* memo, fibers::poller::TimerEvent, fibers::poller::TaskBatch&) noexcept {
        auto* c = static_cast<Ctx*>(memo);
        c->payload = 7;
        c->event.set();  // wakes the fibers with pool submits from the poller thread
    });
    Scheduler scheduler(options(4));
    for (int i = 0; i < kWaiters; ++i) {
        fiber::go(scheduler, [&]() noexcept {
            ctx.event.wait();
            if (ctx.payload != 7) {
                bad.fetch_add(1);
            }
            done.count_down();
        });
    }
    timer.deadline = scheduler.now() + 5ms;
    scheduler.pollers().schedule(timer);
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(bad.load(), 0);
    EXPECT_TRUE(ctx.event.is_set());
}

// Ping-pong of one-shot events between fibers: each round a fresh pair, on
// the waiter's stack (set() lets go of an event before the waiter returns).
TEST(Event, FreshEventPerRoundOnWaiterStack) {
    constexpr int kRounds = 2000;
    Latch done(1);
    std::atomic<int> bad{0};
    Scheduler scheduler(options(4));
    fiber::go(scheduler, [&]() noexcept {
        for (int i = 0; i < kRounds; ++i) {
            fiber::Event reply;  // dies right after wait() returns
            int value = 0;
            fiber::go([&reply, &value, i]() noexcept {
                value = i;
                reply.set();
            });
            reply.wait();
            if (value != i) {
                bad.fetch_add(1);
            }
        }
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(bad.load(), 0);
}

// wait() outside a fiber blocks the thread (ThreadWaiter) until a fiber
// sets the event; several plain threads and fibers wait at once. Each round
// a fresh event on the main thread's stack, destroyed right after the
// waits return (set() lets go of it before waking anybody).
TEST(Event, PlainThreadsWaitForFiber) {
    constexpr int kRounds = 200;
    constexpr int kThreads = 3;
    constexpr int kFibers = 4;
    std::atomic<int> bad{0};
    Scheduler scheduler(options(2));
    for (int round = 0; round < kRounds; ++round) {
        fiber::Event event;
        int payload = 0;  // written before set(), read after wait()
        Latch fibers_done(kFibers);
        for (int f = 0; f < kFibers; ++f) {
            fiber::go(scheduler, [&]() noexcept {
                event.wait();
                if (payload != round + 1) {
                    bad.fetch_add(1);
                }
                fibers_done.count_down();
            });
        }
        std::vector<std::thread> threads;
        for (int t = 0; t < kThreads; ++t) {
            threads.emplace_back([&] {
                event.wait();  // not a fiber: blocks this thread
                if (payload != round + 1) {
                    bad.fetch_add(1);
                }
            });
        }
        fiber::go(scheduler, [&]() noexcept {
            if (round % 2 == 0) {
                this_fiber::yield();  // let some waiters enqueue first
            }
            payload = round + 1;
            event.set();
        });
        event.wait();  // the main thread too
        if (payload != round + 1) {
            bad.fetch_add(1);
        }
        for (auto& t : threads) {
            t.join();
        }
        ASSERT_TRUE(fibers_done.wait()) << fibers_done.remaining() << " fibers stuck";
    }
    EXPECT_EQ(bad.load(), 0);
}

// ------------------------------------------------------------ fiber::WaitGroup

TEST(WaitGroup, FanOutFanInManyWorkers) {
    // kRounds roots run at once: up to kRounds * kChildren stacks in use.
    constexpr int kRounds = 8;
    constexpr int kChildren = 500;
    Latch done(kRounds);
    std::atomic<int> bad{0};
    Scheduler scheduler(options(4));
    for (int round = 0; round < kRounds; ++round) {
        fiber::go(scheduler, [&]() noexcept {
            auto slots = std::make_unique<int[]>(kChildren);  // plain: ordered by done -> wait
            fiber::WaitGroup wg;
            for (int i = 0; i < kChildren; ++i) {
                wg.go([&slots, i]() noexcept {
                    if (i % 3 == 0) {
                        this_fiber::yield();
                    }
                    slots[static_cast<std::size_t>(i)] = i + 1;
                });
            }
            wg.wait();
            for (int i = 0; i < kChildren; ++i) {
                if (slots[static_cast<std::size_t>(i)] != i + 1) {
                    bad.fetch_add(1);
                }
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait(60s));
    EXPECT_EQ(bad.load(), 0);
}

// wg.go destroys the user's closure BEFORE done(): its destructor's writes
// happen-before wait() returns.
TEST(WaitGroup, GoDestroysClosureBeforeDone) {
    constexpr int kChildren = 500;
    struct Marker {
        bool* destroyed;
        explicit Marker(bool* d) noexcept : destroyed(d) {}
        Marker(Marker&& other) noexcept : destroyed(other.destroyed) { other.destroyed = nullptr; }
        ~Marker() {
            if (destroyed != nullptr) {
                *destroyed = true;
            }
        }
    };
    Latch done(1);
    std::atomic<int> missing{0};
    auto destroyed = std::make_unique<bool[]>(kChildren);
    Scheduler scheduler(options(4));
    fiber::go(scheduler, [&]() noexcept {
        fiber::WaitGroup wg;
        for (int i = 0; i < kChildren; ++i) {
            wg.go(scheduler, [marker = Marker(&destroyed[static_cast<std::size_t>(i)])]() noexcept {
                static_cast<void>(marker);
            });
        }
        wg.wait();
        for (int i = 0; i < kChildren; ++i) {
            if (!destroyed[static_cast<std::size_t>(i)]) {
                missing.fetch_add(1);
            }
        }
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(missing.load(), 0);
}

TEST(WaitGroup, ManyWaitersAndThreadPolling) {
    constexpr int kWaiters = 50;
    constexpr int kWorkers = 500;
    Latch done(kWaiters);
    fiber::WaitGroup wg;
    std::atomic<int> finished{0};
    std::atomic<int> bad{0};
    Scheduler scheduler(options(4));
    wg.add(kWorkers);  // before anyone can wait
    for (int i = 0; i < kWaiters; ++i) {
        fiber::go(scheduler, [&]() noexcept {
            wg.wait();
            if (finished.load(std::memory_order_relaxed) != kWorkers) {
                bad.fetch_add(1);
            }
            wg.wait();  // again: returns at once
            done.count_down();
        });
    }
    for (int i = 0; i < kWorkers; ++i) {
        fiber::go(scheduler, [&]() noexcept {
            this_fiber::sleep_for(std::chrono::microseconds(100));
            finished.fetch_add(1, std::memory_order_relaxed);
            wg.done();
        });
    }
    while (!wg.try_wait()) {  // a non-fiber thread joins by polling
        std::this_thread::sleep_for(100us);
    }
    EXPECT_EQ(finished.load(std::memory_order_relaxed), kWorkers);
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(bad.load(), 0);
    wg.wait();  // finished: fine outside a fiber
}

// The count crosses zero many times while nobody waits; the group finishes
// only after wait() and the next zero.
TEST(WaitGroup, CountCrossesZeroBeforeWait) {
    Latch done(1);
    fiber::WaitGroup wg;
    std::atomic<bool> released{false};
    std::atomic<bool> early{false};
    Scheduler scheduler(options(2));
    for (int i = 0; i < 1000; ++i) {
        wg.add(2);
        wg.done();
        wg.done();
    }
    wg.add(1);
    fiber::go(scheduler, [&]() noexcept {
        wg.wait();
        if (!released.load()) {
            early.store(true);
        }
        done.count_down();
    });
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(done.remaining(), 1);
    released.store(true);
    wg.done();
    ASSERT_TRUE(done.wait());
    EXPECT_FALSE(early.load());
}

// The graceful-shutdown pattern: an acceptor fiber starts requests
// continuously (add / done); requests sleep, some spawn a child (add before
// their own done). Shutdown: stop the acceptor, then wait while requests are
// in flight; they keep spawning children during the join. Every request and
// child must be finished when wait() returns.
TEST(WaitGroup, GracefulShutdown) {
    constexpr int kRounds = 10;
    int in_flight_at_wait = 0;
    int children_during_join = 0;
    for (int round = 0; round < kRounds; ++round) {
        Latch done(1);
        std::atomic<int> started{0};
        std::atomic<int> finished{0};
        std::atomic<bool> stop{false};
        std::atomic<bool> joining{false};
        std::atomic<int> spawned_during_join{0};
        std::atomic<int> unfinished_at_return{-1};
        std::atomic<int> count_at_wait{0};
        fiber::WaitGroup requests;
        fiber::Event acceptor_stopped;
        Scheduler scheduler(options(4));
        auto request = [&](int id) {
            return [&, id]() noexcept {
                this_fiber::sleep_for(std::chrono::microseconds(200 + 50 * (id % 7)));
                if (id % 3 == 0) {
                    started.fetch_add(1);
                    if (joining.load()) {
                        spawned_during_join.fetch_add(1);
                    }
                    requests.go([&]() noexcept {  // a child: add(1) before our own done()
                        this_fiber::sleep_for(std::chrono::microseconds(300));
                        finished.fetch_add(1);
                    });
                }
                finished.fetch_add(1);
            };
        };
        fiber::go(scheduler, [&]() noexcept {  // acceptor
            for (int id = 0; !stop.load(); ++id) {
                started.fetch_add(1);
                requests.add();
                fiber::go([&requests, &request, id]() noexcept {
                    request(id)();
                    requests.done();
                });
                if (id % 8 == 0) {
                    this_fiber::sleep_for(std::chrono::microseconds(50));
                } else {
                    this_fiber::yield();
                }
            }
            acceptor_stopped.set();
        });
        fiber::go(scheduler, [&]() noexcept {  // shutdown
            this_fiber::sleep_for(10ms);
            stop.store(true);
            acceptor_stopped.wait();
            count_at_wait.store(static_cast<int>(requests.count_approx()));
            joining.store(true);
            requests.wait();
            unfinished_at_return.store(started.load() - finished.load());
            done.count_down();
        });
        ASSERT_TRUE(done.wait()) << "round " << round;
        EXPECT_EQ(unfinished_at_return.load(), 0) << "round " << round;
        EXPECT_EQ(started.load(), finished.load());
        in_flight_at_wait += count_at_wait.load() > 0 ? 1 : 0;
        children_during_join += spawned_during_join.load();
    }
    EXPECT_GT(in_flight_at_wait, 0);
    EXPECT_GT(children_during_join, 0);
}

// The NOTIFIED path, deterministically, with exactly what the front-ends do
// (a fiber's wait_node, the core's enqueue, park): the waiter enqueues and
// stays on its stack until the waker's done() / set() has returned, so the
// wake finds the fiber RUNNING and only sets NOTIFIED; park() returns at once.
TEST(WaitGroup, WaiterWokenWhileStillSwitchingOut) {
    constexpr int kRounds = 100;
    struct Round {
        WaitGroupCore<> wg;
        EventCore<> event;
        std::atomic<bool> enqueued{false};
        std::atomic<bool> woken{false};
    };
    Latch done(1);
    std::atomic<Round*> current{nullptr};
    std::atomic<Fiber*> waiter{nullptr};
    std::atomic<int> resumed{0};
    int notified_seen = 0;
    Scheduler scheduler(options(2));
    auto rounds = std::make_unique<Round[]>(kRounds);
    for (int i = 0; i < kRounds; ++i) {
        rounds[static_cast<std::size_t>(i)].wg.add(1);
    }
    fiber::go(scheduler, [&]() noexcept {
        Fiber& self = *Fiber::current();
        waiter.store(&self);
        for (int i = 0; i < kRounds; ++i) {
            Round& r = rounds[static_cast<std::size_t>(i)];
            WaitNode node = self.wait_node();
            const bool finished = i % 2 == 0 ? r.wg.wait_or_enqueue(node) : r.event.wait_or_enqueue(node);
            if (!finished) {
                current.store(&r, std::memory_order_release);
                while (!r.woken.load(std::memory_order_acquire)) {
                    fibers::sync::cpu_relax();
                }
                self.park();  // returns at once: the wake already happened
                resumed.fetch_add(1);
            }
        }
        done.count_down();
    });
    for (int i = 0; i < kRounds; ++i) {
        Round* r = nullptr;
        while ((r = current.exchange(nullptr, std::memory_order_acquire)) == nullptr) {
            std::this_thread::yield();
        }
        if (i % 2 == 0) {
            r->wg.done();
        } else {
            r->event.set();
        }
        if (waiter.load()->state_approx() == (Fiber::kRunning | Fiber::kNotified)) {
            ++notified_seen;
        }
        r->woken.store(true, std::memory_order_release);
    }
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(resumed.load(), kRounds);
    EXPECT_EQ(notified_seen, kRounds);
}

// wait() outside a fiber blocks the thread (ThreadWaiter): main fans out
// with wg.go(scheduler, f) and joins, no root fiber. Each round a fresh
// group on main's stack, destroyed right after wait() returns (the last
// done() lets go of it before the wake), plus a second plain thread and a
// fiber waiting on the same group.
TEST(WaitGroup, PlainThreadWaits) {
    constexpr int kRounds = 200;
    constexpr int kChildren = 16;
    std::atomic<int> bad{0};
    Scheduler scheduler(options(4));
    for (int round = 0; round < kRounds; ++round) {
        int slots[kChildren] = {};  // plain: ordered by done() -> wait()
        Latch fiber_waiter(1);
        fiber::WaitGroup wg;
        for (int i = 0; i < kChildren; ++i) {
            wg.go(scheduler, [&slots, i, round]() noexcept {
                if (i % 4 == 0) {
                    this_fiber::yield();
                }
                slots[i] = round + i;
            });
        }
        auto check = [&] {
            for (int i = 0; i < kChildren; ++i) {
                if (slots[i] != round + i) {
                    bad.fetch_add(1);
                }
            }
        };
        fiber::go(scheduler, [&]() noexcept {
            wg.wait();
            check();
            fiber_waiter.count_down();
        });
        std::thread other([&] {
            wg.wait();
            check();
        });
        wg.wait();  // not a fiber: blocks main
        check();
        other.join();
        ASSERT_TRUE(fiber_waiter.wait());
    }
    EXPECT_EQ(bad.load(), 0);
}

TEST(WaitGroupDeathTest, AddAfterFinishedInFibers) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            std::atomic<bool> waited{false};
            fiber::WaitGroup wg;
            Scheduler scheduler(options(2));
            fiber::go(scheduler, [&]() noexcept {
                wg.go([]() noexcept {});
                wg.wait();
                waited.store(true);
            });
            while (!waited.load()) {
                std::this_thread::yield();
            }
            wg.add();
        },
        "after the group finished");
}

}  // namespace
