// fiber::BasicContext on a real Scheduler: states, propagation to fibers
// and child contexts, deadlines (the per-context poller timer), the
// destructor and the deadline record (cancelled lazily, handed back by the
// poller), and the fail-fast lifetime rules. The network waits it
// interrupts are tested in tests/net/context_test.cpp.
//
// No timing assertions: deadlines are generous and only outcomes / order
// are checked. Fibers record into atomics; the test thread checks them.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <new>
#include <thread>
#include <vector>

#include "fibers/fiber/context.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/wait_group.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace this_fiber = fibers::fiber::this_fiber;
using fibers::Context;
using fibers::sched::Scheduler;
using fibers::wait::Error;
using namespace std::chrono_literals;

namespace {

class Latch {
public:
    explicit Latch(std::int64_t count) : count_(count) {}
    void count_down() noexcept { count_.fetch_sub(1, std::memory_order_acq_rel); }
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

private:
    std::atomic<std::int64_t> count_;
};

template <class Pred>
bool eventually(Pred pred, std::chrono::milliseconds timeout = 30s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(100us);
    }
    return true;
}

fibers::sched::SchedulerOptions options(std::size_t workers) {
    fibers::sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = 2;
    o.blocking.num_threads = 1;
    o.stacks.max_stacks = 4096;
    return o;
}

template <class F>
bool in_fiber(Scheduler& scheduler, F f) {
    Latch done(1);
    fiber::go(scheduler, [&]() noexcept {
        f();
        done.count_down();
    });
    return done.wait();
}

// Every timer handed to the scheduler's pollers so far has been handed
// back (its callback ran: kFired or kCancelled), once the pollers have
// been through an iteration: each one gets a posted no-op timer, which
// wakes it; its iteration drains the cancelled nodes from its queue and
// compacts its heap if they dominate it. `scheduled` = the timers handed
// over since the pollers' counters were `before`.
using fibers::poller::PollerStats;

std::uint64_t handed_back(const PollerStats& now, const PollerStats& before) {
    return (now.timers_fired - before.timers_fired) + (now.timers_cancelled - before.timers_cancelled);
}

bool all_timers_handed_back(Scheduler& scheduler, const PollerStats& before, std::uint64_t scheduled) {
    auto& pollers = scheduler.pollers();
    const std::size_t n = pollers.size();
    std::vector<Scheduler::Timer> nudges(n);
    std::atomic<std::size_t> ran{0};
    for (std::size_t i = 0; i < n; ++i) {
        nudges[i].memo = &ran;
        nudges[i].callback = [](void* memo, fibers::poller::TimerEvent, fibers::poller::TaskBatch&) noexcept {
            static_cast<std::atomic<std::size_t>*>(memo)->fetch_add(1);
        };
        pollers.at(i).post(nudges[i]);
    }
    const bool nudged = eventually([&] { return ran.load() == n; });
    // The nudges themselves count as fired.
    return nudged && eventually([&] { return handed_back(pollers.stats(), before) == scheduled + n; });
}

// Storage for a context whose destruction the test controls (death tests).
struct ContextSlot {
    alignas(Context) unsigned char bytes[sizeof(Context)];
    Context& get() noexcept { return *std::launder(reinterpret_cast<Context*>(bytes)); }
    void destroy() noexcept { get().~Context(); }
};

// ---- states ----------------------------------------------------------------------

TEST(Context, CancelOnce) {
    Context ctx = Context::cancellable();
    EXPECT_FALSE(ctx.cancelled());
    EXPECT_FALSE(ctx.reason().has_value());
    EXPECT_EQ(ctx.error_code(), 0);
    EXPECT_FALSE(ctx.has_deadline());
    EXPECT_TRUE(ctx.cancel());
    EXPECT_FALSE(ctx.cancel());
    EXPECT_TRUE(ctx.cancelled());
    EXPECT_EQ(ctx.reason(), Error::kCancelled);
    EXPECT_EQ(ctx.error_code(), ECANCELED);
}

TEST(Context, DeadlineInThePastEndsAtOnce) {
    Scheduler scheduler(options(1));
    Context ctx = Context::with_deadline(scheduler, scheduler.now() - 1ms);
    EXPECT_TRUE(ctx.cancelled());
    EXPECT_EQ(ctx.reason(), Error::kTimedOut);
    EXPECT_EQ(ctx.error_code(), ETIMEDOUT);
    EXPECT_FALSE(ctx.cancel());  // the first reason stays
    EXPECT_EQ(ctx.reason(), Error::kTimedOut);
}

TEST(Context, DeadlineFires) {
    Scheduler scheduler(options(2));
    Context ctx = Context::with_timeout(scheduler, 20ms);
    EXPECT_TRUE(ctx.has_deadline());
    ASSERT_TRUE(eventually([&] { return ctx.cancelled(); }));
    EXPECT_EQ(ctx.reason(), Error::kTimedOut);
    EXPECT_FALSE(ctx.cancel());
}

// Cancel first: the destructor cancels the pending timer and leaves its
// record to the poller (from a plain thread; nothing to wait for).
TEST(Context, CancelBeforeDeadlineThenDestroy) {
    Scheduler scheduler(options(2));
    for (int i = 0; i < 100; ++i) {
        Context ctx = Context::with_timeout(scheduler, 1h);
        EXPECT_TRUE(ctx.cancel());
        EXPECT_EQ(ctx.reason(), Error::kCancelled);
    }
    for (int i = 0; i < 100; ++i) {
        Context ctx = Context::with_timeout(scheduler, 1h);  // never cancelled
    }
}

// The same from fibers, and racing with the timer: deadlines right around
// the destruction (the destructor waits only for a firing callback).
TEST(Context, DestroyRacesWithTimerInFibers) {
    Scheduler scheduler(options(4));
    constexpr int kFibers = 64;
    Latch done(kFibers);
    std::atomic<int> timed_out{0};
    for (int f = 0; f < kFibers; ++f) {
        fiber::go(scheduler, [&, f]() noexcept {
            for (int i = 0; i < 50; ++i) {
                Context ctx = Context::with_timeout(std::chrono::microseconds((f * 7 + i * 13) % 200));
                this_fiber::yield();
                if (ctx.cancelled()) {
                    timed_out.fetch_add(1);
                }
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait());
    EXPECT_GE(timed_out.load(), 0);  // any mix is fine; nothing may crash or hang
}

// ---- the deadline record ---------------------------------------------------------

using Record = fibers::fiber::detail::DeadlineRecord<Scheduler>;
constexpr std::size_t kRecordBlock = Record::block_size();
static_assert(kRecordBlock == 128, "a deadline record is one 128-byte TaskMemoryPool block");

// Many contexts with live (far) deadlines, created and destroyed quickly
// in fibers on several workers and on plain threads: the destructors do
// not wait for anything; every record comes back (each timer is handed
// back by its poller exactly once: kCancelled) and the records are reused
// (the pool does not grow round after round).
TEST(Context, DestroyManyWithLiveDeadlines) {
    Scheduler scheduler(options(4));
    constexpr int kFibers = 16;
    constexpr int kThreads = 2;
    constexpr int kPerTask = 500;
    constexpr int kRounds = 4;
    constexpr std::uint64_t kPerRound = static_cast<std::uint64_t>(kFibers + kThreads) * kPerTask;
    std::size_t capacity_after_first = 0;
    for (int round = 0; round < kRounds; ++round) {
        const PollerStats before = scheduler.pollers().stats();
        Latch done(kFibers);
        std::atomic<int> errors{0};
        auto churn = [&]() noexcept {
            for (int i = 0; i < kPerTask; ++i) {
                Context ctx = Context::with_timeout(scheduler, 1h);
                if (ctx.cancelled() || !ctx.has_deadline()) {
                    errors.fetch_add(1);
                }
                if (i % 3 == 0) {
                    static_cast<void>(ctx.cancel());
                }
            }
        };
        for (int f = 0; f < kFibers; ++f) {
            fiber::go(scheduler, [&]() noexcept {
                churn();
                done.count_down();
            });
        }
        std::vector<std::thread> threads;
        for (int t = 0; t < kThreads; ++t) {
            threads.emplace_back(churn);
        }
        for (auto& t : threads) {
            t.join();
        }
        ASSERT_TRUE(done.wait());
        EXPECT_EQ(errors.load(), 0);
        ASSERT_TRUE(all_timers_handed_back(scheduler, before, kPerRound)) << "round " << round;
        const PollerStats after = scheduler.pollers().stats();
        EXPECT_EQ(after.timers_cancelled - before.timers_cancelled, kPerRound);
        const std::size_t capacity = scheduler.task_memory().capacity<kRecordBlock>();
        if (round == 0) {
            capacity_after_first = capacity;
        } else {
            // A leak would add kPerRound blocks per round; reuse keeps it
            // flat (up to blocks parked in other threads' caches).
            EXPECT_LT(capacity, capacity_after_first + kPerRound / 2) << "round " << round;
        }
    }
}

// Destruction racing the deadline firing: deadlines from 0 to ~50 us,
// contexts destroyed right away or after a yield, by fibers on four
// workers (two pollers fire the timers). Either the destructor's cancel()
// wins or it waits for the callback's walk; nothing may crash, hang or
// leak (the pool stops growing).
TEST(Context, DestroyRacesDeadlineFiring) {
    Scheduler scheduler(options(4));
    constexpr int kFibers = 32;
    constexpr int kPerFiber = 400;
    constexpr int kRounds = 3;
    std::size_t capacity_after_first = 0;
    std::atomic<int> fired{0};
    for (int round = 0; round < kRounds; ++round) {
        Latch done(kFibers);
        for (int f = 0; f < kFibers; ++f) {
            fiber::go(scheduler, [&, f]() noexcept {
                for (int i = 0; i < kPerFiber; ++i) {
                    Context ctx = Context::with_timeout(std::chrono::microseconds((f * 7 + i * 13) % 50));
                    Context child = Context::child_of(ctx, scheduler.now() + std::chrono::microseconds(i % 5));
                    if (i % 2 == 0) {
                        this_fiber::yield();
                    }
                    if (child.cancelled()) {
                        fired.fetch_add(1);
                    }
                }
                done.count_down();
            });
        }
        ASSERT_TRUE(done.wait());
        const std::size_t capacity = scheduler.task_memory().capacity<kRecordBlock>();
        if (round == 0) {
            capacity_after_first = capacity;
        } else {
            EXPECT_LT(capacity, capacity_after_first + kFibers * kPerFiber) << "round " << round;
        }
    }
    EXPECT_GE(fired.load(), 0);  // any mix
}

// Parents and children with live deadlines of their own (a child's own
// timer only when earlier than its parent's), destroyed in various orders
// (children always before their parent), on fibers and threads.
TEST(Context, ParentsAndChildrenWithLiveDeadlinesInAnyOrder) {
    Scheduler scheduler(options(2));
    const PollerStats before = scheduler.pollers().stats();
    std::uint64_t timers = 0;
    // orders[k]: destruction order of {c1, g1, c2, g2} (g1 / g2 children of
    // c1 / c2); a child always before its parent.
    const int orders[][4] = {{1, 0, 3, 2}, {3, 2, 1, 0}, {1, 3, 0, 2}, {3, 1, 2, 0}};
    for (const auto& order : orders) {
        for (int rep = 0; rep < 50; ++rep) {
            Context root = Context::with_timeout(scheduler, 2h);  // a timer
            ContextSlot slots[4];
            Context* c[4];
            c[0] = ::new (slots[0].bytes) Context(Context::child_of(root, scheduler.now() + 1h));  // own timer
            c[1] = ::new (slots[1].bytes) Context(Context::child_of(*c[0], scheduler.now() + 30min));  // own
            c[2] = ::new (slots[2].bytes) Context(Context::child_of(root, scheduler.now() + 3h));  // none (later)
            c[3] = ::new (slots[3].bytes) Context(Context::child_of(*c[2], scheduler.now() + 1h));  // own
            timers += 4;  // root, c1, g1, g2
            EXPECT_LT(c[1]->deadline(), c[0]->deadline());
            EXPECT_EQ(c[2]->deadline(), root.deadline());
            EXPECT_LT(c[3]->deadline(), c[2]->deadline());
            if (rep % 2 == 0) {
                EXPECT_TRUE(c[2]->cancel());  // reaches g2, not c1
                EXPECT_EQ(c[3]->reason(), Error::kCancelled);
                EXPECT_FALSE(c[0]->cancelled());
            }
            for (int k : order) {
                slots[k].destroy();
            }
        }
    }
    // Every one of those (far) timers comes back cancelled.
    ASSERT_TRUE(all_timers_handed_back(scheduler, before, timers));
    EXPECT_EQ(scheduler.pollers().stats().timers_cancelled - before.timers_cancelled, timers);
    // The same shape in fibers, with deadlines that fire while children
    // are being destroyed.
    std::atomic<int> errors{0};
    Latch done(8);
    for (int f = 0; f < 8; ++f) {
        fiber::go(scheduler, [&, f]() noexcept {
            for (int i = 0; i < 100; ++i) {
                Context root = Context::with_timeout(std::chrono::microseconds(30 + f));
                Context c1 = Context::child_of(root, scheduler.now() + std::chrono::microseconds(i % 20));
                Context g1 = Context::child_of(c1);
                Context c2 = Context::child_of(root, scheduler.now() + 1h);
                this_fiber::yield();
                if (g1.cancelled() && g1.reason() != Error::kTimedOut) {
                    errors.fetch_add(1);
                }
            }  // ~c2, ~g1, ~c1, ~root
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(errors.load(), 0);
}

// A context may now be destroyed on a poller thread (a timer callback),
// with its own deadline still pending: the destructor does not wait for
// the poller.
TEST(Context, DestroyFromPollerCallback) {
    Scheduler scheduler(options(1));
    for (int i = 0; i < 100; ++i) {
        struct Job {
            ContextSlot slot;
            Scheduler::Timer timer;
            std::atomic<bool> done{false};
        } job;
        ::new (job.slot.bytes) Context(Context::with_timeout(scheduler, 1h));
        job.timer.memo = &job;
        job.timer.callback = [](void* memo, fibers::poller::TimerEvent, fibers::poller::TaskBatch&) noexcept {
            auto* j = static_cast<Job*>(memo);
            j->slot.destroy();
            j->done.store(true);
        };
        scheduler.pollers().at(0).post(job.timer);
        ASSERT_TRUE(eventually([&] { return job.done.load(); }));
    }
}

// Contexts may outlive the scheduler's stop() (not its destruction): their
// pending timers are cancelled on stopped pollers, and ~Poller hands the
// nodes back (kCancelled) while the TaskMemoryPool is still there.
TEST(Context, DestroyAfterSchedulerStop) {
    for (int round = 0; round < 10; ++round) {
        Scheduler scheduler(options(2));
        Context far = Context::with_timeout(scheduler, 1h);
        Context child = Context::child_of(far, scheduler.now() + 30min);
        Context near = Context::with_timeout(scheduler, std::chrono::microseconds(round * 10));
        Latch done(1);
        fiber::go(scheduler, [&]() noexcept {
            Context inner = Context::with_timeout(1h);
            done.count_down();
        });
        ASSERT_TRUE(done.wait());
        scheduler.stop();
        EXPECT_FALSE(far.cancelled());
        EXPECT_FALSE(child.cancelled());
    }  // ~near, ~child, ~far after the stop; then ~Scheduler
}

// ---- propagation to fibers ---------------------------------------------------------

TEST(Context, FibersInheritThroughGo) {
    Scheduler scheduler(options(2));
    Context ctx = Context::cancellable();
    Latch done(5);
    std::atomic<Context*> root{nullptr};
    std::atomic<Context*> child{nullptr};
    std::atomic<Context*> grandchild{nullptr};
    std::atomic<Context*> wg_child{nullptr};
    std::atomic<Context*> explicit_sched{reinterpret_cast<Context*>(1)};
    std::atomic<Context*> plain{reinterpret_cast<Context*>(1)};
    fiber::go(scheduler, ctx, [&]() noexcept {
        root.store(this_fiber::context());
        fiber::go([&]() noexcept {
            child.store(this_fiber::context());
            fiber::go([&]() noexcept {
                grandchild.store(this_fiber::context());
                done.count_down();
            });
            done.count_down();
        });
        fiber::go(scheduler, [&]() noexcept {  // explicit scheduler: no context
            explicit_sched.store(this_fiber::context());
            done.count_down();
        });
        fiber::WaitGroup wg;
        wg.go([&]() noexcept { wg_child.store(this_fiber::context()); });
        wg.wait();
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        plain.store(this_fiber::context());
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(root.load(), &ctx);
    EXPECT_EQ(child.load(), &ctx);
    EXPECT_EQ(grandchild.load(), &ctx);
    EXPECT_EQ(wg_child.load(), &ctx);
    EXPECT_EQ(explicit_sched.load(), nullptr);
    EXPECT_EQ(plain.load(), nullptr);
    EXPECT_EQ(this_fiber::context(), nullptr);  // not a fiber
    EXPECT_FALSE(this_fiber::cancelled());
}

TEST(Context, ContextScopeSwitchesAndRestores) {
    Scheduler scheduler(options(1));
    Context ctx = Context::cancellable();
    std::atomic<int> ok{0};
    Latch done(1);
    fiber::go(scheduler, ctx, [&]() noexcept {
        Context sub = Context::child_of(ctx);
        {
            fiber::ContextScope scope(sub);
            ok.fetch_add(this_fiber::context() == &sub);
            fiber::ContextScope shield(nullptr);
            ok.fetch_add(this_fiber::context() == nullptr);
            this_fiber::yield();  // survives migration (state is in the fiber)
            ok.fetch_add(this_fiber::context() == nullptr);
        }
        ok.fetch_add(this_fiber::context() == &ctx);
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(ok.load(), 4);
}

// The cooperative check: CPU loops in several fibers (yielding, so they
// migrate between workers) stop once the context is cancelled.
TEST(Context, CooperativeCheckStopsLoops) {
    Scheduler scheduler(options(4));
    Context ctx = Context::cancellable();
    constexpr int kFibers = 16;
    Latch done(kFibers);
    std::atomic<int> running{0};
    std::atomic<int> errors{0};
    for (int f = 0; f < kFibers; ++f) {
        fiber::go(scheduler, ctx, [&]() noexcept {
            running.fetch_add(1);
            while (!this_fiber::cancelled()) {
                this_fiber::yield();
            }
            if (this_fiber::context_error() != ECANCELED) {
                errors.fetch_add(1);
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(eventually([&] { return running.load() == kFibers; }));
    EXPECT_TRUE(ctx.cancel());
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(errors.load(), 0);
}

// ---- children ----------------------------------------------------------------------

TEST(Context, ParentCancelReachesDescendants) {
    Context root = Context::cancellable();
    Context child = Context::child_of(root);
    Context grandchild = Context::child_of(child);
    Context sibling = Context::child_of(root);
    EXPECT_TRUE(sibling.cancel());  // a child's own cancel stays local
    EXPECT_FALSE(root.cancelled());
    EXPECT_FALSE(child.cancelled());
    EXPECT_TRUE(root.cancel());
    EXPECT_EQ(child.reason(), Error::kCancelled);
    EXPECT_EQ(grandchild.reason(), Error::kCancelled);
    EXPECT_EQ(sibling.reason(), Error::kCancelled);
    // A child of an ended parent starts ended.
    Context late = Context::child_of(child);
    EXPECT_EQ(late.reason(), Error::kCancelled);
}

TEST(Context, ParentDeadlineReachesChildrenAsTimeout) {
    Scheduler scheduler(options(2));
    Context root = Context::with_timeout(scheduler, 20ms);
    Context child = Context::child_of(root, scheduler.now() + 1h);  // later: no timer of its own
    Context grandchild = Context::child_of(child);
    EXPECT_EQ(child.deadline(), root.deadline());
    EXPECT_EQ(grandchild.deadline(), root.deadline());
    ASSERT_TRUE(eventually([&] { return grandchild.cancelled(); }));
    EXPECT_EQ(root.reason(), Error::kTimedOut);
    EXPECT_EQ(child.reason(), Error::kTimedOut);
    EXPECT_EQ(grandchild.reason(), Error::kTimedOut);
}

// Nested deadlines: the inner (earlier) one fires first and ends only its
// subtree; the outer one is still live.
TEST(Context, NestedDeadlines) {
    Scheduler scheduler(options(2));
    Context outer = Context::with_timeout(scheduler, 1h);
    Context inner = Context::child_of(outer, scheduler.now() + 20ms);
    Context leaf = Context::child_of(inner);
    EXPECT_LT(inner.deadline(), outer.deadline());
    EXPECT_EQ(leaf.deadline(), inner.deadline());
    ASSERT_TRUE(eventually([&] { return leaf.cancelled(); }));
    EXPECT_EQ(inner.reason(), Error::kTimedOut);
    EXPECT_EQ(leaf.reason(), Error::kTimedOut);
    EXPECT_FALSE(outer.cancelled());
    EXPECT_TRUE(outer.cancel());
}

// Children created and destroyed (with deadlines of their own) in fibers
// while the parent is cancelled from another thread: the destructor's
// delist must wait for a parent's canceller that is cancelling it.
TEST(Context, ChildrenComeAndGoWhileParentIsCancelled) {
    Scheduler scheduler(options(4));
    for (int round = 0; round < 20; ++round) {
        Context root = Context::cancellable();
        constexpr int kFibers = 16;
        Latch done(kFibers);
        std::atomic<int> started{0};
        for (int f = 0; f < kFibers; ++f) {
            fiber::go(scheduler, root, [&, f]() noexcept {
                started.fetch_add(1);
                for (int i = 0; i < 200 && !this_fiber::cancelled(); ++i) {
                    Context child = Context::child_of(*this_fiber::context(),
                                                      scheduler.now() + std::chrono::microseconds(50 + f));
                    Context grandchild = Context::child_of(child);
                    this_fiber::yield();
                }
                done.count_down();
            });
        }
        ASSERT_TRUE(eventually([&] { return started.load() == kFibers; }));
        EXPECT_TRUE(root.cancel());
        ASSERT_TRUE(done.wait());
        EXPECT_FALSE(root.has_dependants_approx());
    }
}

// ---- fail fast -----------------------------------------------------------------------

TEST(ContextDeathTest, DestroyParentWithLiveChild) {
    EXPECT_DEATH(
        {
            ContextSlot parent;
            ContextSlot child;
            ::new (parent.bytes) Context(Context::cancellable());
            ::new (child.bytes) Context(Context::child_of(parent.get()));
            parent.destroy();
        },
        "child context");
}

TEST(ContextDeathTest, DeadlineWithoutScheduler) {
    EXPECT_DEATH(
        {
            Context root = Context::cancellable();
            Context child = Context::child_of(root, std::chrono::steady_clock::now() + 1h);
        },
        "needs a scheduler");
}

}  // namespace
