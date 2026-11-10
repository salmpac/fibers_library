// fiber::BasicContext on a real Scheduler: states, propagation to fibers
// and child contexts, deadlines (the per-context poller timer), the
// destructor's timer retirement and the fail-fast lifetime rules. The
// network waits it interrupts are tested in tests/net/context_test.cpp.
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

// Cancel first: the pending timer is taken back by the destructor (from a
// plain thread: it blocks until the poller has withdrawn it).
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

// The same from fibers (the destructor parks), and racing with the timer:
// deadlines right around the destruction.
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
