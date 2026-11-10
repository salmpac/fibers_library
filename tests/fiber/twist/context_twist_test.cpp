// wait::ContextCore (the claim / delist protocol of request contexts) and
// fiber::BasicContext's parent -> child propagation under Twist, on plain
// threads. The network waits that use it (PollDesc, the deadline timer)
// are checked in tests/net/twist/net_twist_test.cpp.
//
// TWIST_SIM:    Random and PCT schedulers of the deterministic simulator.
// TWIST_FAULTY: real threads with fault injection (+ TSan in
//               clang-twist-faulty-tsan).
//
// What must be handed over lives in twist::assist::Shared cells, whose
// accesses Twist checks for happens-before:
//   canceller -> whoever observes cancelled() (a payload cell);
//   a hook's accesses of the waiter's frame -> the waiter reusing that
//   frame after delist() (the `done` handshake);
//   the waiter's frame (published before enlist) -> the hook.

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
#include "fibers/fiber/context.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/stack/stack.hpp"
#include "fibers/sync/twist_sync.hpp"
#include "fibers/thread_pool/thread_pool.hpp"
#include "fibers/wait/context_core.hpp"

namespace fiber = fibers::fiber;
namespace tp = fibers::thread_pool;
using fibers::sync::TwistSync;
using fibers::wait::Error;

namespace {

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

using Core = fibers::wait::ContextCore<TwistSync>;
using Interrupt = fibers::wait::Interrupt<TwistSync>;

// A single-waiter wait, like a PollDesc direction: the word holds the
// waiter while it waits; the event and the context's hook both try to take
// it out (CAS &waiter -> 0), the winner wakes it. `frame` stands for the
// waiter's stack frame: written before the wait is published, read by
// whoever takes the waiter (and by the hook even when it loses, as a real
// hook reads the waiter's word), written again by the waiter after it has
// left the wait (the frame is reused).
struct Wait {
    twist::ed::std::atomic<std::uintptr_t> word{0};
    twist::assist::Shared<int> frame{0};
    twist::ed::std::atomic<int> wakes{0};
    Error reason = Error::kBroken;  // written by the hook that won, before the wake
    bool interrupted = false;

    bool publish() {
        frame.Write(1);
        std::uintptr_t empty = 0;
        // release: the frame to the event side (it acquires).
        return word.compare_exchange_strong(empty, mine(), std::memory_order_release, std::memory_order_relaxed);
    }

    std::uintptr_t mine() const { return reinterpret_cast<std::uintptr_t>(this); }

    void wake() { TWIST_ASSERT_M(wakes.fetch_add(1, std::memory_order_release) == 0, "woken twice"); }

    // The event (another thread).
    bool event() {
        std::uintptr_t cur = mine();
        if (!word.compare_exchange_strong(cur, 0, std::memory_order_acquire, std::memory_order_relaxed)) {
            return false;
        }
        TWIST_ASSERT_M(frame.Read() == 1, "waiter's frame visible to the event");
        wake();
        return true;
    }

    // The context's hook (the canceller's thread, or the waiter itself).
    static void hook(void* self, Error reason) noexcept {
        auto* w = static_cast<Wait*>(self);
        TWIST_ASSERT_M(w->frame.Read() >= 1, "waiter's frame visible to the hook");
        std::uintptr_t cur = w->mine();
        // relaxed, as PollDesc's claim: the enlist's unlock -> the
        // canceller's lock orders the publish before it.
        if (w->word.compare_exchange_strong(cur, 0, std::memory_order_relaxed, std::memory_order_relaxed)) {
            w->reason = reason;
            w->interrupted = true;
            w->wake();
        }
    }

    // After the wake (acquire: the waker's writes).
    void await() {
        FIBERS_WAIT_BOUNDED(wakes.load(std::memory_order_acquire) != 0, "waiter never woken (lost wakeup)");
    }
};

// The waiter publishes and enlists (or interrupts itself if the context has
// ended), the event fires on one thread, the context is cancelled on
// another. Exactly one wake; the waiter's frame is reused only after every
// access of the hook (delist waits for `done` when the canceller took the
// node); the outcome is consistent.
TEST(ContextTwist, ClaimRacesEventAndCancel) {
    check([] {
        Core ctx;
        Wait w;
        twist::ed::std::atomic<bool> published{false};
        twist::ed::std::thread waiter([&] {
            TWIST_ASSERT_M(w.publish(), "publish");
            published.store(true, std::memory_order_release);
            Interrupt node(&w, &Wait::hook);
            const bool enlisted = ctx.enlist(node);
            if (!enlisted) {
                Wait::hook(&w, *ctx.reason());
            }
            w.await();
            if (enlisted) {
                ctx.delist(node);
            }
            w.frame.Write(2);  // the frame is reused: no hook may still touch it
            if (w.interrupted) {
                TWIST_ASSERT_M(w.reason == Error::kCancelled, "the context's reason");
            }
        });
        twist::ed::std::thread event([&] {
            FIBERS_WAIT_BOUNDED(published.load(std::memory_order_acquire), "never published");
            static_cast<void>(w.event());
        });
        twist::ed::std::thread canceller([&] { TWIST_ASSERT_M(ctx.cancel(Error::kCancelled), "first cancel"); });
        waiter.join();
        event.join();
        canceller.join();
        TWIST_ASSERT_M(w.wakes.load() == 1, "exactly one wake");
        TWIST_ASSERT_M(!ctx.has_dependants_approx(), "nothing left enlisted");
    });
}

// Two waiters under one context and two cancels racing (only one walks):
// both are interrupted (no event here), each exactly once.
TEST(ContextTwist, TwoWaitersTwoCancels) {
    check([] {
        Core ctx;
        Wait w[2];
        twist::ed::std::atomic<int> first{0};
        auto waiter = [&](int i) {
            return [&, i] {
                TWIST_ASSERT_M(w[i].publish(), "publish");
                Interrupt node(&w[i], &Wait::hook);
                const bool enlisted = ctx.enlist(node);
                if (!enlisted) {
                    Wait::hook(&w[i], *ctx.reason());
                }
                w[i].await();
                if (enlisted) {
                    ctx.delist(node);
                }
                w[i].frame.Write(2);
                TWIST_ASSERT_M(w[i].interrupted, "interrupted");
            };
        };
        twist::ed::std::thread waiters[2] = {twist::ed::std::thread(waiter(0)), twist::ed::std::thread(waiter(1))};
        twist::ed::std::thread cancellers[2] = {
            twist::ed::std::thread([&] { first.fetch_add(ctx.cancel(Error::kCancelled) ? 1 : 0); }),
            twist::ed::std::thread([&] { first.fetch_add(ctx.cancel(Error::kTimedOut) ? 1 : 0); }),
        };
        for (auto& t : waiters) {
            t.join();
        }
        for (auto& t : cancellers) {
            t.join();
        }
        TWIST_ASSERT_M(first.load() == 1, "one cancel wins");
        TWIST_ASSERT_M(w[0].reason == *ctx.reason() && w[1].reason == *ctx.reason(), "the winning reason");
    });
}

// What the canceller did before cancel() is visible to whoever observes
// cancelled() / reason() (no lock on the reader's side).
TEST(ContextTwist, CancelPublishes) {
    check([] {
        Core ctx;
        twist::assist::Shared<int> payload{0};
        twist::ed::std::thread canceller([&] {
            payload.Write(1);
            ctx.cancel(Error::kCancelled);
        });
        twist::ed::std::thread observer([&] {
            if (ctx.cancelled()) {
                TWIST_ASSERT_M(payload.Read() == 1, "canceller's write visible after cancelled()");
            }
            if (ctx.reason().has_value()) {
                TWIST_ASSERT_M(payload.Read() == 1, "canceller's write visible after reason()");
            }
        });
        canceller.join();
        observer.join();
    });
}

// ---- fiber::BasicContext: parent -> child -------------------------------------------

// What BasicContext needs without deadlines: nothing is ever scheduled.
class MiniScheduler {
public:
    using SyncPolicy = TwistSync;
    struct Pool {
        void submit(fibers::thread_pool::Task) noexcept { fibers::fail::fail_fast("test: no fibers here"); }
    };
    struct Stacks {
        fibers::stack::Stack* acquire() noexcept { fibers::fail::fail_fast("test: no fibers here"); }
        void release(fibers::stack::Stack*) noexcept {}
    };
    Pool& pool() noexcept { return pool_; }
    Stacks& stacks() noexcept { return stacks_; }

private:
    Pool pool_;
    Stacks stacks_;
};

static_assert(fiber::FiberScheduler<MiniScheduler>);
static_assert(!fiber::DeadlineScheduler<MiniScheduler>);

using Context = fiber::BasicContext<MiniScheduler>;

// A child is created and destroyed on one thread while the parent is
// cancelled on another: the child either starts ended (the parent's enlist
// refused it) or is ended by the parent's walk -- never missed -- and its
// destruction waits for a walk that is cancelling it. A grandchild rides
// along.
TEST(ContextTwist, ParentCancelRacesChildLifetime) {
    check([] {
        Context root = Context::cancellable();
        twist::ed::std::atomic<bool> child_saw_end{false};
        twist::ed::std::atomic<bool> created{false};
        twist::ed::std::atomic<bool> cancelled_root{false};
        twist::ed::std::thread owner([&] {
            Context child = Context::child_of(root);
            Context grandchild = Context::child_of(child);
            created.store(true, std::memory_order_release);
            FIBERS_WAIT_BOUNDED(cancelled_root.load(std::memory_order_acquire), "root never cancelled");
            // root.cancel() has returned: its walk is over.
            child_saw_end.store(child.cancelled() && grandchild.cancelled());
            TWIST_ASSERT_M(grandchild.reason() == Error::kCancelled, "the root's reason");
        });
        twist::ed::std::thread canceller([&] {
            TWIST_ASSERT_M(root.cancel(), "cancel");
            cancelled_root.store(true, std::memory_order_release);
        });
        owner.join();
        canceller.join();
        TWIST_ASSERT_M(child_saw_end.load(), "the child and grandchild ended with the root");
        TWIST_ASSERT_M(!root.has_dependants_approx(), "children delisted");
    });
}

// The child goes away while the parent's walk may be inside its hook
// (cancelling it): ~BasicContext's delist must wait for that.
TEST(ContextTwist, ChildDestroyedDuringParentWalk) {
    check([] {
        Context root = Context::cancellable();
        twist::ed::std::thread owner([&] {
            Context child = Context::child_of(root);
            Context grandchild = Context::child_of(child);
            twist::ed::std::this_thread::yield();
        });  // destroyed while root.cancel() may be walking
        twist::ed::std::thread canceller([&] { root.cancel(); });
        owner.join();
        canceller.join();
        TWIST_ASSERT_M(!root.has_dependants_approx(), "children delisted");
    });
}

}  // namespace
