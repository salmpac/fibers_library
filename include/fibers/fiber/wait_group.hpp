#pragma once

// WaitGroup for stackful fibers: the stackful front-end of
// wait::WaitGroupCore (see wait_group_core.hpp for the algorithm, the rules
// and the memory orders).
//
//   fiber::WaitGroup wg;
//   for (...) {
//       wg.go(scheduler, [&]() noexcept { ... });   // add(1) + go + done()
//   }
//   wg.wait();                                       // in a fiber: parks
//                                                    // (a plain thread: blocks)
//
//   // or by hand, e.g. in-flight requests of a server:
//   wg.add();  fiber::go(...{ handle(request); wg.done(); });
//   ... on shutdown: stop accepting, then wg.wait() (requests may still
//   spawn children: add() before their own done())
//
// Contexts (fibers/fiber/context.hpp): wg.go(f) from a fiber inherits its
// context, wg.go(scheduler, f) runs the child under none. wait() is a join
// and is NOT interrupted by a cancelled context (docs/cancellation.md): the
// children reference the waiter's frame, and under the same context they
// are cancelled too and return.
//
// A one-shot wait: the count may cross zero any number of times before the
// first wait(); wait() closes the group, which finishes when the count next
// reaches zero (or at once if it is zero). After that add() fails fast.
// Everything sequenced before every done() happens-before every return from
// wait(). Any number of fibers may wait.
//
// wait() parks the fiber, not the worker thread: a WaitNode on the fiber's
// own stack goes into the group's event, then the fiber parks; a wake that
// arrives before the fiber has left its stack is absorbed by its state word
// (NOTIFIED). Outside a fiber of a scheduler of type Sched it blocks the
// calling thread on an atomic (wait::ThreadWaiter, one wake: the group
// finishes once): fine for a plain thread (main joining the fibers it
// started, a foreign event loop's thread), never from a plain callback on a
// worker (it would block the worker, and maybe the very fibers that would
// call done()). try_wait() is the non-blocking form for any thread. add()
// and done() work from anywhere; the last done() wakes the waiters (one
// submit per fiber, one notify per thread, from the thread that calls it).

#include <cstdint>
#include <optional>
#include <type_traits>
#include <utility>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/sync/sync.hpp"
#include "fibers/wait/thread_waiter.hpp"
#include "fibers/wait/wait_group_core.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fibers::fiber {

template <FiberScheduler Sched = sched::Scheduler>
class BasicWaitGroup {
public:
    BasicWaitGroup() noexcept = default;
    BasicWaitGroup(const BasicWaitGroup&) = delete;
    BasicWaitGroup& operator=(const BasicWaitGroup&) = delete;

    // n > 0: count += n; n == 0: no-op; n < 0: fails fast. Fails fast after
    // the group has finished. Any thread.
    void add(std::int64_t n = 1) noexcept { core_.add(n); }

    // count -= 1 (fails fast below zero). Any thread.
    void done() noexcept { core_.done(); }

    // Closes the group; true if it has finished (count zero). Any thread,
    // never waits.
    bool try_wait() noexcept { return core_.try_wait(); }

    // Closes the group and waits until it finishes: parks the calling fiber,
    // or blocks the calling thread outside a fiber (see the top comment).
    void wait() noexcept {
        BasicFiber<Sched>* self = BasicFiber<Sched>::current();
        if (self == nullptr) [[unlikely]] {
            wait_thread();
            return;
        }
        wait::WaitNode node = self->wait_node();
        if (!core_.wait_or_enqueue(node)) {
            self->park();  // exactly one wake per publication: the group finishes once
        }
    }

    // add(1), then a new fiber on `scheduler` that runs f(), destroys f and
    // then calls done(): f's captures are gone before wait() can return.
    // Nothing is allocated (f lives next to the fiber, as with fiber::go). A
    // fiber dropped without running (scheduler stopped) never calls done().
    template <class F>
        requires std::is_nothrow_invocable_v<std::decay_t<F>&> &&
                 std::is_nothrow_constructible_v<std::decay_t<F>, F&&> &&
                 std::is_nothrow_move_constructible_v<std::decay_t<F>>
    void go(Sched& scheduler, F&& f) noexcept {
        add(1);
        fiber::go(scheduler, member(std::forward<F>(f)));
    }

    // The same, the new fiber running under `context` (fibers/fiber/
    // context.hpp), which must outlive it.
    template <class F>
        requires std::is_nothrow_invocable_v<std::decay_t<F>&> &&
                 std::is_nothrow_constructible_v<std::decay_t<F>, F&&> &&
                 std::is_nothrow_move_constructible_v<std::decay_t<F>>
    void go(Sched& scheduler, BasicContext<Sched>& context, F&& f) noexcept {
        add(1);
        BasicFiber<Sched>::spawn(scheduler, &context, member(std::forward<F>(f)));
    }

    // From inside a fiber: the same on the current fiber's scheduler, under
    // the current fiber's context (if any), like fiber::go(f).
    template <class F>
        requires std::is_nothrow_invocable_v<std::decay_t<F>&> &&
                 std::is_nothrow_constructible_v<std::decay_t<F>, F&&> &&
                 std::is_nothrow_move_constructible_v<std::decay_t<F>>
    void go(F&& f) noexcept {
        BasicFiber<Sched>& self = BasicFiber<Sched>::current_or_die("fiber: WaitGroup::go(f) outside a fiber");
        add(1);
        BasicFiber<Sched>::spawn(self.scheduler(), self.context(), member(std::forward<F>(f)));
    }

    // Racy snapshot (tests, diagnostics).
    std::uint64_t count_approx() const noexcept { return core_.count_approx(); }

private:
    // Not a fiber: block the thread (a node on its stack, one wake).
    [[gnu::noinline]] void wait_thread() noexcept {
        using Sync = typename Sched::SyncPolicy;
        if constexpr (sync::ParkingSyncPolicy<Sync>) {
            wait::ThreadWaiter<Sync> waiter;
            if (!core_.wait_or_enqueue(waiter.node)) {
                waiter.wait();
            }
        } else {
            if (!core_.try_wait()) {
                fail::fail_fast("fiber: WaitGroup::wait() outside a fiber needs a parking Sync policy");
            }
        }
    }

    // The fiber's closure: f in an optional, reset before done().
    template <class F>
    auto member(F&& f) noexcept {
        using Fn = std::decay_t<F>;
        return [group = this, fn = std::optional<Fn>(std::in_place, std::forward<F>(f))]() mutable noexcept {
            (*fn)();
            fn.reset();
            group->done();
        };
    }

    wait::WaitGroupCore<typename Sched::SyncPolicy> core_;
};

using WaitGroup = BasicWaitGroup<sched::Scheduler>;

}  // namespace fibers::fiber
