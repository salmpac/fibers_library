#pragma once

// Request contexts: cooperative cancellation and deadlines for a tree of
// fibers (Go's context.Context, without allocation and without exceptions).
// The full design, with the list of what is and is not cancellable, is in
// docs/cancellation.md.
//
//   fibers::Context ctx = fibers::Context::with_deadline(sched, sched.now() + 200ms);
//   fiber::go(sched, ctx, [&]() noexcept {      // runs under ctx
//       fiber::go([&]() noexcept { ... });      // inherits ctx (so does wg.go(f))
//       IoResult r = conn.read_some(buf, n);    // ETIMEDOUT / ECANCELED when ctx ends
//       if (fiber::this_fiber::cancelled()) ... // cooperative check
//   });
//   ctx.cancel();                               // from anywhere
//
// What a context does: it ends once, with a reason (cancel(): kCancelled;
// its deadline: kTimedOut; its parent ending: the parent's reason), and at
// that moment interrupts every cancellable wait parked under it (network
// waits: net::PollDesc, net::ConnectionPool::acquire, the HTTP/2 client's
// request) and every child context. Nothing is killed: interrupted waits
// return an error code, and code that checks this_fiber::cancelled()
// returns on its own. Waits that are not cancellable (Mutex, Event,
// WaitGroup, Oneshot / Future, Semaphore, sleep, blocking / fs, resolve)
// simply complete as usual.
//
// Factories (the object is neither copyable nor movable: hooks and the
// timer point into it; the factories return a prvalue built in place):
//   cancellable()                      a root without a deadline
//   with_deadline(sched, t) / (t)      a root that ends with kTimedOut at t
//                                      (on the pollers' clock, sched.now());
//                                      (t) from a fiber: its scheduler
//   with_timeout(sched, d) / (d)       with_deadline(now + d)
//   child_of(parent) / (parent, t)     ends with the parent (same reason),
//                                      by cancel(), or at its own deadline;
//                                      effective deadline min(t, parent's)
//
// Deadline: one poller timer per context with a deadline of its own (none
// for a child whose deadline is not earlier than its parent's: the parent's
// timer reaches it). Its callback, on the poller thread, cancels the
// context with kTimedOut (running the hooks there). A deadline in the past
// ends the context at construction (no timer).
//
// Lifetime (fail fast where it can be seen): the context must outlive
// every fiber that runs under it and every child context; its destructor
// fails fast while a fiber is parked under it or a child exists. A
// user's cancel() must have returned before the destructor runs. The
// destructor of a context whose timer is still pending takes the timer
// back from its poller (a posted retire node withdraw()s it on the poller
// thread) and waits for that: it parks a fiber, or blocks a plain thread;
// not from a poller callback, and the scheduler's pollers must still run
// (destroy contexts before stopping the scheduler). Contexts without a
// deadline, and those whose timer has fired, are destroyed at once.
//
// Fibers: fiber::go(sched, ctx, f) runs f under ctx; go(f) and wg.go(f)
// from a fiber inherit the caller's context; go(sched, f) (an explicit
// scheduler, as library background fibers use) runs under none.
// ContextScope switches the current fiber's context for a scope (a child
// with a tighter deadline, or nullptr to shield a section).
//
// For primitives (a cancellable wait): InterruptScope (below) and
// docs/cancellation.md section 3.3.

#include <cerrno>
#include <cstdint>
#include <optional>
#include <type_traits>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/poller/sources.hpp"
#include "fibers/poller/task_batch.hpp"
#include "fibers/queue/lock_free_spmc_queue.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/sync/sync.hpp"
#include "fibers/wait/context_core.hpp"
#include "fibers/wait/result.hpp"
#include "fibers/wait/thread_waiter.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fibers::fiber {

// The errno a cancellable network wait reports for a context's end.
inline int errno_of(wait::Error reason) noexcept {
    return reason == wait::Error::kTimedOut ? ETIMEDOUT : ECANCELED;
}

// A scheduler whose pollers can run the deadline timer of a context.
template <class S>
concept DeadlineScheduler = FiberScheduler<S> && requires(S& s, typename S::Timer& timer) {
    typename S::Poller;
    { s.now() } -> std::same_as<poller::TimePoint>;
    s.pollers().schedule(timer);
};

namespace detail {

struct NoContextTimers {};

// The deadline timer and the node that retires it (see the top comment).
template <class Sched>
struct ContextTimers {
    typename Sched::Timer timer;   // fires at the deadline: cancel(kTimedOut)
    typename Sched::Timer retire;  // posted by the destructor: withdraws `timer`
    // 1 once the timer's callback has finished with the context (release).
    typename Sched::SyncPolicy::template Atomic<std::uint32_t> done{0};
    bool armed = false;               // `timer` was scheduled
    wait::WaitNode* destroyer = nullptr;  // woken by the retire callback
    // Checker only (Twist simulator): written by the timer's callback
    // before `done` and by the destructor after its retirement (stands for
    // the callback's accesses of the context and for freeing it), so a
    // missing release / acquire on `done` shows up as a race.
    [[no_unique_address]] typename queue::detail::HbProbeOf<typename Sched::SyncPolicy>::type probe{};
};

}  // namespace detail

template <class Sched>
class BasicContext {
    static_assert(FiberScheduler<Sched>, "fiber::BasicContext<Sched>: Sched must be a fiber scheduler");

public:
    using SyncPolicy = typename Sched::SyncPolicy;
    using Core = wait::ContextCore<SyncPolicy>;
    using Interrupt = wait::Interrupt<SyncPolicy>;
    using Fiber = BasicFiber<Sched>;

    static constexpr bool kHasTimers = DeadlineScheduler<Sched>;
    static constexpr poller::TimePoint kNoDeadline = poller::TimePoint::max();

    BasicContext(const BasicContext&) = delete;
    BasicContext& operator=(const BasicContext&) = delete;
    BasicContext(BasicContext&&) = delete;
    BasicContext& operator=(BasicContext&&) = delete;

    ~BasicContext() {
        if constexpr (kHasTimers) {
            retire_timer();
        }
        if (parent_ != nullptr) {
            // Waits if the parent's canceller is cancelling us right now.
            parent_->core_.delist(link_);
        }
        // ~Core fails fast if a wait or a child is still enlisted.
    }

    // ---- factories -----------------------------------------------------------------

    // A root without a deadline: ends only by cancel().
    static BasicContext cancellable() noexcept { return BasicContext(nullptr, nullptr, kNoDeadline); }

    // A root that ends with kTimedOut at `deadline` (sched.now()'s clock;
    // TimePoint{} / max(): none). Any thread.
    static BasicContext with_deadline(Sched& scheduler, poller::TimePoint deadline) noexcept
        requires kHasTimers
    {
        return BasicContext(&scheduler, nullptr, deadline);
    }

    // The same on the current fiber's scheduler (fails fast outside a fiber).
    static BasicContext with_deadline(poller::TimePoint deadline) noexcept
        requires kHasTimers
    {
        return BasicContext(&current_scheduler("context: with_deadline(t) outside a fiber (pass the scheduler)"),
                            nullptr, deadline);
    }

    static BasicContext with_timeout(Sched& scheduler, poller::Duration timeout) noexcept
        requires kHasTimers
    {
        return BasicContext(&scheduler, nullptr, scheduler.now() + timeout);
    }

    static BasicContext with_timeout(poller::Duration timeout) noexcept
        requires kHasTimers
    {
        Sched& scheduler = current_scheduler("context: with_timeout(d) outside a fiber (pass the scheduler)");
        return BasicContext(&scheduler, nullptr, scheduler.now() + timeout);
    }

    // Ends when `parent` ends (with its reason) or by cancel(). The parent
    // must outlive it. Any thread.
    static BasicContext child_of(BasicContext& parent) noexcept {
        return BasicContext(parent.scheduler_, &parent, kNoDeadline);
    }

    // The same, also ending with kTimedOut at `deadline`; the effective
    // deadline is min(deadline, parent's). A timer of its own only if
    // `deadline` is the earlier one (the scheduler: the parent's, else the
    // current fiber's).
    static BasicContext child_of(BasicContext& parent, poller::TimePoint deadline) noexcept
        requires kHasTimers
    {
        return BasicContext(parent.scheduler_, &parent, deadline);
    }

    // ---- state ---------------------------------------------------------------------

    // Ends the context with kCancelled (unless it has ended already: false)
    // and interrupts everything parked under it, children included, on the
    // calling thread. Any thread; must return before the destructor runs.
    bool cancel() noexcept { return core_.cancel(wait::Error::kCancelled); }

    // Ended (cancelled, deadline, or the parent ended). acquire: true orders
    // the caller after whatever the canceller did before cancelling.
    bool cancelled() const noexcept { return core_.cancelled(); }

    // kCancelled / kTimedOut once ended; nullopt while live.
    std::optional<wait::Error> reason() const noexcept { return core_.reason(); }

    // 0 while live, else ECANCELED / ETIMEDOUT.
    int error_code() const noexcept {
        const std::optional<wait::Error> r = core_.reason();
        return r ? errno_of(*r) : 0;
    }

    // The effective deadline (min of its own and its ancestors'), kNoDeadline
    // (TimePoint::max()) if none.
    poller::TimePoint deadline() const noexcept { return deadline_; }
    bool has_deadline() const noexcept { return deadline_ != kNoDeadline; }

    // ---- for cancellable waits ---------------------------------------------------------

    // See wait::ContextCore and InterruptScope.
    bool enlist(Interrupt& h) noexcept { return core_.enlist(h); }
    void delist(Interrupt& h) noexcept { core_.delist(h); }

    // Racy snapshot (tests): a wait or a child is enlisted.
    bool has_dependants_approx() noexcept { return core_.has_dependants_approx(); }

private:
    BasicContext(Sched* scheduler, BasicContext* parent, poller::TimePoint own) noexcept
        : link_(this, &on_parent_end), scheduler_(scheduler) {
        if (own == poller::TimePoint{}) {
            own = kNoDeadline;
        }
        deadline_ = own;
        poller::TimePoint inherited = kNoDeadline;
        if (parent != nullptr) {
            inherited = parent->deadline_;
            if (inherited < deadline_) {
                deadline_ = inherited;
            }
            if (parent->core_.enlist(link_)) {
                parent_ = parent;
            } else {
                // The parent has ended: so has the child, with its reason.
                static_cast<void>(core_.cancel(*parent->core_.reason()));
                return;
            }
        }
        if (own < inherited) {
            if constexpr (kHasTimers) {
                arm(own);
            }
        }
    }

    static Sched& current_scheduler(const char* what) noexcept { return Fiber::current_or_die(what).scheduler(); }

    // ---- the deadline timer (DeadlineScheduler only) ---------------------------------

    void arm(poller::TimePoint deadline) noexcept {
        if (scheduler_ == nullptr) {
            Fiber* self = Fiber::current();
            if (self == nullptr) [[unlikely]] {
                fail::fail_fast("context: a deadline needs a scheduler (create the root with one, or in a fiber)");
            }
            scheduler_ = &self->scheduler();
        }
        if (deadline <= scheduler_->now()) {
            static_cast<void>(core_.cancel(wait::Error::kTimedOut));
            return;
        }
        auto& t = timers_;
        t.timer.memo = this;
        t.timer.callback = &on_timer;
        t.timer.deadline = deadline;
        t.armed = true;
        scheduler_->pollers().schedule(t.timer);
    }

    // The destructor: no timer callback may run (or be running) once it
    // returns.
    void retire_timer() noexcept {
        auto& t = timers_;
        if (!t.armed) {
            return;
        }
        // acquire: the callback's accesses of *this (its cancel walk)
        // happen-before we go on.
        if (t.done.load(std::memory_order_acquire) != 0) {
            t.probe.write();
            return;  // fired and finished: the poller holds no reference
        }
        auto* poller = static_cast<typename Sched::Poller*>(t.timer.poller());
        t.retire.memo = this;
        t.retire.callback = &on_retire;
        Fiber* self = Fiber::current();
        if (self != nullptr) {
            wait::WaitNode node = self->wait_node();
            t.destroyer = &node;
            poller->post(t.retire);
            self->park();  // one wake: on_retire
            t.probe.write();
            return;
        }
        if constexpr (sync::ParkingSyncPolicy<SyncPolicy>) {
            wait::ThreadWaiter<SyncPolicy> waiter;
            t.destroyer = &waiter.node;
            poller->post(t.retire);
            waiter.wait();
            t.probe.write();
        } else {
            fail::fail_fast("context: destroying a context with a pending deadline outside a fiber needs a "
                            "parking Sync policy");
        }
    }

    // Poller thread: the deadline. (kCancelled: the poller is being
    // destroyed with the timer pending; nothing to cancel then.)
    static void on_timer(void* memo, poller::TimerEvent event, poller::TaskBatch&) noexcept {
        auto* ctx = static_cast<BasicContext*>(memo);
        if (event == poller::TimerEvent::kFired) {
            static_cast<void>(ctx->core_.cancel(wait::Error::kTimedOut));
        }
        ctx->timers_.probe.write();
        // release: our accesses of *ctx before its destructor goes on.
        ctx->timers_.done.store(1, std::memory_order_release);  // last touch of *ctx
    }

    // Poller thread (the timer's poller): the destructor waits for this.
    // If the timer's callback has run, it has also returned (one thread):
    // withdraw() finds the node idle. Otherwise it takes it back.
    static void on_retire(void* memo, poller::TimerEvent event, poller::TaskBatch&) noexcept {
        auto* ctx = static_cast<BasicContext*>(memo);
        auto& t = ctx->timers_;
        if (event == poller::TimerEvent::kFired) {
            static_cast<void>(static_cast<typename Sched::Poller*>(t.timer.poller())->withdraw(t.timer));
        }
        // kCancelled: the poller is being destroyed and releases every
        // pending timer itself (the deadline timer included).
        t.destroyer->notify();  // last touch of *ctx (and of the node)
    }

    // A parent's canceller (any thread): we end with the parent's reason.
    static void on_parent_end(void* self, wait::Error reason) noexcept {
        static_cast<void>(static_cast<BasicContext*>(self)->core_.cancel(reason));
    }

    using Timers = std::conditional_t<kHasTimers, detail::ContextTimers<Sched>, detail::NoContextTimers>;

    Core core_;
    Interrupt link_;                  // ours, in parent_'s list
    BasicContext* parent_ = nullptr;  // set iff link_ is enlisted there
    Sched* scheduler_ = nullptr;      // for the deadline timer (may be null without one)
    poller::TimePoint deadline_ = kNoDeadline;
    [[no_unique_address]] Timers timers_;
};

using Context = BasicContext<sched::Scheduler>;

// A cancellable wait's registration with its context, on the waiter's
// stack: construct it AFTER the wait is published (armed / queued), park,
// and let it go out of scope after the park. `fn(self, reason)` claims the
// wait (the same RMW the event path uses: first one wins) and, if it won,
// wakes the waiter with the error; it may run on any thread (the
// canceller's, the poller's for a deadline), or inline in the constructor
// if the context has ended already (then the waiter's own wake just marks
// it NOTIFIED and the park returns at once). The destructor delists, which
// may wait until a canceller that took the node is out of `fn`; only then
// may the waiter's frame go. See docs/cancellation.md, section 3.3.
template <class Sched>
class InterruptScope {
public:
    using Context = BasicContext<Sched>;
    using Fn = typename Context::Interrupt::Fn;

    InterruptScope(Context& context, void* self, Fn fn) noexcept : context_(context), node_(self, fn) {
        enlisted_ = context.enlist(node_);
        if (!enlisted_) {
            fn(self, *context.reason());
        }
    }

    ~InterruptScope() {
        if (enlisted_) {
            context_.delist(node_);
        }
    }

    InterruptScope(const InterruptScope&) = delete;
    InterruptScope& operator=(const InterruptScope&) = delete;

private:
    Context& context_;
    typename Context::Interrupt node_;
    bool enlisted_ = false;
};

// Runs the current fiber under another context for a scope (a child with a
// tighter deadline for one step, or nullptr: no context, e.g. a cleanup
// that must not be interrupted), restoring the previous one at the end. On
// a fiber of Sched only (fails fast elsewhere).
template <class Sched = sched::Scheduler>
class BasicContextScope {
public:
    explicit BasicContextScope(BasicContext<Sched>* context) noexcept
        : fiber_(BasicFiber<Sched>::current_or_die("context: ContextScope outside a fiber")),
          previous_(fiber_.context()) {
        fiber_.set_context(context);
    }

    explicit BasicContextScope(BasicContext<Sched>& context) noexcept : BasicContextScope(&context) {}

    ~BasicContextScope() { fiber_.set_context(previous_); }

    BasicContextScope(const BasicContextScope&) = delete;
    BasicContextScope& operator=(const BasicContextScope&) = delete;

private:
    BasicFiber<Sched>& fiber_;  // stable: a fiber does not move
    BasicContext<Sched>* previous_;
};

using ContextScope = BasicContextScope<sched::Scheduler>;

namespace this_fiber {

// The context the current fiber runs under; nullptr outside a fiber of
// Sched or without one.
template <FiberScheduler Sched = sched::Scheduler>
BasicContext<Sched>* context() noexcept {
    BasicFiber<Sched>* self = BasicFiber<Sched>::current();
    return self != nullptr ? self->context() : nullptr;
}

// The current fiber's context has ended (cancelled or past its deadline):
// the cooperative check for code that does not wait (CPU loops) or waits on
// something that is not cancellable. false without a context.
template <FiberScheduler Sched = sched::Scheduler>
bool cancelled() noexcept {
    BasicContext<Sched>* ctx = context<Sched>();
    return ctx != nullptr && ctx->cancelled();
}

// 0, or ECANCELED / ETIMEDOUT if the current fiber's context has ended.
template <FiberScheduler Sched = sched::Scheduler>
int context_error() noexcept {
    BasicContext<Sched>* ctx = context<Sched>();
    return ctx != nullptr ? ctx->error_code() : 0;
}

}  // namespace this_fiber

}  // namespace fibers::fiber

namespace fibers {

// The request context of the default scheduler (fiber::BasicContext).
using Context = fiber::BasicContext<sched::Scheduler>;

}  // namespace fibers
