#pragma once

// Stackful fibers on a Scheduler.
//
//   fiber::go(scheduler, [&]() noexcept {
//       ...
//       fiber::this_fiber::yield();             // to the back of the queue
//       fiber::go([]() noexcept { ... });       // a sibling, same scheduler
//       fiber::this_fiber::sleep_for(1ms);      // a timer of the poller
//       std::lock_guard guard(mutex);           // fiber::Mutex (mutex.hpp)
//   });
//
// Layout. A fiber lives at the TOP of its own stack (from the scheduler's
// StackPool), together with the user's closure:
//
//   high  +-------------------------+  stack end
//         | BasicFiber              |  TaskHeader (first member), state word,
//         |                         |  coroutine, ...
//         +-------------------------+
//         | closure (user's lambda) |
//         +-------------------------+
//         | coroutine's stack       |  grows down
//   low   +-------------------------+  (guard page below)
//
// Nothing is allocated: spawning is a StackPool acquire (a per-thread cache
// hit normally) plus placement-new. A fiber never moves, so its address is
// its handle: scheduling it is submit(Task{fiber, &step, &drop}). The public
// handle (BasicFiberHandle, 8 bytes) only allows wake().
// On completion the closure is destroyed on the fiber's stack, then the
// trampoline (on the worker's stack, after resume() returned kCompleted)
// destroys the fiber object and returns the stack to the StackPool.
//
// Fiber bodies are `void() noexcept`.
//
// Finding the current fiber (this_fiber::*, Mutex::lock): a library
// thread-local slot, set by the trampoline around every resume(), read only
// through a [[gnu::noinline]] accessor with an asm memory barrier
// (detail::current_fiber_slot): fibers migrate between workers across every
// suspend, and the compiler may otherwise keep a thread-local's address (or a
// "pure" call's result) across the switch. Never cache the result across a
// suspension point; a BasicFiber* itself is stable (the fiber does not move).
// Why not (only) a context argument to the body: std::lock_guard calls
// mutex.lock() with no arguments, and code deep in a call chain should not
// need a context threaded through it. The slot costs one call per lookup.
//
// State machine (Tokio's task state; protects against "woken before it has
// left its stack")
// ---------------------------------------------------------------------------
//   PARKED = 0     suspended, owned by nobody: the next wake() schedules it
//   RUNNING = 1    running, or suspended but not yet past the trampoline's
//                  park CAS (still "on its stack" from the waker's view)
//   NOTIFIED = 2   bit, with RUNNING: woken while running
//   QUEUED = 4     in a ThreadPool queue (or being scheduled)
//
//   wake():   PARKED  -> CAS(PARKED -> QUEUED, acq_rel), submit the fiber
//             RUNNING -> CAS(RUNNING -> RUNNING | NOTIFIED, release); the
//                        trampoline sees it at its park CAS
//             else    -> a second wake for one wait: fail_fast
//   trampoline (Task call_ptr, memo = the fiber), on a worker:
//     state = RUNNING
//     loop: r = coroutine.resume()
//       completed -> destroy the fiber, release the stack, return
//       yield     -> state = QUEUED, submit, return
//       park      -> [run the awaiter, if the fiber parked with one]
//                    CAS(RUNNING -> PARKED, acq_rel) ok -> return
//                    failed (RUNNING | NOTIFIED) -> state = RUNNING,
//                    resume again at once (no queue round trip)
//
// So a fiber may publish itself into a wait list (a WaitNode on its own
// stack) or hand its handle to a timer BEFORE it switches out: whoever wakes
// it early just sets NOTIFIED. Only a wake that finds PARKED schedules it,
// and that can only happen after the trampoline's CAS, i.e. after the fiber
// has left its stack for good. Rule: after wake(), the waker never touches
// the waiter's node or the fiber again (the fiber may be running already).
//
// Contract: wake() is called exactly once per park (by the one party the
// fiber handed itself to); a park without a pending or future wake is a
// fiber that sleeps forever; a fiber that has published itself must park
// (not yield, not complete) before it may be woken again.
//
// Memory orders:
//   * waker -> resumed fiber. PARKED path: the waker's acq_rel CAS is
//     sequenced before its submit; the ThreadPool queue's release / acquire
//     carries everything to the worker that runs the trampoline. NOTIFIED
//     path: the waker's CAS is a release; the trampoline's failing park CAS
//     reads that value with acquire (its failure order), then resumes the
//     fiber on the same thread.
//   * parked fiber -> waker -> next run. The successful park CAS is a
//     release after everything the fiber did on its stack (on this thread,
//     the switch is sequenced); the waker's CAS PARKED -> QUEUED reads it
//     with acquire and passes it on through the queue.
//   * initial state = RUNNING is a relaxed store: the trampoline owns the
//     fiber (it got it from the queue), and a waker can only learn about the
//     fiber from something the fiber publishes after this store (with
//     release), so by coherence its CAS reads RUNNING or a later value.
//   * yield's QUEUED store is relaxed: nobody may wake a yielding fiber.
//     A fiber that has published itself (a WaitNode, its handle, a timer)
//     must park, never yield or complete, until it is woken: a wake that
//     arrived meanwhile (NOTIFIED) would be lost, and a later one would hit a
//     QUEUED or destroyed fiber. Release builds trust this rule (plain store);
//     debug builds (NDEBUG not defined) check it: yield exchanges the state
//     and completion loads it, and either fails fast if NOTIFIED was set.
//   * reason_ (yield / park) and the current-fiber slot are plain: written
//     and read on one thread around the switch.
//
// Parking with an awaiter (BasicFiber::park(awaiter)): the coroutine layer's
// AwaitSuspend pattern, for actions that must run strictly after the fiber
// has left its stack (release a spinlock that guards a wait list, hand the
// fiber to code that would resume it directly). The trampoline runs
// awaiter.await_suspend() first, then the park CAS, so a wake that the
// awaiter enables is handled like any other.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <new>
#include <type_traits>
#include <utility>

#include "fibers/context/sanitizer.hpp"
#include "fibers/context/stack_view.hpp"
#include "fibers/coro/coroutine.hpp"
#include "fibers/fail/fail_fast.hpp"
#include "fibers/poller/sources.hpp"
#include "fibers/poller/task_batch.hpp"
#include "fibers/poller/timer.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/sched/task_header.hpp"
#include "fibers/stack/stack.hpp"
#include "fibers/sync/sync.hpp"
#include "fibers/thread_pool/task.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fibers::fiber {

using thread_pool::Task;

// What a fiber needs from its scheduler (sched::BasicScheduler, or a smaller
// test double): a context-switching Sync policy, a pool to submit Tasks to
// and a StackPool. this_fiber::sleep_* additionally use pollers(), now()
// and the Timer type (sched::BasicScheduler).
template <class S>
concept FiberScheduler = sync::ContextSyncPolicy<typename S::SyncPolicy> &&
                         requires(S& s, Task task, stack::Stack* stack) {
                             s.pool().submit(std::move(task));
                             { s.stacks().acquire() } -> std::same_as<stack::Stack*>;
                             s.stacks().release(stack);
                         };

template <FiberScheduler Sched>
class BasicFiber;

namespace detail {

template <class Sched>
struct CurrentFiber {
    BasicFiber<Sched>* fiber = nullptr;
};

// The fiber running on this thread (nullptr outside fibers). The only access
// to the slot: never inlined, never pure (see the top comment).
template <class Sched>
[[gnu::noinline]] CurrentFiber<Sched>& current_fiber_slot() noexcept {
    asm volatile("" ::: "memory");
    return Sched::SyncPolicy::template thread_local_instance<CurrentFiber<Sched>>();
}

}  // namespace detail

// A fiber's public handle: 8 bytes, only wake(). Valid while the fiber
// waits for this wake (see the contract in the top comment).
template <FiberScheduler Sched>
class BasicFiberHandle {
public:
    BasicFiberHandle() noexcept = default;
    explicit BasicFiberHandle(BasicFiber<Sched>* fiber) noexcept : fiber_(fiber) {}

    // Schedules the fiber (or marks it NOTIFIED if it has not left its stack
    // yet). Once per park; any thread.
    void wake() const noexcept { fiber_->wake(); }

    explicit operator bool() const noexcept { return fiber_ != nullptr; }
    BasicFiber<Sched>* get() const noexcept { return fiber_; }

    friend bool operator==(BasicFiberHandle, BasicFiberHandle) noexcept = default;

private:
    BasicFiber<Sched>* fiber_ = nullptr;
};

template <FiberScheduler Sched>
class BasicFiber {
public:
    using SyncPolicy = typename Sched::SyncPolicy;
    using Coroutine = coro::BasicCoroutine<SyncPolicy>;
    using SuspendContext = coro::BasicSuspendContext<SyncPolicy>;
    using Header = sched::BasicTaskHeader<Sched>;
    using Handle = BasicFiberHandle<Sched>;

    // State word values (see the top comment).
    static constexpr std::uint32_t kParked = 0;
    static constexpr std::uint32_t kRunning = 1;
    static constexpr std::uint32_t kNotified = 2;
    static constexpr std::uint32_t kQueued = 4;

    BasicFiber(const BasicFiber&) = delete;
    BasicFiber& operator=(const BasicFiber&) = delete;
    BasicFiber(BasicFiber&&) = delete;
    BasicFiber& operator=(BasicFiber&&) = delete;

    // Places a new fiber running `f` at the top of a stack from
    // scheduler.stacks() and submits it. Any thread.
    template <class F>
        requires std::is_nothrow_invocable_v<std::decay_t<F>&> &&
                 std::is_nothrow_constructible_v<std::decay_t<F>, F&&>
    static void spawn(Sched& scheduler, F&& f) noexcept {
        using Closure = std::decay_t<F>;
        stack::Stack* stack = scheduler.stacks().acquire();
        const context::StackView view = stack->view();
#if FIBERS_CONTEXT_ASAN
        // A reused stack may still carry redzones of frames that never
        // returned (e.g. the previous coroutine's bottom frame) where the
        // fiber object and the closure go now.
        __asan_unpoison_memory_region(view.data(), view.size());
#endif
        const auto begin = reinterpret_cast<std::uintptr_t>(view.data());
        const std::uintptr_t end = begin + view.size();
        constexpr std::size_t kClosureAlign = std::max<std::size_t>(alignof(Closure), 16);
        const std::uintptr_t fiber_at = (end - sizeof(BasicFiber)) & ~(std::uintptr_t{alignof(BasicFiber)} - 1);
        if (fiber_at < begin + sizeof(Closure) + kClosureAlign + coro::detail::kMinStackBelowClosure)
            [[unlikely]] {
            fail::fail_fast("fiber: stack too small for the fiber and its closure");
        }
        const std::uintptr_t closure_at = (fiber_at - sizeof(Closure)) & ~(std::uintptr_t{kClosureAlign} - 1);
        Closure* closure = ::new (reinterpret_cast<void*>(closure_at)) Closure(std::forward<F>(f));
        const context::StackView below{view.data(), static_cast<std::size_t>(closure_at - begin)};
        auto* fiber = ::new (reinterpret_cast<void*>(fiber_at))
            BasicFiber(scheduler, stack, below, closure, &run_closure<Closure>, &destroy_closure<Closure>);
        count_spawned(scheduler);
        scheduler.pool().submit(fiber->task());
    }

    // The fiber running on this thread, nullptr outside fibers.
    static BasicFiber* current() noexcept { return detail::current_fiber_slot<Sched>().fiber; }

    // Like current(), fail fast outside a fiber.
    static BasicFiber& current_or_die(const char* what) noexcept {
        BasicFiber* fiber = current();
        if (fiber == nullptr) [[unlikely]] {
            fail::fail_fast(what);
        }
        return *fiber;
    }

    // ---- any thread ----------------------------------------------------------

    // See the state machine in the top comment. Exactly once per park.
    void wake() noexcept {
        if (prepare_wake()) {
            scheduler().pool().submit(task());
        }
    }

    // wake() split in two for callers that deliver the Task themselves (the
    // poller's TaskBatch): true -> the caller now owns scheduling and must
    // submit task(); false -> the fiber was still running, nothing to do, and
    // the fiber must not be touched any more.
    bool prepare_wake() noexcept {
        std::uint32_t s = state_.load(std::memory_order_relaxed);
        for (;;) {
            if (s == kParked) {
                if (state_.compare_exchange_weak(s, kQueued, std::memory_order_acq_rel,
                                                 std::memory_order_relaxed)) {
                    return true;
                }
            } else if (s == kRunning) {
                if (state_.compare_exchange_weak(s, kRunning | kNotified, std::memory_order_release,
                                                 std::memory_order_relaxed)) {
                    return false;
                }
            } else {
                fail::fail_fast("fiber: wake() of a fiber that is not waiting (double wake?)");
            }
        }
    }

    // A WaitNode wake function: self is the fiber.
    static void wake_node(void* self) noexcept { static_cast<BasicFiber*>(self)->wake(); }

    // A WaitNode for this fiber (next unset).
    wait::WaitNode wait_node() noexcept { return wait::WaitNode{nullptr, this, &wake_node}; }

    Handle handle() noexcept { return Handle(this); }
    const Header& header() const noexcept { return header_; }
    Sched& scheduler() const noexcept { return *header_.scheduler; }

    // The Task that runs (resumes) this fiber. Only for the party that owns
    // scheduling (after prepare_wake() returned true).
    Task task() noexcept { return Task(this, &step, &drop); }

    // ---- on the fiber itself ----------------------------------------------------

    // To the back of the worker's queue.
    void yield() noexcept {
        reason_ = Reason::kYield;
        ctx_->suspend();
    }

    // Until wake() (which may already have happened: then it returns at once).
    void park() noexcept {
        reason_ = Reason::kPark;
        ctx_->suspend();
    }

    // Park, with awaiter.await_suspend() run by the trampoline after the
    // fiber has left its stack, before the park CAS.
    template <coro::Awaiter A>
    void park(A& awaiter) noexcept {
        reason_ = Reason::kPark;
        ctx_->suspend(awaiter);
    }

    // Parks until a poller fires a timer at `deadline` (on the pollers'
    // clock, see Scheduler::now()). The timer node lives on this stack and
    // goes to scheduler().pollers().schedule(): the next poller in the
    // calling thread's round robin (PollerPool::next()). The poller does not
    // touch the node once its callback is entered, and the callback's wake
    // is the only one (no cancellation, so no lazy-removal reference
    // outlives the sleep). A deadline in the past fires at once, possibly
    // before the fiber has left its stack (NOTIFIED). Zero allocation.
    void sleep_until(poller::TimePoint deadline) noexcept {
        typename Sched::Timer timer(this, &on_timer);
        timer.deadline = deadline;
        scheduler().pollers().schedule(timer);
        park();
    }

    void sleep_for(poller::Duration duration) noexcept {
        sleep_until(scheduler().now() + duration);
    }

    // Racy snapshot of the state word (tests).
    std::uint32_t state_approx() const noexcept { return state_.load(std::memory_order_relaxed); }

private:
    enum class Reason : std::uint8_t { kYield, kPark };

    using RunClosure = void (*)(void* closure) noexcept;

    BasicFiber(Sched& scheduler, stack::Stack* stack, context::StackView below, void* closure,
               RunClosure run, typename Coroutine::Drop destroy) noexcept
        : header_{&scheduler, this},
          stack_(stack),
          closure_(closure),
          run_(run),
          coroutine_(below, coro::BasicRoutine<SyncPolicy>::erased(this, &body), destroy, closure) {}

    ~BasicFiber() = default;

    template <class Closure>
    static void run_closure(void* closure) noexcept {
        Closure& f = *static_cast<Closure*>(closure);
        f();
        f.~Closure();  // on the fiber's stack, at completion
    }

    template <class Closure>
    static void destroy_closure(void* closure) noexcept {
        static_cast<Closure*>(closure)->~Closure();
    }

    // Bottom of the coroutine: runs on the fiber's stack.
    static void body(void* self, SuspendContext ctx) noexcept {
        auto* fiber = static_cast<BasicFiber*>(self);
        fiber->ctx_ = &ctx;  // lives in this frame, i.e. as long as the body runs
        fiber->run_(fiber->closure_);
    }

    // The Task call_ptr: the trampoline, on a worker's stack.
    static void step(void* memo) noexcept {
        auto* fiber = static_cast<BasicFiber*>(memo);
        // The slot of THIS worker thread: the trampoline itself never
        // migrates (it runs on the worker's own stack), only the fiber does.
        detail::CurrentFiber<Sched>& current = detail::current_fiber_slot<Sched>();
        fiber->state_.store(kRunning, std::memory_order_relaxed);
        for (;;) {
            current.fiber = fiber;
            const coro::ResumeResult r = fiber->coroutine_.resume();
            current.fiber = nullptr;
            if (r.completed()) {
#ifndef NDEBUG
                if (fiber->state_.load(std::memory_order_relaxed) != kRunning) [[unlikely]] {
                    fail::fail_fast("fiber: completed with a pending wake (a fiber that published "
                                    "itself must park until woken)");
                }
#endif
                fiber->destroy();
                return;
            }
            if (fiber->reason_ == Reason::kYield) {
                Sched& scheduler = fiber->scheduler();
#ifndef NDEBUG
                // Debug builds catch a broken contract (see "yield" in the
                // top comment) at the cost of an RMW per yield.
                if (fiber->state_.exchange(kQueued, std::memory_order_relaxed) != kRunning) [[unlikely]] {
                    fail::fail_fast("fiber: yield() with a pending wake (a fiber that published "
                                    "itself must park, not yield)");
                }
#else
                fiber->state_.store(kQueued, std::memory_order_relaxed);
#endif
                scheduler.pool().submit(fiber->task());
                return;  // the fiber may already run elsewhere
            }
            if (r.awaiter) {
                r.awaiter.await_suspend();
            }
            std::uint32_t expected = kRunning;
            if (fiber->state_.compare_exchange_strong(expected, kParked, std::memory_order_acq_rel,
                                                      std::memory_order_acquire)) {
                return;  // the fiber belongs to its waker now
            }
            if (expected != (kRunning | kNotified)) [[unlikely]] {
                fail::fail_fast("fiber: corrupt state word at park");
            }
            // Woken before it left its stack: run it again right here.
            fiber->state_.store(kRunning, std::memory_order_relaxed);
        }
    }

    // Task delete_ptr: the pool drops the fiber without running it (stop).
    static void drop(void* memo) noexcept {
        auto* fiber = static_cast<BasicFiber*>(memo);
        Sched& scheduler = fiber->scheduler();
        stack::Stack* stack = fiber->stack_;
        if (!fiber->coroutine_.started()) {
            fiber->~BasicFiber();  // destroys the closure (Coroutine's drop)
        }
        // else: suspended mid-run; without exceptions its frames cannot be
        // unwound, so they are abandoned (no destructors) with the stack.
        scheduler.stacks().release(stack);
        count_finished(scheduler);
    }

    // After completion, on the worker's stack.
    void destroy() noexcept {
        Sched& scheduler = this->scheduler();
        stack::Stack* stack = stack_;
        this->~BasicFiber();
        scheduler.stacks().release(stack);
        count_finished(scheduler);
    }

    // Fiber counts, for schedulers that keep them (sched::BasicScheduler).
    static void count_spawned(Sched& scheduler) noexcept {
        if constexpr (requires { scheduler.count_fiber_spawned(); }) {
            scheduler.count_fiber_spawned();
        }
    }
    static void count_finished(Sched& scheduler) noexcept {
        if constexpr (requires { scheduler.count_fiber_finished(); }) {
            scheduler.count_fiber_finished();
        }
    }

    static void on_timer(void* memo, poller::TimerEvent, poller::TaskBatch& out) noexcept {
        auto* fiber = static_cast<BasicFiber*>(memo);
        if (fiber->prepare_wake()) {
            out.push(fiber->task());
        }
    }

    Header header_;  // first member: the memo points at the fiber
    typename SyncPolicy::template Atomic<std::uint32_t> state_{kQueued};
    Reason reason_ = Reason::kPark;
    stack::Stack* stack_;
    void* closure_;
    RunClosure run_;
    SuspendContext* ctx_ = nullptr;  // while the body runs
    Coroutine coroutine_;
};

// ---- spawning ------------------------------------------------------------------

// A new fiber running f() on `scheduler`. Any thread.
template <FiberScheduler Sched, class F>
    requires std::is_nothrow_invocable_v<std::decay_t<F>&>
void go(Sched& scheduler, F&& f) noexcept {
    BasicFiber<Sched>::spawn(scheduler, std::forward<F>(f));
}

// From inside a fiber: a sibling on the same scheduler.
template <FiberScheduler Sched = sched::Scheduler, class F>
    requires std::is_nothrow_invocable_v<std::decay_t<F>&>
void go(F&& f) noexcept {
    BasicFiber<Sched>& self = BasicFiber<Sched>::current_or_die("fiber: go(f) outside a fiber");
    BasicFiber<Sched>::spawn(self.scheduler(), std::forward<F>(f));
}

// ---- the running fiber ---------------------------------------------------------

namespace this_fiber {

template <FiberScheduler Sched = sched::Scheduler>
bool in_fiber() noexcept {
    return BasicFiber<Sched>::current() != nullptr;
}

template <FiberScheduler Sched = sched::Scheduler>
void yield() noexcept {
    BasicFiber<Sched>::current_or_die("fiber: this_fiber::yield() outside a fiber").yield();
}

template <FiberScheduler Sched = sched::Scheduler>
BasicFiberHandle<Sched> self() noexcept {
    return BasicFiber<Sched>::current_or_die("fiber: this_fiber::self() outside a fiber").handle();
}

// Until someone calls wake() on self() (see the contract in the top comment).
template <FiberScheduler Sched = sched::Scheduler>
void park() noexcept {
    BasicFiber<Sched>::current_or_die("fiber: this_fiber::park() outside a fiber").park();
}

template <FiberScheduler Sched = sched::Scheduler>
void sleep_until(poller::TimePoint deadline) noexcept {
    BasicFiber<Sched>::current_or_die("fiber: this_fiber::sleep_until() outside a fiber").sleep_until(deadline);
}

template <FiberScheduler Sched = sched::Scheduler>
void sleep_for(poller::Duration duration) noexcept {
    BasicFiber<Sched>::current_or_die("fiber: this_fiber::sleep_for() outside a fiber").sleep_for(duration);
}

}  // namespace this_fiber

using Fiber = BasicFiber<sched::Scheduler>;
using FiberHandle = BasicFiberHandle<sched::Scheduler>;

}  // namespace fibers::fiber
