#pragma once

// Running a blocking call off the fiber: the fiber hands an op to its
// scheduler's BlockingPool and parks; a pool thread runs the call and wakes
// the fiber, which resumes on some worker with the result.
//
//   fiber::go(scheduler, [&]() noexcept {
//       int rc = this_fiber::blocking([&]() noexcept { return ::getaddrinfo(host, port, &hints, &res); });
//       ...
//   });
//
// this_fiber::blocking(f) runs a noexcept callable that way and returns its
// result (any type, references and void included). this_fiber::blocking_op(op)
// does the same for any blocking op (blocking/ops.hpp: PreadOp, FsyncOp,
// CallOp, a user's Op<Derived>); the file functions of fs/file.hpp are built
// on it.
//
// Nothing is allocated: the op, the callable's result and the waiter (the
// continuation's memo) all live on the parked fiber's stack, which stays
// mapped and untouched while the fiber is parked.
//
// The handshake
// -------------
//   fiber F (on a worker)                     pool thread P
//   op.set_continuation(Task{&waiter,
//       on_done, on_drop}, Sink::run_inline())
//   blocking().submit(op)  ---------------->  pop(op)            (queue: link
//                                                                 release / acquire)
//   park()                                    result = execute()
//     trampoline: CAS RUNNING -> PARKED       continuation = move(op.continuation)
//                                             on_done(&waiter) (inline sink):
//                                               fiber = waiter.fiber
//                                               fiber->wake()
//   resumes, reads op.result()
//
// F publishes itself (submits the op) BEFORE it parks: P may finish at once,
// while F is still on its stack. The fiber's state word absorbs that (see
// fiber.hpp): wake() then finds RUNNING and only sets NOTIFIED; the
// trampoline's park CAS fails and resumes F at once on the same worker. A
// wake that finds PARKED schedules F through the ThreadPool. So no awaiter
// (post-switch hook) is needed, and the order "submit, then park" is safe.
//
// Lifetime (the op must not be touched by P after the wake). The op, the
// waiter and the result slot are on F's stack, so they die as soon as F
// resumes and returns from blocking(). P's last accesses are:
//   * BasicBlockingOp::run: the result store, moving the continuation and the
//     sink out of the op into P's locals; from then on run() uses only those
//     locals (ops.hpp: "the pool does not touch the op after handing the
//     continuation to the sink").
//   * on_done: loads waiter.fiber into a local, then calls wake() on the
//     fiber, which lives at the top of F's stack too, but is not torn down
//     when F merely resumes: wake() reads and CASes the fiber's state word,
//     and only on the PARKED path (F has left its stack and cannot run until
//     this very wake submits it) reads its scheduler and submits it.
//   * Sink::run_inline's lambda and Task::run touch only P's own Task copy.
// After wake() returns P is back in its loop. So P never touches the op or
// the waiter after the wake; the fiber itself it touches only inside wake(),
// whose contract (fiber.hpp) is exactly that.
//
// Memory orders. This file adds no atomics; it composes existing edges:
//   * F -> P (op parameters, the callable's captures): F's submit pushes the
//     op with the queue's release link store; P pops it with acquire.
//   * P -> F (op.result(), the callable's result, waiter.dropped): written by
//     P before wake(). NOTIFIED path: wake's CAS RUNNING -> RUNNING|NOTIFIED
//     is a release, the trampoline's failing park CAS reads it with acquire
//     and resumes F on that thread. PARKED path: wake's CAS PARKED -> QUEUED
//     (acq_rel) is sequenced before the ThreadPool submit, whose queue
//     release / acquire carries it to the worker that resumes F.
//   * F's writes before parking, to F after migrating (its own frames): the
//     park CAS (release) -> wake's CAS PARKED -> QUEUED (acquire) -> queue.
// The Twist test (tests/fiber/twist/blocking_twist_test.cpp) checks each of these with Shared cells.
//
// Outside a fiber (no current fiber of scheduler type Sched: a plain thread,
// a plain callback task, a fiber of another scheduler type) the call runs
// inline on the calling thread and blocks it. Offloading is a question of
// WHERE a call runs, never of WHETHER it runs: every call runs exactly once.
//
// After the BlockingPool has stopped
// ----------------------------------
// BlockingPool::submit() after (or racing with) stop() drops the op: its
// continuation's delete_ptr runs instead (on_drop), exactly once, either on
// the submitting fiber itself (inside submit(): the wake then sets NOTIFIED
// and park() returns at once) or on the thread draining the queue in stop()
// (the wake then schedules F through the ThreadPool). on_drop marks the
// waiter and wakes F; F then runs the call itself, inline on its worker
// (blocking it). So a fiber is never left parked by the blocking pool, and
// the call's outcome is the same as if it had been offloaded.
// Scheduler::stop() stops the pollers, then the workers, then the blocking
// threads: by the time ops are dropped there, the ThreadPool no longer runs
// tasks, and the woken fiber's Task stays queued until ~ThreadPool drops it
// (the fiber is abandoned with its stack, like every fiber still parked at
// stop(); see scheduler.hpp). An op that is RUNNING when stop() begins
// completes normally (stop() joins the pool threads), and its wake ends the
// same way. The drop-and-run-inline path matters when only the blocking pool
// is stopped while workers still run fibers (scheduler.blocking().stop()).

#include <concepts>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "fibers/blocking/blocking_pool.hpp"
#include "fibers/blocking/ops.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/thread_pool/task.hpp"

namespace fibers::fiber {

// A fiber scheduler with a BlockingPool for its own Sync policy.
template <class S>
concept BlockingScheduler =
    FiberScheduler<S> && requires(S& s, ::fibers::blocking::BasicBlockingOp<typename S::SyncPolicy>& op) {
        s.blocking().submit(op);
    };

namespace detail {

// The continuation's memo: on the parked fiber's stack.
template <class Sched>
struct BlockingWaiter {
    BasicFiber<Sched>* fiber = nullptr;
    bool dropped = false;  // written by on_drop before the wake, read by the fiber after it

    // call_ptr: the op ran (on a pool thread, through Sink::run_inline()).
    static void on_done(void* memo) noexcept {
        // Load the fiber first: once wake() has run, the fiber may be
        // running and the waiter (on its stack) gone.
        BasicFiber<Sched>* fiber = static_cast<BlockingWaiter*>(memo)->fiber;
        fiber->wake();
    }

    // delete_ptr: the pool dropped the op without running it (stopped).
    static void on_drop(void* memo) noexcept {
        auto* waiter = static_cast<BlockingWaiter*>(memo);
        waiter->dropped = true;
        BasicFiber<Sched>* fiber = waiter->fiber;
        fiber->wake();
    }
};

// Submits `op` from the running fiber `self` and parks until the pool has
// run it (true: op.result() is set) or dropped it (false: the call did not
// run). `op` must not have a continuation of its own.
template <class Sched>
bool submit_and_park(BasicFiber<Sched>& self,
                     ::fibers::blocking::BasicBlockingOp<typename Sched::SyncPolicy>& op) noexcept {
    BlockingWaiter<Sched> waiter{&self};
    op.set_continuation(Task(&waiter, &BlockingWaiter<Sched>::on_done, &BlockingWaiter<Sched>::on_drop),
                        ::fibers::blocking::Sink::run_inline());
    self.scheduler().blocking().submit(op);
    // Woken exactly once: by on_done or on_drop (the pool runs or drops
    // every op exactly once). The wake may already have happened.
    self.park();
    return !waiter.dropped;
}

// Storage for blocking()'s result, on the fiber's stack. Written by the pool
// thread (or inline), read by the fiber after the wake.
template <class R>
class BlockingResult {
public:
    BlockingResult() noexcept {}
    BlockingResult(const BlockingResult&) = delete;
    BlockingResult& operator=(const BlockingResult&) = delete;
    ~BlockingResult() {
        if (has_value_) {
            value_.~R();
        }
    }

    template <class F>
    void fill(F& f) noexcept {
        ::new (static_cast<void*>(std::addressof(value_))) R(f());  // guaranteed elision
        has_value_ = true;
    }

    R take() noexcept { return std::move(value_); }

private:
    union {
        R value_;
    };
    bool has_value_ = false;
};

template <class R>
    requires std::is_reference_v<R>
class BlockingResult<R> {
public:
    template <class F>
    void fill(F& f) noexcept {
        R ref = f();
        pointer_ = std::addressof(ref);
    }

    R take() noexcept { return static_cast<R>(*pointer_); }

private:
    std::remove_reference_t<R>* pointer_ = nullptr;
};

}  // namespace detail

namespace this_fiber {

// Runs a blocking op off the fiber: on the current fiber's scheduler's
// BlockingPool, parking the fiber until it is done. Outside a fiber of
// Sched, or if the pool dropped the op (stopped), it runs inline on the
// calling thread. Either way op.result() is set when this returns. `op` is
// a concrete op type (public `IoResult execute() noexcept`), not yet
// queued, without a continuation (the one it is given here is consumed).
template <BlockingScheduler Sched = sched::Scheduler, class Op>
    requires std::derived_from<Op, ::fibers::blocking::BasicBlockingOp<typename Sched::SyncPolicy>> &&
             requires(Op& op) {
                 { op.execute() } noexcept -> std::same_as<::fibers::blocking::IoResult>;
             }
void blocking_op(Op& op) noexcept {
    BasicFiber<Sched>* self = BasicFiber<Sched>::current();
    if (self != nullptr && detail::submit_and_park(*self, op)) {
        return;
    }
    op.set_result(op.execute());
}

// Runs f() on the current fiber's scheduler's BlockingPool and returns its
// result; the fiber parks meanwhile (its worker runs other fibers). f runs
// exactly once: inline on the calling thread outside a fiber of Sched, or
// when the pool has stopped (see the top comment). f may be called on any
// thread; whatever it captures by reference stays valid (it is on the
// parked fiber's stack or older).
template <BlockingScheduler Sched = sched::Scheduler, class F>
    requires std::is_nothrow_invocable_v<F&>
std::invoke_result_t<F&> blocking(F&& f) noexcept {
    using R = std::invoke_result_t<F&>;
    using Sync = typename Sched::SyncPolicy;
    static_assert(std::is_void_v<R> || std::is_reference_v<R> || std::is_nothrow_move_constructible_v<R>,
                  "this_fiber::blocking: the result must be nothrow move constructible");
    BasicFiber<Sched>* self = BasicFiber<Sched>::current();
    if constexpr (std::is_void_v<R>) {
        if (self != nullptr) {
            auto call = [&f]() noexcept { f(); };
            ::fibers::blocking::CallOp<decltype(call), Sync> op(call);
            if (detail::submit_and_park(*self, op)) {
                return;
            }
        }
        f();
    } else {
        if (self != nullptr) {
            detail::BlockingResult<R> slot;
            auto call = [&f, &slot]() noexcept { slot.fill(f); };
            ::fibers::blocking::CallOp<decltype(call), Sync> op(call);
            if (detail::submit_and_park(*self, op)) {
                return slot.take();
            }
        }
        return f();
    }
}

}  // namespace this_fiber

}  // namespace fibers::fiber
