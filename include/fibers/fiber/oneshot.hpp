#pragma once

// Oneshot<T>: a one-shot value hand-off whose state lives in the consumer's
// own frame (no allocation at all). The stackful front-end of
// wait::OneshotState (see wait/oneshot_core.hpp for the protocol and its
// memory orders).
//
//   fiber::Oneshot<Reply> reply;                     // on the consumer's stack
//   send(Request{..., reply.promise()});             // the producer's handle
//   wait::Result<Reply> r = reply.get();             // parks until set / broken
//   if (r) use(*r); else ... r.error() == wait::Error::kBroken ...
//
// The producer (any thread: another fiber, a plain thread, a poller
// callback, a blocking-pool call) holds a fiber::Promise<T>
// (wait::BasicPromise): set(args...) constructs the value in the Oneshot
// and wakes the consumer; dropping the Promise unset (or abandon()) breaks
// it: get() returns Error::kBroken.
//
// Oneshot<void>: a "done" signal without a value: set() takes no
// arguments, get() returns a Result<void> (success or kBroken).
//
// Rules (fail fast where they are broken):
//   * promise() once; get() needs it (otherwise nobody could ever complete).
//   * get() / a successful try_get() consume the result, once: the value is
//     moved out into the Result. Calling either again fails fast.
//   * One consumer: get() / try_get() / ready() from one party at a time
//     (the owner of the Oneshot).
//   * Lifetime: the Oneshot must outlive every touch of the producer. The
//     producer's last touch is its completion (set / abandon), so the
//     Oneshot may be destroyed once a completion has been OBSERVED (get()
//     returned, try_get() or ready() saw it), or if promise() was never
//     called. Destroying it while the Promise is still outstanding and
//     unset fails fast: the producer would later write into a dead frame.
//     An untaken value is destroyed with the Oneshot.
//   Why fail fast and not wait for the producer in the destructor: a
//   destructor that parks hides a wait (and a possible deadlock: the
//   producer may be waiting for us), and outside a fiber it could only
//   block a thread. Code that may walk away from a pending result uses the
//   pooled Future (fiber/future.hpp), which detaches safely.
//
// get() in a fiber of a scheduler of type Sched parks the fiber (a WaitNode
// on its stack, published with the core's CAS, then park(); a completion
// that arrives before the fiber has left its stack is absorbed by its state
// word). Outside such a fiber it blocks the calling thread on an atomic
// (wait::ThreadWaiter): fine for a plain thread (main, a foreign event
// loop's thread), never from a plain callback on a worker (it would block
// the worker). try_get() never waits, from anywhere.
//
// Everything sequenced before set() happens-before get() / try_get()
// returning the value (and before ready() returning true).

#include <optional>
#include <type_traits>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/sync/sync.hpp"
#include "fibers/wait/oneshot_core.hpp"
#include "fibers/wait/result.hpp"
#include "fibers/wait/thread_waiter.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fibers::fiber {

using wait::Error;
using wait::Result;

template <class T, class Sync = sched::Scheduler::SyncPolicy>
using BasicPromise = wait::BasicPromise<T, Sync>;

template <class T>
using Promise = wait::BasicPromise<T, sched::Scheduler::SyncPolicy>;

namespace detail {

// Until `state` is final; returns kReady / kBroken (read with acquire).
// A fiber of Sched parks; any other caller blocks its thread.
template <class Sched, class State>
wait::OneshotStatus wait_final(State& state) noexcept {
    const wait::OneshotStatus s = state.status();
    if (s != wait::OneshotStatus::kPending) [[likely]] {
        return s;
    }
    BasicFiber<Sched>* self = BasicFiber<Sched>::current();
    if (self != nullptr) {
        wait::WaitNode node = self->wait_node();
        if (const wait::OneshotStatus now = state.wait_or_enqueue(node); now != wait::OneshotStatus::kPending) {
            return now;
        }
        self->park();  // exactly one wake: the producer completes once
    } else {
        using Sync = typename Sched::SyncPolicy;
        if constexpr (sync::ParkingSyncPolicy<Sync>) {
            wait::ThreadWaiter<Sync> waiter;
            if (const wait::OneshotStatus now = state.wait_or_enqueue(waiter.node);
                now != wait::OneshotStatus::kPending) {
                return now;
            }
            waiter.wait();
        } else {
            fail::fail_fast("fiber: waiting for a oneshot outside a fiber needs a parking Sync policy");
        }
    }
    return state.status();  // final now (the wake already orders it)
}

}  // namespace detail

template <class T, FiberScheduler Sched = sched::Scheduler>
class BasicOneshot {
public:
    using SyncPolicy = typename Sched::SyncPolicy;
    using State = wait::OneshotState<T, SyncPolicy>;
    using Promise = wait::BasicPromise<T, SyncPolicy>;
    using value_type = T;

    BasicOneshot() noexcept = default;

    // Not movable: the Promise points at it.
    BasicOneshot(const BasicOneshot&) = delete;
    BasicOneshot& operator=(const BasicOneshot&) = delete;

    ~BasicOneshot() {
        if (!promise_taken_) {
            return;  // nobody else ever saw it
        }
        const wait::OneshotStatus s = state_.status();  // acquire: orders the value's construction
        if (s == wait::OneshotStatus::kPending) [[unlikely]] {
            fail::fail_fast("fiber: Oneshot destroyed while its Promise is still outstanding (not set)");
        }
        if (s == wait::OneshotStatus::kReady && !consumed_) {
            state_.destroy_value();
        }
    }

    // The producer's handle. Once.
    Promise promise() noexcept {
        if (promise_taken_) [[unlikely]] {
            fail::fail_fast("fiber: Oneshot::promise() called twice");
        }
        promise_taken_ = true;
        return Promise(&state_);
    }

    // Waits for the result (see the top comment) and consumes it.
    Result<T> get() noexcept {
        check_consumable();
        const wait::OneshotStatus s = detail::wait_final<Sched>(state_);
        consumed_ = true;
        return state_.take(s);
    }

    // The result if the producer has completed (consumes it), nullopt if
    // not yet. Never waits.
    std::optional<Result<T>> try_get() noexcept {
        check_consumable();
        const wait::OneshotStatus s = state_.status();
        if (s == wait::OneshotStatus::kPending) {
            return std::nullopt;
        }
        consumed_ = true;
        return state_.take(s);
    }

    // The producer has completed (set or broken); get() would not wait.
    bool ready() const noexcept { return state_.status() != wait::OneshotStatus::kPending; }

    // get() / try_get() has returned the result.
    bool consumed() const noexcept { return consumed_; }

private:
    void check_consumable() const noexcept {
        if (consumed_) [[unlikely]] {
            fail::fail_fast("fiber: Oneshot result already consumed (get() / try_get() called again)");
        }
        if (!promise_taken_) [[unlikely]] {
            fail::fail_fast("fiber: Oneshot waited on before promise() (nobody can complete it)");
        }
    }

    State state_;
    bool promise_taken_ = false;  // consumer-only, plain
    bool consumed_ = false;       // consumer-only, plain
};

template <class T>
using Oneshot = BasicOneshot<T, sched::Scheduler>;

}  // namespace fibers::fiber
