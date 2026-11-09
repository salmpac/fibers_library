#pragma once

// One-shot Event for stackful fibers: the stackful front-end of
// wait::EventCore (see event_core.hpp for the algorithm and its memory
// orders).
//
//   fiber::Event ready;
//   ... in fibers:            ready.wait();   // parks until set()
//   ... anywhere (any thread, a poller callback, a fiber): ready.set();
//
// set() wakes every waiting fiber and makes all later wait()s return at
// once; it is idempotent (a second set() is a no-op, returns false).
// Everything sequenced before the first set() happens-before every return
// from wait().
//
// wait() parks the fiber, not the worker thread: a WaitNode on the fiber's
// own stack is published with the core's push CAS, then the fiber parks. A
// set() that arrives before the fiber has left its stack is absorbed by the
// fiber's state word (NOTIFIED). wait() must be called from a fiber of a
// scheduler of type Sched when the event is not set yet: outside such a
// fiber it fails fast (it never blocks a thread). set() and is_set() work
// from anywhere. set() wakes the fibers one submit each: into the setting
// worker's own queue (others steal from it), or from outside the pool into
// the workers' mailboxes, round robin.
//
// The event may be destroyed once every wait() has returned (set() does not
// touch it after its exchange); destroying it with waiters fails fast.

#include "fibers/fiber/fiber.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/wait/event_core.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fibers::fiber {

template <FiberScheduler Sched = sched::Scheduler>
class BasicEvent {
public:
    BasicEvent() noexcept = default;
    BasicEvent(const BasicEvent&) = delete;
    BasicEvent& operator=(const BasicEvent&) = delete;

    // true: this call set the event; false: it was already set. Any thread.
    bool set() noexcept { return core_.set(); }

    bool is_set() const noexcept { return core_.is_set(); }

    // Until the event is set (returns at once if it is).
    void wait() noexcept {
        if (core_.is_set()) [[likely]] {
            return;
        }
        wait_slow();
    }

private:
    [[gnu::noinline]] void wait_slow() noexcept {
        BasicFiber<Sched>& self = BasicFiber<Sched>::current_or_die("fiber: Event::wait() outside a fiber");
        wait::WaitNode node = self.wait_node();
        if (!core_.wait_or_enqueue(node)) {
            self.park();  // exactly one wake per publication: set() is one-shot
        }
    }

    wait::EventCore<typename Sched::SyncPolicy> core_;
};

using Event = BasicEvent<sched::Scheduler>;

}  // namespace fibers::fiber
