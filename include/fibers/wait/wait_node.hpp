#pragma once

// WaitNode: the shared core of every waiting primitive (Mutex now; events,
// channels, select later).
//
// A primitive is written once over WaitNode and knows nothing about who
// waits: a stackful fiber, a stackless (C++20) coroutine, anything that can
// be "woken" by a plain function call. A front-end puts a node where the
// waiter lives and fills it:
//
//   stackful fiber  : the node is a local on the fiber's stack, published
//                     into the primitive's wait list from that stack, before
//                     the fiber switches out; wake = the fiber's wake()
//                     (fibers/fiber/fiber.hpp). The fiber's state word makes
//                     a wake that arrives before the switch harmless.
//   stackless       : the node lives in the coroutine frame, published from
//                     await_suspend(h); wake = schedule the coroutine_handle
//                     (self = h.address()). After a successful publish the
//                     awaiter must not touch `this` (it may already be
//                     resumed and gone).
//
// Contract of a node, for primitives and front-ends alike:
//   * A published node is woken at most once per publication: wake(self)
//     is called by whoever takes it out of the wait list, and that is its
//     owner's signal to go on.
//   * After calling wake, the waker never touches the node again (it lives
//     in the waiter's frame and may be gone the moment the waiter runs).
//     Read `wake` and `self` out of the node before the call (notify() does).
//   * The waker's writes before the wake must be visible to the waiter once
//     it runs: wake implementations provide that release / acquire (the
//     fiber's state word, a thread pool queue).
//   * Nodes are aligned to at least 4 (they hold pointers): primitives may
//     use the two low bits of a node pointer as flags (MutexCore: LOCKED,
//     WOKEN).
//
// No virtual functions: `self` + a plain function pointer, like Task.
//
// Open (timeouts / cancellation): a node that leaves a wait list by timeout
// needs a per-node state (WAITING / WOKEN / CANCELLED, first CAS wins) and a
// rule that the node lives until the list's owner unlinks it. The fields
// above stay; that state would be added here, next to them.

#include <cstdint>

namespace fibers::wait {

struct WaitNode {
    using WakeFn = void (*)(void* self) noexcept;

    WaitNode* next = nullptr;  // owned by the wait list the node is in
    void* self = nullptr;
    WakeFn wake = nullptr;

    // Calls wake(self). The node must not be touched afterwards (by anyone
    // but its owner).
    void notify() noexcept {
        const WakeFn fn = wake;
        void* const arg = self;
        fn(arg);
    }
};

static_assert(alignof(WaitNode) >= 4, "the two low bits of a WaitNode* are used as flags");

}  // namespace fibers::wait
