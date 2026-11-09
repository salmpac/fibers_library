#pragma once

// EventCore: a one-shot event over WaitNode, waiter-agnostic. Front-ends
// (fibers/fiber/event.hpp for stackful fibers; WaitGroupCore uses it too)
// add only "how to wait for the node's wake".
//
// The event goes from "not set" to "set" once and stays set. Every waiter
// that arrived before set() is woken by it, exactly once; every waiter
// after it does not wait at all.
//
// State
// -----
//   state_ = 0                        not set, no waiters
//          | kDone (1)                set
//          | WaitNode* (aligned >= 2) not set; head of a Treiber stack of
//                                     waiters (linked through next)
//
//   wait_or_enqueue(node):
//     loop: s = state (acquire)
//           s == kDone -> return true (set: do not wait)
//           node.next = s; CAS(s -> &node, release; failure: acquire)
//                         -> return false: the caller waits for node's wake
//
//   set():
//     list = state.exchange(kDone, acq_rel)
//     list == kDone -> return false (already set: set() is idempotent)
//     for each node in list: next = node->next; node->notify()
//                            (next is read BEFORE notify: the woken waiter
//                            may go on at once and its node, e.g. on a
//                            fiber's stack, disappears)
//     return true
//
// Wake order is LIFO (the stack as it was taken); nobody may rely on it.
// ABA cannot happen: a node is pushed once per publication and the stack is
// only ever taken whole, in the one exchange that also closes it.
//
// set() is idempotent: a second set() is a no-op that returns false (an
// Event is often set from several places, e.g. "first error or shutdown").
// WaitGroupCore relies on the return value to check that it sets its event
// exactly once.
//
// Lifetime: set() does not touch the EventCore after its exchange (only the
// taken nodes), so a waiter may destroy the event as soon as its wait has
// returned (and no other set() / wait is still in progress). Destroying an
// event with waiters fails fast.
//
// Memory orders
// -------------
//   * set() -> waiters. Everything sequenced before set() happens-before
//     every return from a wait that observed the event set:
//       - a waiter that reads kDone (the load or the failing CAS, both
//         acquire) reads it from the exchange (release; every later write is
//         another kDone exchange, an RMW, so the release sequence holds);
//       - a waiter woken through its node: the exchange is sequenced before
//         the notify, and the wake itself (front-end) provides the release /
//         acquire to the woken waiter (WaitNode contract).
//     A redundant set() also releases (it is an exchange), but only waiters
//     that read its kDone synchronise with it: wait() orders after the
//     FIRST set(), not after every one.
//   * waiter -> set(). The push CAS is a release after writing node.next
//     (and whatever the front-end put into the node); the exchange is
//     acquire and reads the newest push; earlier pushes are earlier RMWs of
//     the same release sequence, so every node's fields are visible to the
//     waking loop.
//   * is_set() is an acquire load: true orders the caller after set().

#include <cstdint>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/sync/sync.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fibers::wait {

template <sync::SyncPolicy Sync = sync::StdSync>
class EventCore {
public:
    EventCore() noexcept = default;
    EventCore(const EventCore&) = delete;
    EventCore& operator=(const EventCore&) = delete;

    ~EventCore() {
        const std::uintptr_t s = state_.load(std::memory_order_relaxed);
        if (s != 0 && s != kDone) [[unlikely]] {
            fail::fail_fast("wait: destroying an event that has waiters");
        }
    }

    // true: this call set the event (and woke every waiter); false: it was
    // already set (no-op). Any thread.
    bool set() noexcept {
        const std::uintptr_t list = state_.exchange(kDone, std::memory_order_acq_rel);
        if (list == kDone) {
            return false;
        }
        WaitNode* node = reinterpret_cast<WaitNode*>(list);
        while (node != nullptr) {
            WaitNode* const next = node->next;  // before notify: the node may vanish
            node->notify();
            node = next;
        }
        return true;
    }

    // true: the event is set, do not wait. false: `node` (next is
    // overwritten, self / wake must be set) was pushed onto the waiters'
    // stack; the caller must not touch it until its wake, which comes
    // exactly once (set() is one-shot). After a false the node may be woken
    // (and its owner resumed) before this returns.
    bool wait_or_enqueue(WaitNode& node) noexcept {
        std::uintptr_t s = state_.load(std::memory_order_acquire);
        for (;;) {
            if (s == kDone) {
                return true;
            }
            node.next = reinterpret_cast<WaitNode*>(s);
            if (state_.compare_exchange_weak(s, reinterpret_cast<std::uintptr_t>(&node),
                                             std::memory_order_release, std::memory_order_acquire)) {
                return false;
            }
        }
    }

    bool is_set() const noexcept { return state_.load(std::memory_order_acquire) == kDone; }

    // Racy snapshot (tests, diagnostics): someone is enqueued.
    bool has_waiters_approx() const noexcept {
        const std::uintptr_t s = state_.load(std::memory_order_relaxed);
        return s != 0 && s != kDone;
    }

private:
    // Never a node address: nodes are aligned to at least 2.
    static constexpr std::uintptr_t kDone = 1;
    static_assert(alignof(WaitNode) >= 2, "kDone must not be a WaitNode address");

    typename Sync::template Atomic<std::uintptr_t> state_{0};
};

}  // namespace fibers::wait
