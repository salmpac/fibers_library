#pragma once

// MutexCore: the waiter-agnostic algorithm of the fiber Mutex, over WaitNode.
// Front-ends (fibers/fiber/mutex.hpp for stackful fibers; a stackless one
// later) add only "how to wait for the node's wake".
//
// Barging: unlock() releases the lock and wakes one waiter; whoever gets to
// the lock first takes it (a running fiber may overtake the woken one, which
// then queues again). Starvation is possible and accepted (no handoff mode).
//
// State
// -----
//   state_   = [ Treiber stack of waiters (WaitNode*) | LOCKED (bit 0) ]
//   private_ = plain singly linked FIFO of waiters, touched only by the
//              current lock holder (handed from holder to holder through the
//              release in unlock() / acquire in lock()).
//
//   lock_or_enqueue(node):
//     fast path: fetch_or(LOCKED, acquire) found LOCKED clear -> acquired
//     loop: s = state
//           !LOCKED: CAS(s -> s | LOCKED, acquire)                -> acquired
//           LOCKED:  node.next = head(s); CAS(s -> &node | LOCKED, release)
//                    -> enqueued: the caller waits for node's wake, then
//                       calls lock_or_enqueue again (it may be overtaken
//                       and enqueue again)
//
//   unlock():
//     w = private_.pop_front()                    (we still hold the lock)
//     loop: s = state
//           w == null && head(s) != null: CAS(s -> LOCKED, acquire): take the
//               whole stack, private_ = reverse(it) (oldest first), w = pop
//           else CAS(s -> head(s), release): unlocked
//     if w: w->notify()
//
// Why CAS and not fetch_and(~LOCKED) to unlock: with w == null, a waiter
// that pushes between our look at the head and the release would be left in
// the stack with nobody to wake it. The CAS fails instead, and the next round
// takes the newcomer into private_.
//
// Progress invariant: while there are waiters (in the stack or in private_),
// every unlock() wakes exactly one. The woken one either gets the lock or
// finds it held, re-enqueues, and that holder's unlock() wakes the next.
//
// ABA is harmless: a push only links node.next = head and CASes the head, so
// if the head is the same pointer the link is right whatever happened to it
// in between; taking waiters is "take all" in one CAS that never reads next
// beforehand. A node may leave (taken, woken, overtaken) and come back.
//
// Memory orders
// -------------
//   * Lock hand-over: every unlock is a release RMW on state_, every lock an
//     acquire RMW reading it (directly or through a release sequence: every
//     write of state_ is an RMW), so a critical section happens-before the
//     next one; private_ travels the same way.
//   * Node contents: a push is a release CAS after writing node.next (and
//     whatever the front-end put into the node); the unlocker's take-all CAS
//     is acquire and reads the newest push, and earlier pushes are earlier
//     RMWs of the same release sequences, so every next it follows is visible.
//   * Wake: the waiter's later lock attempt reads state_ again with acquire;
//     the wake itself (front-end) provides the release / acquire that makes
//     the unlocker's writes visible to the woken waiter.
//
// Alternatives not taken: a Vyukov MPSC queue (as in blocking/op_queue.hpp)
// has a window "tail exchanged, prev->next not linked yet" where unlock()
// would have to spin. Reversing a taken batch is O(1) amortised per waiter.

#include <cstdint>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/sync/sync.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fibers::wait {

template <sync::SyncPolicy Sync = sync::StdSync>
class MutexCore {
public:
    MutexCore() noexcept = default;
    MutexCore(const MutexCore&) = delete;
    MutexCore& operator=(const MutexCore&) = delete;

    ~MutexCore() {
        if (state_.load(std::memory_order_relaxed) != 0 || private_ != nullptr) [[unlikely]] {
            fail::fail_fast("wait: destroying a mutex that is locked or has waiters");
        }
    }

    // One RMW, no waiting.
    bool try_lock() noexcept {
        return (state_.fetch_or(kLocked, std::memory_order_acquire) & kLocked) == 0;
    }

    // true: acquired. false: `node` (next is overwritten, self / wake must
    // be set) was pushed onto the waiters' stack; the caller must not touch
    // it until its wake, then call lock_or_enqueue again. After a false the
    // node may be woken (and its owner resumed) before this returns.
    bool lock_or_enqueue(WaitNode& node) noexcept {
        if (try_lock()) [[likely]] {
            return true;
        }
        std::uintptr_t s = state_.load(std::memory_order_relaxed);
        for (;;) {
            if ((s & kLocked) == 0) {
                if (state_.compare_exchange_weak(s, s | kLocked, std::memory_order_acquire,
                                                 std::memory_order_relaxed)) {
                    return true;
                }
                continue;
            }
            node.next = head_of(s);
            if (state_.compare_exchange_weak(s, reinterpret_cast<std::uintptr_t>(&node) | kLocked,
                                             std::memory_order_release, std::memory_order_relaxed)) {
                return false;
            }
        }
    }

    // Precondition: the caller holds the lock.
    void unlock() noexcept {
        WaitNode* w = pop_private();
        std::uintptr_t s = state_.load(std::memory_order_relaxed);
        for (;;) {
            if ((s & kLocked) == 0) [[unlikely]] {
                fail::fail_fast("wait: unlock() of a mutex that is not locked");
            }
            if (w == nullptr && head_of(s) != nullptr) {
                // Take every waiter at once; we keep the lock.
                if (state_.compare_exchange_weak(s, kLocked, std::memory_order_acquire,
                                                 std::memory_order_relaxed)) {
                    private_ = reverse(head_of(s));
                    w = pop_private();
                    s = kLocked;
                }
                continue;
            }
            if (state_.compare_exchange_weak(s, s & ~kLocked, std::memory_order_release,
                                             std::memory_order_relaxed)) {
                break;
            }
        }
        if (w != nullptr) {
            w->notify();
        }
    }

    // Racy snapshot (tests, diagnostics).
    bool locked_approx() const noexcept {
        return (state_.load(std::memory_order_relaxed) & kLocked) != 0;
    }

private:
    static constexpr std::uintptr_t kLocked = 1;

    static WaitNode* head_of(std::uintptr_t s) noexcept {
        return reinterpret_cast<WaitNode*>(s & ~kLocked);
    }

    static WaitNode* reverse(WaitNode* head) noexcept {
        WaitNode* reversed = nullptr;
        while (head != nullptr) {
            WaitNode* next = head->next;
            head->next = reversed;
            reversed = head;
            head = next;
        }
        return reversed;
    }

    WaitNode* pop_private() noexcept {
        WaitNode* w = private_;
        if (w != nullptr) {
            private_ = w->next;
        }
        return w;
    }

    typename Sync::template Atomic<std::uintptr_t> state_{0};
    WaitNode* private_ = nullptr;  // lock holder only
};

}  // namespace fibers::wait
