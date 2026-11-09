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
//   state_   = [ Treiber stack of waiters (WaitNode*) | WOKEN (bit 1) | LOCKED (bit 0) ]
//   private_ = plain singly linked FIFO of waiters, touched only by the
//              current lock holder (handed from holder to holder through the
//              release in unlock() / acquire in lock()).
//
//   WOKEN: a waiter has been woken and has not yet made its next attempt.
//   While it is set, unlock() wakes nobody (Go's mutexWoken): under heavy
//   contention most woken waiters lose to running fibers and park again, and
//   waking a new one on every unlock turned the mutex into a wake/park churn
//   (BM_MutexContention, 512 fibers on 8 workers: 166 ns per critical
//   section without WOKEN, 17 ns with it).
//   Only an unlock() by the holder sets WOKEN; only the woken waiter's next
//   attempt clears it (in the same CAS that takes the lock or re-enqueues).
//   So while a holder runs, WOKEN can go 1 -> 0 but never 0 -> 1.
//
//   lock_or_enqueue(node, woken):
//     fast path (woken == false): fetch_or(LOCKED, acquire) found LOCKED clear
//     loop: s = state; clear = woken ? WOKEN : 0
//           !LOCKED: CAS(s -> (s | LOCKED) & ~clear, acquire)      -> acquired
//           LOCKED:  node.next = head(s);
//                    CAS(s -> &node | LOCKED | (s & WOKEN & ~clear), release)
//                    -> enqueued: the caller waits for node's wake, then
//                       calls lock_or_enqueue(node, true) (it may be
//                       overtaken and enqueue again)
//
//   unlock():
//     w = null
//     loop: s = state
//           WOKEN set, or no waiters at all:  CAS(s -> s & ~LOCKED, release): done
//           w == null && private_ empty: CAS(s -> LOCKED, acquire): take the
//               whole stack, private_ = reverse(it) (oldest first)
//           w == null: w = private_.pop_front()        (we still hold the lock)
//           CAS(s -> (s & ~LOCKED) | WOKEN, release): unlocked; w->notify()
//     Once w is popped the "wake" branch stays taken: WOKEN was seen clear
//     and cannot be set by anyone else while we hold the lock, and waiters
//     only accumulate.
//
// Why CAS and not fetch_and(~LOCKED) to unlock: with w == null, a waiter
// that pushes between our look at the head and the release would be left in
// the stack with nobody to wake it. The CAS fails instead, and the next round
// takes the newcomer into private_.
//
// Progress invariant: while there are waiters (in the stack or in private_),
// either a woken waiter is on its way (WOKEN) or the next unlock() wakes one.
// The woken one either gets the lock or re-enqueues, clearing WOKEN in that
// same RMW of state_; a concurrent unlock() that saw WOKEN set then fails its
// CAS (state_ changed), sees WOKEN clear and wakes the next.
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
//   * WOKEN carries no data, only the progress hand-off: it is set by the
//     release CAS that unlocks and cleared by the woken waiter's acquire CAS
//     (takes the lock) or release CAS (pushes the node), i.e. by the RMWs
//     that need those orders anyway.
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
    // it until its wake, then call lock_or_enqueue(node, true) (woken: the
    // attempt that follows a wake clears WOKEN). After a false the node may
    // be woken (and its owner resumed) before this returns.
    bool lock_or_enqueue(WaitNode& node, bool woken = false) noexcept {
        if (!woken && try_lock()) [[likely]] {
            return true;
        }
        const std::uintptr_t clear = woken ? kWoken : 0;
        std::uintptr_t s = state_.load(std::memory_order_relaxed);
        for (;;) {
            if ((s & kLocked) == 0) {
                if (state_.compare_exchange_weak(s, (s | kLocked) & ~clear, std::memory_order_acquire,
                                                 std::memory_order_relaxed)) {
                    return true;
                }
                continue;
            }
            node.next = head_of(s);
            const std::uintptr_t desired =
                reinterpret_cast<std::uintptr_t>(&node) | kLocked | (s & kWoken & ~clear);
            if (state_.compare_exchange_weak(s, desired, std::memory_order_release,
                                             std::memory_order_relaxed)) {
                return false;
            }
        }
    }

    // Precondition: the caller holds the lock.
    void unlock() noexcept {
        WaitNode* w = nullptr;
        std::uintptr_t s = state_.load(std::memory_order_relaxed);
        for (;;) {
            if ((s & kLocked) == 0) [[unlikely]] {
                fail::fail_fast("wait: unlock() of a mutex that is not locked");
            }
            if (w == nullptr && ((s & kWoken) != 0 || (private_ == nullptr && head_of(s) == nullptr))) {
                // Someone is already on the way, or nobody waits: just release.
                if (state_.compare_exchange_weak(s, s & ~kLocked, std::memory_order_release,
                                                 std::memory_order_relaxed)) {
                    return;
                }
                continue;
            }
            if (w == nullptr && private_ == nullptr) {
                // Take every waiter at once; we keep the lock (WOKEN is clear).
                if (state_.compare_exchange_weak(s, kLocked, std::memory_order_acquire,
                                                 std::memory_order_relaxed)) {
                    private_ = reverse(head_of(s));
                    s = kLocked;
                }
                continue;
            }
            if (w == nullptr) {
                w = pop_private();
            }
            if (state_.compare_exchange_weak(s, (s & ~kLocked) | kWoken, std::memory_order_release,
                                             std::memory_order_relaxed)) {
                break;
            }
        }
        w->notify();
    }

    // Racy snapshot (tests, diagnostics).
    bool locked_approx() const noexcept {
        return (state_.load(std::memory_order_relaxed) & kLocked) != 0;
    }

private:
    static constexpr std::uintptr_t kLocked = 1;
    static constexpr std::uintptr_t kWoken = 2;
    static constexpr std::uintptr_t kBits = kLocked | kWoken;
    static_assert(alignof(WaitNode) > kBits, "WaitNode pointers need two free low bits");

    static WaitNode* head_of(std::uintptr_t s) noexcept {
        return reinterpret_cast<WaitNode*>(s & ~kBits);
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
