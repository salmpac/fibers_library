#pragma once

// SemaphoreCore: a counting semaphore with multi-permit acquire, over
// WaitNode, waiter-agnostic. Front-ends (fibers/fiber/semaphore.hpp for
// stackful fibers) add only "how to wait for the node's wake".
//
// Fairness: FIFO with direct hand-off, not barging (unlike MutexCore).
// While anybody waits, nobody takes permits past the queue: try_acquire()
// fails and acquire() queues behind, even if enough permits are free for
// it. release() hands its permits to the waiters at the head of the queue
// itself (they are subtracted from the count on the waiter's behalf, in the
// releaser's own RMW), and the woken waiter returns without another attempt.
// A head that needs more permits than are free blocks everybody behind it
// (head-of-line blocking): permits accumulate for it until it fits.
// Why not barging like the Mutex:
//   * multi-permit acquire. With barging an acquire(5) waits for five free
//     permits at one moment; a stream of acquire(1)s that each take a permit
//     the moment it is released never lets five accumulate, so it can wait
//     forever (and if the acquire(1)s in turn wait for the acquire(5)'s
//     fiber, that is a deadlock). FIFO bounds every wait by the permits held
//     by those ahead of it.
//   * what a semaphore guards: connections, in-flight requests, memory
//     budgets, i.e. resources held for a while (I/O), where bounded,
//     arrival-ordered waiting matters more than the hand-off latency.
//     Barging wins for short critical sections (the Mutex's case), where the
//     woken waiter would otherwise convoy.
//   * no wake/park churn: every wake is a grant, never "woken, overtaken,
//     park again"; one wake per publication of a node, like EventCore.
// The cost: under contention every acquire takes the lock and parks, and a
// released permit is idle until its new holder runs (one wake + schedule).
// Uncontended (nobody waits) acquire and release are one RMW each.
//
// State
// -----
//   state_ = [ permits : 63 | WAITERS (bit 0) ]   (permits in units of kOne = 2)
//   queue_ = intrusive FIFO of SemaphoreNode (head_, tail_), under lock_
//            (a SpinLock: queue updates only, O(1) per waiter, no waiting
//            and no syscalls while held; like PollDesc's deadline lock).
//   waiters_ = the queue's length: written only under lock_ (relaxed
//            stores), read without it (waiters_approx(), relaxed). A
//            diagnostic mirror: no decision depends on it, it carries no
//            data, so it needs no order.
//
//   Invariant (outside lock_): WAITERS set <=> queue nonempty, and then
//   permits < head.need. WAITERS changes only under lock_, in an RMW of
//   state_; while it is set, permits change only under lock_ too (both fast
//   paths back off), so a waiter is never left with enough free permits.
//
//   try_acquire(n):                                       fast path, no lock
//     s = load (relaxed); loop:
//       WAITERS or permits(s) < n -> false
//       CAS(s -> s - n, acquire)  -> true
//   acquire_or_enqueue(node, n):
//     try_acquire(n) -> true
//     lock; s = load (relaxed); loop:
//       WAITERS        -> append node (FIFO behind the others), unlock, false
//       permits >= n   -> CAS(s -> s - n, acquire): unlock, true (released meanwhile)
//       else           -> CAS(s -> s | WAITERS, relaxed): append node,
//                         unlock, false: the caller waits for node's wake,
//                         after which it holds its n permits
//   release(n):
//     s = load (relaxed); loop while !WAITERS:
//       CAS(s -> s + n, release)  -> done                fast path, no lock
//     WAITERS: lock; s = load (relaxed); loop:
//       avail = permits(s) + n
//       grant heads in order while head.need <= what is left of avail
//       CAS(s -> (avail - granted) | (queue left nonempty ? WAITERS : 0), acq_rel)
//     unlink the granted nodes, unlock, notify each (next read BEFORE the
//     notify: the woken waiter may go on at once and its node vanish)
//
// Why no wake is lost: every decision "wait or not" and "is anybody
// waiting" is made on the one word state_, by RMWs, and WAITERS is set in
// the same lock_ section that appends the node. A release whose CAS comes
// before the enqueuer's WAITERS CAS added its permits first: the enqueuer's
// CAS fails, it re-reads and takes them if they are enough. A release after
// it sees WAITERS and takes the lock, so it runs after the node is in the
// queue. No Dekker pattern, no fence. A release that adds permits while
// WAITERS is set does it under the lock and hands out in the same section,
// so the invariant holds at every unlock.
//
// Why release() adds under the lock (and not fetch_add first, then lock):
// lifetime. With fetch_add first, a release preempted between its add and
// its lock could find the semaphore destroyed: another release's hand-off
// may already have woken a waiter that used those very permits and then
// destroyed the semaphore (the latch pattern: N fibers release(1), one
// acquire(N), then the semaphore goes). Adding under the lock makes every
// release's last touch of the semaphore come before any wake that uses its
// permits: its CAS (fast path), or its unlock (slow path; the notifies after
// it touch only nodes). So a waiter may destroy the semaphore as soon as
// its acquire has returned, provided no other call is still in progress
// that its own acquire did not depend on. Destroying it with waiters fails
// fast.
//
// Memory orders
// -------------
// Required: everything sequenced before a release(n) happens-before every
// return from an acquire that obtained one of its permits (and, by
// transitivity, before the permit's next holder).
//   * every write of state_ is an RMW, so every release's add heads a
//     release sequence that every later read of state_ continues.
//   * fast-path release (CAS, release) -> fast-path acquire (CAS, acquire)
//     or the locked attempt (CAS, acquire): the acquire RMW reads a value
//     in the release's sequence: synchronises with it.
//   * hand-off: the slow release's CAS is acq_rel. Acquire: it hands out
//     permits that other releases added, maybe on the fast path before
//     WAITERS was set (the waiter's WAITERS CAS is relaxed and so does not
//     synchronise with them); reading their sequence synchronises the
//     dispatcher with them, the notify is sequenced after it, and the wake
//     (front-end) provides the release / acquire to the woken waiter
//     (WaitNode contract). Release: what is left after the grants (and the
//     clearing of WAITERS) is read by later fast-path acquirers, which need
//     this release's writes.
//   * queue and nodes (next, need, and whatever the front-end put into the
//     node) travel through lock_ (acquire / release); the WAITERS CAS needs
//     no order of its own: the only thing it announces is "take the lock".
//   * try_acquire's failure and the first loads are relaxed: they decide
//     nothing on their own, every decision is a CAS that re-reads.
//
// Removable waits (timeouts / cancellation; not implemented, the layout is
// ready for it): a waiter that stops waiting takes the lock and looks for
// its node in the queue. Found: unlink it, clear WAITERS if the queue is now
// empty, and run the hand-out (the head may have changed, and permits held
// back for the old head may fit the new one). Not found: it was granted;
// its wake is on the way and must be consumed (park once) before the node
// may go, and the caller then holds the permits (or releases them). A
// doubly linked queue would make the unlink O(1).
//
// Checked under Twist (tests/semaphore/twist): weakening any one of the
// orders above to relaxed (or either half of the acq_rel) makes a simulator
// test report a race on a payload cell.

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <type_traits>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/sync/spin_lock.hpp"
#include "fibers/sync/sync.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fibers::wait {

// A waiter of a SemaphoreCore: the WaitNode (next links the queue; self /
// wake set by the front-end) and how many permits it asks for. Lives where
// the waiter lives (a fiber's stack, a coroutine frame).
struct SemaphoreNode {
    WaitNode node;
    std::uint64_t need = 0;  // written by acquire_or_enqueue

    static SemaphoreNode* from(WaitNode* n) noexcept { return reinterpret_cast<SemaphoreNode*>(n); }
};

static_assert(std::is_standard_layout_v<SemaphoreNode>, "SemaphoreNode::from casts from its first member");

template <sync::SyncPolicy Sync = sync::StdSync>
class SemaphoreCore {
public:
    // Largest number of permits the semaphore can hold.
    static constexpr std::uint64_t kMaxPermits = (std::uint64_t{1} << 63) - 1;

    explicit SemaphoreCore(std::uint64_t initial = 0) noexcept {
        if (initial > kMaxPermits) [[unlikely]] {
            fail::fail_fast("wait: Semaphore initial count too large");
        }
        state_.store(initial * kOne, std::memory_order_relaxed);
    }

    SemaphoreCore(const SemaphoreCore&) = delete;
    SemaphoreCore& operator=(const SemaphoreCore&) = delete;

    ~SemaphoreCore() {
        if ((state_.load(std::memory_order_relaxed) & kWaiters) != 0 || head_ != nullptr) [[unlikely]] {
            fail::fail_fast("wait: destroying a semaphore that has waiters");
        }
    }

    // Takes n permits if they are free and nobody waits. One RMW, any
    // thread. n == 0: true.
    bool try_acquire(std::uint64_t n = 1) noexcept {
        check_count(n);
        if (n == 0) [[unlikely]] {
            return true;  // never queues (it would wait behind the head for nothing)
        }
        std::uint64_t s = state_.load(std::memory_order_relaxed);
        for (;;) {
            if ((s & kWaiters) != 0 || permits_of(s) < n) {
                return false;
            }
            if (state_.compare_exchange_weak(s, s - n * kOne, std::memory_order_acquire,
                                             std::memory_order_relaxed)) {
                return true;
            }
        }
    }

    // true: n permits acquired. false: `node` (self / wake must be set; next
    // and need are overwritten) was queued; the caller must not touch it
    // until its wake, which comes exactly once, and then holds the n
    // permits. After a false the node may be woken (and its owner resumed)
    // before this returns.
    bool acquire_or_enqueue(SemaphoreNode& node, std::uint64_t n = 1) noexcept {
        if (try_acquire(n)) [[likely]] {
            return true;
        }
        std::lock_guard guard(lock_);
        std::uint64_t s = state_.load(std::memory_order_relaxed);
        for (;;) {
            if ((s & kWaiters) != 0) {
                break;  // behind the others; WAITERS only changes under the lock
            }
            if (permits_of(s) >= n) {
                if (state_.compare_exchange_weak(s, s - n * kOne, std::memory_order_acquire,
                                                 std::memory_order_relaxed)) {
                    return true;  // released between try_acquire and here
                }
                continue;
            }
            // relaxed: WAITERS only says "take the lock"; the node goes out
            // through the lock.
            if (state_.compare_exchange_weak(s, s | kWaiters, std::memory_order_relaxed,
                                             std::memory_order_relaxed)) {
                break;
            }
        }
        node.need = n;
        append(node);
        return false;
    }

    // Returns n permits and hands them to the waiters at the head of the
    // queue (as many as fit, in order). One RMW when nobody waits. Any
    // thread; n == 0: no-op. Wakes run on the calling thread, after the
    // lock is dropped: the semaphore is not touched after them.
    void release(std::uint64_t n = 1) noexcept {
        check_count(n);
        if (n == 0) {
            return;
        }
        std::uint64_t s = state_.load(std::memory_order_relaxed);
        while ((s & kWaiters) == 0) {
            check_overflow(s, n);
            if (state_.compare_exchange_weak(s, s + n * kOne, std::memory_order_release,
                                             std::memory_order_relaxed)) {
                return;
            }
        }
        release_slow(n);
    }

    // Racy snapshots (tests, diagnostics).
    std::uint64_t available_approx() const noexcept { return permits_of(state_.load(std::memory_order_relaxed)); }
    bool has_waiters_approx() const noexcept { return (state_.load(std::memory_order_relaxed) & kWaiters) != 0; }
    // How many waiters are queued (enqueued and not yet granted).
    std::uint64_t waiters_approx() const noexcept { return waiters_.load(std::memory_order_relaxed); }

private:
    static constexpr std::uint64_t kWaiters = 1;
    static constexpr std::uint64_t kOne = 2;

    static constexpr std::uint64_t permits_of(std::uint64_t s) noexcept { return s / kOne; }

    static void check_count(std::uint64_t n) noexcept {
        if (n > kMaxPermits) [[unlikely]] {
            fail::fail_fast("wait: Semaphore permit count too large");
        }
    }

    static void check_overflow(std::uint64_t s, std::uint64_t n) noexcept {
        if (permits_of(s) > kMaxPermits - n) [[unlikely]] {
            fail::fail_fast("wait: Semaphore permit count overflow");
        }
    }

    [[gnu::noinline]] void release_slow(std::uint64_t n) noexcept {
        WaitNode* granted = nullptr;  // the granted prefix of the queue, in order
        {
            std::lock_guard guard(lock_);
            std::uint64_t s = state_.load(std::memory_order_relaxed);
            WaitNode* rest = nullptr;
            for (;;) {
                check_overflow(s, n);
                std::uint64_t avail = permits_of(s) + n;
                // The queue cannot change under the lock: recomputing after a
                // failed CAS (a fast path changed the count) walks the same
                // nodes.
                rest = head_;
                while (rest != nullptr && SemaphoreNode::from(rest)->need <= avail) {
                    avail -= SemaphoreNode::from(rest)->need;
                    rest = rest->next;
                }
                const std::uint64_t desired = avail * kOne | (rest != nullptr ? kWaiters : 0);
                if (state_.compare_exchange_weak(s, desired, std::memory_order_acq_rel,
                                                 std::memory_order_relaxed)) {
                    break;
                }
            }
            if (rest != head_) {
                granted = head_;
                // Cut the granted prefix off the queue.
                WaitNode* last = granted;
                std::uint64_t count = 1;
                while (last->next != rest) {
                    last = last->next;
                    ++count;
                }
                waiters_.store(waiters_.load(std::memory_order_relaxed) - count, std::memory_order_relaxed);
                last->next = nullptr;
                head_ = rest;
                if (rest == nullptr) {
                    tail_ = nullptr;
                }
            }
        }
        while (granted != nullptr) {
            WaitNode* const next = granted->next;  // before notify: the node may vanish
            granted->notify();
            granted = next;
        }
    }

    // Under lock_.
    void append(SemaphoreNode& node) noexcept {
        node.node.next = nullptr;
        if (tail_ == nullptr) {
            head_ = &node.node;
        } else {
            tail_->next = &node.node;
        }
        tail_ = &node.node;
        waiters_.store(waiters_.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
    }

    typename Sync::template Atomic<std::uint64_t> state_{0};
    sync::SpinLock<Sync> lock_;
    WaitNode* head_ = nullptr;  // under lock_
    WaitNode* tail_ = nullptr;  // under lock_
    typename Sync::template Atomic<std::uint64_t> waiters_{0};  // written under lock_, read racily
};

}  // namespace fibers::wait
