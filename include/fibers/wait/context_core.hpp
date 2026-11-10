#pragma once

// ContextCore: the waiter-agnostic heart of a request context
// (fiber::BasicContext, fibers/fiber/context.hpp): a cancel state and an
// intrusive list of Interrupt hooks ("how to interrupt me") of everything
// that currently depends on it: waits parked under it, and child contexts.
// See docs/cancellation.md for the whole design.
//
// State
// -----
//   state_  = 0 (live) | Error::kCancelled | Error::kTimedOut; set once,
//             under lock_ (first cancel wins), read without it
//   lock_   SpinLock: the list and every node's `linked` flag (O(1) list
//           operations only; no hook ever runs under it)
//   list    doubly linked Interrupt nodes (head_, tail_)
//
//   enlist(h):  lock; state != 0 -> unlock, false (the caller interrupts
//               itself: runs h.fn); else append h (linked, done = 0),
//               unlock, true
//   delist(h):  lock; h.linked -> unlink, unlock, return (no canceller saw
//               it); else unlock and spin until h.done (acquire): a
//               canceller popped h and may still be inside h.fn
//   cancel(r):  lock; state != 0 -> unlock, false
//               state = r (release)
//               loop: pop the head h (linked = false), unlock,
//                     h.fn(h.self, r), h.done = 1 (release, last touch),
//                     lock
//               unlock, true
//
// Why the hooks run outside the lock: a hook claims a wait and wakes a
// fiber (a pool submit), or cancels a whole child context; holding the lock
// meanwhile would make every fiber woken by the walk spin behind the rest of
// it, and nest the child's lock inside the parent's. With the pop-then-call
// walk no lock is ever nested, and a woken fiber waits (in delist) only for
// its own hook to return.
//
// Exactly one canceller walks the list (the 0 -> reason transition is
// under the lock), so every node is popped at most once, and once state_ is
// set enlist() refuses: a node is either walked or refused, never missed.
//
// Contract for a hook's owner (a waiter, a child):
//   * enlist() only after the wait is published (armed / queued), so that
//     the hook finds something to claim; if enlist() returns false, run the
//     hook yourself (h.fn(h.self, reason())).
//   * call delist() (after a successful enlist) before the node, or
//     anything its hook touches, goes away; delist() may spin until a
//     concurrent hook call on it returns.
//   * the hook must not block and must not call back into this context.
//
// Lifetime: the destructor fails fast while any node is enlisted; a cancel()
// must have returned before the destructor runs (the caller's business:
// whoever calls cancel() must be done with the context, like Event::set()).
//
// Memory orders
// -------------
//   * state_: cancel()'s store is a release (it is under the lock, but
//     cancelled() / reason() read it without the lock): whatever the
//     canceller did before cancel() is visible to a party that observes the
//     cancellation with acquire. enlist() reads it under the lock (relaxed:
//     the lock orders it against cancel()'s store).
//   * list, prev / next, linked: plain, under lock_ (SpinLock acquire /
//     release). A node's fields (self, fn) are written before enlist()'s
//     lock, so the canceller reads them after its own lock.
//   * done: the canceller's store (release) is its last touch of the node;
//     delist()'s load (acquire) reads it, so every access the hook made (to
//     the node, and to the waiter's frame that the node points at, e.g. a
//     claim CAS that lost) happens-before the owner reuses that memory.
// Checked under Twist (tests/fiber/twist/context_twist_test.cpp).

#include <cstdint>
#include <mutex>
#include <optional>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/sync/spin_lock.hpp"
#include "fibers/sync/sync.hpp"
#include "fibers/wait/result.hpp"

namespace fibers::wait {

// One dependant of a context: a parked wait or a child context. Lives with
// its owner (a fiber's stack, the child context). No virtual functions:
// self + a plain function pointer.
template <sync::SyncPolicy Sync = sync::StdSync>
struct Interrupt {
    using Fn = void (*)(void* self, Error reason) noexcept;

    Interrupt() noexcept = default;
    Interrupt(void* self, Fn fn) noexcept : self(self), fn(fn) {}
    Interrupt(const Interrupt&) = delete;
    Interrupt& operator=(const Interrupt&) = delete;

    void* self = nullptr;
    Fn fn = nullptr;

    // Owned by the context the node is enlisted in (under its lock).
    Interrupt* prev = nullptr;
    Interrupt* next = nullptr;
    bool linked = false;
    // 1 once a canceller that popped the node is done with it.
    typename Sync::template Atomic<std::uint32_t> done{0};
};

template <sync::SyncPolicy Sync = sync::StdSync>
class ContextCore {
public:
    using Node = Interrupt<Sync>;

    ContextCore() noexcept = default;
    ContextCore(const ContextCore&) = delete;
    ContextCore& operator=(const ContextCore&) = delete;

    ~ContextCore() {
        std::lock_guard guard(lock_);
        if (head_ != nullptr) [[unlikely]] {
            fail::fail_fast("context: destroyed while a fiber waits under it or a child context exists");
        }
    }

    // Ends the context with `reason` (kCancelled / kTimedOut) and runs every
    // enlisted hook once, on the calling thread. false: it had ended already
    // (the first reason stays). Any thread.
    bool cancel(Error reason) noexcept {
        lock_.lock();
        if (state_.load(std::memory_order_relaxed) != 0) {
            lock_.unlock();
            return false;
        }
        // release: see "Memory orders" (readers do not take the lock).
        state_.store(static_cast<std::uint32_t>(reason), std::memory_order_release);
        while (Node* h = head_) {
            unlink_locked(*h);
            lock_.unlock();
            const typename Node::Fn fn = h->fn;
            fn(h->self, reason);
            // release: the hook's accesses before the owner's delist().
            h->done.store(1, std::memory_order_release);  // last touch of h
            lock_.lock();
        }
        lock_.unlock();
        return true;
    }

    // true: `h` (self / fn set) is enlisted; a later cancel() runs its hook
    // (unless delist() comes first). false: the context has ended; nothing
    // was enlisted, the caller runs the hook itself if it must.
    bool enlist(Node& h) noexcept {
        std::lock_guard guard(lock_);
        if (state_.load(std::memory_order_relaxed) != 0) {  // under the lock
            return false;
        }
        h.done.store(0, std::memory_order_relaxed);  // read only after a pop (lock)
        h.linked = true;
        h.next = nullptr;
        h.prev = tail_;
        (tail_ != nullptr ? tail_->next : head_) = &h;
        tail_ = &h;
        return true;
    }

    // After a successful enlist(h): takes h out, or waits until the
    // canceller that popped it has finished its hook.
    void delist(Node& h) noexcept {
        {
            std::lock_guard guard(lock_);
            if (h.linked) {
                unlink_locked(h);
                return;
            }
        }
        // acquire: the hook's accesses happen-before our return.
        while (h.done.load(std::memory_order_acquire) == 0) {
            Sync::spin_hint();  // the canceller is inside h.fn (short)
        }
    }

    // Ended (cancelled or deadline). acquire: orders the caller after the
    // canceller's writes before cancel().
    bool cancelled() const noexcept { return state_.load(std::memory_order_acquire) != 0; }

    // Why it ended; nullopt while live. acquire, as cancelled().
    std::optional<Error> reason() const noexcept {
        const std::uint32_t s = state_.load(std::memory_order_acquire);
        if (s == 0) {
            return std::nullopt;
        }
        return static_cast<Error>(s);
    }

    // Racy snapshot (tests, diagnostics): something is enlisted.
    bool has_dependants_approx() noexcept {
        std::lock_guard guard(lock_);
        return head_ != nullptr;
    }

private:
    void unlink_locked(Node& h) noexcept {
        (h.prev != nullptr ? h.prev->next : head_) = h.next;
        (h.next != nullptr ? h.next->prev : tail_) = h.prev;
        h.prev = h.next = nullptr;
        h.linked = false;
    }

    typename Sync::template Atomic<std::uint32_t> state_{0};
    sync::SpinLock<Sync> lock_;
    Node* head_ = nullptr;  // under lock_
    Node* tail_ = nullptr;  // under lock_
};

}  // namespace fibers::wait
