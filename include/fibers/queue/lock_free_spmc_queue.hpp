#pragma once

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <type_traits>

#include "fibers/sync/sync.hpp"
#include "fibers/thread_pool/task.hpp"

namespace fibers::queue {

namespace detail {

// One ring slot kept in atomic words, so that consumers may read it
// speculatively while the producer overwrites it (see LockFreeSPMCQueue).
// Raw is the slot content without ownership semantics.
//
// Primary template: trivially copyable values of at most one word.
template <class T, class Sync>
struct SpmcSlot {
    static_assert(std::is_trivially_copyable_v<T> && sizeof(T) <= sizeof(std::uint64_t),
                  "LockFreeSPMCQueue<T>: T must be Task or a trivially copyable word");
    using Raw = T;

    static Raw release(T& value) noexcept { return value; }
    // out := raw, as an owning value (raw came from a successful claim or
    // from a speculative read that may be abandoned via unstage).
    static void stage(T& out, Raw raw) noexcept { out = raw; }
    static void unstage(T&) noexcept {}
    static void drop(Raw) noexcept {}

    void store(Raw raw) noexcept { value.store(raw, std::memory_order_relaxed); }
    Raw load() const noexcept { return value.load(std::memory_order_relaxed); }

    typename Sync::template Atomic<T> value{T{}};
};

// Task: its three fields, each in its own atomic word. A speculative read may
// combine fields of two different tasks; such a copy is never used.
template <class Sync>
struct SpmcSlot<thread_pool::Task, Sync> {
    using Raw = thread_pool::Task::Raw;

    static Raw release(thread_pool::Task& task) noexcept { return task.release(); }
    static void stage(thread_pool::Task& out, Raw raw) noexcept { out = thread_pool::Task::from_raw(raw); }
    // Abandons a speculative copy: empties `out` without calling delete_ptr.
    static void unstage(thread_pool::Task& out) noexcept { static_cast<void>(out.release()); }
    static void drop(Raw raw) noexcept { thread_pool::Task::from_raw(raw).reset(); }

    void store(Raw raw) noexcept {
        memo.store(raw.memo, std::memory_order_relaxed);
        call_ptr.store(raw.call_ptr, std::memory_order_relaxed);
        delete_ptr.store(raw.delete_ptr, std::memory_order_relaxed);
    }
    Raw load() const noexcept {
        return Raw{memo.load(std::memory_order_relaxed), call_ptr.load(std::memory_order_relaxed),
                   delete_ptr.load(std::memory_order_relaxed)};
    }

    typename Sync::template Atomic<void*> memo{nullptr};
    typename Sync::template Atomic<thread_pool::Task::CallFn> call_ptr{nullptr};
    typename Sync::template Atomic<thread_pool::Task::DeleteFn> delete_ptr{nullptr};
};

// Optional checker hook of a Sync policy (TwistSync provides it under the
// Twist simulator):
//   HbProbe       write() / read() are plain, non-atomic accesses to a dummy
//                 cell that the checker tracks for happens-before races;
//   NoPreemption  RAII guard: the current thread is not preempted inside.
//
// Slot contents are atomics (racy by design), and a checker that gives every
// atomic load the latest value (the Twist simulator does) cannot see a missing
// release / acquire on them. So each slot also has a probe: the producer
// write()s it before publishing, the consumer read()s it right before a CAS
// that will succeed. A missing edge in either direction (publication or slot
// reuse) then shows up as a data race on the probe.
template <class Sync>
concept HasHbProbe = requires {
    typename Sync::HbProbe;
    typename Sync::NoPreemption;
};

struct NoHbProbe {
    void write() noexcept {}
    void read() const noexcept {}
};

template <class Sync>
struct HbProbeOf {
    using type = NoHbProbe;
};

template <HasHbProbe Sync>
struct HbProbeOf<Sync> {
    using type = typename Sync::HbProbe;
};

}  // namespace detail

// Lock-free implementation of a worker's main_box (Go runq / Tokio local
// queue style). Satisfies MainQueue for T = Task; T may also be a trivially
// copyable word (used in tests).
//
// Bounded ring (capacity rounded up to a power of two) with two monotonic
// 64-bit counters, `head` (next item to take) and `tail` (next free slot).
// They never wrap in practice, so there is no ABA on head.
//
// * One producer: the owning worker. It writes slots [tail, tail + n) and
//   publishes them with one store to `tail`. No read-modify-write on push.
// * Many consumers: the owner (try_pop_batch) and thieves (try_steal_batch)
//   run the same algorithm. Read head and tail, copy the slots of
//   [head, head + n) (n <= max), then claim them with one CAS head -> head + n.
//   If the CAS fails, somebody else took (some of) those items, and the
//   copies are abandoned without running any destructor / delete_ptr.
//
// The copies are speculative: by the time a consumer reads a slot, the
// producer may already be overwriting it (that consumer's CAS will then fail).
// So slots are atomics accessed with relaxed order; ordering comes from head
// and tail only.
//
// Memory orders
// -------------
// (1) Publication: producer writes slot i (relaxed) -> tail.store(i + 1,
//     release) ... consumer tail.load(acquire) sees >= i + 1 -> reads slot i.
//     The release/acquire pair makes the slot write (and everything the
//     producer did before, e.g. building the task's memo) happen-before the
//     consumer's read and, after a successful claim, the task's execution.
//
// (2) Slot reuse (write-after-read): consumer reads slot i -> CAS head from
//     h <= i to h + n > i (success: RELEASE) ... producer head.load(acquire)
//     sees >= h + n -> producer overwrites slot i (as index i + capacity).
//     The CAS is the only way head moves past i and every later CAS is an RMW
//     (so it continues the release sequence), so whichever value >= h + n the
//     producer sees, the consumer's slot reads happen-before the overwrite and
//     cannot observe it (read-write coherence). Without release, the read
//     could be satisfied after the CAS became visible and return the new task:
//     one task taken twice, another lost.
//
// (3) Sane snapshots: consumers load head (acquire) BEFORE tail (acquire).
//     Whoever wrote that head value had read a tail >= it before its release
//     CAS, so the tail we load afterwards is not older: tail - head >= 0. It
//     may exceed capacity when our head is stale (the producer has since
//     reused slots); then we reload head instead of copying garbage. For the
//     same reason a failed CAS reloads head with acquire. Success is acq_rel:
//     release is what (2) needs; the acquire half is not needed by this
//     algorithm but is free on x86 and keeps each claim ordered after the
//     previous one.
//
// (4) The producer reads head with acquire (needed by (2)) and caches it:
//     head only grows, so the cached value under-estimates free space, and the
//     ordering in (2) was established by the load that produced the cache.
//     free_space() is therefore an exact lower bound: it can only grow until
//     the owner pushes again.
//
// The owner retries a failed claim until it succeeds or the queue is empty
// (each failure means another consumer made progress). Thieves give up after
// kStealAttempts failed claims, so they never spin on a contended victim.
template <class T = thread_pool::Task, sync::SyncPolicy Sync = sync::StdSync>
class LockFreeSPMCQueue {
    using Slot = detail::SpmcSlot<T, Sync>;
    using Raw = typename Slot::Raw;
    using Probe = typename detail::HbProbeOf<Sync>::type;

public:
    static constexpr std::size_t kStealAttempts = 4;

    explicit LockFreeSPMCQueue(std::size_t capacity)
        : capacity_(std::bit_ceil(std::max<std::size_t>(capacity, 1))),
          mask_(capacity_ - 1),
          cells_(std::make_unique<Cell[]>(capacity_)) {}

    LockFreeSPMCQueue(const LockFreeSPMCQueue&) = delete;
    LockFreeSPMCQueue& operator=(const LockFreeSPMCQueue&) = delete;

    // Drops the items still queued. Requires exclusive access.
    ~LockFreeSPMCQueue() {
        const std::uint64_t head = head_.value.load(std::memory_order_acquire);
        const std::uint64_t tail = tail_.value.load(std::memory_order_acquire);
        for (std::uint64_t i = head; i != tail; ++i) {
            Slot::drop(cells_[i & mask_].slot.load());
        }
    }

    std::size_t capacity() const noexcept { return capacity_; }

    // ---- owner, producer side ----

    // Moves from `value` only on success.
    bool try_push(T&& value) noexcept {
        const std::uint64_t tail = tail_.value.load(std::memory_order_relaxed);  // own counter
        if (tail - tail_.cached_head >= capacity_) {
            tail_.cached_head = head_.value.load(std::memory_order_acquire);  // (4)
            if (tail - tail_.cached_head >= capacity_) {
                return false;
            }
        }
        write(tail, Slot::release(value));
        tail_.value.store(tail + 1, std::memory_order_release);  // (1)
        return true;
    }

    // Moves the first k items (k is returned) from `items`.
    std::size_t try_push_batch(T* items, std::size_t count) noexcept {
        const std::uint64_t tail = tail_.value.load(std::memory_order_relaxed);
        std::size_t free = capacity_ - static_cast<std::size_t>(tail - tail_.cached_head);
        if (free < count) {
            tail_.cached_head = head_.value.load(std::memory_order_acquire);  // (4)
            free = capacity_ - static_cast<std::size_t>(tail - tail_.cached_head);
        }
        const std::size_t n = std::min(free, count);
        for (std::size_t i = 0; i < n; ++i) {
            write(tail + i, Slot::release(items[i]));
        }
        if (n != 0) {
            tail_.value.store(tail + n, std::memory_order_release);  // (1)
        }
        return n;
    }

    // Exact lower bound: free space can only grow until the owner pushes.
    std::size_t free_space() noexcept {
        tail_.cached_head = head_.value.load(std::memory_order_acquire);  // (4)
        const std::uint64_t tail = tail_.value.load(std::memory_order_relaxed);
        return capacity_ - static_cast<std::size_t>(tail - tail_.cached_head);
    }

    // ---- owner, consumer side ----

    // Both pops stage speculative copies in out[0..max) (move-assigned, as
    // in Go's runqgrab): pass empty tasks there. A task left in out[0..max)
    // may be dropped even when nothing is returned.
    std::size_t try_pop_batch(T* out, std::size_t max) noexcept {
        return take(out, max, std::numeric_limits<std::size_t>::max());
    }

    // ---- any thread ----

    // Takes a batch of up to `max` items. Never blocks; may spuriously return
    // 0 after kStealAttempts lost races.
    std::size_t try_steal_batch(T* out, std::size_t max) noexcept {
        return take(out, max, kStealAttempts);
    }

    std::size_t size_approx() const noexcept {
        const std::uint64_t head = head_.value.load(std::memory_order_acquire);
        const std::uint64_t tail = tail_.value.load(std::memory_order_acquire);
        const std::uint64_t size = tail - head;
        return size > capacity_ ? 0 : static_cast<std::size_t>(size);
    }

    bool empty_approx() const noexcept { return size_approx() == 0; }

private:
    struct Cell {
        Slot slot;
        [[no_unique_address]] Probe probe;
    };

    void write(std::uint64_t index, Raw raw) noexcept {
        Cell& cell = cells_[index & mask_];
        cell.slot.store(raw);
        cell.probe.write();
    }

    std::size_t take(T* out, std::size_t max, std::size_t attempts) noexcept {
        if (max == 0) {
            return 0;
        }
        std::uint64_t head = head_.value.load(std::memory_order_acquire);  // (3)
        for (;;) {
            const std::uint64_t tail = tail_.value.load(std::memory_order_acquire);  // (1), (3)
            const std::uint64_t size = tail - head;
            if (size == 0) {
                return 0;
            }
            if (size > capacity_) {
                // Stale head: those slots have been reused. Not a claim attempt
                // yet, but bounded the same way.
                if (--attempts == 0) {
                    return 0;
                }
                head = head_.value.load(std::memory_order_acquire);
                continue;
            }
            const std::size_t n = std::min(static_cast<std::size_t>(size), max);
            for (std::size_t i = 0; i < n; ++i) {
                Slot::stage(out[i], cells_[(head + i) & mask_].slot.load());
            }
            if (claim(head, n)) {
                return n;
            }
            // Lost the race: the copies own nothing.
            for (std::size_t i = 0; i < n; ++i) {
                Slot::unstage(out[i]);
            }
            if (--attempts == 0) {
                return 0;
            }
        }
    }

    // CAS head: head -> head + n. On failure `head` gets the current value.
    bool claim(std::uint64_t& head, std::size_t n) noexcept {
        if constexpr (detail::HasHbProbe<Sync>) {
            // Checker only: read the probes "together with" a CAS known to
            // succeed (no preemption in between), so they are ordered by the
            // CAS exactly like the speculative slot reads before it.
            typename Sync::NoPreemption atomically;
            if (head_.value.load(std::memory_order_relaxed) == head) {
                for (std::size_t i = 0; i < n; ++i) {
                    cells_[(head + i) & mask_].probe.read();
                }
            }
            return cas_head(head, n);
        } else {
            return cas_head(head, n);
        }
    }

    bool cas_head(std::uint64_t& head, std::size_t n) noexcept {
        return head_.value.compare_exchange_strong(head, head + n,
                                                   std::memory_order_acq_rel,  // (2), (3)
                                                   std::memory_order_acquire);  // (3)
    }

    struct alignas(sync::kCacheLineSize) Head {
        typename Sync::template Atomic<std::uint64_t> value{0};
    };

    struct alignas(sync::kCacheLineSize) Tail {
        typename Sync::template Atomic<std::uint64_t> value{0};
        std::uint64_t cached_head = 0;  // producer-local, <= head
    };

    const std::size_t capacity_;
    const std::size_t mask_;
    std::unique_ptr<Cell[]> cells_;

    Head head_;
    Tail tail_;
};

}  // namespace fibers::queue
