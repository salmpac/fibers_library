#pragma once

// AtomicPointer: a versioned, slot-indirected atomic value with transactions.
// Adapted from the user's atomic_pointer_cpp prototype for library use.
//
// The current value lives in a slot of a fixed table; the 64-bit state word
// holds [slot:16][version:48]. A transaction
//   1. reads the state and pins the current slot (per-slot refcount),
//   2. builds the new value with `maker(current, next)`,
//   3. writes it into a spare slot it owns and CASes the state to
//      (spare, version + 1).
// Every commit bumps the version, so a successful CAS proves that no other
// commit happened in between: this, not the refcount, is the ABA guard.
// The refcount decides who recycles a slot (and, generically, who reclaims the
// old value: the last holder runs Reclaim). Slots are recycled through a
// Ticketor (a bag of free slot numbers).
//
// Changes against the prototype:
//   - `maker` and Reclaim are template parameters (no std::function, no
//     allocation per transaction);
//   - all atomics and spin hints come from Sync (Twist-checkable); slot-bag
//     scans start at a sticky per-thread cursor owned by the caller (seeded
//     from Sync::random_seed) instead of a thread_local std::mt19937 draw;
//   - the table size is an explicit constructor argument (max_threads);
//   - a spare slot is reserved once per transaction and kept across failed
//     CAS attempts (the prototype took and returned a ticket per attempt);
//   - no hold-and-wait: if the table is exhausted while a slot is pinned, the
//     pin is dropped before waiting for a free slot, so more than max_threads
//     concurrent users can only spin, never deadlock;
//   - pin retries while the refcount is positive instead of failing the whole
//     transaction on a lost refcount CAS;
//   - after pinning, the state is re-read: the maker only ever sees the value
//     that was current at that moment (stale pins retry early instead of
//     building a doomed value from a recycled slot);
//   - the two releases of the old slot after a commit are one fetch_sub(2);
//   - waits call Sync::spin_hint().
//
// Cost per committed pinned transaction (uncontended): state load, pin CAS,
// state re-load, ticket take (exchange), spare refcount store, commit CAS,
// unpin fetch_sub, and when the old slot dies a ticket return CAS: about six
// read-modify-writes, against one CAS for a tagged-pointer CAS loop.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <type_traits>

#include "thread_pool/sync.hpp"

namespace object_pool {

using thread_pool::kCacheLineSize;
using thread_pool::SyncPolicy;

// How a transaction reads the current value before running its maker.
enum class ReadMode {
    // Pin the current slot (refcount) and re-check the state: the prototype's
    // protocol. The value stays alive (no Reclaim) while the maker runs.
    kPinned,
    // Read the slot, then re-check the state: the maker sees a value that was
    // current at that moment (published, happens-before established), but
    // nothing keeps it alive afterwards. Two RMWs cheaper than kPinned; only
    // for NoReclaim (values whose referents are never freed, e.g. type-stable
    // pool nodes).
    kValidated,
    // Read the slot, no re-check: the maker may see a stale value and must
    // not dereference it (e.g. a push that only stores it into its own node).
    // The commit CAS still rejects any transaction that raced with a commit.
    kRaw,
};

struct NoReclaim {
    template <class V>
    void operator()(const V&) const noexcept {}
};

// Bag of free slot numbers ("tickets") 1..tickets spread over more buckets
// than tickets, so both taking and returning usually succeed within a probe or
// two. Scans start at the caller's per-thread cursor and leave it where they
// succeeded: a thread returns a slot to the bucket it last took one from and
// takes it back next time, so it keeps reusing a few cache-hot buckets and
// slots (the prototype started every scan at a random bucket, which made each
// transaction touch several cold cache lines). Distinct threads start at
// distinct random cursors.
template <SyncPolicy Sync>
class Ticketor {
public:
    using Ticket = std::uint32_t;  // 0 = none

    Ticketor(std::size_t tickets, std::size_t buckets)
        : count_(buckets), buckets_(std::make_unique<Bucket[]>(buckets)) {
        for (std::size_t i = 0; i < tickets; ++i) {
            buckets_[i].ticket.store(static_cast<Ticket>(i + 1), std::memory_order_relaxed);
        }
    }

    // One scan over all buckets; 0 if every probed bucket was empty.
    Ticket try_take(std::size_t& cursor) noexcept {
        std::size_t i = cursor % count_;
        for (std::size_t probed = 0; probed < count_; ++probed) {
            auto& bucket = buckets_[i].ticket;
            if (bucket.load(std::memory_order_relaxed) != 0) {
                // acquire: pairs with give(); everything the previous holders
                // of the slot did happens-before our reuse.
                if (const Ticket t = bucket.exchange(0, std::memory_order_acquire); t != 0) {
                    cursor = i;
                    return t;
                }
            }
            if (++i == count_) {
                i = 0;
            }
        }
        return 0;
    }

    Ticket take(std::size_t& cursor) noexcept {
        for (;;) {
            if (const Ticket t = try_take(cursor); t != 0) {
                return t;
            }
            Sync::spin_hint();
        }
    }

    void give(Ticket ticket, std::size_t& cursor) noexcept {
        std::size_t i = cursor % count_;
        for (;;) {
            for (std::size_t probed = 0; probed < count_; ++probed) {
                auto& bucket = buckets_[i].ticket;
                Ticket expected = 0;
                if (bucket.load(std::memory_order_relaxed) == 0 &&
                    bucket.compare_exchange_strong(expected, ticket, std::memory_order_release,
                                                   std::memory_order_relaxed)) {
                    cursor = i;
                    return;
                }
                if (++i == count_) {
                    i = 0;
                }
            }
            Sync::spin_hint();
        }
    }

private:
    struct alignas(kCacheLineSize) Bucket {
        typename Sync::template Atomic<Ticket> ticket{0};
    };

    std::size_t count_;
    std::unique_ptr<Bucket[]> buckets_;
};

template <class Value, SyncPolicy Sync, class Reclaim = NoReclaim>
    requires std::is_trivially_copyable_v<Value>
class AtomicPointer {
public:
    static constexpr unsigned kVersionBits = 48;
    static constexpr std::uint64_t kVersionMask = (std::uint64_t{1} << kVersionBits) - 1;

    // A thread holds at most two slots (a pinned one and its spare) and the
    // state holds one, so 2 * max_threads + 1 slots always suffice; like the
    // prototype we keep 4x tickets in 6x buckets so scans stay short.
    explicit AtomicPointer(std::size_t max_threads, Value initial = Value{},
                           Reclaim reclaim = Reclaim{})
        : tickets_(checked_tickets(max_threads)),
          ticketor_(tickets_ - 1, 6 * max_threads),
          slots_(std::make_unique<Slot[]>(tickets_ + 1)),
          reclaim_(reclaim) {
        // Tickets 1..tickets_-1 are in the bag; ticket tickets_ starts as the
        // current slot.
        const std::uint32_t first = static_cast<std::uint32_t>(tickets_);
        slots_[first].value.store(initial, std::memory_order_relaxed);
        slots_[first].refs.store(1, std::memory_order_relaxed);
        state_.store(std::uint64_t{first} << kVersionBits, std::memory_order_relaxed);
    }

    AtomicPointer(const AtomicPointer&) = delete;
    AtomicPointer& operator=(const AtomicPointer&) = delete;

    // Runs `maker(const Value& current, Value& next) -> bool` until it either
    // commits `next` (returns the replaced value) or the maker declines
    // (returns nullopt, nothing changes). The maker may run several times.
    //
    // Whatever the ReadMode, a commit succeeds only if the version is
    // unchanged, i.e. only if the slot read was current for the whole
    // transaction; see ReadMode for what the maker may do with the value.
    // `cursor`: the calling thread's scan position in the slot bag (any value;
    // keep one per thread, initially random, and pass it every time).
    template <ReadMode kMode, class Maker>
    std::optional<Value> transact(Maker&& maker, std::size_t& cursor) {
        static_assert(kMode != ReadMode::kValidated || std::is_same_v<Reclaim, NoReclaim>,
                      "an unpinned value may be reclaimed while the maker runs");
        constexpr bool kPin = kMode == ReadMode::kPinned;
        std::uint32_t spare = 0;  // owned slot with refs == 1 (our reference)
        for (;;) {
            const std::uint64_t state = state_.load(std::memory_order_acquire);
            const auto current = static_cast<std::uint32_t>(state >> kVersionBits);
            if constexpr (kPin) {
                if (!pin(current)) {
                    continue;  // slot already died: the state has moved on
                }
            }
            const Value value = slots_[current].value.load(std::memory_order_relaxed);
            if constexpr (kMode != ReadMode::kRaw) {
                if (state_.load(std::memory_order_acquire) != state) {
                    if constexpr (kPin) {
                        unpin(current, 1, cursor);
                    }
                    continue;
                }
            }
            Value next;
            if (!maker(value, next)) {
                if constexpr (kPin) {
                    unpin(current, 1, cursor);
                }
                if (spare != 0) {
                    unpin(spare, 1, cursor);
                }
                return std::nullopt;
            }
            if (spare == 0) {
                spare = ticketor_.try_take(cursor);
                if (spare == 0) {
                    // Table exhausted: never wait while holding a pin.
                    if constexpr (kPin) {
                        unpin(current, 1, cursor);
                    }
                    spare = ticketor_.take(cursor);
                    slots_[spare].refs.store(1, std::memory_order_relaxed);
                    continue;
                }
                // refs was 0, so nobody can pin the slot concurrently (pins
                // only increment a positive count).
                slots_[spare].refs.store(1, std::memory_order_relaxed);
            }
            // Stale pinners of `spare` may read this; they are doomed to fail
            // their commit (the version moved on), and the value is atomic.
            slots_[spare].value.store(next, std::memory_order_relaxed);

            std::uint64_t expected = state;
            const std::uint64_t desired =
                (std::uint64_t{spare} << kVersionBits) | ((state + 1) & kVersionMask);
            // release: publishes `next` (and whatever the maker wrote, e.g. a
            // pushed node's link and payload); acquire: the popped value's
            // contents were published by an earlier commit.
            if (state_.compare_exchange_strong(expected, desired, std::memory_order_acq_rel,
                                               std::memory_order_relaxed)) {
                // Our spare reference now is the state's reference. The old
                // slot loses the state's reference (and our pin).
                unpin(current, kPin ? 2 : 1, cursor);
                return value;
            }
            if constexpr (kPin) {
                unpin(current, 1, cursor);
            }
            if constexpr (!std::is_same_v<Reclaim, NoReclaim>) {
                // The rejected value must go through Reclaim once its stale
                // pinners are gone, so the slot cannot be overwritten.
                unpin(spare, 1, cursor);
                spare = 0;
            }
            // else keep `spare` for the next attempt
        }
    }

    // Snapshot of the current value (no pin; may be stale immediately).
    Value load() const noexcept {
        for (;;) {
            const std::uint64_t state = state_.load(std::memory_order_acquire);
            const Value value =
                slots_[state >> kVersionBits].value.load(std::memory_order_relaxed);
            if (state_.load(std::memory_order_acquire) == state) {
                return value;
            }
        }
    }

private:
    struct alignas(kCacheLineSize) Slot {
        typename Sync::template Atomic<std::int32_t> refs{0};
        typename Sync::template Atomic<Value> value{Value{}};
    };

    static std::size_t checked_tickets(std::size_t max_threads) {
        const std::size_t tickets = 4 * (max_threads == 0 ? 1 : max_threads);
        if (tickets >= (std::size_t{1} << (64 - kVersionBits))) {
            throw std::invalid_argument("AtomicPointer: max_threads too large");
        }
        return tickets;
    }

    bool pin(std::uint32_t slot) noexcept {
        auto& refs = slots_[slot].refs;
        std::int32_t r = refs.load(std::memory_order_relaxed);
        while (r > 0) {
            // acquire: the slot's value was written before it was published.
            if (refs.compare_exchange_weak(r, r + 1, std::memory_order_acquire,
                                           std::memory_order_relaxed)) {
                return true;
            }
        }
        return false;
    }

    void unpin(std::uint32_t slot, std::int32_t n, std::size_t& cursor) {
        // acq_rel: the last holder sees every other holder's reads before it
        // reclaims the value and recycles the slot.
        if (slots_[slot].refs.fetch_sub(n, std::memory_order_acq_rel) == n) {
            reclaim_(slots_[slot].value.load(std::memory_order_relaxed));
            ticketor_.give(slot, cursor);
        }
    }

    const std::size_t tickets_;
    Ticketor<Sync> ticketor_;
    std::unique_ptr<Slot[]> slots_;
    [[no_unique_address]] Reclaim reclaim_;
    alignas(kCacheLineSize) typename Sync::template Atomic<std::uint64_t> state_{0};
};

}  // namespace object_pool
