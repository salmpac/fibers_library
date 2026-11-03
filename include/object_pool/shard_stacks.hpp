#pragma once

// Free-list structures for one shard of ShardedPool (sharded_pool.hpp).
//
// All of them are intrusive LIFO stacks of NodeArena nodes linked through
// NodeHook::next. The pool's memory is type-stable (nodes are freed only with
// the pool), so reading `next` of a node that another thread has just popped
// is memory-safe; it may only be stale. What each variant has to handle is
// therefore ABA on the head, not reclamation:
//
//   LockedStack        - head under a lock (spin lock by default).
//   TaggedStack        - Treiber stack, head = [tag:32][index:32] in one
//                        64-bit CAS word; the tag is bumped on every change.
//   AtomicPointerStack - head is the user's AtomicPointer<NodeIndex>; its
//                        48-bit version plays the role of the tag; the
//                        refcount/slot machinery comes along with it.
//   HazardStack        - untagged Treiber stack whose poppers protect the
//                        head with a hazard pointer; a released node that a
//                        popper protects is parked on a side list until it is
//                        not, which rules ABA out.
//
// Interface (used by ShardedPool):
//   using Shared = ...;          pool-wide state, constructible from options
//   explicit Stack(const PoolOptions&);
//   Hook* try_pop(const Arena&, Shared&, PoolThreadState&);   nullptr if empty
//   void push_chain(const Arena&, Shared&, PoolThreadState&, Hook* first, Hook* last);
//     pushes a caller-owned chain first..last (linked via next) at once.
//
// The doubly linked hooks: none of these stacks needs NodeHook::prev. Pushing
// a whole chain in O(1) needs only its first and last node, and keeping prev
// of the new head consistent in a lock-free stack would require updating two
// words atomically (the head and the new head's prev), which a single CAS
// cannot do; prev would be stale for every node another thread popped.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>

#include "object_pool/atomic_pointer.hpp"
#include "object_pool/node_arena.hpp"
#include "object_pool/sharded_object_pool.hpp"
#include "thread_pool/spin_lock.hpp"

namespace object_pool {

struct NoShared {
    explicit NoShared(const PoolOptions&) noexcept {}
};

// ------------------------------------------------------------ LockedStack

template <SyncPolicy Sync, class Lock = thread_pool::SpinLock<Sync>>
class LockedStack {
public:
    using Shared = NoShared;
    using Hook = detail::NodeHook<Sync>;

    explicit LockedStack(const PoolOptions&) noexcept {}

    template <class Arena>
    Hook* try_pop(const Arena& arena, Shared&, detail::PoolThreadState&) {
        std::lock_guard guard(lock_);
        if (head_ == detail::kNullNode) {
            return nullptr;
        }
        Hook* node = arena.hook(head_);
        head_ = node->next.load(std::memory_order_relaxed);
        return node;
    }

    template <class Arena>
    void push_chain(const Arena&, Shared&, detail::PoolThreadState&, Hook* first, Hook* last) {
        std::lock_guard guard(lock_);
        last->next.store(head_, std::memory_order_relaxed);
        head_ = first->self;
    }

private:
    Lock lock_;
    detail::NodeIndex head_ = detail::kNullNode;
};

// ------------------------------------------------------------ TaggedStack

template <SyncPolicy Sync>
class TaggedStack {
public:
    using Shared = NoShared;
    using Hook = detail::NodeHook<Sync>;

    explicit TaggedStack(const PoolOptions&) noexcept {}

    template <class Arena>
    Hook* try_pop(const Arena& arena, Shared&, detail::PoolThreadState&) {
        // acquire: the node (link and payload) was published by the
        // release-CAS of its push; every later head change is an RMW, so it
        // continues that release sequence.
        std::uint64_t head = head_.load(std::memory_order_acquire);
        for (;;) {
            const detail::NodeIndex index = index_of(head);
            if (index == detail::kNullNode) {
                return nullptr;
            }
            Hook* node = arena.hook(index);
            // Speculative: the node may be popped and re-linked by someone
            // else right now. Then the head changed and the CAS fails (the tag
            // differs even if the same node is back on top).
            const detail::NodeIndex next = node->next.load(std::memory_order_relaxed);
            if (head_.compare_exchange_weak(head, pack(next, tag_of(head) + 1),
                                            std::memory_order_acquire,
                                            std::memory_order_acquire)) {
                return node;
            }
        }
    }

    template <class Arena>
    void push_chain(const Arena&, Shared&, detail::PoolThreadState&, Hook* first, Hook* last) {
        std::uint64_t head = head_.load(std::memory_order_relaxed);
        do {
            last->next.store(index_of(head), std::memory_order_relaxed);
            // release: the chain's links and the objects' contents.
        } while (!head_.compare_exchange_weak(head, pack(first->self, tag_of(head) + 1),
                                              std::memory_order_release,
                                              std::memory_order_relaxed));
    }

private:
    static detail::NodeIndex index_of(std::uint64_t word) noexcept {
        return static_cast<detail::NodeIndex>(word);
    }
    static std::uint32_t tag_of(std::uint64_t word) noexcept {
        return static_cast<std::uint32_t>(word >> 32);
    }
    static std::uint64_t pack(detail::NodeIndex index, std::uint32_t tag) noexcept {
        return (std::uint64_t{tag} << 32) | index;
    }

    // A pop can be fooled only if, between its load and its CAS, exactly a
    // multiple of 2^32 head changes happen on this shard and the same node is
    // on top again.
    typename Sync::template Atomic<std::uint64_t> head_{0};
};

// ------------------------------------------------------------ AtomicPointerStack

// kPopMode: kPinned is the prototype's protocol (refcount pin per pop);
// kValidated drops the two refcount RMWs. Both are correct for type-stable
// nodes: the version is what rejects stale pops, not the pin.
template <SyncPolicy Sync, ReadMode kPopMode = ReadMode::kPinned>
class AtomicPointerStack {
public:
    using Shared = NoShared;
    using Hook = detail::NodeHook<Sync>;

    explicit AtomicPointerStack(const PoolOptions& options)
        : head_(options.max_threads, detail::kNullNode) {}

    template <class Arena>
    Hook* try_pop(const Arena& arena, Shared&, detail::PoolThreadState& thread) {
        // Cheap emptiness check: acquire() probes empty shards a lot, and a
        // pinned transaction would cost two RMWs on the shard just to see null.
        if (head_.load() == detail::kNullNode) {
            return nullptr;
        }
        // One transaction: head -> head->next. The maker sees the head that
        // was current at its validation; reading its next is a
        // speculative read of a type-stable node (see NodeHook).
        const auto popped = head_.template transact<kPopMode>(
            [&arena](detail::NodeIndex current, detail::NodeIndex& next) {
                if (current == detail::kNullNode) {
                    return false;
                }
                next = arena.hook(current)->next.load(std::memory_order_relaxed);
                return true;
            },
            thread.ticket_cursor);
        return popped ? arena.hook(*popped) : nullptr;
    }

    // Push needs no pin: the maker only stores the current head index into the
    // caller's own node, and the commit succeeds only if that head was current
    // for the whole transaction (version unchanged).
    template <class Arena>
    void push_chain(const Arena&, Shared&, detail::PoolThreadState& thread, Hook* first,
                    Hook* last) {
        head_.template transact<ReadMode::kRaw>(
            [first, last](detail::NodeIndex current, detail::NodeIndex& next) {
                last->next.store(current, std::memory_order_relaxed);
                next = first->self;
                return true;
            },
            thread.ticket_cursor);
    }

private:
    AtomicPointer<detail::NodeIndex, Sync> head_;
};

// ------------------------------------------------------------ HazardStack

// Untagged Treiber stack whose poppers protect the head with a hazard pointer.
//
// Each shard has its own hazard slots (kSlots, packed into one cache line);
// a pop claims a free slot by publishing the head index it is about to pop.
// release() checks the slots of the shard it releases to: if a popper of this
// shard protects the node, the node is parked on retired_ instead of free_;
// otherwise it goes straight to free_. retired_ is drained (take-all exchange,
// which is ABA-free) back into free_ once its nodes are no longer protected.
//
// Why per-shard protection suffices: a popper P of shard X can only be fooled
// if the node A it protects is back on top of X's free_; A gets there only by
// a release onto X (or a drain of X's retired_), and both check X's slots.
//
// Why a pop cannot be fooled (ABA): P publishes hp = A (seq_cst), then
// re-reads free_ == A (seq_cst). If A is later popped by someone else, that
// removal CAS (seq_cst) comes after P's validation in the seq_cst order, and
// the hazard check of any release/drain that could put A back happens after
// the removal (happens-before through the object's hand-off), so it sees
// hp = A and parks A until P clears hp. Hence while P holds hp, A is in free_
// only as the incarnation P validated, its next is the one P read (if A is
// still on top), and the CAS A -> next is correct. The StoreLoad order between
// publishing hp and re-reading free_ is what needs seq_cst; with release /
// acquire both could be reordered and the validation would prove nothing.
//
// Cost: pop = slot claim CAS + validating load + head CAS + slot clear, two of
// them on the shard's hazard line (shared by its poppers); release = load of
// the hazard line + head CAS (+ a load of retired_).
template <SyncPolicy Sync>
class HazardStack {
public:
    using Shared = NoShared;
    using Hook = detail::NodeHook<Sync>;

    // Hazard slots per shard (= max concurrent poppers of one shard; more
    // wait for a slot to free up).
    static constexpr std::size_t kSlots = kCacheLineSize / sizeof(detail::NodeIndex);

    explicit HazardStack(const PoolOptions& options) noexcept
        : slots_(std::clamp<std::size_t>(options.max_threads, 1, kSlots)) {}

    template <class Arena>
    Hook* try_pop(const Arena& arena, Shared&, detail::PoolThreadState& thread) {
        for (;;) {
            if (Hook* node = pop_free(arena, thread)) {
                return node;
            }
            if (!drain(arena)) {
                return nullptr;
            }
        }
    }

    // Release path: straight to free_ unless a popper protects the node.
    template <class Arena>
    void push_chain(const Arena& arena, Shared&, detail::PoolThreadState&, Hook* first,
                    Hook* last) {
        if (first == last) {  // the common case: one released object
            push(is_protected(first->self) ? retired_ : free_, first, first);
        } else {  // split, keeping the order
            Hook* safe_first = nullptr;
            Hook* safe_last = nullptr;
            Hook* kept_first = nullptr;
            Hook* kept_last = nullptr;
            for (Hook* node = first; node != nullptr;) {
                Hook* next = node == last
                                 ? nullptr
                                 : arena.hook(node->next.load(std::memory_order_relaxed));
                if (is_protected(node->self)) {
                    append(kept_first, kept_last, node);
                } else {
                    append(safe_first, safe_last, node);
                }
                node = next;
            }
            if (kept_first != nullptr) {
                push(retired_, kept_first, kept_last);
            }
            if (safe_first != nullptr) {
                push(free_, safe_first, safe_last);
            }
        }
        // Parked nodes would otherwise wait until free_ runs empty.
        if (retired_.load(std::memory_order_relaxed) != detail::kNullNode) [[unlikely]] {
            drain(arena);
        }
    }

    // Nodes that have never been in a free list cannot be protected (hazard
    // pointers only ever hold indices read from free_).
    template <class Arena>
    void push_fresh_chain(const Arena&, Shared&, detail::PoolThreadState&, Hook* first,
                          Hook* last) {
        push(free_, first, last);
    }

private:
    template <class Arena>
    Hook* pop_free(const Arena& arena, detail::PoolThreadState& thread) {
        detail::NodeIndex head = free_.load(std::memory_order_relaxed);
        if (head == detail::kNullNode) {
            return nullptr;
        }
        const std::size_t slot = protect(head, thread);
        for (;;) {
            // Validate: the protected node is still the head after hp was
            // published. seq_cst pairs with the publication (StoreLoad); it
            // also acquires the node, published by a release-CAS push.
            const detail::NodeIndex now = free_.load(std::memory_order_seq_cst);
            if (now != head) {
                if (now == detail::kNullNode) {
                    clear(slot);
                    return nullptr;
                }
                head = now;
                hazards_[slot].store(head, std::memory_order_seq_cst);
                continue;
            }
            Hook* node = arena.hook(head);
            // Speculative (see NodeHook): if the node is popped meanwhile its
            // next may be rewritten, but then free_ != head and the CAS fails.
            const detail::NodeIndex next = node->next.load(std::memory_order_relaxed);
            // seq_cst: the removal must be ordered with the hazard checks of
            // later releases (see above).
            if (free_.compare_exchange_strong(head, next, std::memory_order_seq_cst,
                                              std::memory_order_relaxed)) {
                clear(slot);
                return node;
            }
            if (head == detail::kNullNode) {
                clear(slot);
                return nullptr;
            }
            hazards_[slot].store(head, std::memory_order_seq_cst);
        }
    }

    // Claims a free slot and publishes `index` in it.
    std::size_t protect(detail::NodeIndex index, detail::PoolThreadState& thread) noexcept {
        std::size_t i = thread.hazard_hint % slots_;
        for (;;) {
            for (std::size_t k = 0; k < slots_; ++k) {
                detail::NodeIndex expected = detail::kNullNode;
                if (hazards_[i].load(std::memory_order_relaxed) == detail::kNullNode &&
                    hazards_[i].compare_exchange_strong(expected, index,
                                                        std::memory_order_seq_cst,
                                                        std::memory_order_relaxed)) {
                    thread.hazard_hint = i;
                    return i;
                }
                if (++i == slots_) {
                    i = 0;
                }
            }
            Sync::spin_hint();  // more concurrent poppers than slots
        }
    }

    void clear(std::size_t slot) noexcept {
        hazards_[slot].store(detail::kNullNode, std::memory_order_release);
    }

    bool is_protected(detail::NodeIndex index) const noexcept {
        for (std::size_t i = 0; i < slots_; ++i) {
            if (hazards_[i].load(std::memory_order_seq_cst) == index) {
                return true;
            }
        }
        return false;
    }

    // Moves unprotected parked nodes to free_. True if it moved any.
    template <class Arena>
    bool drain(const Arena& arena) {
        if (retired_.load(std::memory_order_relaxed) == detail::kNullNode) {
            return false;
        }
        // acq_rel: acquire the parked nodes (links, payload); the hazard check
        // below must come after this take.
        detail::NodeIndex index = retired_.exchange(detail::kNullNode, std::memory_order_acq_rel);
        Hook* safe_first = nullptr;
        Hook* safe_last = nullptr;
        Hook* kept_first = nullptr;
        Hook* kept_last = nullptr;
        // We own the taken nodes: re-link them into two chains (stale poppers
        // may still read their next; it is atomic).
        while (index != detail::kNullNode) {
            Hook* node = arena.hook(index);
            index = node->next.load(std::memory_order_relaxed);
            if (is_protected(node->self)) {
                append(kept_first, kept_last, node);
            } else {
                append(safe_first, safe_last, node);
            }
        }
        if (kept_first != nullptr) {
            push(retired_, kept_first, kept_last);
        }
        if (safe_first == nullptr) {
            return false;
        }
        push(free_, safe_first, safe_last);
        return true;
    }

    // Appends a node we own to a chain we own (doubly linked while owned).
    static void append(Hook*& first, Hook*& last, Hook* node) noexcept {
        if (last == nullptr) {
            first = node;
        } else {
            last->next.store(node->self, std::memory_order_relaxed);
        }
        node->prev = last == nullptr ? detail::kNullNode : last->self;
        last = node;
    }

    // Pushing a chain is ABA-safe: the CAS only needs the head to be what
    // last->next says.
    static void push(typename Sync::template Atomic<detail::NodeIndex>& list, Hook* first,
                     Hook* last) noexcept {
        detail::NodeIndex head = list.load(std::memory_order_relaxed);
        do {
            last->next.store(head, std::memory_order_relaxed);
            // release: links and the objects' contents.
        } while (!list.compare_exchange_weak(head, first->self, std::memory_order_release,
                                             std::memory_order_relaxed));
    }

    struct alignas(kCacheLineSize) HazardLine {
        static_assert(kSlots == 16);
        typename Sync::template Atomic<detail::NodeIndex> slot[kSlots] = {
            0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u};
        auto& operator[](std::size_t i) noexcept { return slot[i]; }
        const auto& operator[](std::size_t i) const noexcept { return slot[i]; }
    };

    alignas(kCacheLineSize) typename Sync::template Atomic<detail::NodeIndex> free_{
        detail::kNullNode};
    HazardLine hazards_;
    alignas(kCacheLineSize) typename Sync::template Atomic<detail::NodeIndex> retired_{
        detail::kNullNode};
    std::size_t slots_;
};

}  // namespace object_pool
