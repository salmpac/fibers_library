#pragma once

// Free-list structure for one shard of ShardedPool (sharded_pool.hpp).
//
// All of them are intrusive LIFO stacks of NodeArena nodes linked through
// NodeHook::next. The pool's memory is type-stable (nodes are freed only with
// the pool), so reading `next` of a node that another thread has just popped
// is memory-safe; it may only be stale. What each variant has to handle is
// therefore ABA on the head, not reclamation:
//
//   TaggedStack        - Treiber stack, head = [tag:32][index:32] in one
//                        64-bit CAS word; the tag is bumped on every change.
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

#include "object_pool/node_arena.hpp"
#include "object_pool/sharded_object_pool.hpp"

namespace object_pool {

struct NoShared {
    explicit NoShared(const PoolOptions&) noexcept {}
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

}  // namespace object_pool
