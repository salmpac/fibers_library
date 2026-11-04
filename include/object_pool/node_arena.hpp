#pragma once

// Type-stable node storage shared by the intrusive shard stacks.
//
// Every pooled object lives in a node: [NodeHook][T]. Nodes are allocated in
// chunks and destroyed only with the arena, so a node's memory stays a valid
// node for the arena's whole lifetime. That is what lets the lock-free stacks
// read `next` of a node they have not won yet (the read may be stale, never
// dangling).
//
// Nodes are named by 32-bit indices: [entry:8][offset:24], entry 0 is never
// used, so index 0 is the null link. Indices instead of pointers keep a link
// small enough to pack with a 32-bit ABA tag into one 64-bit CAS word.
//
// All memory (the chunks and the fixed-size chunk directory) comes from the
// arena's PoolAllocator (pool_allocator.hpp): one allocation at construction,
// one per growth, all returned at destruction.

#include <algorithm>
#include <cstddef>
#include <type_traits>
#include <cstdint>
#include <mutex>
#include <new>
#include <utility>

#include "object_pool/pool_allocator.hpp"
#include "thread_pool/fail_fast.hpp"
#include "thread_pool/sync.hpp"

namespace object_pool::detail {

using thread_pool::SyncPolicy;

using NodeIndex = std::uint32_t;
inline constexpr NodeIndex kNullNode = 0;

// Intrusive hooks: the pooled objects form a doubly linked list.
//
// `next` is the only link the shard stacks need. It is an atomic because
// lock-free poppers read it speculatively: a thread reads head->next before
// its CAS on the head succeeds, while another thread may already have popped
// that node and be re-linking it. The CAS then fails and the value is
// discarded, but with a plain field the read itself would be a data race (UB,
// and reported by TSan/Twist). Relaxed is enough: a value that is actually
// used was published by the release-CAS that made the node the head.
//
// `prev` is maintained only inside chains owned by a single thread (a freshly
// allocated chunk, a chain being spliced); no stack reads it, so it is plain.
template <SyncPolicy Sync>
struct NodeHook {
    typename Sync::template Atomic<NodeIndex> next{kNullNode};
    NodeIndex prev = kNullNode;
    NodeIndex self = kNullNode;  // own index, immutable after construction
};

// T must be nothrow default constructible (fail-fast library: no exceptions).
template <class T, SyncPolicy Sync, PoolAllocator Alloc = DefaultAllocator>
    requires std::is_nothrow_default_constructible_v<T>
class NodeArena {
public:
    using Hook = NodeHook<Sync>;

    // Doubly linked chain of nodes owned by the caller: first..last via next,
    // last..first via prev.
    struct Chain {
        Hook* first = nullptr;
        Hook* last = nullptr;
        std::size_t size = 0;
    };

    static constexpr unsigned kOffsetBits = 24;
    static constexpr std::size_t kEntryNodes = std::size_t{1} << kOffsetBits;
    static constexpr std::size_t kEntries = 256;  // entry 0 reserved (null)
    // Growth is geometric past this ratio, so the 255 usable entries do not
    // run out before the 32-bit index space does.
    static constexpr std::size_t kGrowDivisor = 16;

    explicit NodeArena(Alloc alloc = Alloc()) noexcept : alloc_(std::move(alloc)) {
        void* memory = alloc_.allocate(sizeof(Directory), alignof(Directory));
        directory_ = ::new (memory) Directory{};
    }

    NodeArena(const NodeArena&) = delete;
    NodeArena& operator=(const NodeArena&) = delete;

    ~NodeArena() {
        for (std::size_t b = 0; b < blocks_; ++b) {
            const Block& block = directory_->blocks[b];
            destroy_nodes(block.memory, block.count);
            alloc_.deallocate(block.memory, block.count * kStride, kAlign);
        }
        directory_->~Directory();
        alloc_.deallocate(directory_, sizeof(Directory), alignof(Directory));
    }

    Alloc& allocator() noexcept { return alloc_; }
    const Alloc& allocator() const noexcept { return alloc_; }

    Hook* hook(NodeIndex index) const noexcept {
        std::byte* base =
            directory_->entries[index >> kOffsetBits].base.load(std::memory_order_acquire);
        return hook_at(base + (index & (kEntryNodes - 1)) * kStride);
    }

    static T* value(Hook* hook) noexcept {
        return value_of(reinterpret_cast<std::byte*>(hook));
    }

    static Hook* hook_of(T* object) noexcept {
        return hook_at(reinterpret_cast<std::byte*>(object) - kValueOffset);
    }

    // Allocates max(min_count, capacity / kGrowDivisor) nodes (at least 1)
    // and returns them as one chain.
    Chain grow(std::size_t min_count) noexcept {
        std::lock_guard guard(lock_);
        const std::size_t count =
            std::max<std::size_t>({min_count, capacity_ / kGrowDivisor, 1});
        const std::size_t entries = (count + kEntryNodes - 1) / kEntryNodes;
        if (entries > kEntries - next_entry_) {
            thread_pool::fail_fast("object_pool: node index space exhausted");
        }

        auto* memory = static_cast<std::byte*>(alloc_.allocate(count * kStride, kAlign));
        for (std::size_t i = 0; i < count; ++i) {
            std::byte* node = memory + i * kStride;
            ::new (static_cast<void*>(node)) Hook{};
            ::new (static_cast<void*>(node + kValueOffset)) T();  // nothrow (required)
        }
        // Cannot overflow: every block takes at least one of the
        // kEntries - 1 usable entries, checked above.
        directory_->blocks[blocks_++] = Block{memory, count};

        const std::size_t first_entry = next_entry_;
        next_entry_ += entries;
        for (std::size_t e = 0; e < entries; ++e) {
            directory_->entries[first_entry + e].base.store(memory + e * kEntryNodes * kStride,
                                                            std::memory_order_release);
        }
        auto index_of = [first_entry](std::size_t i) {
            return static_cast<NodeIndex>(((first_entry + i / kEntryNodes) << kOffsetBits) |
                                          (i % kEntryNodes));
        };
        for (std::size_t i = 0; i < count; ++i) {
            Hook* h = hook_at(memory + i * kStride);
            h->self = index_of(i);
            h->prev = i == 0 ? kNullNode : index_of(i - 1);
            h->next.store(i + 1 == count ? kNullNode : index_of(i + 1),
                          std::memory_order_relaxed);
        }
        capacity_ += count;
        return Chain{hook_at(memory), hook_at(memory + (count - 1) * kStride), count};
    }

    std::size_t capacity() const {
        std::lock_guard guard(lock_);
        return capacity_;
    }

private:
    static constexpr std::size_t round_up(std::size_t n, std::size_t a) {
        return (n + a - 1) / a * a;
    }

    static constexpr std::size_t kAlign = std::max(alignof(Hook), alignof(T));
    static constexpr std::size_t kValueOffset = round_up(sizeof(Hook), alignof(T));
    static constexpr std::size_t kStride = round_up(kValueOffset + sizeof(T), kAlign);

    static Hook* hook_at(std::byte* node) noexcept {
        return std::launder(reinterpret_cast<Hook*>(node));
    }
    static T* value_of(std::byte* node) noexcept {
        return std::launder(reinterpret_cast<T*>(node + kValueOffset));
    }
    static void destroy_nodes(std::byte* memory, std::size_t count) noexcept {
        for (std::size_t i = 0; i < count; ++i) {
            std::byte* node = memory + i * kStride;
            value_of(node)->~T();
            hook_at(node)->~Hook();
        }
    }

    // Written once (under lock_) before any node of the entry is published,
    // read by every index -> node translation. Atomic because a stale index
    // (e.g. read speculatively from a recycled slot) may be translated by a
    // thread that has no happens-before with the grow.
    struct Entry {
        typename Sync::template Atomic<std::byte*> base{nullptr};
    };

    // One allocated chunk of `count` nodes.
    struct Block {
        std::byte* memory = nullptr;
        std::size_t count = 0;
    };

    // Fixed size, allocated once with the arena: the 8-bit entry field bounds
    // both arrays, so growth never reallocates the directory (and allocates
    // nothing but the chunk itself). `entries` is the hot part (index
    // translation); `blocks` is only touched by grow() and the destructor.
    struct Directory {
        Entry entries[kEntries];
        Block blocks[kEntries - 1];
    };

    [[no_unique_address]] Alloc alloc_;
    Directory* directory_ = nullptr;
    mutable typename Sync::Mutex lock_;
    std::size_t blocks_ = 0;  // used prefix of directory_->blocks
    std::size_t next_entry_ = 1;
    std::size_t capacity_ = 0;
};

}  // namespace object_pool::detail
