#pragma once

// ShardedPool<T, Stack, Sync, Alloc>: the sharded object pool of
// sharded_object_pool.hpp with intrusive nodes and a pluggable per-shard free
// list (see shard_stacks.hpp).
//
// Contract (see sharded_object_pool.hpp): objects are default
// constructed, owned by the pool for its whole lifetime (type-stable), shard
// selection per operation is ShardSelect::kRoundRobin or kHome, an empty start
// shard makes acquire() probe the others, and when all are empty the pool
// grows (by max(grow_chunk, capacity / 16) objects; the geometric part keeps
// the 32-bit node index space usable).
//
// Memory: every byte the pool owns (object chunks, chunk directory, shard
// array) comes from `Alloc` (a PoolAllocator, see pool_allocator.hpp; default
// aligned ::operator new). The pool allocates at construction and on growth
// only, never on acquire()/release() fast paths, and returns everything to the
// same allocator instance on destruction.

#include <cstddef>
#include <type_traits>
#include <new>
#include <utility>

#include "fibers/object_pool/node_arena.hpp"
#include "fibers/object_pool/pool_allocator.hpp"
#include "fibers/object_pool/shard_stacks.hpp"
#include "fibers/object_pool/sharded_object_pool.hpp"

namespace fibers::object_pool {

template <class T, class Stack, sync::SyncPolicy Sync = sync::StdSync,
          PoolAllocator Alloc = DefaultAllocator>
    requires std::is_nothrow_default_constructible_v<T>
class ShardedPool {
    using Arena = detail::NodeArena<T, Sync, Alloc>;
    using Hook = typename Arena::Hook;

public:
    using value_type = T;
    using stack_type = Stack;
    using allocator_type = Alloc;

    // `alloc` is moved into the pool and used for all of its memory.
    // Fail fast: out of memory here terminates the process.
    explicit ShardedPool(PoolOptions options = {}, Alloc alloc = Alloc()) noexcept
        : options_(detail::normalize(options)),
          arena_(std::move(alloc)),
          shared_(options_),
          shards_(options_, arena_.allocator()) {
        preallocate();
    }

    ShardedPool(const ShardedPool&) = delete;
    ShardedPool& operator=(const ShardedPool&) = delete;

    ~ShardedPool() { shards_.destroy(arena_.allocator()); }

    // Grows the pool when every shard is empty; out of memory terminates.
    T* acquire() noexcept {
        detail::PoolThreadState& thread = detail::pool_thread_state<Sync>();
        const std::size_t n = options_.shards;
        const std::size_t start = next_shard(thread) % n;
        std::size_t i = start;
        for (std::size_t probed = 0; probed < n; ++probed) {
            if (Hook* node = shards_[i].stack.try_pop(arena_, shared_, thread)) {
                return Arena::value(node);
            }
            if (++i == n) {
                i = 0;
            }
        }
        return grow_and_take(start, thread);
    }

    void release(T* object) noexcept {
        detail::PoolThreadState& thread = detail::pool_thread_state<Sync>();
        Hook* node = Arena::hook_of(object);
        shards_[next_shard(thread) % options_.shards].stack.push_chain(arena_, shared_, thread,
                                                                       node, node);
    }

    // Total number of objects owned by the pool (in use + free).
    std::size_t capacity() const noexcept { return arena_.capacity(); }

    const Alloc& get_allocator() const noexcept { return arena_.allocator(); }

private:
    struct alignas(sync::kCacheLineSize) Shard {
        explicit Shard(const PoolOptions& options) noexcept : stack(options) {}
        Stack stack;
    };

    // Fixed array of non-movable shards, in memory from the pool allocator.
    // It does not store the allocator (the arena owns the one instance):
    // the owner calls destroy() exactly once, from ~ShardedPool or when the
    // pool constructor fails after the shards were built.
    class Shards {
    public:
        static_assert(std::is_nothrow_constructible_v<Shard, const PoolOptions&>,
                      "shard stacks must be nothrow constructible");

        Shards(const PoolOptions& options, Alloc& alloc) noexcept : count_(options.shards) {
            data_ = static_cast<Shard*>(alloc.allocate(bytes(), alignof(Shard)));
            for (std::size_t i = 0; i < count_; ++i) {
                ::new (static_cast<void*>(data_ + i)) Shard(options);
            }
        }
        Shards(const Shards&) = delete;
        Shards& operator=(const Shards&) = delete;

        void destroy(Alloc& alloc) noexcept { destroy(alloc, count_); }

        Shard& operator[](std::size_t i) noexcept { return data_[i]; }

    private:
        std::size_t bytes() const noexcept { return count_ * sizeof(Shard); }

        void destroy(Alloc& alloc, std::size_t built) noexcept {
            for (std::size_t i = 0; i < built; ++i) {
                data_[i].~Shard();
            }
            alloc.deallocate(data_, bytes(), alignof(Shard));
        }

        std::size_t count_;
        Shard* data_;
    };

    // Spreads options_.initial_objects fresh objects over the shards.
    void preallocate() {
        if (options_.initial_objects == 0) {
            return;
        }
        detail::PoolThreadState& thread = detail::pool_thread_state<Sync>();
        const typename Arena::Chain chain = arena_.grow(options_.initial_objects);
        Hook* node = chain.first;
        for (std::size_t i = 0; i < chain.size; ++i) {
            Hook* next = i + 1 < chain.size
                             ? arena_.hook(node->next.load(std::memory_order_relaxed))
                             : nullptr;
            push_fresh(shards_[i % options_.shards], thread, node, node);
            node = next;
        }
    }

    std::size_t next_shard(detail::PoolThreadState& thread) noexcept {
        return static_cast<std::size_t>(
            options_.select == ShardSelect::kRoundRobin ? thread.round_robin++ : thread.home);
    }

    T* grow_and_take(std::size_t shard, detail::PoolThreadState& thread) {
        const typename Arena::Chain chain = arena_.grow(options_.grow_chunk);
        if (chain.size > 1) {
            Hook* rest = arena_.hook(chain.first->next.load(std::memory_order_relaxed));
            push_fresh(shards_[shard], thread, rest, chain.last);
        }
        return Arena::value(chain.first);
    }

    // Freshly allocated nodes: a stack may have a cheaper path for them.
    void push_fresh(Shard& shard, detail::PoolThreadState& thread, Hook* first, Hook* last) {
        if constexpr (requires { shard.stack.push_fresh_chain(arena_, shared_, thread, first, last); }) {
            shard.stack.push_fresh_chain(arena_, shared_, thread, first, last);
        } else {
            shard.stack.push_chain(arena_, shared_, thread, first, last);
        }
    }

    const PoolOptions options_;
    Arena arena_;
    typename Stack::Shared shared_;
    Shards shards_;
};

// The variants, ready to use.
template <class T, sync::SyncPolicy Sync = sync::StdSync, PoolAllocator Alloc = DefaultAllocator>
using TaggedPool = ShardedPool<T, TaggedStack<Sync>, Sync, Alloc>;

// Default object pool: shards are tagged Treiber stacks, the fastest
// lock-free variant in object_pool_bench. Keep PoolOptions::select at
// kRoundRobin when objects are released on other threads (fibers, tasks).
//
//   DefaultPool<T> a(options);                                  // ::operator new
//   DefaultPool<T, StdSync, MyAlloc> b(options, MyAlloc{...});  // stateful custom
//   DefaultPool<T, StdSync, PmrAllocator> c(options, &resource); // std::pmr
template <class T, sync::SyncPolicy Sync = sync::StdSync, PoolAllocator Alloc = DefaultAllocator>
using DefaultPool = TaggedPool<T, Sync, Alloc>;

}  // namespace fibers::object_pool
