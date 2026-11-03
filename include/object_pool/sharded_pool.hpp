#pragma once

// ShardedPool<T, Stack, Sync>: the sharded object pool of
// sharded_object_pool.hpp with intrusive nodes and a pluggable per-shard free
// list (see shard_stacks.hpp).
//
// Same contract and options as MutexVectorPool: objects are default
// constructed, owned by the pool for its whole lifetime (type-stable), shard
// selection per operation is ShardSelect::kRoundRobin or kHome, an empty start
// shard makes acquire() probe the others, and when all are empty the pool
// grows (by max(grow_chunk, capacity / 16) objects; the geometric part keeps
// the 32-bit node index space usable).

#include <cstddef>
#include <new>

#include "object_pool/node_arena.hpp"
#include "object_pool/shard_stacks.hpp"
#include "object_pool/sharded_object_pool.hpp"

namespace object_pool {

template <class T, class Stack, SyncPolicy Sync = StdSync>
    requires std::default_initializable<T>
class ShardedPool {
    using Arena = detail::NodeArena<T, Sync>;
    using Hook = typename Arena::Hook;

public:
    using value_type = T;
    using stack_type = Stack;

    explicit ShardedPool(PoolOptions options = {})
        : options_(detail::normalize(options)), shared_(options_), shards_(options_) {
        if (options_.initial_objects != 0) {
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
    }

    ShardedPool(const ShardedPool&) = delete;
    ShardedPool& operator=(const ShardedPool&) = delete;

    T* acquire() {
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

    void release(T* object) {
        detail::PoolThreadState& thread = detail::pool_thread_state<Sync>();
        Hook* node = Arena::hook_of(object);
        shards_[next_shard(thread) % options_.shards].stack.push_chain(arena_, shared_, thread,
                                                                       node, node);
    }

    // Total number of objects owned by the pool (in use + free).
    std::size_t capacity() const { return arena_.capacity(); }

private:
    struct alignas(kCacheLineSize) Shard {
        explicit Shard(const PoolOptions& options) : stack(options) {}
        Stack stack;
    };

    // Fixed array of non-movable shards.
    class Shards {
    public:
        explicit Shards(const PoolOptions& options) : count_(options.shards) {
            data_ = static_cast<Shard*>(
                ::operator new(count_ * sizeof(Shard), std::align_val_t{alignof(Shard)}));
            std::size_t built = 0;
            try {
                for (; built < count_; ++built) {
                    ::new (static_cast<void*>(data_ + built)) Shard(options);
                }
            } catch (...) {
                destroy(built);
                throw;
            }
        }
        Shards(const Shards&) = delete;
        Shards& operator=(const Shards&) = delete;
        ~Shards() { destroy(count_); }

        Shard& operator[](std::size_t i) noexcept { return data_[i]; }

    private:
        void destroy(std::size_t built) noexcept {
            for (std::size_t i = 0; i < built; ++i) {
                data_[i].~Shard();
            }
            ::operator delete(data_, std::align_val_t{alignof(Shard)});
        }

        std::size_t count_;
        Shard* data_;
    };

    std::size_t next_shard(detail::PoolThreadState& thread) noexcept {
        return static_cast<std::size_t>(
            options_.select == ShardSelect::kRoundRobin ? thread.cursor++ : thread.cursor);
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
template <class T, SyncPolicy Sync = StdSync, class Lock = thread_pool::SpinLock<Sync>>
using LockedListPool = ShardedPool<T, LockedStack<Sync, Lock>, Sync>;

template <class T, SyncPolicy Sync = StdSync>
using TaggedPool = ShardedPool<T, TaggedStack<Sync>, Sync>;

template <class T, SyncPolicy Sync = StdSync>
using AtomicPointerPool = ShardedPool<T, AtomicPointerStack<Sync>, Sync>;

// AtomicPointer without the per-pop refcount pin (validated read instead).
template <class T, SyncPolicy Sync = StdSync>
using AtomicPointerValidatedPool =
    ShardedPool<T, AtomicPointerStack<Sync, ReadMode::kValidated>, Sync>;

template <class T, SyncPolicy Sync = StdSync>
using HazardPool = ShardedPool<T, HazardStack<Sync>, Sync>;

}  // namespace object_pool
