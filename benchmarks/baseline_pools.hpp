#pragma once

// Baseline pools for object_pool_bench only (not part of the library):
// they show what the lock-free DefaultPool buys.
//   MutexVectorPool - every shard is a lock + std::vector<T*> free list;
//   LockedListPool  - ShardedPool with a lock-protected intrusive stack per
//                     shard (same nodes/arena as DefaultPool, lock instead of
//                     the tagged CAS).

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>

#include "object_pool/sharded_pool.hpp"
#include "thread_pool/spin_lock.hpp"

namespace object_pool::baseline {

using object_pool::detail::PoolThreadState;
using object_pool::kCacheLineSize;
using object_pool::NoShared;
using object_pool::PoolOptions;
using object_pool::ShardSelect;
using object_pool::StdSync;
using object_pool::SyncPolicy;
namespace detail = object_pool::detail;

// Baseline: every shard is a lock + std::vector<T*> free list.
// Lock defaults to Sync::Mutex; thread_pool::SpinLock<Sync> also fits.
template <class T, SyncPolicy Sync = StdSync, class Lock = typename Sync::Mutex>
    requires std::default_initializable<T>
class MutexVectorPool {
public:
    using value_type = T;

    explicit MutexVectorPool(PoolOptions options = {})
        : options_(detail::normalize(options)),
          shards_(std::make_unique<Shard[]>(options_.shards)) {
        if (options_.initial_objects != 0) {
            T* objects = allocate_chunk(options_.initial_objects);
            for (std::size_t i = 0; i < options_.initial_objects; ++i) {
                shards_[i % options_.shards].free.push_back(objects + i);
            }
        }
    }

    MutexVectorPool(const MutexVectorPool&) = delete;
    MutexVectorPool& operator=(const MutexVectorPool&) = delete;

    T* acquire() {
        const std::size_t n = options_.shards;
        const std::size_t start = next_shard();
        for (std::size_t i = 0; i < n; ++i) {
            Shard& shard = shards_[(start + i) % n];
            std::lock_guard guard(shard.lock);
            if (!shard.free.empty()) {
                T* object = shard.free.back();
                shard.free.pop_back();
                return object;
            }
        }
        return grow_and_take(start);
    }

    void release(T* object) {
        Shard& shard = shards_[next_shard() % options_.shards];
        std::lock_guard guard(shard.lock);
        shard.free.push_back(object);
    }

    // Total number of objects owned by the pool (in use + free).
    std::size_t capacity() const {
        std::lock_guard guard(grow_lock_);
        return capacity_;
    }

private:
    struct alignas(kCacheLineSize) Shard {
        Lock lock;
        std::vector<T*> free;
    };

    std::size_t next_shard() noexcept {
        detail::PoolThreadState& state = detail::pool_thread_state<Sync>();
        return static_cast<std::size_t>(
            options_.select == ShardSelect::kRoundRobin ? state.cursor++ : state.cursor);
    }

    T* allocate_chunk(std::size_t count) {
        std::lock_guard guard(grow_lock_);
        chunks_.push_back(std::make_unique<T[]>(count));
        capacity_ += count;
        return chunks_.back().get();
    }

    T* grow_and_take(std::size_t shard_index) {
        T* objects = allocate_chunk(options_.grow_chunk);
        if (options_.grow_chunk > 1) {
            Shard& shard = shards_[shard_index % options_.shards];
            std::lock_guard guard(shard.lock);
            for (std::size_t i = 1; i < options_.grow_chunk; ++i) {
                shard.free.push_back(objects + i);
            }
        }
        return objects;
    }

    const PoolOptions options_;
    std::unique_ptr<Shard[]> shards_;

    mutable typename Sync::Mutex grow_lock_;
    std::vector<std::unique_ptr<T[]>> chunks_;
    std::size_t capacity_ = 0;
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

template <class T, SyncPolicy Sync = StdSync, class Lock = thread_pool::SpinLock<Sync>>
using LockedListPool = ShardedPool<T, LockedStack<Sync, Lock>, Sync>;

}  // namespace object_pool::baseline
