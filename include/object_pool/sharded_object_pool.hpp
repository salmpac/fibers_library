#pragma once

// Sharded object pools.
//
// A pool owns objects of type T for its whole lifetime: acquire() hands out a
// pointer to an existing (default-constructed, possibly reused) object,
// release() takes it back. Objects are destroyed only with the pool, so their
// memory is type-stable. When every shard is empty the pool grows by a chunk.
//
// Shard selection per operation:
//   kRoundRobin - each acquire/release moves the thread's cursor to the next
//                 shard (spreads load, objects migrate between shards);
//   kHome       - a thread always starts at its own (random) home shard.
// Either way, an empty shard makes acquire() scan the others before growing.

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "thread_pool/sync.hpp"

namespace object_pool {

using thread_pool::kCacheLineSize;
using thread_pool::StdSync;
using thread_pool::SyncPolicy;

// What benchmarks and tests expect from any pool implementation.
template <class P>
concept ObjectPool = requires(P pool, typename P::value_type* object) {
    { pool.acquire() } -> std::same_as<typename P::value_type*>;
    pool.release(object);
    { pool.capacity() } -> std::convertible_to<std::size_t>;
};

enum class ShardSelect { kRoundRobin, kHome };

struct PoolOptions {
    std::size_t shards = 16;
    std::size_t initial_objects = 1024;  // preallocated, spread over shards
    std::size_t grow_chunk = 256;        // objects allocated when all shards are empty
    ShardSelect select = ShardSelect::kRoundRobin;
};

namespace detail {

struct PoolThreadState {
    std::uint64_t cursor = 0;
    bool seeded = false;
};

// Same rules as thread_pool::detail::thread_state(): fibers may migrate
// between threads, so the thread-local is reached only through a call that is
// never inlined nor treated as pure.
template <class Sync>
[[gnu::noinline]] PoolThreadState& pool_thread_state() noexcept {
    asm volatile("" ::: "memory");
    PoolThreadState& state = Sync::template thread_local_instance<PoolThreadState>();
    if (!state.seeded) [[unlikely]] {
        state.cursor = static_cast<std::uint64_t>(Sync::random_seed());
        state.seeded = true;
    }
    return state;
}

inline PoolOptions normalize(PoolOptions options) {
    options.shards = std::max<std::size_t>(options.shards, 1);
    options.grow_chunk = std::max<std::size_t>(options.grow_chunk, 1);
    return options;
}

}  // namespace detail

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

}  // namespace object_pool
