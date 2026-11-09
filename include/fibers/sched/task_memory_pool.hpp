#pragma once

// TaskMemoryPool: memory for plain-callback tasks, in size classes.
//
// Classes of 64, 128, 256 and 512 bytes (16-byte aligned), each a sharded
// object pool (object_pool::DefaultPool) of fixed-size blocks, so a callback
// task costs no allocation on the hot path: the pool's per-thread cache hit
// is a plain load and store. The class is chosen at compile time from the
// size of the node (TaskHeader + closure) by the caller (Scheduler::submit).
// Nodes larger than kMaxBlockSize are rejected with a static_assert: keep
// big state in an object of your own (from your own pool) and capture a
// pointer to it.
//
// Blocks are released by whichever thread runs (or drops) the task: the pools
// use ShardSelect::kRoundRobin, as objects released on other threads should
// (see object_pool::DefaultPool).
//
// Growth: a class whose cache and shards are empty grows by the pool's
// grow_chunk (through the object pool's allocator, ::operator new by
// default). Size initial_blocks for the expected number of callbacks in
// flight.

#include <array>
#include <cstddef>

#include "fibers/object_pool/sharded_pool.hpp"
#include "fibers/sync/sync.hpp"

namespace fibers::sched {

struct TaskMemoryPoolOptions {
    // Blocks preallocated per size class.
    std::size_t initial_blocks = 256;
    // Object-pool tuning, the same for every class (see object_pool::PoolOptions).
    std::size_t grow_chunk = 256;
    std::size_t shards = 8;
    std::size_t thread_cache_capacity = 16;
    std::size_t max_cached_threads = 64;
};

template <sync::SyncPolicy Sync = sync::StdSync>
class TaskMemoryPool {
public:
    static constexpr std::size_t kBlockAlign = 16;
    static constexpr std::array<std::size_t, 4> kBlockSizes = {64, 128, 256, 512};
    static constexpr std::size_t kMaxBlockSize = kBlockSizes.back();

    // The smallest block size that holds `size` bytes; 0 if none does.
    static constexpr std::size_t block_size_for(std::size_t size) noexcept {
        for (const std::size_t block : kBlockSizes) {
            if (size <= block) {
                return block;
            }
        }
        return 0;
    }

    explicit TaskMemoryPool(TaskMemoryPoolOptions options = {}) noexcept
        : c64_(pool_options(options)),
          c128_(pool_options(options)),
          c256_(pool_options(options)),
          c512_(pool_options(options)) {}

    TaskMemoryPool(const TaskMemoryPool&) = delete;
    TaskMemoryPool& operator=(const TaskMemoryPool&) = delete;

    // A block of exactly BlockSize bytes (one of kBlockSizes), kBlockAlign
    // aligned, with unspecified contents.
    template <std::size_t BlockSize>
    void* acquire() noexcept {
        return pool<BlockSize>().acquire()->bytes;
    }

    // `block` must come from acquire<BlockSize>() of this pool.
    template <std::size_t BlockSize>
    void release(void* block) noexcept {
        // bytes is the first and only member of Block.
        pool<BlockSize>().release(static_cast<Block<BlockSize>*>(block));
    }

    template <std::size_t BlockSize>
    std::size_t capacity() const noexcept {
        return pool_of<BlockSize>(*this).capacity();
    }

    // Hands the calling thread's cached blocks back to the shared shards.
    void flush_thread_cache() noexcept {
        c64_.flush_thread_cache();
        c128_.flush_thread_cache();
        c256_.flush_thread_cache();
        c512_.flush_thread_cache();
    }

private:
    template <std::size_t N>
    struct Block {
        alignas(kBlockAlign) std::byte bytes[N];
    };
    static_assert(sizeof(Block<64>) == 64 && alignof(Block<64>) == kBlockAlign);

    template <std::size_t N>
    using Pool = object_pool::DefaultPool<Block<N>, Sync>;

    template <std::size_t BlockSize>
    Pool<BlockSize>& pool() noexcept {
        return pool_of<BlockSize>(*this);
    }

    template <std::size_t BlockSize, class Self>
    static auto& pool_of(Self& self) noexcept {
        if constexpr (BlockSize == 64) {
            return self.c64_;
        } else if constexpr (BlockSize == 128) {
            return self.c128_;
        } else if constexpr (BlockSize == 256) {
            return self.c256_;
        } else {
            static_assert(BlockSize == 512, "TaskMemoryPool: not a block size (see kBlockSizes)");
            return self.c512_;
        }
    }

    static object_pool::PoolOptions pool_options(const TaskMemoryPoolOptions& options) noexcept {
        object_pool::PoolOptions pool;
        pool.shards = options.shards;
        pool.initial_objects = options.initial_blocks;
        pool.grow_chunk = options.grow_chunk;
        pool.select = object_pool::ShardSelect::kRoundRobin;  // released on other threads
        pool.thread_cache_capacity = options.thread_cache_capacity;
        pool.max_cached_threads = options.max_cached_threads;
        return pool;
    }

    Pool<64> c64_;
    Pool<128> c128_;
    Pool<256> c256_;
    Pool<512> c512_;
};

}  // namespace fibers::sched
