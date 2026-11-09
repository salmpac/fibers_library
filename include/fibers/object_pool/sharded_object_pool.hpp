#pragma once

// Sharded object pools: common definitions (options, shard selection,
// per-thread state, the ObjectPool concept). The pool itself is
// ShardedPool / DefaultPool in sharded_pool.hpp.
//
// A pool owns objects of type T for its whole lifetime: acquire() hands out a
// pointer to an existing (default-constructed, possibly reused) object,
// release() takes it back. Objects are destroyed only with the pool, so their
// memory is type-stable. When every shard is empty the pool grows by a chunk.
//
// Shard selection per operation:
//   kRoundRobin - each acquire/release moves the thread's round-robin
//                 counter to the next shard (spreads load, objects migrate
//                 between shards);
//   kHome       - a thread always starts at its own (random) home shard.
// The per-thread state is shared by all pools: the round-robin counter and the
// home seed are separate, so round-robin pools never move a thread's home
// shard in kHome pools.
// Either way, an empty shard makes acquire() scan the others before growing.
//
// Per-thread caches (PoolOptions::thread_cache_capacity > 0): every pool owns
// a small cache per thread in front of the shards, so that a thread's
// acquire/release usually touch only memory of its own (no atomics, no
// fences). See ShardedPool in sharded_pool.hpp. The caches of a pool are an
// array indexed by a small dense per-thread id (PoolThreadState::cache_slot):
//   - a thread gets its id on its first pool operation (any pool of the same
//     Sync policy): the smallest id not held by a live thread, from a
//     process-wide registry (ThreadIdRegistry, mutex + bitmap, cold path);
//   - the id is the same in every pool, so one thread uses slot `id` of each;
//   - when the thread exits, a thread-local lease returns the id, and the next
//     thread to start using pools gets it, together with whatever the exited
//     thread left in slot `id` of every pool. Ids stay dense (at most the
//     number of threads alive at once), and objects cached by an exited
//     thread are stranded only until another thread starts;
//   - threads whose id is >= PoolOptions::max_cached_threads (or that find
//     all kMaxThreadIds ids taken) use the shards directly.

#include <algorithm>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <mutex>

#include "fibers/sync/sync.hpp"

namespace fibers::object_pool {


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

    // Per-thread cache: at most this many free objects per thread
    // (0 = no caches, every operation goes to the shards). 8 measured at
    // least as fast as no cache in every object_pool_bench scenario (4-60x in
    // single-thread and churn ones) while keeping few objects out of the
    // shards; 16 helps long bursts and producer/consumer flows a bit more.
    std::size_t thread_cache_capacity = 8;
    // Objects moved between a cache and the shards at once (one CAS);
    // 0 = capacity / 2 (at least 1). Clamped to [1, capacity].
    std::size_t thread_cache_batch = 0;
    // Number of per-thread caches (slots for thread ids 0..n-1); threads with
    // a larger id use the shards. Clamped to detail::kMaxThreadIds.
    std::size_t max_cached_threads = 64;
};

namespace detail {

inline constexpr std::uint32_t kNoCacheSlot = UINT32_MAX;
// Thread ids handed out at once at most (threads alive and using pools).
inline constexpr std::size_t kMaxThreadIds = 4096;

struct PoolThreadState {
    std::uint64_t round_robin = 0;  // advanced by every op of a kRoundRobin pool
    std::uint64_t home = 0;         // fixed per thread; home shard = home % shards
    // Dense per-thread id: the index of this thread's cache in every pool
    // (kNoCacheSlot: none). See ThreadIdRegistry.
    std::uint32_t cache_slot = kNoCacheSlot;
    bool seeded = false;
};

// Process-wide source of PoolThreadState::cache_slot (one per Sync policy):
// the smallest id no live thread holds. Used only when a thread starts and
// exits. The mutex also hands the caches over: everything the exited thread
// wrote to its slots happens-before the release() of its id, which
// happens-before the acquire() of the next thread that gets it.
template <class Sync>
class ThreadIdRegistry {
public:
    std::uint32_t acquire() noexcept {
        std::lock_guard guard(lock_);
        for (std::size_t w = 0; w < kWords; ++w) {
            if (used_[w] != ~std::uint64_t{0}) {
                const int bit = std::countr_one(used_[w]);
                used_[w] |= std::uint64_t{1} << bit;
                return static_cast<std::uint32_t>(w * 64 + static_cast<std::size_t>(bit));
            }
        }
        return kNoCacheSlot;
    }

    void release(std::uint32_t id) noexcept {
        std::lock_guard guard(lock_);
        used_[id / 64] &= ~(std::uint64_t{1} << (id % 64));
    }

private:
    static constexpr std::size_t kWords = kMaxThreadIds / 64;

    typename Sync::Mutex lock_;
    std::uint64_t used_[kWords] = {};
};

// The thread-local PoolThreadState, without seeding (see pool_thread_state).
template <class Sync>
[[gnu::noinline]] PoolThreadState& raw_pool_thread_state() noexcept {
    asm volatile("" ::: "memory");
    return Sync::template thread_local_instance<PoolThreadState>();
}

// Returns the thread's id when the thread exits. A separate thread-local, so
// that PoolThreadState stays trivially destructible (no TLS init/destructor
// guard on the hot accessor); it is touched only when the id is assigned.
template <class Sync>
struct ThreadIdLease {
    std::uint32_t id = kNoCacheSlot;

    ~ThreadIdLease() {
        if (id == kNoCacheSlot) {
            return;
        }
        // Pool operations from later thread-local destructors of this thread
        // must not touch the slot any more: they go to the shards.
        raw_pool_thread_state<Sync>().cache_slot = kNoCacheSlot;
        Sync::template global_instance<ThreadIdRegistry<Sync>>().release(id);
    }
};

// Same rules as thread_pool::detail::thread_state(): fibers may migrate
// between threads, so the thread-local is reached only through a call that is
// never inlined nor treated as pure.
template <class Sync>
[[gnu::noinline]] PoolThreadState& pool_thread_state() noexcept {
    asm volatile("" ::: "memory");
    PoolThreadState& state = Sync::template thread_local_instance<PoolThreadState>();
    if (!state.seeded) [[unlikely]] {
        state.round_robin = static_cast<std::uint64_t>(Sync::random_seed());
        state.home = static_cast<std::uint64_t>(Sync::random_seed());
        state.cache_slot = Sync::template global_instance<ThreadIdRegistry<Sync>>().acquire();
        Sync::template thread_local_instance<ThreadIdLease<Sync>>().id = state.cache_slot;
        state.seeded = true;
    }
    return state;
}

inline PoolOptions normalize(PoolOptions options) {
    options.shards = std::max<std::size_t>(options.shards, 1);
    options.grow_chunk = std::max<std::size_t>(options.grow_chunk, 1);
    if (options.thread_cache_capacity == 0) {
        options.max_cached_threads = 0;
    }
    options.max_cached_threads = std::min(options.max_cached_threads, kMaxThreadIds);
    if (options.max_cached_threads == 0) {
        options.thread_cache_capacity = 0;
    }
    if (options.thread_cache_batch == 0) {
        options.thread_cache_batch = options.thread_cache_capacity / 2;
    }
    options.thread_cache_batch =
        std::clamp<std::size_t>(options.thread_cache_batch, 1,
                                std::max<std::size_t>(options.thread_cache_capacity, 1));
    return options;
}

}  // namespace detail

}  // namespace fibers::object_pool
