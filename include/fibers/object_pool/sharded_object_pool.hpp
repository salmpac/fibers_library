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

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstdint>

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
};

namespace detail {

struct PoolThreadState {
    std::uint64_t round_robin = 0;  // advanced by every op of a kRoundRobin pool
    std::uint64_t home = 0;         // fixed per thread; home shard = home % shards
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
        state.round_robin = static_cast<std::uint64_t>(Sync::random_seed());
        state.home = static_cast<std::uint64_t>(Sync::random_seed());
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

}  // namespace fibers::object_pool
