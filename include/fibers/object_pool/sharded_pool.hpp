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
// array, per-thread cache array) comes from `Alloc` (a PoolAllocator, see
// pool_allocator.hpp; default aligned ::operator new). The pool allocates at
// construction and on growth only, never on acquire()/release() fast paths,
// and returns everything to the same allocator instance on destruction.
//
// Per-thread caches (PoolOptions::thread_cache_capacity = C > 0; "magazines"
// after Bonwick): in front of the shards every thread has a small LIFO array
// of up to C free objects of this pool, touched only by that thread, so a hit
// is a plain load and store (no atomics, no fences).
//   acquire: cache non-empty -> pop from it. Else refill: pop a batch of B
//            objects from one shard with ONE CAS (try_pop_chain; start shard
//            per ShardSelect, probing the others), hand out the former top,
//            cache the rest. All shards empty -> grow, hand out one, cache
//            up to B - 1, put the rest on the start shard.
//   release: cache not full -> push into it. Else flush its B coldest
//            objects to a shard as one chain with ONE CAS (push_chain), then
//            push.
// B = PoolOptions::thread_cache_batch (default C / 2). After any shard
// exchange the cache is about half full, so at least ~C/2 operations in
// either direction hit it again. That is the hysteresis Bonwick gets from two
// magazines (a current and a previous one): two magazines of C exchanged
// whole behave like one array of 2C exchanging C at a time, so a separate
// second magazine is not implemented.
//
// Layout: one array of max_cached_threads slots, allocated once at
// construction from `Alloc`; slot i = [count][Hook* items[C]], padded to
// whole cache lines, belongs to the thread whose PoolThreadState::cache_slot
// is i (a dense id from a process-wide registry, see sharded_object_pool.hpp).
// Every operation re-reads the id through the noinline thread-state accessor,
// so a fiber that migrates between operations uses the cache of the thread it
// runs on. Threads with an id >= max_cached_threads use the shards directly,
// as with C = 0.
//
// Caches and capacity(): cached objects are still owned by the pool (they
// count in capacity() and are destroyed with it), but other threads cannot
// get them: up to max_cached_threads * C objects can sit in caches. Size the
// pool with that headroom, keep C small, and let a thread that stops using
// the pool for a long time (a worker about to park) call
// flush_thread_cache(). When a thread exits without flushing, its objects stay
// in its slot: not lost, not leaked, but unavailable until another thread
// starts using pools and inherits the id (and with it the slot and its
// objects), or until the pool is destroyed. A thread that exits for good can
// flush first to hand its objects back at once.

#include <algorithm>
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
          shards_(options_, arena_.allocator()),
          caches_(options_, arena_.allocator()) {
        preallocate();
    }

    ShardedPool(const ShardedPool&) = delete;
    ShardedPool& operator=(const ShardedPool&) = delete;

    // Cached objects are nodes of the arena, destroyed with it.
    ~ShardedPool() {
        caches_.destroy(arena_.allocator());
        shards_.destroy(arena_.allocator());
    }

    // Grows the pool when the cache and every shard are empty; out of memory
    // terminates.
    T* acquire() noexcept {
        detail::PoolThreadState& thread = detail::pool_thread_state<Sync>();
        if (thread.cache_slot < caches_.threads()) {
            std::byte* cache = caches_.slot(thread.cache_slot);
            std::size_t& count = Caches::count(cache);
            if (count != 0) [[likely]] {
                return Arena::value(Caches::items(cache)[--count]);
            }
            return refill_and_take(cache, thread);
        }
        return take_from_shards(thread);
    }

    void release(T* object) noexcept {
        detail::PoolThreadState& thread = detail::pool_thread_state<Sync>();
        Hook* node = Arena::hook_of(object);
        if (thread.cache_slot < caches_.threads()) {
            std::byte* cache = caches_.slot(thread.cache_slot);
            std::size_t& count = Caches::count(cache);
            if (count < options_.thread_cache_capacity) [[likely]] {
                Caches::items(cache)[count++] = node;
                return;
            }
            flush_and_put(cache, node, thread);
            return;
        }
        shards_[next_shard(thread) % options_.shards].stack.push_chain(arena_, shared_, thread,
                                                                       node, node);
    }

    // Returns the calling thread's cached objects to one shard (one CAS), so
    // that other threads can have them: e.g. before a worker parks or exits.
    // Returns how many were moved (0 without a cache).
    std::size_t flush_thread_cache() noexcept {
        detail::PoolThreadState& thread = detail::pool_thread_state<Sync>();
        if (thread.cache_slot >= caches_.threads()) {
            return 0;
        }
        std::byte* cache = caches_.slot(thread.cache_slot);
        const std::size_t count = Caches::count(cache);
        if (count != 0) {
            push_items(Caches::items(cache), count, thread);
            Caches::count(cache) = 0;
        }
        return count;
    }

    // Total number of objects owned by the pool (in use, free in the shards,
    // and free in per-thread caches).
    std::size_t capacity() const noexcept { return arena_.capacity(); }

    // Diagnostics: free objects in the calling thread's cache.
    std::size_t thread_cached_objects() const noexcept {
        const detail::PoolThreadState& thread = detail::pool_thread_state<Sync>();
        return thread.cache_slot < caches_.threads()
                   ? Caches::count(caches_.slot(thread.cache_slot))
                   : 0;
    }

    // Diagnostics: free objects in all per-thread caches. Reads the other
    // threads' caches without synchronisation, so call it only while no
    // thread uses the pool (e.g. after joining the workers); otherwise it is
    // a data race.
    std::size_t cached_objects() const noexcept {
        std::size_t total = 0;
        for (std::size_t i = 0; i < caches_.threads(); ++i) {
            total += Caches::count(caches_.slot(i));
        }
        return total;
    }

    // The options after normalisation (defaults filled in, values clamped).
    const PoolOptions& options() const noexcept { return options_; }

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

    // The per-thread caches: options.max_cached_threads slots of
    // [std::size_t count][Hook* items[capacity]], each padded to whole cache
    // lines (no false sharing between threads), in one block from the pool
    // allocator; no block when caching is off. Like Shards, it does not store
    // the allocator: the pool calls destroy() exactly once.
    class Caches {
    public:
        Caches(const PoolOptions& options, Alloc& alloc) noexcept
            : threads_(options.max_cached_threads),
              stride_(round_up(kItemsOffset + options.thread_cache_capacity * sizeof(Hook*),
                               sync::kCacheLineSize)) {
            if (threads_ == 0) {
                return;
            }
            data_ = static_cast<std::byte*>(alloc.allocate(bytes(), sync::kCacheLineSize));
            for (std::size_t i = 0; i < threads_; ++i) {
                std::byte* slot = data_ + i * stride_;
                ::new (static_cast<void*>(slot)) std::size_t{0};
                ::new (static_cast<void*>(slot + kItemsOffset)) Hook*[options.thread_cache_capacity]{};
            }
        }
        Caches(const Caches&) = delete;
        Caches& operator=(const Caches&) = delete;

        // Only trivially destructible objects live in the block.
        void destroy(Alloc& alloc) noexcept {
            if (data_ != nullptr) {
                alloc.deallocate(data_, bytes(), sync::kCacheLineSize);
            }
        }

        std::size_t threads() const noexcept { return threads_; }
        std::byte* slot(std::size_t i) const noexcept { return data_ + i * stride_; }

        static std::size_t& count(std::byte* slot) noexcept {
            return *std::launder(reinterpret_cast<std::size_t*>(slot));
        }
        static Hook** items(std::byte* slot) noexcept {
            return std::launder(reinterpret_cast<Hook**>(slot + kItemsOffset));
        }

    private:
        static constexpr std::size_t round_up(std::size_t n, std::size_t a) {
            return (n + a - 1) / a * a;
        }
        static constexpr std::size_t kItemsOffset = sizeof(std::size_t);
        static_assert(kItemsOffset % alignof(Hook*) == 0);

        std::size_t bytes() const noexcept { return threads_ * stride_; }

        std::size_t threads_;
        std::size_t stride_;
        std::byte* data_ = nullptr;
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

    // The shard path (the whole acquire() without a cache).
    T* take_from_shards(detail::PoolThreadState& thread) noexcept {
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

    T* grow_and_take(std::size_t shard, detail::PoolThreadState& thread) {
        const typename Arena::Chain chain = arena_.grow(options_.grow_chunk);
        if (chain.size > 1) {
            Hook* rest = arena_.hook(chain.first->next.load(std::memory_order_relaxed));
            push_fresh(shards_[shard], thread, rest, chain.last);
        }
        return Arena::value(chain.first);
    }

    // Empty cache: one batch from the first non-empty shard. The former top
    // is handed out, the rest cached so that they come out in stack order.
    // All shards empty: grow and keep up to batch - 1 fresh objects.
    [[gnu::noinline]] T* refill_and_take(std::byte* cache,
                                         detail::PoolThreadState& thread) noexcept {
        const std::size_t n = options_.shards;
        const std::size_t batch = options_.thread_cache_batch;
        Hook** items = Caches::items(cache);
        const std::size_t start = next_shard(thread) % n;
        std::size_t i = start;
        for (std::size_t probed = 0; probed < n; ++probed) {
            const HookChain<Hook> chain = pop_chain(shards_[i], thread, batch);
            if (chain.size != 0) {
                // Ours now: the links are those of the successful pop.
                const std::size_t rest = chain.size - 1;
                Hook* node = chain.first;
                for (std::size_t j = 0; j < rest; ++j) {
                    node = arena_.hook(node->next.load(std::memory_order_relaxed));
                    items[rest - 1 - j] = node;
                }
                Caches::count(cache) = rest;
                return Arena::value(chain.first);
            }
            if (++i == n) {
                i = 0;
            }
        }
        const typename Arena::Chain chain = arena_.grow(options_.grow_chunk);
        const std::size_t keep = std::min(batch, chain.size) - 1;
        Hook* node = chain.first;
        for (std::size_t j = 0; j < keep; ++j) {
            node = arena_.hook(node->next.load(std::memory_order_relaxed));
            items[j] = node;
        }
        Caches::count(cache) = keep;
        if (node != chain.last) {
            push_fresh(shards_[start], thread,
                       arena_.hook(node->next.load(std::memory_order_relaxed)), chain.last);
        }
        return Arena::value(chain.first);
    }

    // Full cache: its `batch` coldest objects (the bottom of the array) go to
    // one shard as one chain, the others move down, then `node` goes on top.
    [[gnu::noinline]] void flush_and_put(std::byte* cache, Hook* node,
                                         detail::PoolThreadState& thread) noexcept {
        const std::size_t capacity = options_.thread_cache_capacity;
        const std::size_t batch = options_.thread_cache_batch;
        Hook** items = Caches::items(cache);
        push_items(items, batch, thread);
        std::copy(items + batch, items + capacity, items);
        std::size_t count = capacity - batch;
        items[count++] = node;
        Caches::count(cache) = count;
    }

    // Links items[0..count) into one chain (items[0] on top) and pushes it
    // onto the next shard with one push_chain (count >= 1).
    void push_items(Hook** items, std::size_t count, detail::PoolThreadState& thread) noexcept {
        for (std::size_t j = 0; j + 1 < count; ++j) {
            items[j]->next.store(items[j + 1]->self, std::memory_order_relaxed);
        }
        shards_[next_shard(thread) % options_.shards].stack.push_chain(
            arena_, shared_, thread, items[0], items[count - 1]);
    }

    // Up to `max` nodes from one shard: in one operation if the stack has
    // try_pop_chain, else by repeated try_pop.
    HookChain<Hook> pop_chain(Shard& shard, detail::PoolThreadState& thread, std::size_t max) {
        if constexpr (requires { shard.stack.try_pop_chain(arena_, shared_, thread, max); }) {
            return shard.stack.try_pop_chain(arena_, shared_, thread, max);
        } else {
            HookChain<Hook> chain;
            while (chain.size < max) {
                Hook* node = shard.stack.try_pop(arena_, shared_, thread);
                if (node == nullptr) {
                    break;
                }
                if (chain.size == 0) {
                    chain.first = node;
                } else {
                    chain.last->next.store(node->self, std::memory_order_relaxed);
                }
                chain.last = node;
                ++chain.size;
            }
            return chain;
        }
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
    Caches caches_;
};

// The variants, ready to use.
template <class T, sync::SyncPolicy Sync = sync::StdSync, PoolAllocator Alloc = DefaultAllocator>
using TaggedPool = ShardedPool<T, TaggedStack<Sync>, Sync, Alloc>;

// Default object pool: shards are tagged Treiber stacks, the fastest
// lock-free variant in object_pool_bench, with per-thread caches of 8 objects
// in front of them (PoolOptions::thread_cache_capacity; 0 turns them off).
// Keep PoolOptions::select at kRoundRobin when objects are released on other
// threads (fibers, tasks).
//
//   DefaultPool<T> a(options);                                  // ::operator new
//   DefaultPool<T, StdSync, MyAlloc> b(options, MyAlloc{...});  // stateful custom
//   DefaultPool<T, StdSync, PmrAllocator> c(options, &resource); // std::pmr
template <class T, sync::SyncPolicy Sync = sync::StdSync, PoolAllocator Alloc = DefaultAllocator>
using DefaultPool = TaggedPool<T, Sync, Alloc>;

}  // namespace fibers::object_pool
