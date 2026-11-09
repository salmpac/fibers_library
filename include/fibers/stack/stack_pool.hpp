#pragma once

// StackPool: fiber stacks for reuse, without allocation on the hot path.
//
// Built on the sharded object pool (object_pool::DefaultPool): the pooled
// objects are Stack slots. A slot is default-constructed empty (Stack's
// nothrow default constructor maps nothing); acquire() takes a slot from the
// pool and, the first time that slot is handed out, maps its stack (cold
// path). release() returns the slot with its stack still mapped, so a slot
// once mapped stays mapped until the StackPool is destroyed and every later
// acquire() of it is just the object pool's acquire (per-thread cache hit:
// no atomics, no fences; else one CAS on a shard).
//
// Why lazy slots rather than a slot type that maps in its constructor: the
// stack size is a run-time option, while the pool default-constructs its
// objects (the size would have to be a template parameter), and the object
// pool grows in chunks (max(grow_chunk, capacity / 16) objects at once),
// which would map many stacks at once and could not respect max_stacks.
//
// Limits. The pool is created with exactly max_stacks slots (a few dozen
// bytes each, no stacks mapped yet), so at most max_stacks stacks are ever
// mapped. The object pool grows only when the caller's cache and every shard
// are empty, i.e. when all max_stacks slots are in use or sitting in other
// threads' caches; the first such grown (empty) slot handed out is a
// fail_fast. Like any object pool with per-thread caches, up to
// max_cached_threads * thread_cache_capacity free stacks can sit in caches
// out of reach of other threads: leave that headroom in max_stacks (or set
// thread_cache_capacity = 0), and let a thread that stops using the pool call
// flush_thread_cache().
//
// Preallocated stacks are mapped (and their top page touched) at
// construction, then put into the pool.
//
// Threads: acquire() and release() from any threads, a stack may be released
// by another thread than the one that acquired it (fibers migrate). The
// object pool gives the happens-before from a release() to the acquire()
// that gets the same slot. All stacks must be released before the pool is
// destroyed; destroying the pool unmaps every stack.

#include <algorithm>
#include <cstddef>
#include <cstring>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/object_pool/sharded_pool.hpp"
#include "fibers/stack/stack.hpp"
#include "fibers/sync/sync.hpp"

namespace fibers::stack {

struct StackPoolOptions {
    std::size_t stack_size = Stack::kDefaultSize;  // usable bytes, rounded up to pages
    std::size_t preallocated = 0;                   // stacks mapped at construction
    std::size_t max_stacks = 4096;                  // stacks mapped at most (>= 1)

    // Object-pool tuning (see object_pool::PoolOptions).
    std::size_t shards = 8;
    std::size_t thread_cache_capacity = 8;
    std::size_t max_cached_threads = 64;
};

template <sync::SyncPolicy Sync = sync::StdSync>
class StackPool {
public:
    explicit StackPool(StackPoolOptions options = {}) noexcept
        : stack_size_(options.stack_size),
          max_stacks_(std::max<std::size_t>(options.max_stacks, 1)),
          slots_(pool_options(options, max_stacks_)) {
        if (options.preallocated > max_stacks_) {
            fail::fail_fast("stack: StackPool: preallocated > max_stacks");
        }
        preallocate(options.preallocated);
    }

    StackPool(const StackPool&) = delete;
    StackPool& operator=(const StackPool&) = delete;

    // A mapped stack of at least stack_size() usable bytes, owned by the
    // pool. Fail fast beyond max_stacks or when out of memory.
    Stack* acquire() noexcept {
        Stack* stack = slots_.acquire();
        if (stack->empty()) [[unlikely]] {
            map(*stack);
        }
        return stack;
    }

    // `stack` must come from acquire() of this pool. Its memory keeps
    // whatever the last fiber left in it.
    void release(Stack* stack) noexcept { slots_.release(stack); }

    // Hands the calling thread's cached stacks back to the shared shards.
    std::size_t flush_thread_cache() noexcept { return slots_.flush_thread_cache(); }

    // Stacks mapped so far (in use or free); never exceeds max_stacks().
    std::size_t mapped_stacks() const noexcept {
        return mapped_.load(std::memory_order_relaxed);
    }

    std::size_t stack_size() const noexcept { return stack_size_; }
    std::size_t max_stacks() const noexcept { return max_stacks_; }

private:
    static object_pool::PoolOptions pool_options(const StackPoolOptions& options,
                                                 std::size_t max_stacks) noexcept {
        object_pool::PoolOptions pool;
        pool.shards = std::max<std::size_t>(options.shards, 1);
        pool.initial_objects = max_stacks;  // every slot there will ever be
        pool.grow_chunk = 1;                // growth means "over the limit" anyway
        pool.select = object_pool::ShardSelect::kRoundRobin;  // released on other threads
        pool.thread_cache_capacity = options.thread_cache_capacity;
        pool.max_cached_threads = options.max_cached_threads;
        return pool;
    }

    // Cold: first use of a slot.
    [[gnu::noinline]] void map(Stack& slot) noexcept {
        // Slots beyond the initial max_stacks exist only if the object pool
        // had to grow: every slot was in use (or cached by other threads).
        if (slots_.capacity() > max_stacks_) {
            fail::fail_fast("stack: StackPool: more than max_stacks stacks in use "
                            "(or held in other threads' caches)");
        }
        slot = Stack::allocate(stack_size_);
        mapped_.fetch_add(1, std::memory_order_relaxed);
    }

    // Takes `count` slots out of the pool, maps them, and puts them back.
    // They have to be out of the pool all at once (else acquire() would hand
    // the same slot back), and the pool must not allocate, so the slots are
    // chained through a pointer stored at the top of each fresh stack (a
    // page the fiber will touch first anyway).
    void preallocate(std::size_t count) noexcept {
        Stack* chain = nullptr;
        for (std::size_t i = 0; i < count; ++i) {
            Stack* stack = acquire();
            std::memcpy(link_of(*stack), &chain, sizeof(chain));
            chain = stack;
        }
        while (chain != nullptr) {
            Stack* next = nullptr;
            std::memcpy(&next, link_of(*chain), sizeof(next));
            release(chain);
            chain = next;
        }
    }

    static std::byte* link_of(const Stack& stack) noexcept {
        const StackView view = stack.view();
        return view.data() + view.size() - sizeof(Stack*);
    }

    const std::size_t stack_size_;
    const std::size_t max_stacks_;
    typename Sync::template Atomic<std::size_t> mapped_{0};
    object_pool::DefaultPool<Stack, Sync> slots_;
};

}  // namespace fibers::stack
