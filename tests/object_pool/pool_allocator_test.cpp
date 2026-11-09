// Custom allocators for the object pool (pool_allocator.hpp).
//
// Outside sanitizer builds this binary replaces the global operator
// new/delete with counting versions, so the tests can also prove that the pool
// allocates nothing behind its allocator's back. (ASan/TSan runtimes bring
// their own operator new and cannot be replaced; there the leak checker and
// the per-allocator book-keeping below still verify every block.)

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory_resource>
#include <mutex>
#include <new>
#include <set>
#include <thread>
#include <vector>

#include "fibers/object_pool/pool_allocator.hpp"
#include "fibers/object_pool/sharded_pool.hpp"

namespace op = fibers::object_pool;

// ------------------------------------------------------------ global new

#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define POOL_TEST_SANITIZED 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
#define POOL_TEST_SANITIZED 1
#endif
#endif

#ifdef POOL_TEST_SANITIZED
constexpr bool kCountsGlobalNew = false;
#else
constexpr bool kCountsGlobalNew = true;
#endif

namespace {

std::atomic<std::size_t> g_global_news{0};
std::atomic<std::size_t> g_global_max_align{0};

constexpr std::size_t round_up(std::size_t n, std::size_t a) { return (n + a - 1) / a * a; }

#ifndef POOL_TEST_SANITIZED
void* counted_new(std::size_t bytes, std::size_t align) {
    g_global_news.fetch_add(1, std::memory_order_relaxed);
    std::size_t seen = g_global_max_align.load(std::memory_order_relaxed);
    while (seen < align && !g_global_max_align.compare_exchange_weak(seen, align)) {
    }
    void* p = align <= alignof(std::max_align_t)
                  ? std::malloc(bytes == 0 ? 1 : bytes)
                  : std::aligned_alloc(align, round_up(bytes == 0 ? 1 : bytes, align));
    if (p == nullptr) {
        std::abort();  // fail fast, like the library
    }
    return p;
}
#endif

}  // namespace

#ifndef POOL_TEST_SANITIZED
void* operator new(std::size_t n) { return counted_new(n, alignof(std::max_align_t)); }
void* operator new[](std::size_t n) { return counted_new(n, alignof(std::max_align_t)); }
void* operator new(std::size_t n, std::align_val_t a) {
    return counted_new(n, static_cast<std::size_t>(a));
}
void* operator new[](std::size_t n, std::align_val_t a) {
    return counted_new(n, static_cast<std::size_t>(a));
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
// The nothrow forms too: a sanitizer runtime would otherwise provide them with
// its own allocator, mismatching our deletes.
void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
    return counted_new(n, alignof(std::max_align_t));
}
void* operator new[](std::size_t n, const std::nothrow_t&) noexcept {
    return counted_new(n, alignof(std::max_align_t));
}
void* operator new(std::size_t n, std::align_val_t a, const std::nothrow_t&) noexcept {
    return counted_new(n, static_cast<std::size_t>(a));
}
void* operator new[](std::size_t n, std::align_val_t a, const std::nothrow_t&) noexcept {
    return counted_new(n, static_cast<std::size_t>(a));
}
void operator delete(void* p, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t, const std::nothrow_t&) noexcept {
    std::free(p);
}
#endif  // POOL_TEST_SANITIZED

namespace {

// ------------------------------------------------------------ helpers

// Book-keeping of one allocator: every live block with its size and
// alignment. Uses no global operator new itself (fixed array, aligned_alloc).
struct AllocStats {
    struct Record {
        void* p;
        std::size_t bytes;
        std::size_t align;
    };

    void* allocate(std::size_t bytes, std::size_t align) noexcept {
        void* p = std::aligned_alloc(align, round_up(bytes == 0 ? 1 : bytes, align));
        if (p == nullptr) {
            std::abort();  // fail fast
        }
        std::lock_guard guard(mutex);
        if (reinterpret_cast<std::uintptr_t>(p) % align != 0 || live_count == live.size()) {
            ++errors;
        } else {
            live[live_count++] = Record{p, bytes, align};
        }
        ++allocs;
        bytes_live += bytes;
        max_align = std::max(max_align, align);
        min_align = std::min(min_align, align);
        return p;
    }

    // Must match exactly one live allocation (pointer, size and alignment).
    void deallocate(void* p, std::size_t bytes, std::size_t align) noexcept {
        {
            std::lock_guard guard(mutex);
            ++deallocs;
            std::size_t i = 0;
            while (i < live_count && live[i].p != p) ++i;
            if (i == live_count || live[i].bytes != bytes || live[i].align != align) {
                ++errors;  // unknown pointer, double free or wrong size/alignment
                return;    // leak it rather than corrupt the heap
            }
            live[i] = live[--live_count];
            bytes_live -= bytes;
        }
        std::free(p);
    }

    bool contains(const void* object) {
        std::lock_guard guard(mutex);
        const auto* q = static_cast<const std::byte*>(object);
        for (std::size_t i = 0; i < live_count; ++i) {
            const auto* base = static_cast<const std::byte*>(live[i].p);
            if (q >= base && q < base + live[i].bytes) return true;
        }
        return false;
    }

    std::mutex mutex;
    std::array<Record, 512> live{};
    std::size_t live_count = 0;
    std::size_t allocs = 0;
    std::size_t deallocs = 0;
    std::size_t bytes_live = 0;
    std::size_t errors = 0;
    std::size_t max_align = 0;
    std::size_t min_align = SIZE_MAX;
};

// Stateful and move-only: the pool must keep the instance it was given.
class CountingAllocator {
public:
    CountingAllocator(AllocStats& stats, int id) : stats_(&stats), id_(id) {}
    CountingAllocator(CountingAllocator&& other) noexcept
        : stats_(other.stats_), id_(other.id_) {
        other.stats_ = nullptr;
    }
    CountingAllocator(const CountingAllocator&) = delete;
    CountingAllocator& operator=(const CountingAllocator&) = delete;

    void* allocate(std::size_t bytes, std::size_t align) noexcept {
        return stats_->allocate(bytes, align);
    }
    void deallocate(void* p, std::size_t bytes, std::size_t align) noexcept {
        stats_->deallocate(p, bytes, align);
    }

    int id() const { return id_; }

private:
    AllocStats* stats_;
    int id_;
};

static_assert(op::PoolAllocator<CountingAllocator>);

class CountingResource : public std::pmr::memory_resource {
public:
    AllocStats stats;

private:
    void* do_allocate(std::size_t bytes, std::size_t align) override {
        return stats.allocate(bytes, align);
    }
    void do_deallocate(void* p, std::size_t bytes, std::size_t align) override {
        stats.deallocate(p, bytes, align);
    }
    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
        return this == &other;
    }
};

struct Object {
    std::uint64_t payload[3] = {};
};

struct alignas(128) Wide {
    unsigned char bytes[200] = {};
};

// Not nothrow default constructible: the pools must reject it at compile time.
struct ThrowingCtor {
    ThrowingCtor() {}  // NOLINT: deliberately not noexcept
};

template <class T>
concept Poolable = requires { typename op::DefaultPool<T>; };

// A memory resource that refuses: the pool must fail fast, not throw.
class RefusingResource : public std::pmr::memory_resource {
    void* do_allocate(std::size_t, std::size_t) override { throw std::bad_alloc(); }
    void do_deallocate(void*, std::size_t, std::size_t) override {}
    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
        return this == &other;
    }
};

using CountingPool = op::DefaultPool<Object, fibers::sync::StdSync, CountingAllocator>;

// The per-thread pool state is seeded on first use; do it outside the
// measured windows.
void warm_up() { static_cast<void>(op::detail::pool_thread_state<fibers::sync::StdSync>()); }

template <class T>
bool aligned_to(const T* p, std::size_t align) {
    return reinterpret_cast<std::uintptr_t>(p) % align == 0;
}

}  // namespace

// ------------------------------------------------------------ zero overhead

static_assert(std::is_empty_v<op::NewDeleteAllocator>);
static_assert(std::is_same_v<op::DefaultPool<Object>,
                             op::DefaultPool<Object, fibers::sync::StdSync, op::DefaultAllocator>>);
// An empty allocator takes no space; a stateful one is stored once.
static_assert(sizeof(op::DefaultPool<Object, fibers::sync::StdSync, op::PmrAllocator>) ==
              sizeof(op::DefaultPool<Object>) + sizeof(void*));

// ------------------------------------------------------------ default allocator

// The default allocator is aligned ::operator new: exactly four allocations
// for a pool that does not grow (directory, shard array, per-thread cache
// array, initial chunk), one
// more per growth, nothing on acquire/release.
TEST(PoolAllocator, DefaultUsesAlignedOperatorNewOnly) {
    if (!kCountsGlobalNew) GTEST_SKIP() << "global operator new not replaceable here";
    warm_up();
    const std::size_t before = g_global_news.load();
    std::size_t after_construction = 0;
    std::size_t after_reuse = 0;
    std::size_t after_growth = 0;
    {
        op::DefaultPool<Object> pool({.shards = 4, .initial_objects = 8, .grow_chunk = 8});
        after_construction = g_global_news.load();
        for (int round = 0; round < 100; ++round) {
            Object* held[8];
            for (Object*& o : held) o = pool.acquire();
            for (Object* o : held) pool.release(o);
        }
        after_reuse = g_global_news.load();
        Object* held[9];
        for (Object*& o : held) o = pool.acquire();  // the 9th grows
        for (Object* o : held) pool.release(o);
        after_growth = g_global_news.load();
    }
    EXPECT_EQ(after_construction - before, 4u);
    EXPECT_EQ(after_reuse, after_construction);
    EXPECT_EQ(after_growth - after_reuse, 1u);
}

TEST(PoolAllocator, DefaultHonoursOverAlignment) {
    warm_up();
    g_global_max_align.store(0);
    op::DefaultPool<Wide> pool({.shards = 2, .initial_objects = 5, .grow_chunk = 3});
    std::vector<Wide*> held;
    for (int i = 0; i < 20; ++i) {
        held.push_back(pool.acquire());
        EXPECT_TRUE(aligned_to(held.back(), 128));
    }
    for (Wide* w : held) pool.release(w);
    if (kCountsGlobalNew) {
        EXPECT_GE(g_global_max_align.load(), 128u);
    }
}

// ------------------------------------------------------------ stateful allocator

// Every byte comes from the allocator (and no global operator new is called),
// every block is returned exactly once with its size and alignment.
TEST(PoolAllocator, CountingAllocatorSeesAllMemory) {
    warm_up();
    AllocStats stats;
    std::vector<Object*> seen;
    seen.reserve(16 * 50);  // the test's own memory, before the measured window
    const std::size_t global_before = g_global_news.load();
    {
        CountingPool pool({.shards = 4, .initial_objects = 16}, CountingAllocator(stats, 7));
        EXPECT_EQ(pool.get_allocator().id(), 7);
        EXPECT_EQ(stats.allocs, 4u);  // directory, shard array, cache array, initial chunk
        for (int round = 0; round < 50; ++round) {
            std::array<Object*, 16> held;
            for (Object*& o : held) o = pool.acquire();
            for (Object* o : held) {
                EXPECT_TRUE(stats.contains(o));
                seen.push_back(o);
                pool.release(o);
            }
        }
        EXPECT_EQ(stats.allocs, 4u);  // reuse allocates nothing
        EXPECT_EQ(stats.deallocs, 0u);
    }
    EXPECT_EQ(g_global_news.load(), global_before);
    EXPECT_EQ(std::set<Object*>(seen.begin(), seen.end()).size(), 16u);
    EXPECT_EQ(stats.errors, 0u);
    EXPECT_EQ(stats.deallocs, stats.allocs);
    EXPECT_EQ(stats.live_count, 0u);
    EXPECT_EQ(stats.bytes_live, 0u);
}

// Per-thread caches: one more block (the cache array, cache-line aligned) at
// construction, nothing on acquire/release (hits, refills, flushes), all
// returned at destruction, with objects still sitting in the caches.
TEST(PoolAllocator, ThreadCachesAllocateOnceAtConstruction) {
    warm_up();
    AllocStats stats;
    const std::size_t global_before = g_global_news.load();
    {
        CountingPool pool({.shards = 4,
                           .initial_objects = 64,
                           .thread_cache_capacity = 8,
                           .max_cached_threads = 4096},
                          CountingAllocator(stats, 3));
        EXPECT_EQ(stats.allocs, 4u);  // directory, shard array, cache array, initial chunk
        EXPECT_GE(stats.max_align, 64u);
        for (int round = 0; round < 50; ++round) {
            std::array<Object*, 40> held;
            for (Object*& o : held) o = pool.acquire();
            for (Object* o : held) pool.release(o);
        }
        EXPECT_EQ(stats.allocs, 4u);
        EXPECT_GT(pool.thread_cached_objects(), 0u);
    }
    EXPECT_EQ(g_global_news.load(), global_before);
    EXPECT_EQ(stats.errors, 0u);
    EXPECT_EQ(stats.deallocs, stats.allocs);
    EXPECT_EQ(stats.live_count, 0u);
    EXPECT_EQ(stats.bytes_live, 0u);
}

TEST(PoolAllocator, GrowthAcrossSeveralChunks) {
    warm_up();
    AllocStats stats;
    std::vector<Object*> held;
    held.reserve(100);
    const std::size_t global_before = g_global_news.load();
    {
        CountingPool pool({.shards = 2, .initial_objects = 4, .grow_chunk = 3},
                          CountingAllocator(stats, 1));
        for (int i = 0; i < 100; ++i) held.push_back(pool.acquire());
        for (Object* o : held) EXPECT_TRUE(stats.contains(o));
        // One block per growth step: max(grow_chunk, capacity / 16) objects.
        std::size_t capacity = 4;
        std::size_t growths = 0;
        for (; capacity < 100; ++growths) capacity += std::max<std::size_t>(3, capacity / 16);
        EXPECT_GT(growths, 20u);
        EXPECT_EQ(pool.capacity(), capacity);
        EXPECT_EQ(stats.allocs, 3u + 1u + growths);  // directory, shards, caches, initial
        EXPECT_EQ(stats.deallocs, 0u);  // type-stable: nothing freed while alive
        for (Object* o : held) pool.release(o);
        EXPECT_EQ(stats.deallocs, 0u);
    }
    EXPECT_EQ(g_global_news.load(), global_before);
    EXPECT_EQ(std::set<Object*>(held.begin(), held.end()).size(), 100u);
    EXPECT_EQ(stats.errors, 0u);
    EXPECT_EQ(stats.deallocs, stats.allocs);
    EXPECT_EQ(stats.live_count, 0u);
}

TEST(PoolAllocator, InstancesAreKeptApart) {
    AllocStats a_stats;
    AllocStats b_stats;
    {
        CountingPool a({.shards = 1, .initial_objects = 2, .grow_chunk = 1},
                       CountingAllocator(a_stats, 1));
        CountingPool b({.shards = 3, .initial_objects = 0}, CountingAllocator(b_stats, 2));
        EXPECT_EQ(a.get_allocator().id(), 1);
        EXPECT_EQ(b.get_allocator().id(), 2);
        Object* x = a.acquire();
        Object* y = b.acquire();
        EXPECT_TRUE(a_stats.contains(x));
        EXPECT_FALSE(b_stats.contains(x));
        EXPECT_TRUE(b_stats.contains(y));
        a.release(x);
        b.release(y);
        EXPECT_EQ(a_stats.allocs, 4u);
        EXPECT_EQ(b_stats.allocs, 4u);  // directory, shards, caches, first growth
    }
    for (AllocStats* s : {&a_stats, &b_stats}) {
        EXPECT_EQ(s->errors, 0u);
        EXPECT_EQ(s->deallocs, s->allocs);
        EXPECT_EQ(s->live_count, 0u);
    }
}

TEST(PoolAllocator, ConcurrentGrowth) {
    constexpr int kThreads = 8;
    constexpr int kPerThread = 200;
    AllocStats stats;
    {
        CountingPool pool({.shards = 4, .initial_objects = 0, .grow_chunk = 5},
                          CountingAllocator(stats, 3));
        std::vector<std::thread> threads;
        std::atomic<int> foreign{0};
        for (int t = 0; t < kThreads; ++t) {
            threads.emplace_back([&] {
                std::vector<Object*> held;
                held.reserve(kPerThread);
                for (int i = 0; i < kPerThread; ++i) held.push_back(pool.acquire());
                for (Object* o : held) {
                    if (!stats.contains(o)) foreign.fetch_add(1);
                    pool.release(o);
                }
            });
        }
        for (auto& th : threads) th.join();
        EXPECT_EQ(foreign.load(), 0);
        EXPECT_GE(pool.capacity(), std::size_t{kPerThread});
    }
    EXPECT_EQ(stats.errors, 0u);
    EXPECT_EQ(stats.deallocs, stats.allocs);
    EXPECT_EQ(stats.live_count, 0u);
}

// ------------------------------------------------------------ fail fast

static_assert(Poolable<Object>);
static_assert(!Poolable<ThrowingCtor>, "throwing T() must be rejected");

void build_pool_on_refusing_resource() {
    RefusingResource resource;
    op::DefaultPool<Object, fibers::sync::StdSync, op::PmrAllocator> pool({.shards = 2}, &resource);
}

struct Tiny {
    unsigned char byte = 0;
};

// One growth that needs more 2^24-node entries than the directory has; the
// size check happens before any allocation.
void build_pool_beyond_index_space() {
    op::DefaultPool<Tiny> pool({.shards = 1, .initial_objects = std::size_t{1} << 33});
}

// Allocation failure is fatal: no exception escapes the pool.
TEST(PoolAllocatorDeathTest, RefusedMemoryTerminates) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(build_pool_on_refusing_resource(), "");
}

// Running out of 32-bit node indices is detected and reported.
TEST(PoolAllocatorDeathTest, IndexSpaceExhaustionTerminates) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(build_pool_beyond_index_space(), "node index space exhausted");
}

// ------------------------------------------------------------ over-alignment

TEST(PoolAllocator, OverAlignedObjects) {
    AllocStats stats;
    {
        op::DefaultPool<Wide, fibers::sync::StdSync, CountingAllocator> pool(
            {.shards = 3, .initial_objects = 7, .grow_chunk = 5}, CountingAllocator(stats, 1));
        std::vector<Wide*> held;
        for (int i = 0; i < 40; ++i) {
            held.push_back(pool.acquire());
            EXPECT_TRUE(aligned_to(held.back(), 128));
            EXPECT_TRUE(stats.contains(held.back()));
        }
        EXPECT_EQ(std::set<Wide*>(held.begin(), held.end()).size(), held.size());
        for (Wide* w : held) pool.release(w);
    }
    EXPECT_GE(stats.max_align, 128u);  // chunks were requested 128-aligned
    EXPECT_EQ(stats.errors, 0u);
    EXPECT_EQ(stats.deallocs, stats.allocs);
    EXPECT_EQ(stats.live_count, 0u);
}

// ------------------------------------------------------------ std::pmr

TEST(PoolAllocator, PmrCountingResource) {
    warm_up();
    CountingResource resource;
    std::vector<Wide*> held;
    held.reserve(12);
    const std::size_t global_before = g_global_news.load();
    {
        op::DefaultPool<Wide, fibers::sync::StdSync, op::PmrAllocator> pool(
            {.shards = 2, .initial_objects = 3, .grow_chunk = 2}, &resource);
        EXPECT_EQ(pool.get_allocator().resource(), &resource);
        for (int i = 0; i < 12; ++i) {
            held.push_back(pool.acquire());
            EXPECT_TRUE(aligned_to(held.back(), 128));
            EXPECT_TRUE(resource.stats.contains(held.back()));
        }
        for (Wide* w : held) pool.release(w);
        EXPECT_EQ(resource.stats.deallocs, 0u);
    }
    EXPECT_EQ(g_global_news.load(), global_before);
    EXPECT_EQ(resource.stats.errors, 0u);
    EXPECT_GE(resource.stats.max_align, 128u);
    EXPECT_EQ(resource.stats.deallocs, resource.stats.allocs);
    EXPECT_EQ(resource.stats.live_count, 0u);
}

// A monotonic buffer with no upstream: everything must fit in the buffer, so
// any allocation outside the resource would be either a global new (counted)
// or a bad_alloc.
TEST(PoolAllocator, PmrMonotonicBuffer) {
    warm_up();
    alignas(128) static std::byte buffer[1 << 16];
    std::pmr::monotonic_buffer_resource arena(buffer, sizeof(buffer),
                                              std::pmr::null_memory_resource());
    std::vector<Object*> held;
    held.reserve(100);
    const std::size_t global_before = g_global_news.load();
    {
        op::DefaultPool<Object, fibers::sync::StdSync, op::PmrAllocator> pool(
            {.shards = 4, .initial_objects = 32, .grow_chunk = 16}, &arena);
        for (int i = 0; i < 100; ++i) held.push_back(pool.acquire());
        for (Object* o : held) {
            const auto* p = reinterpret_cast<const std::byte*>(o);
            EXPECT_TRUE(p >= buffer && p < buffer + sizeof(buffer));
        }
        for (Object* o : held) pool.release(o);
    }
    EXPECT_EQ(g_global_news.load(), global_before);
}

TEST(PoolAllocator, PmrDefaultResource) {
    CountingResource resource;
    std::pmr::memory_resource* previous = std::pmr::set_default_resource(&resource);
    {
        op::DefaultPool<Object, fibers::sync::StdSync, op::PmrAllocator> pool({.initial_objects = 4});
        EXPECT_EQ(pool.get_allocator().resource(), &resource);
        pool.release(pool.acquire());
    }
    std::pmr::set_default_resource(previous);
    EXPECT_EQ(resource.stats.allocs, 4u);
    EXPECT_EQ(resource.stats.errors, 0u);
    EXPECT_EQ(resource.stats.deallocs, resource.stats.allocs);
}
