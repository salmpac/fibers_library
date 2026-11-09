#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <random>
#include <set>
#include <thread>
#include <vector>

#include "fibers/object_pool/sharded_pool.hpp"

namespace op = fibers::object_pool;

namespace {

struct Object {
    std::atomic<int> owner{0};  // id of the thread holding it, 0 = free
    Object* prev = nullptr;
    Object* next = nullptr;
};

// Fail-fast guards: a broken pool or stack must fail the test, not hang or
// grow without bound (a run of the tests once ate ~28 GB when a mutant made a
// free list cyclic and a drain loop recorded failures forever). Every wait is
// bounded in time, every drain in steps, and stress loops check capacity().

// Yields until `ready()`; false after 30 s.
template <class Pred>
bool wait_for(Pred&& ready) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (!ready()) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::yield();
    }
    return true;
}

// Checked every kCapacityCheckEvery iterations of a stress loop: a pool whose
// capacity exceeds `bound` (objects in use + caches + grow slack) is growing
// instead of reusing; the worker records a violation and stops.
constexpr int kCapacityCheckEvery = 1024;

// Upper bound for capacity() when at most `out` objects are ever out of the
// shards at once (held, cached or in transit) and at most `growers` threads
// acquire. A pool grows only when its shards look empty, i.e. with capacity
// <= out (or still initial), by max(grow_chunk, capacity / 16) objects, and
// up to `growers` threads may grow at the same moment:
//   C <= out + growers * (grow_chunk + C / 16)
//   => C <= (out + growers * grow_chunk) * 16 / (16 - growers).
std::size_t capacity_bound(std::size_t initial, std::size_t out, std::size_t growers,
                           std::size_t grow_chunk) {
    return std::max(initial, (out + growers * grow_chunk) * 16 / (16 - growers) + 1);
}
template <class Pool>
bool capacity_runaway(const Pool& pool, std::size_t bound, int iteration) {
    return iteration % kCapacityCheckEvery == 0 && pool.capacity() > bound;
}

// Pool configurations under test: `Pool` and `options(PoolOptions)`.
// (Lock-based baselines live in benchmarks/ only.)
// kCache: PoolOptions::thread_cache_capacity (0 = no per-thread caches).
template <class P, op::ShardSelect kSelect, std::size_t kCache = 0>
struct Sharded {
    using Pool = P;
    static constexpr std::size_t kCacheCapacity = kCache;
    static op::PoolOptions options(op::PoolOptions o) {
        o.select = kSelect;
        o.thread_cache_capacity = kCache;
        return o;
    }
};

constexpr auto kRR = op::ShardSelect::kRoundRobin;
constexpr auto kHome = op::ShardSelect::kHome;

using Pools = ::testing::Types<Sharded<op::DefaultPool<Object>, kRR>,
                               Sharded<op::DefaultPool<Object>, kHome>,
                               Sharded<op::DefaultPool<Object>, kRR, 8>,
                               Sharded<op::DefaultPool<Object>, kHome, 8>,
                               Sharded<op::DefaultPool<Object>, kRR, 2>>;

template <class Impl>
class PoolTest : public ::testing::Test {};
TYPED_TEST_SUITE(PoolTest, Pools);

static_assert(op::ObjectPool<op::DefaultPool<Object>>);

}  // namespace

TYPED_TEST(PoolTest, ReusesObjectsWithoutGrowing) {
    typename TypeParam::Pool pool(TypeParam::options({.shards = 4, .initial_objects = 16}));
    std::set<Object*> seen;
    for (int round = 0; round < 100; ++round) {
        std::vector<Object*> held;
        for (int i = 0; i < 16; ++i) held.push_back(pool.acquire());
        EXPECT_EQ(std::set<Object*>(held.begin(), held.end()).size(), 16u);  // distinct
        for (Object* o : held) {
            seen.insert(o);
            pool.release(o);
        }
    }
    EXPECT_EQ(pool.capacity(), 16u);
    EXPECT_EQ(seen.size(), 16u);
}

TYPED_TEST(PoolTest, GrowsWhenExhausted) {
    typename TypeParam::Pool pool(
        TypeParam::options({.shards = 2, .initial_objects = 4, .grow_chunk = 3}));
    std::vector<Object*> held;
    for (int i = 0; i < 20; ++i) held.push_back(pool.acquire());
    EXPECT_EQ(std::set<Object*>(held.begin(), held.end()).size(), 20u);
    EXPECT_GE(pool.capacity(), 20u);
    for (Object* o : held) pool.release(o);
}

TYPED_TEST(PoolTest, StartsEmpty) {
    typename TypeParam::Pool pool(TypeParam::options({.shards = 3, .initial_objects = 0}));
    Object* o = pool.acquire();
    ASSERT_NE(o, nullptr);
    pool.release(o);
}

// No object is ever handed to two threads at once.
TYPED_TEST(PoolTest, ConcurrentExclusiveOwnership) {
    constexpr int kThreads = 8;
    constexpr int kIterations = 50'000;
    constexpr int kHeld = 4;
    typename TypeParam::Pool pool(TypeParam::options({.shards = 4, .initial_objects = 8}));
    const std::size_t bound = capacity_bound(
        8, kThreads * (kHeld + TypeParam::kCacheCapacity), kThreads, 256);
    std::atomic<int> violations{0};

    std::vector<std::thread> threads;
    for (int t = 1; t <= kThreads; ++t) {
        threads.emplace_back([&, t] {
            Object* held[kHeld];
            for (int it = 0; it < kIterations; ++it) {
                if (capacity_runaway(pool, bound, it)) {
                    violations.fetch_add(1);
                    return;
                }
                for (Object*& o : held) {
                    o = pool.acquire();
                    if (o->owner.exchange(t) != 0) violations.fetch_add(1);
                }
                for (Object* o : held) {
                    if (o->owner.exchange(0) != t) violations.fetch_add(1);
                    pool.release(o);
                }
            }
        });
    }
    for (auto& th : threads) th.join();
    EXPECT_EQ(violations.load(), 0);
    EXPECT_LE(pool.capacity(), bound);
}

// Objects acquired on one thread and released on another (task-like flow).
TYPED_TEST(PoolTest, CrossThreadRelease) {
    constexpr std::size_t kCount = 100'000;
    typename TypeParam::Pool pool(TypeParam::options({.shards = 4, .initial_objects = 64}));
    std::vector<Object*> handoff(kCount);
    std::atomic<std::size_t> produced{0};

    std::thread producer([&] {
        for (std::size_t i = 0; i < kCount; ++i) {
            handoff[i] = pool.acquire();
            produced.store(i + 1, std::memory_order_release);
        }
    });
    for (std::size_t consumed = 0; consumed < kCount; ++consumed) {
        if (!wait_for([&] { return consumed < produced.load(std::memory_order_acquire); })) {
            ADD_FAILURE() << "producer stuck";
            break;
        }
        pool.release(handoff[consumed]);
    }
    producer.join();
    EXPECT_GE(pool.capacity(), 64u);
    // The producer may run ahead by up to kCount objects (it alone grows),
    // plus the two threads' caches.
    EXPECT_LE(pool.capacity(), capacity_bound(64, kCount + 2 * TypeParam::kCacheCapacity, 1, 256));
}

// The per-thread state is shared by all pools: using a round-robin pool must
// not move the thread's home shard in a kHome pool. In kHome a released object
// is on top of the home shard, so the next acquire() returns it again.
TEST(PoolThreadState, RoundRobinPoolDoesNotMoveHomeShard) {
    op::DefaultPool<Object> home_pool({.shards = 8,
                                       .initial_objects = 8,
                                       .select = op::ShardSelect::kHome,
                                       .thread_cache_capacity = 0});
    op::DefaultPool<Object> rr_pool({.shards = 8,
                                     .initial_objects = 8,
                                     .select = op::ShardSelect::kRoundRobin,
                                     .thread_cache_capacity = 0});
    Object* mine = home_pool.acquire();
    for (int round = 0; round < 50; ++round) {
        home_pool.release(mine);
        for (int i = 0; i <= round % 5; ++i) {  // advance the round-robin counter
            rr_pool.release(rr_pool.acquire());
        }
        Object* again = home_pool.acquire();
        ASSERT_EQ(again, mine) << "home shard moved in round " << round;
    }
    home_pool.release(mine);
}

// ------------------------------------------------------------ shard stacks

namespace {

using Arena = op::detail::NodeArena<Object, fibers::sync::StdSync>;
using Hook = Arena::Hook;

op::detail::PoolThreadState& thread_state() {
    return op::detail::pool_thread_state<fibers::sync::StdSync>();
}

template <class Stack>
struct StackHarness {
    explicit StackHarness(op::PoolOptions options = {}) : shared(options), stack(options) {}

    Hook* pop() { return stack.try_pop(arena, shared, thread_state()); }
    op::HookChain<Hook> pop_chain(std::size_t max) {
        return stack.try_pop_chain(arena, shared, thread_state(), max);
    }
    // The nodes of a popped chain, first..last.
    std::vector<Hook*> nodes_of(const op::HookChain<Hook>& chain) {
        std::vector<Hook*> nodes;
        Hook* node = chain.first;
        for (std::size_t i = 0; i < chain.size; ++i) {
            nodes.push_back(node);
            if (i + 1 < chain.size) node = arena.hook(node->next.load());
        }
        if (chain.size != 0) {
            EXPECT_EQ(nodes.back(), chain.last);
        }
        return nodes;
    }
    void push(Hook* node) { push_chain(node, node); }
    void push_chain(Hook* first, Hook* last) {
        stack.push_chain(arena, shared, thread_state(), first, last);
    }
    // Pushes `count` fresh nodes one by one; returns them in push order.
    std::vector<Hook*> push_fresh(std::size_t count) {
        auto chain = arena.grow(count);
        std::vector<Hook*> nodes;
        Hook* node = chain.first;
        for (std::size_t i = 0; i < chain.size; ++i) {
            nodes.push_back(node);
            node = i + 1 < chain.size ? arena.hook(node->next.load()) : nullptr;
        }
        for (Hook* h : nodes) push(h);
        return nodes;
    }

    Arena arena;
    typename Stack::Shared shared;
    Stack stack;
};

template <class Stack>
class StackTest : public ::testing::Test {};

using Stacks = ::testing::Types<op::TaggedStack<fibers::sync::StdSync>>;
TYPED_TEST_SUITE(StackTest, Stacks);


}  // namespace

TYPED_TEST(StackTest, EmptyPopsNull) {
    StackHarness<TypeParam> h;
    EXPECT_EQ(h.pop(), nullptr);
    auto nodes = h.push_fresh(1);
    EXPECT_EQ(h.pop(), nodes[0]);
    EXPECT_EQ(h.pop(), nullptr);
}

TYPED_TEST(StackTest, SingleThreadLifo) {
    StackHarness<TypeParam> h;
    auto nodes = h.push_fresh(5);
    for (int round = 0; round < 3; ++round) {
        for (std::size_t i = nodes.size(); i-- > 0;) {
            EXPECT_EQ(h.pop(), nodes[i]);
        }
        EXPECT_EQ(h.pop(), nullptr);
        for (Hook* n : nodes) h.push(n);
    }
}

TYPED_TEST(StackTest, PushChainKeepsOrderAndPrevLinks) {
    StackHarness<TypeParam> h;
    auto chain = h.arena.grow(4);
    std::vector<Hook*> order;
    for (Hook* n = chain.first; order.size() < chain.size;) {
        order.push_back(n);
        if (order.size() < chain.size) n = h.arena.hook(n->next.load());
    }
    // A fresh chain is doubly linked.
    for (std::size_t i = 1; i < order.size(); ++i) {
        EXPECT_EQ(order[i]->prev, order[i - 1]->self);
    }
    EXPECT_EQ(order.back(), chain.last);
    auto below = h.push_fresh(1);
    h.push_chain(chain.first, chain.last);
    for (Hook* n : order) EXPECT_EQ(h.pop(), n);
    EXPECT_EQ(h.pop(), below[0]);
    EXPECT_EQ(h.pop(), nullptr);
}

TYPED_TEST(StackTest, ValueHookRoundTrip) {
    StackHarness<TypeParam> h;
    auto nodes = h.push_fresh(3);
    for (Hook* n : nodes) {
        Object* o = Arena::value(n);
        EXPECT_EQ(Arena::hook_of(o), n);
        EXPECT_EQ(h.arena.hook(n->self), n);
    }
}

// Every node is owned by at most one thread; at the end all nodes are back
// exactly once (nothing lost, nothing duplicated).
TYPED_TEST(StackTest, StressExclusiveOwnership) {
    {
        constexpr int kThreads = 8;
        constexpr int kIterations = 40'000;
        constexpr std::size_t kNodes = 12;
        op::PoolOptions options;
        StackHarness<TypeParam> h(options);
        auto nodes = h.push_fresh(kNodes);
        std::atomic<int> violations{0};
        std::vector<std::thread> threads;
        for (int t = 1; t <= kThreads; ++t) {
            threads.emplace_back([&, t] {
                for (int it = 0; it < kIterations; ++it) {
                    Hook* held[2];
                    for (Hook*& n : held) {
                        if (!wait_for([&] { return (n = h.pop()) != nullptr; })) {
                            violations.fetch_add(1);  // nodes lost: give up
                            return;
                        }
                        if (Arena::value(n)->owner.exchange(t) != 0) violations.fetch_add(1);
                    }
                    for (Hook* n : held) {
                        if (Arena::value(n)->owner.exchange(0) != t) violations.fetch_add(1);
                        h.push(n);
                    }
                }
            });
        }
        for (auto& th : threads) th.join();
        EXPECT_EQ(violations.load(), 0);
        std::set<Hook*> back;
        for (std::size_t i = 0; i <= 2 * kNodes; ++i) {  // bounded: a corrupt stack may cycle
            Hook* n = h.pop();
            if (n == nullptr) break;
            EXPECT_TRUE(back.insert(n).second) << "duplicated node";
        }
        EXPECT_EQ(back, std::set<Hook*>(nodes.begin(), nodes.end())) << "lost node";
    }
}

// Popped on one thread, pushed back on another.
TYPED_TEST(StackTest, CrossThreadRelease) {
    constexpr std::size_t kCount = 100'000;
    constexpr std::size_t kNodes = 16;
    StackHarness<TypeParam> h;
    h.push_fresh(kNodes);
    std::vector<std::atomic<Hook*>> handoff(kNodes);
    for (auto& slot : handoff) slot.store(nullptr);
    std::thread producer([&] {
        for (std::size_t i = 0; i < kCount; ++i) {
            Hook* n = nullptr;
            auto& slot = handoff[i % kNodes];
            if (!wait_for([&] { return (n = h.pop()) != nullptr; }) ||
                !wait_for([&] { return slot.load(std::memory_order_acquire) == nullptr; })) {
                ADD_FAILURE() << "producer stuck";
                return;
            }
            Arena::value(n)->owner.store(1, std::memory_order_relaxed);
            slot.store(n, std::memory_order_release);
        }
    });
    for (std::size_t i = 0; i < kCount; ++i) {
        auto& slot = handoff[i % kNodes];
        Hook* n = nullptr;
        if (!wait_for([&] { return (n = slot.load(std::memory_order_acquire)) != nullptr; })) {
            ADD_FAILURE() << "consumer stuck";
            break;
        }
        slot.store(nullptr, std::memory_order_relaxed);
        EXPECT_EQ(Arena::value(n)->owner.exchange(0, std::memory_order_relaxed), 1);
        h.push(n);
    }
    producer.join();
    std::set<Hook*> back;
    for (std::size_t i = 0; i <= 2 * kNodes; ++i) {  // bounded: a corrupt stack may cycle
        Hook* n = h.pop();
        if (n == nullptr) break;
        back.insert(n);
    }
    EXPECT_EQ(back.size(), kNodes);
}

// ------------------------------------------------------------ batch pop

TYPED_TEST(StackTest, PopChainEmpty) {
    StackHarness<TypeParam> h;
    for (std::size_t max : {1u, 2u, 8u}) {
        auto chain = h.pop_chain(max);
        EXPECT_EQ(chain.size, 0u);
        EXPECT_EQ(chain.first, nullptr);
        EXPECT_EQ(chain.last, nullptr);
    }
}

// Stack order (first = former top), partial when fewer nodes are left.
TYPED_TEST(StackTest, PopChainOrderAndPartial) {
    StackHarness<TypeParam> h;
    auto nodes = h.push_fresh(5);  // top: nodes[4]
    EXPECT_EQ(h.nodes_of(h.pop_chain(3)), (std::vector<Hook*>{nodes[4], nodes[3], nodes[2]}));
    EXPECT_EQ(h.nodes_of(h.pop_chain(1)), (std::vector<Hook*>{nodes[1]}));
    EXPECT_EQ(h.nodes_of(h.pop_chain(10)), (std::vector<Hook*>{nodes[0]}));
    EXPECT_EQ(h.pop_chain(4).size, 0u);
    EXPECT_EQ(h.pop(), nullptr);
}

// A popped chain can be pushed back as one chain; an exact fit empties it.
TYPED_TEST(StackTest, PopChainExactAndRepush) {
    StackHarness<TypeParam> h;
    auto nodes = h.push_fresh(4);
    auto chain = h.pop_chain(4);
    EXPECT_EQ(chain.size, 4u);
    EXPECT_EQ(h.pop(), nullptr);
    h.push_chain(chain.first, chain.last);
    for (std::size_t i = nodes.size(); i-- > 0;) EXPECT_EQ(h.pop(), nodes[i]);
    EXPECT_EQ(h.pop(), nullptr);
}

// Batch pops, single pops, chain pushes and single pushes from 8 threads:
// every node owned by at most one thread, all back exactly once at the end.
TYPED_TEST(StackTest, PopChainConcurrentStress) {
    constexpr int kThreads = 8;
    constexpr int kIterations = 30'000;
    constexpr std::size_t kNodes = 24;
    StackHarness<TypeParam> h;
    auto nodes = h.push_fresh(kNodes);
    std::atomic<int> violations{0};
    std::vector<std::thread> threads;
    for (int t = 1; t <= kThreads; ++t) {
        threads.emplace_back([&, t] {
            std::minstd_rand rng(static_cast<unsigned>(t));
            std::vector<Hook*> held;
            for (int it = 0; it < kIterations; ++it) {
                const std::size_t max = 1 + rng() % 5;
                if (rng() % 4 == 0) {
                    if (Hook* n = h.pop()) held.push_back(n);
                } else {
                    for (Hook* n : h.nodes_of(h.pop_chain(max))) held.push_back(n);
                }
                for (Hook* n : held) {
                    if (Arena::value(n)->owner.exchange(t) != 0) violations.fetch_add(1);
                }
                for (Hook* n : held) {
                    if (Arena::value(n)->owner.exchange(0) != t) violations.fetch_add(1);
                }
                if (held.empty()) {
                    std::this_thread::yield();
                } else if (rng() % 2 == 0) {
                    for (Hook* n : held) h.push(n);
                } else {
                    for (std::size_t i = 0; i + 1 < held.size(); ++i) {
                        held[i]->next.store(held[i + 1]->self, std::memory_order_relaxed);
                    }
                    h.push_chain(held.front(), held.back());
                }
                held.clear();
            }
        });
    }
    for (auto& th : threads) th.join();
    EXPECT_EQ(violations.load(), 0);
    std::set<Hook*> back;
    for (std::size_t i = 0; i <= 2 * kNodes; ++i) {  // bounded: a corrupt stack may cycle
        Hook* n = h.pop();
        if (n == nullptr) break;
        EXPECT_TRUE(back.insert(n).second) << "duplicated node";
    }
    EXPECT_EQ(back, std::set<Hook*>(nodes.begin(), nodes.end())) << "lost node";
}

// ------------------------------------------------------------ per-thread caches

namespace {

// Shard operations of CountingPool (tests run one at a time).
struct ShardOps {
    std::atomic<std::size_t> pops{0};        // try_pop calls
    std::atomic<std::size_t> chain_pops{0};  // try_pop_chain calls
    std::atomic<std::size_t> popped{0};      // nodes popped by either
    std::atomic<std::size_t> pushes{0};      // push_chain calls
    std::atomic<std::size_t> pushed{0};      // nodes pushed
    void reset() {
        pops = 0;
        chain_pops = 0;
        popped = 0;
        pushes = 0;
        pushed = 0;
    }
    std::size_t total() const { return pops + chain_pops + pushes; }
};
ShardOps g_ops;

// TaggedStack that counts its operations.
template <class Sync>
class CountingStack : public op::TaggedStack<Sync> {
    using Base = op::TaggedStack<Sync>;

public:
    using Hook = typename Base::Hook;
    using Base::Base;

    template <class A>
    Hook* try_pop(const A& arena, op::NoShared& shared, op::detail::PoolThreadState& thread) {
        ++g_ops.pops;
        Hook* node = Base::try_pop(arena, shared, thread);
        if (node != nullptr) ++g_ops.popped;
        return node;
    }
    template <class A>
    op::HookChain<Hook> try_pop_chain(const A& arena, op::NoShared& shared,
                                      op::detail::PoolThreadState& thread, std::size_t max) {
        ++g_ops.chain_pops;
        auto chain = Base::try_pop_chain(arena, shared, thread, max);
        g_ops.popped += chain.size;
        return chain;
    }
    template <class A>
    void push_chain(const A& arena, op::NoShared& shared, op::detail::PoolThreadState& thread,
                    Hook* first, Hook* last) {
        ++g_ops.pushes;
        std::size_t n = 1;
        for (Hook* h = first; h != last && n <= (1u << 20); h = arena.hook(h->next.load())) ++n;
        g_ops.pushed += n;
        Base::push_chain(arena, shared, thread, first, last);
    }
};

using CountingPool = op::ShardedPool<Object, CountingStack<fibers::sync::StdSync>>;

op::PoolOptions cached(std::size_t capacity, std::size_t objects = 64, std::size_t shards = 4) {
    return op::PoolOptions{.shards = shards,
                           .initial_objects = objects,
                           .grow_chunk = 16,
                           .thread_cache_capacity = capacity};
}

// Acquires capacity() objects on the calling thread (call it when every other
// cache is flushed): they must be distinct and the pool must not grow.
template <class Pool>
std::vector<Object*> drain(Pool& pool) {
    const std::size_t capacity = pool.capacity();
    std::vector<Object*> all;
    for (std::size_t i = 0; i < capacity; ++i) all.push_back(pool.acquire());
    EXPECT_EQ(pool.capacity(), capacity) << "objects were stranded or lost";
    EXPECT_EQ(std::set<Object*>(all.begin(), all.end()).size(), all.size()) << "duplicated";
    return all;
}

// The calling thread's id (its cache slot in every pool).
std::uint32_t my_slot() { return thread_state().cache_slot; }

// Runs `body(i)` on `n` threads that all hold their ids at the same time
// (each waits for the others after its first pool operation `first(i)`).
template <class First, class Body>
void run_concurrently(int n, First first, Body body) {
    std::atomic<int> ready{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < n; ++i) {
        threads.emplace_back([&, i] {
            first(i);
            ready.fetch_add(1);
            if (!wait_for([&] { return ready.load() >= n; })) ADD_FAILURE() << "barrier stuck";
            body(i);
        });
    }
    for (auto& t : threads) t.join();
}

}  // namespace

TEST(ThreadCache, OptionsNormalised) {
    op::DefaultPool<Object> defaults({.initial_objects = 0});  // caches on by default
    EXPECT_EQ(defaults.options().thread_cache_capacity, 8u);
    EXPECT_EQ(defaults.options().thread_cache_batch, 4u);
    EXPECT_EQ(defaults.options().max_cached_threads, 64u);
    op::DefaultPool<Object> off({.initial_objects = 0, .thread_cache_capacity = 0});
    EXPECT_EQ(off.options().thread_cache_capacity, 0u);
    EXPECT_EQ(off.options().max_cached_threads, 0u);
    op::DefaultPool<Object> on({.initial_objects = 0, .thread_cache_capacity = 8});
    EXPECT_EQ(on.options().thread_cache_batch, 4u);
    EXPECT_EQ(on.options().max_cached_threads, 64u);
    op::DefaultPool<Object> one({.initial_objects = 0, .thread_cache_capacity = 1});
    EXPECT_EQ(one.options().thread_cache_batch, 1u);
    op::DefaultPool<Object> big_batch(
        {.initial_objects = 0, .thread_cache_capacity = 4, .thread_cache_batch = 9});
    EXPECT_EQ(big_batch.options().thread_cache_batch, 4u);
    op::DefaultPool<Object> no_threads(
        {.initial_objects = 0, .thread_cache_capacity = 4, .max_cached_threads = 0});
    EXPECT_EQ(no_threads.options().thread_cache_capacity, 0u);
}

// After the first refill, acquire/release pairs and bursts within the cache
// touch no shard at all.
TEST(ThreadCache, HitPathDoesNoShardOperations) {
    CountingPool pool(cached(8));
    g_ops.reset();
    Object* first = pool.acquire();  // empty cache: one batch of 4 from a shard
    EXPECT_EQ(g_ops.chain_pops.load(), 1u);
    EXPECT_EQ(g_ops.popped.load(), 4u);
    EXPECT_EQ(pool.thread_cached_objects(), 3u);
    pool.release(first);
    g_ops.reset();
    for (int i = 0; i < 1000; ++i) {
        Object* a = pool.acquire();
        Object* b = pool.acquire();
        Object* c = pool.acquire();
        pool.release(b);
        pool.release(a);
        pool.release(c);
    }
    EXPECT_EQ(g_ops.total(), 0u);
    EXPECT_EQ(pool.thread_cached_objects(), 4u);
}

// Full cache: one push of `batch` objects, then the release goes to the
// cache. Empty cache: one pop of `batch` objects.
TEST(ThreadCache, FlushAndRefillMoveOneBatch) {
    CountingPool pool(cached(8, 64));
    std::vector<Object*> held;
    for (int i = 0; i < 20; ++i) held.push_back(pool.acquire());  // 5 refills of 4
    EXPECT_EQ(pool.thread_cached_objects(), 0u);
    g_ops.reset();
    for (int i = 0; i < 8; ++i) pool.release(held[i]);  // fills the cache
    EXPECT_EQ(g_ops.total(), 0u);
    pool.release(held[8]);  // full: flush 4, keep 4, + 1
    EXPECT_EQ(g_ops.pushes.load(), 1u);
    EXPECT_EQ(g_ops.pushed.load(), 4u);
    EXPECT_EQ(pool.thread_cached_objects(), 5u);
    for (int i = 9; i < 20; ++i) pool.release(held[i]);
    EXPECT_EQ(g_ops.pushes.load(), 3u);  // full again at held[12] and held[16]
    EXPECT_EQ(g_ops.pushed.load(), 12u);
    EXPECT_EQ(pool.thread_cached_objects(), 8u);
    // The cache keeps the hottest objects: the latest releases come back first.
    std::vector<Object*> mine;
    mine.push_back(pool.acquire());
    mine.push_back(pool.acquire());
    EXPECT_EQ(mine[0], held[19]);
    EXPECT_EQ(mine[1], held[18]);
    g_ops.reset();
    for (int i = 0; i < 6; ++i) mine.push_back(pool.acquire());  // empties the cache
    EXPECT_EQ(g_ops.total(), 0u);
    mine.push_back(pool.acquire());  // refill: 4 from a shard
    EXPECT_EQ(g_ops.chain_pops.load(), 1u);
    EXPECT_EQ(g_ops.popped.load(), 4u);
    EXPECT_EQ(pool.thread_cached_objects(), 3u);
    for (Object* o : mine) pool.release(o);
}

// Capacity 0: today's behaviour, one shard operation per acquire/release.
TEST(ThreadCache, DisabledGoesToShards) {
    CountingPool pool(cached(0));
    g_ops.reset();
    for (int i = 0; i < 100; ++i) pool.release(pool.acquire());
    EXPECT_GE(g_ops.pops.load(), 100u);  // plus probes of empty shards
    EXPECT_EQ(g_ops.popped.load(), 100u);
    EXPECT_EQ(g_ops.chain_pops.load(), 0u);
    EXPECT_EQ(g_ops.pushes.load(), 100u);
    EXPECT_EQ(pool.thread_cached_objects(), 0u);
    EXPECT_EQ(pool.cached_objects(), 0u);
    EXPECT_EQ(pool.flush_thread_cache(), 0u);
}

TEST(ThreadCache, GrowsIntoTheCache) {
    CountingPool pool({.shards = 2,
                       .initial_objects = 0,
                       .grow_chunk = 10,
                       .thread_cache_capacity = 8});
    g_ops.reset();
    Object* o = pool.acquire();  // grow 10: 1 out, 3 cached, 6 to a shard
    EXPECT_EQ(pool.capacity(), 10u);
    EXPECT_EQ(pool.thread_cached_objects(), 3u);
    EXPECT_EQ(g_ops.pushes.load(), 1u);
    EXPECT_EQ(g_ops.pushed.load(), 6u);
    pool.release(o);
    std::vector<Object*> all = drain(pool);
    for (Object* x : all) pool.release(x);
}

TEST(ThreadCache, FlushThreadCache) {
    CountingPool pool(cached(8, 16));
    std::vector<Object*> held;
    for (int i = 0; i < 6; ++i) held.push_back(pool.acquire());
    for (Object* o : held) pool.release(o);
    const std::size_t cached_before = pool.thread_cached_objects();
    EXPECT_EQ(cached_before, 8u);
    EXPECT_EQ(pool.cached_objects(), cached_before);
    g_ops.reset();
    EXPECT_EQ(pool.flush_thread_cache(), cached_before);
    EXPECT_EQ(g_ops.pushes.load(), 1u);  // one chain, one CAS
    EXPECT_EQ(g_ops.pushed.load(), cached_before);
    EXPECT_EQ(pool.thread_cached_objects(), 0u);
    EXPECT_EQ(pool.flush_thread_cache(), 0u);
    // Another thread gets every object.
    std::thread([&] {
        std::vector<Object*> all = drain(pool);
        EXPECT_EQ(all.size(), 16u);
        for (Object* o : all) pool.release(o);
        pool.flush_thread_cache();
    }).join();
    EXPECT_EQ(pool.cached_objects(), 0u);
}

// Thread ids are dense and recycled: concurrent threads hold distinct small
// ids; a thread started after another one exited gets the freed id.
TEST(ThreadCache, ThreadIdsAreDenseAndRecycled) {
    const std::uint32_t main_slot = my_slot();
    ASSERT_NE(main_slot, op::detail::kNoCacheSlot);
    constexpr int kThreads = 8;
    std::vector<std::uint32_t> slots(kThreads);
    run_concurrently(
        kThreads, [&](int i) { slots[static_cast<std::size_t>(i)] = my_slot(); }, [](int) {});
    std::set<std::uint32_t> distinct(slots.begin(), slots.end());
    distinct.insert(main_slot);
    EXPECT_EQ(distinct.size(), kThreads + 1u);
    // Nothing else runs: the ids are the kThreads + 1 smallest.
    EXPECT_LE(*distinct.rbegin(), static_cast<std::uint32_t>(kThreads));
    std::uint32_t first = 0;
    std::thread([&] { first = my_slot(); }).join();
    for (int i = 0; i < 5; ++i) {
        std::uint32_t again = 0;
        std::thread([&] { again = my_slot(); }).join();
        EXPECT_EQ(again, first) << "the id of an exited thread was not reused";
    }
}

// A thread that exits without flushing leaves its cached objects in its slot:
// still owned (capacity()), and the next thread that gets the id inherits
// them (hand-over ordered by the id registry's mutex).
TEST(ThreadCache, ExitedThreadsCacheIsInherited) {
    op::DefaultPool<Object> pool(cached(4, 8, 1));  // batch 2
    std::uint32_t exited_slot = 0;
    std::vector<Object*> left;
    std::thread([&] {
        exited_slot = my_slot();
        Object* a = pool.acquire();  // refill 2: a out, 1 cached
        Object* b = pool.acquire();
        a->owner.store(7);
        b->owner.store(7);
        pool.release(a);
        pool.release(b);
        left = {a, b};
        EXPECT_EQ(pool.thread_cached_objects(), 2u);
    }).join();
    EXPECT_EQ(pool.cached_objects(), 2u);
    std::thread([&] {
        ASSERT_EQ(my_slot(), exited_slot);
        EXPECT_EQ(pool.thread_cached_objects(), 2u);
        Object* x = pool.acquire();
        Object* y = pool.acquire();
        EXPECT_EQ(std::set<Object*>({x, y}), std::set<Object*>(left.begin(), left.end()));
        EXPECT_EQ(x->owner.exchange(0), 7);
        EXPECT_EQ(y->owner.exchange(0), 7);
        // The rest of the pool is in the shard: no growth for 6 more.
        std::vector<Object*> rest;
        for (int i = 0; i < 6; ++i) rest.push_back(pool.acquire());
        EXPECT_EQ(pool.capacity(), 8u);
        pool.release(x);
        pool.release(y);
        for (Object* o : rest) pool.release(o);
        pool.flush_thread_cache();
    }).join();
    EXPECT_EQ(pool.cached_objects(), 0u);
}

// Two pools used by the same thread have separate caches.
TEST(ThreadCache, PoolsDoNotShareCaches) {
    op::DefaultPool<Object> a(cached(8, 16));
    op::DefaultPool<Object> b(cached(8, 16));
    std::vector<Object*> from_a;
    for (int i = 0; i < 6; ++i) from_a.push_back(a.acquire());
    for (Object* o : from_a) a.release(o);
    EXPECT_EQ(a.thread_cached_objects(), 8u);
    EXPECT_EQ(b.thread_cached_objects(), 0u);
    std::set<Object*> a_cached;
    for (int i = 0; i < 8; ++i) a_cached.insert(a.acquire());
    for (Object* o : a_cached) a.release(o);
    std::vector<Object*> from_b;
    for (int i = 0; i < 16; ++i) {
        from_b.push_back(b.acquire());
        EXPECT_EQ(a_cached.count(from_b.back()), 0u) << "b handed out an object of a";
    }
    EXPECT_EQ(b.capacity(), 16u);
    for (Object* o : from_b) b.release(o);
    EXPECT_EQ(b.thread_cached_objects(), 8u);
    EXPECT_EQ(a.thread_cached_objects(), 8u);  // untouched by b
    for (int i = 0; i < 8; ++i) EXPECT_EQ(a_cached.count(a.acquire()), 1u);
}

// Objects acquired on one thread and released on another migrate through
// flushes (consumer) and refills (producer); with every cache flushed at the
// end, nothing is lost or duplicated.
TEST(ThreadCache, CrossThreadMigration) {
    constexpr std::size_t kCount = 200'000;
    CountingPool pool(cached(8, 64));
    std::vector<std::atomic<Object*>> ring(32);
    for (auto& slot : ring) slot.store(nullptr);
    g_ops.reset();
    // In flight at most: ring (32) + both caches (2 x 8) + 1 held < the 64
    // preallocated; only the producer acquires.
    const std::size_t bound = capacity_bound(64, 32 + 2 * 8 + 1, 1, 16);
    std::atomic<bool> stop{false};
    std::thread producer([&] {
        for (std::size_t i = 0; i < kCount && !stop.load(); ++i) {
            if (capacity_runaway(pool, bound, static_cast<int>(i))) {
                ADD_FAILURE() << "pool grows instead of reusing: " << pool.capacity();
                stop.store(true);
                return;
            }
            Object* o = pool.acquire();
            if (o->owner.exchange(1) != 0) ADD_FAILURE() << "object owned twice";
            auto& slot = ring[i % ring.size()];
            if (!wait_for([&] { return slot.load(std::memory_order_acquire) == nullptr; })) {
                ADD_FAILURE() << "producer stuck";
                stop.store(true);
                return;
            }
            slot.store(o, std::memory_order_release);
        }
        pool.flush_thread_cache();
    });
    std::thread consumer([&] {
        for (std::size_t i = 0; i < kCount; ++i) {
            auto& slot = ring[i % ring.size()];
            Object* o = nullptr;
            if (!wait_for([&] {
                    return (o = slot.load(std::memory_order_acquire)) != nullptr || stop.load();
                }) || o == nullptr) {
                if (!stop.load()) ADD_FAILURE() << "consumer stuck";
                stop.store(true);
                break;
            }
            slot.store(nullptr, std::memory_order_relaxed);
            if (o->owner.exchange(0) != 1) ADD_FAILURE() << "object lost its owner";
            pool.release(o);
        }
        pool.flush_thread_cache();
    });
    producer.join();
    consumer.join();
    // Batches only: one push per 4 releases, no single pops.
    EXPECT_EQ(g_ops.pops.load(), 0u);
    EXPECT_LE(g_ops.pushes.load(), kCount / 4 + 2);
    EXPECT_LE(pool.capacity(), bound);
    EXPECT_EQ(pool.cached_objects(), 0u);
    std::vector<Object*> all = drain(pool);
    for (Object* o : all) pool.release(o);
}

// Threads whose id does not fit max_cached_threads use the shards directly,
// side by side with cached threads.
TEST(ThreadCache, ThreadsBeyondMaxCachedThreads) {
    constexpr int kThreads = 8;
    op::PoolOptions options = cached(4, 64);
    options.max_cached_threads = 3;
    CountingPool pool(options);
    // Out of the shards: 8 x 3 held + 3 caches x 4.
    const std::size_t bound = capacity_bound(64, kThreads * 3 + 3 * 4, kThreads, 16);
    std::vector<std::uint32_t> slots(kThreads);
    std::vector<std::size_t> cached_after(kThreads);
    std::atomic<int> violations{0};
    run_concurrently(
        kThreads,
        [&](int i) {
            const auto t = static_cast<std::size_t>(i);
            slots[t] = my_slot();
            pool.release(pool.acquire());
            cached_after[t] = pool.thread_cached_objects();
        },
        [&](int i) {
            const int me = i + 1;
            Object* held[3];
            for (int it = 0; it < 30'000; ++it) {
                if (capacity_runaway(pool, bound, it)) {
                    violations.fetch_add(1);
                    return;
                }
                for (Object*& o : held) {
                    o = pool.acquire();
                    if (o->owner.exchange(me) != 0) violations.fetch_add(1);
                }
                for (Object* o : held) {
                    if (o->owner.exchange(0) != me) violations.fetch_add(1);
                    pool.release(o);
                }
            }
            pool.flush_thread_cache();
        });
    int uncached = 0;
    for (int i = 0; i < kThreads; ++i) {
        const auto t = static_cast<std::size_t>(i);
        if (slots[t] < 3) {
            EXPECT_EQ(cached_after[t], 2u) << "thread with id " << slots[t];
        } else {
            EXPECT_EQ(cached_after[t], 0u) << "thread with id " << slots[t];
            ++uncached;
        }
    }
    EXPECT_GE(uncached, kThreads - 3);  // ids are distinct while alive
    EXPECT_EQ(violations.load(), 0);
    EXPECT_LE(pool.capacity(), bound);
    pool.flush_thread_cache();
    EXPECT_EQ(pool.cached_objects(), 0u);
    std::vector<Object*> all = drain(pool);
    for (Object* o : all) pool.release(o);
}

// Exclusive ownership with tiny caches and a scarce pool: constant
// flush/refill between 8 threads; afterwards (all flushed) nothing lost.
TEST(ThreadCache, ScarceStressExclusiveOwnership) {
    constexpr int kThreads = 8;
    constexpr int kIterations = 50'000;
    op::DefaultPool<Object> pool({.shards = 2,
                                  .initial_objects = 16,
                                  .grow_chunk = 1,
                                  .thread_cache_capacity = 2,
                                  .thread_cache_batch = 2});
    // Out of the shards: 8 x (2 held + 2 cached).
    const std::size_t bound = capacity_bound(16, kThreads * 4, kThreads, 1);
    std::atomic<int> violations{0};
    std::vector<std::thread> threads;
    for (int t = 1; t <= kThreads; ++t) {
        threads.emplace_back([&, t] {
            for (int it = 0; it < kIterations; ++it) {
                if (capacity_runaway(pool, bound, it)) {
                    violations.fetch_add(1);
                    return;
                }
                Object* a = pool.acquire();
                if (a->owner.exchange(t) != 0) violations.fetch_add(1);
                Object* b = pool.acquire();
                if (b->owner.exchange(t) != 0) violations.fetch_add(1);
                if (a->owner.exchange(0) != t) violations.fetch_add(1);
                pool.release(a);
                if (b->owner.exchange(0) != t) violations.fetch_add(1);
                pool.release(b);
            }
            pool.flush_thread_cache();
        });
    }
    for (auto& th : threads) th.join();
    EXPECT_EQ(violations.load(), 0);
    EXPECT_LE(pool.capacity(), bound);
    EXPECT_EQ(pool.cached_objects(), 0u);
    std::vector<Object*> all = drain(pool);
    for (Object* o : all) EXPECT_EQ(o->owner.load(), 0);
    for (Object* o : all) pool.release(o);
}
