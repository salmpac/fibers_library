#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <set>
#include <thread>
#include <vector>

#include "object_pool/atomic_pointer.hpp"
#include "object_pool/sharded_object_pool.hpp"
#include "object_pool/sharded_pool.hpp"
#include "thread_pool/spin_lock.hpp"

namespace op = object_pool;

namespace {

struct Object {
    std::atomic<int> owner{0};  // id of the thread holding it, 0 = free
    Object* prev = nullptr;
    Object* next = nullptr;
};

// Every pool implementation goes into `Pools`; tests run for all of them.
// An entry provides `Pool` and `options(PoolOptions)`.
template <op::ShardSelect kSelect, class Lock>
struct MutexVector {
    using Pool = op::MutexVectorPool<Object, op::StdSync, Lock>;
    static op::PoolOptions options(op::PoolOptions o) {
        o.select = kSelect;
        return o;
    }
};

template <class P, op::ShardSelect kSelect>
struct Sharded {
    using Pool = P;
    static op::PoolOptions options(op::PoolOptions o) {
        o.select = kSelect;
        return o;
    }
};

constexpr auto kRR = op::ShardSelect::kRoundRobin;
constexpr auto kHome = op::ShardSelect::kHome;

using Pools = ::testing::Types<
    MutexVector<kRR, std::mutex>,
    MutexVector<kHome, std::mutex>,
    MutexVector<kRR, thread_pool::SpinLock<>>,
    MutexVector<kHome, thread_pool::SpinLock<>>,
    Sharded<op::LockedListPool<Object>, kRR>,
    Sharded<op::LockedListPool<Object>, kHome>,
    Sharded<op::TaggedPool<Object>, kRR>,
    Sharded<op::TaggedPool<Object>, kHome>,
    Sharded<op::AtomicPointerPool<Object>, kRR>,
    Sharded<op::AtomicPointerPool<Object>, kHome>,
    Sharded<op::AtomicPointerValidatedPool<Object>, kRR>,
    Sharded<op::AtomicPointerValidatedPool<Object>, kHome>,
    Sharded<op::HazardPool<Object>, kRR>,
    Sharded<op::HazardPool<Object>, kHome>>;

template <class Impl>
class PoolTest : public ::testing::Test {};
TYPED_TEST_SUITE(PoolTest, Pools);

static_assert(op::ObjectPool<op::MutexVectorPool<Object>>);
static_assert(op::ObjectPool<op::LockedListPool<Object>>);
static_assert(op::ObjectPool<op::TaggedPool<Object>>);
static_assert(op::ObjectPool<op::AtomicPointerPool<Object>>);
static_assert(op::ObjectPool<op::HazardPool<Object>>);

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
    std::atomic<int> violations{0};

    std::vector<std::thread> threads;
    for (int t = 1; t <= kThreads; ++t) {
        threads.emplace_back([&, t] {
            Object* held[kHeld];
            for (int it = 0; it < kIterations; ++it) {
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
    EXPECT_LE(pool.capacity(), 8u + kThreads * kHeld + 256u * kThreads);
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
    std::size_t consumed = 0;
    while (consumed < kCount) {
        if (consumed < produced.load(std::memory_order_acquire)) {
            pool.release(handoff[consumed++]);
        }
    }
    producer.join();
    EXPECT_GE(pool.capacity(), 64u);
}

// Fewer max_threads than threads: the lock-free variants must only spin for a
// free slot (AtomicPointer tables, hazard slots), never deadlock or break.
TYPED_TEST(PoolTest, MoreThreadsThanMaxThreads) {
    constexpr int kThreads = 6;
    constexpr int kIterations = 20'000;
    auto options = TypeParam::options({.shards = 2, .initial_objects = 4, .grow_chunk = 2});
    options.max_threads = 1;
    typename TypeParam::Pool pool(options);
    std::atomic<int> violations{0};
    std::vector<std::thread> threads;
    for (int t = 1; t <= kThreads; ++t) {
        threads.emplace_back([&, t] {
            for (int it = 0; it < kIterations; ++it) {
                Object* o = pool.acquire();
                if (o->owner.exchange(t) != 0) violations.fetch_add(1);
                if (o->owner.exchange(0) != t) violations.fetch_add(1);
                pool.release(o);
            }
        });
    }
    for (auto& th : threads) th.join();
    EXPECT_EQ(violations.load(), 0);
}

// ------------------------------------------------------------ shard stacks

namespace {

using Arena = op::detail::NodeArena<Object, op::StdSync>;
using Hook = Arena::Hook;

op::detail::PoolThreadState& thread_state() {
    return op::detail::pool_thread_state<op::StdSync>();
}

template <class Stack>
struct StackHarness {
    explicit StackHarness(op::PoolOptions options = {}) : shared(options), stack(options) {}

    Hook* pop() { return stack.try_pop(arena, shared, thread_state()); }
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

using Stacks = ::testing::Types<op::LockedStack<op::StdSync>, op::TaggedStack<op::StdSync>,
                                op::AtomicPointerStack<op::StdSync>,
                                op::AtomicPointerStack<op::StdSync, op::ReadMode::kValidated>,
                                op::HazardStack<op::StdSync>>;
TYPED_TEST_SUITE(StackTest, Stacks);

std::uint64_t lcg(std::uint64_t& seed) {
    return seed = seed * 6364136223846793005ull + 1442695040888963407ull;
}

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
// exactly once (nothing lost, nothing duplicated). Also with fewer
// max_threads than threads.
TYPED_TEST(StackTest, StressExclusiveOwnership) {
    for (std::size_t max_threads : {std::size_t{8}, std::size_t{2}}) {
        constexpr int kThreads = 8;
        constexpr int kIterations = 40'000;
        constexpr std::size_t kNodes = 12;
        op::PoolOptions options;
        options.max_threads = max_threads;
        StackHarness<TypeParam> h(options);
        auto nodes = h.push_fresh(kNodes);
        std::atomic<int> violations{0};
        std::vector<std::thread> threads;
        for (int t = 1; t <= kThreads; ++t) {
            threads.emplace_back([&, t] {
                for (int it = 0; it < kIterations; ++it) {
                    Hook* held[2];
                    for (Hook*& n : held) {
                        while ((n = h.pop()) == nullptr) std::this_thread::yield();
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
        while (Hook* n = h.pop()) EXPECT_TRUE(back.insert(n).second) << "duplicated node";
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
            Hook* n;
            while ((n = h.pop()) == nullptr) std::this_thread::yield();
            Arena::value(n)->owner.store(1, std::memory_order_relaxed);
            auto& slot = handoff[i % kNodes];
            while (slot.load(std::memory_order_acquire) != nullptr) std::this_thread::yield();
            slot.store(n, std::memory_order_release);
        }
    });
    for (std::size_t i = 0; i < kCount; ++i) {
        auto& slot = handoff[i % kNodes];
        Hook* n;
        while ((n = slot.load(std::memory_order_acquire)) == nullptr) std::this_thread::yield();
        slot.store(nullptr, std::memory_order_relaxed);
        EXPECT_EQ(Arena::value(n)->owner.exchange(0, std::memory_order_relaxed), 1);
        h.push(n);
    }
    producer.join();
    std::set<Hook*> back;
    while (Hook* n = h.pop()) back.insert(n);
    EXPECT_EQ(back.size(), kNodes);
}

TEST(AtomicPointer, TransactionsCommitAndAbort) {
    op::AtomicPointer<std::uint32_t, op::StdSync> ap(1, 5u);
    std::size_t cursor = 0;
    auto add = [](std::uint32_t d) {
        return [d](std::uint32_t cur, std::uint32_t& next) {
            next = cur + d;
            return true;
        };
    };
    EXPECT_EQ(ap.transact<op::ReadMode::kPinned>(add(1), cursor), 5u);
    EXPECT_EQ(ap.transact<op::ReadMode::kValidated>(add(2), cursor), 6u);
    EXPECT_EQ(ap.load(), 8u);
    EXPECT_EQ(ap.transact<op::ReadMode::kPinned>([](std::uint32_t, std::uint32_t&) { return false; }, cursor),
              std::nullopt);
    EXPECT_EQ(ap.load(), 8u);
    // Many transactions on a 4-slot table: slots must be recycled.
    for (int i = 0; i < 1000; ++i) ap.transact<op::ReadMode::kPinned>(add(1), cursor);
    EXPECT_EQ(ap.load(), 1008u);
}

// Pinned and unpinned transactions mixed; no update may be lost.
TEST(AtomicPointer, ConcurrentIncrements) {
    constexpr int kThreads = 8;
    constexpr int kPerThread = 50'000;
    op::AtomicPointer<std::uint64_t, op::StdSync> ap(kThreads, 0);
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            std::uint64_t seed = static_cast<std::uint64_t>(t) + 1;
            std::size_t cursor = static_cast<std::size_t>(lcg(seed));
            auto inc = [](std::uint64_t cur, std::uint64_t& next) {
                next = cur + 1;
                return true;
            };
            for (int i = 0; i < kPerThread; ++i) {
                switch (t % 3) {
                    case 0: ap.transact<op::ReadMode::kPinned>(inc, cursor); break;
                    case 1: ap.transact<op::ReadMode::kValidated>(inc, cursor); break;
                    default: ap.transact<op::ReadMode::kRaw>(inc, cursor); break;
                }
            }
        });
    }
    for (auto& th : threads) th.join();
    EXPECT_EQ(ap.load(), std::uint64_t{kThreads} * kPerThread);
}
