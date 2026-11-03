#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <set>
#include <thread>
#include <vector>

#include "object_pool/sharded_object_pool.hpp"
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

using Pools = ::testing::Types<
    MutexVector<op::ShardSelect::kRoundRobin, std::mutex>,
    MutexVector<op::ShardSelect::kHome, std::mutex>,
    MutexVector<op::ShardSelect::kRoundRobin, thread_pool::SpinLock<>>,
    MutexVector<op::ShardSelect::kHome, thread_pool::SpinLock<>>>;

template <class Impl>
class PoolTest : public ::testing::Test {};
TYPED_TEST_SUITE(PoolTest, Pools);

static_assert(op::ObjectPool<op::MutexVectorPool<Object>>);

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
