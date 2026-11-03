#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <thread>
#include <vector>

#include "thread_pool/sharded_queue.hpp"
#include "thread_pool/slow_queue.hpp"

using thread_pool::ShardedQueue;
using thread_pool::SlowQueue;

TEST(ShardedQueue, PushPopSingleThread) {
    ShardedQueue<int> q(3, 2);
    EXPECT_EQ(q.shard_count(), 3u);
    for (int i = 0; i < 6; ++i) {
        EXPECT_TRUE(q.try_push(int{i}, 0));  // spills over to other shards
    }
    int extra = 6;
    EXPECT_FALSE(q.try_push(std::move(extra), 0));

    int out[16];
    EXPECT_EQ(q.try_pop_batch(out, 16, 0), 6u);
    EXPECT_TRUE(q.empty_approx());
}

TEST(ShardedQueue, StealTakesBatchAcrossShards) {
    ShardedQueue<int> q(2, 8);
    for (int i = 0; i < 12; ++i) ASSERT_TRUE(q.try_push(int{i}, 0));  // 8 + 4
    int out[16];
    EXPECT_EQ(q.try_steal_batch(out, 5, 0), 5u);   // capped by batch
    EXPECT_EQ(q.try_steal_batch(out, 16, 0), 7u);  // 3 left in shard 0 + 4 in shard 1
    EXPECT_EQ(q.try_steal_batch(out, 16, 0), 0u);
}

TEST(ShardedQueue, ManyProducersOwnerAndThieves) {
    constexpr int kProducers = 4;
    constexpr std::size_t kPerProducer = 200'000;
    constexpr std::size_t kTotal = kProducers * kPerProducer;
    ShardedQueue<std::size_t> q(4, 128);

    std::atomic<std::size_t> consumed{0};
    std::atomic<std::size_t> sum{0};
    auto account = [&](const std::size_t* buf, std::size_t n) {
        std::size_t local = 0;
        for (std::size_t i = 0; i < n; ++i) local += buf[i];
        sum.fetch_add(local);
        consumed.fetch_add(n);
    };

    std::vector<std::thread> threads;
    for (int p = 0; p < kProducers; ++p) {
        threads.emplace_back([&, p] {
            for (std::size_t i = 0; i < kPerProducer; ++i) {
                std::size_t value = p * kPerProducer + i + 1;
                while (!q.try_push(std::move(value), p)) std::this_thread::yield();
            }
        });
    }
    // owner
    threads.emplace_back([&] {
        std::size_t buf[32], cursor = 0;
        while (consumed.load() < kTotal) account(buf, q.try_pop_batch(buf, 32, cursor++));
    });
    // thieves
    for (int t = 0; t < 2; ++t) {
        threads.emplace_back([&, t] {
            std::size_t buf[32];
            while (consumed.load() < kTotal) account(buf, q.try_steal_batch(buf, 32, t));
        });
    }
    for (auto& t : threads) t.join();

    EXPECT_EQ(consumed.load(), kTotal);
    EXPECT_EQ(sum.load(), kTotal * (kTotal + 1) / 2);
}

TEST(ShardedQueue, PushBatchSpillsAcrossShards) {
    ShardedQueue<int> q(2, 4);
    int in[10] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    EXPECT_EQ(q.try_push_batch(in, 10, 1), 8u);  // home shard 1, then shard 0
    int out[16];
    EXPECT_EQ(q.try_pop_batch(out, 16, 0), 8u);
    EXPECT_EQ(q.try_push_batch(in + 8, 2, 0), 2u);
}

TEST(SlowQueue, PushBatch) {
    SlowQueue<int> q;
    int in[5] = {0, 1, 2, 3, 4};
    q.push_batch(in, 5);
    q.push_batch(in, 0);
    int out[8];
    ASSERT_EQ(q.try_pop_batch(out, 8), 5u);
    for (int i = 0; i < 5; ++i) EXPECT_EQ(out[i], i);
}

TEST(SlowQueue, FifoBatches) {
    SlowQueue<int> q;
    EXPECT_FALSE(q.maybe_nonempty());
    for (int i = 0; i < 10; ++i) q.push(int{i});
    EXPECT_TRUE(q.maybe_nonempty());
    int out[4];
    ASSERT_EQ(q.try_pop_batch(out, 4), 4u);
    for (int i = 0; i < 4; ++i) EXPECT_EQ(out[i], i);
    ASSERT_EQ(q.try_pop_batch(out, 4), 4u);
    ASSERT_EQ(q.try_pop_batch(out, 4), 2u);
    EXPECT_FALSE(q.maybe_nonempty());
}
