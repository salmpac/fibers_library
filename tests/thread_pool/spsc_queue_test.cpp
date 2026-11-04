#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <thread>
#include <vector>

#include "fibers/queue/locked_spsc_queue.hpp"
#include "fibers/queue/spsc_queue.hpp"
#include "fibers/thread_pool/task.hpp"

using fibers::queue::LockedSPSCQueue;
using fibers::queue::SPSCQueue;
using fibers::thread_pool::Task;

TEST(SPSCQueue, CapacityRoundsUpToPowerOfTwo) {
    EXPECT_EQ(SPSCQueue<int>(0).capacity(), 1u);
    EXPECT_EQ(SPSCQueue<int>(5).capacity(), 8u);
    EXPECT_EQ(SPSCQueue<int>(16).capacity(), 16u);
}

TEST(SPSCQueue, FifoAndFull) {
    SPSCQueue<int> q(4);
    for (int i = 0; i < 4; ++i) {
        EXPECT_TRUE(q.try_push(int{i}));
    }
    int extra = 42;
    EXPECT_FALSE(q.try_push(std::move(extra)));
    EXPECT_EQ(q.free_space(), 0u);
    for (int i = 0; i < 4; ++i) {
        int v = -1;
        ASSERT_TRUE(q.try_pop(v));
        EXPECT_EQ(v, i);
    }
    int v;
    EXPECT_FALSE(q.try_pop(v));
}

TEST(SPSCQueue, BatchesWrapAround) {
    SPSCQueue<int> q(8);
    int next_in = 0, next_out = 0;
    for (int round = 0; round < 100; ++round) {
        int in[5];
        for (int& x : in) x = next_in++;
        ASSERT_EQ(q.try_push_batch(in, 5), 5u);
        int out[8];
        ASSERT_EQ(q.try_pop_batch(out, 8), 5u);
        for (int i = 0; i < 5; ++i) EXPECT_EQ(out[i], next_out++);
    }
}

TEST(SPSCQueue, PartialBatchPush) {
    SPSCQueue<int> q(4);
    int in[6] = {0, 1, 2, 3, 4, 5};
    EXPECT_EQ(q.try_push_batch(in, 6), 4u);
    EXPECT_EQ(q.size(), 4u);
}

TEST(SPSCQueue, FailedPushDoesNotConsume) {
    int deleted = 0;  // must outlive `q`: its remaining task bumps it on destruction
    SPSCQueue<Task> q(1);
    auto make = [&] {
        return Task(&deleted, [](void*) noexcept {},
                    [](void* m) noexcept { ++*static_cast<int*>(m); });
    };
    ASSERT_TRUE(q.try_push(make()));
    Task t = make();
    EXPECT_FALSE(q.try_push(std::move(t)));
    EXPECT_TRUE(t);
}

TEST(SPSCQueue, DestructionDropsRemainingTasks) {
    int deleted = 0;
    {
        SPSCQueue<Task> q(8);
        for (int i = 0; i < 5; ++i) {
            ASSERT_TRUE(q.try_push(Task(&deleted, [](void*) noexcept {},
                                        [](void* m) noexcept { ++*static_cast<int*>(m); })));
        }
        Task t;
        ASSERT_TRUE(q.try_pop(t));
        std::move(t).run();
    }
    EXPECT_EQ(deleted, 4);
}

TEST(SPSCQueue, ConcurrentProducerConsumerPreservesOrder) {
    constexpr std::size_t kCount = 1'000'000;
    SPSCQueue<std::size_t> q(1024);

    std::thread producer([&] {
        std::size_t buf[16];
        std::size_t next = 0;
        while (next < kCount) {
            const std::size_t n = std::min<std::size_t>(16, kCount - next);
            for (std::size_t i = 0; i < n; ++i) buf[i] = next + i;
            next += q.try_push_batch(buf, n);
        }
    });

    std::size_t expected = 0;
    std::size_t buf[32];
    while (expected < kCount) {
        const std::size_t n = q.try_pop_batch(buf, 32);
        for (std::size_t i = 0; i < n; ++i) {
            ASSERT_EQ(buf[i], expected++);
        }
    }
    producer.join();
}

TEST(LockedSPSCQueue, OwnerAndThievesSeeEachItemOnce) {
    constexpr std::size_t kCount = 500'000;
    constexpr int kThieves = 3;
    LockedSPSCQueue<std::size_t> q(256);

    std::atomic<std::size_t> consumed{0};
    std::atomic<std::size_t> sum{0};
    std::atomic<bool> done{false};

    std::vector<std::thread> thieves;
    for (int t = 0; t < kThieves; ++t) {
        thieves.emplace_back([&] {
            std::size_t buf[16];
            while (!done.load(std::memory_order_acquire)) {
                const std::size_t n = q.try_steal_batch(buf, 16);
                std::size_t local = 0;
                for (std::size_t i = 0; i < n; ++i) local += buf[i];
                sum.fetch_add(local);
                consumed.fetch_add(n);
            }
        });
    }

    // Owner: single producer, also pops.
    std::size_t next = 1;
    std::size_t buf[16];
    while (next <= kCount) {
        if (q.try_push(std::size_t{next})) ++next;
        if (next % 7 == 0) {
            const std::size_t n = q.try_pop_batch(buf, 16);
            std::size_t local = 0;
            for (std::size_t i = 0; i < n; ++i) local += buf[i];
            sum.fetch_add(local);
            consumed.fetch_add(n);
        }
    }
    while (consumed.load() < kCount) {
        const std::size_t n = q.try_pop_batch(buf, 16);
        std::size_t local = 0;
        for (std::size_t i = 0; i < n; ++i) local += buf[i];
        sum.fetch_add(local);
        consumed.fetch_add(n);
    }
    done.store(true, std::memory_order_release);
    for (auto& t : thieves) t.join();

    EXPECT_EQ(consumed.load(), kCount);
    EXPECT_EQ(sum.load(), kCount * (kCount + 1) / 2);
}
