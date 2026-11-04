#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <thread>
#include <vector>

#include "fibers/queue/lock_free_spmc_queue.hpp"
#include "fibers/queue/locked_spsc_queue.hpp"
#include "fibers/thread_pool/task.hpp"
#include "fibers/thread_pool/thread_pool.hpp"

using fibers::queue::LockedSPSCQueue;
using fibers::queue::LockFreeSPMCQueue;
using fibers::thread_pool::Task;

static_assert(fibers::thread_pool::MainQueue<LockFreeSPMCQueue<Task>>);
static_assert(fibers::thread_pool::MainQueue<LockFreeSPMCQueue<>>);

namespace {


// call_ptr bumps `called`, delete_ptr bumps `deleted`.
struct Counters {
    std::atomic<int> called{0};
    std::atomic<int> deleted{0};
};

Task counted(Counters& c) {
    return Task(
        &c, [](void* m) noexcept { static_cast<Counters*>(m)->called.fetch_add(1); },
        [](void* m) noexcept { static_cast<Counters*>(m)->deleted.fetch_add(1); });
}

}  // namespace

// ------------------------------------------------------------ single thread

TEST(LockFreeSPMCQueue, CapacityRoundsUpToPowerOfTwo) {
    EXPECT_EQ(LockFreeSPMCQueue<int>(0).capacity(), 1u);
    EXPECT_EQ(LockFreeSPMCQueue<int>(5).capacity(), 8u);
    EXPECT_EQ(LockFreeSPMCQueue<int>(16).capacity(), 16u);
}

TEST(LockFreeSPMCQueue, FifoAndFull) {
    LockFreeSPMCQueue<int> q(4);
    EXPECT_EQ(q.free_space(), 4u);
    for (int i = 0; i < 4; ++i) {
        EXPECT_TRUE(q.try_push(int{i}));
        EXPECT_EQ(q.free_space(), static_cast<std::size_t>(3 - i));
    }
    int extra = 42;
    EXPECT_FALSE(q.try_push(std::move(extra)));
    EXPECT_EQ(q.free_space(), 0u);
    EXPECT_EQ(q.size_approx(), 4u);
    for (int i = 0; i < 4; ++i) {
        int v = -1;
        ASSERT_EQ(q.try_pop_batch(&v, 1), 1u);
        EXPECT_EQ(v, i);
    }
    int v = -1;
    EXPECT_EQ(q.try_pop_batch(&v, 1), 0u);
    EXPECT_EQ(q.try_steal_batch(&v, 1), 0u);
    EXPECT_EQ(q.free_space(), 4u);
    EXPECT_TRUE(q.empty_approx());
}

TEST(LockFreeSPMCQueue, StealIsFifoToo) {
    LockFreeSPMCQueue<int> q(8);
    int in[5] = {10, 11, 12, 13, 14};
    ASSERT_EQ(q.try_push_batch(in, 5), 5u);
    int out[8] = {};
    ASSERT_EQ(q.try_steal_batch(out, 2), 2u);
    EXPECT_EQ(out[0], 10);
    EXPECT_EQ(out[1], 11);
    ASSERT_EQ(q.try_pop_batch(out, 8), 3u);
    EXPECT_EQ(out[0], 12);
    EXPECT_EQ(out[2], 14);
}

TEST(LockFreeSPMCQueue, PartialBatches) {
    LockFreeSPMCQueue<int> q(4);
    int in[6] = {0, 1, 2, 3, 4, 5};
    EXPECT_EQ(q.try_push_batch(in, 6), 4u);
    EXPECT_EQ(q.try_push_batch(in + 4, 2), 0u);
    EXPECT_EQ(q.size_approx(), 4u);
    int out[3];
    EXPECT_EQ(q.try_pop_batch(out, 3), 3u);  // up to max
    EXPECT_EQ(out[2], 2);
    EXPECT_EQ(q.try_push_batch(in + 4, 2), 2u);  // only 3 free, 2 asked
    int rest[8];
    EXPECT_EQ(q.try_pop_batch(rest, 8), 3u);  // only what is there
    EXPECT_EQ(rest[0], 3);
    EXPECT_EQ(rest[1], 4);
    EXPECT_EQ(rest[2], 5);
}

TEST(LockFreeSPMCQueue, BatchesWrapAround) {
    LockFreeSPMCQueue<int> q(8);
    int next_in = 0, next_out = 0;
    for (int round = 0; round < 1000; ++round) {
        int in[5];
        for (int& x : in) x = next_in++;
        ASSERT_EQ(q.try_push_batch(in, 5), 5u);
        int out[8];
        const std::size_t n = round % 2 == 0 ? q.try_pop_batch(out, 8) : q.try_steal_batch(out, 8);
        ASSERT_EQ(n, 5u);
        for (int i = 0; i < 5; ++i) EXPECT_EQ(out[i], next_out++);
    }
}

TEST(LockFreeSPMCQueue, TaskRoundTrip) {
    Counters c;
    {
        LockFreeSPMCQueue<Task> q(4);
        ASSERT_TRUE(q.try_push(counted(c)));
        Task batch[2] = {counted(c), counted(c)};
        ASSERT_EQ(q.try_push_batch(batch, 2), 2u);
        EXPECT_FALSE(batch[0]);  // moved from
        EXPECT_FALSE(batch[1]);
        Task out[4];
        ASSERT_EQ(q.try_pop_batch(out, 4), 3u);
        for (Task& t : out) {
            if (t) std::move(t).run();
        }
        EXPECT_FALSE(out[3]);
    }
    EXPECT_EQ(c.called.load(), 3);
    EXPECT_EQ(c.deleted.load(), 0);
}

TEST(LockFreeSPMCQueue, FailedPushDoesNotConsume) {
    Counters c;  // must outlive `q`: its remaining task bumps it on destruction
    LockFreeSPMCQueue<Task> q(1);
    ASSERT_TRUE(q.try_push(counted(c)));
    Task t = counted(c);
    EXPECT_FALSE(q.try_push(std::move(t)));
    EXPECT_TRUE(t);
    Task batch[1] = {counted(c)};
    EXPECT_EQ(q.try_push_batch(batch, 1), 0u);
    EXPECT_TRUE(batch[0]);
}

TEST(LockFreeSPMCQueue, EmptyPopLeavesOutputEmpty) {
    LockFreeSPMCQueue<Task> q(4);
    Task out[2];
    EXPECT_EQ(q.try_pop_batch(out, 2), 0u);
    EXPECT_EQ(q.try_steal_batch(out, 2), 0u);
    EXPECT_EQ(q.try_pop_batch(out, 0), 0u);
    EXPECT_FALSE(out[0]);
    EXPECT_FALSE(out[1]);
}

TEST(LockFreeSPMCQueue, DestructionDropsRemainingTasks) {
    Counters c;
    {
        LockFreeSPMCQueue<Task> q(8);
        // Wrap around first, so the remaining range straddles the ring end.
        for (int round = 0; round < 3; ++round) {
            Task in[5] = {counted(c), counted(c), counted(c), counted(c), counted(c)};
            ASSERT_EQ(q.try_push_batch(in, 5), 5u);
            Task out[5];
            ASSERT_EQ(q.try_pop_batch(out, 5), 5u);
            for (Task& t : out) std::move(t).run();
        }
        for (int i = 0; i < 7; ++i) ASSERT_TRUE(q.try_push(counted(c)));
        Task t;
        ASSERT_EQ(q.try_steal_batch(&t, 1), 1u);
        std::move(t).run();
    }
    EXPECT_EQ(c.called.load(), 16);
    EXPECT_EQ(c.deleted.load(), 6);
}

// ------------------------------------------------------------ concurrency
//
// Owner = single producer that also pops; N thieves steal. Run for both main
// box implementations.

template <class Q>
class MainBoxQueue : public ::testing::Test {};

using MainBoxQueues = ::testing::Types<LockedSPSCQueue<Task>, LockFreeSPMCQueue<Task>>;
TYPED_TEST_SUITE(MainBoxQueue, MainBoxQueues);

namespace {

// Per-task memo: `payload` is plain memory written by the owner right before
// the push and read by whoever runs the task, so a broken publication is a
// data race for TSan; `hits` counts executions.
struct Item {
    std::size_t payload = 0;
    std::atomic<int> hits{0};
};

void run_item(void* m) noexcept {
    auto* item = static_cast<Item*>(m);
    if (item->payload != 1) std::abort();
    item->hits.fetch_add(1, std::memory_order_relaxed);
}

// Every task must run exactly once and none may be dropped.
template <class Q>
void owner_and_thieves(std::size_t capacity, int thieves, std::size_t count, std::size_t batch) {
    auto items = std::make_unique<Item[]>(count);
    auto make = [&](std::size_t i) {
        items[i].payload = 1;
        return Task(&items[i], &run_item,
                    [](void*) noexcept { std::abort(); });  // never dropped
    };
    std::atomic<std::size_t> consumed{0};
    std::atomic<bool> done{false};
    {
        Q q(capacity);
        std::vector<std::thread> threads;
        for (int t = 0; t < thieves; ++t) {
            threads.emplace_back([&] {
                std::vector<Task> buf(batch);
                while (!done.load(std::memory_order_acquire)) {
                    const std::size_t n = q.try_steal_batch(buf.data(), batch);
                    for (std::size_t i = 0; i < n; ++i) std::move(buf[i]).run();
                    consumed.fetch_add(n);
                }
            });
        }

        std::vector<Task> buf(batch);
        auto pop = [&] {
            const std::size_t n = q.try_pop_batch(buf.data(), batch);
            for (std::size_t i = 0; i < n; ++i) std::move(buf[i]).run();
            consumed.fetch_add(n);
        };
        std::size_t next = 0;
        while (next < count) {
            if (next % 3 == 0) {
                // batched push of up to 3
                Task in[3];
                std::size_t k = 0;
                for (; k < 3 && next + k < count; ++k) in[k] = make(next + k);
                const std::size_t pushed = q.try_push_batch(in, k);
                for (std::size_t i = pushed; i < k; ++i) static_cast<void>(in[i].release());
                next += pushed;
            } else {
                Task t = make(next);
                if (q.try_push(std::move(t))) {
                    ++next;
                } else {
                    static_cast<void>(t.release());  // not queued: recreated next time
                }
            }
            if (next % 7 == 0) pop();
        }
        while (consumed.load() < count) pop();
        done.store(true, std::memory_order_release);
        for (auto& t : threads) t.join();
        EXPECT_TRUE(q.try_pop_batch(buf.data(), batch) == 0);
    }
    EXPECT_EQ(consumed.load(), count);
    std::size_t bad = 0;
    for (std::size_t i = 0; i < count; ++i) bad += items[i].hits.load() != 1;
    EXPECT_EQ(bad, 0u);
}

}  // namespace

TYPED_TEST(MainBoxQueue, OwnerAndThievesRunEachTaskOnce) {
    owner_and_thieves<TypeParam>(256, 3, 300'000, 16);
}

TYPED_TEST(MainBoxQueue, TinyQueueManyThieves) {
    owner_and_thieves<TypeParam>(2, 7, 200'000, 4);
}

TYPED_TEST(MainBoxQueue, LargeBatchesExceedingCapacity) {
    owner_and_thieves<TypeParam>(8, 3, 200'000, 32);
}

// Producer never pops (a blocked worker): thieves drain everything, each
// thief sees its items in FIFO order.
TEST(LockFreeSPMCQueue, ThievesDrainBlockedOwnerInOrder) {
    constexpr std::size_t kCount = 500'000;
    constexpr int kThieves = 4;
    LockFreeSPMCQueue<std::size_t> q(64);
    std::atomic<std::size_t> consumed{0};
    std::atomic<std::size_t> sum{0};
    std::atomic<bool> disorder{false};

    std::vector<std::thread> thieves;
    for (int t = 0; t < kThieves; ++t) {
        thieves.emplace_back([&] {
            std::size_t buf[8];
            std::size_t last = 0;
            while (consumed.load() < kCount) {
                const std::size_t n = q.try_steal_batch(buf, 8);
                std::size_t local = 0;
                for (std::size_t i = 0; i < n; ++i) {
                    if (buf[i] <= last) disorder.store(true);
                    last = buf[i];
                    local += buf[i];
                }
                sum.fetch_add(local);
                consumed.fetch_add(n);
            }
        });
    }
    std::size_t in[16];
    for (std::size_t next = 1; next <= kCount;) {
        std::size_t k = 0;
        for (; k < 16 && next + k <= kCount; ++k) in[k] = next + k;
        next += q.try_push_batch(in, k);
    }
    for (auto& t : thieves) t.join();
    EXPECT_EQ(consumed.load(), kCount);
    EXPECT_EQ(sum.load(), kCount * (kCount + 1) / 2);
    EXPECT_FALSE(disorder.load());
}

// free_space() measured by the owner is a lower bound under concurrent steals:
// pushing that many always succeeds (ThreadPool::commit relies on it).
TEST(LockFreeSPMCQueue, FreeSpaceIsALowerBound) {
    constexpr std::size_t kRounds = 200'000;
    LockFreeSPMCQueue<std::size_t> q(16);
    std::atomic<bool> done{false};
    std::thread thief([&] {
        std::size_t buf[4];
        while (!done.load()) static_cast<void>(q.try_steal_batch(buf, 4));
    });
    std::size_t in[16] = {};
    bool ok = true;
    for (std::size_t r = 0; r < kRounds && ok; ++r) {
        const std::size_t free = q.free_space();
        ok = free <= 16 && q.try_push_batch(in, free) == free;
    }
    done.store(true);
    thief.join();
    EXPECT_TRUE(ok);
}
