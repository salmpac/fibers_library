// Stack (mmap + guard page) and StackPool (on the sharded object pool).

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstring>
#include <set>
#include <thread>
#include <vector>

#include "fibers/queue/spsc_queue.hpp"
#include "fibers/stack/stack.hpp"
#include "fibers/stack/stack_pool.hpp"

namespace stack = fibers::stack;
using stack::Stack;
using stack::StackPool;
using stack::StackPoolOptions;

namespace {

TEST(Stack, DefaultIsEmpty) {
    Stack s;
    EXPECT_TRUE(s.empty());
    EXPECT_TRUE(s.view().empty());
}

TEST(Stack, AllocateRoundsUpToPagesAndIsWritable) {
    const std::size_t page = Stack::page_size();
    Stack s = Stack::allocate(page + 1);
    ASSERT_FALSE(s.empty());
    EXPECT_EQ(s.size(), 2 * page);
    EXPECT_EQ(s.guard_size(), page);
    EXPECT_EQ(s.view().data(), s.mapping() + page);
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(s.view().data()) % page, 0u);
    std::memset(s.view().data(), 0xab, s.view().size());
    EXPECT_EQ(s.view()[s.size() - 1], std::byte{0xab});
}

TEST(Stack, DefaultSize) {
    Stack s = Stack::allocate();
    EXPECT_EQ(s.size(), Stack::kDefaultSize);
}

TEST(Stack, Move) {
    Stack a = Stack::allocate();
    std::byte* data = a.view().data();
    Stack b = std::move(a);
    EXPECT_TRUE(a.empty());  // NOLINT(bugprone-use-after-move): moved-from is empty
    EXPECT_EQ(b.view().data(), data);
    Stack c = Stack::allocate();
    c = std::move(b);
    EXPECT_EQ(c.view().data(), data);
}

TEST(StackDeathTest, GuardPageFaults) {
    Stack s = Stack::allocate();
    EXPECT_DEATH(
        {
            volatile std::byte* below = s.view().data() - 1;
            *below = std::byte{1};
        },
        "");
}

TEST(StackPool, ReusesStacks) {
    StackPool<> pool(StackPoolOptions{.stack_size = 16 * 1024, .max_stacks = 16});
    EXPECT_EQ(pool.mapped_stacks(), 0u);
    Stack* a = pool.acquire();
    ASSERT_FALSE(a->empty());
    EXPECT_GE(a->size(), 16u * 1024);
    EXPECT_EQ(pool.mapped_stacks(), 1u);
    pool.release(a);
    Stack* b = pool.acquire();
    EXPECT_EQ(b, a);  // from the thread cache
    EXPECT_EQ(pool.mapped_stacks(), 1u);
    pool.release(b);
}

TEST(StackPool, Preallocated) {
    StackPool<> pool(StackPoolOptions{.preallocated = 10, .max_stacks = 10});
    EXPECT_EQ(pool.mapped_stacks(), 10u);
    std::vector<Stack*> stacks;
    for (int i = 0; i < 10; ++i) {
        stacks.push_back(pool.acquire());
        EXPECT_FALSE(stacks.back()->empty());
    }
    EXPECT_EQ(pool.mapped_stacks(), 10u);  // none mapped on demand
    EXPECT_EQ(std::set<Stack*>(stacks.begin(), stacks.end()).size(), 10u);
    for (Stack* s : stacks) {
        pool.release(s);
    }
}

TEST(StackPool, UpToMax) {
    StackPool<> pool(StackPoolOptions{.max_stacks = 32, .thread_cache_capacity = 0});
    std::vector<Stack*> stacks;
    for (int round = 0; round < 3; ++round) {
        for (int i = 0; i < 32; ++i) {
            stacks.push_back(pool.acquire());
        }
        for (Stack* s : stacks) {
            pool.release(s);
        }
        stacks.clear();
    }
    EXPECT_EQ(pool.mapped_stacks(), 32u);
}

TEST(StackPoolDeathTest, BeyondMax) {
    EXPECT_DEATH(
        {
            StackPool<> pool(StackPoolOptions{.max_stacks = 4, .thread_cache_capacity = 0});
            for (int i = 0; i < 5; ++i) {
                pool.acquire();
            }
        },
        "more than max_stacks");
}

// Acquired on one thread, released on another (fibers migrate), through an
// SPSC queue; stacks are written by both sides.
TEST(StackPool, CrossThread) {
    constexpr std::size_t kMax = 64;
    StackPool<> pool(StackPoolOptions{.stack_size = 8 * 1024, .max_stacks = kMax});
    fibers::queue::SPSCQueue<Stack*> queue(16);
    constexpr int kItems = 100'000;
    std::thread consumer([&] {
        for (int i = 0; i < kItems; ++i) {
            Stack* s = nullptr;
            while (!queue.try_pop(s)) {
                std::this_thread::yield();
            }
            auto view = s->view();
            EXPECT_EQ(view[view.size() - 1], std::byte(i & 0xff));
            view[0] = std::byte{0};
            pool.release(s);
        }
        pool.flush_thread_cache();
    });
    for (int i = 0; i < kItems; ++i) {
        Stack* s = pool.acquire();
        auto view = s->view();
        view[view.size() - 1] = std::byte(i & 0xff);
        while (!queue.try_push(std::move(s))) {
            std::this_thread::yield();
        }
    }
    consumer.join();
    EXPECT_LE(pool.mapped_stacks(), kMax);
}

}  // namespace
