#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <set>
#include <thread>
#include <utility>
#include <vector>

#include "thread_pool/thread_pool.hpp"

using namespace std::chrono_literals;
using thread_pool::LockedSPSCQueue;
using thread_pool::Task;
using thread_pool::ThreadPool;
using thread_pool::ThreadPoolOptions;

static_assert(thread_pool::MainQueue<LockedSPSCQueue<Task>>);

namespace {

std::atomic<std::size_t> g_mutex_locks{0};
std::atomic<std::size_t> g_spin_hints{0};

// Policy that overrides Mutex and spin_hint to prove they are actually used.
struct CountingSync : thread_pool::StdSync {
    struct Mutex {
        void lock() {
            g_mutex_locks.fetch_add(1, std::memory_order_relaxed);
            m.lock();
        }
        bool try_lock() {
            g_mutex_locks.fetch_add(1, std::memory_order_relaxed);
            return m.try_lock();
        }
        void unlock() { m.unlock(); }
        std::mutex m;
    };

    static void spin_hint() noexcept {
        g_spin_hints.fetch_add(1, std::memory_order_relaxed);
        thread_pool::cpu_relax();
    }
};

static_assert(thread_pool::SyncPolicy<CountingSync>);

// Spins until counter reaches `expected` or timeout expires.
bool wait_for(const std::atomic<std::size_t>& counter, std::size_t expected,
              std::chrono::seconds timeout = 60s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (counter.load() < expected) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::yield();
    }
    return counter.load() == expected;
}

void spawn_tree(ThreadPool<>& pool, std::atomic<std::size_t>& counter, int depth) {
    counter.fetch_add(1, std::memory_order_relaxed);
    if (depth == 0) return;
    pool.submit([&pool, &counter, depth] { spawn_tree(pool, counter, depth - 1); });
    pool.submit([&pool, &counter, depth] { spawn_tree(pool, counter, depth - 1); });
}

}  // namespace

TEST(ThreadPool, RunsTasksFromExternalThreads) {
    constexpr int kProducers = 4;
    constexpr std::size_t kPerProducer = 50'000;
    ThreadPool<> pool(ThreadPoolOptions{.num_workers = 4});
    std::atomic<std::size_t> counter{0};

    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&] {
            for (std::size_t i = 0; i < kPerProducer; ++i) {
                pool.submit([&counter] { counter.fetch_add(1, std::memory_order_relaxed); });
            }
        });
    }
    for (auto& t : producers) t.join();
    EXPECT_TRUE(wait_for(counter, kProducers * kPerProducer));
}

TEST(ThreadPool, RecursiveSpawnFromWorkers) {
    constexpr int kDepth = 16;
    ThreadPool<> pool(ThreadPoolOptions{.num_workers = 4});
    std::atomic<std::size_t> counter{0};
    pool.submit([&] { spawn_tree(pool, counter, kDepth); });
    EXPECT_TRUE(wait_for(counter, (std::size_t{1} << (kDepth + 1)) - 1));
}

TEST(ThreadPool, TinyQueuesFallBackToOverflow) {
    ThreadPool<> pool(ThreadPoolOptions{
        .num_workers = 3,
        .main_box_capacity = 2,
        .mail_box_shards = 1,
        .mail_box_shard_capacity = 2,
        .batch_size = 1,
        .mail_box_check_interval = 1,
    });
    std::atomic<std::size_t> counter{0};

    std::vector<std::thread> producers;
    for (int p = 0; p < 3; ++p) {
        producers.emplace_back([&] {
            for (int i = 0; i < 20'000; ++i) {
                pool.submit([&counter] { counter.fetch_add(1, std::memory_order_relaxed); });
            }
        });
    }
    for (auto& t : producers) t.join();
    EXPECT_TRUE(wait_for(counter, 60'000));
}

TEST(ThreadPool, TinyQueuesRecursiveSpawn) {
    constexpr int kDepth = 12;
    ThreadPool<> pool(ThreadPoolOptions{
        .num_workers = 3,
        .main_box_capacity = 2,
        .mail_box_shards = 1,
        .mail_box_shard_capacity = 2,
        .batch_size = 1,
    });
    std::atomic<std::size_t> counter{0};
    pool.submit([&] { spawn_tree(pool, counter, kDepth); });
    EXPECT_TRUE(wait_for(counter, (std::size_t{1} << (kDepth + 1)) - 1));
}

TEST(ThreadPool, WorkIsStolenByIdleWorkers) {
    constexpr std::size_t kTasks = 64;
    ThreadPool<> pool(ThreadPoolOptions{.num_workers = 4});
    std::atomic<std::size_t> counter{0};
    std::mutex mutex;
    std::set<std::thread::id> ids;

    // All children land in a single worker's main_box.
    pool.submit([&] {
        for (std::size_t i = 0; i < kTasks; ++i) {
            pool.submit([&] {
                std::this_thread::sleep_for(2ms);
                {
                    std::lock_guard guard(mutex);
                    ids.insert(std::this_thread::get_id());
                }
                counter.fetch_add(1);
            });
        }
    });
    ASSERT_TRUE(wait_for(counter, kTasks));
    std::lock_guard guard(mutex);
    EXPECT_GT(ids.size(), 1u);
}

TEST(ThreadPool, StopDropsPendingTasksWithoutLeaks) {
    constexpr std::size_t kTasks = 2'000;
    std::atomic<std::size_t> ran{0};
    std::atomic<std::size_t> dropped{0};

    struct Probe {
        std::atomic<std::size_t>* ran;
        std::atomic<std::size_t>* dropped;
        bool done = false;

        Probe(std::atomic<std::size_t>* r, std::atomic<std::size_t>* d) : ran(r), dropped(d) {}
        Probe(Probe&& o) noexcept : ran(o.ran), dropped(std::exchange(o.dropped, nullptr)) {}
        ~Probe() {
            if (dropped != nullptr && !done) dropped->fetch_add(1);
        }
        void operator()() {
            std::this_thread::sleep_for(100us);
            ran->fetch_add(1);
            done = true;
        }
    };

    {
        ThreadPool<> pool(ThreadPoolOptions{.num_workers = 2});
        for (std::size_t i = 0; i < kTasks; ++i) pool.submit(Probe(&ran, &dropped));
        std::this_thread::sleep_for(10ms);
    }
    EXPECT_EQ(ran.load() + dropped.load(), kTasks);
    EXPECT_GT(dropped.load(), 0u);
}

TEST(ThreadPool, InWorkerThread) {
    ThreadPool<> pool(ThreadPoolOptions{.num_workers = 1});
    EXPECT_FALSE(pool.in_worker_thread());
    std::atomic<std::size_t> inside{0};
    pool.submit([&] { inside.store(pool.in_worker_thread() ? 1 : 2); });
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (inside.load() == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    EXPECT_EQ(inside.load(), 1u);
}

TEST(ThreadPool, CustomSyncPolicyIsUsed) {
    std::atomic<std::size_t> counter{0};
    {
        ThreadPool<CountingSync> pool(ThreadPoolOptions{
            .num_workers = 2,
            .main_box_capacity = 2,
            .mail_box_shards = 1,
            .mail_box_shard_capacity = 2,
            .batch_size = 1,
        });
        for (int i = 0; i < 10'000; ++i) {
            pool.submit([&counter] { counter.fetch_add(1, std::memory_order_relaxed); });
        }
        EXPECT_TRUE(wait_for(counter, 10'000));
        // No work left: idle workers must reach Sync::spin_hint soon.
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (g_spin_hints.load() == 0 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
    }
    EXPECT_GT(g_mutex_locks.load(), 0u);  // overflow_box went through Sync::Mutex
    EXPECT_GT(g_spin_hints.load(), 0u);   // idle workers went through Sync::spin_hint
}

// A worker blocked inside a task that keeps spawning: everything it spawns must
// be executed by the other workers through work stealing.
TEST(ThreadPool, TasksOfBlockedWorkerAreStolen) {
    constexpr std::size_t kTasks = 200'000;
    ThreadPool<> pool(ThreadPoolOptions{.num_workers = 4});
    std::atomic<std::size_t> counter{0};
    std::atomic<std::size_t> ran_on_spammer{0};
    std::atomic<std::thread::id> spammer{};

    pool.submit([&] {
        spammer.store(std::this_thread::get_id());
        for (std::size_t i = 0; i < kTasks; ++i) {
            pool.submit([&] {
                if (std::this_thread::get_id() == spammer.load()) ran_on_spammer.fetch_add(1);
                counter.fetch_add(1, std::memory_order_relaxed);
            });
        }
        // Stay blocked until the others have drained everything (bounded).
        const auto deadline = std::chrono::steady_clock::now() + 60s;
        while (counter.load() < kTasks && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
    });
    EXPECT_TRUE(wait_for(counter, kTasks));
    EXPECT_EQ(ran_on_spammer.load(), 0u);
}

namespace {

void count_task(void* memo) noexcept {
    static_cast<std::atomic<std::size_t>*>(memo)->fetch_add(1, std::memory_order_relaxed);
}

std::vector<Task> make_count_tasks(std::atomic<std::size_t>& counter, std::size_t n) {
    std::vector<Task> tasks;
    tasks.reserve(n);
    for (std::size_t i = 0; i < n; ++i) tasks.emplace_back(&counter, &count_task, nullptr);
    return tasks;
}

}  // namespace

TEST(ThreadPool, SubmitBatchFromExternalThreads) {
    constexpr std::size_t kPerThread = 100'000;
    constexpr std::size_t kChunk = 1'000;
    ThreadPool<> pool(ThreadPoolOptions{.num_workers = 4});
    std::atomic<std::size_t> counter{0};

    std::vector<std::thread> producers;
    for (int p = 0; p < 2; ++p) {
        producers.emplace_back([&] {
            for (std::size_t i = 0; i < kPerThread; i += kChunk) {
                auto tasks = make_count_tasks(counter, kChunk);
                pool.submit_batch(tasks);
                for (const Task& t : tasks) ASSERT_FALSE(t);  // all moved from
            }
        });
    }
    for (auto& t : producers) t.join();
    EXPECT_TRUE(wait_for(counter, 2 * kPerThread));
}

TEST(ThreadPool, SubmitBatchOverflowsWithTinyQueues) {
    constexpr std::size_t kBatches = 500;
    constexpr std::size_t kChunk = 100;
    ThreadPool<> pool(ThreadPoolOptions{
        .num_workers = 3,
        .main_box_capacity = 2,
        .mail_box_shards = 1,
        .mail_box_shard_capacity = 2,
        .batch_size = 4,
    });
    std::atomic<std::size_t> counter{0};

    // From a worker: main_box (cap 2) + overflow_box.
    pool.submit([&] {
        for (std::size_t b = 0; b < kBatches; ++b) {
            auto tasks = make_count_tasks(counter, kChunk);
            pool.submit_batch(tasks);
        }
    });
    // From outside: mail_box shards (cap 2) + overflow_box.
    for (std::size_t b = 0; b < kBatches; ++b) {
        auto tasks = make_count_tasks(counter, kChunk);
        pool.submit_batch(tasks);
    }
    EXPECT_TRUE(wait_for(counter, 2 * kBatches * kChunk));
}

TEST(ThreadPool, WorksWithoutStealBackoff) {
    constexpr int kDepth = 14;
    ThreadPool<> pool(ThreadPoolOptions{.num_workers = 4, .max_steal_backoff = 0});
    std::atomic<std::size_t> counter{0};
    pool.submit([&] { spawn_tree(pool, counter, kDepth); });
    EXPECT_TRUE(wait_for(counter, (std::size_t{1} << (kDepth + 1)) - 1));
}
