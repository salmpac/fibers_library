#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "thread_pool/lock_free_spmc_queue.hpp"
#include "thread_pool/thread_pool.hpp"

#include "heap_task.hpp"

using namespace std::chrono_literals;
using thread_pool::LockedSPSCQueue;
using thread_pool::LockFreeSPMCQueue;
using thread_pool::Task;
using thread_pool::ThreadPool;
using thread_pool::ThreadPoolOptions;
using thread_pool::test::heap_task;

static_assert(thread_pool::MainQueue<LockedSPSCQueue<Task>>);
static_assert(thread_pool::MainQueue<LockFreeSPMCQueue<Task>>);

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

// Every ThreadPool test runs once per main_box implementation.
struct LockedMainBox {
    template <class Sync>
    using Queue = LockedSPSCQueue<Task, Sync>;
};

struct LockFreeMainBox {
    template <class Sync>
    using Queue = LockFreeSPMCQueue<Task, Sync>;
};

template <class Box, class Sync = thread_pool::StdSync>
using PoolOf = ThreadPool<Sync, typename Box::template Queue<Sync>>;

struct MainBoxName {
    template <class Box>
    static std::string GetName(int) {
        return std::is_same_v<Box, LockedMainBox> ? "LockedSPSC" : "LockFreeSPMC";
    }
};

template <class Pool>
void spawn_tree(Pool& pool, std::atomic<std::size_t>& counter, int depth) {
    counter.fetch_add(1, std::memory_order_relaxed);
    if (depth == 0) return;
    pool.submit(heap_task([&pool, &counter, depth] { spawn_tree(pool, counter, depth - 1); }));
    pool.submit(heap_task([&pool, &counter, depth] { spawn_tree(pool, counter, depth - 1); }));
}

}  // namespace

template <class Box>
class ThreadPoolTest : public ::testing::Test {};

using MainBoxes = ::testing::Types<LockedMainBox, LockFreeMainBox>;
TYPED_TEST_SUITE(ThreadPoolTest, MainBoxes, MainBoxName);

TYPED_TEST(ThreadPoolTest, RunsTasksFromExternalThreads) {
    using Pool = PoolOf<TypeParam>;
    constexpr int kProducers = 4;
    constexpr std::size_t kPerProducer = 50'000;
    Pool pool(ThreadPoolOptions{.num_workers = 4});
    std::atomic<std::size_t> counter{0};

    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&] {
            for (std::size_t i = 0; i < kPerProducer; ++i) {
                pool.submit(heap_task([&counter] { counter.fetch_add(1, std::memory_order_relaxed); }));
            }
        });
    }
    for (auto& t : producers) t.join();
    EXPECT_TRUE(wait_for(counter, kProducers * kPerProducer));
}

TYPED_TEST(ThreadPoolTest, RecursiveSpawnFromWorkers) {
    using Pool = PoolOf<TypeParam>;
    constexpr int kDepth = 16;
    Pool pool(ThreadPoolOptions{.num_workers = 4});
    std::atomic<std::size_t> counter{0};
    pool.submit(heap_task([&] { spawn_tree(pool, counter, kDepth); }));
    EXPECT_TRUE(wait_for(counter, (std::size_t{1} << (kDepth + 1)) - 1));
}

TYPED_TEST(ThreadPoolTest, TinyQueuesFallBackToOverflow) {
    using Pool = PoolOf<TypeParam>;
    Pool pool(ThreadPoolOptions{
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
                pool.submit(heap_task([&counter] { counter.fetch_add(1, std::memory_order_relaxed); }));
            }
        });
    }
    for (auto& t : producers) t.join();
    EXPECT_TRUE(wait_for(counter, 60'000));
}

TYPED_TEST(ThreadPoolTest, TinyQueuesRecursiveSpawn) {
    using Pool = PoolOf<TypeParam>;
    constexpr int kDepth = 12;
    Pool pool(ThreadPoolOptions{
        .num_workers = 3,
        .main_box_capacity = 2,
        .mail_box_shards = 1,
        .mail_box_shard_capacity = 2,
        .batch_size = 1,
    });
    std::atomic<std::size_t> counter{0};
    pool.submit(heap_task([&] { spawn_tree(pool, counter, kDepth); }));
    EXPECT_TRUE(wait_for(counter, (std::size_t{1} << (kDepth + 1)) - 1));
}

TYPED_TEST(ThreadPoolTest, WorkIsStolenByIdleWorkers) {
    using Pool = PoolOf<TypeParam>;
    constexpr std::size_t kTasks = 64;
    Pool pool(ThreadPoolOptions{.num_workers = 4});
    std::atomic<std::size_t> counter{0};
    std::mutex mutex;
    std::set<std::thread::id> ids;

    // All children land in a single worker's main_box.
    pool.submit(heap_task([&] {
        for (std::size_t i = 0; i < kTasks; ++i) {
            pool.submit(heap_task([&] {
                std::this_thread::sleep_for(2ms);
                {
                    std::lock_guard guard(mutex);
                    ids.insert(std::this_thread::get_id());
                }
                counter.fetch_add(1);
            }));
        }
    }));
    ASSERT_TRUE(wait_for(counter, kTasks));
    std::lock_guard guard(mutex);
    EXPECT_GT(ids.size(), 1u);
}

TYPED_TEST(ThreadPoolTest, StopDropsPendingTasksWithoutLeaks) {
    using Pool = PoolOf<TypeParam>;
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
        Pool pool(ThreadPoolOptions{.num_workers = 2});
        for (std::size_t i = 0; i < kTasks; ++i) pool.submit(heap_task(Probe(&ran, &dropped)));
        std::this_thread::sleep_for(10ms);
    }
    EXPECT_EQ(ran.load() + dropped.load(), kTasks);
    EXPECT_GT(dropped.load(), 0u);
}

TYPED_TEST(ThreadPoolTest, InWorkerThread) {
    using Pool = PoolOf<TypeParam>;
    Pool pool(ThreadPoolOptions{.num_workers = 1});
    EXPECT_FALSE(pool.in_worker_thread());
    std::atomic<std::size_t> inside{0};
    pool.submit(heap_task([&] { inside.store(pool.in_worker_thread() ? 1 : 2); }));
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (inside.load() == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    EXPECT_EQ(inside.load(), 1u);
}

TYPED_TEST(ThreadPoolTest, CustomSyncPolicyIsUsed) {
    // Fresh counts for this instantiation (the globals are shared by all).
    g_mutex_locks.store(0);
    g_spin_hints.store(0);
    std::atomic<std::size_t> counter{0};
    {
        PoolOf<TypeParam, CountingSync> pool(ThreadPoolOptions{
            .num_workers = 2,
            .main_box_capacity = 2,
            .mail_box_shards = 1,
            .mail_box_shard_capacity = 2,
            .batch_size = 1,
        });
        for (int i = 0; i < 10'000; ++i) {
            pool.submit(heap_task([&counter] { counter.fetch_add(1, std::memory_order_relaxed); }));
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
TYPED_TEST(ThreadPoolTest, TasksOfBlockedWorkerAreStolen) {
    using Pool = PoolOf<TypeParam>;
    constexpr std::size_t kTasks = 200'000;
    std::atomic<std::size_t> counter{0};
    std::atomic<std::size_t> ran_on_spammer{0};
    std::atomic<std::thread::id> spammer{};
    // Declared last, so destroyed (joined) first: the spammer task may still
    // be reading `counter` when wait_for() below returns.
    Pool pool(ThreadPoolOptions{.num_workers = 4});

    pool.submit(heap_task([&] {
        spammer.store(std::this_thread::get_id());
        for (std::size_t i = 0; i < kTasks; ++i) {
            pool.submit(heap_task([&] {
                if (std::this_thread::get_id() == spammer.load()) ran_on_spammer.fetch_add(1);
                counter.fetch_add(1, std::memory_order_relaxed);
            }));
        }
        // Stay blocked until the others have drained everything (bounded).
        const auto deadline = std::chrono::steady_clock::now() + 60s;
        while (counter.load() < kTasks && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
    }));
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

TYPED_TEST(ThreadPoolTest, SubmitBatchFromExternalThreads) {
    using Pool = PoolOf<TypeParam>;
    constexpr std::size_t kPerThread = 100'000;
    constexpr std::size_t kChunk = 1'000;
    Pool pool(ThreadPoolOptions{.num_workers = 4});
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

TYPED_TEST(ThreadPoolTest, SubmitBatchOverflowsWithTinyQueues) {
    using Pool = PoolOf<TypeParam>;
    constexpr std::size_t kBatches = 500;
    constexpr std::size_t kChunk = 100;
    Pool pool(ThreadPoolOptions{
        .num_workers = 3,
        .main_box_capacity = 2,
        .mail_box_shards = 1,
        .mail_box_shard_capacity = 2,
        .batch_size = 4,
    });
    std::atomic<std::size_t> counter{0};

    // From a worker: main_box (cap 2) + overflow_box.
    pool.submit(heap_task([&] {
        for (std::size_t b = 0; b < kBatches; ++b) {
            auto tasks = make_count_tasks(counter, kChunk);
            pool.submit_batch(tasks);
        }
    }));
    // From outside: mail_box shards (cap 2) + overflow_box.
    for (std::size_t b = 0; b < kBatches; ++b) {
        auto tasks = make_count_tasks(counter, kChunk);
        pool.submit_batch(tasks);
    }
    EXPECT_TRUE(wait_for(counter, 2 * kBatches * kChunk));
}

TYPED_TEST(ThreadPoolTest, WorksWithoutStealBackoff) {
    using Pool = PoolOf<TypeParam>;
    constexpr int kDepth = 14;
    Pool pool(ThreadPoolOptions{.num_workers = 4, .max_steal_backoff = 0});
    std::atomic<std::size_t> counter{0};
    pool.submit(heap_task([&] { spawn_tree(pool, counter, kDepth); }));
    EXPECT_TRUE(wait_for(counter, (std::size_t{1} << (kDepth + 1)) - 1));
}
