// ThreadPool<TwistSync> under Twist.
//
// TWIST_SIM:    every scenario is explored with the Random and PCT schedulers
//               of the deterministic simulator; on failure the offending
//               schedule is printed and can be replayed.
// TWIST_FAULTY: every scenario runs several times on real threads with fault
//               injection (pairs well with TSan).
//
// Scenarios are tiny on purpose (simulation is slow), but queues are tiny too,
// so overflow, stealing from mail/main/overflow boxes and stop all get hit.

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <vector>

#include <twist/assist/assert.hpp>
#include <twist/cross.hpp>
#include <twist/ed/std/atomic.hpp>
#include <twist/ed/std/thread.hpp>

#if defined(__TWIST_SIM__)
#include <twist/sim.hpp>
#endif

#include "thread_pool/thread_pool.hpp"
#include "thread_pool/twist_sync.hpp"

namespace tp = thread_pool;

namespace {

using Pool = tp::ThreadPool<tp::TwistSync>;
using Counter = twist::ed::std::atomic<std::size_t>;

std::size_t env_or(const char* name, std::size_t fallback) {
    const char* value = std::getenv(name);
    return value != nullptr ? static_cast<std::size_t>(std::strtoull(value, nullptr, 10)) : fallback;
}

// Runs `body` under the configured Twist mode; reports a failing schedule.
// Env: FIBERS_TWIST_RUNS (runs per scheduler / repetitions), FIBERS_TWIST_SEED.
void check(const std::function<void()>& body) {
#if defined(__TWIST_SIM__)
    const std::size_t runs = env_or("FIBERS_TWIST_RUNS", 2'000);
    const std::uint64_t seed = env_or("FIBERS_TWIST_SEED", 42);
    {
        twist::sim::sched::RandomScheduler random{{.seed = seed, .max_runs = runs}};
        const auto explored = twist::sim::Explore(random, body);
        if (explored.found) {
            twist::sim::Print(body, explored.found->schedule);
            FAIL() << "Random scheduler, after " << explored.sims << " runs:\n"
                   << explored.found->result.std_err;
        }
    }
    {
        twist::sim::sched::PctScheduler pct{{.depth = 3, .seed = seed + 1, .max_runs = runs}};
        const auto explored = twist::sim::Explore(pct, body);
        if (explored.found) {
            twist::sim::Print(body, explored.found->schedule);
            FAIL() << "PCT scheduler, after " << explored.sims << " runs:\n"
                   << explored.found->result.std_err;
        }
    }
#else
    const std::size_t runs = env_or("FIBERS_TWIST_RUNS", 50);
    for (std::size_t i = 0; i < runs; ++i) {
        twist::cross::Run(body);
    }
#endif
}

tp::ThreadPoolOptions tiny(std::size_t workers) {
    return tp::ThreadPoolOptions{
        .num_workers = workers,
        .main_box_capacity = 2,
        .mail_box_shards = 2,
        .mail_box_shard_capacity = 2,
        .batch_size = 2,
        .mail_box_check_interval = 2,
        .max_steal_backoff = 4,  // each spin_hint is a yield in the simulator
    };
}

// Bounded wait: a lost task must fail the run (and print the schedule), not
// hang the simulator. In simulation the bound is in scheduling steps, on real
// threads in wall-clock time.
void wait_until(Counter& counter, std::size_t target) {
#if defined(__TWIST_SIM__)
    for (std::size_t step = 0; counter.load() < target; ++step) {
        TWIST_ASSERT_M(step < 5'000, "task lost: counter never reached target");
        twist::ed::std::this_thread::yield();
    }
#else
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (counter.load() < target) {
        TWIST_ASSERT_M(std::chrono::steady_clock::now() < deadline,
                       "task lost: counter never reached target");
        twist::ed::std::this_thread::yield();
    }
#endif
}

void count_task(void* memo) noexcept { static_cast<Counter*>(memo)->fetch_add(1); }

tp::Task count(Counter& counter) { return tp::Task(&counter, &count_task, nullptr); }

}  // namespace

TEST(ThreadPoolTwist, ExternalSubmitAndBatch) {
    check([] {
        Counter done{0};
        Pool pool(tiny(2));
        for (int i = 0; i < 4; ++i) {
            pool.submit(count(done));
        }
        std::vector<tp::Task> batch;
        for (int i = 0; i < 5; ++i) {
            batch.push_back(count(done));
        }
        pool.submit_batch(batch);
        wait_until(done, 9);
        pool.stop();
        TWIST_ASSERT_M(done.load() == 9, "every task runs exactly once");
    });
}

TEST(ThreadPoolTwist, ConcurrentExternalProducers) {
    // One mail_box shard per worker: producers have to share it.
    check([] {
        Counter done{0};
        auto options = tiny(2);
        options.mail_box_shards = 1;
        Pool pool(options);
        auto produce = [&] {
            pool.submit(count(done));
            pool.submit(count(done));
        };
        twist::ed::std::thread p1(produce);
        twist::ed::std::thread p2(produce);
        std::vector<tp::Task> batch;
        for (int i = 0; i < 3; ++i) {
            batch.push_back(count(done));
        }
        pool.submit_batch(batch);
        p1.join();
        p2.join();
        wait_until(done, 7);
        pool.stop();
        TWIST_ASSERT_M(done.load() == 7, "every task runs exactly once");
    });
}

TEST(ThreadPoolTwist, RecursiveSpawnFromWorkers) {
    // Binary tree of depth 3 = 15 tasks, spawned from inside the workers.
    check([] {
        Counter done{0};
        Pool pool(tiny(2));

        struct Node {
            Pool* pool;
            Counter* done;
            int index;
        };
        static constexpr int kDepth = 3;
        static constexpr int kNodes = (1 << (kDepth + 1)) - 1;
        std::vector<Node> nodes(kNodes);
        // Heap-indexed tree: node i has children 2i+1, 2i+2.
        for (int i = 0; i < kNodes; ++i) {
            nodes[i] = Node{&pool, &done, i};
        }
        struct Tree {
            static void visit(void* memo) noexcept {
                auto* node = static_cast<Node*>(memo);
                node->done->fetch_add(1);
                const int left = 2 * node->index + 1;
                if (left < kNodes) {
                    Node* base = node - node->index;
                    tp::Task children[2] = {tp::Task(base + left, &visit, nullptr),
                                            tp::Task(base + left + 1, &visit, nullptr)};
                    node->pool->submit_batch(children, 2);
                }
            }
        };
        pool.submit(tp::Task(&nodes[0], &Tree::visit, nullptr));
        wait_until(done, kNodes);
        pool.stop();
        TWIST_ASSERT_M(done.load() == static_cast<std::size_t>(kNodes), "whole tree visited");
    });
}

TEST(ThreadPoolTwist, TasksOfBlockedWorkerAreStolen) {
    check([] {
        static constexpr std::size_t kTasks = 6;
        Counter done{0};
        Counter spammer_finished{0};
        Pool pool(tiny(3));

        struct Spammer {
            Pool* pool;
            Counter* done;
            Counter* finished;
            static void run(void* memo) noexcept {
                auto* self = static_cast<Spammer*>(memo);
                for (std::size_t i = 0; i < kTasks; ++i) {
                    self->pool->submit(count(*self->done));
                }
                // Stay blocked: only the other workers can run the tasks.
                wait_until(*self->done, kTasks);
                self->finished->fetch_add(1);
            }
        } spammer{&pool, &done, &spammer_finished};

        pool.submit(tp::Task(&spammer, &Spammer::run, nullptr));
        wait_until(spammer_finished, 1);
        pool.stop();
        TWIST_ASSERT_M(done.load() == kTasks, "every task runs exactly once");
    });
}

TEST(ThreadPoolTwist, StopDropsPendingTasks) {
    check([] {
        static constexpr std::size_t kTasks = 8;
        Counter ran{0};
        Counter dropped{0};

        struct Memo {
            Counter* ran;
            Counter* dropped;
        } memo{&ran, &dropped};

        auto make = [&] {
            return tp::Task(
                &memo,
                [](void* m) noexcept { static_cast<Memo*>(m)->ran->fetch_add(1); },
                [](void* m) noexcept { static_cast<Memo*>(m)->dropped->fetch_add(1); });
        };

        {
            Pool pool(tiny(2));
            for (std::size_t i = 0; i < kTasks; ++i) {
                pool.submit(make());
            }
            pool.stop();
        }  // queues destroyed: pending tasks dropped
        TWIST_ASSERT_M(ran.load() + dropped.load() == kTasks,
                       "each task is either run or dropped, exactly once");
    });
}
