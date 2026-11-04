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
// Every pool scenario runs once per main_box implementation; the lock-free
// main_box also gets a direct owner + thieves scenario.

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <string>
#include <type_traits>
#include <vector>

#include <twist/assist/assert.hpp>
#include <twist/assist/shared.hpp>
#include <twist/cross.hpp>
#include <twist/ed/std/atomic.hpp>
#include <twist/ed/std/thread.hpp>

#if defined(__TWIST_SIM__)
#include <twist/sim.hpp>
#endif

#include "fibers/queue/lock_free_spmc_queue.hpp"
#include "fibers/thread_pool/thread_pool.hpp"
#include "fibers/sync/twist_sync.hpp"

namespace tp = fibers::thread_pool;
namespace q = fibers::queue;
using fibers::sync::TwistSync;

namespace {

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
        .idle_spins_before_park = 8,  // park early and often (~2-3 idle rounds)
    };
}

// Bounded waits: a lost task or wakeup must fail the run (and print the
// schedule), not hang the simulator. In simulation the bound is in scheduling
// steps, on real threads in wall-clock time.
#if defined(__TWIST_SIM__)
#define FIBERS_WAIT_BOUNDED(condition, message)                 \
    for (std::size_t step_ = 0; !(condition); ++step_) {        \
        TWIST_ASSERT_M(step_ < 5'000, message);                 \
        twist::ed::std::this_thread::yield();                   \
    }
#else
#define FIBERS_WAIT_BOUNDED(condition, message)                                       \
    for (const auto deadline_ = std::chrono::steady_clock::now() + std::chrono::seconds(30); \
         !(condition);) {                                                             \
        TWIST_ASSERT_M(std::chrono::steady_clock::now() < deadline_, message);        \
        twist::ed::std::this_thread::yield();                                         \
    }
#endif

void wait_until(Counter& counter, std::size_t target) {
    FIBERS_WAIT_BOUNDED(counter.load() >= target, "task lost: counter never reached target");
}

// Until every worker sleeps (they must: nothing is queued).
template <class Pool>
void wait_all_parked(const Pool& pool) {
    FIBERS_WAIT_BOUNDED(pool.parked_workers() == pool.num_workers(),
                        "idle workers never all parked");
}

void count_task(void* memo) noexcept { static_cast<Counter*>(memo)->fetch_add(1); }

tp::Task count(Counter& counter) { return tp::Task(&counter, &count_task, nullptr); }

using LockedPool = tp::ThreadPool<TwistSync, q::LockedSPSCQueue<tp::Task, TwistSync>>;
using LockFreePool = tp::ThreadPool<TwistSync, q::LockFreeSPMCQueue<tp::Task, TwistSync>>;

struct PoolName {
    template <class Pool>
    static std::string GetName(int) {
        return std::is_same_v<Pool, LockedPool> ? "LockedSPSC" : "LockFreeSPMC";
    }
};

}  // namespace

template <class Pool>
class ThreadPoolTwist : public ::testing::Test {};

using Pools = ::testing::Types<LockedPool, LockFreePool>;
TYPED_TEST_SUITE(ThreadPoolTwist, Pools, PoolName);

TYPED_TEST(ThreadPoolTwist, ExternalSubmitAndBatch) {
    using Pool = TypeParam;
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

TYPED_TEST(ThreadPoolTwist, ConcurrentExternalProducers) {
    using Pool = TypeParam;
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

TYPED_TEST(ThreadPoolTwist, RecursiveSpawnFromWorkers) {
    using Pool = TypeParam;
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

TYPED_TEST(ThreadPoolTwist, TasksOfBlockedWorkerAreStolen) {
    using Pool = TypeParam;
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

TYPED_TEST(ThreadPoolTwist, StopDropsPendingTasks) {
    using Pool = TypeParam;
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

// ---- parking: every lost wakeup must surface as a Twist deadlock (all
// threads blocked, e.g. in stop()'s join) or a failed bounded wait.

TYPED_TEST(ThreadPoolTwist, AllParkedThenExternalSubmit) {
    using Pool = TypeParam;
    check([] {
        Counter done{0};
        Pool pool(tiny(2));
        wait_all_parked(pool);
        pool.submit(count(done));
        wait_until(done, 1);
        wait_all_parked(pool);
        std::vector<tp::Task> batch;
        for (int i = 0; i < 3; ++i) {
            batch.push_back(count(done));
        }
        pool.submit_batch(batch);
        wait_until(done, 4);
        pool.stop();
        TWIST_ASSERT_M(done.load() == 4, "every task runs exactly once");
    });
}

TYPED_TEST(ThreadPoolTwist, SingleWorkerParkUnparkRace) {
    using Pool = TypeParam;
    // No waiting for the worker to park: submits race with it going to sleep.
    check([] {
        static constexpr std::size_t kTasks = 4;
        Counter done{0};
        Pool pool(tiny(1));
        for (std::size_t i = 0; i < kTasks; ++i) {
            pool.submit(count(done));
            wait_until(done, i + 1);
        }
        pool.stop();
        TWIST_ASSERT_M(done.load() == kTasks, "every task runs exactly once");
    });
}

TYPED_TEST(ThreadPoolTwist, BurstIdleBurst) {
    using Pool = TypeParam;
    check([] {
        Counter done{0};
        Pool pool(tiny(2));

        // A task that spawns two children locally (into its main_box).
        struct Spawner {
            Pool* pool;
            Counter* done;
            static void run(void* memo) noexcept {
                auto* self = static_cast<Spawner*>(memo);
                tp::Task children[2] = {count(*self->done), count(*self->done)};
                self->pool->submit_batch(children, 2);
                self->pool->submit(count(*self->done));
                self->done->fetch_add(1);
            }
        } spawner{&pool, &done};

        for (int round = 1; round <= 2; ++round) {
            pool.submit(tp::Task(&spawner, &Spawner::run, nullptr));
            pool.submit(count(done));
            pool.submit(count(done));
            wait_until(done, 6 * static_cast<std::size_t>(round));
            wait_all_parked(pool);  // idle in between
        }
        pool.stop();
        TWIST_ASSERT_M(done.load() == 12, "every task runs exactly once");
    });
}

TYPED_TEST(ThreadPoolTwist, StopWhileParked) {
    using Pool = TypeParam;
    check([] {
        Counter done{0};
        Pool pool(tiny(2));
        pool.submit(count(done));
        wait_until(done, 1);
        wait_all_parked(pool);
        pool.stop();  // a parked worker that is not woken deadlocks this join
    });
}

TYPED_TEST(ThreadPoolTwist, StopRacesWithParking) {
    using Pool = TypeParam;
    check([] {
        Counter done{0};
        Pool pool(tiny(2));
        pool.submit(count(done));
        pool.stop();  // workers may be anywhere between RUNNING and PARKED
        TWIST_ASSERT_M(done.load() <= 1, "at most the one task ran");
    });
}

TYPED_TEST(ThreadPoolTwist, ExternalProducersRaceWithParking) {
    using Pool = TypeParam;
    check([] {
        Counter done{0};
        Pool pool(tiny(2));
        auto produce = [&] {
            pool.submit(count(done));
            twist::ed::std::this_thread::yield();
            pool.submit(count(done));
        };
        twist::ed::std::thread p1(produce);
        twist::ed::std::thread p2(produce);
        p1.join();
        p2.join();
        wait_until(done, 4);
        pool.stop();
        TWIST_ASSERT_M(done.load() == 4, "every task runs exactly once");
    });
}

// The lock-free main_box on its own: the owner pushes (single and batched) and
// pops, two thieves steal. Each task's memo is plain shared data (checked for
// happens-before by the simulator) written by the owner before the push, so a
// broken publication shows up at the memo too, not only at the queue's probes.
TEST(LockFreeSPMCQueueTwist, OwnerAndThieves) {
    check([] {
        static constexpr std::size_t kTasks = 6;
        using Queue = q::LockFreeSPMCQueue<tp::Task, TwistSync>;

        struct Memo {
            twist::assist::Shared<std::size_t> payload{std::size_t{0}};
            Counter runs{0};
        };
        struct Work {
            static void run(void* m) noexcept {
                auto* memo = static_cast<Memo*>(m);
                TWIST_ASSERT_M(memo->payload.Read() == 7, "memo published");
                memo->runs.fetch_add(1);
            }
            static void drop(void* m) noexcept {
                auto* memo = static_cast<Memo*>(m);
                TWIST_ASSERT_M(memo->payload.Read() == 7, "memo published");
                memo->runs.fetch_add(1);  // dropped counts as consumed once
            }
        };

        Memo memos[kTasks];
        Counter stop{0};
        {
            Queue queue(2);
            auto steal = [&] {
                tp::Task buf[2];
                while (stop.load() == 0) {
                    const std::size_t n = queue.try_steal_batch(buf, 2);
                    for (std::size_t i = 0; i < n; ++i) std::move(buf[i]).run();
                    if (n == 0) twist::ed::std::this_thread::yield();
                }
            };
            twist::ed::std::thread thief1(steal);
            twist::ed::std::thread thief2(steal);

            tp::Task out[2];
            auto pop = [&] {
                const std::size_t n = queue.try_pop_batch(out, 2);
                for (std::size_t i = 0; i < n; ++i) std::move(out[i]).run();
            };
            for (std::size_t next = 0; next < kTasks;) {
                *memos[next].payload = 7;
                if (next % 2 == 0 && next + 1 < kTasks) {
                    *memos[next + 1].payload = 7;
                    tp::Task in[2] = {tp::Task(&memos[next], &Work::run, &Work::drop),
                                      tp::Task(&memos[next + 1], &Work::run, &Work::drop)};
                    const std::size_t pushed = queue.try_push_batch(in, 2);
                    for (std::size_t i = pushed; i < 2; ++i) static_cast<void>(in[i].release());
                    next += pushed;
                } else {
                    tp::Task task(&memos[next], &Work::run, &Work::drop);
                    if (queue.try_push(std::move(task))) {
                        ++next;
                    } else {
                        static_cast<void>(task.release());
                    }
                }
                if (next == 3) pop();
                if (queue.free_space() == 0) twist::ed::std::this_thread::yield();
            }
            pop();
            stop.store(1);
            thief1.join();
            thief2.join();
        }  // queue destroyed: whatever is left is dropped
        for (Memo& memo : memos) {
            TWIST_ASSERT_M(memo.runs.load() == 1, "each task taken exactly once");
        }
    });
}
