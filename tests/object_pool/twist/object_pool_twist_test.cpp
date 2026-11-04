// Lock-free object pools (sharded_pool.hpp) with TwistSync under Twist.
//
// TWIST_SIM:    every scenario is explored with the Random and PCT schedulers
//               of the deterministic simulator; on failure the offending
//               schedule is printed and can be replayed.
// TWIST_FAULTY: every scenario runs several times on real threads with fault
//               injection (pairs well with TSan).
//
// Pools are tiny (one or two shards, 2-3 objects) and every thread holds two
// objects at once and releases them in acquisition order (pop A, pop B,
// push A: the classic ABA pattern). Checks:
//   - exclusive ownership: `owner` is claimed with a relaxed exchange, so a
//     node handed out twice is seen, while no happens-before is added;
//   - publication: `payload` is twist::assist::Shared, so a release -> acquire
//     hand-off that is not ordered by the pool is reported as a data race;
//   - no lost or duplicated nodes: afterwards all `capacity()` objects can be
//     acquired without the pool growing, and they are distinct.

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <vector>

#include <twist/assist/assert.hpp>
#include <twist/assist/shared.hpp>
#include <twist/cross.hpp>
#include <twist/ed/std/atomic.hpp>
#include <twist/ed/std/thread.hpp>

#if defined(__TWIST_SIM__)
#include <twist/sim.hpp>
#endif

#include "object_pool/sharded_pool.hpp"
#include "thread_pool/twist_sync.hpp"

namespace op = object_pool;
using thread_pool::TwistSync;

namespace {

std::size_t env_or(const char* name, std::size_t fallback) {
    const char* value = std::getenv(name);
    return value != nullptr ? static_cast<std::size_t>(std::strtoull(value, nullptr, 10)) : fallback;
}

// Runs `body` under the configured Twist mode; reports a failing schedule.
// Env: FIBERS_TWIST_RUNS (runs per scheduler / repetitions), FIBERS_TWIST_SEED,
// FIBERS_TWIST_PCT_DEPTH (default 3), FIBERS_TWIST_VERBOSE.
void check(const std::function<void()>& body) {
#if defined(__TWIST_SIM__)
    const std::size_t runs = env_or("FIBERS_TWIST_RUNS", 2'000);
    const std::uint64_t seed = env_or("FIBERS_TWIST_SEED", 42);
    const std::size_t depth = env_or("FIBERS_TWIST_PCT_DEPTH", 3);
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
        twist::sim::sched::PctScheduler pct{
            {.depth = depth, .seed = seed + 1, .max_runs = runs}};
        const auto explored = twist::sim::Explore(pct, body);
        if (explored.found) {
            twist::sim::Print(body, explored.found->schedule);
            FAIL() << "PCT scheduler, after " << explored.sims << " runs:\n"
                   << explored.found->result.std_err;
        }
    }
#else
    // A failure panics (aborts); FIBERS_TWIST_VERBOSE=1 shows which repetition.
    const std::size_t runs = env_or("FIBERS_TWIST_RUNS", 50);
    const bool verbose = env_or("FIBERS_TWIST_VERBOSE", 0) != 0;
    for (std::size_t i = 0; i < runs; ++i) {
        if (verbose) {
            std::fprintf(stderr, "[twist] repetition %zu\n", i + 1);
        }
        twist::cross::Run(body);
    }
#endif
}

// Simulation explores interleavings, so a couple of rounds suffice there; on
// real threads (fault injection) longer runs are needed to hit races.
#if defined(__TWIST_SIM__)
constexpr int kRounds = 2;
constexpr int kItems = 3;
#else
constexpr int kRounds = 300;
constexpr int kItems = 300;
#endif

// Bounded wait: a stuck hand-off must fail the run (and print the schedule),
// not hang. Bound in scheduling steps (sim) or wall-clock time (threads).
template <class Pred>
void wait_until(Pred&& done) {
#if defined(__TWIST_SIM__)
    for (std::size_t step = 0; !done(); ++step) {
        TWIST_ASSERT_M(step < 10'000, "hand-off stuck");
        twist::ed::std::this_thread::yield();
    }
#else
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (!done()) {
        TWIST_ASSERT_M(std::chrono::steady_clock::now() < deadline, "hand-off stuck");
        twist::ed::std::this_thread::yield();
    }
#endif
}

struct Object {
    // Twist's atomic/Shared constructors are not declared noexcept; the pool
    // requires a nothrow T(), so promise it here (a throw would terminate).
    Object() noexcept : owner(0), payload(0) {}

    twist::ed::std::atomic<int> owner;  // thread holding it, 0 = free
    twist::assist::Shared<int> payload;
};

void take(Object* o, int me) {
    TWIST_ASSERT_M(o->owner.exchange(me, std::memory_order_relaxed) == 0,
                   "object handed out to two threads at once");
    *o->payload = me;  // plain write: must be ordered after the previous owner's
}

void give_back(Object* o, int me) {
    TWIST_ASSERT_M(static_cast<int>(o->payload) == me, "payload changed while owned");
    TWIST_ASSERT_M(o->owner.exchange(0, std::memory_order_relaxed) == me,
                   "object lost its owner while held");
}

template <class Pool>
void verify_all_back(Pool& pool) {
    const std::size_t capacity = pool.capacity();
    std::vector<Object*> all;
    for (std::size_t i = 0; i < capacity; ++i) {
        all.push_back(pool.acquire());
    }
    TWIST_ASSERT_M(pool.capacity() == capacity, "a node was lost (pool had to grow)");
    for (std::size_t i = 0; i < all.size(); ++i) {
        TWIST_ASSERT_M(all[i]->owner.load(std::memory_order_relaxed) == 0, "owned node in pool");
        for (std::size_t j = 0; j < i; ++j) {
            TWIST_ASSERT_M(all[i] != all[j], "a node was in the free lists twice");
        }
    }
    for (Object* o : all) {
        pool.release(o);
    }
}

// `threads` threads, each `rounds` times: acquire A, acquire B, release A,
// release B.
template <class Pool>
void churn(op::PoolOptions options, int threads, int rounds) {
    check([=] {
        Pool pool(options);
        std::vector<twist::ed::std::thread> workers;
        for (int t = 1; t <= threads; ++t) {
            workers.emplace_back([&pool, t, rounds] {
                for (int r = 0; r < rounds; ++r) {
                    Object* a = pool.acquire();
                    take(a, t);
                    Object* b = pool.acquire();
                    take(b, t);
                    give_back(a, t);
                    pool.release(a);
                    give_back(b, t);
                    pool.release(b);
                }
            });
        }
        for (auto& w : workers) {
            w.join();
        }
        verify_all_back(pool);
    });
}

// `threads` threads, each `rounds` times: acquire one, release it. With fewer
// objects than threads the free list runs empty all the time.
template <class Pool>
void single_churn(op::PoolOptions options, int threads, int rounds) {
    check([=] {
        Pool pool(options);
        std::vector<twist::ed::std::thread> workers;
        for (int t = 1; t <= threads; ++t) {
            workers.emplace_back([&pool, t, rounds] {
                for (int r = 0; r < rounds; ++r) {
                    Object* o = pool.acquire();
                    take(o, t);
                    give_back(o, t);
                    pool.release(o);
                }
            });
        }
        for (auto& w : workers) {
            w.join();
        }
        verify_all_back(pool);
    });
}

// Producer acquires, consumer releases (objects migrate between threads).
template <class Pool>
void hand_off(op::PoolOptions options, int items) {
    check([=] {
        Pool pool(options);
        twist::ed::std::atomic<Object*> slot{nullptr};
        twist::ed::std::thread consumer([&] {
            for (int i = 0; i < items; ++i) {
                Object* o = nullptr;
                wait_until([&] { return (o = slot.exchange(nullptr)) != nullptr; });
                give_back(o, 1);
                pool.release(o);
            }
        });
        for (int i = 0; i < items; ++i) {
            Object* o = pool.acquire();
            take(o, 1);
            wait_until([&] { return slot.load() == nullptr; });
            slot.store(o);
        }
        // a third thread churns on the same shards meanwhile
        twist::ed::std::thread other([&] {
            for (int r = 0; r < kRounds; ++r) {
                Object* o = pool.acquire();
                take(o, 2);
                give_back(o, 2);
                pool.release(o);
            }
        });
        consumer.join();
        other.join();
        verify_all_back(pool);
    });
}

op::PoolOptions tiny(std::size_t shards, std::size_t objects, op::ShardSelect select) {
    return op::PoolOptions{.shards = shards,
                           .initial_objects = objects,
                           .grow_chunk = 1,
                           .select = select};
}

template <class Pool>
class PoolTwist : public ::testing::Test {};

using Pools = ::testing::Types<op::DefaultPool<Object, TwistSync>>;
TYPED_TEST_SUITE(PoolTwist, Pools);

}  // namespace

// One shard = one stack: 3 threads x 2 held objects on a 3-object pool.
TYPED_TEST(PoolTwist, OneShardAbaChurn) {
    churn<TypeParam>(tiny(1, 3, op::ShardSelect::kHome), 3, kRounds);
}

// One shard, 2 objects, 3 threads holding one each: constant empty/refill.
TYPED_TEST(PoolTwist, OneShardScarce) {
    single_churn<TypeParam>(tiny(1, 2, op::ShardSelect::kHome), 3, kRounds + 1);
}

// Two shards, round robin: objects migrate between shards, probing of other
// shards and growth are hit.
TYPED_TEST(PoolTwist, TwoShardsRoundRobin) {
    churn<TypeParam>(tiny(2, 2, op::ShardSelect::kRoundRobin), 3, kRounds);
}

TYPED_TEST(PoolTwist, CrossThreadHandOff) {
    hand_off<TypeParam>(tiny(1, 2, op::ShardSelect::kHome), kItems);
}
