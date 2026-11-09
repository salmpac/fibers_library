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

#include "fibers/object_pool/sharded_pool.hpp"
#include "fibers/sync/twist_sync.hpp"

namespace op = fibers::object_pool;
using fibers::sync::TwistSync;

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
                pool.flush_thread_cache();
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
                pool.flush_thread_cache();
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
            pool.flush_thread_cache();
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
            pool.flush_thread_cache();
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
                           .select = select,
                           .thread_cache_capacity = 0};  // uncached scenarios
}

// Tiny pool with tiny per-thread caches: capacity 2, batch `batch` (2: every
// refill is a batch pop walking one link, every flush a 2-node chain).
op::PoolOptions tiny_cached(std::size_t shards, std::size_t objects, op::ShardSelect select,
                            std::size_t batch) {
    op::PoolOptions options = tiny(shards, objects, select);
    options.thread_cache_capacity = 2;
    options.thread_cache_batch = batch;  // max_cached_threads: default (64)
    return options;
}

std::uint32_t my_slot() { return op::detail::pool_thread_state<TwistSync>().cache_slot; }

// The calling thread has a cache in a pool with default max_cached_threads:
// ids are recycled when threads exit (and, in the simulator, the registry is
// a TWISTED_STATIC that restarts with every run), so they stay small;
// otherwise the cached scenarios would silently test the uncached path.
void check_has_cache_slot() {
    TWIST_ASSERT_M(my_slot() < 16, "thread id not recycled / not reset: no cache slot");
}

// ------------------------------------------------------------ batch pop

using Arena = op::detail::NodeArena<Object, TwistSync>;
using Hook = Arena::Hook;
using Stack = op::TaggedStack<TwistSync>;

// `threads` threads on ONE tagged stack of `nodes` nodes, each `rounds`
// times: batch-pop up to 2 nodes (or single-pop every third round), own them,
// push them back as one chain (or one by one in odd rounds). Pops racing with
// pushes and other pops hit the speculative walk and its validation.
void stack_batch_churn(std::size_t nodes, int threads, int rounds) {
    check([=] {
        const op::PoolOptions options;
        Arena arena;
        Stack::Shared shared(options);
        Stack stack(options);
        auto& state = op::detail::pool_thread_state<TwistSync>();
        {
            const auto chain = arena.grow(nodes);
            stack.push_chain(arena, shared, state, chain.first, chain.last);
        }
        std::vector<twist::ed::std::thread> workers;
        for (int t = 1; t <= threads; ++t) {
            workers.emplace_back([&, t] {
                auto& me = op::detail::pool_thread_state<TwistSync>();
                for (int r = 0; r < rounds; ++r) {
                    std::vector<Hook*> held;
                    wait_until([&] {
                        if (r % 3 == 2) {
                            if (Hook* node = stack.try_pop(arena, shared, me)) {
                                held.push_back(node);
                            }
                        } else {
                            const auto chain = stack.try_pop_chain(arena, shared, me, 2);
                            Hook* node = chain.first;
                            for (std::size_t i = 0; i < chain.size; ++i) {
                                held.push_back(node);
                                if (i + 1 < chain.size) {
                                    node = arena.hook(node->next.load(std::memory_order_relaxed));
                                }
                            }
                            TWIST_ASSERT_M(chain.size == 0 || held.back() == chain.last,
                                           "chain.last is not the last node");
                        }
                        return !held.empty();
                    });
                    for (Hook* node : held) {
                        take(Arena::value(node), t);
                    }
                    for (Hook* node : held) {
                        give_back(Arena::value(node), t);
                    }
                    if (r % 2 == 0) {
                        for (std::size_t i = 0; i + 1 < held.size(); ++i) {
                            held[i]->next.store(held[i + 1]->self, std::memory_order_relaxed);
                        }
                        stack.push_chain(arena, shared, me, held.front(), held.back());
                    } else {
                        for (Hook* node : held) {
                            stack.push_chain(arena, shared, me, node, node);
                        }
                    }
                }
            });
        }
        for (auto& w : workers) {
            w.join();
        }
        // All nodes back exactly once.
        std::vector<Hook*> all;
        while (Hook* node = stack.try_pop(arena, shared, state)) {
            TWIST_ASSERT_M(all.size() < nodes, "more nodes than were pushed");
            for (Hook* seen : all) {
                TWIST_ASSERT_M(seen != node, "a node was in the stack twice");
            }
            all.push_back(node);
        }
        TWIST_ASSERT_M(all.size() == nodes, "a node was lost");
    });
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

// ------------------------------------------------------------ per-thread caches
//
// Caches of capacity 2 in front of a tiny pool: objects move between threads
// only through flushes (push_chain) and refills (try_pop_chain), so the
// Shared<int> payload checks that this hand-off orders the previous owner's
// writes before the next owner's.

// One shard, 3 objects, 3 threads holding two each (ABA pattern), batch 2:
// refills race with flushes and with each other on one stack; the pool grows.
TYPED_TEST(PoolTwist, CachedOneShardAbaChurn) {
    churn<TypeParam>(tiny_cached(1, 3, op::ShardSelect::kHome, 2), 3, kRounds);
}

// Same with batch 1 (single-node refills and flushes through the chain path).
TYPED_TEST(PoolTwist, CachedOneShardAbaChurnBatch1) {
    churn<TypeParam>(tiny_cached(1, 3, op::ShardSelect::kHome, 1), 3, kRounds);
}

// Two shards, round robin: refills probe the other shard, flushes spread.
TYPED_TEST(PoolTwist, CachedTwoShardsRoundRobin) {
    churn<TypeParam>(tiny_cached(2, 4, op::ShardSelect::kRoundRobin, 2), 3, kRounds);
}

// Producer acquires, consumer releases: every object migrates via a flush of
// the consumer's cache and a refill of the producer's.
TYPED_TEST(PoolTwist, CachedCrossThreadHandOff) {
    hand_off<TypeParam>(tiny_cached(1, 3, op::ShardSelect::kHome, 2), kItems);
}

// A thread exits WITHOUT flushing; a thread started after it inherits its id
// and with it the cached objects. The hand-over of the cache (and of the
// objects' payloads) is ordered only by the id registry: lease destructor
// -> registry mutex -> next thread's first pool operation. A third thread
// churns on the shards meanwhile.
TYPED_TEST(PoolTwist, CachedSlotInheritedAfterExit) {
    check([] {
        TypeParam pool(tiny_cached(1, 3, op::ShardSelect::kHome, 2));
        check_has_cache_slot();
        std::uint32_t exited_slot = op::detail::kNoCacheSlot;
        twist::ed::std::atomic<bool> second_done{false};
        twist::ed::std::atomic<bool> churner_has_id{false};
        twist::ed::std::thread churner([&] {
            check_has_cache_slot();
            churner_has_id.store(true);
            for (int r = 0; r < kRounds; ++r) {
                Object* o = pool.acquire();
                take(o, 3);
                give_back(o, 3);
                pool.release(o);
            }
            // Keep this id until `second` has its own: then the only freed
            // small id is the one `first` returned.
            wait_until([&] { return second_done.load(); });
            pool.flush_thread_cache();
        });
        // Ids are taken on the first pool operation: let the churner take its
        // own first, so that it cannot grab the one `first` frees.
        wait_until([&] { return churner_has_id.load(); });
        twist::ed::std::thread first([&] {
            check_has_cache_slot();
            exited_slot = my_slot();
            Object* a = pool.acquire();
            take(a, 1);
            give_back(a, 1);
            pool.release(a);  // stays in this thread's cache
        });
        first.join();
        twist::ed::std::thread second([&] {
            TWIST_ASSERT_M(my_slot() == exited_slot, "the freed id was not reused");
            TWIST_ASSERT_M(pool.thread_cached_objects() >= 1, "inherited cache is empty");
            for (int r = 0; r < 2; ++r) {
                Object* o = pool.acquire();
                take(o, 2);  // checks the previous owner's payload write is ordered
                give_back(o, 2);
                pool.release(o);
            }
            pool.flush_thread_cache();
            second_done.store(true);
        });
        second.join();
        churner.join();
        verify_all_back(pool);
    });
}

// ------------------------------------------------------------ batch pop

// 3 threads, batch pops of up to 2 and pushes (chains and single nodes) on
// one stack of 3 nodes.
TEST(StackTwist, BatchPopRacesPushesAndPops) {
    stack_batch_churn(3, 3, kRounds + 1);
}

// 4 nodes: two threads can each hold a full batch while the third walks.
TEST(StackTwist, BatchPopFourNodes) {
    stack_batch_churn(4, 3, kRounds + 1);
}
