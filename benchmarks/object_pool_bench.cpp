// Object pool benchmarks: acquire/release throughput of pool implementations.
//
// Scenarios (per pool type):
//   Pair        - one thread: acquire + release, LIFO;
//   Burst/64    - one thread: 64 acquires, then 64 releases;
//   Churn/T     - T threads, each doing bursts of 8 acquires + 8 releases;
//   Cross/P     - P producer/consumer pairs: producer acquires and hands the
//                 object over an SPSC queue, consumer releases it (the
//                 "allocated by submitter, freed by worker" pattern).
// items_per_second counts objects (one acquire + one release each).
//
// To add a pool implementation: define a default-constructible type
// satisfying fibers::object_pool::ObjectPool for `Obj` and call REGISTER_POOL below.

#include <benchmark/benchmark.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include "baseline_pools.hpp"
#include "fibers/object_pool/sharded_pool.hpp"
#include "fibers/sync/spin_lock.hpp"
#include "fibers/queue/spsc_queue.hpp"

namespace op = fibers::object_pool;

namespace {

// 64-byte object with intrusive doubly-linked hooks.
struct Obj {
    Obj* prev = nullptr;
    Obj* next = nullptr;
    std::uint64_t payload[6] = {};
};
static_assert(sizeof(Obj) == 64);

constexpr op::PoolOptions kOptions{.shards = 16, .initial_objects = 4096, .grow_chunk = 256};

// kCache: PoolOptions::thread_cache_capacity (0 = no per-thread caches; the
// baselines ignore it).
template <class Pool, op::ShardSelect kSelect, std::size_t kCache = 0>
struct Configured : Pool {
    Configured() : Pool(with_select()) {}
    static op::PoolOptions with_select() {
        op::PoolOptions options = kOptions;
        options.select = kSelect;
        options.thread_cache_capacity = kCache;
        return options;
    }
};

// Reference point: plain operator new / delete (glibc malloc).
struct NewDelete {
    using value_type = Obj;
    Obj* acquire() { return new Obj; }
    void release(Obj* object) { delete object; }
    std::size_t capacity() const { return 0; }
};

using MutexRR = Configured<op::baseline::MutexVectorPool<Obj>, op::ShardSelect::kRoundRobin>;
using MutexHome = Configured<op::baseline::MutexVectorPool<Obj>, op::ShardSelect::kHome>;
using SpinRR = Configured<op::baseline::MutexVectorPool<Obj, fibers::sync::StdSync, fibers::sync::SpinLock<>>,
                          op::ShardSelect::kRoundRobin>;
using SpinHome = Configured<op::baseline::MutexVectorPool<Obj, fibers::sync::StdSync, fibers::sync::SpinLock<>>,
                            op::ShardSelect::kHome>;

// Intrusive-node pools (sharded_pool.hpp): shard free list = spin-locked list,
// tagged Treiber stack (DefaultPool's shards), all without per-thread caches.
using LockedRR = Configured<op::baseline::LockedListPool<Obj>, op::ShardSelect::kRoundRobin>;
using LockedHome = Configured<op::baseline::LockedListPool<Obj>, op::ShardSelect::kHome>;
using TaggedRR = Configured<op::TaggedPool<Obj>, op::ShardSelect::kRoundRobin>;
using TaggedHome = Configured<op::TaggedPool<Obj>, op::ShardSelect::kHome>;
// Tagged shards with per-thread caches of 4, 8, 16 objects (batch = half);
// _C8 is DefaultPool's default configuration.
using TaggedRR_C4 = Configured<op::TaggedPool<Obj>, op::ShardSelect::kRoundRobin, 4>;
using TaggedRR_C8 = Configured<op::TaggedPool<Obj>, op::ShardSelect::kRoundRobin, 8>;
using TaggedRR_C16 = Configured<op::TaggedPool<Obj>, op::ShardSelect::kRoundRobin, 16>;
using TaggedHome_C4 = Configured<op::TaggedPool<Obj>, op::ShardSelect::kHome, 4>;
using TaggedHome_C8 = Configured<op::TaggedPool<Obj>, op::ShardSelect::kHome, 8>;
using TaggedHome_C16 = Configured<op::TaggedPool<Obj>, op::ShardSelect::kHome, 16>;

static_assert(op::ObjectPool<NewDelete>);
static_assert(op::ObjectPool<MutexRR>);

constexpr std::size_t kOpsPerThread = std::size_t{1} << 19;

// ------------------------------------------------------------ 1 thread

template <class Pool>
void BM_Pair(benchmark::State& state) {
    Pool pool;
    for (auto _ : state) {
        Obj* o = pool.acquire();
        benchmark::DoNotOptimize(o);
        pool.release(o);
    }
    state.SetItemsProcessed(state.iterations());
}

template <class Pool>
void BM_Burst(benchmark::State& state) {
    const auto burst = static_cast<std::size_t>(state.range(0));
    Pool pool;
    std::vector<Obj*> held(burst);
    for (auto _ : state) {
        for (Obj*& o : held) o = pool.acquire();
        benchmark::DoNotOptimize(held.data());
        for (Obj* o : held) pool.release(o);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(burst));
}

// ------------------------------------------------------------ N threads

template <class Pool>
void BM_Churn(benchmark::State& state) {
    const auto threads = static_cast<std::size_t>(state.range(0));
    constexpr std::size_t kBurst = 8;
    Pool pool;
    for (auto _ : state) {
        std::vector<std::thread> workers;
        for (std::size_t t = 0; t < threads; ++t) {
            workers.emplace_back([&] {
                Obj* held[kBurst];
                for (std::size_t i = 0; i < kOpsPerThread; i += kBurst) {
                    for (Obj*& o : held) o = pool.acquire();
                    benchmark::DoNotOptimize(held);
                    for (Obj* o : held) pool.release(o);
                }
            });
        }
        for (auto& w : workers) w.join();
    }
    state.SetItemsProcessed(state.iterations() *
                            static_cast<std::int64_t>(threads * kOpsPerThread));
}

template <class Pool>
void BM_Cross(benchmark::State& state) {
    const auto pairs = static_cast<std::size_t>(state.range(0));
    Pool pool;
    for (auto _ : state) {
        std::vector<std::unique_ptr<fibers::queue::SPSCQueue<Obj*>>> channels;
        for (std::size_t p = 0; p < pairs; ++p) {
            channels.push_back(std::make_unique<fibers::queue::SPSCQueue<Obj*>>(1024));
        }
        std::vector<std::thread> threads;
        for (std::size_t p = 0; p < pairs; ++p) {
            auto& channel = *channels[p];
            threads.emplace_back([&pool, &channel] {  // producer
                for (std::size_t i = 0; i < kOpsPerThread;) {
                    Obj* o = pool.acquire();
                    while (!channel.try_push(std::move(o))) fibers::sync::cpu_relax();
                    ++i;
                }
            });
            threads.emplace_back([&pool, &channel] {  // consumer
                Obj* buf[32];
                for (std::size_t i = 0; i < kOpsPerThread;) {
                    const std::size_t n = channel.try_pop_batch(buf, 32);
                    if (n == 0) {
                        fibers::sync::cpu_relax();
                        continue;
                    }
                    for (std::size_t j = 0; j < n; ++j) pool.release(buf[j]);
                    i += n;
                }
            });
        }
        for (auto& th : threads) th.join();
    }
    state.SetItemsProcessed(state.iterations() *
                            static_cast<std::int64_t>(pairs * kOpsPerThread));
}

#define REGISTER_POOL(Pool)                                                          \
    BENCHMARK_TEMPLATE(BM_Pair, Pool);                                               \
    BENCHMARK_TEMPLATE(BM_Burst, Pool)->Arg(64);                                     \
    BENCHMARK_TEMPLATE(BM_Churn, Pool)                                               \
        ->ArgName("threads")->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)                \
        ->UseRealTime()->Unit(benchmark::kMillisecond);                              \
    BENCHMARK_TEMPLATE(BM_Cross, Pool)                                               \
        ->ArgName("pairs")->Arg(1)->Arg(2)->Arg(4)->Arg(8)                           \
        ->UseRealTime()->Unit(benchmark::kMillisecond)

REGISTER_POOL(NewDelete);
REGISTER_POOL(MutexRR);
REGISTER_POOL(MutexHome);
REGISTER_POOL(SpinRR);
REGISTER_POOL(SpinHome);
REGISTER_POOL(LockedRR);
REGISTER_POOL(LockedHome);
REGISTER_POOL(TaggedRR);
REGISTER_POOL(TaggedHome);
REGISTER_POOL(TaggedRR_C4);
REGISTER_POOL(TaggedRR_C8);
REGISTER_POOL(TaggedRR_C16);
REGISTER_POOL(TaggedHome_C4);
REGISTER_POOL(TaggedHome_C8);
REGISTER_POOL(TaggedHome_C16);

}  // namespace
