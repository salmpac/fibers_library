// Queue micro-benchmarks. Items are real Tasks (24 bytes, move-only).

#include <benchmark/benchmark.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <thread>
#include <vector>

#include "thread_pool/locked_spsc_queue.hpp"
#include "thread_pool/sharded_queue.hpp"
#include "thread_pool/slow_queue.hpp"
#include "thread_pool/spsc_queue.hpp"
#include "thread_pool/task.hpp"

namespace tp = thread_pool;

namespace {

constexpr std::size_t kItems = std::size_t{1} << 20;
constexpr std::size_t kCapacity = 256;

void noop(void*) noexcept {}
tp::Task make_task() noexcept { return tp::Task(nullptr, &noop, nullptr); }

void fill(std::vector<tp::Task>& buf, std::size_t n) {
    for (std::size_t i = 0; i < n; ++i) buf[i] = make_task();
}

// ---------------------------------------------------------------- 1 thread

void BM_SPSC_PushPop(benchmark::State& state) {
    tp::SPSCQueue<tp::Task> q(kCapacity);
    tp::Task out;
    for (auto _ : state) {
        q.try_push(make_task());
        q.try_pop(out);
        benchmark::DoNotOptimize(out);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_SPSC_PushPop);

void BM_SPSC_PushPopBatch(benchmark::State& state) {
    const auto batch = static_cast<std::size_t>(state.range(0));
    tp::SPSCQueue<tp::Task> q(kCapacity);
    std::vector<tp::Task> in(batch), out(batch);
    for (auto _ : state) {
        fill(in, batch);
        q.try_push_batch(in.data(), batch);
        benchmark::DoNotOptimize(q.try_pop_batch(out.data(), batch));
    }
    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(batch));
}
BENCHMARK(BM_SPSC_PushPopBatch)->ArgName("batch")->Arg(1)->Arg(32);

// Owner of a main_box with nobody stealing: cost of the uncontended read lock.
void BM_LockedSPSC_OwnerPushPopBatch(benchmark::State& state) {
    const auto batch = static_cast<std::size_t>(state.range(0));
    tp::LockedSPSCQueue<tp::Task> q(kCapacity);
    std::vector<tp::Task> in(batch), out(batch);
    for (auto _ : state) {
        fill(in, batch);
        q.try_push_batch(in.data(), batch);
        benchmark::DoNotOptimize(q.try_pop_batch(out.data(), batch));
    }
    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(batch));
}
BENCHMARK(BM_LockedSPSC_OwnerPushPopBatch)->ArgName("batch")->Arg(1)->Arg(32);

// ---------------------------------------------------------------- N threads

// One producer thread, the benchmark thread consumes.
void BM_SPSC_ProducerConsumer(benchmark::State& state) {
    const auto batch = static_cast<std::size_t>(state.range(0));
    tp::SPSCQueue<tp::Task> q(kCapacity);
    std::vector<tp::Task> out(batch);

    for (auto _ : state) {
        std::thread producer([&] {
            std::vector<tp::Task> buf(batch);
            for (std::size_t sent = 0; sent < kItems;) {
                const std::size_t n = std::min(batch, kItems - sent);
                fill(buf, n);
                const std::size_t k = q.try_push_batch(buf.data(), n);
                sent += k;
                if (k == 0) tp::cpu_relax();
            }
        });
        for (std::size_t received = 0; received < kItems;) {
            const std::size_t k = q.try_pop_batch(out.data(), batch);
            received += k;
            if (k == 0) tp::cpu_relax();
        }
        producer.join();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(kItems));
}
BENCHMARK(BM_SPSC_ProducerConsumer)
    ->ArgName("batch")->Arg(1)->Arg(32)
    ->UseRealTime()->Unit(benchmark::kMillisecond);

// Owner pushes and pops its main_box while `thieves` threads keep stealing.
void BM_LockedSPSC_OwnerWithThieves(benchmark::State& state) {
    const auto thieves = static_cast<std::size_t>(state.range(0));
    constexpr std::size_t kBatch = 32;
    tp::LockedSPSCQueue<tp::Task> q(kCapacity);

    for (auto _ : state) {
        std::atomic<std::size_t> consumed{0};
        std::atomic<bool> done{false};
        std::vector<std::thread> threads;
        for (std::size_t t = 0; t < thieves; ++t) {
            threads.emplace_back([&] {
                std::vector<tp::Task> buf(kBatch);
                while (!done.load(std::memory_order_relaxed)) {
                    if (const std::size_t k = q.try_steal_batch(buf.data(), kBatch)) {
                        consumed.fetch_add(k, std::memory_order_relaxed);
                    } else {
                        tp::cpu_relax();
                    }
                }
            });
        }

        std::vector<tp::Task> in(kBatch), out(kBatch);
        std::size_t pushed = 0;
        while (consumed.load(std::memory_order_relaxed) < kItems) {
            if (pushed < kItems) {
                const std::size_t n = std::min(kBatch, kItems - pushed);
                fill(in, n);
                pushed += q.try_push_batch(in.data(), n);
            }
            if (const std::size_t k = q.try_pop_batch(out.data(), kBatch)) {
                consumed.fetch_add(k, std::memory_order_relaxed);
            }
        }
        done.store(true);
        for (auto& t : threads) t.join();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(kItems));
}
BENCHMARK(BM_LockedSPSC_OwnerWithThieves)
    ->ArgName("thieves")->Arg(0)->Arg(1)->Arg(3)
    ->UseRealTime()->Unit(benchmark::kMillisecond);

// `producers` threads push into one ShardedQueue (mail_box), the benchmark
// thread drains it as the owner.
void BM_ShardedQueue_MPSC(benchmark::State& state) {
    const auto producers = static_cast<std::size_t>(state.range(0));
    const auto shards = static_cast<std::size_t>(state.range(1));
    constexpr std::size_t kBatch = 32;
    tp::ShardedQueue<tp::Task> q(shards, kCapacity);
    std::vector<tp::Task> out(kBatch);
    const std::size_t per_producer = kItems / producers;
    const std::size_t total = per_producer * producers;

    for (auto _ : state) {
        std::vector<std::thread> threads;
        for (std::size_t p = 0; p < producers; ++p) {
            threads.emplace_back([&, p] {
                for (std::size_t i = 0; i < per_producer;) {
                    if (q.try_push(make_task(), p)) {
                        ++i;
                    } else {
                        tp::cpu_relax();
                    }
                }
            });
        }
        std::size_t cursor = 0;
        for (std::size_t received = 0; received < total;) {
            const std::size_t k = q.try_pop_batch(out.data(), kBatch, cursor++);
            received += k;
            if (k == 0) tp::cpu_relax();
        }
        for (auto& t : threads) t.join();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(total));
}
BENCHMARK(BM_ShardedQueue_MPSC)
    ->ArgNames({"producers", "shards"})
    ->ArgsProduct({{1, 2, 4, 8}, {1, 4, 8}})
    ->UseRealTime()->Unit(benchmark::kMillisecond);

// Same shape for the mutex fallback, for comparison.
void BM_SlowQueue_MPSC(benchmark::State& state) {
    const auto producers = static_cast<std::size_t>(state.range(0));
    constexpr std::size_t kBatch = 32;
    tp::SlowQueue<tp::Task> q;
    std::vector<tp::Task> out(kBatch);
    const std::size_t per_producer = kItems / producers;
    const std::size_t total = per_producer * producers;

    for (auto _ : state) {
        std::vector<std::thread> threads;
        for (std::size_t p = 0; p < producers; ++p) {
            threads.emplace_back([&] {
                for (std::size_t i = 0; i < per_producer; ++i) q.push(make_task());
            });
        }
        for (std::size_t received = 0; received < total;) {
            const std::size_t k = q.try_pop_batch(out.data(), kBatch);
            received += k;
            if (k == 0) tp::cpu_relax();
        }
        for (auto& t : threads) t.join();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(total));
}
BENCHMARK(BM_SlowQueue_MPSC)
    ->ArgName("producers")->Arg(1)->Arg(2)->Arg(4)->Arg(8)
    ->UseRealTime()->Unit(benchmark::kMillisecond);

}  // namespace
