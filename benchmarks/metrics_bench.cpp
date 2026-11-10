// Hot-path cost of fibers::metrics instruments against plain atomics, from
// 1..N threads hammering one instrument (google benchmark's ->Threads).
//
//   PlainAtomicInc      one shared std::atomic<uint64_t>, fetch_add relaxed:
//                       the naive counter (one cache line for all threads)
//   PaddedPerThreadInc  each thread its own cache-line atomic: the floor a
//                       sharded counter can approach (no shard lookup)
//   CounterInc          metrics::Counter::inc (shard lookup through the
//                       noinline thread-local accessor + fetch_add on the
//                       thread's row), registry with 16 shards
//   CounterIncOneShard  the same with shards = 1 (no lookup, one shared cell)
//   GaugeAdd            metrics::Gauge::add (one shared cell)
//   HistogramObserve    metrics::Histogram::observe, 16 latency buckets
//                       (binary search + two fetch_adds on the thread's row)
//   Scrape              Registry::write of 100 counters + 10 histograms into
//                       a 4 KiB buffer with a discarding sink (one thread)
//
// Indicative only (machine shared with other work). Run pinned to CPUs 0-9:
// `taskset -c 0-9 metrics_bench`.

#include <benchmark/benchmark.h>

#include <atomic>
#include <cstdint>
#include <string>
#include <string_view>

#include "fibers/metrics/metrics.hpp"
#include "fibers/sync/sync.hpp"

namespace metrics = fibers::metrics;

namespace {

std::atomic<std::uint64_t> g_plain{0};

struct alignas(fibers::sync::kCacheLineSize) Padded {
    std::atomic<std::uint64_t> value{0};
};
Padded g_padded[64];

metrics::Registry& registry16() {
    static metrics::Registry registry({.shards = 16});
    return registry;
}

metrics::Registry& registry1() {
    static metrics::Registry registry({.shards = 1});
    return registry;
}

void PlainAtomicInc(benchmark::State& state) {
    for (auto _ : state) {
        g_plain.fetch_add(1, std::memory_order_relaxed);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(PlainAtomicInc)->ThreadRange(1, 8)->UseRealTime();

void PaddedPerThreadInc(benchmark::State& state) {
    std::atomic<std::uint64_t>& mine = g_padded[state.thread_index() % 64].value;
    for (auto _ : state) {
        mine.fetch_add(1, std::memory_order_relaxed);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(PaddedPerThreadInc)->ThreadRange(1, 8)->UseRealTime();

void CounterInc(benchmark::State& state) {
    static metrics::Counter counter = registry16().counter("bench_counter_total", "Bench.");
    for (auto _ : state) {
        counter.inc();
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(CounterInc)->ThreadRange(1, 8)->UseRealTime();

void CounterIncOneShard(benchmark::State& state) {
    static metrics::Counter counter = registry1().counter("bench_counter_total", "Bench.");
    for (auto _ : state) {
        counter.inc();
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(CounterIncOneShard)->ThreadRange(1, 8)->UseRealTime();

void GaugeAdd(benchmark::State& state) {
    static metrics::Gauge gauge = registry16().gauge("bench_gauge", "Bench.");
    for (auto _ : state) {
        gauge.add(1);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(GaugeAdd)->ThreadRange(1, 8)->UseRealTime();

void HistogramObserve(benchmark::State& state) {
    static metrics::Histogram histogram =
        registry16().histogram("bench_latency_seconds", "Bench.", metrics::kLatencyBucketsUs, 1e6);
    std::int64_t value = 37 * (state.thread_index() + 1);
    for (auto _ : state) {
        histogram.observe(value);
        value = (value * 7 + 13) & 0xFFFFF;  // spread over the buckets (< ~1 s)
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(HistogramObserve)->ThreadRange(1, 8)->UseRealTime();

void Scrape(benchmark::State& state) {
    metrics::Registry registry({.shards = 16});
    auto counters = registry.counter_family("scrape_counter_total", "Bench.", {"id"});
    auto histograms = registry.histogram_family("scrape_latency_seconds", "Bench.", {"id"},
                                                metrics::kLatencyBucketsUs, 1e6);
    for (int i = 0; i < 100; ++i) {
        counters.with({std::to_string(i)}).add(static_cast<std::uint64_t>(i) * 1000);
    }
    for (int i = 0; i < 10; ++i) {
        histograms.with({std::to_string(i)}).observe(i * 1000);
    }
    std::uint64_t bytes = 0;
    auto discard = [&bytes](std::string_view data) noexcept -> int {
        bytes += data.size();
        return 0;
    };
    char buffer[4096];
    for (auto _ : state) {
        metrics::Exposition out(buffer, metrics::Sink::ref(discard));
        registry.write(out);
        out.flush();
    }
    state.SetBytesProcessed(static_cast<std::int64_t>(bytes));
}
BENCHMARK(Scrape);

}  // namespace
