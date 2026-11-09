// BlockingPool benchmarks.
//
//   BM_Handoff         main-pool task -> blocking pool -> continuation back on
//                      the main pool (both timestamps taken on main-pool
//                      workers), arg 0: blocking pool idle (threads asleep,
//                      200 us between samples), arg 1: busy (a background
//                      submitter keeps 16 x 2 us tasks queued, threads never
//                      sleep). median / p99 in ns.
//   BM_Throughput      tiny blocking tasks (one relaxed increment) from 1 / 4 /
//                      8 submitter threads into one pool of 8 threads.
//   BM_Pread4K         4 KiB pread from the page cache: arg 0 inline
//                      preadv2(RWF_NOWAIT), arg 1 offloaded (PreadOp through
//                      the pool, completion seen by the caller), arg 2 plain
//                      pread for reference.
//   BM_IdleCpu         process CPU over 500 ms with 8 idle pool threads.
//
// Latency / CPU results are counters; run with --benchmark_repetitions=3 on
// CPUs 0-9 (taskset -c 0-9). The test file for BM_Pread4K is created in the
// current directory.

#include <benchmark/benchmark.h>

#include <fcntl.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <thread>
#include <vector>

#include "fibers/blocking/blocking_pool.hpp"
#include "fibers/blocking/nowait.hpp"
#include "fibers/blocking/ops.hpp"
#include "fibers/thread_pool/thread_pool.hpp"

namespace blocking = fibers::blocking;

namespace {

using namespace std::chrono_literals;
using blocking::BlockingPool;
using blocking::IoResult;
using blocking::Sink;
using blocking::Task;
using MainPool = fibers::thread_pool::ThreadPool<>;

std::int64_t mono_ns() {
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<std::int64_t>(ts.tv_sec) * 1'000'000'000 + ts.tv_nsec;
}

void report_percentiles(benchmark::State& state, std::vector<std::int64_t>& samples) {
    if (samples.empty()) return;
    std::sort(samples.begin(), samples.end());
    state.counters["median_ns"] = static_cast<double>(samples[samples.size() / 2]);
    state.counters["p99_ns"] = static_cast<double>(samples[samples.size() * 99 / 100]);
    state.counters["max_ns"] = static_cast<double>(samples.back());
    state.counters["samples"] = static_cast<double>(samples.size());
}

std::chrono::microseconds process_cpu() {
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    auto tv = [](const timeval& t) { return std::chrono::seconds(t.tv_sec) + std::chrono::microseconds(t.tv_usec); };
    return tv(usage.ru_utime) + tv(usage.ru_stime);
}

void spin_for_ns(std::int64_t ns) {
    const std::int64_t until = mono_ns() + ns;
    while (mono_ns() < until) fibers::sync::cpu_relax();
}

// ---------------------------------------------------------------------------

// Keeps `inflight` 2 us tasks in the pool until stopped.
class BackgroundLoad {
public:
    BackgroundLoad(BlockingPool<>& pool, int inflight) : pool_(pool), inflight_(inflight) {
        thread_ = std::thread([this] { run(); });
    }
    ~BackgroundLoad() {
        stop_.store(true);
        thread_.join();
        while (outstanding_.load() > 0) std::this_thread::yield();
    }
    std::uint64_t completed() const { return completed_.load(); }

private:
    static void work(void* memo) noexcept {
        auto* self = static_cast<BackgroundLoad*>(memo);
        spin_for_ns(2'000);
        self->completed_.fetch_add(1, std::memory_order_relaxed);
        self->outstanding_.fetch_sub(1, std::memory_order_release);
    }
    void run() {
        while (!stop_.load(std::memory_order_relaxed)) {
            if (outstanding_.load(std::memory_order_acquire) >= inflight_) {
                fibers::sync::cpu_relax();
                continue;
            }
            outstanding_.fetch_add(1);
            if (!pool_.try_submit(Task(this, &work, nullptr))) outstanding_.fetch_sub(1);
        }
    }

    BlockingPool<>& pool_;
    const int inflight_;
    std::atomic<bool> stop_{false};
    std::atomic<int> outstanding_{0};
    std::atomic<std::uint64_t> completed_{0};
    std::thread thread_;
};

struct HandoffProbe {
    BlockingPool<>* pool = nullptr;
    MainPool* main = nullptr;
    blocking::CallOp<IoResult (*)()> op{[]() { return IoResult{}; }};
    std::int64_t started = 0;  // written on a main worker before the op is published
    std::atomic<std::int64_t> finished{0};

    // Runs on a main-pool worker: the "fiber" offloads its blocking call.
    static void start(void* memo) noexcept {
        auto* self = static_cast<HandoffProbe*>(memo);
        self->op.set_continuation(Task(self, &finish, nullptr), Sink::of(*self->main));
        self->started = mono_ns();
        while (!blocking::try_submit_op(*self->pool, self->op)) fibers::sync::cpu_relax();
    }
    // The continuation, back on a main-pool worker.
    static void finish(void* memo) noexcept {
        auto* self = static_cast<HandoffProbe*>(memo);
        self->finished.store(mono_ns(), std::memory_order_release);
    }
};

void BM_Handoff(benchmark::State& state) {
    const bool busy = state.range(0) == 1;
    MainPool main({.num_workers = 4});
    BlockingPool<> pool({.num_threads = 8});
    std::unique_ptr<BackgroundLoad> load;
    if (busy) load = std::make_unique<BackgroundLoad>(pool, 16);
    HandoffProbe probe;
    probe.pool = &pool;
    probe.main = &main;
    const int kSamples = busy ? 20'000 : 5'000;
    std::vector<std::int64_t> samples;
    samples.reserve(kSamples);
    for (auto _ : state) {
        for (int i = 0; i < kSamples; ++i) {
            if (!busy) std::this_thread::sleep_for(200us);  // everybody back asleep
            probe.finished.store(0, std::memory_order_relaxed);
            main.submit(Task(&probe, &HandoffProbe::start, nullptr));
            std::int64_t finished;
            while ((finished = probe.finished.load(std::memory_order_acquire)) == 0) fibers::sync::cpu_relax();
            samples.push_back(finished - probe.started);
        }
    }
    if (busy) state.counters["background_tasks"] = static_cast<double>(load->completed());
    load.reset();
    report_percentiles(state, samples);
}
BENCHMARK(BM_Handoff)->Arg(0)->Arg(1)->Iterations(1)->UseRealTime()->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------------------------

void BM_Throughput(benchmark::State& state) {
    const int submitters = static_cast<int>(state.range(0));
    constexpr std::uint64_t kTasks = 400'000;
    std::atomic<std::uint64_t> done{0};
    auto tick = [](void* memo) noexcept {
        static_cast<std::atomic<std::uint64_t>*>(memo)->fetch_add(1, std::memory_order_relaxed);
    };
    BlockingPool<> pool({.num_threads = 8, .queue_capacity = 1024});
    std::uint64_t target = 0;
    std::uint64_t refused = 0;
    for (auto _ : state) {
        std::atomic<std::uint64_t> refused_now{0};
        std::vector<std::thread> threads;
        for (int s = 0; s < submitters; ++s) {
            threads.emplace_back([&, s] {
                const std::uint64_t mine = kTasks / submitters + (s == 0 ? kTasks % submitters : 0);
                std::uint64_t r = 0;
                for (std::uint64_t i = 0; i < mine; ++i) {
                    while (!pool.try_submit(Task(&done, tick, nullptr))) {
                        ++r;
                        std::this_thread::yield();
                    }
                }
                refused_now.fetch_add(r);
            });
        }
        for (auto& t : threads) t.join();
        target += kTasks;
        while (done.load(std::memory_order_relaxed) < target) fibers::sync::cpu_relax();
        refused += refused_now.load();
    }
    state.SetItemsProcessed(static_cast<std::int64_t>(target));
    state.counters["refused_per_task"] = static_cast<double>(refused) / static_cast<double>(target);
    state.counters["sleeps_per_task"] = static_cast<double>(pool.sleeps()) / static_cast<double>(target);
}
BENCHMARK(BM_Throughput)->Arg(1)->Arg(4)->Arg(8)->UseRealTime()->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------------------------

class CachedFile {
public:
    static constexpr std::size_t kSize = 1 << 20;

    CachedFile() {
        char name[] = "./blocking_bench_XXXXXX";
        fd_ = mkstemp(name);
        if (fd_ < 0) {
            std::perror("mkstemp");
            std::abort();
        }
        unlink(name);
        std::vector<char> data(kSize, 'x');
        if (pwrite(fd_, data.data(), data.size(), 0) != static_cast<ssize_t>(kSize)) std::abort();
        if (pread(fd_, data.data(), data.size(), 0) != static_cast<ssize_t>(kSize)) std::abort();  // warm
    }
    ~CachedFile() { close(fd_); }
    int fd() const { return fd_; }

private:
    int fd_ = -1;
};

void BM_Pread4K(benchmark::State& state) {
    const int mode = static_cast<int>(state.range(0));
    CachedFile file;
    BlockingPool<> pool({.num_threads = 8});
    blocking::NowaitSupport support;
    alignas(64) char buf[4096];
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> page(0, static_cast<int>(CachedFile::kSize / 4096) - 1);
    std::atomic<bool> done{false};
    std::uint64_t inline_hits = 0;
    for (auto _ : state) {
        const off_t offset = static_cast<off_t>(page(rng)) * 4096;
        if (mode == 0) {
            const IoResult r = blocking::try_read_nowait(file.fd(), buf, sizeof(buf), offset, support);
            if (r.value == 4096) ++inline_hits;
        } else if (mode == 1) {
            blocking::PreadOp op(file.fd(), buf, sizeof(buf), offset);
            done.store(false, std::memory_order_relaxed);
            op.set_continuation(Task(&done, [](void* m) noexcept {
                static_cast<std::atomic<bool>*>(m)->store(true, std::memory_order_release);
            }, nullptr), Sink::run_inline());
            if (!blocking::try_submit_op(pool, op)) std::abort();
            while (!done.load(std::memory_order_acquire)) fibers::sync::cpu_relax();
        } else {
            if (pread(file.fd(), buf, sizeof(buf), offset) != 4096) std::abort();
        }
        benchmark::DoNotOptimize(buf);
    }
    if (mode == 0) {
        state.counters["inline_hit_ratio"] =
            static_cast<double>(inline_hits) / static_cast<double>(state.iterations());
    }
    state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) * 4096);
}
BENCHMARK(BM_Pread4K)->Arg(0)->Arg(1)->Arg(2)->UseRealTime();

// ---------------------------------------------------------------------------

void BM_IdleCpu(benchmark::State& state) {
    for (auto _ : state) {
        BlockingPool<> pool({.num_threads = 8});
        std::this_thread::sleep_for(20ms);
        const auto sleeps_before = pool.sleeps();
        const auto before = process_cpu();
        std::this_thread::sleep_for(500ms);
        const auto cpu = process_cpu() - before;
        state.counters["cpu_pct"] = 100.0 * static_cast<double>(cpu.count()) / 500'000.0;
        state.counters["wakeups"] = static_cast<double>(pool.sleeps() - sleeps_before);
        state.counters["sleeping_threads"] = static_cast<double>(pool.sleeping_threads());
    }
}
BENCHMARK(BM_IdleCpu)->Iterations(1)->UseRealTime()->Unit(benchmark::kMillisecond);

}  // namespace
