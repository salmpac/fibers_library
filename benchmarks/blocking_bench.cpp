// BlockingPool benchmarks.
//
//   BM_Handoff/busy/threads  main-pool task -> blocking pool -> continuation
//                      back on the main pool (both timestamps taken on
//                      main-pool workers). busy 0: blocking pool idle
//                      (threads asleep, 200 us between samples); busy 1: a
//                      background submitter offers a fixed load of 400k
//                      ops/s, 2 us each (0.8 cores' worth), at most 16 in
//                      flight; the probe shares the pool with it. A fixed
//                      rate, so that a faster pool does not get a heavier
//                      load (an unthrottled generator saturates every pool
//                      thread and starves the main pool on 10 CPUs).
//                      median / p99 in ns.
//   BM_Throughput/submitters/threads  tiny ops (one relaxed increment) from
//                      1 / 4 / 8 submitter threads; ops/s.
//   BM_Pread4K/mode    4 KiB pread from the page cache: 0 inline
//                      preadv2(RWF_NOWAIT), 1 offloaded (PreadOp through the
//                      pool, completion seen by the caller), 2 plain pread
//                      for reference.
//   BM_IdleCpu/threads process CPU over 500 ms with idle pool threads.
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
#include <memory>
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

// ---- the API under test (one place, so an A/B run can swap it) ----
blocking::BlockingPoolOptions pool_options(std::size_t threads) { return {.num_threads = threads}; }
void submit_op(BlockingPool<>& pool, blocking::BlockingOp& op) { pool.submit(op); }
using NowaitHandle = blocking::NowaitSupport::Fs;
NowaitHandle nowait_handle(blocking::NowaitSupport& support, int fd) { return support.filesystem(fd); }
IoResult read_nowait(blocking::NowaitSupport& support, NowaitHandle fs, int fd, void* buf, std::size_t len,
                     off_t offset) {
    return blocking::try_read_nowait(fd, buf, len, offset, fs, support);
}
// ---- end of the API under test ----

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

// Offers 2 us ops at a fixed rate (at most `inflight` outstanding) until
// destroyed.
class BackgroundLoad {
public:
    static constexpr int kMaxInflight = 64;
    static constexpr std::int64_t kPeriodNs = 2'500;  // 400k ops/s

    BackgroundLoad(BlockingPool<>& pool, int inflight) : pool_(pool), inflight_(std::min(inflight, kMaxInflight)) {
        thread_ = std::thread([this] { run(); });
    }
    ~BackgroundLoad() {
        stop_.store(true);
        thread_.join();
        for (int i = 0; i < inflight_; ++i) {
            while (ops_[i].busy.load(std::memory_order_acquire)) std::this_thread::yield();
        }
    }
    std::uint64_t completed() const { return completed_.load(); }

private:
    struct WorkOp final : blocking::Op<WorkOp> {
        BackgroundLoad* owner = nullptr;
        std::atomic<bool> busy{false};
        IoResult execute() noexcept {
            spin_for_ns(2'000);
            owner->completed_.fetch_add(1, std::memory_order_relaxed);
            return IoResult{};
        }
        // The continuation (run inline): the pool is done with the op.
        static void done(void* memo) noexcept {
            static_cast<WorkOp*>(memo)->busy.store(false, std::memory_order_release);
        }
    };

    void run() {
        std::int64_t due = mono_ns();
        while (!stop_.load(std::memory_order_relaxed)) {
            for (int i = 0; i < inflight_ && mono_ns() >= due; ++i) {
                if (!ops_[i].busy.load(std::memory_order_acquire)) {
                    due += kPeriodNs;
                    ops_[i].owner = this;
                    ops_[i].busy.store(true, std::memory_order_relaxed);
                    ops_[i].set_continuation(Task(&ops_[i], &WorkOp::done, nullptr), Sink::run_inline());
                    submit_op(pool_, ops_[i]);
                }
            }
            fibers::sync::cpu_relax();
        }
    }

    BlockingPool<>& pool_;
    const int inflight_;
    WorkOp ops_[kMaxInflight];
    std::atomic<bool> stop_{false};
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
        submit_op(*self->pool, self->op);
    }
    // The continuation, back on a main-pool worker.
    static void finish(void* memo) noexcept {
        auto* self = static_cast<HandoffProbe*>(memo);
        self->finished.store(mono_ns(), std::memory_order_release);
    }
};

void BM_Handoff(benchmark::State& state) {
    const bool busy = state.range(0) == 1;
    const auto threads = static_cast<std::size_t>(state.range(1));
    MainPool main({.num_workers = 4});
    BlockingPool<> pool(pool_options(threads));
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
    if (busy) {
        state.counters["background_ops_per_s"] = benchmark::Counter(static_cast<double>(load->completed()),
                                                                    benchmark::Counter::kIsRate);
    }
    load.reset();
    report_percentiles(state, samples);
}
BENCHMARK(BM_Handoff)
    ->ArgNames({"busy", "threads"})
    ->Args({0, 2})
    ->Args({1, 2})
    ->Args({0, 8})
    ->Args({1, 8})
    ->Iterations(1)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------------------------

struct TickOp final : blocking::Op<TickOp> {
    std::atomic<std::uint64_t>* done = nullptr;
    IoResult execute() noexcept {
        done->fetch_add(1, std::memory_order_relaxed);
        return IoResult{};
    }
};

void BM_Throughput(benchmark::State& state) {
    const int submitters = static_cast<int>(state.range(0));
    const auto threads = static_cast<std::size_t>(state.range(1));
    constexpr std::uint64_t kOps = 400'000;
    std::atomic<std::uint64_t> done{0};
    // One op object per submitted job, allocated once: a round is "submit
    // kOps distinct jobs and wait for all of them".
    auto ops = std::make_unique<TickOp[]>(kOps);
    for (std::uint64_t i = 0; i < kOps; ++i) ops[i].done = &done;
    BlockingPool<> pool(pool_options(threads));
    std::uint64_t target = 0;
    for (auto _ : state) {
        std::vector<std::thread> workers;
        for (int s = 0; s < submitters; ++s) {
            workers.emplace_back([&, s] {
                const std::uint64_t begin = kOps * s / submitters;
                const std::uint64_t end = kOps * (s + 1) / submitters;
                for (std::uint64_t i = begin; i < end; ++i) submit_op(pool, ops[i]);
            });
        }
        for (auto& t : workers) t.join();
        target += kOps;
        while (done.load(std::memory_order_relaxed) < target) fibers::sync::cpu_relax();
    }
    state.SetItemsProcessed(static_cast<std::int64_t>(target));
    state.counters["sleeps_per_op"] = static_cast<double>(pool.sleeps()) / static_cast<double>(target);
}
BENCHMARK(BM_Throughput)
    ->ArgNames({"submitters", "threads"})
    ->Args({1, 2})
    ->Args({4, 2})
    ->Args({8, 2})
    ->Args({1, 8})
    ->Args({4, 8})
    ->Args({8, 8})
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

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
    BlockingPool<> pool(pool_options(2));
    blocking::NowaitSupport support;
    const NowaitHandle fs = nowait_handle(support, file.fd());  // once per open file
    alignas(64) char buf[4096];
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> page(0, static_cast<int>(CachedFile::kSize / 4096) - 1);
    std::atomic<bool> done{false};
    std::uint64_t inline_hits = 0;
    for (auto _ : state) {
        const off_t offset = static_cast<off_t>(page(rng)) * 4096;
        if (mode == 0) {
            const IoResult r = read_nowait(support, fs, file.fd(), buf, sizeof(buf), offset);
            if (r.value == 4096) ++inline_hits;
        } else if (mode == 1) {
            blocking::PreadOp op(file.fd(), buf, sizeof(buf), offset);
            done.store(false, std::memory_order_relaxed);
            op.set_continuation(Task(&done, [](void* m) noexcept {
                static_cast<std::atomic<bool>*>(m)->store(true, std::memory_order_release);
            }, nullptr), Sink::run_inline());
            submit_op(pool, op);
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
BENCHMARK(BM_Pread4K)->ArgName("mode")->Arg(0)->Arg(1)->Arg(2)->UseRealTime();

// ---------------------------------------------------------------------------

void BM_IdleCpu(benchmark::State& state) {
    const auto threads = static_cast<std::size_t>(state.range(0));
    for (auto _ : state) {
        BlockingPool<> pool(pool_options(threads));
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
BENCHMARK(BM_IdleCpu)->ArgName("threads")->Arg(2)->Arg(8)->Iterations(1)->UseRealTime()->Unit(benchmark::kMillisecond);

}  // namespace
