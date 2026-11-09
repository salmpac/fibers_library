// fs benchmarks: file I/O and this_fiber::blocking() from fibers on a full Scheduler.
//
//   BM_Pread4K/mode    4 KiB pread from the page cache, one fiber (or the
//                      benchmark thread), latency per call measured around
//                      each call:
//                        0 fiber, fs::pread (RWF_NOWAIT inline hit)
//                        1 fiber, fs::pread with every nowait attempt
//                          answering EAGAIN (forced offload: one round trip
//                          through the BlockingPool per read)
//                        2 plain ::pread on the benchmark thread (reference)
//                        3 fiber, plain ::pread on the worker (no offload,
//                          what a fiber that just blocks would pay when hot)
//   BM_Pwrite4K/mode   4 KiB pwrite, one fiber: 0 fs::pwrite with a tracked
//                      filesystem (ext4 rejects nowait writes: learnt once,
//                      then offloaded), 1 untracked (offloaded at once),
//                      2 plain ::pwrite on the benchmark thread,
//                      3 a doomed pwritev2(RWF_NOWAIT) alone (what an untracked
//                      nowait attempt would cost per write on ext4 / tmpfs)
//   BM_OffloadRoundTrip/idle  this_fiber::blocking of an empty call: 0 back to back,
//                      1 with 200 us sleeps between calls (blocking threads
//                      parked each time: two wake-ups per call)
//   BM_PreadThroughput/fibers/workers/offload  N fibers on W workers each
//                      reading 4 KiB pages, default 2 blocking threads;
//                      offload 0: hot cache (inline), 1: forced offload.
//                      items/s = reads/s.
//
// Every run reports cpus (the affinity mask size) and max_cpu: run under
// taskset -c 0-9 and check max_cpu <= 9. Results are counters (median_ns /
// p99_ns are per call); --benchmark_repetitions=3. The test file is created
// in the current directory (a disk filesystem, not tmpfs).

#include <benchmark/benchmark.h>

#include <fcntl.h>
#include <sched.h>
#include <sys/uio.h>
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

#include "fibers/blocking/nowait.hpp"
#include "fibers/fiber/blocking.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fs/file.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace fs = fibers::fs;
namespace blocking = fibers::blocking;
using blocking::IoResult;
using blocking::NowaitSupport;
using fibers::sched::Scheduler;
using namespace std::chrono_literals;

namespace {

std::int64_t mono_ns() {
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<std::int64_t>(ts.tv_sec) * 1'000'000'000 + ts.tv_nsec;
}

void report_placement(benchmark::State& state) {
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set) != 0) return;
    int max_cpu = -1;
    for (int c = 0; c < CPU_SETSIZE; ++c) {
        if (CPU_ISSET(c, &set)) max_cpu = c;
    }
    state.counters["cpus"] = CPU_COUNT(&set);
    state.counters["max_cpu"] = max_cpu;
}

void report_percentiles(benchmark::State& state, std::vector<std::int64_t>& samples) {
    if (samples.empty()) return;
    std::int64_t sum = 0;
    for (std::int64_t s : samples) sum += s;
    std::sort(samples.begin(), samples.end());
    state.counters["mean_ns"] = static_cast<double>(sum) / static_cast<double>(samples.size());
    state.counters["median_ns"] = static_cast<double>(samples[samples.size() / 2]);
    state.counters["p99_ns"] = static_cast<double>(samples[samples.size() * 99 / 100]);
    state.counters["max_ns"] = static_cast<double>(samples.back());
}

fibers::sched::SchedulerOptions options(std::size_t workers) {
    fibers::sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = 1;
    // o.blocking: the default 2 threads.
    return o;
}

// Waits for fibers from the benchmark thread.
class Latch {
public:
    explicit Latch(std::int64_t count) : count_(count) {}
    void count_down() noexcept { count_.fetch_sub(1, std::memory_order_acq_rel); }
    void wait() {
        while (count_.load(std::memory_order_acquire) != 0) std::this_thread::sleep_for(50us);
    }

private:
    std::atomic<std::int64_t> count_;
};

class CachedFile {
public:
    static constexpr std::size_t kSize = 4 << 20;
    static constexpr std::size_t kPages = kSize / 4096;

    CachedFile() {
        char name[] = "./fs_bench_XXXXXX";
        fd_ = mkstemp(name);
        if (fd_ < 0) {
            std::perror("mkstemp");
            std::abort();
        }
        unlink(name);
        std::vector<char> data(kSize, 'x');
        if (::pwrite(fd_, data.data(), data.size(), 0) != static_cast<ssize_t>(kSize)) std::abort();
        if (::pread(fd_, data.data(), data.size(), 0) != static_cast<ssize_t>(kSize)) std::abort();  // warm
    }
    ~CachedFile() { ::close(fd_); }
    int fd() const { return fd_; }

private:
    int fd_ = -1;
};

std::vector<off_t> random_offsets(std::size_t n, unsigned seed) {
    std::mt19937 rng(seed);
    std::uniform_int_distribution<std::size_t> page(0, CachedFile::kPages - 1);
    std::vector<off_t> offsets(n);
    for (auto& o : offsets) o = static_cast<off_t>(page(rng) * 4096);
    return offsets;
}

ssize_t always_eagain(int, const iovec*, int, off_t, int) {
    errno = EAGAIN;
    return -1;
}

// ---------------------------------------------------------------------------

void BM_Pread4K(benchmark::State& state) {
    const int mode = static_cast<int>(state.range(0));
    const std::size_t ops = mode == 1 ? 50'000 : 200'000;
    CachedFile file;
    NowaitSupport real;
    NowaitSupport forced(&always_eagain, &always_eagain);
    NowaitSupport& support = mode == 1 ? forced : real;
    const NowaitSupport::Fs nowait = support.filesystem(file.fd());
    const std::vector<off_t> offsets = random_offsets(ops, 42);
    std::vector<std::int64_t> samples(ops);
    std::size_t bad = 0;
    Scheduler scheduler(options(2));
    for (auto _ : state) {
        auto body = [&]() noexcept {
            alignas(64) char buf[4096];
            for (std::size_t i = 0; i < ops; ++i) {
                const std::int64_t t0 = mono_ns();
                ssize_t n;
                if (mode == 0 || mode == 1) {
                    n = fs::pread(file.fd(), buf, sizeof(buf), offsets[i], nowait, support).value;
                } else {
                    n = ::pread(file.fd(), buf, sizeof(buf), offsets[i]);
                }
                samples[i] = mono_ns() - t0;
                bad += n == 4096 ? 0 : 1;
                benchmark::DoNotOptimize(buf);
            }
        };
        if (mode == 2) {
            body();
        } else {
            Latch done(1);
            fiber::go(scheduler, [&]() noexcept {
                body();
                done.count_down();
            });
            done.wait();
        }
    }
    if (bad != 0) state.SkipWithError("short read");
    report_percentiles(state, samples);
    report_placement(state);
    state.SetItemsProcessed(static_cast<std::int64_t>(ops));
}
BENCHMARK(BM_Pread4K)->ArgName("mode")->DenseRange(0, 3)->Iterations(1)->UseRealTime()->Unit(benchmark::kMillisecond);

void BM_Pwrite4K(benchmark::State& state) {
    const int mode = static_cast<int>(state.range(0));
    const std::size_t ops = mode == 2 || mode == 3 ? 200'000 : 50'000;
    CachedFile file;
    NowaitSupport support;
    const NowaitSupport::Fs tracked = support.filesystem(file.fd());
    const std::vector<off_t> offsets = random_offsets(ops, 7);
    std::vector<std::int64_t> samples(ops);
    std::size_t bad = 0;
    int doomed_errno = 0;
    Scheduler scheduler(options(2));
    for (auto _ : state) {
        auto body = [&]() noexcept {
            alignas(64) char buf[4096] = {'y'};
            for (std::size_t i = 0; i < ops; ++i) {
                const std::int64_t t0 = mono_ns();
                ssize_t n;
                if (mode == 0) {
                    n = fs::pwrite(file.fd(), buf, sizeof(buf), offsets[i], tracked, support).value;
                } else if (mode == 1) {
                    n = fs::pwrite(file.fd(), buf, sizeof(buf), offsets[i]).value;
                } else if (mode == 2) {
                    n = ::pwrite(file.fd(), buf, sizeof(buf), offsets[i]);
                } else {
                    const iovec iov{buf, sizeof(buf)};
                    n = ::pwritev2(file.fd(), &iov, 1, offsets[i], RWF_NOWAIT);
                    if (n < 0) {
                        doomed_errno = errno;
                        n = 4096;  // the expected outcome here is a failure
                    }
                }
                samples[i] = mono_ns() - t0;
                bad += n == 4096 ? 0 : 1;
            }
        };
        if (mode >= 2) {
            body();
        } else {
            Latch done(1);
            fiber::go(scheduler, [&]() noexcept {
                body();
                done.count_down();
            });
            done.wait();
        }
    }
    if (bad != 0) state.SkipWithError("short write");
    if (mode == 0) state.counters["nowait_write_supported"] = support.write_supported(tracked) ? 1 : 0;
    if (mode == 3) state.counters["nowait_errno"] = doomed_errno;
    report_percentiles(state, samples);
    report_placement(state);
    state.SetItemsProcessed(static_cast<std::int64_t>(ops));
}
BENCHMARK(BM_Pwrite4K)->ArgName("mode")->DenseRange(0, 3)->Iterations(1)->UseRealTime()->Unit(benchmark::kMillisecond);

void BM_OffloadRoundTrip(benchmark::State& state) {
    const bool idle = state.range(0) == 1;
    const std::size_t ops = idle ? 5'000 : 100'000;
    std::vector<std::int64_t> samples(ops);
    Scheduler scheduler(options(2));
    for (auto _ : state) {
        Latch done(1);
        fiber::go(scheduler, [&]() noexcept {
            for (std::size_t i = 0; i < ops; ++i) {
                if (idle) fiber::this_fiber::sleep_for(200us);  // every thread back asleep
                const std::int64_t t0 = mono_ns();
                fiber::this_fiber::blocking([]() noexcept {});
                samples[i] = mono_ns() - t0;
            }
            done.count_down();
        });
        done.wait();
    }
    report_percentiles(state, samples);
    report_placement(state);
}
BENCHMARK(BM_OffloadRoundTrip)->ArgName("idle")->Arg(0)->Arg(1)->Iterations(1)->UseRealTime()->Unit(benchmark::kMillisecond);

void BM_PreadThroughput(benchmark::State& state) {
    const auto fibers_count = static_cast<std::size_t>(state.range(0));
    const auto workers = static_cast<std::size_t>(state.range(1));
    const bool offload = state.range(2) == 1;
    const std::size_t total = offload ? 200'000 : 2'000'000;
    const std::size_t per_fiber = total / fibers_count;
    CachedFile file;
    NowaitSupport real;
    NowaitSupport forced(&always_eagain, &always_eagain);
    NowaitSupport& support = offload ? forced : real;
    const NowaitSupport::Fs nowait = support.filesystem(file.fd());
    const std::vector<off_t> offsets = random_offsets(4096, 3);
    std::atomic<std::size_t> bad{0};
    Scheduler scheduler(options(workers));
    for (auto _ : state) {
        Latch done(static_cast<std::int64_t>(fibers_count));
        for (std::size_t f = 0; f < fibers_count; ++f) {
            fiber::go(scheduler, [&, f]() noexcept {
                alignas(64) char buf[4096];
                std::size_t errors = 0;
                for (std::size_t i = 0; i < per_fiber; ++i) {
                    const off_t at = offsets[(f * 977 + i) % offsets.size()];
                    errors += fs::pread(file.fd(), buf, sizeof(buf), at, nowait, support).value == 4096 ? 0 : 1;
                }
                bad.fetch_add(errors);
                done.count_down();
            });
        }
        done.wait();
    }
    if (bad.load() != 0) state.SkipWithError("short read");
    report_placement(state);
    state.SetItemsProcessed(static_cast<std::int64_t>(per_fiber * fibers_count * state.iterations()));
}
BENCHMARK(BM_PreadThroughput)
    ->ArgNames({"fibers", "workers", "offload"})
    ->ArgsProduct({{1, 16, 256}, {2, 4, 8}, {0, 1}})
    ->Iterations(1)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

}  // namespace
