// Poller and shared-memory ring benchmarks.
//
//   BM_TimerScheduleFire     schedule -> fire throughput (timers due now)
//   BM_EventfdWakeLatency    eventfd write -> Epollable callback, poller asleep
//                            in epoll_wait (median / p99)
//   BM_RingThroughput        cross-process (fork) ring throughput, consumer
//                            spinning (Checkable, never sleeps) or allowed to
//                            sleep (default spin budget)
//   BM_RingLatency           cross-process one-way latency (send timestamp in
//                            the item, CLOCK_MONOTONIC is system-wide), with
//                            the consumer spinning or sleeping (eventfd wake)
//   BM_IdleCpu               poller CPU over 500 ms: epoll only / armable ring
//
// Latency / CPU results are reported as counters (ns, %), one experiment per
// repetition (Iterations(1)); run with --benchmark_repetitions=3.

#include <benchmark/benchmark.h>

#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

#include "ipc/ring_checkable.hpp"
#include "ipc/spsc_ring.hpp"
#include "poller/eventfd.hpp"
#include "poller/poller.hpp"

namespace {

using namespace std::chrono_literals;
using poller::TaskBatch;
using poller::TaskSink;
using poller::TimerEvent;

struct InlineSink {
    void submit_batch(poller::Task* tasks, std::size_t count) noexcept {
        for (std::size_t i = 0; i < count; ++i) std::move(tasks[i]).run();
    }
};

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

// ---------------------------------------------------------------------------

void BM_TimerScheduleFire(benchmark::State& state) {
    const std::size_t batch = static_cast<std::size_t>(state.range(0));
    InlineSink sink;
    poller::Poller<> p(TaskSink::of(sink));
    p.start();
    std::atomic<std::size_t> fired{0};
    std::vector<poller::Timer> timers(batch);
    for (auto& t : timers) {
        t.memo = &fired;
        t.callback = [](void* memo, TimerEvent, TaskBatch&) noexcept {
            static_cast<std::atomic<std::size_t>*>(memo)->fetch_add(1, std::memory_order_release);
        };
    }
    std::size_t target = 0;
    for (auto _ : state) {
        const auto now = p.now();
        for (auto& t : timers) {
            t.deadline = now;
            p.schedule(t);
        }
        target += batch;
        while (fired.load(std::memory_order_acquire) < target) {
            thread_pool::cpu_relax();
        }
    }
    p.stop();
    state.SetItemsProcessed(static_cast<std::int64_t>(target));
    state.counters["wakeups_per_batch"] =
        static_cast<double>(p.stats().timer_wakeups) / static_cast<double>(state.iterations());
}
BENCHMARK(BM_TimerScheduleFire)->Arg(1)->Arg(64)->Arg(1024)->UseRealTime();

// ---------------------------------------------------------------------------

void BM_EventfdWakeLatency(benchmark::State& state) {
    InlineSink sink;
    poller::Poller<> p(TaskSink::of(sink));
    struct Probe {
        int fd = poller::make_eventfd();
        std::atomic<std::int64_t> seen_at{0};
    } probe;
    poller::Epollable e{probe.fd, &probe, [](void* memo, std::uint32_t events, TaskBatch&) noexcept {
                            if ((events & EPOLLIN) == 0) return;
                            auto* self = static_cast<Probe*>(memo);
                            poller::eventfd_drain(self->fd);
                            self->seen_at.store(mono_ns(), std::memory_order_release);
                        }};
    p.watch(e, EPOLLIN | EPOLLET);
    p.start();
    constexpr int kSamples = 20'000;
    std::vector<std::int64_t> samples;
    samples.reserve(kSamples);
    for (auto _ : state) {
        for (int i = 0; i < kSamples; ++i) {
            std::this_thread::sleep_for(20us);  // the poller is back in epoll_wait
            probe.seen_at.store(0, std::memory_order_relaxed);
            const std::int64_t sent = mono_ns();
            poller::eventfd_signal(probe.fd);
            std::int64_t seen;
            while ((seen = probe.seen_at.load(std::memory_order_acquire)) == 0) {
                thread_pool::cpu_relax();
            }
            samples.push_back(seen - sent);
        }
    }
    p.stop();
    p.unwatch(e);
    close(probe.fd);
    report_percentiles(state, samples);
}
BENCHMARK(BM_EventfdWakeLatency)->Iterations(1)->UseRealTime()->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------------------------
// Cross-process ring: fork()ed producer, poller + RingCheckable consumer.

struct Item {
    std::int64_t sent_ns;
    std::uint64_t seq;
};
using Ring = ipc::Ring<Item>;

struct Shared {
    std::atomic<int> go{0};
    std::atomic<std::uint64_t> rings{0};
};

struct Experiment {
    static constexpr std::size_t kCapacity = 4096;
    std::size_t bytes = Ring::bytes_for(kCapacity) + 4096;
    void* memory = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    Ring ring = Ring::create(memory, Ring::bytes_for(kCapacity), kCapacity);
    Shared* shared = new (static_cast<char*>(memory) + Ring::bytes_for(kCapacity)) Shared{};
    int bell_fd = poller::make_eventfd();

    ~Experiment() {
        close(bell_fd);
        munmap(memory, bytes);
    }

    // Child: wait for `go`, then send `count` items, `gap` apart (0: flat out).
    template <class Gap>
    pid_t spawn_producer(std::uint64_t count, Gap gap) {
        const pid_t pid = fork();
        if (pid != 0) return pid;
        ipc::Producer<Item> producer(ring);
        ipc::EventFdDoorbell bell{bell_fd};
        while (shared->go.load(std::memory_order_acquire) == 0) {
        }
        Item batch[32];
        std::uint64_t next = 0;
        if (gap == Gap::zero()) {
            while (next < count) {
                const std::size_t n = static_cast<std::size_t>(std::min<std::uint64_t>(32, count - next));
                for (std::size_t i = 0; i < n; ++i) batch[i] = Item{0, next + i};
                const std::size_t pushed = producer.try_push(batch, n);
                if (pushed != 0 && producer.notify(bell)) shared->rings.fetch_add(1);
                next += pushed;
            }
        } else {
            for (; next < count; ++next) {
                const std::int64_t until = mono_ns() + std::chrono::nanoseconds(gap).count();
                while (mono_ns() < until) {
                }
                Item item{mono_ns(), next};
                while (producer.try_push(&item, 1) == 0) {
                }
                if (producer.notify(bell)) shared->rings.fetch_add(1);
            }
        }
        _exit(0);
    }
};

struct Received {
    std::atomic<std::uint64_t> count{0};
    std::vector<std::int64_t>* latencies = nullptr;
};

void on_items(void* context, const Item* items, std::size_t n, TaskBatch&) noexcept {
    auto* r = static_cast<Received*>(context);
    if (r->latencies != nullptr) {
        const std::int64_t now = mono_ns();
        for (std::size_t i = 0; i < n; ++i) r->latencies->push_back(now - items[i].sent_ns);
    }
    r->count.fetch_add(n, std::memory_order_release);
}

// arg 0: spin forever, 1: default spin budget (may sleep)
void BM_RingThroughput(benchmark::State& state) {
    constexpr std::uint64_t kItems = 20'000'000;
    const bool spin = state.range(0) == 0;
    std::uint64_t rings = 0;
    for (auto _ : state) {
        Experiment ex;
        const pid_t child = ex.spawn_producer(kItems, std::chrono::nanoseconds::zero());
        InlineSink sink;
        Received received;
        poller::PollerOptions options;
        if (spin) options.spin_budget = poller::kSpinForever;
        poller::Poller<> p(TaskSink::of(sink), options);
        ipc::RingCheckable<Item, poller::StdSync, ipc::EventFdDoorbell, 64> consumer(
            ex.ring, ipc::EventFdDoorbell{ex.bell_fd}, &on_items, &received);
        p.add(consumer.checkable());
        p.start();
        const auto start = std::chrono::steady_clock::now();
        ex.shared->go.store(1, std::memory_order_release);
        while (received.count.load(std::memory_order_acquire) < kItems) {
            std::this_thread::sleep_for(100us);
        }
        const auto elapsed = std::chrono::steady_clock::now() - start;
        state.SetIterationTime(std::chrono::duration<double>(elapsed).count());
        waitpid(child, nullptr, 0);
        p.stop();
        p.remove(consumer.checkable());
        rings += ex.shared->rings.load();
    }
    state.SetItemsProcessed(static_cast<std::int64_t>(kItems * state.iterations()));
    state.counters["doorbells"] = static_cast<double>(rings);
}
BENCHMARK(BM_RingThroughput)->Arg(0)->Arg(1)->Iterations(1)->UseManualTime()->Unit(benchmark::kMillisecond);

// arg 0: consumer spins, items 20 us apart; 1: consumer sleeps, items 1 ms apart.
void BM_RingLatency(benchmark::State& state) {
    const bool spin = state.range(0) == 0;
    const std::uint64_t items = spin ? 20'000 : 1'000;
    const auto gap = spin ? std::chrono::nanoseconds(20us) : std::chrono::nanoseconds(1ms);
    std::vector<std::int64_t> latencies;
    latencies.reserve(items);
    std::uint64_t rings = 0, sleeps = 0;
    for (auto _ : state) {
        Experiment ex;
        const pid_t child = ex.spawn_producer(items, gap);
        InlineSink sink;
        Received received;
        received.latencies = &latencies;
        poller::PollerOptions options;
        options.spin_budget = spin ? poller::kSpinForever : 200;
        poller::Poller<> p(TaskSink::of(sink), options);
        ipc::RingCheckable<Item> consumer(ex.ring, ipc::EventFdDoorbell{ex.bell_fd}, &on_items, &received);
        p.add(consumer.checkable());
        p.start();
        ex.shared->go.store(1, std::memory_order_release);
        while (received.count.load(std::memory_order_acquire) < items) {
            std::this_thread::sleep_for(1ms);
        }
        waitpid(child, nullptr, 0);
        p.stop();
        p.remove(consumer.checkable());
        rings += ex.shared->rings.load();
        sleeps += p.stats().sleeps;
    }
    report_percentiles(state, latencies);
    state.counters["doorbells"] = static_cast<double>(rings);
    state.counters["poller_sleeps"] = static_cast<double>(sleeps);
}
BENCHMARK(BM_RingLatency)->Arg(0)->Arg(1)->Iterations(1)->UseRealTime()->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------------------------

// arg 0: epoll only (one idle eventfd); 1: one armable ring Checkable.
void BM_IdleCpu(benchmark::State& state) {
    for (auto _ : state) {
        InlineSink sink;
        poller::Poller<> p(TaskSink::of(sink));
        Experiment ex;
        Received received;
        ipc::RingCheckable<Item> consumer(ex.ring, ipc::EventFdDoorbell{ex.bell_fd}, &on_items, &received);
        poller::Epollable idle_fd{ex.bell_fd, nullptr, [](void*, std::uint32_t, TaskBatch&) noexcept {}};
        if (state.range(0) == 0) {
            p.watch(idle_fd, EPOLLIN | EPOLLET);
        } else {
            p.add(consumer.checkable());
        }
        p.start();
        std::this_thread::sleep_for(20ms);
        const auto before = process_cpu();
        std::this_thread::sleep_for(500ms);
        const auto cpu = process_cpu() - before;
        state.counters["cpu_pct"] = 100.0 * static_cast<double>(cpu.count()) / 500'000.0;
        state.counters["sleeps"] = static_cast<double>(p.stats().sleeps);
        p.stop();
        if (state.range(0) == 0) {
            p.unwatch(idle_fd);
        } else {
            p.remove(consumer.checkable());
        }
    }
}
BENCHMARK(BM_IdleCpu)->Arg(0)->Arg(1)->Iterations(1)->UseRealTime()->Unit(benchmark::kMillisecond);

}  // namespace
