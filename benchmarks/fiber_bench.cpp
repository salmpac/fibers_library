// Fiber scheduler benchmarks (StdSync, EpollBackend pollers).
//
//   GoRunToCompletion/W     a root fiber spawns 1000 empty fibers (local
//                           submits), all run to completion; W workers.
//                           items/s = fibers.
//   SubmitCallback/W        the same with plain Scheduler::submit callbacks
//                           (TaskMemoryPool blocks).
//   YieldLoop/F/W           F fibers on W workers, each yields 10000 times;
//                           per_op = wall time per yield (all fibers).
//   ParkWakePingPong/W      two fibers hand the turn to each other with
//                           wake() + park(); per_op = one hand-over (half a
//                           round trip). W = 1: same worker; W = 2: the
//                           partner usually runs on the other worker.
//   MutexContention<M>/N/W  N fibers on W workers, each 2000 x {lock; ++x;
//                           unlock}; M = fiber::Mutex (parks the fiber) or
//                           std::mutex (blocks the worker thread).
//                           per_op = wall time per critical section.
//   SleepWakeLatency/P      one fiber: sleep_for(50us), measure how late it
//                           runs after its deadline (overshoot_ns, mean);
//                           P pollers.
//   SleepZero/P             sleep_for(0) round trip through a poller.
//
// Run pinned to CPUs 0-9: `taskset -c 0-9 fiber_bench`. Counters cpu_min /
// cpu_max are sched_getcpu() of the fibers (sampled), cpu_main that of the
// benchmark thread, so placement is visible in the output.

#include <benchmark/benchmark.h>

#include <sched.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <thread>

#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/mutex.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace sched = fibers::sched;
namespace this_fiber = fibers::fiber::this_fiber;
using fibers::fiber::Fiber;
using fibers::sched::Scheduler;

namespace {

[[gnu::noinline]] int current_cpu() {
    asm volatile("" ::: "memory");
    return sched_getcpu();
}

// Min / max CPU seen by fibers (relaxed, sampled).
struct CpuRange {
    std::atomic<int> min{1 << 20};
    std::atomic<int> max{-1};

    void sample() noexcept {
        const int cpu = current_cpu();
        int m = min.load(std::memory_order_relaxed);
        while (cpu < m && !min.compare_exchange_weak(m, cpu, std::memory_order_relaxed)) {
        }
        m = max.load(std::memory_order_relaxed);
        while (cpu > m && !max.compare_exchange_weak(m, cpu, std::memory_order_relaxed)) {
        }
    }

    void report(benchmark::State& state) const {
        state.counters["cpu_min"] = min.load();
        state.counters["cpu_max"] = max.load();
        state.counters["cpu_main"] = current_cpu();
    }
};

// Join from the benchmark thread.
class Latch {
public:
    void reset(std::int64_t count) noexcept { count_.store(count, std::memory_order_relaxed); }

    void count_down() noexcept {
        if (count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            count_.notify_all();
        }
    }

    void wait() noexcept {
        for (std::int64_t v = count_.load(std::memory_order_acquire); v != 0;
             v = count_.load(std::memory_order_acquire)) {
            count_.wait(v, std::memory_order_acquire);
        }
    }

private:
    std::atomic<std::int64_t> count_{0};
};

sched::SchedulerOptions options(std::size_t workers, std::size_t pollers = 1) {
    sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = pollers;
    o.blocking.num_threads = 1;
    o.stacks.stack_size = 16 * 1024;
    o.stacks.max_stacks = 8192;
    o.stacks.preallocated = 1024;
    o.task_memory.initial_blocks = 4096;
    return o;
}

benchmark::Counter per_op(double ops) {
    return benchmark::Counter(ops, benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

void BM_GoRunToCompletion(benchmark::State& state) {
    constexpr int kFibers = 1000;
    Scheduler scheduler(options(static_cast<std::size_t>(state.range(0))));
    Latch done;
    CpuRange cpus;
    for (auto _ : state) {
        done.reset(kFibers + 1);
        fiber::go(scheduler, [&]() noexcept {
            for (int i = 0; i < kFibers; ++i) {
                fiber::go([&]() noexcept { done.count_down(); });
            }
            cpus.sample();
            done.count_down();
        });
        done.wait();
    }
    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * kFibers);
    cpus.report(state);
}
BENCHMARK(BM_GoRunToCompletion)->Arg(1)->Arg(2)->Arg(4)->Arg(8)->UseRealTime();

void BM_SubmitCallback(benchmark::State& state) {
    constexpr int kTasks = 1000;
    Scheduler scheduler(options(static_cast<std::size_t>(state.range(0))));
    Latch done;
    CpuRange cpus;
    for (auto _ : state) {
        done.reset(kTasks + 1);
        scheduler.submit([&]() noexcept {
            for (int i = 0; i < kTasks; ++i) {
                scheduler.submit([&]() noexcept { done.count_down(); });
            }
            cpus.sample();
            done.count_down();
        });
        done.wait();
    }
    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * kTasks);
    cpus.report(state);
}
BENCHMARK(BM_SubmitCallback)->Arg(1)->Arg(2)->Arg(4)->Arg(8)->UseRealTime();

void BM_YieldLoop(benchmark::State& state) {
    constexpr int kYields = 10'000;
    const int fibers = static_cast<int>(state.range(0));
    Scheduler scheduler(options(static_cast<std::size_t>(state.range(1))));
    Latch done;
    CpuRange cpus;
    for (auto _ : state) {
        done.reset(fibers);
        for (int f = 0; f < fibers; ++f) {
            fiber::go(scheduler, [&]() noexcept {
                for (int i = 0; i < kYields; ++i) {
                    this_fiber::yield();
                }
                cpus.sample();
                done.count_down();
            });
        }
        done.wait();
    }
    state.counters["per_op"] = per_op(static_cast<double>(state.iterations()) * fibers * kYields);
    cpus.report(state);
}
BENCHMARK(BM_YieldLoop)->Args({1, 1})->Args({2, 1})->Args({8, 1})->Args({8, 4})->UseRealTime();

void BM_ParkWakePingPong(benchmark::State& state) {
    constexpr int kRounds = 10'000;
    Scheduler scheduler(options(static_cast<std::size_t>(state.range(0))));
    Latch done;
    CpuRange cpus;
    for (auto _ : state) {
        std::atomic<Fiber*> slot[2] = {nullptr, nullptr};
        done.reset(2);
        auto player = [&](int me) {
            return [&, me]() noexcept {
                // Published = waiting for the partner's wake: from here on
                // this fiber parks, never yields (see fiber.hpp).
                slot[me].store(this_fiber::self().get(), std::memory_order_release);
                Fiber* other = nullptr;
                for (int i = 0; i < kRounds; ++i) {
                    if (me == 0 || i > 0) {
                        this_fiber::park();
                    }
                    if (!(me == 0 && i == kRounds - 1)) {
                        // Set by now: player 1 is spawned only after player
                        // 0 published, and player 0 first runs this after
                        // player 1 woke it (so after it published).
                        while (other == nullptr) {
                            other = slot[1 - me].load(std::memory_order_acquire);
                        }
                        other->wake();
                    }
                }
                cpus.sample();
                done.count_down();
            };
        };
        // Player 0 parks first; it must be published before player 1 serves.
        fiber::go(scheduler, player(0));
        while (slot[0].load(std::memory_order_acquire) == nullptr) {
            std::this_thread::yield();
        }
        fiber::go(scheduler, player(1));
        done.wait();
    }
    state.counters["per_op"] = per_op(static_cast<double>(state.iterations()) * (2 * kRounds - 1));
    cpus.report(state);
}
BENCHMARK(BM_ParkWakePingPong)->Arg(1)->Arg(2)->UseRealTime();

template <class M>
void BM_MutexContention(benchmark::State& state) {
    constexpr int kOps = 2000;
    const int fibers = static_cast<int>(state.range(0));
    Scheduler scheduler(options(static_cast<std::size_t>(state.range(1))));
    Latch done;
    CpuRange cpus;
    M mutex;
    std::int64_t counter = 0;
    for (auto _ : state) {
        done.reset(fibers);
        for (int f = 0; f < fibers; ++f) {
            fiber::go(scheduler, [&]() noexcept {
                for (int i = 0; i < kOps; ++i) {
                    std::lock_guard guard(mutex);
                    ++counter;
                }
                cpus.sample();
                done.count_down();
            });
        }
        done.wait();
    }
    benchmark::DoNotOptimize(counter);
    state.counters["per_op"] = per_op(static_cast<double>(state.iterations()) * fibers * kOps);
    cpus.report(state);
}
BENCHMARK(BM_MutexContention<fiber::Mutex>)
    ->Args({1, 1})->Args({8, 1})->Args({8, 4})->Args({64, 4})->Args({64, 8})->UseRealTime();
BENCHMARK(BM_MutexContention<std::mutex>)
    ->Args({1, 1})->Args({8, 1})->Args({8, 4})->Args({64, 4})->Args({64, 8})->UseRealTime();

void BM_SleepWakeLatency(benchmark::State& state) {
    constexpr int kSleeps = 200;
    const auto duration = std::chrono::microseconds(50);
    Scheduler scheduler(options(2, static_cast<std::size_t>(state.range(0))));
    Latch done;
    CpuRange cpus;
    std::int64_t overshoot_ns = 0;
    std::int64_t sleeps = 0;
    for (auto _ : state) {
        done.reset(1);
        fiber::go(scheduler, [&]() noexcept {
            for (int i = 0; i < kSleeps; ++i) {
                const auto deadline = scheduler.now() + duration;
                this_fiber::sleep_until(deadline);
                overshoot_ns += (scheduler.now() - deadline).count();
                ++sleeps;
            }
            cpus.sample();
            done.count_down();
        });
        done.wait();
    }
    state.counters["overshoot_ns"] = static_cast<double>(overshoot_ns) / static_cast<double>(sleeps);
    cpus.report(state);
}
BENCHMARK(BM_SleepWakeLatency)->Arg(1)->Arg(2)->UseRealTime()->Unit(benchmark::kMillisecond);

void BM_SleepZero(benchmark::State& state) {
    constexpr int kSleeps = 1000;
    Scheduler scheduler(options(2, static_cast<std::size_t>(state.range(0))));
    Latch done;
    CpuRange cpus;
    for (auto _ : state) {
        done.reset(1);
        fiber::go(scheduler, [&]() noexcept {
            for (int i = 0; i < kSleeps; ++i) {
                this_fiber::sleep_for(std::chrono::nanoseconds(0));
            }
            cpus.sample();
            done.count_down();
        });
        done.wait();
    }
    state.counters["per_op"] = per_op(static_cast<double>(state.iterations()) * kSleeps);
    cpus.report(state);
}
BENCHMARK(BM_SleepZero)->Arg(1)->Arg(2)->UseRealTime();

}  // namespace
