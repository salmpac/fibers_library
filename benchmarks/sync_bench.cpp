// Fiber synchronisation primitives other than Mutex (StdSync, EpollBackend
// pollers).
//
//   WaitGroupFanOutFanIn/N/W  a root fiber: N x wg.go(empty fiber), then
//                             wg.wait(); W workers. items/s = child fibers
//                             (spawn + run + done + the join, amortised).
//   GoLatchFanOutFanIn/N/W    the same join done with an atomic counter and
//                             a last-one-wakes handle: the baseline for the
//                             WaitGroup's own cost.
//   EventBroadcast/N/W        N fibers wait on one Event, the root sets it;
//                             items/s = woken fibers.
//
// Run pinned to CPUs 0-9: `taskset -c 0-9 sync_bench`. Counters cpu_min /
// cpu_max are sched_getcpu() of the fibers (sampled), cpu_main that of the
// benchmark thread, so placement is visible in the output.

#include <benchmark/benchmark.h>

#include <sched.h>

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "fibers/fiber/event.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/wait_group.hpp"
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

sched::SchedulerOptions options(std::size_t workers) {
    sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = 1;
    o.blocking.num_threads = 1;
    o.stacks.stack_size = 16 * 1024;
    o.stacks.max_stacks = 8192;
    o.stacks.preallocated = 1024;
    o.task_memory.initial_blocks = 1024;
    return o;
}

void BM_WaitGroupFanOutFanIn(benchmark::State& state) {
    const auto children = static_cast<int>(state.range(0));
    Scheduler scheduler(options(static_cast<std::size_t>(state.range(1))));
    Latch done;
    CpuRange cpus;
    for (auto _ : state) {
        done.reset(1);
        fiber::go(scheduler, [&]() noexcept {
            fiber::WaitGroup wg;
            for (int i = 0; i < children; ++i) {
                wg.go([&cpus, i]() noexcept {
                    if (i % 64 == 0) {
                        cpus.sample();
                    }
                });
            }
            wg.wait();
            done.count_down();
        });
        done.wait();
    }
    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * children);
    cpus.report(state);
}
BENCHMARK(BM_WaitGroupFanOutFanIn)
    ->Args({100, 1})
    ->Args({1000, 1})
    ->Args({1000, 2})
    ->Args({1000, 4})
    ->Args({1000, 8})
    ->Args({10000, 8})
    ->UseRealTime();

// Baseline: the same fan-out with a hand-rolled join (counter + the root's
// handle, woken by the last child; root parks after publishing itself).
void BM_GoLatchFanOutFanIn(benchmark::State& state) {
    const auto children = static_cast<int>(state.range(0));
    Scheduler scheduler(options(static_cast<std::size_t>(state.range(1))));
    Latch done;
    CpuRange cpus;
    for (auto _ : state) {
        done.reset(1);
        fiber::go(scheduler, [&]() noexcept {
            // +1: the root's own unit, dropped after it has published itself.
            std::atomic<std::int64_t> pending{children + 1};
            Fiber* root = Fiber::current();
            for (int i = 0; i < children; ++i) {
                fiber::go([&, i]() noexcept {
                    if (i % 64 == 0) {
                        cpus.sample();
                    }
                    if (pending.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                        root->wake();
                    }
                });
            }
            if (pending.fetch_sub(1, std::memory_order_acq_rel) != 1) {
                root->park();
            }
            done.count_down();
        });
        done.wait();
    }
    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * children);
    cpus.report(state);
}
BENCHMARK(BM_GoLatchFanOutFanIn)
    ->Args({100, 1})
    ->Args({1000, 1})
    ->Args({1000, 2})
    ->Args({1000, 4})
    ->Args({1000, 8})
    ->Args({10000, 8})
    ->UseRealTime();

void BM_EventBroadcast(benchmark::State& state) {
    const auto waiters = static_cast<int>(state.range(0));
    Scheduler scheduler(options(static_cast<std::size_t>(state.range(1))));
    Latch done;
    CpuRange cpus;
    for (auto _ : state) {
        done.reset(1);
        fiber::go(scheduler, [&]() noexcept {
            fiber::Event event;
            fiber::WaitGroup woken;
            for (int i = 0; i < waiters; ++i) {
                woken.go([&event, &cpus, i]() noexcept {
                    event.wait();
                    if (i % 64 == 0) {
                        cpus.sample();
                    }
                });
            }
            this_fiber::yield();  // let some of them park first
            event.set();
            woken.wait();
            done.count_down();
        });
        done.wait();
    }
    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * waiters);
    cpus.report(state);
}
BENCHMARK(BM_EventBroadcast)->Args({1000, 1})->Args({1000, 4})->Args({1000, 8})->UseRealTime();

}  // namespace
