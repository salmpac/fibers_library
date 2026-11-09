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
//   OneshotRequestResponse/P/W
//   FutureRequestResponse/P/W P client / server fiber pairs, each running
//                             1000 request / response round trips: the
//                             client sends {i, reply promise} through the
//                             server's current request channel, the server
//                             answers {i + 1, promise of its next request
//                             channel}. Oneshot: both channels on the
//                             fibers' stacks; Future: pooled pairs from the
//                             scheduler's TaskMemoryPool. items/s = round
//                             trips (2 hand-offs, 2 parks / wakes each).
//
// Run pinned to CPUs 0-9: `taskset -c 0-9 sync_bench`. Counters cpu_min /
// cpu_max are sched_getcpu() of the fibers (sampled), cpu_main that of the
// benchmark thread, so placement is visible in the output.

#include <benchmark/benchmark.h>

#include <sched.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

#include "fibers/fiber/event.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/future.hpp"
#include "fibers/fiber/oneshot.hpp"
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

// ---- request / response ------------------------------------------------------

constexpr int kRoundTrips = 1000;

// The same message types for both variants: a fiber::Promise is the same
// type whether its state is in a Oneshot or in a pooled block.
struct Request;
struct Reply {
    int value = 0;
    fiber::Promise<Request> next;  // the server's next request channel
};
struct Request {
    int value = 0;
    fiber::Promise<Reply> reply;
};

// Client of one pair (spawns its server). Oneshot: every channel lives on
// the stack of the fiber that receives on it.
void oneshot_pair(Latch& done, CpuRange& cpus) {
    fiber::Oneshot<fiber::Promise<Request>> hello;
    fiber::go([&done, p = hello.promise()]() mutable noexcept {  // server
        std::optional<fiber::Oneshot<Request>> slots[2];
        slots[0].emplace();
        p.set(slots[0]->promise());
        for (int i = 0;; ++i) {
            fibers::wait::Result<Request> rq = slots[i % 2]->get();
            if (!rq) {
                break;  // the client has gone
            }
            fiber::Oneshot<Request>& next = slots[(i + 1) % 2].emplace();
            rq->reply.set(Reply{rq->value + 1, next.promise()});
        }
        done.count_down();
    });
    fiber::Promise<Request> to_server = *hello.get();
    int bad = 0;
    for (int i = 0; i < kRoundTrips; ++i) {
        fiber::Oneshot<Reply> reply;
        to_server.set(Request{i, reply.promise()});
        fibers::wait::Result<Reply> r = reply.get();
        bad += r->value != i + 1 ? 1 : 0;
        to_server = std::move(r->next);
        if (i % 256 == 0) {
            cpus.sample();
        }
    }
    benchmark::DoNotOptimize(bad);
}

// The same with pooled Promise / Future pairs (scheduler.task_memory()).
void future_pair(Scheduler& scheduler, Latch& done, CpuRange& cpus) {
    auto [hello_promise, hello] = fiber::make_promise_future<fiber::Promise<Request>>(scheduler);
    fiber::go([&scheduler, &done, p = std::move(hello_promise)]() mutable noexcept {  // server
        auto [first_promise, first] = fiber::make_promise_future<Request>(scheduler);
        p.set(std::move(first_promise));
        fiber::Future<Request> current = std::move(first);
        for (;;) {
            fibers::wait::Result<Request> rq = current.get();
            if (!rq) {
                break;
            }
            auto [next_promise, next] = fiber::make_promise_future<Request>(scheduler);
            rq->reply.set(Reply{rq->value + 1, std::move(next_promise)});
            current = std::move(next);
        }
        done.count_down();
    });
    fiber::Promise<Request> to_server = *hello.get();
    int bad = 0;
    for (int i = 0; i < kRoundTrips; ++i) {
        auto [reply_promise, reply] = fiber::make_promise_future<Reply>(scheduler);
        to_server.set(Request{i, std::move(reply_promise)});
        fibers::wait::Result<Reply> r = reply.get();
        bad += r->value != i + 1 ? 1 : 0;
        to_server = std::move(r->next);
        if (i % 256 == 0) {
            cpus.sample();
        }
    }
    benchmark::DoNotOptimize(bad);
}

template <bool kPooled>
void request_response(benchmark::State& state) {
    const auto pairs = static_cast<int>(state.range(0));
    Scheduler scheduler(options(static_cast<std::size_t>(state.range(1))));
    Latch done;
    CpuRange cpus;
    for (auto _ : state) {
        done.reset(2 * pairs);  // clients and servers
        for (int p = 0; p < pairs; ++p) {
            fiber::go(scheduler, [&]() noexcept {
                if constexpr (kPooled) {
                    future_pair(scheduler, done, cpus);
                } else {
                    oneshot_pair(done, cpus);
                }
                done.count_down();
            });
        }
        done.wait();
    }
    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * pairs * kRoundTrips);
    cpus.report(state);
}

void BM_OneshotRequestResponse(benchmark::State& state) { request_response<false>(state); }
void BM_FutureRequestResponse(benchmark::State& state) { request_response<true>(state); }

BENCHMARK(BM_OneshotRequestResponse)
    ->Args({1, 1})
    ->Args({1, 2})
    ->Args({1, 4})
    ->Args({1, 8})
    ->Args({64, 1})
    ->Args({64, 2})
    ->Args({64, 4})
    ->Args({64, 8})
    ->UseRealTime();
BENCHMARK(BM_FutureRequestResponse)
    ->Args({1, 1})
    ->Args({1, 2})
    ->Args({1, 4})
    ->Args({1, 8})
    ->Args({64, 1})
    ->Args({64, 2})
    ->Args({64, 4})
    ->Args({64, 8})
    ->UseRealTime();

}  // namespace
