// Cost of request contexts (fibers/fiber/context.hpp, docs/cancellation.md).
//
//   SocketPingPong/C/W    two fibers bounce one byte over a socketpair
//                         (each read parks: EAGAIN, arm, park, an edge
//                         from the poller); C = 0: no context, 1: both
//                         fibers under Context::cancellable(), 2: under a
//                         context with a deadline (1 h). per_op = one round
//                         trip. The difference C=1/2 - C=0 is the enlist /
//                         delist of the interrupt hook per parked wait.
//   ContextLifecycle/K    in a fiber, create + destroy one context per
//                         iteration: K = 0 cancellable(), 1 child_of(a
//                         live parent), 2 with_timeout(1 h) (a deadline
//                         record from the TaskMemoryPool, its poller timer
//                         scheduled; the destructor cancels it lazily and
//                         the poller hands the record back to the pool
//                         later: no round trip, no waiting), 3
//                         with_timeout(0) (ends at construction, no timer).
//                         per_op = one create + destroy.
//
// Run pinned to CPUs 0-9: `taskset -c 0-9 context_bench`.

#include <benchmark/benchmark.h>

#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

#include "fibers/fiber/context.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace net = fibers::net;
using fibers::Context;
using fibers::sched::Scheduler;
using namespace std::chrono_literals;

namespace {

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

fibers::sched::SchedulerOptions options(std::size_t workers) {
    fibers::sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = 1;
    o.blocking.num_threads = 1;
    o.stacks.stack_size = 64 * 1024;
    o.stacks.max_stacks = 64;
    return o;
}

benchmark::Counter per_op(double ops) {
    return benchmark::Counter(ops, benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

void BM_SocketPingPong(benchmark::State& state) {
    constexpr int kRounds = 2000;
    const int mode = static_cast<int>(state.range(0));
    Scheduler scheduler(options(static_cast<std::size_t>(state.range(1))));
    int fd[2];
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fd) != 0) {
        std::abort();
    }
    net::Stream a;
    net::Stream b;
    if (a.adopt(scheduler, fd[0]) != 0 || b.adopt(scheduler, fd[1]) != 0) {
        std::abort();
    }
    Context plain = Context::cancellable();
    Context timed = Context::with_timeout(scheduler, 1h);
    Context* ctx = mode == 1 ? &plain : mode == 2 ? &timed : nullptr;
    Latch done;
    std::atomic<int> errors{0};
    auto player = [&](net::Stream& s, bool serve_first) {
        return [&, serve_first]() noexcept {
            char c = 'x';
            for (int i = 0; i < kRounds; ++i) {
                if (serve_first && !s.write_all(&c, 1)) {
                    errors.fetch_add(1);
                }
                if (!s.read_exact(&c, 1)) {
                    errors.fetch_add(1);
                }
                if (!serve_first && !s.write_all(&c, 1)) {
                    errors.fetch_add(1);
                }
            }
            done.count_down();
        };
    };
    for (auto _ : state) {
        done.reset(2);
        if (ctx != nullptr) {
            fiber::go(scheduler, *ctx, player(a, true));
            fiber::go(scheduler, *ctx, player(b, false));
        } else {
            fiber::go(scheduler, player(a, true));
            fiber::go(scheduler, player(b, false));
        }
        done.wait();
    }
    state.counters["per_op"] = per_op(static_cast<double>(state.iterations()) * kRounds);
    if (errors.load() != 0) {
        state.SkipWithError("I/O error");
    }
    static_cast<void>(a.close());
    static_cast<void>(b.close());
}
BENCHMARK(BM_SocketPingPong)
    ->Args({0, 1})->Args({1, 1})->Args({2, 1})
    ->Args({0, 2})->Args({1, 2})->Args({2, 2})
    ->UseRealTime();

void BM_ContextLifecycle(benchmark::State& state) {
    constexpr int kPerFiber = 1000;
    const int kind = static_cast<int>(state.range(0));
    Scheduler scheduler(options(1));
    Context parent = Context::cancellable();
    Latch done;
    for (auto _ : state) {
        done.reset(1);
        fiber::go(scheduler, [&]() noexcept {
            for (int i = 0; i < kPerFiber; ++i) {
                switch (kind) {
                    case 0: {
                        Context c = Context::cancellable();
                        benchmark::DoNotOptimize(&c);
                        break;
                    }
                    case 1: {
                        Context c = Context::child_of(parent);
                        benchmark::DoNotOptimize(&c);
                        break;
                    }
                    case 2: {
                        Context c = Context::with_timeout(1h);
                        benchmark::DoNotOptimize(&c);
                        break;
                    }
                    default: {
                        Context c = Context::with_timeout(0ns);
                        benchmark::DoNotOptimize(&c);
                        break;
                    }
                }
            }
            done.count_down();
        });
        done.wait();
    }
    state.counters["per_op"] = per_op(static_cast<double>(state.iterations()) * kPerFiber);
}
BENCHMARK(BM_ContextLifecycle)->Arg(0)->Arg(1)->Arg(2)->Arg(3)->UseRealTime();

}  // namespace
