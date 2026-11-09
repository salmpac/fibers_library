// Context switch, coroutine and stack pool benchmarks.
//
//   MachineContextRoundTrip   raw switch_to there and back (two switches)
//   CoroutineRoundTrip        resume() + ctx.suspend() (two switches plus the
//                             coroutine bookkeeping)
//   CoroutineAwaiterRoundTrip resume() + ctx.suspend(awaiter) + the resumer
//                             calling awaiter.await_suspend()
//   CoroutineRandomOrder/N/S  N coroutines resumed in a pseudo-random order
//                             (one resume() = round trip), each suspending
//                             at one of S distinct call sites chosen by its
//                             own pseudo-random sequence. RoundTrip above is
//                             a best case: one coroutine, one suspend site,
//                             so the switch's indirect jump always goes to
//                             the same place and its stack stays in L1.
//                             S = 1 isolates the cost of many stacks;
//                             S = 4 / 8 adds mispredicted jump targets.
//   MakeCoroutineRun          make_coroutine on a reused stack + run to
//                             completion (setup, first switch, exit)
//   MakeCoroutinePooled       the same with the stack acquired from and
//                             released to a StackPool every time
//   StackPoolLocal            acquire + release on one thread
//   StackPoolCrossThread      acquired on the benchmark thread, released on
//                             a consumer thread (an SPSC queue between them)
//
// Run pinned, e.g. `taskset -c 0-9 coro_bench`: the counters cpu / cpu_peer
// report sched_getcpu() of the threads involved (sampled at the end).

#include <benchmark/benchmark.h>

#include <sched.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include "fibers/context/context.hpp"
#include "fibers/coro/coroutine.hpp"
#include "fibers/queue/spsc_queue.hpp"
#include "fibers/stack/stack.hpp"
#include "fibers/stack/stack_pool.hpp"

namespace context = fibers::context;
namespace coro = fibers::coro;
namespace stack = fibers::stack;

namespace {

// sched_getcpu() re-read after every switch (see coro/coroutine.hpp on why a
// fiber must not let the compiler cache such things).
[[gnu::noinline]] int current_cpu() {
    asm volatile("" ::: "memory");
    return sched_getcpu();
}

void BM_MachineContextRoundTrip(benchmark::State& state) {
    struct Ping {
        context::MachineContext self;
        context::MachineContext* back = nullptr;
        bool stop = false;
        static void run(void* arg) noexcept {
            auto& p = *static_cast<Ping*>(arg);
            while (!p.stop) {
                p.self.switch_to(*p.back);
            }
            p.self.exit_to(*p.back);
        }
    };
    stack::Stack s = stack::Stack::allocate();
    context::MachineContext main;
    Ping ping;
    ping.back = &main;
    ping.self.setup(s.view(), context::Trampoline{&ping, &Ping::run});
    for (auto _ : state) {
        main.switch_to(ping.self);
    }
    ping.stop = true;
    main.switch_to(ping.self);
    state.counters["cpu"] = current_cpu();
}
BENCHMARK(BM_MachineContextRoundTrip);

void BM_CoroutineRoundTrip(benchmark::State& state) {
    stack::Stack s = stack::Stack::allocate();
    bool stop = false;
    auto body = [&stop](coro::SuspendContext ctx) noexcept {
        while (!stop) {
            ctx.suspend();
        }
    };
    coro::Coroutine co(s.view(), body);
    for (auto _ : state) {
        benchmark::DoNotOptimize(co.resume());
    }
    stop = true;
    co.resume();
    state.counters["cpu"] = current_cpu();
}
BENCHMARK(BM_CoroutineRoundTrip);

void BM_CoroutineAwaiterRoundTrip(benchmark::State& state) {
    struct Count {
        std::size_t* n;
        void await_suspend() noexcept { ++*n; }
    };
    stack::Stack s = stack::Stack::allocate();
    bool stop = false;
    std::size_t calls = 0;
    auto body = [&](coro::SuspendContext ctx) noexcept {
        Count awaiter{&calls};
        while (!stop) {
            ctx.suspend(awaiter);
        }
    };
    coro::Coroutine co(s.view(), body);
    for (auto _ : state) {
        const coro::ResumeResult r = co.resume();
        r.awaiter.await_suspend();
    }
    stop = true;
    co.resume();
    benchmark::DoNotOptimize(calls);
    state.counters["cpu"] = current_cpu();
}
BENCHMARK(BM_CoroutineAwaiterRoundTrip);

// Distinct suspend sites. Every case is "asm marker; suspend; different
// work": the marker (an asm volatile with its own text) keeps the compiler
// from hoisting the common suspend above the switch, and the different work
// after it keeps it from sinking the suspends into one call. So each site has
// its own call into the switch, i.e. its own return address (checked with
// objdump: one switch_stacks call per site).
#define FIBERS_SITE(n, work)                       \
    case n:                                        \
        asm volatile("nop # suspend site " #n ::: "memory"); \
        ctx.suspend();                             \
        work;                                      \
        break

struct RandomSiteBody {
    std::uint64_t rng;
    std::uint64_t sites;
    std::uint64_t sink = 0;
    bool stop = false;

    void operator()(coro::SuspendContext ctx) noexcept {
        while (!stop) {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            switch (rng % sites) {
                FIBERS_SITE(0, sink += 1);
                FIBERS_SITE(1, sink ^= 0x55);
                FIBERS_SITE(2, sink *= 3);
                FIBERS_SITE(3, sink -= 7);
                FIBERS_SITE(4, sink += rng);
                FIBERS_SITE(5, sink ^= rng >> 3);
                FIBERS_SITE(6, sink = (sink << 1) | 1);
                FIBERS_SITE(7, sink = ~sink);
                default:
                    break;
            }
            benchmark::DoNotOptimize(sink);
        }
    }
};
#undef FIBERS_SITE

void BM_CoroutineRandomOrder(benchmark::State& state) {
    const auto count = static_cast<std::size_t>(state.range(0));
    const auto sites = static_cast<std::uint64_t>(state.range(1));
    std::vector<stack::Stack> stacks;
    std::vector<RandomSiteBody> bodies(count);
    std::vector<std::unique_ptr<coro::Coroutine>> coroutines;
    stacks.reserve(count);
    coroutines.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        stacks.push_back(stack::Stack::allocate(16 * 1024));
        bodies[i].rng = 0x9E3779B97F4A7C15ull * (i + 1);
        bodies[i].sites = sites;
        coroutines.push_back(std::make_unique<coro::Coroutine>(stacks[i].view(), bodies[i]));
        coroutines[i]->resume();  // to its first suspend
    }
    // Resume order: a precomputed pseudo-random sequence (no RNG in the loop).
    constexpr std::size_t kOrder = 1 << 16;
    std::vector<std::uint32_t> order(kOrder);
    std::uint64_t x = 0x2545F4914F6CDD1Dull;
    for (auto& o : order) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        o = static_cast<std::uint32_t>(x % count);
    }
    std::size_t next = 0;
    for (auto _ : state) {
        coroutines[order[next]]->resume();
        next = (next + 1) & (kOrder - 1);
    }
    for (std::size_t i = 0; i < count; ++i) {
        bodies[i].stop = true;
        coroutines[i]->resume();
    }
    state.counters["cpu"] = current_cpu();
}
BENCHMARK(BM_CoroutineRandomOrder)
    ->Args({1, 1})->Args({1, 4})
    ->Args({256, 1})->Args({256, 4})->Args({256, 8})
    ->Args({4096, 1})->Args({4096, 8});

void BM_MakeCoroutineRun(benchmark::State& state) {
    stack::Stack s = stack::Stack::allocate();
    std::size_t runs = 0;
    for (auto _ : state) {
        auto co = coro::make_coroutine(s.view(), [&runs](coro::SuspendContext) noexcept { ++runs; });
        co.resume();
    }
    benchmark::DoNotOptimize(runs);
    state.counters["cpu"] = current_cpu();
}
BENCHMARK(BM_MakeCoroutineRun);

void BM_MakeCoroutinePooled(benchmark::State& state) {
    stack::StackPool<> pool({.preallocated = 16, .max_stacks = 64});
    std::size_t runs = 0;
    for (auto _ : state) {
        stack::Stack* s = pool.acquire();
        {
            auto co = coro::make_coroutine(s->view(), [&runs](coro::SuspendContext) noexcept { ++runs; });
            co.resume();
        }
        pool.release(s);
    }
    benchmark::DoNotOptimize(runs);
    state.counters["cpu"] = current_cpu();
}
BENCHMARK(BM_MakeCoroutinePooled);

void BM_StackPoolLocal(benchmark::State& state) {
    stack::StackPool<> pool({.preallocated = 64, .max_stacks = 256});
    for (auto _ : state) {
        stack::Stack* s = pool.acquire();
        benchmark::DoNotOptimize(s);
        pool.release(s);
    }
    state.counters["cpu"] = current_cpu();
}
BENCHMARK(BM_StackPoolLocal);

// Producer = the benchmark thread (acquire), consumer = a helper thread
// (release). Items/s counts stacks passed through.
void BM_StackPoolCrossThread(benchmark::State& state) {
    stack::StackPool<> pool({.stack_size = 16 * 1024, .preallocated = 512, .max_stacks = 1024});
    fibers::queue::SPSCQueue<stack::Stack*> queue(256);
    std::atomic<bool> done{false};
    std::atomic<int> consumer_cpu{-1};
    std::thread consumer([&] {
        stack::Stack* s = nullptr;
        for (;;) {
            if (queue.try_pop(s)) {
                pool.release(s);
            } else if (done.load(std::memory_order_acquire)) {
                while (queue.try_pop(s)) {
                    pool.release(s);
                }
                break;
            } else {
                fibers::sync::cpu_relax();
            }
        }
        consumer_cpu.store(current_cpu());
        pool.flush_thread_cache();
    });
    std::size_t passed = 0;
    for (auto _ : state) {
        stack::Stack* s = pool.acquire();
        while (!queue.try_push(std::move(s))) {
            fibers::sync::cpu_relax();
        }
        ++passed;
    }
    done.store(true, std::memory_order_release);
    consumer.join();
    state.SetItemsProcessed(static_cast<std::int64_t>(passed));
    state.counters["cpu"] = current_cpu();
    state.counters["cpu_peer"] = consumer_cpu.load();
    state.counters["mapped"] = static_cast<double>(pool.mapped_stacks());
}
BENCHMARK(BM_StackPoolCrossThread)->UseRealTime();

}  // namespace
