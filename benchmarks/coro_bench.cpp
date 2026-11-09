// Context switch, coroutine and stack pool benchmarks.
//
//   MachineContextRoundTrip   raw switch_to there and back (two switches)
//   CoroutineRoundTrip        resume() + ctx.suspend() (two switches plus the
//                             coroutine bookkeeping)
//   CoroutineAwaiterRoundTrip resume() + ctx.suspend(awaiter) + the resumer
//                             calling awaiter.await_suspend()
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
#include <thread>

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
