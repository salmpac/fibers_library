// ThreadPool benchmarks.
//
// 1. Chains ("fiber-like"): C chains, each owning one reusable memo with a
//    countdown. A task is a plain function: it decrements the counter and,
//    until it reaches zero, re-submits itself with the same memo from inside
//    the worker. C * S tasks in total (default 1000 * 10'000 = 10M). No
//    allocations on the hot path; the overflow_box (SlowQueue) must stay unused.
//
// 2. BlockedSpammer: one task occupies a worker and submits 10M tasks in a
//    loop into its own main_box. That worker never runs them: the rest of the
//    pool has to drain them via work stealing.
//
// 3. ExternalSpammer: a non-worker thread submits 10M tasks, round-robin into
//    the workers' mail_boxes.
//
// Spammers submit one by one (batch = 1, ThreadPool::submit) or in batches
// (ThreadPool::submit_batch); backoff = ThreadPoolOptions::max_steal_backoff
// (0 = idle thieves never pause).
//
// In 2 and 3 the queues may overflow by design; overflow_locks reports how
// often the SlowQueue mutex was taken (per iteration).
//
// Each pool benchmark runs for both main_box implementations (LockedPool =
// LockedSPSCQueue, LockFreePool = LockFreeSPMCQueue). Narrow the argument
// sets with --benchmark_filter, e.g. 'workers:(4|16)/batch:(1|256)/backoff:256'.

#include <benchmark/benchmark.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <thread>
#include <vector>

#include "thread_pool/lock_free_spmc_queue.hpp"
#include "thread_pool/thread_pool.hpp"

namespace tp = thread_pool;

namespace {

// Counts every Sync::Mutex acquisition. The only mutex in the pool guards the
// overflow_box, so a non-zero count means the SlowQueue was used.
std::atomic<std::size_t> g_overflow_locks{0};

struct BenchSync : tp::StdSync {
    struct Mutex {
        void lock() {
            g_overflow_locks.fetch_add(1, std::memory_order_relaxed);
            m.lock();
        }
        bool try_lock() {
            g_overflow_locks.fetch_add(1, std::memory_order_relaxed);
            return m.try_lock();
        }
        void unlock() { m.unlock(); }
        std::mutex m;
    };
};

// Every pool benchmark runs once per main_box implementation.
using LockedPool = tp::ThreadPool<BenchSync, tp::LockedSPSCQueue<tp::Task, BenchSync>>;
using LockFreePool = tp::ThreadPool<BenchSync, tp::LockFreeSPMCQueue<tp::Task, BenchSync>>;

template <class Pool>
struct alignas(tp::kCacheLineSize) Chain {
    Pool* pool;
    std::atomic<std::size_t>* finished;
    std::size_t remaining;
    std::size_t total_chains;
};

// Only one task per chain is alive at a time, so `remaining` needs no atomics:
// the queue hand-off orders consecutive steps.
template <class Pool>
void chain_step(void* memo) noexcept {
    auto* chain = static_cast<Chain<Pool>*>(memo);
    if (--chain->remaining != 0) {
        chain->pool->submit(tp::Task(chain, &chain_step<Pool>, nullptr));
        return;
    }
    if (chain->finished->fetch_add(1, std::memory_order_acq_rel) + 1 == chain->total_chains) {
        chain->finished->notify_all();
    }
}

template <class Pool>
void BM_ThreadPool_Chains(benchmark::State& state) {
    const auto workers = static_cast<std::size_t>(state.range(0));
    const auto chains = static_cast<std::size_t>(state.range(1));
    const auto steps = static_cast<std::size_t>(state.range(2));

    Pool pool(tp::ThreadPoolOptions{.num_workers = workers});
    std::vector<Chain<Pool>> memos(chains);
    std::atomic<std::size_t> finished{0};
    const std::size_t overflow_before = g_overflow_locks.load();

    for (auto _ : state) {
        finished.store(0);
        for (Chain<Pool>& c : memos) {
            c = Chain<Pool>{&pool, &finished, steps, chains};
        }
        for (Chain<Pool>& c : memos) {
            pool.submit(tp::Task(&c, &chain_step<Pool>, nullptr));
        }
        for (std::size_t v; (v = finished.load(std::memory_order_acquire)) != chains;) {
            finished.wait(v);
        }
    }

    const std::size_t overflow = g_overflow_locks.load() - overflow_before;
    if (overflow != 0) {
        state.SkipWithError("overflow_box (SlowQueue) was used");
    }
    const double tasks = static_cast<double>(chains * steps);
    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations() * chains * steps));
    state.counters["tasks"] = tasks;
    state.counters["time/task"] = benchmark::Counter(
        tasks, benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
    state.counters["overflow_locks"] = static_cast<double>(overflow);
}

BENCHMARK_TEMPLATE(BM_ThreadPool_Chains, LockedPool)
    ->ArgNames({"workers", "chains", "steps"})
    ->ArgsProduct({{1, 2, 4, 8, 16}, {1000}, {10'000}})
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_ThreadPool_Chains, LockFreePool)
    ->ArgNames({"workers", "chains", "steps"})
    ->ArgsProduct({{1, 2, 4, 8, 16}, {1000}, {10'000}})
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

// Same chain logic, no pool: the floor for "call a function pointer, bump a
// counter" on one thread.
void BM_Baseline_DirectCalls(benchmark::State& state) {
    const auto chains = static_cast<std::size_t>(state.range(0));
    const auto steps = static_cast<std::size_t>(state.range(1));
    std::vector<std::size_t> remaining(chains);
    void (*volatile step)(std::size_t*) = [](std::size_t* r) { --*r; };

    for (auto _ : state) {
        for (auto& r : remaining) r = steps;
        for (std::size_t c = 0; c < chains; ++c) {
            while (remaining[c] != 0) step(&remaining[c]);
        }
        benchmark::DoNotOptimize(remaining.data());
    }
    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations() * chains * steps));
}

BENCHMARK(BM_Baseline_DirectCalls)
    ->ArgNames({"chains", "steps"})
    ->Args({1000, 10'000})
    ->Unit(benchmark::kMillisecond);

// ------------------------------------------------------------- spammers

constexpr std::size_t kSpamTasks = 10'000'000;

// Contention-free completion counter: each thread bumps its own cache line,
// the waiter sums all lines.
class ShardedCounter {
public:
    ShardedCounter() : id_(next_id_.fetch_add(1) + 1) {}

    void inc() noexcept {
        Slot& slot = local_slot();
        slot.value.store(slot.value.load(std::memory_order_relaxed) + 1,
                         std::memory_order_relaxed);
    }

    std::size_t sum() const noexcept {
        std::size_t total = 0;
        for (const Slot& slot : slots_) total += slot.value.load(std::memory_order_relaxed);
        return total;
    }

    void reset() noexcept {
        for (Slot& slot : slots_) slot.value.store(0, std::memory_order_relaxed);
    }

private:
    struct alignas(tp::kCacheLineSize) Slot {
        std::atomic<std::size_t> value{0};
    };
    static constexpr std::size_t kSlots = 64;

    Slot& local_slot() noexcept {
        struct Cache {
            std::uint64_t owner = 0;
            Slot* slot = nullptr;
        };
        thread_local Cache cache;
        if (cache.owner != id_) [[unlikely]] {
            const std::size_t index = next_slot_.fetch_add(1, std::memory_order_relaxed);
            if (index >= kSlots) std::abort();
            cache = Cache{id_, &slots_[index]};
        }
        return *cache.slot;
    }

    inline static std::atomic<std::uint64_t> next_id_{0};
    const std::uint64_t id_;
    std::atomic<std::size_t> next_slot_{0};
    std::array<Slot, kSlots> slots_{};
};

void wait_for_sum(const ShardedCounter& counter, std::size_t target) {
    while (counter.sum() < target) {
        std::this_thread::sleep_for(std::chrono::microseconds(20));
    }
}

void unit_task(void* memo) noexcept { static_cast<ShardedCounter*>(memo)->inc(); }

// Submits `tasks` unit tasks one by one or in batches of `batch`.
template <class Pool>
void spam(Pool& pool, ShardedCounter& done, std::size_t tasks, std::size_t batch) {
    if (batch <= 1) {
        for (std::size_t i = 0; i < tasks; ++i) {
            pool.submit(tp::Task(&done, &unit_task, nullptr));
        }
        return;
    }
    std::vector<tp::Task> buffer(batch);
    for (std::size_t i = 0; i < tasks; i += batch) {
        const std::size_t n = std::min(batch, tasks - i);
        for (std::size_t j = 0; j < n; ++j) {
            buffer[j] = tp::Task(&done, &unit_task, nullptr);
        }
        pool.submit_batch(buffer.data(), n);
    }
}

template <class Pool>
struct SpamContext {
    Pool* pool;
    ShardedCounter* done;
    std::size_t tasks;
    std::size_t batch;
    std::atomic<std::int64_t> spam_ns{0};
    std::atomic<bool> finished{false};
};

template <class Pool>
void spammer_task(void* memo) noexcept {
    auto* ctx = static_cast<SpamContext<Pool>*>(memo);
    const auto start = std::chrono::steady_clock::now();
    spam(*ctx->pool, *ctx->done, ctx->tasks, ctx->batch);
    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - start).count();
    ctx->spam_ns.fetch_add(ns, std::memory_order_relaxed);
    ctx->finished.store(true, std::memory_order_release);
}

void report_spam(benchmark::State& state, std::size_t overflow, double submit_ns) {
    const double iterations = static_cast<double>(state.iterations());
    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations() * kSpamTasks));
    state.counters["time/task"] = benchmark::Counter(
        static_cast<double>(kSpamTasks),
        benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
    state.counters["submit_ms"] = submit_ns / iterations / 1e6;
    state.counters["overflow_locks"] = static_cast<double>(overflow) / iterations;
}

template <class Pool>
void BM_ThreadPool_BlockedSpammer(benchmark::State& state) {
    const auto workers = static_cast<std::size_t>(state.range(0));
    const auto batch = static_cast<std::size_t>(state.range(1));
    const auto backoff = static_cast<std::size_t>(state.range(2));

    Pool pool(tp::ThreadPoolOptions{.num_workers = workers,
                                    .main_box_capacity = 4096,
                                    .max_steal_backoff = backoff});
    ShardedCounter done;
    SpamContext<Pool> ctx{&pool, &done, kSpamTasks, batch};
    const std::size_t overflow_before = g_overflow_locks.load();

    for (auto _ : state) {
        done.reset();
        ctx.finished.store(false);
        pool.submit(tp::Task(&ctx, &spammer_task<Pool>, nullptr));
        wait_for_sum(done, kSpamTasks);
        while (!ctx.finished.load(std::memory_order_acquire)) std::this_thread::yield();
    }

    report_spam(state, g_overflow_locks.load() - overflow_before,
                static_cast<double>(ctx.spam_ns.load()));
}

BENCHMARK_TEMPLATE(BM_ThreadPool_BlockedSpammer, LockedPool)
    ->ArgNames({"workers", "batch", "backoff"})
    ->ArgsProduct({{2, 4, 8, 16}, {1, 32, 256}, {0, 256}})
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_ThreadPool_BlockedSpammer, LockFreePool)
    ->ArgNames({"workers", "batch", "backoff"})
    ->ArgsProduct({{2, 4, 8, 16}, {1, 32, 256}, {0, 256}})
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

template <class Pool>
void BM_ThreadPool_ExternalSpammer(benchmark::State& state) {
    const auto workers = static_cast<std::size_t>(state.range(0));
    const auto batch = static_cast<std::size_t>(state.range(1));
    const auto backoff = static_cast<std::size_t>(state.range(2));

    Pool pool(tp::ThreadPoolOptions{.num_workers = workers, .max_steal_backoff = backoff});
    ShardedCounter done;
    const std::size_t overflow_before = g_overflow_locks.load();
    double submit_ns = 0;

    for (auto _ : state) {
        done.reset();
        const auto start = std::chrono::steady_clock::now();
        spam(pool, done, kSpamTasks, batch);
        submit_ns += static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - start).count());
        wait_for_sum(done, kSpamTasks);
    }

    report_spam(state, g_overflow_locks.load() - overflow_before, submit_ns);
}

BENCHMARK_TEMPLATE(BM_ThreadPool_ExternalSpammer, LockedPool)
    ->ArgNames({"workers", "batch", "backoff"})
    ->ArgsProduct({{1, 2, 4, 8, 16}, {1, 32, 256}, {0, 256}})
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_ThreadPool_ExternalSpammer, LockFreePool)
    ->ArgNames({"workers", "batch", "backoff"})
    ->ArgsProduct({{1, 2, 4, 8, 16}, {1, 32, 256}, {0, 256}})
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

}  // namespace
