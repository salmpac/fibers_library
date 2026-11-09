// OneshotCore / OneshotState / BasicPromise (plain threads, plain WaitNodes),
// fw::ThreadWaiter, and the fiber Oneshot / Future (MiniScheduler) under
// Twist.
//
// TWIST_SIM:    each scenario is explored with the Random and PCT schedulers
//               of the deterministic simulator.
// TWIST_FAULTY: each scenario runs several times on real threads with fault
//               injection (clang-twist-faulty-tsan adds TSan).
//
// What the protocol must hand over is checked for happens-before:
//   producer -> consumer: a payload cell (twist::assist::Shared) written
//     before set() / abandon() and read once the consumer has the result;
//   consumer -> producer: a cell written before the node is published, read
//     inside the node's wake (stands for the node's own fields);
//   the side that frees a pooled state -> the other side's last touch: the
//     state's HbProbe (simulator), written by emplace / destroy / release,
//     read by take and before detach; under TSan the real memory.
// Each memory order of the core and of ThreadWaiter is needed by at least
// one of these checks (see the weakening table in the commit that added
// these tests).

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <new>

#include <twist/assist/assert.hpp>
#include <twist/assist/shared.hpp>
#include <twist/cross.hpp>
#include <twist/ed/std/atomic.hpp>
#include <twist/ed/std/thread.hpp>

#if defined(__TWIST_SIM__)
#include <twist/sim.hpp>
#endif

#include "fibers/context/twist_context.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/future.hpp"
#include "fibers/fiber/oneshot.hpp"
#include "fibers/stack/stack.hpp"
#include "fibers/sync/twist_sync.hpp"
#include "fibers/thread_pool/thread_pool.hpp"
#include "fibers/wait/oneshot_core.hpp"
#include "fibers/wait/result.hpp"
#include "fibers/wait/thread_waiter.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fiber = fibers::fiber;
namespace tp = fibers::thread_pool;
namespace fw = fibers::wait;
using fibers::sync::TwistSync;
using fibers::wait::OneshotStatus;
using fibers::wait::WaitNode;

namespace {

using Counter = twist::ed::std::atomic<std::size_t>;

std::size_t env_or(const char* name, std::size_t fallback) {
    const char* value = std::getenv(name);
    return value != nullptr ? static_cast<std::size_t>(std::strtoull(value, nullptr, 10)) : fallback;
}

// Env: FIBERS_TWIST_RUNS (runs per scheduler / repetitions), FIBERS_TWIST_SEED.
void check(const std::function<void()>& body, std::size_t sim_runs = 2'000) {
#if defined(__TWIST_SIM__)
    const std::size_t runs = env_or("FIBERS_TWIST_RUNS", sim_runs);
    const std::uint64_t seed = env_or("FIBERS_TWIST_SEED", 42);
    {
        twist::sim::sched::RandomScheduler random{{.seed = seed, .max_runs = runs}};
        const auto explored = twist::sim::Explore(random, body);
        if (explored.found) {
            twist::sim::Print(body, explored.found->schedule);
            FAIL() << "Random scheduler, after " << explored.sims << " runs:\n"
                   << explored.found->result.std_err;
        }
    }
    {
        twist::sim::sched::PctScheduler pct{{.depth = 3, .seed = seed + 1, .max_runs = runs}};
        const auto explored = twist::sim::Explore(pct, body);
        if (explored.found) {
            twist::sim::Print(body, explored.found->schedule);
            FAIL() << "PCT scheduler, after " << explored.sims << " runs:\n"
                   << explored.found->result.std_err;
        }
    }
#else
    static_cast<void>(sim_runs);
    const std::size_t runs = env_or("FIBERS_TWIST_RUNS", 50);
    for (std::size_t i = 0; i < runs; ++i) {
        twist::cross::Run(body);
    }
#endif
}

// Bounded waits: a lost wakeup must fail the run, not hang. Steps under the
// simulator, wall time on real threads (a step bound alone flakes under
// fault injection).
#if defined(__TWIST_SIM__)
#define FIBERS_WAIT_BOUNDED(condition, message)          \
    for (std::size_t step_ = 0; !(condition); ++step_) { \
        TWIST_ASSERT_M(step_ < 20'000, message);         \
        twist::ed::std::this_thread::yield();            \
    }
#else
#define FIBERS_WAIT_BOUNDED(condition, message)                                               \
    for (const auto deadline_ = std::chrono::steady_clock::now() + std::chrono::seconds(30); \
         !(condition);) {                                                                     \
        TWIST_ASSERT_M(std::chrono::steady_clock::now() < deadline_, message);                \
        twist::ed::std::this_thread::yield();                                                 \
    }
#endif

using State = fw::OneshotState<int, TwistSync>;
using Promise = fw::BasicPromise<int, TwistSync>;

// A value whose destructor writes its memory (TSan sees a destroy that races
// with the construction).
struct Value {
    int v = 0;
    explicit Value(int x) noexcept : v(x) {}
    Value(Value&& other) noexcept : v(other.v) {}
    ~Value() { *static_cast<volatile int*>(&v) = -1; }
};
using ValueState = fw::OneshotState<Value, TwistSync>;
using ValuePromise = fw::BasicPromise<Value, TwistSync>;

// A spinning consumer node: `mine` is written before the node is published
// and read by the wake (on the producer's thread); the wake then releases
// `woken`, which the consumer spins on.
struct SpinWaiter {
    WaitNode node;
    twist::assist::Shared<int> mine{0};
    twist::ed::std::atomic<int> woken{0};

    SpinWaiter() noexcept { node = WaitNode{nullptr, this, &SpinWaiter::wake}; }

    static void wake(void* self) noexcept {
        auto* w = static_cast<SpinWaiter*>(self);
        TWIST_ASSERT_M(w->mine.Read() == 1, "consumer's writes visible to the producer");
        TWIST_ASSERT_M(w->woken.fetch_add(1, std::memory_order_release) == 0, "woken twice");
    }

    void await() {
        FIBERS_WAIT_BOUNDED(woken.load(std::memory_order_acquire) != 0, "consumer never woken (lost wakeup)");
    }
};

// Blocks for pooled states: raw memory mapped once outside the simulation
// (fixed addresses: the simulator digests atomic values), handed out by
// per-run atomic flags. A fiber::OneshotAllocator.
constexpr std::size_t kBlocks = 4;
constexpr std::size_t kBlockSize = 256;  // the simulator's probes are not small
alignas(16) std::byte g_blocks[kBlocks][kBlockSize];

class TestBlocks {
public:
    void* allocate(std::size_t bytes, std::size_t alignment) noexcept {
        if (bytes > kBlockSize || alignment > 16) {
            fibers::fail::fail_fast("test: TestBlocks: block too small");
        }
        for (std::size_t i = 0; i < kBlocks; ++i) {
            if (!used_[i].exchange(true, std::memory_order_acquire)) {
                live_.fetch_add(1, std::memory_order_relaxed);
                return g_blocks[i];
            }
        }
        fibers::fail::fail_fast("test: TestBlocks: out of blocks");
    }

    void deallocate(void* p, std::size_t, std::size_t) noexcept {
        const auto i = static_cast<std::size_t>(static_cast<std::byte(*)[kBlockSize]>(p) - g_blocks);
        TWIST_ASSERT_M(used_[i].load(std::memory_order_relaxed), "block freed twice");
        live_.fetch_sub(1, std::memory_order_relaxed);
        released_.fetch_add(1, std::memory_order_relaxed);
        used_[i].store(false, std::memory_order_release);
    }

    std::size_t live() const noexcept { return live_.load(); }
    std::size_t released() const noexcept { return released_.load(); }

    template <class S>
    S* make() noexcept {
        void* block = allocate(sizeof(S), alignof(S));
        return ::new (block) S(
            [](void* self, void* p) noexcept { static_cast<TestBlocks*>(self)->deallocate(p, sizeof(S), alignof(S)); },
            this);
    }

private:
    twist::ed::std::atomic<bool> used_[kBlocks] = {false, false, false, false};
    Counter live_{0};
    Counter released_{0};
};
static_assert(fiber::OneshotAllocator<TestBlocks>);

// ------------------------------------------------------------ plain threads

// set() / abandon() race with the consumer's status() fast path, its
// wait_or_enqueue (load, CAS) and the wake. The consumer uses the status
// that wait_or_enqueue returns without re-reading the word, as the
// front-ends do.
void set_vs_wait(bool broken) {
    check([broken] {
        State state;
        Promise promise(&state);
        twist::assist::Shared<int> payload{0};
        SpinWaiter waiter;
        twist::ed::std::thread producer([&] {
            payload.Write(1);
            if (broken) {
                promise.abandon();
            } else {
                promise.set(7);
            }
        });
        waiter.mine.Write(1);
        OneshotStatus s = state.status();
        if (s == OneshotStatus::kPending) {
            s = state.wait_or_enqueue(waiter.node);
            if (s == OneshotStatus::kPending) {
                waiter.await();
                s = state.status();
            }
        }
        TWIST_ASSERT_M(s == (broken ? OneshotStatus::kBroken : OneshotStatus::kReady), "final status");
        TWIST_ASSERT_M(payload.Read() == 1, "producer's write visible with the result");
        fw::Result<int> r = state.take(s);
        TWIST_ASSERT_M(r.has_value() == !broken && (broken || *r == 7), "result");
        producer.join();
        TWIST_ASSERT_M(waiter.woken.load() <= 1, "woken at most once");
    });
}

TEST(OneshotTwist, SetVsWait) { set_vs_wait(false); }

TEST(OneshotTwist, BrokenVsWait) { set_vs_wait(true); }

// fw::ThreadWaiter: the consumer trusts the wake alone (does not read the
// word again), so the value travels through the waiter's DONE handshake.
TEST(OneshotTwist, ThreadWaiterHandsOverTheWake) {
    check([] {
        State state;
        Promise promise(&state);
        twist::assist::Shared<int> payload{0};
        twist::ed::std::thread producer([&] {
            payload.Write(1);
            promise.set(5);
        });
        fw::ThreadWaiter<TwistSync> waiter;
        OneshotStatus s = state.wait_or_enqueue(waiter.node);
        if (s == OneshotStatus::kPending) {
            waiter.wait();
            s = OneshotStatus::kReady;  // the only completion in this test
        }
        TWIST_ASSERT_M(payload.Read() == 1, "producer's write visible after the wake");
        TWIST_ASSERT_M(*state.take(s) == 5, "value");
        producer.join();
    });
}

// Pooled: the consumer detaches while the producer completes. Exactly one
// side frees the block; the value is destroyed once, after its construction.
void detach_vs_complete(bool broken) {
    check([broken] {
        TestBlocks blocks;
        ValueState* state = blocks.make<ValueState>();
        ValuePromise promise(state);
        twist::ed::std::thread producer([&] {
            if (broken) {
                promise.abandon();
            } else {
                promise.set(1);
            }
        });
        const OneshotStatus s = state->detach();
        if (s != OneshotStatus::kPending) {
            if (s == OneshotStatus::kReady) {
                state->destroy_value();
            }
            state->release();
        }
        producer.join();
        TWIST_ASSERT_M(blocks.released() == 1 && blocks.live() == 0, "freed exactly once");
    });
}

TEST(OneshotTwist, PooledDetachVsSet) { detach_vs_complete(false); }

TEST(OneshotTwist, PooledDetachVsAbandon) { detach_vs_complete(true); }

// Pooled: the consumer waits (ThreadWaiter), takes the value and frees the
// block while the producer may still be just past its exchange.
TEST(OneshotTwist, PooledGetVsSet) {
    check([] {
        TestBlocks blocks;
        ValueState* state = blocks.make<ValueState>();
        ValuePromise promise(state);
        twist::assist::Shared<int> payload{0};
        twist::ed::std::thread producer([&] {
            payload.Write(1);
            promise.set(9);
        });
        fw::ThreadWaiter<TwistSync> waiter;
        OneshotStatus s = state->wait_or_enqueue(waiter.node);
        if (s == OneshotStatus::kPending) {
            waiter.wait();
            s = state->status();
        }
        TWIST_ASSERT_M(payload.Read() == 1, "producer's write visible with the result");
        fw::Result<Value> r = state->take(s);
        TWIST_ASSERT_M(r && r->v == 9, "value");
        state->release();
        producer.join();
        TWIST_ASSERT_M(blocks.released() == 1, "freed once");
    });
}

// ------------------------------------------------------------------ fibers

constexpr std::size_t kStackSize = 256 * 1024;

tp::ThreadPoolOptions tiny(std::size_t workers) {
    return tp::ThreadPoolOptions{
        .num_workers = workers,
        .main_box_capacity = 2,
        .mail_box_shards = 1,
        .mail_box_shard_capacity = 2,
        .batch_size = 2,
        .mail_box_check_interval = 2,
        .max_steal_backoff = 4,
        .idle_spins_before_park = 8,
    };
}

// Fiber stacks mapped once, OUTSIDE the simulation, handed out in a fixed
// order (see tests/fiber/twist/fiber_twist_test.cpp).
constexpr std::size_t kTestStacks = 8;
fibers::stack::Stack g_stacks[kTestStacks];

void map_test_stacks() {
    for (auto& stack : g_stacks) {
        if (stack.empty()) {
            stack = fibers::stack::Stack::allocate(kStackSize);
        }
    }
}

class TestStackPool {
public:
    fibers::stack::Stack* acquire() noexcept {
        for (std::size_t i = 0; i < kTestStacks; ++i) {
            if (!used_[i].exchange(true, std::memory_order_acquire)) {
                return &g_stacks[i];
            }
        }
        fibers::fail::fail_fast("test: out of fiber stacks");
    }

    void release(fibers::stack::Stack* stack) noexcept {
        used_[static_cast<std::size_t>(stack - g_stacks)].store(false, std::memory_order_release);
    }

private:
    twist::ed::std::atomic<bool> used_[kTestStacks] = {false, false, false, false, false, false, false, false};
};

// What a fiber needs from a scheduler, and nothing else.
class MiniScheduler {
public:
    using SyncPolicy = TwistSync;

    explicit MiniScheduler(std::size_t workers) : pool_(tiny(workers)) {}

    tp::ThreadPool<TwistSync>& pool() noexcept { return pool_; }
    TestStackPool& stacks() noexcept { return stacks_; }

private:
    TestStackPool stacks_;  // outlives the pool's queues
    tp::ThreadPool<TwistSync> pool_;
};

static_assert(fiber::FiberScheduler<MiniScheduler>);

using Oneshot = fiber::BasicOneshot<int, MiniScheduler>;
using Future = fiber::BasicFuture<int, MiniScheduler>;

// A fiber waits on a Oneshot on its own stack; a plain thread sets it (the
// wake races with the fiber's switch-out: NOTIFIED or PARKED).
TEST(OneshotTwist, FiberOneshotSetFromThread) {
    map_test_stacks();
    check([] {
        Counter done{0};
        Counter handed{0};
        twist::assist::Shared<int> payload{0};
        Promise promise;  // filled by the fiber, then handed to the thread
        MiniScheduler scheduler(2);
        fiber::go(scheduler, [&]() noexcept {
            Oneshot reply;
            promise = reply.promise();
            handed.store(1, std::memory_order_release);
            fw::Result<int> r = reply.get();
            TWIST_ASSERT_M(r && *r == 3, "value");
            TWIST_ASSERT_M(payload.Read() == 1, "producer's write visible after get");
            done.fetch_add(1);
        });
        twist::ed::std::thread producer([&] {
            FIBERS_WAIT_BOUNDED(handed.load(std::memory_order_acquire) == 1, "promise never handed over");
            payload.Write(1);
            promise.set(3);
        });
        producer.join();
        FIBERS_WAIT_BOUNDED(done.load() == 1, "consumer never woken (lost wakeup)");
        scheduler.pool().stop();
    });
}

// Pooled pair between two fibers; in half of the runs the consumer detaches
// instead of waiting (the free races with the producer's set).
void fiber_future(bool detach) {
    map_test_stacks();
    check([detach] {
        Counter done{0};
        twist::assist::Shared<int> payload{0};
        TestBlocks blocks;
        MiniScheduler scheduler(2);
        fiber::go(scheduler, [&]() noexcept {
            auto [promise, future] = fiber::make_promise_future<int, MiniScheduler>(blocks);
            fiber::go(scheduler, [&payload, &done, p = std::move(promise)]() mutable noexcept {
                payload.Write(1);
                p.set(4);
                done.fetch_add(1);
            });
            if (detach) {
                future.detach();
            } else {
                fw::Result<int> r = future.get();
                TWIST_ASSERT_M(r && *r == 4, "value");
                TWIST_ASSERT_M(payload.Read() == 1, "producer's write visible after get");
            }
            done.fetch_add(1);
        });
        FIBERS_WAIT_BOUNDED(done.load() == 2, "a fiber never finished (lost wakeup)");
        scheduler.pool().stop();
        TWIST_ASSERT_M(blocks.released() == 1 && blocks.live() == 0, "freed exactly once");
    });
}

TEST(OneshotTwist, FiberFutureGet) { fiber_future(false); }

TEST(OneshotTwist, FiberFutureDetach) { fiber_future(true); }

// A plain thread blocks in Future::get() (ThreadWaiter) while a fiber sets.
TEST(OneshotTwist, ThreadGetsFutureFromFiber) {
    map_test_stacks();
    check([] {
        twist::assist::Shared<int> payload{0};
        TestBlocks blocks;
        MiniScheduler scheduler(1);
        auto [promise, future] = fiber::make_promise_future<int, MiniScheduler>(blocks);
        fiber::go(scheduler, [&payload, p = std::move(promise)]() mutable noexcept {
            payload.Write(1);
            p.set(6);
        });
        fw::Result<int> r = future.get();  // not a fiber: blocks this thread
        TWIST_ASSERT_M(r && *r == 6, "value");
        TWIST_ASSERT_M(payload.Read() == 1, "producer's write visible after get");
        scheduler.pool().stop();
        TWIST_ASSERT_M(blocks.released() == 1, "freed once");
    });
}

}  // namespace
