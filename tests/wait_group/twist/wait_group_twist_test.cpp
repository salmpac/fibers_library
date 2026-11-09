// EventCore / WaitGroupCore (plain threads, plain WaitNodes) and the fiber
// Event / WaitGroup (MiniScheduler) under Twist.
//
// TWIST_SIM:    each scenario is explored with the Random and PCT schedulers
//               of the deterministic simulator.
// TWIST_FAULTY: each scenario runs several times on real threads with fault
//               injection (clang-twist-faulty-tsan adds TSan).
//
// What the protocols must hand over lives in twist::assist::Shared cells,
// whose accesses Twist checks for happens-before:
//   setter / done()s -> every returning waiter (payload cells);
//   waiter -> the waking thread (a cell written before the node is pushed,
//   read inside the node's wake: stands for the node's own fields).
// Each memory order of the cores is needed by at least one of these checks
// (see the weakening table in the commit that added these tests).

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>

#include <twist/assist/assert.hpp>
#include <twist/assist/shared.hpp>
#include <twist/cross.hpp>
#include <twist/ed/std/atomic.hpp>
#include <twist/ed/std/thread.hpp>

#if defined(__TWIST_SIM__)
#include <twist/sim.hpp>
#endif

#include "fibers/context/twist_context.hpp"
#include "fibers/fiber/event.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/wait_group.hpp"
#include "fibers/stack/stack.hpp"
#include "fibers/sync/twist_sync.hpp"
#include "fibers/thread_pool/thread_pool.hpp"
#include "fibers/wait/event_core.hpp"
#include "fibers/wait/wait_group_core.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fiber = fibers::fiber;
namespace tp = fibers::thread_pool;
using fibers::sync::TwistSync;
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

// Bounded waits: a lost wakeup must fail the run, not hang the simulator.
#if defined(__TWIST_SIM__)
#define FIBERS_WAIT_BOUNDED(condition, message)          \
    for (std::size_t step_ = 0; !(condition); ++step_) { \
        TWIST_ASSERT_M(step_ < 20'000, message);         \
        twist::ed::std::this_thread::yield();            \
    }
#else
#define FIBERS_WAIT_BOUNDED(condition, message)                                                  \
    for (const auto deadline_ = std::chrono::steady_clock::now() + std::chrono::seconds(30);    \
         !(condition);) {                                                                        \
        TWIST_ASSERT_M(std::chrono::steady_clock::now() < deadline_, message);                   \
        twist::ed::std::this_thread::yield();                                                    \
    }
#endif

// ------------------------------------------------------------ plain threads

// A thread-side waiter: `mine` is written before the node is pushed and read
// by the wake (on the waking thread); the wake then releases `woken`, which
// the waiting thread spins on.
struct ThreadWaiter {
    WaitNode node;
    twist::assist::Shared<int> mine{0};
    twist::ed::std::atomic<int> woken{0};

    ThreadWaiter() noexcept { node = WaitNode{nullptr, this, &ThreadWaiter::wake}; }

    static void wake(void* self) noexcept {
        auto* w = static_cast<ThreadWaiter*>(self);
        TWIST_ASSERT_M(w->mine.Read() == 1, "waiter's writes visible to the waker");
        TWIST_ASSERT_M(w->woken.fetch_add(1, std::memory_order_release) == 0, "woken twice");
    }

    // After wait_or_enqueue returned false.
    void await() {
        FIBERS_WAIT_BOUNDED(woken.load(std::memory_order_acquire) != 0, "waiter never woken (lost wakeup)");
    }
};

// set() races with two enqueues and an is_set() poll: whoever returns sees
// the setter's payload; every enqueued node is woken exactly once.
TEST(WaitTwist, EventCoreSetVsEnqueue) {
    check([] {
        fibers::wait::EventCore<TwistSync> event;
        twist::assist::Shared<int> payload{0};
        ThreadWaiter waiters[2];
        bool enqueued[2] = {false, false};
        twist::ed::std::thread setter([&] {
            payload.Write(1);
            TWIST_ASSERT_M(event.set(), "first set() sets");
            TWIST_ASSERT_M(!event.set(), "second set() is a no-op");
        });
        twist::ed::std::thread threads[2] = {
            twist::ed::std::thread([&] {
                waiters[0].mine.Write(1);
                if (!event.wait_or_enqueue(waiters[0].node)) {
                    enqueued[0] = true;
                    waiters[0].await();
                }
                TWIST_ASSERT_M(payload.Read() == 1, "setter's write visible after wait");
            }),
            twist::ed::std::thread([&] {
                waiters[1].mine.Write(1);
                if (event.is_set()) {
                    TWIST_ASSERT_M(payload.Read() == 1, "setter's write visible after is_set()");
                    return;
                }
                if (!event.wait_or_enqueue(waiters[1].node)) {
                    enqueued[1] = true;
                    waiters[1].await();
                }
                TWIST_ASSERT_M(payload.Read() == 1, "setter's write visible after wait");
            }),
        };
        setter.join();
        threads[0].join();
        threads[1].join();
        for (int i = 0; i < 2; ++i) {
            TWIST_ASSERT_M(waiters[i].woken.load() == (enqueued[i] ? 1 : 0), "woken exactly once iff enqueued");
        }
    });
}

// Two done()s race with two waiters (fetch_or vs the last fetch_sub, enqueue
// vs set); one worker adds a child unit during the join (before its own
// done()). Every waiter sees every done()'s write.
TEST(WaitTwist, WaitGroupCoreDoneVsWait) {
    check([] {
        fibers::wait::WaitGroupCore<TwistSync> wg;
        twist::assist::Shared<int> cells[3] = {0, 0, 0};
        ThreadWaiter waiters[2];
        wg.add(2);
        twist::ed::std::thread workers[2] = {
            twist::ed::std::thread([&] {
                cells[0].Write(1);
                wg.done();
            }),
            twist::ed::std::thread([&] {
                wg.add(1);  // a child, covered by our own unit
                cells[1].Write(1);
                wg.done();
                cells[2].Write(1);  // the child's work
                wg.done();
            }),
        };
        auto waiter = [&](int i) {
            return [&, i] {
                waiters[i].mine.Write(1);
                if (!wg.wait_or_enqueue(waiters[i].node)) {
                    waiters[i].await();
                }
                for (auto& cell : cells) {
                    TWIST_ASSERT_M(cell.Read() == 1, "every done()'s write visible after wait");
                }
            };
        };
        twist::ed::std::thread waiting[2] = {twist::ed::std::thread(waiter(0)), twist::ed::std::thread(waiter(1))};
        for (auto& t : workers) {
            t.join();
        }
        for (auto& t : waiting) {
            t.join();
        }
        TWIST_ASSERT_M(waiters[0].woken.load() <= 1 && waiters[1].woken.load() <= 1, "woken at most once");
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
// order (see tests/fiber/twist/fiber_twist_test.cpp: the simulator's
// determinism check digests atomic values, and fiber addresses travel
// through atomics).
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

using WaitGroup = fiber::BasicWaitGroup<MiniScheduler>;
using Event = fiber::BasicEvent<MiniScheduler>;

// A parent fiber fans out three children with wg.go and waits; the last
// done() races with the parent's fetch_or / enqueue / park (NOTIFIED or
// PARKED wake).
TEST(WaitTwist, FiberWaitGroupFanOutFanIn) {
    map_test_stacks();
    check([] {
        constexpr int kChildren = 3;
        Counter done{0};
        twist::assist::Shared<int> cells[kChildren] = {0, 0, 0};
        MiniScheduler scheduler(2);
        WaitGroup wg;
        fiber::go(scheduler, [&]() noexcept {
            for (int i = 0; i < kChildren; ++i) {
                wg.go([&cells, i]() noexcept { cells[i].Write(1); });
            }
            wg.wait();
            for (auto& cell : cells) {
                TWIST_ASSERT_M(cell.Read() == 1, "child's write visible after wait");
            }
            done.fetch_add(1);
        });
        FIBERS_WAIT_BOUNDED(done.load() == 1, "parent never woken (lost wakeup)");
        scheduler.pool().stop();
    });
}

// Two fibers wait on an Event that a plain thread sets.
TEST(WaitTwist, FiberEventSetFromThread) {
    map_test_stacks();
    check([] {
        Counter done{0};
        twist::assist::Shared<int> payload{0};
        MiniScheduler scheduler(2);
        Event event;
        for (int f = 0; f < 2; ++f) {
            fiber::go(scheduler, [&]() noexcept {
                event.wait();
                TWIST_ASSERT_M(payload.Read() == 1, "setter's write visible after wait");
                done.fetch_add(1);
            });
        }
        twist::ed::std::thread setter([&] {
            payload.Write(1);
            event.set();
        });
        setter.join();
        FIBERS_WAIT_BOUNDED(done.load() == 2, "waiter never woken (lost wakeup)");
        scheduler.pool().stop();
    });
}

}  // namespace
