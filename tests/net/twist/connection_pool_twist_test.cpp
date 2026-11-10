// net::BasicConnectionPool under Twist: the waiter protocol (a return or
// close() resolving a timed waiter vs its deadline timer firing on the
// poller thread, the retire / withdraw step), with FakeBackend pollers and
// a fake connection (no I/O).
//
// TWIST_SIM:    Random and PCT schedulers of the deterministic simulator
//               (x86-TSO store buffering in the clang-twist-sim preset).
// TWIST_FAULTY: real threads with fault injection (+ TSan in
//               clang-twist-faulty-tsan).
//
// A lost wake hangs a fiber (caught by the bounded waits); a timer left in
// the poller after its waiter returned is a use-after-return of the stack
// node (the simulator / TSan / the poller's own checks). The connection
// carries a Shared cell written by the holder before the return and read by
// the next holder: the hand-off must order them.

#include <gtest/gtest.h>

#include <cerrno>
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
#include "fibers/fiber/fiber.hpp"
#include "fibers/net/connection_pool.hpp"
#include "fibers/poller/fake_backend.hpp"
#include "fibers/poller/poller_pool.hpp"
#include "fibers/stack/stack_pool.hpp"
#include "fibers/sync/twist_sync.hpp"
#include "fibers/thread_pool/thread_pool.hpp"

namespace fiber = fibers::fiber;
namespace net = fibers::net;
namespace tp = fibers::thread_pool;
using fibers::poller::TimePoint;
using fibers::sync::TwistSync;

namespace {

using Counter = twist::ed::std::atomic<std::size_t>;

std::size_t env_or(const char* name, std::size_t fallback) {
    const char* value = std::getenv(name);
    return value != nullptr ? static_cast<std::size_t>(std::strtoull(value, nullptr, 10)) : fallback;
}

// Env: FIBERS_TWIST_RUNS (runs per scheduler / repetitions), FIBERS_TWIST_SEED.
void check(const std::function<void()>& body, std::size_t sim_runs = 500) {
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
        TWIST_ASSERT_M(step_ < 50'000, message);         \
        twist::ed::std::this_thread::yield();            \
    }
#else
#define FIBERS_WAIT_BOUNDED(condition, message)                                                  \
    for (const auto deadline_ = std::chrono::steady_clock::now() + std::chrono::seconds(60);    \
         !(condition);) {                                                                        \
        TWIST_ASSERT_M(std::chrono::steady_clock::now() < deadline_, message);                   \
        twist::ed::std::this_thread::yield();                                                    \
    }
#endif

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

// Fiber stacks mapped once, outside the simulation, handed out in a fixed
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

// What the pool needs: fibers (pool, stacks), a PollerPool of FakeBackend
// pollers (timers), now().
class PoolScheduler {
public:
    using SyncPolicy = TwistSync;
    using PollerPool = fibers::poller::PollerPool<fibers::poller::FakeBackend<TwistSync>, TwistSync>;
    using Poller = PollerPool::PollerType;
    using Timer = PollerPool::Timer;

    explicit PoolScheduler(std::size_t workers)
        : pool_(tiny(workers)), pollers_(fibers::poller::TaskSink::of(pool_), {.num_pollers = 1}) {
        pollers_.start();
    }

    ~PoolScheduler() { stop(); }

    void stop() noexcept {
        pollers_.stop();
        pool_.stop();
    }

    tp::ThreadPool<TwistSync>& pool() noexcept { return pool_; }
    TestStackPool& stacks() noexcept { return stacks_; }
    PollerPool& pollers() noexcept { return pollers_; }
    TimePoint now() noexcept { return pollers_.at(0).now(); }

private:
    TestStackPool stacks_;
    tp::ThreadPool<TwistSync> pool_;
    PollerPool pollers_;
};

static_assert(net::NetScheduler<PoolScheduler>);

struct FakeConn {
    FakeConn() noexcept = default;  // (Shared's constructor is not declared noexcept)

    bool open = false;
    twist::assist::Shared<int> data{0};  // the holder's writes, for the next holder
};

struct FakeFactory {
    int open(FakeConn& c, TimePoint) noexcept {
        c.open = true;
        return 0;
    }

    void close(FakeConn& c) noexcept { c.open = false; }
};

using Pool = net::BasicConnectionPool<FakeConn, FakeFactory, PoolScheduler>;
using Lease = Pool::Lease;

// The main thread sequences the phases with bounded waits; the fibers never
// spin on each other (a fiber's yield loop can starve the fiber it waits
// for under the simulator's scheduling).

// One connection (max_open 1), held by the test; B waits for it with a
// deadline of 1 ms (simulated) while C returns it: the return's hand-off (B
// woken by C, then B retires its timer) races with the timer's callback (B
// woken with ETIMEDOUT). `broken`: C marks it broken, so B gets the
// capacity and opens a new connection instead. Then a fiber closes the
// pool.
void return_races_deadline(bool broken) {
    map_test_stacks();
    check([broken] {
        Counter done{0};
        twist::ed::std::atomic<int> outcome{-1};
        PoolScheduler scheduler(2);
        Pool pool(scheduler, FakeFactory{}, net::ConnectionPoolOptions{.max_open = 1});
        Lease a;
        fiber::go(scheduler, [&]() noexcept {
            TWIST_ASSERT_M(pool.acquire(a) == 0, "first acquire opens");
            a->data.Write(1);
            done.fetch_add(1);
        });
        FIBERS_WAIT_BOUNDED(done.load() == 1, "first acquire");
        fiber::go(scheduler, [&]() noexcept {
            Lease b;
            const int e = pool.acquire(b, scheduler.now() + std::chrono::milliseconds(1));
            TWIST_ASSERT_M(e == 0 || e == ETIMEDOUT, "a connection or a timeout");
            if (e == 0) {
                TWIST_ASSERT_M(b->open, "an open connection");
                if (!broken) {
                    TWIST_ASSERT_M(b->data.Read() == 1, "the holder's writes, handed over");
                }
            }
            outcome.store(e);
            done.fetch_add(1);
        });
        fiber::go(scheduler, [&]() noexcept {
            if (broken) {
                a.mark_broken();
            }
            a.reset();
            done.fetch_add(1);
        });
        FIBERS_WAIT_BOUNDED(done.load() == 3, "waiter or returner never finished (lost wake)");
        fiber::go(scheduler, [&]() noexcept {
            TWIST_ASSERT_M(pool.close() == 0, "close");
            done.fetch_add(1);
        });
        FIBERS_WAIT_BOUNDED(done.load() == 4, "close never returned");
        const net::ConnectionPoolStats s = pool.stats();
        TWIST_ASSERT_M(s.open == 0 && s.waiting == 0, "everything closed");
        TWIST_ASSERT_M(outcome.load() == 0 || outcome.load() == ETIMEDOUT, "an outcome");
        scheduler.stop();
    });
}

TEST(ConnectionPoolTwist, ReturnRacesDeadline) { return_races_deadline(false); }

TEST(ConnectionPoolTwist, BrokenReturnRacesDeadline) { return_races_deadline(true); }

// close() resolves a timed waiter (ECANCELED) while its deadline fires
// (ETIMEDOUT): one of the two, exactly one wake, the timer retired. close()
// then waits for the leased connection, returned afterwards.
TEST(ConnectionPoolTwist, CloseRacesDeadline) {
    map_test_stacks();
    check([] {
        Counter done{0};
        Counter closed{0};
        PoolScheduler scheduler(2);
        Pool pool(scheduler, FakeFactory{}, net::ConnectionPoolOptions{.max_open = 1});
        Lease a;
        fiber::go(scheduler, [&]() noexcept {
            TWIST_ASSERT_M(pool.acquire(a) == 0, "first acquire opens");
            done.fetch_add(1);
        });
        FIBERS_WAIT_BOUNDED(done.load() == 1, "first acquire");
        fiber::go(scheduler, [&]() noexcept {
            Lease b;
            const int e = pool.acquire(b, scheduler.now() + std::chrono::milliseconds(1));
            TWIST_ASSERT_M(e == ECANCELED || e == ETIMEDOUT, "cancelled or timed out");
            done.fetch_add(1);
        });
        fiber::go(scheduler, [&]() noexcept {
            TWIST_ASSERT_M(pool.close() == 0, "close");  // waits for the lease
            closed.fetch_add(1);
        });
        FIBERS_WAIT_BOUNDED(done.load() == 2, "the waiter never woken");
        TWIST_ASSERT_M(closed.load() == 0, "close() returned with a lease out");
        fiber::go(scheduler, [&]() noexcept {
            a.reset();
            done.fetch_add(1);
        });
        FIBERS_WAIT_BOUNDED(done.load() == 3 && closed.load() == 1, "holder or closer never finished");
        TWIST_ASSERT_M(pool.stats().open == 0, "everything closed");
        scheduler.stop();
    });
}

}  // namespace
