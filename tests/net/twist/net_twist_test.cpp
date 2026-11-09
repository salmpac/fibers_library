// net::BasicPollDesc under Twist: the readiness words (rd: waiter / READY /
// CLOSED), the operation references and close().
//
// A FakeBackend fake fd stands in for the socket: the "syscall" of a read
// is a check of an atomic counter of available messages (EAGAIN when there
// is none), and the producer makes data available, then signal()s the fake
// fd, which the poller reports as an EPOLLIN edge (EPOLLET: one per
// signal). The poller thread runs the descriptor's callback exactly as with
// epoll, and close() posts its unwatch timer to it.
//
// TWIST_SIM:    Random and PCT schedulers of the deterministic simulator
//               (x86-TSO store buffering in the clang-twist-sim preset).
// TWIST_FAULTY: real threads with fault injection (+ TSan in
//               clang-twist-faulty-tsan).
//
// The words carry pointers to waiters; a missing release / acquire on them
// is caught through the waiter's HbProbe (see IoWaiter) and, for the
// operation references, through Shared cells written by an operation and
// read by the closer after close() returned.

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
#include "fibers/net/poll_desc.hpp"
#include "fibers/poller/fake_backend.hpp"
#include "fibers/poller/poller_pool.hpp"
#include "fibers/stack/stack_pool.hpp"
#include "fibers/sync/twist_sync.hpp"
#include "fibers/thread_pool/thread_pool.hpp"

namespace fiber = fibers::fiber;
namespace net = fibers::net;
namespace tp = fibers::thread_pool;
using fibers::sync::TwistSync;

namespace {

using Counter = twist::ed::std::atomic<std::size_t>;

std::size_t env_or(const char* name, std::size_t fallback) {
    const char* value = std::getenv(name);
    return value != nullptr ? static_cast<std::size_t>(std::strtoull(value, nullptr, 10)) : fallback;
}

// Env: FIBERS_TWIST_RUNS (runs per scheduler / repetitions), FIBERS_TWIST_SEED.
void check(const std::function<void()>& body, std::size_t sim_runs = 1'000) {
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
// order (see tests/fiber/twist/fiber_twist_test.cpp: addresses travel
// through atomics and must not differ between simulated runs).
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

// What a PollDesc needs: a ThreadPool, stacks, and a PollerPool of
// FakeBackend pollers.
class NetScheduler {
public:
    using SyncPolicy = TwistSync;
    using PollerPool = fibers::poller::PollerPool<fibers::poller::FakeBackend<TwistSync>, TwistSync>;
    using Poller = PollerPool::PollerType;
    using Timer = PollerPool::Timer;

    explicit NetScheduler(std::size_t workers)
        : pool_(tiny(workers)), pollers_(fibers::poller::TaskSink::of(pool_), {.num_pollers = 1}) {
        pollers_.start();
    }

    ~NetScheduler() { stop(); }

    void stop() noexcept {
        pollers_.stop();
        pool_.stop();
    }

    tp::ThreadPool<TwistSync>& pool() noexcept { return pool_; }
    TestStackPool& stacks() noexcept { return stacks_; }
    PollerPool& pollers() noexcept { return pollers_; }
    fibers::poller::FakeBackend<TwistSync>& backend() noexcept { return pollers_.at(0).backend(); }

private:
    TestStackPool stacks_;
    tp::ThreadPool<TwistSync> pool_;
    PollerPool pollers_;
};

static_assert(net::NetScheduler<NetScheduler>);

using PollDesc = net::BasicPollDesc<NetScheduler>;
using net::Direction;

// A reader fiber takes kMessages messages, parking whenever none is
// available; a producer thread publishes each one and signals the fake fd
// (one edge each). Every edge races with the reader's syscall / arm: a lost
// edge leaves the reader parked forever. Then a plain thread closes.
TEST(NetTwist, ReadinessEdgesAreNotLost) {
    map_test_stacks();
    check([] {
        constexpr int kMessages = 3;
        Counter done{0};
        twist::ed::std::atomic<int> available{0};
        twist::assist::Shared<int> payload[kMessages] = {0, 0, 0};
        NetScheduler scheduler(2);
        const int fd = scheduler.backend().create_event();
        PollDesc desc;
        TWIST_ASSERT_M(desc.open(scheduler, fd) == 0, "open");
        fiber::go(scheduler, [&]() noexcept {
            int consumed = 0;
            while (consumed < kMessages) {
                PollDesc::Guard guard(desc);
                TWIST_ASSERT_M(static_cast<bool>(guard), "open descriptor");
                const ssize_t r = desc.io(Direction::kRead, [&]() noexcept -> ssize_t {
                    return available.load(std::memory_order_acquire) > consumed ? 1 : -EAGAIN;
                });
                TWIST_ASSERT_M(r == 1, "a message");
                TWIST_ASSERT_M(payload[consumed].Read() == consumed + 1, "the message's data");
                ++consumed;
            }
            done.fetch_add(1);
        });
        twist::ed::std::thread producer([&] {
            for (int i = 0; i < kMessages; ++i) {
                payload[i].Write(i + 1);
                available.store(i + 1, std::memory_order_release);
                scheduler.backend().signal(fd);
            }
        });
        producer.join();
        FIBERS_WAIT_BOUNDED(done.load() == 1, "reader never woken (lost edge)");
        TWIST_ASSERT_M(desc.close() == 0, "close");
        TWIST_ASSERT_M(desc.close() == EBADF, "second close");
        scheduler.stop();
    });
}

// close() races with a reader that may be anywhere: before enter() (it
// then fails), between enter() and its arm (it sees CLOSED), or parked (the
// close timer wakes it with ECANCELED). close() returns only after the
// reader's operation left: its write inside the operation is visible to
// the closer (leave's release -> the closer's acquire, directly or through
// the last leaver's wake). Closer: a plain thread or a fiber.
void close_races_reader(bool closer_is_fiber) {
    map_test_stacks();
    check([closer_is_fiber] {
        Counter done{0};
        twist::ed::std::atomic<bool> entered{false};
        twist::ed::std::atomic<int> closed{-1};
        twist::assist::Shared<int> inside{0};
        NetScheduler scheduler(2);
        const int fd = scheduler.backend().create_event();
        PollDesc desc;
        TWIST_ASSERT_M(desc.open(scheduler, fd) == 0, "open");
        fiber::go(scheduler, [&]() noexcept {
            PollDesc::Guard guard(desc);
            if (guard) {
                entered.store(true, std::memory_order_relaxed);
                const ssize_t r = desc.io(Direction::kRead, []() noexcept -> ssize_t { return -EAGAIN; });
                TWIST_ASSERT_M(r == -ECANCELED, "cancelled by close");
                inside.Write(1);
            }
            done.fetch_add(1);
        });
        auto close_and_check = [&] {
            const int r = desc.close();
            TWIST_ASSERT_M(r == 0, "close");
            if (entered.load(std::memory_order_relaxed)) {
                TWIST_ASSERT_M(inside.Read() == 1, "the operation finished before close() returned");
            }
            closed.store(r);
        };
        if (closer_is_fiber) {
            fiber::go(scheduler, [&]() noexcept {
                close_and_check();
                done.fetch_add(1);
            });
        } else {
            // A thread of its own (not the simulation's main thread, where
            // a reported race trips the simulator's own run-queue check).
            twist::ed::std::thread closer(close_and_check);
            closer.join();
        }
        FIBERS_WAIT_BOUNDED(done.load() == (closer_is_fiber ? 2u : 1u), "reader or closer never woken");
        TWIST_ASSERT_M(closed.load() == 0, "closed");
        scheduler.stop();
    });
}

TEST(NetTwist, CloseFromThreadRacesReader) { close_races_reader(false); }

TEST(NetTwist, CloseFromFiberRacesReader) { close_races_reader(true); }

// open() on one thread publishes the descriptor's fields (through refs_) to
// an operation that enters on another thread and to a closer on a third:
// enter() keeps failing until open() is done, then sees everything open()
// wrote; close() too, though it learns of the open only from a relaxed flag.
TEST(NetTwist, OpenPublishesToEnterAndClose) {
    map_test_stacks();
    check([] {
        twist::ed::std::atomic<bool> operated{false};
        NetScheduler scheduler(1);
        const int fd = scheduler.backend().create_event();
        PollDesc desc;
        twist::ed::std::thread opener([&] { TWIST_ASSERT_M(desc.open(scheduler, fd) == 0, "open"); });
        twist::ed::std::thread operation([&] {
            FIBERS_WAIT_BOUNDED(desc.enter(), "open() never became visible");
            // Does not have to wait: fine outside a fiber.
            TWIST_ASSERT_M(desc.io(Direction::kRead, []() noexcept -> ssize_t { return 1; }) == 1, "op");
            desc.leave();
            operated.store(true, std::memory_order_relaxed);
        });
        twist::ed::std::thread closer([&] {
            FIBERS_WAIT_BOUNDED(operated.load(std::memory_order_relaxed), "operation never ran");
            TWIST_ASSERT_M(desc.close() == 0, "close");
        });
        opener.join();
        operation.join();
        closer.join();
        scheduler.stop();
    });
}

// Edges and close at once: the reader consumes what arrives until close()
// cancels it; the producer keeps signalling. No lost edge before the close,
// no wake after it, and the poller never sees the descriptor after its
// unwatch (the descriptor is destroyed right after close()).
TEST(NetTwist, EdgesRacingClose) {
    map_test_stacks();
    check([] {
        constexpr int kMessages = 2;
        Counter done{0};
        twist::ed::std::atomic<int> available{0};
        twist::ed::std::atomic<bool> got_all{false};
        NetScheduler scheduler(2);
        const int fd = scheduler.backend().create_event();
        {
            PollDesc desc;
            TWIST_ASSERT_M(desc.open(scheduler, fd) == 0, "open");
            fiber::go(scheduler, [&]() noexcept {
                int consumed = 0;
                for (;;) {
                    PollDesc::Guard guard(desc);
                    if (!guard) {
                        break;
                    }
                    const ssize_t r = desc.io(Direction::kRead, [&]() noexcept -> ssize_t {
                        return available.load(std::memory_order_acquire) > consumed ? 1 : -EAGAIN;
                    });
                    if (r != 1) {
                        TWIST_ASSERT_M(r == -ECANCELED, "only close stops the reader");
                        break;
                    }
                    if (++consumed == kMessages) {
                        got_all.store(true);
                    }
                }
                done.fetch_add(1);
            });
            twist::ed::std::thread producer([&] {
                for (int i = 0; i < kMessages; ++i) {
                    available.store(i + 1, std::memory_order_release);
                    scheduler.backend().signal(fd);
                }
            });
            // Close once everything was consumed (a lost edge hangs here),
            // while the producer may still be signalling.
            FIBERS_WAIT_BOUNDED(got_all.load(), "reader missed an edge");
            TWIST_ASSERT_M(desc.close() == 0, "close");
            producer.join();
            FIBERS_WAIT_BOUNDED(done.load() == 1, "reader not cancelled");
        }  // ~PollDesc: the poller must not reference it any more
        scheduler.backend().signal(fd);  // an edge for the unwatched fd: ignored
        scheduler.stop();
    });
}

}  // namespace
