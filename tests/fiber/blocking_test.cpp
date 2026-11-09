// this_fiber::blocking (fiber/blocking.hpp) on the full Scheduler (StdSync):
// results of every kind, outside fibers, workers kept free while a call
// blocks, many fibers, and the drop path after BlockingPool::stop().
//
// Fibers never use gtest assertions themselves: they record into atomics or
// plain fields that the test thread reads after a Latch (whose acq_rel
// count_down / acquire wait order them).

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include "fibers/blocking/ops.hpp"
#include "fibers/fiber/blocking.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace blocking = fibers::blocking;
namespace sched = fibers::sched;
namespace this_fiber = fibers::fiber::this_fiber;
using fibers::sched::Scheduler;
using namespace std::chrono_literals;

namespace {

// Joins fibers from the test thread. Declare it BEFORE the scheduler.
class Latch {
public:
    explicit Latch(std::int64_t count) : count_(count) {}

    void count_down() noexcept { count_.fetch_sub(1, std::memory_order_acq_rel); }

    bool wait(std::chrono::milliseconds timeout = 30s) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (count_.load(std::memory_order_acquire) != 0) {
            if (std::chrono::steady_clock::now() > deadline) {
                return false;
            }
            std::this_thread::sleep_for(100us);
        }
        return true;
    }

private:
    std::atomic<std::int64_t> count_;
};

sched::SchedulerOptions options(std::size_t workers, std::size_t blocking_threads = 2) {
    sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = 1;
    o.blocking.num_threads = blocking_threads;
    o.stacks.max_stacks = 1024;
    return o;
}

TEST(Blocking, RunsOnABlockingThreadAndReturnsTheResult) {
    Latch done(1);
    int result = 0;
    bool ran_on_worker = true;
    bool in_fiber_after = false;
    Scheduler scheduler(options(2));
    fiber::go(scheduler, [&]() noexcept {
        result = this_fiber::blocking([&]() noexcept {
            ran_on_worker = scheduler.in_worker_thread();
            return 42;
        });
        in_fiber_after = this_fiber::in_fiber();
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(result, 42);
    EXPECT_FALSE(ran_on_worker) << "offload must not run the call on a worker";
    EXPECT_TRUE(in_fiber_after);
}

TEST(Blocking, VoidReferenceAndMoveOnlyResults) {
    Latch done(1);
    int calls = 0;
    int target = 5;
    int* ref_address = nullptr;
    std::unique_ptr<int> moved;
    Scheduler scheduler(options(1));
    fiber::go(scheduler, [&]() noexcept {
        this_fiber::blocking([&]() noexcept { ++calls; });
        int& ref = this_fiber::blocking([&]() noexcept -> int& { return target; });
        ref_address = &ref;
        moved = this_fiber::blocking([]() noexcept { return std::make_unique<int>(7); });
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(ref_address, &target);
    ASSERT_NE(moved, nullptr);
    EXPECT_EQ(*moved, 7);
}

TEST(Blocking, OutsideAFiberRunsInline) {
    Scheduler scheduler(options(1));
    const std::thread::id me = std::this_thread::get_id();
    std::thread::id ran_on;
    const int r = this_fiber::blocking([&]() noexcept {
        ran_on = std::this_thread::get_id();
        return 3;
    });
    EXPECT_EQ(r, 3);
    EXPECT_EQ(ran_on, me);
    // Plain callbacks are not fibers either: inline on the worker.
    Latch done(1);
    bool on_worker = false;
    scheduler.submit([&]() noexcept {
        this_fiber::blocking([&]() noexcept { on_worker = scheduler.in_worker_thread(); });
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_TRUE(on_worker);
}

// The point of offloading: a slow call parks its fiber, not the worker. One
// worker; fiber A's call can only finish once fiber B has run on that worker.
TEST(Blocking, SlowCallDoesNotBlockTheWorker) {
    Latch done(2);
    std::atomic<bool> b_ran{false};
    bool a_saw_b = false;
    int b_steps = 0;
    Scheduler scheduler(options(1));
    fiber::go(scheduler, [&]() noexcept {
        a_saw_b = this_fiber::blocking([&]() noexcept {
            const auto deadline = std::chrono::steady_clock::now() + 10s;
            while (!b_ran.load()) {
                if (std::chrono::steady_clock::now() > deadline) return false;
                std::this_thread::sleep_for(1ms);
            }
            std::this_thread::sleep_for(20ms);  // still slow after B ran
            return true;
        });
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        for (int i = 0; i < 100; ++i) {
            ++b_steps;
            this_fiber::yield();
        }
        b_ran.store(true);
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_TRUE(a_saw_b);
    EXPECT_EQ(b_steps, 100);
}

// Many fibers, few workers, the default 2 blocking threads: every call runs
// exactly once and every fiber gets its own result.
TEST(Blocking, ManyFibersFewWorkers) {
    constexpr int kFibers = 500;
    constexpr int kRounds = 4;
    Latch done(kFibers);
    std::atomic<int> calls{0};
    std::vector<int> sums(kFibers, 0);
    Scheduler scheduler(options(2));
    for (int f = 0; f < kFibers; ++f) {
        fiber::go(scheduler, [&, f]() noexcept {
            for (int r = 0; r < kRounds; ++r) {
                sums[f] += this_fiber::blocking([&calls, f, r]() noexcept {
                    calls.fetch_add(1, std::memory_order_relaxed);
                    return f * 10 + r;
                });
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(calls.load(), kFibers * kRounds);
    for (int f = 0; f < kFibers; ++f) {
        EXPECT_EQ(sums[f], f * 10 * kRounds + 6) << f;
    }
}

// ----------------------------------------------------- after BlockingPool::stop

// submit() after stop drops the op on the submitting fiber itself: the fiber
// is woken (NOTIFIED), runs the call inline on its worker, gets the result.
TEST(BlockingAfterStop, DroppedOpRunsInlineOnTheWorker) {
    Latch done(1);
    int result = 0;
    bool on_worker = false;
    Scheduler scheduler(options(1));
    scheduler.blocking().stop();
    fiber::go(scheduler, [&]() noexcept {
        result = this_fiber::blocking([&]() noexcept {
            on_worker = scheduler.in_worker_thread();
            return 9;
        });
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(result, 9);
    EXPECT_TRUE(on_worker);
}

// The op is queued (the only blocking thread is busy) when stop() begins:
// stop()'s drain drops it on the stopping thread, which wakes the parked
// fiber through the ThreadPool; the fiber runs the call inline.
TEST(BlockingAfterStop, QueuedOpDroppedByStopWakesTheFiber) {
    Latch done(1);
    std::atomic<bool> gate_entered{false};
    std::atomic<bool> gate_open{false};
    std::atomic<bool> fiber_submitted{false};
    int result = 0;
    bool on_worker = false;
    Scheduler scheduler(options(1, 1));
    // Occupies the blocking thread. Fire-and-forget: valid until the stop.
    auto gate_fn = [&]() noexcept {
        gate_entered.store(true);
        while (!gate_open.load()) std::this_thread::sleep_for(100us);
    };
    blocking::CallOp<decltype(gate_fn)> gate(gate_fn);
    scheduler.blocking().submit(gate);
    while (!gate_entered.load()) std::this_thread::sleep_for(100us);
    fiber::go(scheduler, [&]() noexcept {
        fiber_submitted.store(true);
        result = this_fiber::blocking([&]() noexcept {
            on_worker = scheduler.in_worker_thread();
            return 11;
        });
        done.count_down();
    });
    while (!fiber_submitted.load()) std::this_thread::sleep_for(100us);
    std::this_thread::sleep_for(10ms);  // let it submit and park
    std::thread stopper([&] { scheduler.blocking().stop(); });
    while (!scheduler.blocking().stop_requested()) std::this_thread::sleep_for(100us);
    gate_open.store(true);
    stopper.join();
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(result, 11);
    EXPECT_TRUE(on_worker) << "a dropped call runs inline on the fiber's worker";
}

// Stop racing with many offloading fibers: every call still runs exactly
// once (on a pool thread, or inline after a drop) and no fiber hangs.
TEST(BlockingAfterStop, StopRacingWithOffloads) {
    constexpr int kFibers = 300;
    Latch done(kFibers);
    std::atomic<int> calls{0};
    std::atomic<int> wrong{0};
    Scheduler scheduler(options(3));
    for (int f = 0; f < kFibers; ++f) {
        fiber::go(scheduler, [&, f]() noexcept {
            for (int r = 0; r < 3; ++r) {
                const int v = this_fiber::blocking([&calls, f]() noexcept {
                    calls.fetch_add(1);
                    return f;
                });
                if (v != f) wrong.fetch_add(1);
            }
            done.count_down();
        });
    }
    std::this_thread::sleep_for(1ms);
    scheduler.blocking().stop();
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(calls.load(), kFibers * 3);
    EXPECT_EQ(wrong.load(), 0);
}

}  // namespace
