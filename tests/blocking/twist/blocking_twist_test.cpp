// BlockingPool<TwistSync> under Twist.
//
// TWIST_SIM:    every scenario is explored with the Random and PCT schedulers
//               of the deterministic simulator (store buffering on in the
//               clang-twist-sim preset).
// TWIST_FAULTY: every scenario runs several times on real threads with fault
//               injection (pairs well with TSan).
//
// A lost wakeup shows up as
//   * a deadlock: every thread blocked (pool threads in wait(), the test
//     thread joining them in stop()), or
//   * a failed bounded wait: the test waits a bounded number of steps
//     (simulation) or 30 s (real threads) for tasks that never run.
// Task payloads are twist::assist::Shared, so a task that is not published
// to its pool thread with happens-before is reported as a data race.

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
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

#include "fibers/blocking/blocking_pool.hpp"
#include "fibers/sync/twist_sync.hpp"

namespace blocking = fibers::blocking;

namespace {

using Sync = fibers::sync::TwistSync;
using Pool = blocking::BlockingPool<Sync>;
using blocking::Task;
using Counter = twist::ed::std::atomic<std::size_t>;

std::size_t env_or(const char* name, std::size_t fallback) {
    const char* value = std::getenv(name);
    return value != nullptr ? static_cast<std::size_t>(std::strtoull(value, nullptr, 10)) : fallback;
}

// Env: FIBERS_TWIST_RUNS (runs per scheduler / repetitions), FIBERS_TWIST_SEED.
void check(const std::function<void()>& body) {
#if defined(__TWIST_SIM__)
    const std::size_t runs = env_or("FIBERS_TWIST_RUNS", 10'000);
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
    const std::size_t runs = env_or("FIBERS_TWIST_RUNS", 50);
    for (std::size_t i = 0; i < runs; ++i) {
        twist::cross::Run(body);
    }
#endif
}

// Bounded wait for `pred` (scheduling steps in simulation, 30 s on real
// threads); `what` is printed when the bound expires.
template <class Pred>
void wait_until(Pred pred, const char* what) {
#if defined(__TWIST_SIM__)
    for (std::size_t step = 0; !pred(); ++step) {
        if (step == 5'000) std::fprintf(stderr, "bounded wait expired: %s\n", what);
        TWIST_ASSERT_M(step < 5'000, "bounded wait expired");
        twist::ed::std::this_thread::yield();
    }
#else
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (!pred()) {
        const bool expired = std::chrono::steady_clock::now() >= deadline;
        if (expired) std::fprintf(stderr, "bounded wait expired: %s\n", what);
        TWIST_ASSERT_M(!expired, "bounded wait expired");
        twist::ed::std::this_thread::yield();
    }
#endif
}

void wait_count(Counter& counter, std::size_t target, const char* what) {
    wait_until([&] { return counter.load() >= target; }, what);
}

void wait_all_asleep(Pool& pool) {
    wait_until([&] { return pool.sleeping_threads() == pool.num_threads(); }, "pool threads asleep");
}

// A task with plain payload written by its producer: reading it on the pool
// thread checks the publication through the queue.
struct Job {
    twist::assist::Shared<int> payload{0};
    Counter* ran = nullptr;
    Counter* dropped = nullptr;

    Task make(Counter& r, Counter& d) {
        ran = &r;
        dropped = &d;
        *payload = 7;
        return Task(
            this,
            [](void* m) noexcept {
                auto* job = static_cast<Job*>(m);
                TWIST_ASSERT_M(job->payload.Read() == 7, "task payload published");
                job->ran->fetch_add(1);
            },
            [](void* m) noexcept { static_cast<Job*>(m)->dropped->fetch_add(1); });
    }
};

// Blocks its pool thread until released.
struct Hold {
    Counter entered{0};
    Counter released{0};

    Task make() {
        return Task(this, [](void* m) noexcept {
            auto* h = static_cast<Hold*>(m);
            h->entered.fetch_add(1);
            wait_count(h->released, 1, "hold released");
        }, nullptr);
    }
};

// Submits with bounded retries (a full queue whose threads never wake must
// fail the run, not spin forever).
void submit(Pool& pool, Task task) {
    wait_until([&] { return pool.try_submit(std::move(task)); }, "submit into a full queue");
}

}  // namespace

// Producers push into a pool whose threads are all asleep.
TEST(BlockingPoolTwist, ProducersVsSleepingThreads) {
    check([] {
        Counter ran{0}, dropped{0};
        Job jobs[6];
        Pool pool({.num_threads = 2, .queue_capacity = 2});
        wait_all_asleep(pool);
        auto produce = [&](int first) {
            for (int i = first; i < first + 3; ++i) submit(pool, jobs[i].make(ran, dropped));
        };
        twist::ed::std::thread a(produce, 0);
        twist::ed::std::thread b(produce, 3);
        a.join();
        b.join();
        wait_count(ran, 6, "all tasks ran");
        pool.stop();
        TWIST_ASSERT_M(ran.load() == 6 && dropped.load() == 0, "every task runs exactly once");
    });
}

// No waiting for the threads to fall asleep: submits race with them going to
// sleep, and with the thread coming back from the previous task.
TEST(BlockingPoolTwist, SubmitRacesGoingToSleep) {
    check([] {
        Counter ran{0}, dropped{0};
        Job jobs[3];
        Pool pool({.num_threads = 1, .queue_capacity = 4});
        submit(pool, jobs[0].make(ran, dropped));
        wait_count(ran, 1, "first task ran");
        submit(pool, jobs[1].make(ran, dropped));
        submit(pool, jobs[2].make(ran, dropped));
        wait_count(ran, 3, "all tasks ran");
        pool.stop();
        TWIST_ASSERT_M(ran.load() == 3, "every task runs exactly once");
    });
}

// One thread asleep, the other busy: the notified sleeper and the busy thread
// race for the new task. The loser must go back to sleep, not pop an empty
// queue.
TEST(BlockingPoolTwist, StolenWakeup) {
    check([] {
        Counter ran{0}, dropped{0};
        Job jobs[2];
        Hold hold;
        Pool pool({.num_threads = 2, .queue_capacity = 4});
        submit(pool, hold.make());
        wait_count(hold.entered, 1, "hold entered");
        wait_until([&] { return pool.sleeping_threads() == 1; }, "other thread asleep");
        submit(pool, jobs[0].make(ran, dropped));
        hold.released.fetch_add(1);
        submit(pool, jobs[1].make(ran, dropped));
        wait_count(ran, 2, "both tasks ran");
        pool.stop();
        TWIST_ASSERT_M(ran.load() == 2, "every task runs exactly once");
    });
}

// A batch into sleeping threads: every accepted task runs.
TEST(BlockingPoolTwist, BatchIntoSleepingThreads) {
    check([] {
        Counter ran{0}, dropped{0};
        Job jobs[4];
        Pool pool({.num_threads = 2, .queue_capacity = 3});
        wait_all_asleep(pool);
        Task tasks[4] = {jobs[0].make(ran, dropped), jobs[1].make(ran, dropped), jobs[2].make(ran, dropped),
                         jobs[3].make(ran, dropped)};
        const std::size_t accepted = pool.try_submit_batch(tasks, 4);
        TWIST_ASSERT_M(accepted >= 1 && accepted <= 3, "batch bounded by capacity");
        std::size_t total = accepted;
        wait_until([&] { return (total += pool.try_submit_batch(tasks + total, 4 - total)) == 4; },
                   "rest of the batch");
        wait_count(ran, 4, "all tasks ran");
        pool.stop();
        TWIST_ASSERT_M(ran.load() == 4 && dropped.load() == 0, "every task runs exactly once");
    });
}

// stop() races with sleeping threads (no wait for them to fall asleep in one
// variant). A lost stop wakeup deadlocks in join().
TEST(BlockingPoolTwist, StopRacesSleeping) {
    check([] {
        {
            Pool pool({.num_threads = 2, .queue_capacity = 2});
            pool.stop();
        }
        {
            Pool pool({.num_threads = 2, .queue_capacity = 2});
            wait_all_asleep(pool);
            pool.stop();
            pool.stop();  // idempotent
        }
    });
}

// stop() races with a producer: every task is run, dropped by the pool, or
// refused (and kept by the producer); exactly one of those, exactly once.
TEST(BlockingPoolTwist, StopRacesSubmit) {
    check([] {
        static constexpr int kTasks = 4;
        Counter ran{0}, dropped{0}, refused{0};
        Job jobs[kTasks];
        Pool pool({.num_threads = 1, .queue_capacity = 2});
        twist::ed::std::thread producer([&] {
            for (int i = 0; i < kTasks; ++i) {
                Task task = jobs[i].make(ran, dropped);
                if (!pool.try_submit(std::move(task))) {
                    TWIST_ASSERT_M(static_cast<bool>(task), "refused task stays with the producer");
                    static_cast<void>(task.release());
                    refused.fetch_add(1);
                }
            }
        });
        pool.stop();
        producer.join();
        pool.stop();
        TWIST_ASSERT_M(ran.load() + dropped.load() + refused.load() == kTasks,
                       "each task is run, dropped or refused, exactly once");
    });
}

// Several producers race for the last free slots of a full queue.
TEST(BlockingPoolTwist, QueueFullRace) {
    check([] {
        Counter ran{0}, dropped{0}, accepted{0}, refused{0};
        Job jobs[3];
        Hold hold;
        Pool pool({.num_threads = 1, .queue_capacity = 2});
        submit(pool, hold.make());
        wait_count(hold.entered, 1, "hold entered");
        auto produce = [&](int i) {
            Task task = jobs[i].make(ran, dropped);
            if (pool.try_submit(std::move(task))) {
                accepted.fetch_add(1);
            } else {
                TWIST_ASSERT_M(static_cast<bool>(task), "refused task stays with the producer");
                static_cast<void>(task.release());
                refused.fetch_add(1);
            }
        };
        twist::ed::std::thread a(produce, 0);
        twist::ed::std::thread b(produce, 1);
        twist::ed::std::thread c(produce, 2);
        a.join();
        b.join();
        c.join();
        TWIST_ASSERT_M(accepted.load() == 2 && refused.load() == 1, "exactly the capacity is accepted");
        hold.released.fetch_add(1);
        wait_count(ran, 2, "accepted tasks ran");
        pool.stop();
        TWIST_ASSERT_M(ran.load() == 2 && dropped.load() == 0, "accepted tasks run exactly once");
    });
}
