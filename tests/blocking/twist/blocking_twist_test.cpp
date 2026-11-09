// BlockingPool<TwistSync> under Twist.
//
// TWIST_SIM:    every scenario is explored with the Random and PCT schedulers
//               of the deterministic simulator (x86-TSO store buffering on in
//               the clang-twist-sim preset, so a missing seq_cst fence in the
//               wake protocol is observable).
// TWIST_FAULTY: every scenario runs several times on real threads with fault
//               injection (pairs well with TSan).
//
// A lost wakeup shows up as
//   * a deadlock: every thread blocked (pool threads parked, the test thread
//     joining them in stop()), or
//   * a failed bounded wait: the test waits a bounded number of steps
//     (simulation) or 30 s (real threads) for ops that never run.
// Op payloads are twist::assist::Shared, so an op whose fields are not
// published to its pool thread with happens-before is reported as a data
// race.

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
using blocking::IoResult;
using blocking::Sink;
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

// An op with a plain payload written by its producer: reading it on the pool
// thread checks the publication through the queue. Its continuation counts
// runs and drops (memo = the op).
struct Job final : blocking::Op<Job, Sync> {
    twist::assist::Shared<int> payload{0};
    Counter* ran = nullptr;
    Counter* dropped = nullptr;

    // Producer side: fills the op before submitting it.
    Job& prepare(Counter& r, Counter& d) {
        ran = &r;
        dropped = &d;
        *payload = 7;
        set_continuation(Task(
                             this, [](void* m) noexcept { static_cast<Job*>(m)->ran->fetch_add(1); },
                             [](void* m) noexcept { static_cast<Job*>(m)->dropped->fetch_add(1); }),
                         Sink::run_inline());
        return *this;
    }

    IoResult execute() noexcept {
        TWIST_ASSERT_M(payload.Read() == 7, "op payload published");
        return IoResult::success(1);
    }
};

// Blocks its pool thread until released.
struct Hold final : blocking::Op<Hold, Sync> {
    Counter entered{0};
    Counter released{0};

    IoResult execute() noexcept {
        entered.fetch_add(1);
        wait_count(released, 1, "hold released");
        return IoResult{};
    }
};

}  // namespace

// Producers push into a pool whose threads are all asleep.
TEST(BlockingPoolTwist, ProducersVsParkedThreads) {
    check([] {
        Counter ran{0}, dropped{0};
        Job jobs[4];
        Pool pool({.num_threads = 2});
        wait_all_asleep(pool);
        auto produce = [&](int first) {
            for (int i = first; i < first + 2; ++i) pool.submit(jobs[i].prepare(ran, dropped));
        };
        twist::ed::std::thread a(produce, 0);
        twist::ed::std::thread b(produce, 2);
        a.join();
        b.join();
        wait_count(ran, 4, "all ops ran");
        pool.stop();
        TWIST_ASSERT_M(ran.load() == 4 && dropped.load() == 0, "every op runs exactly once");
    });
}

// No waiting for the thread to fall asleep: each submit races with the
// thread coming back from the previous op and going to sleep, i.e. a
// producer mid-push (between its exchange and its link) against the
// consumer's announce / re-check / park. One producer, one consumer: the
// smallest Vyukov-window scenario.
TEST(BlockingPoolTwist, PushRacesGoingToSleep) {
    check([] {
        Counter ran{0}, dropped{0};
        Job jobs[3];
        Pool pool({.num_threads = 1});
        pool.submit(jobs[0].prepare(ran, dropped));
        wait_count(ran, 1, "first op ran");
        pool.submit(jobs[1].prepare(ran, dropped));
        pool.submit(jobs[2].prepare(ran, dropped));
        wait_count(ran, 3, "all ops ran");
        pool.stop();
        TWIST_ASSERT_M(ran.load() == 3, "every op runs exactly once");
    });
}

// Two producers race each other into an empty queue (the second one's link
// goes into the first one's node, possibly still in transit) while the only
// consumer parks or pops.
TEST(BlockingPoolTwist, TwoProducersMidPushVsConsumer) {
    check([] {
        Counter ran{0}, dropped{0};
        Job jobs[2];
        Pool pool({.num_threads = 1});
        twist::ed::std::thread a([&] { pool.submit(jobs[0].prepare(ran, dropped)); });
        twist::ed::std::thread b([&] { pool.submit(jobs[1].prepare(ran, dropped)); });
        a.join();
        b.join();
        wait_count(ran, 2, "both ops ran");
        pool.stop();
        TWIST_ASSERT_M(ran.load() == 2 && dropped.load() == 0, "every op runs exactly once");
    });
}

// One thread asleep, the other busy: the woken thread and the busy one race
// for the new op; the loser goes back to sleep. Then a second op must still
// wake somebody.
TEST(BlockingPoolTwist, StolenWakeup) {
    check([] {
        Counter ran{0}, dropped{0};
        Job jobs[2];
        Hold hold;
        Pool pool({.num_threads = 2});
        pool.submit(hold);
        wait_count(hold.entered, 1, "hold entered");
        wait_until([&] { return pool.sleeping_threads() == 1; }, "other thread asleep");
        pool.submit(jobs[0].prepare(ran, dropped));
        hold.released.fetch_add(1);
        pool.submit(jobs[1].prepare(ran, dropped));
        wait_count(ran, 2, "both ops ran");
        pool.stop();
        TWIST_ASSERT_M(ran.load() == 2, "every op runs exactly once");
    });
}

// Two ops into two sleeping threads: one wake each (both threads end up
// holding an op at the same time).
TEST(BlockingPoolTwist, EachPushWakesAThread) {
    check([] {
        Hold holds[2];
        Pool pool({.num_threads = 2});
        wait_all_asleep(pool);
        pool.submit(holds[0]);
        pool.submit(holds[1]);
        wait_count(holds[0].entered, 1, "first hold entered");
        wait_count(holds[1].entered, 1, "second hold entered (needs a second thread)");
        holds[0].released.fetch_add(1);
        holds[1].released.fetch_add(1);
        pool.stop();
    });
}

// stop() races with threads going to sleep (no wait in the first variant).
// A lost stop wakeup deadlocks in join().
TEST(BlockingPoolTwist, StopRacesSleeping) {
    check([] {
        {
            Pool pool({.num_threads = 2});
            pool.stop();
        }
        {
            Pool pool({.num_threads = 2});
            wait_all_asleep(pool);
            pool.stop();
            pool.stop();  // idempotent
        }
    });
}

// stop() races with producers: every op is run or dropped (by stop()'s drain
// or by its own submit), exactly once; none stays behind in the queue.
TEST(BlockingPoolTwist, StopRacesSubmit) {
    check([] {
        static constexpr int kOps = 4;
        Counter ran{0}, dropped{0};
        Job jobs[kOps];
        {
            Pool pool({.num_threads = 1});
            twist::ed::std::thread a([&] {
                for (int i = 0; i < kOps / 2; ++i) pool.submit(jobs[i].prepare(ran, dropped));
            });
            twist::ed::std::thread b([&] {
                for (int i = kOps / 2; i < kOps; ++i) pool.submit(jobs[i].prepare(ran, dropped));
            });
            pool.stop();
            a.join();
            b.join();
            TWIST_ASSERT_M(ran.load() + dropped.load() == kOps,
                           "each op is run or dropped once all submits returned");
        }
        TWIST_ASSERT_M(ran.load() + dropped.load() == kOps, "each op is run or dropped, exactly once");
    });
}

// A queued op behind a running one when stop() comes: dropped exactly once;
// a submit after stop() drops at once.
TEST(BlockingPoolTwist, StopDropsQueuedAndLateOps) {
    check([] {
        Counter ran{0}, dropped{0};
        Job queued, late;
        Hold hold;
        Pool pool({.num_threads = 1});
        pool.submit(hold);
        wait_count(hold.entered, 1, "hold entered");
        pool.submit(queued.prepare(ran, dropped));
        twist::ed::std::thread stopper([&] { pool.stop(); });
        wait_until([&] { return pool.stop_requested(); }, "stop requested");
        hold.released.fetch_add(1);
        stopper.join();
        TWIST_ASSERT_M(ran.load() == 0 && dropped.load() == 1, "queued op dropped once");
        pool.submit(late.prepare(ran, dropped));
        TWIST_ASSERT_M(ran.load() == 0 && dropped.load() == 2, "late op dropped before submit returns");
    });
}
