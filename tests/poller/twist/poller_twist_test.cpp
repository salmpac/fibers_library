// Poller<FakeBackend<TwistSync>, TwistSync> and the in-process ipc ring under
// Twist.
//
// TWIST_SIM:    every scenario is explored with the Random and PCT schedulers
//               of the deterministic simulator (simulated time: the poller
//               sleeps on a Twist condition variable until a deadline).
// TWIST_FAULTY: every scenario runs several times on real threads with fault
//               injection (pairs well with TSan).
//
// A lost wakeup shows up as
//   * a deadlock (the poller sleeps without a deadline while the test thread
//     joins it), or
//   * a failed bounded wait: the poller sleeps until a far deadline (1 h)
//     while the test waits a bounded number of steps (simulation) or 30 s
//     (real threads) for something that should have woken it.
// Published plain data (timer payloads, ring items) is twist::assist::Shared,
// so a missing release / acquire is reported as a data race.

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
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

#include "fibers/ipc/doorbell.hpp"
#include "fibers/ipc/ring_checkable.hpp"
#include "fibers/ipc/spsc_ring.hpp"
#include "fibers/poller/fake_backend.hpp"
#include "fibers/poller/poller.hpp"
#include "fibers/sync/twist_sync.hpp"

namespace ipc = fibers::ipc;
namespace poller = fibers::poller;

namespace {

using Sync = fibers::sync::TwistSync;
using Backend = poller::FakeBackend<Sync>;
using Poller = poller::Poller<Backend, Sync>;
using Timer = Poller::Timer;
using poller::TaskBatch;
using poller::TaskSink;
using poller::TimerEvent;
using Counter = twist::ed::std::atomic<std::size_t>;
using namespace std::chrono_literals;

std::size_t env_or(const char* name, std::size_t fallback) {
    const char* value = std::getenv(name);
    return value != nullptr ? static_cast<std::size_t>(std::strtoull(value, nullptr, 10)) : fallback;
}

// Same driver as the thread pool's Twist tests (more runs by default: a
// poller scenario simulates in well under a millisecond).
// Env: FIBERS_TWIST_RUNS (runs per scheduler / repetitions), FIBERS_TWIST_SEED.
void check(const std::function<void()>& body) {
#if defined(__TWIST_SIM__)
    const std::size_t runs = env_or("FIBERS_TWIST_RUNS", 10'000);  // scenarios are cheap
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

// Bounded wait (scheduling steps in simulation, wall-clock on real threads).
// `what` is printed when the bound expires.
void wait_until(Counter& counter, std::size_t target, const char* what) {
#if defined(__TWIST_SIM__)
    for (std::size_t step = 0; counter.load() < target; ++step) {
        if (step == 5'000) std::fprintf(stderr, "bounded wait expired: %s\n", what);
        TWIST_ASSERT_M(step < 5'000, "bounded wait expired");
        twist::ed::std::this_thread::yield();
    }
#else
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (counter.load() < target) {
        const bool expired = std::chrono::steady_clock::now() >= deadline;
        if (expired) std::fprintf(stderr, "bounded wait expired: %s\n", what);
        TWIST_ASSERT_M(!expired, "bounded wait expired");
        twist::ed::std::this_thread::yield();
    }
#endif
}

// Bounded retry loop for producers (a full ring whose consumer never wakes
// must fail the run, not spin forever: a runnable thread hides the deadlock).
class Patience {
public:
    void yield(const char* what) {
#if defined(__TWIST_SIM__)
        const bool expired = ++steps_ >= 5'000;
#else
        const bool expired = std::chrono::steady_clock::now() >= deadline_;
#endif
        if (expired) std::fprintf(stderr, "bounded retry expired: %s\n", what);
        TWIST_ASSERT_M(!expired, "bounded retry expired");
        twist::ed::std::this_thread::yield();
    }

private:
    [[maybe_unused]] std::size_t steps_ = 0;
    std::chrono::steady_clock::time_point deadline_ =
        std::chrono::steady_clock::now() + std::chrono::seconds(30);
};

struct InlineSink {
    void submit_batch(poller::Task* tasks, std::size_t count) noexcept {
        for (std::size_t i = 0; i < count; ++i) std::move(tasks[i]).run();
    }
};

poller::PollerOptions tiny() {
    return poller::PollerOptions{
        .spin_budget = 1,  // every spin_hint is a scheduling point
        .os_check_interval = 2,
        .min_backoff = 10us,
        .max_backoff = 40us,
    };
}

// A timer whose payload is plain shared data written by the scheduling thread:
// the callback reading it checks the MPSC publication (release / acquire).
struct CheckedTimer {
    Timer timer;
    twist::assist::Shared<int> payload{0};
    Counter* fired = nullptr;
    Counter* cancelled = nullptr;

    static void callback(void* memo, TimerEvent event, TaskBatch&) noexcept {
        auto* self = static_cast<CheckedTimer*>(memo);
        TWIST_ASSERT_M(self->payload.Read() == 7, "timer payload published");
        (event == TimerEvent::kFired ? self->fired : self->cancelled)->fetch_add(1);
    }

    void prepare(Counter& f, Counter& c, poller::TimePoint deadline) {
        fired = &f;
        cancelled = &c;
        *payload = 7;
        timer.memo = this;
        timer.callback = &callback;
        timer.deadline = deadline;
    }
};

}  // namespace

// Three producer threads push into the timer MPSC queue while the poller
// drains it and sleeps between deadlines.
TEST(PollerTwist, TimerMpscFromThreeThreads) {
    check([] {
        Counter fired{0}, cancelled{0};
        InlineSink sink;
        CheckedTimer timers[6];
        Poller p(TaskSink::of(sink), tiny());
        p.start();
        auto produce = [&](int first) {
            for (int i = first; i < first + 2; ++i) {
                timers[i].prepare(fired, cancelled, p.now() + std::chrono::microseconds(20 * i));
                p.schedule(timers[i].timer);
            }
        };
        twist::ed::std::thread a(produce, 0);
        twist::ed::std::thread b(produce, 2);
        twist::ed::std::thread c(produce, 4);
        a.join();
        b.join();
        c.join();
        wait_until(fired, 6, "timer lost in the MPSC queue / poller never woke");
        p.stop();
        TWIST_ASSERT_M(fired.load() == 6 && cancelled.load() == 0, "each timer fires exactly once");
    });
}

// The poller goes to sleep until a far deadline while two producers schedule
// earlier ones: at least the earliest must wake it (wake-if-earlier, Dekker).
TEST(PollerTwist, WakeIfEarlierRace) {
    check([] {
        Counter fired{0}, cancelled{0};
        InlineSink sink;
        CheckedTimer far, near1, near2;
        Poller p(TaskSink::of(sink), tiny());
        far.prepare(fired, cancelled, p.now() + 1h);
        p.schedule(far.timer);
        p.start();
        twist::ed::std::thread t1([&] {
            near1.prepare(fired, cancelled, p.now() + 30us);
            p.schedule(near1.timer);
        });
        twist::ed::std::thread t2([&] {
            near2.prepare(fired, cancelled, p.now() + 10us);
            p.schedule(near2.timer);
        });
        t1.join();
        t2.join();
        wait_until(fired, 2, "lost wakeup: the poller slept through an earlier deadline");
        TWIST_ASSERT_M(p.cancel(far.timer), "far timer still pending");
        p.stop();
    });  // ~Poller delivers far as kCancelled
}

// cancel() racing with the deadline: exactly one callback, and cancel()'s
// answer matches it.
TEST(PollerTwist, CancelRace) {
    check([] {
        Counter fired{0}, cancelled{0}, poke_fired{0}, poke_cancelled{0};
        InlineSink sink;
        CheckedTimer t, poke;
        Poller p(TaskSink::of(sink), tiny());
        p.start();
        t.prepare(fired, cancelled, p.now() + 20us);
        p.schedule(t.timer);
        const bool ok = p.cancel(t.timer);
        // Make sure the poller iterates after the cancel (reclamation).
        poke.prepare(poke_fired, poke_cancelled, p.now());
        p.schedule(poke.timer);
        Counter& expected = ok ? cancelled : fired;
        wait_until(expected, 1, "timer neither fired nor released");
        p.stop();
        TWIST_ASSERT_M(fired.load() + cancelled.load() == 1, "exactly one callback");
    });
}

// stop() racing with producers and with the poller falling asleep. A lost
// stop wakeup deadlocks (the poller sleeps forever, we join it).
TEST(PollerTwist, StopRaces) {
    check([] {
        Counter fired{0}, cancelled{0};
        InlineSink sink;
        CheckedTimer timers[3];  // must outlive the poller that owns them
        {
            Poller p(TaskSink::of(sink), tiny());
            p.start();
            twist::ed::std::thread producer([&] {
                for (int i = 0; i < 3; ++i) {
                    timers[i].prepare(fired, cancelled, p.now() + std::chrono::microseconds(10 * i));
                    p.schedule(timers[i].timer);
                }
            });
            p.stop();  // may run before, during or after the producer
            producer.join();
            p.start();  // restart: pending timers continue
            p.stop();
        }  // ~Poller: whatever did not fire is delivered as kCancelled
        TWIST_ASSERT_M(fired.load() + cancelled.load() == 3, "every timer handed back exactly once");
    });
}

namespace {

struct Item {
    std::uint32_t index;
};

using TwistRing = ipc::Ring<Item, Sync>;
using Doorbell = ipc::FakeDoorbell<Backend>;
using Consumer = ipc::RingCheckable<Item, Sync, Doorbell, 2>;

struct RingState {
    static constexpr std::size_t kItems = 4;
    twist::assist::Shared<int> payload[kItems];
    Counter received{0};
    std::uint32_t expected = 0;  // poller thread only

    static void on_items(void* context, const Item* items, std::size_t count, TaskBatch&) noexcept {
        auto* self = static_cast<RingState*>(context);
        for (std::size_t i = 0; i < count; ++i) {
            TWIST_ASSERT_M(items[i].index == self->expected, "ring order");
            TWIST_ASSERT_M(self->payload[items[i].index].Read() == 1, "ring item published");
            ++self->expected;
        }
        self->received.fetch_add(count);
    }
};

}  // namespace

// The arm / re-check / sleep protocol: a producer thread pushes into an
// in-process ring (Twist atomics, probed slots) while the poller polls, arms
// (sleeping = 1 + re-check), sleeps on the fake eventfd and disarms.
TEST(PollerTwist, CheckableArmRaceWithRingProducer) {
    check([] {
        RingState state;
        InlineSink sink;
        const std::size_t bytes = TwistRing::bytes_for(2);
        void* memory = ::operator new(bytes, std::align_val_t{64});
        TwistRing ring = TwistRing::create(memory, bytes, 2);
        {
            Poller p(TaskSink::of(sink), tiny());
            Doorbell bell{&p.backend(), p.backend().create_event()};
            Consumer consumer(ring, bell, &RingState::on_items, &state);
            p.add(consumer.checkable());
            p.start();
            twist::ed::std::thread producer([&] {
                ipc::Producer<Item, Sync> out(ring);
                Doorbell doorbell = bell;
                Patience patience;
                for (std::uint32_t i = 0; i < RingState::kItems;) {
                    *state.payload[i] = 1;
                    const Item item{i};
                    if (out.push(&item, 1, doorbell) == 1) {
                        ++i;
                    } else {
                        patience.yield("ring stays full: lost wakeup of the consumer");
                    }
                }
            });
            producer.join();
            wait_until(state.received, RingState::kItems,
                       "lost wakeup: the consumer sleeps with items in the ring");
            p.stop();
            p.remove(consumer.checkable());
        }
        ring.destroy();
        ::operator delete(memory, std::align_val_t{64});
    });
}

// The same protocol with the stop flag racing an armed sleep.
TEST(PollerTwist, StopWhileArmed) {
    check([] {
        RingState state;
        InlineSink sink;
        const std::size_t bytes = TwistRing::bytes_for(2);
        void* memory = ::operator new(bytes, std::align_val_t{64});
        TwistRing ring = TwistRing::create(memory, bytes, 2);
        {
            Poller p(TaskSink::of(sink), tiny());
            Doorbell bell{&p.backend(), p.backend().create_event()};
            Consumer consumer(ring, bell, &RingState::on_items, &state);
            p.add(consumer.checkable());
            p.start();
            twist::ed::std::this_thread::yield();
            p.stop();
            TWIST_ASSERT_M(ring.header()->sleeping.load() == 0, "disarmed after waking");
            p.remove(consumer.checkable());
        }
        ring.destroy();
        ::operator delete(memory, std::align_val_t{64});
    });
}
