// fiber::Event: a one-shot broadcast ("start gun", "config loaded",
// "shutting down").
//
//   event_broadcast [--workers=4] [--runners=100]
//
//   fiber::Event start;
//   start.wait();   // in fibers: parks until set(); returns at once if already set
//   start.set();    // anywhere (fiber, plain thread, callback): wakes every waiter
//
// Hot path: wait() on a set event is one atomic load. Otherwise the fiber
// pushes a node on its own stack onto the event and parks (its worker runs
// other fibers). set() is one atomic exchange, then one wake (a submit to a
// worker queue) per parked fiber.
//
// Key rules:
//   * One-shot: it cannot be reset. A second set() is a no-op (returns
//     false). For repeated signals use a new Event per round.
//   * Everything done before the first set() is visible after wait().
//   * wait() must run in a fiber unless the event is already set (a plain
//     thread that would have to wait fails fast).
//   * Destroy it only after every wait() has returned.
//
// Here N runner fibers park on one event, and main (a plain thread) fires it.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <thread>

#include "../common.hpp"
#include "fibers/fiber/event.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/oneshot.hpp"
#include "fibers/fiber/wait_group.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
using fibers::sched::Scheduler;
using namespace std::chrono_literals;

namespace {

void store_min(std::atomic<std::int64_t>& a, std::int64_t v) {
    for (std::int64_t seen = a.load(); v < seen && !a.compare_exchange_weak(seen, v);) {
    }
}

void store_max(std::atomic<std::int64_t>& a, std::int64_t v) {
    for (std::int64_t seen = a.load(); v > seen && !a.compare_exchange_weak(seen, v);) {
    }
}

}  // namespace

int main(int argc, char** argv) {
    const examples::Args args(argc, argv);
    const auto workers = static_cast<std::size_t>(args.integer("workers", 4));
    const auto runners = static_cast<int>(args.integer("runners", 100));

    fibers::sched::SchedulerOptions options;
    options.pool.num_workers = workers;
    options.blocking.num_threads = 1;
    Scheduler scheduler(options);

    fiber::Event start;
    std::atomic<int> at_the_line{0};
    std::chrono::steady_clock::time_point fired;  // written before set(): every runner sees it after wait()
    std::atomic<std::int64_t> first_ns{INT64_MAX}, last_ns{0};

    fiber::Oneshot<bool> late_wait;  // main blocks on it until the root fiber is done
    fiber::go(scheduler, [&, done = late_wait.promise()]() mutable noexcept {
        fiber::WaitGroup wg;
        for (int r = 0; r < runners; ++r) {
            wg.go([&]() noexcept {
                at_the_line.fetch_add(1, std::memory_order_relaxed);
                start.wait();  // parks: no worker is held while the runners wait
                const std::int64_t ns = (std::chrono::steady_clock::now() - fired).count();
                store_min(first_ns, ns);
                store_max(last_ns, ns);
            });
        }
        wg.wait();
        start.wait();  // a late waiter: the event is set, this returns at once
        done.set(start.is_set());
    });

    // Fire from main (Event::set() works from any thread) once every runner
    // is at the line (some may still be on their way into wait(): fine,
    // wait() on a set event returns at once).
    while (at_the_line.load(std::memory_order_relaxed) < runners) {
        std::this_thread::sleep_for(100us);
    }
    fired = std::chrono::steady_clock::now();
    const bool first_set = start.set();   // wakes every parked runner
    const bool second_set = start.set();  // a no-op: false
    const fiber::Result<bool> late_ok = late_wait.get();
    scheduler.stop();

    std::printf("event_broadcast: %d runners on %zu workers waited on one Event; main fired it\n", runners,
                workers);
    std::printf("  every runner was running again within %.3f ms (the first after %.3f ms)\n",
                static_cast<double>(last_ns.load()) / 1e6, static_cast<double>(first_ns.load()) / 1e6);
    std::printf("  set() returned %d, the second set() %d; a late wait() returned at once: %d\n", first_set,
                second_set, late_ok && *late_ok);
    return first_set && !second_set && late_ok && *late_ok ? 0 : 1;
}
