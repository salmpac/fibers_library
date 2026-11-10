// fiber::Mutex: mutual exclusion between fibers.
//
//   mutex_counter [--workers=4] [--fibers=100] [--increments=1000] [--yield-every=100]
//
// What it is for: shared state that several fibers modify, where a critical
// section may itself wait (yield, sleep, I/O, another lock). The waiting
// unit is the FIBER, not the worker thread:
//
//   * lock() fast path: one CAS on the mutex word, nothing else.
//   * lock() slow path: the fiber puts a wait node on its own stack, pushes
//     it onto the mutex and parks. Its worker thread goes on running other
//     fibers. No allocation, no syscall.
//   * unlock(): one CAS; if somebody waits, one of them is woken (submitted
//     to a worker queue). The mutex is barging: a running fiber may take the
//     lock before the woken one gets there (throughput over strict FIFO).
//
// Key rules:
//   * Use it with std::lock_guard / std::unique_lock like any mutex.
//   * A fiber holding a fiber::Mutex MAY park: yield, sleep_for, socket I/O,
//     this_fiber::blocking. Contenders park too, the workers stay free.
//   * NEVER hold a std::mutex (or a spin lock) across a park / sleep / I/O.
//     The holder is descheduled with the lock held; a contender on another
//     fiber then blocks its whole worker thread in std::mutex::lock(), and
//     with few workers nobody is left to resume the holder: deadlock. And
//     the holder may resume on a different thread, unlocking a std::mutex
//     from a thread that does not own it (undefined behaviour). std::mutex
//     is fine only for short sections that never wait.
//   * lock() must be called from a fiber (it fails fast if it would have to
//     wait on a plain thread); try_lock() / unlock() work anywhere.
//
// Part 1: N fibers x M increments under the mutex, yielding INSIDE the
// critical section now and then; an invariant (a == b) is checked on every
// entry. Part 2: a fiber sleeps 20 ms holding the lock while 2 x workers
// fibers wait for it and an unrelated ticker fiber keeps running: if lock()
// blocked threads, the contenders would occupy every worker and the ticker
// would never run. main (a plain thread) never touches the mutex: it spawns
// the fibers with WaitGroup::go(scheduler, f) and joins them with wait(),
// which blocks a plain thread.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <mutex>

#include "../common.hpp"
#include "fibers/fiber/event.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/mutex.hpp"
#include "fibers/fiber/wait_group.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace this_fiber = fibers::fiber::this_fiber;
using fibers::sched::Scheduler;
using namespace std::chrono_literals;

namespace {

struct Counter {
    fiber::Mutex mutex;
    // Guarded by mutex. The critical section increments a, maybe yields,
    // then increments b: whoever enters must always see a == b.
    std::uint64_t a = 0;
    std::uint64_t b = 0;
    std::uint64_t violations = 0;
};

}  // namespace

int main(int argc, char** argv) {
    const examples::Args args(argc, argv);
    const auto workers = static_cast<std::size_t>(args.integer("workers", 4));
    const auto num_fibers = static_cast<std::uint64_t>(args.integer("fibers", 100));
    const auto increments = static_cast<std::uint64_t>(args.integer("increments", 1000));
    const auto yield_every = static_cast<std::uint64_t>(args.integer("yield-every", 100));

    fibers::sched::SchedulerOptions options;
    options.pool.num_workers = workers;
    options.blocking.num_threads = 1;
    Scheduler scheduler(options);

    Counter counter;
    std::uint64_t contenders_served = 0;  // guarded by counter.mutex
    std::atomic<int> ticks{0};

    // main() is a plain thread: it must not lock a fiber::Mutex (lock()
    // fails fast if it would have to wait outside a fiber), so only fibers
    // touch the counter. main joins them with WaitGroup::wait(), which blocks
    // a plain thread; everything the fibers wrote before their done() is
    // visible after it.
    // ---- part 1: many fibers, one counter ----
    const auto start = std::chrono::steady_clock::now();
    {
        fiber::WaitGroup wg;
        for (std::uint64_t f = 0; f < num_fibers; ++f) {
            wg.go(scheduler, [&]() noexcept {
                for (std::uint64_t i = 1; i <= increments; ++i) {
                    std::lock_guard guard(counter.mutex);
                    if (counter.a != counter.b) {
                        ++counter.violations;
                    }
                    ++counter.a;
                    if (yield_every != 0 && i % yield_every == 0) {
                        // Descheduled WITH the lock held: other fibers that
                        // want it park, the worker runs the rest.
                        this_fiber::yield();
                    }
                    ++counter.b;
                }
            });
        }
        wg.wait();  // main blocks until every fiber is done
    }
    const double part1_ms =
        std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();

    // ---- part 2: the holder sleeps, the workers stay free ----
    {
        fiber::Event locked;
        std::atomic<bool> released{false};
        fiber::WaitGroup wg;
        wg.go(scheduler, [&]() noexcept {
            std::lock_guard guard(counter.mutex);
            locked.set();
            this_fiber::sleep_for(20ms);  // parks holding the lock: fine for fiber::Mutex
            released.store(true, std::memory_order_relaxed);
        });
        for (std::size_t c = 0; c < 2 * workers; ++c) {
            wg.go(scheduler, [&]() noexcept {
                locked.wait();
                std::lock_guard guard(counter.mutex);  // parks ~20 ms, its worker does not
                ++contenders_served;
            });
        }
        wg.go(scheduler, [&]() noexcept {  // never touches the mutex
            locked.wait();
            while (!released.load(std::memory_order_relaxed)) {
                this_fiber::sleep_for(1ms);
                ticks.fetch_add(1, std::memory_order_relaxed);
            }
        });
        wg.wait();
    }
    scheduler.stop();  // every fiber has finished: nothing is abandoned

    const std::uint64_t expected = num_fibers * increments;
    std::printf("mutex_counter: %zu workers, %llu fibers x %llu increments, a yield inside the lock every %llu\n",
                workers, static_cast<unsigned long long>(num_fibers), static_cast<unsigned long long>(increments),
                static_cast<unsigned long long>(yield_every));
    std::printf("  counter = %llu (expected %llu), invariant violations: %llu, %.1f ms\n",
                static_cast<unsigned long long>(counter.b), static_cast<unsigned long long>(expected),
                static_cast<unsigned long long>(counter.violations), part1_ms);
    std::printf("  a fiber slept 20 ms holding the lock: %llu contenders parked on it (more than %zu workers),\n"
                "  and a ticker fiber still ran %d times meanwhile\n",
                static_cast<unsigned long long>(contenders_served), workers, ticks.load());
    const bool ok = counter.b == expected && counter.violations == 0 && contenders_served == 2 * workers;
    return ok ? 0 : 1;
}
