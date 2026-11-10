// fiber::WaitGroup: wait for a group of fibers (Go's sync.WaitGroup).
//
//   wait_group_fanout [--workers=4] [--fibers=16] [--numbers=1000000] [--serve-ms=20]
//
// What it is for: "start N things, continue when all of them are done":
// fan-out / fan-in of a computation, joining the fibers of a request, and
// draining in-flight work on shutdown.
//
//   wg.go(f)      add(1), spawn a fiber running f, done() after f (and its
//                 captures) are gone. Nothing is allocated. In a fiber:
//                 on its scheduler; anywhere: wg.go(scheduler, f).
//   wg.add(n)     count += n           (any thread)
//   wg.done()     count -= 1           (any thread; the last one wakes the waiters)
//   wg.wait()     parks the calling fiber until the count is zero (a plain
//                 thread, like main here: blocks the thread)
//
// On the hot path add() / done() are one atomic RMW each; wait() puts a node
// on the waiting fiber's stack and parks it (its worker runs other fibers).
//
// Key rules:
//   * Everything a fiber did before its done() is visible after wait()
//     returns: results can go into plain variables, no extra locking.
//   * add() BEFORE the work can finish: before spawning the fiber, never
//     inside it (otherwise wait() may see zero too early).
//   * One-shot: wait() closes the group; it finishes the next time the count
//     reaches zero, and add() after that fails fast. While the count is still
//     above zero, add() is fine: a request in flight may add its children
//     during the shutdown join, because it holds a count itself.
//   * wait() in a fiber parks the fiber; on a plain thread (main) it blocks
//     the thread: never do that from a plain callback running on a worker.
//     try_wait() never waits.
//
// Both parts run on main, a plain thread: wg.go(scheduler, f) spawns onto
// the scheduler, wg.wait() blocks main. Part 1: fan-out / fan-in, a sum
// computed in slices by N fibers. Part 2: a toy server. An acceptor spawns
// a request fiber every 200 us for --serve-ms; some requests spawn a child
// (an "audit write") that outlives them. Shutdown stops the acceptor and
// calls wait() while requests and children are still in flight.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>

#include "../common.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/wait_group.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace this_fiber = fibers::fiber::this_fiber;
using fibers::sched::Scheduler;
using namespace std::chrono_literals;

namespace {

double ms_since(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
}

struct Server {
    fiber::WaitGroup in_flight;  // the acceptor + every request + every child
    std::atomic<bool> stopping{false};
    std::atomic<std::uint64_t> started{0}, finished{0};
    std::atomic<std::uint64_t> children_started{0}, children_finished{0};

    void handle(std::uint64_t id) {
        this_fiber::sleep_for(std::chrono::microseconds(500 + id * 2654435761u % 3000));  // the "work"
        if (id % 4 == 0) {
            // A child that outlives this request: add() for it NOW, while
            // this request still holds its own count, so the group cannot
            // reach zero in between, even if wait() has already started.
            children_started.fetch_add(1, std::memory_order_relaxed);
            in_flight.go([this]() noexcept {
                this_fiber::sleep_for(2ms);
                children_finished.fetch_add(1, std::memory_order_relaxed);
            });
        }
        finished.fetch_add(1, std::memory_order_relaxed);
    }
};

}  // namespace

int main(int argc, char** argv) {
    const examples::Args args(argc, argv);
    const auto workers = static_cast<std::size_t>(args.integer("workers", 4));
    const auto num_fibers = static_cast<std::uint64_t>(args.integer("fibers", 16));
    const auto numbers = static_cast<std::uint64_t>(args.integer("numbers", 1'000'000));
    const auto serve_ms = args.integer("serve-ms", 20);

    fibers::sched::SchedulerOptions options;
    options.pool.num_workers = workers;
    options.blocking.num_threads = 1;
    Scheduler scheduler(options);

    int status = 0;

    // ---- part 1: fan-out / fan-in ----
    {
        const auto start = std::chrono::steady_clock::now();
        std::vector<std::uint64_t> partial(num_fibers);  // slot i is written only by fiber i
        fiber::WaitGroup wg;
        for (std::uint64_t i = 0; i < num_fibers; ++i) {
            wg.go(scheduler, [&, i]() noexcept {
                const std::uint64_t lo = numbers * i / num_fibers + 1, hi = numbers * (i + 1) / num_fibers;
                std::uint64_t sum = 0;
                for (std::uint64_t n = lo; n <= hi; ++n) {
                    sum += n;
                }
                partial[i] = sum;  // a plain write: done() publishes it
            });
        }
        wg.wait();  // main blocks; every partial[i] is visible afterwards
        std::uint64_t total = 0;
        for (const std::uint64_t p : partial) {
            total += p;
        }
        const std::uint64_t expected = numbers * (numbers + 1) / 2;
        std::printf("fan-out: %llu fibers summed 1..%llu in slices: %llu (expected %llu), %.2f ms\n",
                    static_cast<unsigned long long>(num_fibers), static_cast<unsigned long long>(numbers),
                    static_cast<unsigned long long>(total), static_cast<unsigned long long>(expected),
                    ms_since(start));
        status |= total == expected ? 0 : 1;
    }

    // ---- part 2: graceful shutdown of a toy server ----
    {
        Server server;
        // The acceptor is in the group too: while it runs, the count is at
        // least 1, so its add()s are always legal, even during wait().
        server.in_flight.go(scheduler, [&server]() noexcept {
            for (std::uint64_t id = 0; !server.stopping.load(std::memory_order_relaxed); ++id) {
                server.started.fetch_add(1, std::memory_order_relaxed);
                server.in_flight.go([&server, id]() noexcept { server.handle(id); });
                this_fiber::sleep_for(200us);  // the next "connection"
            }
        });

        std::this_thread::sleep_for(std::chrono::milliseconds(serve_ms));
        server.stopping.store(true, std::memory_order_relaxed);  // stop accepting
        const auto start = std::chrono::steady_clock::now();
        const std::uint64_t requests_open = server.started.load() - server.finished.load();
        const std::uint64_t children_open = server.children_started.load() - server.children_finished.load();
        server.in_flight.wait();  // the acceptor exits, the requests and their children drain

        std::printf("server: ran %lld ms; at shutdown ~%llu requests and ~%llu children were in flight\n",
                    static_cast<long long>(serve_ms), static_cast<unsigned long long>(requests_open),
                    static_cast<unsigned long long>(children_open));
        std::printf("server: wait() returned after %.1f ms: %llu/%llu requests and %llu/%llu children finished\n",
                    ms_since(start), static_cast<unsigned long long>(server.finished.load()),
                    static_cast<unsigned long long>(server.started.load()),
                    static_cast<unsigned long long>(server.children_finished.load()),
                    static_cast<unsigned long long>(server.children_started.load()));
        status |= server.finished.load() == server.started.load() &&
                          server.children_finished.load() == server.children_started.load()
                      ? 0
                      : 1;
    }

    scheduler.stop();
    return status;
}
