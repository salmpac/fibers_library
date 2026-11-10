// fiber::Semaphore: a bounded resource pool, fair (FIFO) under contention.
//
//   semaphore_pool [--workers=4] [--connections=3] [--clients=12] [--queries=4]
//
// What it is for: "at most K at a time": K database connections, K
// concurrent uploads, a byte budget (multi-permit acquire), backpressure
// between plain threads and fibers.
//
//   fiber::Semaphore sem(K);
//   { fiber::SemaphoreGuard permit(sem); ... }        // acquire(1) ... release(1)
//   fiber::SemaphoreGuard big(sem, 3);                // 3 permits at once
//   fiber::SemaphoreGuard t(sem, 1, std::try_to_lock); if (!t) reject();
//
// Hot path: an uncontended acquire / release is one atomic RMW. A fiber that
// has to wait queues a node on its own stack and parks (its worker runs
// other fibers); release(n) hands the permits DIRECTLY to the waiters at the
// head of the queue, in arrival order, and wakes them.
//
// Key rules:
//   * FIFO, no overtaking: while anybody waits, even try_acquire() fails.
//     A multi-permit request at the head holds smaller ones behind it until
//     enough permits are free, so big requests never starve.
//   * A semaphore counts permits, it does not say WHICH resource you got:
//     pair it with a free list (here: a CAS over "busy" flags).
//   * Permits are not owned: any fiber or thread may release them, and a
//     SemaphoreGuard may be moved to another fiber.
//   * acquire(n) with n larger than the capacity waits forever.
//   * acquire() in a fiber parks the fiber; on a plain thread it blocks the
//     thread (never do that from a plain callback on a worker).
//
// Part 1: clients compete for K connections; part 2: fail-fast probes with
// try_to_lock while the pool is saturated; part 3: the queue order with
// mixed request sizes. main (a plain thread) drives all three: it spawns
// the fibers with WaitGroup::go(scheduler, f) and joins them with wait(),
// which blocks a plain thread.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <thread>
#include <vector>

#include "../common.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/semaphore.hpp"
#include "fibers/fiber/wait_group.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace this_fiber = fibers::fiber::this_fiber;
using fibers::sched::Scheduler;
using namespace std::chrono_literals;

namespace {

struct ConnectionPool {
    explicit ConnectionPool(std::size_t size) : permits(size), busy(size), uses(size) {}

    // Holding a permit guarantees a free connection exists; find and claim it.
    std::size_t claim() {
        for (;;) {
            for (std::size_t i = 0; i < busy.size(); ++i) {
                bool expected = false;
                if (busy[i].compare_exchange_strong(expected, true, std::memory_order_acquire)) {
                    return i;
                }
            }
        }
    }
    void unclaim(std::size_t i) { busy[i].store(false, std::memory_order_release); }

    fiber::Semaphore permits;
    std::vector<std::atomic<bool>> busy;
    std::vector<std::atomic<int>> uses;
    std::atomic<int> in_use{0}, max_in_use{0};
};

}  // namespace

int main(int argc, char** argv) {
    const examples::Args args(argc, argv);
    const auto workers = static_cast<std::size_t>(args.integer("workers", 4));
    const auto connections = static_cast<std::size_t>(args.integer("connections", 3));
    const auto clients = static_cast<int>(args.integer("clients", 12));
    const auto queries = static_cast<int>(args.integer("queries", 4));

    fibers::sched::SchedulerOptions options;
    options.pool.num_workers = workers;
    options.blocking.num_threads = 1;
    Scheduler scheduler(options);

    int status = 0;

    // ---- parts 1 and 2: K connections, many clients, fail-fast probes ----
    {
        ConnectionPool pool(connections);
        std::atomic<int> probes{0}, rejected{0};
        fiber::WaitGroup clients_wg;
        for (int c = 0; c < clients; ++c) {
            clients_wg.go(scheduler, [&]() noexcept {
                for (int q = 0; q < queries; ++q) {
                    fiber::SemaphoreGuard permit(pool.permits);  // parks while all K are taken
                    const std::size_t conn = pool.claim();
                    const int now = pool.in_use.fetch_add(1) + 1;
                    for (int seen = pool.max_in_use.load(); now > seen;) {
                        pool.max_in_use.compare_exchange_weak(seen, now);
                    }
                    pool.uses[conn].fetch_add(1, std::memory_order_relaxed);
                    this_fiber::sleep_for(1ms);  // the "query", holding the connection
                    pool.in_use.fetch_sub(1);
                    pool.unclaim(conn);
                }  // ~SemaphoreGuard: release(1), handed to the next waiter
            });
        }
        // A health check that must not queue behind the clients: it
        // either gets a connection right now or reports "busy".
        std::atomic<bool> clients_done{false};
        fiber::WaitGroup probe_wg;
        probe_wg.go(scheduler, [&]() noexcept {
            while (!clients_done.load(std::memory_order_relaxed)) {
                probes.fetch_add(1, std::memory_order_relaxed);
                {
                    fiber::SemaphoreGuard permit(pool.permits, 1, std::try_to_lock);
                    if (permit) {
                        pool.unclaim(pool.claim());  // "ping" a connection
                    } else {
                        rejected.fetch_add(1, std::memory_order_relaxed);
                    }
                }
                this_fiber::sleep_for(500us);
            }
        });
        clients_wg.wait();
        clients_done.store(true, std::memory_order_relaxed);
        probe_wg.wait();
        // The pool is idle now: the same probe succeeds at once.
        const fiber::SemaphoreGuard idle(pool.permits, 1, std::try_to_lock);

        std::printf("pool: %zu connections, %d clients x %d queries\n", connections, clients, queries);
        std::printf("  max connections in use at once: %d (limit %zu); queries per connection:",
                    pool.max_in_use.load(), connections);
        int total = 0;
        for (std::size_t i = 0; i < connections; ++i) {
            std::printf(" c%zu=%d", i, pool.uses[i].load());
            total += pool.uses[i].load();
        }
        std::printf("\n  fail-fast probes (try_to_lock) while the clients ran: %d tried, %d rejected as busy;"
                    " on the idle pool: %s\n",
                    probes.load(), rejected.load(), idle ? "got one" : "rejected");
        const bool ok = pool.max_in_use.load() <= static_cast<int>(connections) && total == clients * queries && idle;
        status |= ok ? 0 : 1;
    }

    // ---- part 3: FIFO hand-off with mixed request sizes ----
    {
        fiber::Semaphore sem(3);
        constexpr int kWaiters = 5;
        constexpr std::uint64_t kWant[kWaiters] = {1, 3, 1, 1, 2};
        std::chrono::steady_clock::time_point released;  // set before release(3): waiters see it
        double granted_ms[kWaiters] = {};
        std::atomic<int> sequence{0};
        int grant_seq[kWaiters] = {};

        sem.acquire(3);  // main holds everything while the queue forms (free: no wait)
        fiber::WaitGroup wg;
        for (int w = 0; w < kWaiters; ++w) {
            wg.go(scheduler, [&, w]() noexcept {
                fiber::SemaphoreGuard permits(sem, kWant[w]);  // queues in spawn order
                const auto waited = std::chrono::steady_clock::now() - released;
                granted_ms[w] = std::chrono::duration<double, std::milli>(waited).count();
                grant_seq[w] = sequence.fetch_add(1);
                this_fiber::sleep_for(2ms);  // hold them a little
            });
            // Let waiter w queue up before w + 1 arrives. A timing
            // assumption: on a badly overloaded machine the arrival
            // order can differ, so the order is printed, not checked.
            std::this_thread::sleep_for(1ms);
        }
        released = std::chrono::steady_clock::now();
        sem.release(3);  // w0 gets 1; w1 needs 3 and only 2 are free: everybody waits behind w1
        wg.wait();

        std::printf("fairness: 3 permits, all held while 5 fibers queue: w0(1) w1(3) w2(1) w3(1) w4(2);\n"
                    "  after the holder released them (each waiter keeps its permits 2 ms):\n ");
        int overtook_w1 = 0;
        for (int w = 0; w < kWaiters; ++w) {
            std::printf(" w%d +%.1f ms%s", w, granted_ms[w], w + 1 < kWaiters ? "," : "\n");
            overtook_w1 += w > 1 && grant_seq[w] < grant_seq[1] ? 1 : 0;
        }
        std::printf("  later arrivals that overtook the 3-permit request w1: %d\n", overtook_w1);
    }
    scheduler.stop();
    return status;
}
