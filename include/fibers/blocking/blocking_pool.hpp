#pragma once

// BlockingPool: a small fixed set of threads for calls that block in the
// kernel (Tokio's spawn_blocking).
//
// Fibers must never block a worker thread of the main ThreadPool. Blocking
// syscalls (file I/O: pread / pwrite / open / fsync / stat, DNS: getaddrinfo,
// third-party blocking libraries) are handed to this pool instead, as
// caller-owned ops (ops.hpp): one of its threads runs the call, however long
// the disk takes, then hands the op's continuation to the main pool, which
// resumes the fiber.
//
// Threads. num_threads, default 2, fixed for the pool's lifetime. The main
// pool already has one worker per core; these threads sleep most of the time
// (in the blocking call, or parked waiting for work), so a small number keeps
// "threads <= cores" in spirit while still overlapping a couple of slow
// calls. More concurrent blocking calls than threads simply queue (the queue
// is unbounded). Possible future option, Tokio-style: grow on demand up to a
// cap while every thread is busy and ops are queued, and let threads idle for
// some time exit again; not implemented (fixed threads keep the pool
// allocation-free after construction and the wake protocol simple).
//
// Queue (op_queue.hpp). Intrusive Vyukov MPSC queue of ops: unbounded, no
// storage of its own, no allocation. Producers (fiber workers, any thread)
// never lock and never wait: a push is one exchange and one store. Pool
// threads pop under a short consumer-side lock (Sync::Mutex).
//
// submit(op) always succeeds. After stop() it drops the op instead: its
// continuation's delete_ptr runs and the continuation never runs (see
// "Submit after stop" below for why this, rather than returning false).
//
// Parking (as in ThreadPool, not a condition variable)
// ----------------------------------------------------
// Per-thread state word (Slot::state): RUNNING -> PARKING -> PARKED. Only the
// owner moves RUNNING -> PARKING -> PARKED; a waker (producer or stop())
// moves PARKING / PARKED -> RUNNING with an exchange, and calls notify_one
// only if the exchange returned PARKED. A thread still PARKING when a waker
// gets to it does not go to sleep at all (its CAS PARKING -> PARKED fails).
//
// idle_ = [ stop : 1 | count : 63 ]: count = threads between "announced
// parking" and "back to running" (an over-estimate while they wake up, never
// an under-estimate of the PARKING / PARKED ones); stop = stop() was called.
//
//   pool thread (P)                      producer (submit)
//   pop(); got an op -> run it, loop     push(op)  (exchange tail, link)
//   state.store(PARKING)       relaxed   fence(seq_cst)                (F1)
//   w = idle_.fetch_add(1)     seq_cst   w = idle_.load()          relaxed
//   fence(seq_cst)                  (F2) w == 0 -> done (the fast path)
//   if stop in w: leave                  stop in w -> drop queued ops
//   re-check: pop()                      else for each slot:
//   nothing -> CAS PARKING -> PARKED       state != RUNNING &&
//              ok: state.wait(PARKED)      (prev = state.exchange(RUNNING))
//   state.store(RUNNING), idle_ -= 1           != RUNNING:
//   run what the re-check popped, if any         prev == PARKED -> notify_one
//                                                stop: one wake per push
//
// Lost wakeup (Dekker): F1 and F2 are seq_cst fences, so one precedes the
// other in the single total order S. If F1 precedes F2, P's re-check (after
// F2) sees the push, linked before F1. If F2 precedes F1, the producer's
// load of idle_ (after F1) sees P's increment, and its scan sees P's PARKING
// (or later): its exchange, an RMW totally ordered with P's CAS on the same
// word, either makes the CAS fail (P does not sleep) or returns PARKED and
// notifies. If the scan finds every slot RUNNING, each of those threads is
// awake, and its next parking attempt announces itself after the producer's
// exchange / load read RUNNING, so its F2 follows F1 in S and its re-check
// sees the op. The exchange must be one RMW: a load of PARKING followed by
// a store of RUNNING could let P's CAS to PARKED slip in between and then
// overwrite PARKED without a notify. The fences are needed formally (the
// re-check and idle_ loads are relaxed). On x86 the RMWs next to them
// (the tail exchange, the idle_ increment) already drain the store buffer
// and the consumer waits for in-transit links (below), so there they are
// redundant; under the C++ model (and on ARM) they are not: GenMC -rc11
// finds the lost wakeup without either one.
//
// One wake per push, no thundering herd: a producer wakes at most one thread
// (the first non-RUNNING slot whose exchange it wins); n pushes into n
// sleeping threads wake n different threads. The fast path when nobody
// sleeps is the push, F1 and one load of idle_ (a line written only when
// threads park or wake). The slow path is out of line.
//
// The in-transit window (a producer exchanged tail_ but has not linked)
// ------------------------------------------------------------------------
// pop() (for work, in the re-check and in drains) never reports it as empty:
// it spins (Sync::spin_hint) until the link appears. The producer is two
// instructions from it; only if that thread is preempted right there does a
// pool thread spin for longer (holding the consumer lock; the other pool
// threads then sleep on it).
// Giving up would lose a wakeup. Every push wakes at most one thread, meant
// for that op. Say a thread was woken for op A, A is linked and at the head,
// but the link from A to the next op B is in transit: Vyukov's pop cannot
// return A until it sees A's successor. A thread that gave up and parked
// would leave A queued; B's producer then wakes ONE thread after linking,
// which takes A, and B stays queued while a thread sleeps, possibly for as
// long as A blocks (Twist finds this: two ops into two sleeping threads end
// up on one thread). The same holds for nodes linked behind an in-transit
// first node.
//
// Stop
// ----
// stop(): sets the stop bit (seq_cst RMW on idle_) and fences (F3), wakes
// every thread (exchange + notify, as above) and joins them: a thread that
// is running an op finishes it first, a thread that sees the bit exits
// without popping more. Then it drains the queue, dropping every op
// (BlockingOp::drop: the continuation's delete_ptr) exactly once (pops are
// serialised). Idempotent; concurrent calls are serialised. Must not be
// called from a thread of this pool (it would join itself).
//
// Submit after stop. submit() does not check the stop bit before pushing:
// it pushes, and the F1 load of idle_ it does anyway tells it whether stop()
// has begun. If so, it drains the queue itself (its own op included), so the
// op is dropped before submit() returns, or by stop()'s drain. The F1 / F3
// pair makes this exact: if F1 precedes F3 in S, stop()'s drain (after F3)
// sees the push; otherwise the producer sees the stop bit. Every op is
// therefore run, or dropped exactly once; none is left behind in the queue.
// Why drop rather than return false:
// * A lock-free push cannot check "stopped?" and enqueue atomically; an
//   exact false would need a producer-side RMW or lock on every submit.
//   Afterwards the only exact answer is "dropped".
// * Fibers (fiber/blocking.hpp) submit and then park. A false would have to
//   be handled on the submitting fiber, as a second path next to ops that
//   stop() drops after they were queued; with the drop, both go through the
//   continuation's delete_ptr (it wakes the fiber, which then runs the call
//   inline). One path instead of two, and no retry loop anywhere.
// * ThreadPool also drops (delete_ptr) what it will not run.
// The drop runs on the submitting thread (delete_ptr is user code).
//
// No allocation after construction, no exceptions: the constructor's
// allocations / thread creation terminate the process on failure (noexcept).
// No thread-locals. All synchronisation goes through the Sync policy.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "fibers/blocking/op_queue.hpp"
#include "fibers/blocking/ops.hpp"
#include "fibers/sync/affinity.hpp"
#include "fibers/sync/sync.hpp"

namespace fibers::blocking {

struct BlockingPoolOptions {
    std::size_t num_threads = 2;  // minimum 1
    // Pin thread i to cpus[i % cpus.size()] (empty: no pinning).
    sync::CpuList cpus{};
};

template <sync::ParkingSyncPolicy Sync = sync::StdSync>
class BlockingPool {
public:
    using Op = BasicBlockingOp<Sync>;

    explicit BlockingPool(BlockingPoolOptions options = {}) noexcept
        : num_threads_(std::max<std::size_t>(options.num_threads, 1)),
          slots_(std::make_unique<Slot[]>(num_threads_)) {
        threads_.reserve(num_threads_);
        for (std::size_t i = 0; i < num_threads_; ++i) {
            threads_.emplace_back([this, i, cpu = options.cpus.for_thread(i)] {
                sync::pin_current_thread<Sync>(cpu, "BlockingPool");
                thread_loop(i);
            });
        }
    }

    BlockingPool(const BlockingPool&) = delete;
    BlockingPool& operator=(const BlockingPool&) = delete;

    ~BlockingPool() { stop(); }

    // Queues `op` for a pool thread; wakes one sleeping thread if any. Never
    // blocks, never fails, never allocates; callable from any thread (fiber
    // workers and this pool's own threads included). After stop() (or racing
    // with it) the op is dropped instead: see the top comment. Precondition:
    // `op` is not queued or running already.
    void submit(Op& op) noexcept {
        queue_.push(op);
        Sync::atomic_thread_fence(std::memory_order_seq_cst);  // F1
        const std::uint64_t idle = idle_.load(std::memory_order_relaxed);
        if (idle != 0) [[unlikely]] {
            after_push_slow(idle);
        }
    }

    // See the top comment. Precondition: not called from a pool thread.
    void stop() noexcept {
        std::lock_guard<typename Sync::Mutex> serialise(stop_mutex_);
        idle_.fetch_or(kStopBit, std::memory_order_seq_cst);
        Sync::atomic_thread_fence(std::memory_order_seq_cst);  // F3
        for (std::size_t i = 0; i < num_threads_; ++i) {
            unpark(slots_[i]);
        }
        for (auto& thread : threads_) {
            thread.join();
        }
        threads_.clear();
        drain();
    }

    std::size_t num_threads() const noexcept { return num_threads_; }

    // Diagnostics (racy snapshots, for tests and monitoring).
    // Threads currently asleep (state PARKED).
    std::size_t sleeping_threads() const noexcept {
        std::size_t n = 0;
        for (std::size_t i = 0; i < num_threads_; ++i) {
            n += slots_[i].state.load(std::memory_order_relaxed) == kParked ? 1 : 0;
        }
        return n;
    }
    // Times a thread went to sleep since construction.
    std::uint64_t sleeps() const noexcept {
        std::uint64_t n = 0;
        for (std::size_t i = 0; i < num_threads_; ++i) {
            n += slots_[i].sleeps.load(std::memory_order_relaxed);
        }
        return n;
    }
    bool stop_requested() const noexcept { return (idle_.load(std::memory_order_acquire) & kStopBit) != 0; }

private:
    static constexpr std::uint32_t kRunning = 0;
    static constexpr std::uint32_t kParking = 1;
    static constexpr std::uint32_t kParked = 2;

    static constexpr std::uint64_t kStopBit = std::uint64_t{1} << 63;

    struct alignas(sync::kCacheLineSize) Slot {
        typename Sync::template Atomic<std::uint32_t> state{kRunning};
        typename Sync::template Atomic<std::uint64_t> sleeps{0};  // owner-written counter
    };

    void thread_loop(std::size_t index) noexcept {
        Slot& self = slots_[index];
        for (;;) {
            if (stop_requested()) {
                return;
            }
            if (Op* op = queue_.pop()) {
                op->run();
                continue;
            }
            Op* op = park(self);
            if (op != nullptr) {
                op->run();
            }
        }
    }

    // Announces parking, re-checks the queue and sleeps unless it found an op
    // (returned, to be run), a waker got here first, or stop was requested.
    Op* park(Slot& self) noexcept {
        self.state.store(kParking, std::memory_order_relaxed);
        const std::uint64_t idle = idle_.fetch_add(1, std::memory_order_seq_cst);
        Sync::atomic_thread_fence(std::memory_order_seq_cst);  // F2
        Op* op = nullptr;
        if ((idle & kStopBit) == 0) {
            op = queue_.pop();  // re-check
            if (op == nullptr) {
                std::uint32_t expected = kParking;
                if (self.state.compare_exchange_strong(expected, kParked, std::memory_order_seq_cst,
                                                       std::memory_order_acquire)) {
                    self.sleeps.store(self.sleeps.load(std::memory_order_relaxed) + 1,
                                      std::memory_order_relaxed);
                    self.state.wait(kParked, std::memory_order_acquire);  // until != PARKED
                }
                // else: a waker moved us to RUNNING while PARKING.
            }
        }
        self.state.store(kRunning, std::memory_order_relaxed);
        idle_.fetch_sub(1, std::memory_order_relaxed);
        return op;
    }

    // Moves `slot` out of PARKING / PARKED, notifying it if it sleeps.
    // Returns true if it was not RUNNING. The caller has published its work
    // (or the stop bit) and executed a seq_cst fence before.
    static bool unpark(Slot& slot) noexcept {
        if (slot.state.load(std::memory_order_relaxed) == kRunning) {
            return false;
        }
        const std::uint32_t prev = slot.state.exchange(kRunning, std::memory_order_seq_cst);
        if (prev == kParked) {
            slot.state.notify_one();
        }
        return prev != kRunning;
    }

    // Out of line: keeps submit()'s fast path small.
    [[gnu::noinline]] void after_push_slow(std::uint64_t idle) noexcept {
        if ((idle & kStopBit) != 0) {
            drain();
            return;
        }
        for (std::size_t i = 0; i < num_threads_; ++i) {
            if (unpark(slots_[i])) {
                return;  // one wake per push
            }
        }
    }

    // Drops every queued op. The drop runs outside the consumer lock
    // (delete_ptr is user code; it may even submit again).
    void drain() noexcept {
        while (Op* op = queue_.pop()) {
            op->drop();
        }
    }

    const std::size_t num_threads_;
    const std::unique_ptr<Slot[]> slots_;
    detail::OpQueue<Sync> queue_;
    // Read by every submit (F1 load), written only when threads park / wake
    // and by stop(): its own cache line.
    alignas(sync::kCacheLineSize) typename Sync::template Atomic<std::uint64_t> idle_{0};

    typename Sync::Mutex stop_mutex_;  // serialises stop()
    std::vector<typename Sync::Thread> threads_;
};

}  // namespace fibers::blocking
