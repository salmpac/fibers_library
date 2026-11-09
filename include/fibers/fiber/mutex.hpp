#pragma once

// Mutex for stackful fibers: the stackful front-end of wait::MutexCore
// (barging; see mutex_core.hpp for the algorithm and its memory orders).
//
//   fiber::Mutex mutex;
//   ... in a fiber:
//   std::lock_guard guard(mutex);   // or std::unique_lock, try_lock()
//
// lock() parks the fiber, not the worker thread: the slow path puts a
// WaitNode on the fiber's own stack, publishes it with the core's push CAS
// and parks. A wake that arrives before the fiber has left its stack is
// absorbed by the fiber's state word (NOTIFIED: the trampoline resumes it at
// once), so no post-switch hook is needed. With spins > 0 (see BasicMutex)
// the first attempt spins before parking. After a wake the fiber makes one
// attempt without spinning (it may be overtaken and queue again).
//
// lock() must be called from a fiber of a scheduler of type Sched: when it
// has to wait outside such a fiber it fails fast (it never blocks a thread).
// try_lock() and unlock() work from anywhere (unlock by the holder).
// unlock() wakes at most one fiber: a submit into the waker's pool queue.

#include <cstddef>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/wait/mutex_core.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fibers::fiber {

template <FiberScheduler Sched = sched::Scheduler>
class BasicMutex {
public:
    // Spin rounds (test-and-test-and-set with a pause) before parking,
    // only when the scheduler has more than one worker: the holder can then
    // be running on another thread and release soon; on one worker it cannot
    // run while we spin. 0 disables spinning. Only the first attempt of a lock()
    // spins: after a wake it makes one attempt (clearing WOKEN, see
    // mutex_core.hpp) and parks again if it loses.
    //
    // Default 0: in BM_MutexContention / BM_MutexWithWork (8-512 fibers on
    // 4-8 workers, see fiber_bench.cpp) no spin budget from 16 to 1024 beat
    // parking at once by more than noise; most lost, by up to 8x: spinning
    // waiters pull the line away from the holder's core and overtake the
    // woken waiter, while without spinning the lock stays on one core and
    // WOKEN keeps the other waiters parked.
    static constexpr std::size_t kDefaultSpins = 0;

    BasicMutex() noexcept = default;
    explicit BasicMutex(std::size_t spins) noexcept : spins_(spins) {}
    BasicMutex(const BasicMutex&) = delete;
    BasicMutex& operator=(const BasicMutex&) = delete;

    void lock() noexcept {
        if (core_.try_lock()) [[likely]] {
            return;
        }
        lock_slow();
    }

    bool try_lock() noexcept { return core_.try_lock(); }

    void unlock() noexcept { core_.unlock(); }

private:
    [[gnu::noinline]] void lock_slow() noexcept {
        BasicFiber<Sched>& self = BasicFiber<Sched>::current_or_die("fiber: Mutex::lock() outside a fiber");
        if (spins_ != 0 && self.scheduler().pool().num_workers() > 1 && core_.try_lock_spinning(spins_)) {
            return;
        }
        wait::WaitNode node = self.wait_node();
        // After a wake the attempt is a woken one: it clears WOKEN. (A woken
        // fiber must not spin: try_lock() would take the lock leaving WOKEN
        // set, and nobody would wake the remaining waiters.)
        for (bool woken = false; !core_.lock_or_enqueue(node, woken); woken = true) {
            self.park();
        }
    }

    wait::MutexCore<typename Sched::SyncPolicy> core_;
    std::size_t spins_ = kDefaultSpins;
};

using Mutex = BasicMutex<sched::Scheduler>;

}  // namespace fibers::fiber
