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
// once), so no post-switch hook is needed. After a wake the fiber tries
// again from the start (it may be overtaken and queue again).
//
// lock() must be called from a fiber of a scheduler of type Sched (fail fast
// otherwise); try_lock() and unlock() from anywhere (unlock by the holder).
// unlock() wakes at most one fiber: a submit into the waker's pool queue.

#include "fibers/fail/fail_fast.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/wait/mutex_core.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fibers::fiber {

template <FiberScheduler Sched = sched::Scheduler>
class BasicMutex {
public:
    BasicMutex() noexcept = default;
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
        wait::WaitNode node = self.wait_node();
        while (!core_.lock_or_enqueue(node)) {
            self.park();
        }
    }

    wait::MutexCore<typename Sched::SyncPolicy> core_;
};

using Mutex = BasicMutex<sched::Scheduler>;

}  // namespace fibers::fiber
