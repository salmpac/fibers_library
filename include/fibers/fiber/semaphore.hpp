#pragma once

// Counting semaphore for stackful fibers: the stackful front-end of
// wait::SemaphoreCore (FIFO with direct hand-off; see semaphore_core.hpp
// for the algorithm, the fairness policy and the memory orders).
//
//   fiber::Semaphore slots(16);                 // 16 permits
//   ... in a fiber:
//   {
//       fiber::SemaphoreGuard permit(slots);    // acquire(1) ... release(1)
//       query(db);
//   }
//   slots.acquire(4);  ...  slots.release(4);   // several at once
//   if (slots.try_acquire()) { ... }            // never waits
//
// Policy, in short: while anybody waits nobody overtakes the queue (even
// try_acquire() fails), release(n) hands permits to the waiters at the head
// in arrival order, as many as fit, and a large request at the head holds
// smaller ones behind it until enough permits have been released for it.
// So no waiter starves, multi-permit acquires included; the price is that a
// released permit waits for its new holder to be scheduled. Uncontended
// acquire / release are one atomic RMW each.
//
// Everything sequenced before a release(n) happens-before the return of an
// acquire that obtained one of its permits.
//
// acquire() in a fiber of a scheduler of type Sched parks the fiber, not the
// worker thread: a node on the fiber's own stack is queued under the core's
// lock, then the fiber parks; a hand-off that arrives before the fiber has
// left its stack is absorbed by its state word (NOTIFIED). Outside such a
// fiber it blocks the calling thread on an atomic (wait::ThreadWaiter):
// fine for a plain thread (main, a producer thread throttled by permits
// that fibers release), never from a plain callback on a worker (it would
// block the worker, and maybe the very fibers that would release). Why not
// fail fast like Mutex::lock(): a semaphore is often the backpressure
// between plain threads and fibers, and the hand-off makes the thread's wait
// a single wake, exactly what ThreadWaiter supports. try_acquire() and
// release() work from anywhere; release() wakes the granted fibers one
// submit each, from the calling thread.
//
// Permits are not owned by a fiber: any fiber or thread may release what
// another acquired (and a SemaphoreGuard may be moved between them).
// Releasing more than was acquired simply raises the count. An acquire(n)
// with n larger than the permits that will ever be free waits forever (and
// holds everybody behind it): size n by the semaphore's capacity.
//
// Lifetime: a waiter may destroy the semaphore as soon as its acquire has
// returned if no other call is in progress (the releases that handed it
// permits no longer touch the semaphore by then); destroying it with
// waiters fails fast.

#include <cstdint>
#include <mutex>
#include <utility>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/sync/sync.hpp"
#include "fibers/wait/semaphore_core.hpp"
#include "fibers/wait/thread_waiter.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fibers::fiber {

template <FiberScheduler Sched = sched::Scheduler>
class BasicSemaphore {
public:
    using Core = wait::SemaphoreCore<typename Sched::SyncPolicy>;

    static constexpr std::uint64_t kMaxPermits = Core::kMaxPermits;

    explicit BasicSemaphore(std::uint64_t initial = 0) noexcept : core_(initial) {}
    BasicSemaphore(const BasicSemaphore&) = delete;
    BasicSemaphore& operator=(const BasicSemaphore&) = delete;

    // Takes n permits, waiting (FIFO) until they are handed over. n == 0
    // returns at once.
    void acquire(std::uint64_t n = 1) noexcept {
        if (core_.try_acquire(n)) [[likely]] {
            return;
        }
        acquire_slow(n);
    }

    // Takes n permits if they are free and nobody waits. Never waits; any
    // thread.
    bool try_acquire(std::uint64_t n = 1) noexcept { return core_.try_acquire(n); }

    // Returns n permits (n == 0: no-op), handing them to waiters in order.
    // Any thread; never waits (a short spin lock when somebody waits).
    void release(std::uint64_t n = 1) noexcept { core_.release(n); }

    // Racy snapshots (tests, diagnostics, metrics).
    std::uint64_t available_approx() const noexcept { return core_.available_approx(); }
    bool has_waiters_approx() const noexcept { return core_.has_waiters_approx(); }
    // Queued waiters (fibers and threads that have not been granted yet).
    std::uint64_t waiters_approx() const noexcept { return core_.waiters_approx(); }

private:
    [[gnu::noinline]] void acquire_slow(std::uint64_t n) noexcept {
        BasicFiber<Sched>* self = BasicFiber<Sched>::current();
        if (self != nullptr) {
            wait::SemaphoreNode node{self->wait_node(), 0};
            if (!core_.acquire_or_enqueue(node, n)) {
                self->park();  // exactly one wake: the hand-off of our n permits
            }
            return;
        }
        using Sync = typename Sched::SyncPolicy;
        if constexpr (sync::ParkingSyncPolicy<Sync>) {
            wait::ThreadWaiter<Sync> waiter;
            wait::SemaphoreNode node{waiter.node, 0};
            // The core wakes node.node, whose self is the waiter.
            if (!core_.acquire_or_enqueue(node, n)) {
                waiter.wait();
            }
        } else {
            fail::fail_fast("fiber: Semaphore::acquire() outside a fiber needs a parking Sync policy");
        }
    }

    Core core_;
};

using Semaphore = BasicSemaphore<sched::Scheduler>;

// RAII permits: acquires in the constructor (or adopts / tries), releases
// what it holds in the destructor. Move-only; may be moved to another fiber
// or thread (permits are not owned by a fiber).
//
//   fiber::SemaphoreGuard g(sem, 2);                       // acquire(2)
//   fiber::SemaphoreGuard t(sem, 1, std::try_to_lock);     // if (t) ...
//   fiber::SemaphoreGuard a(sem, 3, std::adopt_lock);      // already acquired
template <FiberScheduler Sched = sched::Scheduler>
class BasicSemaphoreGuard {
public:
    using Semaphore = BasicSemaphore<Sched>;

    BasicSemaphoreGuard() noexcept = default;

    explicit BasicSemaphoreGuard(Semaphore& sem, std::uint64_t n = 1) noexcept : sem_(&sem), permits_(n) {
        sem.acquire(n);
    }

    BasicSemaphoreGuard(Semaphore& sem, std::uint64_t n, std::try_to_lock_t) noexcept {
        if (sem.try_acquire(n)) {
            sem_ = &sem;
            permits_ = n;
        }
    }

    BasicSemaphoreGuard(Semaphore& sem, std::uint64_t n, std::adopt_lock_t) noexcept : sem_(&sem), permits_(n) {}

    BasicSemaphoreGuard(BasicSemaphoreGuard&& other) noexcept
        : sem_(std::exchange(other.sem_, nullptr)), permits_(std::exchange(other.permits_, 0)) {}

    BasicSemaphoreGuard& operator=(BasicSemaphoreGuard&& other) noexcept {
        if (this != &other) {
            reset();
            sem_ = std::exchange(other.sem_, nullptr);
            permits_ = std::exchange(other.permits_, 0);
        }
        return *this;
    }

    BasicSemaphoreGuard(const BasicSemaphoreGuard&) = delete;
    BasicSemaphoreGuard& operator=(const BasicSemaphoreGuard&) = delete;

    ~BasicSemaphoreGuard() { reset(); }

    // Releases the held permits now (no-op if none).
    void reset() noexcept {
        if (Semaphore* const sem = std::exchange(sem_, nullptr)) {
            sem->release(std::exchange(permits_, 0));
        }
    }

    // Gives up ownership without releasing; returns how many permits it
    // held (the caller releases them).
    std::uint64_t disown() noexcept {
        sem_ = nullptr;
        return std::exchange(permits_, 0);
    }

    // Holds permits (a try_to_lock guard: the attempt succeeded).
    bool owns() const noexcept { return sem_ != nullptr; }
    explicit operator bool() const noexcept { return owns(); }

    std::uint64_t permits() const noexcept { return permits_; }

private:
    Semaphore* sem_ = nullptr;
    std::uint64_t permits_ = 0;
};

using SemaphoreGuard = BasicSemaphoreGuard<sched::Scheduler>;

}  // namespace fibers::fiber
