#pragma once

// BlockingPool: a small fixed set of threads for calls that block in the
// kernel (Tokio's spawn_blocking).
//
// Fibers must never block a worker thread of the main ThreadPool. Blocking
// syscalls (file I/O: pread / pwrite / open / fsync / stat, DNS: getaddrinfo,
// third-party blocking libraries) are handed to this pool instead: one of its
// threads runs the call, however long the disk takes, and then hands a
// continuation to the main pool (see ops.hpp), which resumes the fiber.
//
// Threads. num_threads (default 8) may exceed the core count: these threads
// spend their time asleep in the kernel, either inside the blocking call or
// waiting for work. ("Threads <= cores" is a rule for the main pool's
// workers.) An idle thread sleeps on a condition variable: idle CPU ~ 0.
//
// Queue. One bounded FIFO ring of Tasks, allocated once in the constructor,
// guarded by Sync::Mutex. No stealing and no lock-free tricks: a blocking task
// costs microseconds to milliseconds, so what matters is that idle threads
// really sleep and that no wakeup is lost, not handoff speed.
//
// Wake protocol (everything under mutex_):
//
//   pool thread                           producer (try_submit)
//   lock                                  lock
//   while (size_ == 0 && !stopping_) {    if stopping_ or full: unlock, false
//     ++waiters_                          push; wake = waiters_ > 0
//     not_empty_.wait(lock)               unlock
//     --waiters_                          if (wake) not_empty_.notify_one()
//   }
//   stopping_ -> exit; else pop, unlock, run, relock
//
// * A thread decides to sleep (size_ == 0) and registers in waiters_ in one
//   critical section, and wait() releases the mutex atomically with blocking.
//   So a producer that pushes later sees waiters_ > 0 and notifies; one that
//   pushed earlier was seen by the size_ check. That is the whole lost-wakeup
//   argument; it needs waiters_ to be read under the same lock as the push
//   (reading it before taking the lock is the classic lost wakeup).
// * notify_one per pushed task, only when somebody sleeps: no syscall when
//   every thread is busy. A notify that finds its thread already awake (it
//   woke spuriously, or a busy thread took the task first) is harmless: the
//   woken thread re-checks and sleeps again.
// * The wait is a loop on the predicate: spurious wakeups (Twist injects
//   them) and "stolen" wakeups (notified, but another thread took the task
//   first) both find size_ == 0 and wait again.
// * Notification happens after unlock (the woken thread does not wake up into
//   a held mutex). Pool lifetime is the caller's business, as for any object.
//
// Submitting: try_submit / try_submit_batch never block; a full queue (or a
// stopped pool) is reported with false / a short count and the caller keeps
// its tasks. There is deliberately no blocking submit():
// * The callers that matter are fiber workers, and a worker must never block.
//   A blocking submit would be a trap on exactly the threads this pool exists
//   to protect: one full queue would stall every worker that submits.
// * Backpressure then belongs to the caller, who knows what to do: a fiber
//   yields and retries (its worker keeps running other fibers), a plain
//   thread may retry after a sleep or run the call inline, a server may shed
//   load. A full queue is a sizing signal; queue_capacity is cheap (24 bytes
//   per slot).
// * It keeps the pool to one condition variable and one kind of sleeper, so
//   stop() has nobody but pool threads to wake.
//
// stop(): requests stop, wakes every thread and joins them. A task that is
// running finishes first; tasks still queued are dropped through delete_ptr
// (as in ThreadPool), exactly once; later submits return false. Graceful
// draining (wait until queued work is done, then stop) is the caller's job.
// Idempotent; concurrent calls are serialised. Must not be called from a
// thread of this pool (it would join itself).
//
// No allocation after construction, no exceptions: the constructor's
// allocations / thread creation terminate the process on failure (noexcept),
// impossible states go through fail::fail_fast.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/sync/sync.hpp"
#include "fibers/thread_pool/task.hpp"

namespace fibers::blocking {

using thread_pool::Task;

struct BlockingPoolOptions {
    std::size_t num_threads = 8;       // minimum 1
    std::size_t queue_capacity = 1024;  // minimum 1
};

template <sync::SleepSyncPolicy Sync = sync::StdSync>
class BlockingPool {
public:
    explicit BlockingPool(BlockingPoolOptions options = {}) noexcept
        : capacity_(std::max<std::size_t>(options.queue_capacity, 1)),
          ring_(std::make_unique<Task[]>(capacity_)) {
        const std::size_t n = std::max<std::size_t>(options.num_threads, 1);
        threads_.reserve(n);
        for (std::size_t i = 0; i < n; ++i) {
            threads_.emplace_back([this] { thread_loop(); });
        }
        num_threads_ = n;
    }

    BlockingPool(const BlockingPool&) = delete;
    BlockingPool& operator=(const BlockingPool&) = delete;

    ~BlockingPool() { stop(); }

    // Enqueues `task`. Moves from it only on success: on false (queue full or
    // pool stopped) the caller still owns it. An empty task is accepted and
    // ignored. Never blocks (beyond the queue mutex); callable from any
    // thread, fiber workers and this pool's own threads included.
    [[nodiscard]] bool try_submit(Task&& task) noexcept {
        if (!task) {
            return true;
        }
        bool wake = false;
        {
            Lock lock(mutex_);
            if (stopping_ || size_ == capacity_) {
                return false;
            }
            push_locked(std::move(task));
            wake = waiters_ > 0;
        }
        if (wake) {
            not_empty_.notify_one();
        }
        return true;
    }

    // Enqueues the longest prefix of tasks[0..count) that fits, moving from
    // those tasks only; returns its length (0 when full or stopped). The rest
    // stay with the caller. One lock, and at most one notify per accepted
    // task (fewer if fewer threads sleep). Precondition: no task is empty.
    [[nodiscard]] std::size_t try_submit_batch(Task* tasks, std::size_t count) noexcept {
        std::size_t accepted = 0;
        std::size_t wakes = 0;
        bool wake_all = false;
        {
            Lock lock(mutex_);
            if (stopping_) {
                return 0;
            }
            accepted = std::min(count, capacity_ - size_);
            for (std::size_t i = 0; i < accepted; ++i) {
                push_locked(std::move(tasks[i]));
            }
            wakes = std::min(accepted, waiters_);
            wake_all = wakes > 0 && wakes == waiters_;
        }
        if (wake_all) {
            not_empty_.notify_all();
        } else {
            for (std::size_t i = 0; i < wakes; ++i) {
                not_empty_.notify_one();
            }
        }
        return accepted;
    }

    // See the header comment. Precondition: not called from a pool thread.
    void stop() noexcept {
        std::lock_guard<typename Sync::Mutex> serialise(stop_mutex_);
        {
            Lock lock(mutex_);
            stopping_ = true;
        }
        not_empty_.notify_all();
        for (auto& thread : threads_) {
            thread.join();
        }
        threads_.clear();
        // Only we touch the ring now: pool threads are gone and submitters
        // see stopping_. Drop outside the lock: delete_ptr is user code.
        for (;;) {
            Task task;
            {
                Lock lock(mutex_);
                if (size_ == 0) {
                    break;
                }
                task = pop_locked();
            }
            task.reset();
        }
    }

    std::size_t num_threads() const noexcept { return num_threads_; }
    std::size_t capacity() const noexcept { return capacity_; }

    // Diagnostics (snapshots under the lock, for tests and monitoring).
    std::size_t queued() const noexcept {
        Lock lock(mutex_);
        return size_;
    }
    // Threads currently asleep waiting for work.
    std::size_t sleeping_threads() const noexcept {
        Lock lock(mutex_);
        return waiters_;
    }
    // Times a thread went to sleep waiting for work (spurious wakeups that
    // find nothing count again).
    std::uint64_t sleeps() const noexcept {
        Lock lock(mutex_);
        return sleeps_;
    }
    bool stop_requested() const noexcept {
        Lock lock(mutex_);
        return stopping_;
    }

private:
    using Lock = std::unique_lock<typename Sync::Mutex>;

    void push_locked(Task&& task) noexcept {
        std::size_t tail = head_ + size_;
        if (tail >= capacity_) {
            tail -= capacity_;
        }
        ring_[tail] = std::move(task);
        ++size_;
    }

    Task pop_locked() noexcept {
        if (size_ == 0) {
            fail::fail_fast("BlockingPool: pop from an empty queue");
        }
        Task task = std::move(ring_[head_]);
        head_ = head_ + 1 == capacity_ ? 0 : head_ + 1;
        --size_;
        return task;
    }

    void thread_loop() noexcept {
        Lock lock(mutex_);
        for (;;) {
            while (size_ == 0 && !stopping_) {
                ++waiters_;
                ++sleeps_;
                not_empty_.wait(lock);
                --waiters_;
            }
            if (stopping_) {
                return;
            }
            Task task = pop_locked();
            lock.unlock();
            if (task) {
                std::move(task).run();
            }
            lock.lock();
        }
    }

    const std::size_t capacity_;
    std::size_t num_threads_ = 0;

    mutable typename Sync::Mutex mutex_;
    typename Sync::CondVar not_empty_;
    // Guarded by mutex_:
    std::unique_ptr<Task[]> ring_;
    std::size_t head_ = 0;
    std::size_t size_ = 0;
    std::size_t waiters_ = 0;
    std::uint64_t sleeps_ = 0;
    bool stopping_ = false;

    typename Sync::Mutex stop_mutex_;  // serialises stop()
    std::vector<typename Sync::Thread> threads_;
};

}  // namespace fibers::blocking
