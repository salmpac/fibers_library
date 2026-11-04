#pragma once

#include <algorithm>
#include <cstddef>
#include <mutex>

#include "fibers/sync/spin_lock.hpp"
#include "fibers/queue/spsc_queue.hpp"

namespace fibers::queue {

// v1 implementation of a worker's main_box.
//
// Single producer (the owning worker, lock-free), many consumers serialised by
// a read spin lock. The owner takes the read lock to pop its own batches;
// thieves only try_lock it, so in the hot path (no stealing) the lock is
// uncontended and always taken by the same thread.
template <class T, sync::SyncPolicy Sync = sync::StdSync>
class LockedSPSCQueue {
public:
    explicit LockedSPSCQueue(std::size_t capacity) : queue_(capacity) {}

    std::size_t capacity() const noexcept { return queue_.capacity(); }

    // ---- owner, producer side (no lock) ----

    bool try_push(T&& value) noexcept { return queue_.try_push(std::move(value)); }

    std::size_t try_push_batch(T* items, std::size_t count) noexcept {
        return queue_.try_push_batch(items, count);
    }

    std::size_t free_space() noexcept { return queue_.free_space(); }

    // ---- owner, consumer side ----

    std::size_t try_pop_batch(T* out, std::size_t max) noexcept {
        // The owner is the only producer, so a stale view can only
        // overestimate the size: "empty" here is exact.
        if (queue_.empty_approx()) {
            return 0;
        }
        std::lock_guard guard(read_lock_);
        return queue_.try_pop_batch(out, max);
    }

    // ---- any thread ----

    // Takes a batch of up to `max` items. Never blocks.
    std::size_t try_steal_batch(T* out, std::size_t max) noexcept {
        if (queue_.empty_approx() || !read_lock_.try_lock()) {
            return 0;
        }
        const std::size_t n = queue_.try_pop_batch(out, max);
        read_lock_.unlock();
        return n;
    }

    std::size_t size_approx() const noexcept { return queue_.size_approx(); }

    bool empty_approx() const noexcept { return queue_.empty_approx(); }

private:
    SPSCQueue<T, Sync> queue_;
    alignas(sync::kCacheLineSize) sync::SpinLock<Sync> read_lock_;
};

}  // namespace fibers::queue
