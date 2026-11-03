#pragma once

#include <algorithm>
#include <cstddef>
#include <deque>
#include <mutex>

#include "thread_pool/sync.hpp"

namespace thread_pool {

// Unbounded mutex-protected fallback queue. Readers first check a relaxed
// size counter so that an empty SlowQueue costs a single relaxed load.
template <class T, SyncPolicy Sync = StdSync>
class SlowQueue {
public:
    void push(T&& value) {
        std::lock_guard guard(mutex_);
        queue_.push_back(std::move(value));
        size_.store(queue_.size(), std::memory_order_relaxed);
    }

    // Moves all of items[0..count) under a single lock.
    void push_batch(T* items, std::size_t count) {
        if (count == 0) {
            return;
        }
        std::lock_guard guard(mutex_);
        for (std::size_t i = 0; i < count; ++i) {
            queue_.push_back(std::move(items[i]));
        }
        size_.store(queue_.size(), std::memory_order_relaxed);
    }

    std::size_t try_pop_batch(T* out, std::size_t max) {
        if (!maybe_nonempty()) {
            return 0;
        }
        std::lock_guard guard(mutex_);
        const std::size_t n = std::min(max, queue_.size());
        for (std::size_t i = 0; i < n; ++i) {
            out[i] = std::move(queue_.front());
            queue_.pop_front();
        }
        size_.store(queue_.size(), std::memory_order_relaxed);
        return n;
    }

    // Like try_pop_batch, but never waits for the mutex: for thieves, so they
    // cannot form a convoy that blocks the producer or the owner.
    std::size_t try_steal_batch(T* out, std::size_t max) {
        if (!maybe_nonempty()) {
            return 0;
        }
        std::unique_lock guard(mutex_, std::try_to_lock);
        if (!guard.owns_lock()) {
            return 0;
        }
        const std::size_t n = std::min(max, queue_.size());
        for (std::size_t i = 0; i < n; ++i) {
            out[i] = std::move(queue_.front());
            queue_.pop_front();
        }
        size_.store(queue_.size(), std::memory_order_relaxed);
        return n;
    }

    // Relaxed hint: may be stale in either direction.
    bool maybe_nonempty() const noexcept {
        return size_.load(std::memory_order_relaxed) != 0;
    }

private:
    alignas(kCacheLineSize) typename Sync::template Atomic<std::size_t> size_{0};
    typename Sync::Mutex mutex_;
    std::deque<T> queue_;
};

}  // namespace thread_pool
