#pragma once

#include "thread_pool/sync.hpp"

namespace thread_pool {

// Test-and-test-and-set spin lock built on Sync atomics. Satisfies Lockable.
template <SyncPolicy Sync = StdSync>
class SpinLock {
public:
    void lock() noexcept {
        for (;;) {
            if (!locked_.exchange(true, std::memory_order_acquire)) {
                return;
            }
            while (locked_.load(std::memory_order_relaxed)) {
                Sync::spin_hint();
            }
        }
    }

    bool try_lock() noexcept {
        return !locked_.load(std::memory_order_relaxed) &&
               !locked_.exchange(true, std::memory_order_acquire);
    }

    void unlock() noexcept { locked_.store(false, std::memory_order_release); }

private:
    typename Sync::template Atomic<bool> locked_{false};
};

}  // namespace thread_pool
