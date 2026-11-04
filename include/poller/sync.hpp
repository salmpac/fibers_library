#pragma once

// Sync policy of the poller and of the in-process side of ipc rings.
//
// It extends thread_pool's policy (atomics, mutex, threads, spin hint, ...)
// with the three things the reactor protocols need beyond it:
//
//   fence_seq_cst()   std::atomic_thread_fence(seq_cst) (the ipc ring's
//                     producer side of the Dekker handshake);
//   CondVar           condition variable usable with std::unique_lock<Mutex>
//                     (only the FakeBackend sleeps on it);
//   Clock             steady clock (the FakeBackend's time source; under the
//                     Twist simulator: simulated time).
//
// Kept here rather than in thread_pool/sync.hpp so the thread pool's policy
// concept stays untouched; the two could be merged later.

#include <atomic>
#include <chrono>
#include <condition_variable>

#include "thread_pool/sync.hpp"

namespace poller {

using thread_pool::kCacheLineSize;
using thread_pool::SyncPolicy;

// What the ipc ring needs on top of SyncPolicy.
template <class S>
concept FenceSyncPolicy = SyncPolicy<S> && requires { S::fence_seq_cst(); };

// What the FakeBackend needs on top of SyncPolicy.
template <class S>
concept SleepSyncPolicy = SyncPolicy<S> && requires {
    typename S::CondVar;
    typename S::Clock;
    S::Clock::now();
};

struct StdSync : thread_pool::StdSync {
    using CondVar = std::condition_variable;
    using Clock = std::chrono::steady_clock;

    static void fence_seq_cst() noexcept { std::atomic_thread_fence(std::memory_order_seq_cst); }
};

static_assert(FenceSyncPolicy<StdSync>);
static_assert(SleepSyncPolicy<StdSync>);

}  // namespace poller
