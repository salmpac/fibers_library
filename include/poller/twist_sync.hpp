#pragma once

// poller::TwistSync: thread_pool::TwistSync plus the fence, condition variable
// and clock of Twist, so the reactor protocols (timer MPSC + wake-if-earlier,
// arm / re-check / sleep, stop) run under the Twist simulator (simulated time)
// or with fault injection.
//
// Requires linking against the `twist` CMake target (FIBERS_TWIST=ON).

#include <atomic>

#include <twist/ed/std/atomic.hpp>
#include <twist/ed/std/chrono.hpp>
#include <twist/ed/std/condition_variable.hpp>

#include "poller/sync.hpp"
#include "thread_pool/twist_sync.hpp"

namespace poller {

struct TwistSync : thread_pool::TwistSync {
    using CondVar = twist::ed::std::condition_variable;
    using Clock = twist::ed::std::chrono::steady_clock;

    static void fence_seq_cst() noexcept {
#if defined(__TWIST_SIM__)
        // The simulator's memory model gives every atomic load the latest
        // value (sequentially consistent values; only happens-before is
        // tracked, for race detection) and rejects seq_cst fences. acq_rel
        // keeps the fence's happens-before effect. Consequence: store->load
        // reordering, i.e. a MISSING seq_cst fence, cannot be observed in
        // simulation; see the ring's cross-process test for that.
        twist::ed::std::atomic_thread_fence(std::memory_order_acq_rel);
#else
        twist::ed::std::atomic_thread_fence(std::memory_order_seq_cst);
#endif
    }
};

static_assert(FenceSyncPolicy<TwistSync>);
static_assert(SleepSyncPolicy<TwistSync>);

}  // namespace poller
