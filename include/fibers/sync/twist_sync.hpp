#pragma once

// Sync policy backed by Twist (https://gitlab.com/Lipovsky/twist).
//
// Every atomic, fence, mutex, condition variable, clock, thread, busy-wait hint
// and thread-local of the library goes through Twist, so both fault injection
// (TWIST_FAULTY) and the deterministic simulator (TWIST_SIM, simulated time)
// see all synchronisation in the library.
//
// Requires linking against the `twist` CMake target (FIBERS_TWIST=ON), built
// with TWIST_ED_STD_ATOMIC_WAIT=ON (cmake/Twist.cmake sets it): parked workers
// sleep in Atomic<std::uint32_t>::wait, which the simulator models as a futex.
//
// Needs Twist with our local patches (third_party/twist/patches): seq_cst
// fences in the simulator, and the opt-in x86-TSO store buffers
// (TWIST_SIM_STORE_BUFFERING, on in the clang-twist-sim preset and the nix
// twist-sim package) under which a missing seq_cst fence is observable.

#include <twist/ed/static/thread_local/var.hpp>
#include <twist/ed/static/var.hpp>
#include <twist/ed/std/atomic.hpp>
#include <twist/ed/std/chrono.hpp>
#include <twist/ed/std/condition_variable.hpp>
#include <twist/ed/std/mutex.hpp>
#include <twist/ed/std/random.hpp>
#include <twist/ed/std/thread.hpp>
#include <twist/ed/wait/spin.hpp>

#if defined(__TWIST_SIM__)
#include <twist/assist/preempt.hpp>
#include <twist/assist/shared.hpp>
#endif

#include <cstdint>

#include "fibers/sync/sync.hpp"

namespace fibers::sync {

struct TwistSync {
    template <class T>
    using Atomic = twist::ed::std::atomic<T>;
    using Mutex = twist::ed::std::mutex;
    using Thread = twist::ed::std::thread;
    using CondVar = twist::ed::std::condition_variable;
    using Clock = twist::ed::std::chrono::steady_clock;
    // Defined in fibers/context/twist_context.hpp (include it to switch
    // contexts; it needs the `sure` target besides `twist`).
    using ExecutionContext = context::TwistContext;

    // In the simulator this is a scheduling point (yield).
    static void spin_hint() noexcept { twist::ed::CpuRelax(); }

    // seq_cst included: the simulator supports it (local Twist patch 0001)
    // and, in store-buffering mode, it drains the thread's store buffer like
    // MFENCE. Weakening it to acq_rel would hide exactly the store -> load
    // reorderings that mode exists to expose.
    static void atomic_thread_fence(std::memory_order order) noexcept {
        twist::ed::std::atomic_thread_fence(order);
    }

    template <class T>
    static T& thread_local_instance() noexcept {
        TWISTED_STATIC_THREAD_LOCAL(T, instance);
        return *instance;
    }

    // Under the simulator a fresh instance for every simulated run, so
    // process-wide state (e.g. the object_pool thread-id registry) restarts
    // with it.
    template <class T>
    static T& global_instance() noexcept {
        TWISTED_STATIC(T, instance);
        return *instance;
    }

    // Randomness controlled by Twist: deterministic for a given simulation
    // seed, different between explored runs.
    static std::uint64_t random_seed() {
        twist::ed::std::random_device device;
        return (std::uint64_t{device()} << 32) ^ device();
    }

#if defined(__TWIST_SIM__)
    // Checker hooks for lock-free containers (see queue::detail::HasHbProbe in
    // lock_free_spmc_queue.hpp and ipc::detail::HasHbProbe in spsc_ring.hpp).
    // Even with store buffering the simulator is x86-TSO: loads are never
    // reordered with loads nor stores with stores, so a missing release /
    // acquire between racy-by-design atomics does not change any value they
    // read. It is only visible through happens-before checks of plain
    // memory: this probe.
    class HbProbe {
    public:
        void write() noexcept { cell_.Write(0); }
        void read() const noexcept { static_cast<void>(cell_.Read()); }

    private:
        twist::assist::Shared<char> cell_{char{0}};
    };

    using NoPreemption = twist::assist::NoPreemptionGuard;
#endif
};

static_assert(SyncPolicy<TwistSync>);
static_assert(ParkingSyncPolicy<TwistSync>);
static_assert(SleepSyncPolicy<TwistSync>);
static_assert(ContextSyncPolicy<TwistSync>);

}  // namespace fibers::sync
