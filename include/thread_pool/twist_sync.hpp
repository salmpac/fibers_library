#pragma once

// Sync policy backed by Twist (https://gitlab.com/Lipovsky/twist).
//
// Every atomic, mutex, thread, busy-wait hint and thread-local of the pool goes
// through Twist, so both fault injection (TWIST_FAULTY) and the deterministic
// simulator (TWIST_SIM) see all synchronisation in the library.
//
// Requires linking against the `twist` CMake target (FIBERS_TWIST=ON).

#include <twist/ed/static/thread_local/var.hpp>
#include <twist/ed/std/atomic.hpp>
#include <twist/ed/std/mutex.hpp>
#include <twist/ed/std/random.hpp>
#include <twist/ed/std/thread.hpp>
#include <twist/ed/wait/spin.hpp>

#if defined(__TWIST_SIM__)
#include <twist/assist/preempt.hpp>
#include <twist/assist/shared.hpp>
#endif

#include <cstdint>

#include "thread_pool/sync.hpp"

namespace thread_pool {

struct TwistSync {
    template <class T>
    using Atomic = twist::ed::std::atomic<T>;
    using Mutex = twist::ed::std::mutex;
    using Thread = twist::ed::std::thread;

    // In the simulator this is a scheduling point (yield).
    static void spin_hint() noexcept { twist::ed::CpuRelax(); }

    template <class T>
    static T& thread_local_instance() noexcept {
        TWISTED_STATIC_THREAD_LOCAL(T, instance);
        return *instance;
    }

    // Randomness controlled by Twist: deterministic for a given simulation
    // seed, different between explored runs.
    static std::uint64_t random_seed() {
        twist::ed::std::random_device device;
        return (std::uint64_t{device()} << 32) ^ device();
    }

#if defined(__TWIST_SIM__)
    // Checker hooks for lock-free containers (see detail::HasHbProbe in
    // lock_free_spmc_queue.hpp). The simulator gives every atomic load the
    // latest value, so ordering bugs between racy-by-design atomics are only
    // visible through happens-before checks of plain memory: this probe.
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

}  // namespace thread_pool
