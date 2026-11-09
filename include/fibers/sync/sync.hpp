#pragma once

#include <atomic>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <random>
#include <thread>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

namespace fibers::sync {

// std::hardware_destructive_interference_size triggers -Winterference-size in
// headers on GCC, so we pin the value explicitly.
inline constexpr std::size_t kCacheLineSize = 64;

inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    _mm_pause();
#elif defined(__aarch64__)
    asm volatile("yield" ::: "memory");
#endif
}

// Synchronisation policy. Every concurrency primitive in the library comes
// from here, so a checker (e.g. Twist) can be plugged in by providing its own
// policy. There is one policy per backend (StdSync below, TwistSync in
// twist_sync.hpp) providing everything; components state what they use with
// the concepts below.
//
// SyncPolicy: what every component needs.
//
//   Atomic<T>                    std::atomic-compatible (incl. memory orders)
//   atomic_thread_fence(order)   std::atomic_thread_fence (incl. seq_cst)
//   Mutex                        Lockable (lock / try_lock / unlock)
//   Thread                       constructible from a callable, has join()
//   spin_hint()                  called on every busy-wait iteration
//   thread_local_instance<T>()   per-thread, value-initialised T&
//   global_instance<T>()         process-wide, value-initialised T& (one per
//                                T; under the simulator: per simulated run)
//   random_seed()                fresh entropy for per-thread PRNGs (under a
//                                checker: its controlled randomness, so runs
//                                are reproducible yet differ between runs)
template <class S>
concept SyncPolicy = requires {
    typename S::template Atomic<std::size_t>;
    S::atomic_thread_fence(std::memory_order_seq_cst);
    typename S::Mutex;
    typename S::Thread;
    S::spin_hint();
    { S::template thread_local_instance<int>() } -> std::same_as<int&>;
    { S::template global_instance<int>() } -> std::same_as<int&>;
    { S::random_seed() } -> std::convertible_to<std::uint64_t>;
};

// ParkingSyncPolicy: SyncPolicy plus C++20 atomic wait / notify on
// Atomic<std::uint32_t> (ThreadPool parks idle workers on it).
template <class S>
concept ParkingSyncPolicy = SyncPolicy<S> && requires(typename S::template Atomic<std::uint32_t>& word) {
    word.wait(std::uint32_t{0}, std::memory_order_acquire);
    word.notify_one();
    word.notify_all();
};

// SleepSyncPolicy: SyncPolicy plus timed sleeping (poller's FakeBackend):
//
//   CondVar   condition variable usable with std::unique_lock<Mutex>
//   Clock     steady clock (under the Twist simulator: simulated time)
template <class S>
concept SleepSyncPolicy = SyncPolicy<S> && requires {
    typename S::CondVar;
    typename S::Clock;
    S::Clock::now();
};

namespace detail {
template <class T>
inline thread_local T std_thread_local_instance{};
template <class T>
inline T std_global_instance{};
}  // namespace detail

struct StdSync {
    template <class T>
    using Atomic = std::atomic<T>;
    using Mutex = std::mutex;
    using Thread = std::thread;
    using CondVar = std::condition_variable;
    using Clock = std::chrono::steady_clock;

    static void spin_hint() noexcept { cpu_relax(); }

    static void atomic_thread_fence(std::memory_order order) noexcept {
        std::atomic_thread_fence(order);
    }

    // Types with a destructor (run at thread exit) use a block-scope
    // thread_local: GCC 16 does not reliably run destructors of
    // thread_local variable templates (in a minimal repro it skipped them,
    // or re-ran a dynamic initialiser after the first write), while the
    // block-scope form works with both compilers. Trivially destructible
    // types keep the variable template (no guard on access).
    template <class T>
    static T& thread_local_instance() noexcept {
        if constexpr (std::is_trivially_destructible_v<T>) {
            return detail::std_thread_local_instance<T>;
        } else {
            thread_local T instance{};
            return instance;
        }
    }

    template <class T>
    static T& global_instance() noexcept {
        return detail::std_global_instance<T>;
    }

    static std::uint64_t random_seed() {
        std::random_device device;
        return (std::uint64_t{device()} << 32) ^ device();
    }
};

static_assert(SyncPolicy<StdSync>);
static_assert(ParkingSyncPolicy<StdSync>);
static_assert(SleepSyncPolicy<StdSync>);

}  // namespace fibers::sync
