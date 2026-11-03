#pragma once

#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <random>
#include <thread>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

namespace thread_pool {

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
// policy:
//
//   Atomic<T>                    std::atomic-compatible (incl. memory orders)
//   Mutex                        Lockable (lock / try_lock / unlock)
//   Thread                       constructible from a callable, has join()
//   spin_hint()                  called on every busy-wait iteration
//   thread_local_instance<T>()   per-thread, value-initialised T&
//   random_seed()                fresh entropy for per-thread PRNGs (under a
//                                checker: its controlled randomness, so runs
//                                are reproducible yet differ between runs)
template <class S>
concept SyncPolicy = requires {
    typename S::template Atomic<std::size_t>;
    typename S::Mutex;
    typename S::Thread;
    S::spin_hint();
    { S::template thread_local_instance<int>() } -> std::same_as<int&>;
    { S::random_seed() } -> std::convertible_to<std::uint64_t>;
};

namespace detail {
template <class T>
inline thread_local T std_thread_local_instance{};
}  // namespace detail

struct StdSync {
    template <class T>
    using Atomic = std::atomic<T>;
    using Mutex = std::mutex;
    using Thread = std::thread;

    static void spin_hint() noexcept { cpu_relax(); }

    template <class T>
    static T& thread_local_instance() noexcept {
        return detail::std_thread_local_instance<T>;
    }

    static std::uint64_t random_seed() {
        std::random_device device;
        return (std::uint64_t{device()} << 32) ^ device();
    }
};

static_assert(SyncPolicy<StdSync>);

}  // namespace thread_pool
