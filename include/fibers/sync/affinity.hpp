#pragma once

// CPU affinity for the library's threads (ThreadPool workers, pollers,
// blocking threads).
//
//   options.pool.cpus = {0, 2, 4};        // worker i -> cpus[i % 3]
//   CpuList::parse("0,2,8-9", list);      // the same, from a command line
//
// A CpuList is a fixed-capacity value (no allocation, safe to copy into
// options and keep). Each pool resolves its thread's CPU at thread start and
// calls pin_current_thread() once, on that thread, before its loop; an
// empty list means no pinning (the default). pthread_setaffinity_np failing
// (a CPU that does not exist or is outside the process's cpuset) is a
// configuration error: fail fast with the CPU number.
//
// Under a checker policy (TwistSync: Sync::Thread is not std::thread) pinning
// is a no-op: the threads are simulated.

#include <pthread.h>
#include <sched.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <initializer_list>
#include <string_view>
#include <thread>
#include <type_traits>

#include "fibers/fail/fail_fast.hpp"

namespace fibers::sync {

class CpuList {
public:
    static constexpr std::size_t kCapacity = 128;

    constexpr CpuList() noexcept = default;

    // Fail fast on more than kCapacity entries or a negative CPU.
    CpuList(std::initializer_list<int> cpus) noexcept {
        for (const int cpu : cpus) {
            if (!push_back(cpu)) {
                fail::fail_fast("CpuList: too many CPUs or a negative CPU number");
            }
        }
    }

    // false: full, or cpu out of [0, 65535].
    bool push_back(int cpu) noexcept {
        if (size_ == kCapacity || cpu < 0 || cpu > 0xFFFF) {
            return false;
        }
        cpus_[size_++] = static_cast<std::uint16_t>(cpu);
        return true;
    }

    std::size_t size() const noexcept { return size_; }
    bool empty() const noexcept { return size_ == 0; }
    int operator[](std::size_t i) const noexcept { return cpus_[i]; }

    // The CPU of thread `index` of a pool: cpus[index % size()], or -1 (no
    // pinning) for an empty list.
    int for_thread(std::size_t index) const noexcept {
        return size_ == 0 ? -1 : static_cast<int>(cpus_[index % size_]);
    }

    // "0,2,4-6" (Linux cpulist syntax without strides); "" gives an empty
    // list. false (and `out` unspecified) on a syntax error or overflow.
    static bool parse(std::string_view text, CpuList& out) noexcept {
        out = CpuList{};
        std::size_t pos = 0;
        while (pos < text.size()) {
            int first = 0;
            if (!parse_number(text, pos, first)) {
                return false;
            }
            int last = first;
            if (pos < text.size() && text[pos] == '-') {
                ++pos;
                if (!parse_number(text, pos, last) || last < first) {
                    return false;
                }
            }
            for (int cpu = first; cpu <= last; ++cpu) {
                if (!out.push_back(cpu)) {
                    return false;
                }
            }
            if (pos < text.size()) {
                if (text[pos] != ',') {
                    return false;
                }
                ++pos;
            }
        }
        return true;
    }

private:
    static bool parse_number(std::string_view text, std::size_t& pos, int& value) noexcept {
        const std::size_t start = pos;
        value = 0;
        while (pos < text.size() && text[pos] >= '0' && text[pos] <= '9') {
            value = value * 10 + (text[pos] - '0');
            if (value > 0xFFFF) {
                return false;
            }
            ++pos;
        }
        return pos != start;
    }

    std::uint16_t cpus_[kCapacity]{};
    std::size_t size_ = 0;
};

// Pins the calling thread to `cpu`; cpu < 0: nothing. `who` names the pool in
// the fail-fast message. Call it on the thread itself, at its start.
template <class Sync>
void pin_current_thread(int cpu, const char* who) noexcept {
    if constexpr (std::is_same_v<typename Sync::Thread, std::thread>) {
        if (cpu < 0) {
            return;
        }
        if (cpu >= CPU_SETSIZE) {
            std::fprintf(stderr, "fibers_library: %s: CPU %d is out of range\n", who, cpu);
            fail::fail_fast("pin_current_thread: CPU out of range");
        }
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(cpu, &set);
        // pthread_self() is fine here: a pool thread at its start, never a
        // fiber (which could migrate between threads).
        const int error = ::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set);
        if (error != 0) {
            std::fprintf(stderr, "fibers_library: %s: cannot pin a thread to CPU %d (error %d)\n", who,
                         cpu, error);
            fail::fail_fast("pin_current_thread: pthread_setaffinity_np failed");
        }
    } else {
        static_cast<void>(cpu);
        static_cast<void>(who);
    }
}

}  // namespace fibers::sync
