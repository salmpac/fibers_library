#pragma once

// Shared bits of the examples: --key=value arguments, CPU lists, a
// fixed-bucket log-linear latency histogram, fd limits.

#include <sys/resource.h>
#include <time.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string_view>

#include "fibers/sync/affinity.hpp"

namespace examples {

// --key=value / --flag arguments.
class Args {
public:
    Args(int argc, char** argv) : argc_(argc), argv_(argv) {}

    const char* get(const char* key) const {
        const std::size_t len = std::strlen(key);
        for (int i = 1; i < argc_; ++i) {
            const char* a = argv_[i];
            if (std::strncmp(a, "--", 2) == 0 && std::strncmp(a + 2, key, len) == 0) {
                if (a[2 + len] == '=') {
                    return a + 3 + len;
                }
                if (a[2 + len] == '\0') {
                    return "1";
                }
            }
        }
        return nullptr;
    }

    long long integer(const char* key, long long fallback) const {
        const char* v = get(key);
        return v != nullptr ? std::strtoll(v, nullptr, 10) : fallback;
    }

    double real(const char* key, double fallback) const {
        const char* v = get(key);
        return v != nullptr ? std::strtod(v, nullptr) : fallback;
    }

    const char* string(const char* key, const char* fallback) const {
        const char* v = get(key);
        return v != nullptr ? v : fallback;
    }

    fibers::sync::CpuList cpus(const char* key) const {
        fibers::sync::CpuList list;
        const char* v = get(key);
        if (v != nullptr && !fibers::sync::CpuList::parse(v, list)) {
            std::fprintf(stderr, "bad cpu list --%s=%s\n", key, v);
            std::exit(2);
        }
        return list;
    }

private:
    int argc_;
    char** argv_;
};

inline std::int64_t now_ns() noexcept {
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<std::int64_t>(ts.tv_sec) * 1'000'000'000 + ts.tv_nsec;
}

// Raises RLIMIT_NOFILE's soft limit to the hard one (10k connections).
inline void raise_fd_limit() {
    rlimit limit;
    if (getrlimit(RLIMIT_NOFILE, &limit) == 0 && limit.rlim_cur < limit.rlim_max) {
        limit.rlim_cur = limit.rlim_max;
        setrlimit(RLIMIT_NOFILE, &limit);
    }
}

// Log-linear histogram of nanosecond values (HdrHistogram-like): for each
// power of two 2^k (k = 0..kMaxExponent) kSub linear sub-buckets, so the
// relative error is below 1 / kSub (~3 %). Fixed size, relaxed atomic
// counters: record() never allocates and may run on any thread.
class Histogram {
public:
    static constexpr int kSubBits = 5;
    static constexpr std::uint64_t kSub = 1u << kSubBits;  // 32
    static constexpr int kMaxExponent = 40;                 // ~1100 s
    static constexpr std::size_t kBuckets = (kMaxExponent + 1) * kSub;

    void record(std::int64_t ns) noexcept {
        const std::uint64_t v = ns < 0 ? 0 : static_cast<std::uint64_t>(ns);
        counts_[index(v)].fetch_add(1, std::memory_order_relaxed);
    }

    void merge_into(std::uint64_t* out) const noexcept {
        for (std::size_t i = 0; i < kBuckets; ++i) {
            out[i] += counts_[i].load(std::memory_order_relaxed);
        }
    }

    // Bucket i covers [lower(i), lower(i + 1)).
    static std::uint64_t lower(std::size_t i) noexcept {
        const std::size_t group = i / kSub;
        const std::uint64_t sub = i % kSub;
        if (group == 0) {
            return sub;
        }
        const int shift = static_cast<int>(group) - 1;
        return (kSub + sub) << shift;
    }

    static std::size_t index(std::uint64_t v) noexcept {
        if (v < kSub) {
            return static_cast<std::size_t>(v);
        }
        const int msb = 63 - __builtin_clzll(v);  // >= kSubBits
        const int shift = msb - kSubBits;
        std::size_t group = static_cast<std::size_t>(shift) + 1;
        if (group > static_cast<std::size_t>(kMaxExponent)) {
            return kBuckets - 1;
        }
        const std::uint64_t sub = (v >> shift) - kSub;
        return group * kSub + static_cast<std::size_t>(sub);
    }

    // Value at quantile q (0..1) of merged counts: the bucket's midpoint.
    static double quantile(const std::uint64_t* counts, std::uint64_t total, double q) noexcept {
        if (total == 0) {
            return 0;
        }
        std::uint64_t rank = static_cast<std::uint64_t>(q * static_cast<double>(total));
        if (rank >= total) {
            rank = total - 1;
        }
        std::uint64_t seen = 0;
        for (std::size_t i = 0; i < kBuckets; ++i) {
            seen += counts[i];
            if (seen > rank) {
                const double lo = static_cast<double>(lower(i));
                const double hi = i + 1 < kBuckets ? static_cast<double>(lower(i + 1)) : lo;
                return (lo + hi) / 2;
            }
        }
        return static_cast<double>(lower(kBuckets - 1));
    }

    static double max_value(const std::uint64_t* counts) noexcept {
        for (std::size_t i = kBuckets; i-- > 0;) {
            if (counts[i] != 0) {
                return static_cast<double>(i + 1 < kBuckets ? lower(i + 1) : lower(i));
            }
        }
        return 0;
    }

private:
    std::atomic<std::uint64_t> counts_[kBuckets]{};
};

}  // namespace examples
