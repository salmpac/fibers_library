#pragma once

// Production backend: epoll + eventfd + CLOCK_MONOTONIC (Linux).
//
// Sleeping uses epoll_pwait2 (Linux >= 5.11): the timeout is a timespec, so the
// poller can sleep until the earliest timer deadline with nanosecond
// granularity and no extra syscalls. The alternative, a timerfd registered in
// epoll, costs a timerfd_settime per change of the earliest deadline plus a
// read on expiry and buys nothing in precision. If the kernel lacks
// epoll_pwait2 (ENOSYS) we fall back to epoll_pwait with the timeout rounded UP
// to whole milliseconds (timers fire late, never early).
//
// The libc wrapper: glibc has one since 2.35; musl (1.2.6 at least) has none,
// so there (and wherever FIBERS_HAVE_EPOLL_PWAIT2 is 0) it is the raw
// syscall(SYS_epoll_pwait2, ...), which behaves the same (-1 + errno, ENOSYS
// on old kernels). Detected at compile time from the libc's version macros;
// define FIBERS_HAVE_EPOLL_PWAIT2 to 0 / 1 to override.
//
// Precision is then bounded by the poller thread's timer slack (50 us by
// default on Linux); on_reactor_start() lowers it to EpollOptions::timer_slack.

#include <sys/epoll.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>

#include "fibers/poller/backend.hpp"
#include "fibers/poller/eventfd.hpp"
#include "fibers/fail/fail_fast.hpp"

#ifndef FIBERS_HAVE_EPOLL_PWAIT2
#if defined(__GLIBC__) && (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 35))
#define FIBERS_HAVE_EPOLL_PWAIT2 1
#else
#define FIBERS_HAVE_EPOLL_PWAIT2 0
#endif
#endif

namespace fibers::poller {

namespace detail {

// epoll_pwait2(epfd, events, maxevents, timeout, no sigmask) as a raw
// syscall (what a libc without the wrapper gets; any libc may call it).
inline int epoll_pwait2_syscall(int epfd, epoll_event* events, int maxevents, const timespec* timeout) noexcept {
#if defined(SYS_epoll_pwait2)
    constexpr long kSyscall = SYS_epoll_pwait2;
#else
    constexpr long kSyscall = 441;  // the same number on every architecture (asm-generic, x86_64)
#endif
    // The kernel takes a struct __kernel_timespec (64-bit seconds and
    // nanoseconds); a time64 libc's timespec has that layout.
    static_assert(sizeof(timespec) == 16 && sizeof(timespec::tv_sec) == 8,
                  "epoll_pwait2 by syscall needs a 64-bit time_t timespec");
    // No sigmask: its size argument is not looked at.
    return static_cast<int>(::syscall(kSyscall, epfd, events, maxevents, timeout, nullptr, 0));
}

// The libc wrapper if there is one, the raw syscall otherwise.
inline int epoll_pwait2(int epfd, epoll_event* events, int maxevents, const timespec* timeout) noexcept {
#if FIBERS_HAVE_EPOLL_PWAIT2
    return ::epoll_pwait2(epfd, events, maxevents, timeout, nullptr);
#else
    return epoll_pwait2_syscall(epfd, events, maxevents, timeout);
#endif
}

}  // namespace detail

struct EpollOptions {
    // PR_SET_TIMERSLACK for the poller thread; 0 keeps the thread's default.
    std::chrono::nanoseconds timer_slack{1000};
    // false: never try epoll_pwait2, always the millisecond fallback (what a
    // kernel older than 5.11 gets; for tests).
    bool use_epoll_pwait2 = true;
};

class EpollBackend {
public:
    static constexpr std::size_t kMaxEvents = 128;

    explicit EpollBackend(EpollOptions options = {}) noexcept
        : options_(options), no_pwait2_(!options.use_epoll_pwait2) {
        epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd_ < 0) {
            fail::fail_fast("epoll_create1() failed");
        }
        wake_fd_ = make_eventfd();
        epoll_event event{};
        event.events = EPOLLIN;
        event.data.ptr = &wake_fd_;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &event) != 0) {
            fail::fail_fast("epoll_ctl(wake eventfd) failed");
        }
    }

    EpollBackend(const EpollBackend&) = delete;
    EpollBackend& operator=(const EpollBackend&) = delete;

    ~EpollBackend() {
        ::close(wake_fd_);
        ::close(epoll_fd_);
    }

    TimePoint now() noexcept { return std::chrono::steady_clock::now(); }

    void wake() noexcept { eventfd_signal(wake_fd_); }

    int watch(int fd, std::uint32_t events, void* tag) noexcept {
        epoll_event event{};
        event.events = events;
        event.data.ptr = tag;
        return ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) == 0 ? 0 : errno;
    }

    int unwatch(int fd) noexcept {
        return ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) == 0 ? 0 : errno;
    }

    std::size_t wait(ReadyEvent* out, std::size_t max, TimePoint deadline) noexcept {
        const int capacity = static_cast<int>(std::min(max, kMaxEvents));
        int count = 0;
        if (deadline == TimePoint::min()) {
            count = ::epoll_wait(epoll_fd_, events_, capacity, 0);
        } else if (deadline == TimePoint::max()) {
            count = ::epoll_wait(epoll_fd_, events_, capacity, -1);
        } else {
            const auto timeout = deadline - now();
            count = timeout.count() <= 0 ? ::epoll_wait(epoll_fd_, events_, capacity, 0)
                                         : timed_wait(capacity, timeout);
        }
        if (count < 0) {
            if (errno == EINTR) {
                return 0;
            }
            fail::fail_fast("epoll_wait() failed");
        }
        std::size_t ready = 0;
        for (int i = 0; i < count; ++i) {
            if (events_[i].data.ptr == &wake_fd_) {
                eventfd_drain(wake_fd_);
                continue;
            }
            out[ready++] = ReadyEvent{events_[i].data.ptr, events_[i].events};
        }
        return ready;
    }

    void on_reactor_start() noexcept {
        if (options_.timer_slack.count() > 0) {
            // Best effort: failure only costs precision.
            static_cast<void>(::prctl(PR_SET_TIMERSLACK,
                                      static_cast<unsigned long>(options_.timer_slack.count()), 0,
                                      0, 0));
        }
    }

    int epoll_fd() const noexcept { return epoll_fd_; }

private:
    int timed_wait(int capacity, std::chrono::nanoseconds timeout) noexcept {
        if (!no_pwait2_) {
            timespec ts{};
            ts.tv_sec = static_cast<time_t>(timeout.count() / 1'000'000'000);
            ts.tv_nsec = static_cast<long>(timeout.count() % 1'000'000'000);
            const int count = detail::epoll_pwait2(epoll_fd_, events_, capacity, &ts);
            if (count >= 0 || errno != ENOSYS) {
                return count;
            }
            no_pwait2_ = true;  // kernel < 5.11: from now on, milliseconds
        }
        // Round up: never wake before the deadline.
        const auto ms = (timeout.count() + 999'999) / 1'000'000;
        return ::epoll_pwait(epoll_fd_, events_, capacity, static_cast<int>(std::min<std::int64_t>(ms, 1 << 30)),
                             nullptr);
    }

    EpollOptions options_;
    int epoll_fd_ = -1;
    int wake_fd_ = -1;
    bool no_pwait2_ = false;
    epoll_event events_[kMaxEvents];
};

static_assert(Backend<EpollBackend>);

}  // namespace fibers::poller
