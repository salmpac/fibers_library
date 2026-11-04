#pragma once

// Production backend: epoll + eventfd + CLOCK_MONOTONIC (Linux).
//
// Sleeping uses epoll_pwait2 (Linux >= 5.11): the timeout is a timespec, so the
// poller can sleep until the earliest timer deadline with nanosecond
// granularity and no extra syscalls. The alternative, a timerfd registered in
// epoll, costs a timerfd_settime per change of the earliest deadline plus a
// read on expiry and buys nothing in precision. If the kernel lacks
// epoll_pwait2 (ENOSYS) we fall back to epoll_wait with the timeout rounded UP
// to whole milliseconds (timers fire late, never early).
//
// Precision is then bounded by the poller thread's timer slack (50 us by
// default on Linux); on_reactor_start() lowers it to EpollOptions::timer_slack.

#include <sys/epoll.h>
#include <sys/prctl.h>
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

namespace fibers::poller {

struct EpollOptions {
    // PR_SET_TIMERSLACK for the poller thread; 0 keeps the thread's default.
    std::chrono::nanoseconds timer_slack{1000};
};

class EpollBackend {
public:
    static constexpr std::size_t kMaxEvents = 128;

    explicit EpollBackend(EpollOptions options = {}) noexcept : options_(options) {
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
            const int count = ::epoll_pwait2(epoll_fd_, events_, capacity, &ts, nullptr);
            if (count >= 0 || errno != ENOSYS) {
                return count;
            }
            no_pwait2_ = true;
        }
        // Round up: never wake before the deadline.
        const auto ms = (timeout.count() + 999'999) / 1'000'000;
        return ::epoll_wait(epoll_fd_, events_, capacity,
                            static_cast<int>(std::min<std::int64_t>(ms, 1 << 30)));
    }

    EpollOptions options_;
    int epoll_fd_ = -1;
    int wake_fd_ = -1;
    bool no_pwait2_ = false;
    epoll_event events_[kMaxEvents];
};

static_assert(Backend<EpollBackend>);

}  // namespace fibers::poller
