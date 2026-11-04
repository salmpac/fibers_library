#pragma once

// FakeBackend: an in-process stand-in for epoll + eventfd + clock, built only
// from the Sync policy (mutex, condition variable, clock). Under Twist
// (poller::TwistSync) the poller thread sleeps on a Twist condition variable
// with simulated time, so the simulator can explore the reactor protocols and
// report a lost wakeup as a deadlock (untimed sleep) or a late timer.
//
// Fake fds behave like non-blocking eventfds: signal() adds to a counter,
// drain() resets it; a watched fake fd is readable (EPOLLIN) while its counter
// is non-zero (level-triggered), or once per signal() with EPOLLET.
//
// Fake fd numbers start at kFdBase so they never collide with real ones.

#include <sys/epoll.h>

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>

#include "poller/backend.hpp"
#include "poller/sync.hpp"
#include "thread_pool/fail_fast.hpp"

namespace poller {

template <SleepSyncPolicy Sync = StdSync>
class FakeBackend {
public:
    static constexpr int kFdBase = 1 << 20;
    static constexpr int kMaxFds = 16;

    FakeBackend() noexcept = default;
    FakeBackend(const FakeBackend&) = delete;
    FakeBackend& operator=(const FakeBackend&) = delete;

    TimePoint now() noexcept {
        return TimePoint(std::chrono::duration_cast<Duration>(Sync::Clock::now().time_since_epoch()));
    }

    void wake() noexcept {
        Lock lock(mutex_);
        woken_ = true;
        cv_.notify_all();
    }

    // A new fake eventfd (counter 0).
    int create_event() noexcept {
        Lock lock(mutex_);
        if (used_ == kMaxFds) {
            thread_pool::fail_fast("FakeBackend: out of fake fds");
        }
        return kFdBase + used_++;
    }

    // eventfd write.
    void signal(int fd, std::uint64_t value = 1) noexcept {
        Lock lock(mutex_);
        Fd& f = at(fd);
        f.count += value;
        f.edge = true;
        cv_.notify_all();
    }

    // eventfd read on a non-blocking fd: returns and resets the counter.
    std::uint64_t drain(int fd) noexcept {
        Lock lock(mutex_);
        Fd& f = at(fd);
        const std::uint64_t value = f.count;
        f.count = 0;
        f.edge = false;
        return value;
    }

    int watch(int fd, std::uint32_t events, void* tag) noexcept {
        Lock lock(mutex_);
        if (fd < kFdBase || fd >= kFdBase + used_) {
            return EBADF;
        }
        Fd& f = at(fd);
        if (f.watched) {
            return EEXIST;
        }
        f.watched = true;
        f.events = events;
        f.tag = tag;
        cv_.notify_all();  // epoll reports an fd that is ready at ADD time
        return 0;
    }

    int unwatch(int fd) noexcept {
        Lock lock(mutex_);
        if (fd < kFdBase || fd >= kFdBase + used_) {
            return EBADF;
        }
        Fd& f = at(fd);
        if (!f.watched) {
            return ENOENT;
        }
        f.watched = false;
        return 0;
    }

    std::size_t wait(ReadyEvent* out, std::size_t max, TimePoint deadline) noexcept {
        Lock lock(mutex_);
        for (;;) {
            const std::size_t ready = collect(out, max);
            if (ready > 0 || woken_) {
                woken_ = false;
                return ready;
            }
            if (deadline == TimePoint::min()) {
                return 0;
            }
            if (deadline == TimePoint::max()) {
                cv_.wait(lock);
                continue;
            }
            if (now() >= deadline) {
                return 0;
            }
            using ClockDuration = typename Sync::Clock::duration;
            const typename Sync::Clock::time_point until(
                std::chrono::duration_cast<ClockDuration>(deadline.time_since_epoch()));
            cv_.wait_until(lock, until);  // spurious wakeups: loop re-checks
        }
    }

    void on_reactor_start() noexcept {}

private:
    using Lock = std::unique_lock<typename Sync::Mutex>;

    struct Fd {
        std::uint64_t count = 0;
        bool edge = false;
        bool watched = false;
        std::uint32_t events = 0;
        void* tag = nullptr;
    };

    Fd& at(int fd) noexcept {
        if (fd < kFdBase || fd >= kFdBase + used_) {
            thread_pool::fail_fast("FakeBackend: bad fake fd");
        }
        return fds_[fd - kFdBase];
    }

    std::size_t collect(ReadyEvent* out, std::size_t max) noexcept {
        std::size_t ready = 0;
        for (int i = 0; i < used_ && ready < max; ++i) {
            Fd& f = fds_[i];
            if (!f.watched || f.count == 0 || (f.events & EPOLLIN) == 0) {
                continue;
            }
            if ((f.events & EPOLLET) != 0) {
                if (!f.edge) {
                    continue;
                }
                f.edge = false;
            }
            out[ready++] = ReadyEvent{f.tag, EPOLLIN};
        }
        return ready;
    }

    typename Sync::Mutex mutex_;
    typename Sync::CondVar cv_;
    bool woken_ = false;
    int used_ = 0;
    Fd fds_[kMaxFds];
};

static_assert(Backend<FakeBackend<StdSync>>);

}  // namespace poller
