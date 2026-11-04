#pragma once

// eventfd helpers (Linux). EINTR is retried, EAGAIN is not an error.

#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>

#include "thread_pool/fail_fast.hpp"

namespace poller {

// Non-blocking, close-on-exec eventfd (counter mode). Fails fast.
inline int make_eventfd() noexcept {
    const int fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (fd < 0) {
        thread_pool::fail_fast("eventfd() failed");
    }
    return fd;
}

// Adds `value` to the counter. EAGAIN (counter would overflow) means it is
// already readable, which is all a wakeup needs.
inline void eventfd_signal(int fd, std::uint64_t value = 1) noexcept {
    for (;;) {
        const ssize_t written = ::write(fd, &value, sizeof(value));
        if (written == static_cast<ssize_t>(sizeof(value))) {
            return;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written < 0 && errno == EAGAIN) {
            return;
        }
        thread_pool::fail_fast("eventfd write failed");
    }
}

// Resets the counter of a non-blocking eventfd; returns the old value (0 if it
// was not signalled).
inline std::uint64_t eventfd_drain(int fd) noexcept {
    for (;;) {
        std::uint64_t value = 0;
        const ssize_t got = ::read(fd, &value, sizeof(value));
        if (got == static_cast<ssize_t>(sizeof(value))) {
            return value;
        }
        if (got < 0 && errno == EINTR) {
            continue;
        }
        if (got < 0 && errno == EAGAIN) {
            return 0;
        }
        thread_pool::fail_fast("eventfd read failed");
    }
}

}  // namespace poller
