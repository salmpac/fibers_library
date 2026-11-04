#pragma once

// OS backend interface of the poller.
//
// Everything the reactor needs from the OS goes through a Backend object owned
// by the Poller, so the protocol logic (timer MPSC + wake-if-earlier,
// arm / re-check / sleep, stop) can run under Twist with FakeBackend, while
// production uses EpollBackend (epoll + eventfd + CLOCK_MONOTONIC).
//
//   now()                      the clock deadlines refer to.
//   wake()                     any thread: make the current or the next wait()
//                              return promptly (sticky, like an eventfd write).
//   watch(fd, events, tag)     any thread: start reporting readiness of `fd`
//                              (epoll semantics for `events`) as {tag, mask};
//                              takes effect even while wait() blocks.
//                              Returns 0 or an errno value.
//   unwatch(fd)                any thread: stop watching. 0 or errno.
//   wait(out, max, deadline)   poller thread: block until something is ready,
//                              wake() was called, or `deadline` passed;
//                              deadline == TimePoint::min(): do not block,
//                              deadline == TimePoint::max(): no timeout.
//                              Fills up to `max` events (never the internal
//                              wake source), returns their number; a spurious
//                              0 is allowed.
//   on_reactor_start()         called once on the poller thread before the loop.

#include <concepts>
#include <cstddef>
#include <cstdint>

#include "fibers/poller/sources.hpp"

namespace fibers::poller {

struct ReadyEvent {
    void* tag = nullptr;
    std::uint32_t events = 0;
};

template <class B>
concept Backend = requires(B backend, ReadyEvent* out, std::size_t max, TimePoint deadline, int fd,
                           std::uint32_t events, void* tag) {
    { backend.now() } noexcept -> std::same_as<TimePoint>;
    { backend.wake() } noexcept;
    { backend.watch(fd, events, tag) } noexcept -> std::same_as<int>;
    { backend.unwatch(fd) } noexcept -> std::same_as<int>;
    { backend.wait(out, max, deadline) } noexcept -> std::same_as<std::size_t>;
    { backend.on_reactor_start() } noexcept;
};

}  // namespace fibers::poller
