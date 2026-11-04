#pragma once

// Event sources of the poller. All of them are caller-owned and intrusive: the
// poller never allocates; whatever bookkeeping it needs lives in the object.

#include <chrono>
#include <cstddef>
#include <cstdint>

#include "fibers/poller/task_batch.hpp"

namespace fibers::poller {

// Time base of the poller: deadlines are points on the backend's monotonic
// clock (EpollBackend: std::chrono::steady_clock, i.e. CLOCK_MONOTONIC;
// FakeBackend under Twist: simulated time). Use Poller::now() to build them.
using TimePoint = std::chrono::steady_clock::time_point;
using Duration = std::chrono::nanoseconds;

// ---------------------------------------------------------------------------
// Epollable: a file descriptor watched by epoll.
//
// Registered once with Poller::watch (default interest: edge-triggered
// EPOLLIN | EPOLLOUT | EPOLLRDHUP), with epoll_data.ptr pointing at this object,
// and unregistered explicitly with Poller::unwatch. `callback` runs on the
// poller thread with the epoll event mask.
//
// Expected fiber usage (edge-triggered):
//
//   for (;;) {
//       n = read(fd, ...);                        // fd is O_NONBLOCK
//       if (n >= 0 || errno != EAGAIN) break;     // data, EOF or error
//       park the fiber on `readable` of this connection;
//       // callback(memo, EPOLLIN, out): if a fiber is parked on `readable`,
//       // out.push(Task "resume that fiber") -- otherwise remember the edge
//       // (set a flag) so the next read attempt is not skipped.
//   }
//
// With edge triggering the callback fires once per readiness change, so the
// remembered-edge flag (or simply always retrying the read before parking, as
// above) is what prevents lost wakeups; the poller does not re-arm anything.
struct Epollable {
    using Callback = void (*)(void* memo, std::uint32_t events, TaskBatch& out) noexcept;

    int fd = -1;
    void* memo = nullptr;
    Callback callback = nullptr;
};

// ---------------------------------------------------------------------------
// Checkable: a polling source (e.g. a shared-memory ring fed by a DPDK
// process) that the poller checks in a loop while there is work, and puts to
// sleep (arm) when there is none.
//
//   poll(memo, out)  does the work (e.g. drains a batch, pushes tasks into
//                    `out`); returns how much was done (0 = nothing).
//   arm(memo)        optional (nullptr = unsupported). Announces "consumer
//                    sleeping" to the producer, re-checks, and returns
//                      fd >= 0           sleep: wait for this fd to become
//                                        readable (level-triggered), it is
//                                        what the producer signals;
//                      kArmWorkAppeared  work arrived meanwhile, don't sleep;
//                      kArmUnsupported   can't sleep on anything right now.
//                    The fd must be the same on every call (registered with
//                    epoll on first use, until the Checkable is removed).
//   disarm(memo)     undo arm after waking (clear the flag, drain the fd).
//                    Called exactly once after every arm that returned an fd.
//                    May be nullptr if there is nothing to undo.
struct Checkable {
    using PollFn = std::size_t (*)(void* memo, TaskBatch& out) noexcept;
    using ArmFn = int (*)(void* memo) noexcept;
    using DisarmFn = void (*)(void* memo) noexcept;

    static constexpr int kArmWorkAppeared = -1;
    static constexpr int kArmUnsupported = -2;

    void* memo = nullptr;
    PollFn poll = nullptr;
    ArmFn arm = nullptr;
    DisarmFn disarm = nullptr;

    // Poller-private bookkeeping (intrusive list, registered wake fd).
    struct Hook {
        Checkable* prev = nullptr;
        Checkable* next = nullptr;
        int watched_fd = -1;
        bool linked = false;
        bool armed = false;
    } hook{};
};

// ---------------------------------------------------------------------------
// Timers: see timer.hpp (BasicTimer<Sync>, carries atomics of the policy).

enum class TimerEvent : std::uint8_t {
    kFired,      // the deadline passed
    kCancelled,  // cancel() succeeded earlier; the poller now lets go of the node
};

}  // namespace fibers::poller
