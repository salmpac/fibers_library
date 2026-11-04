#pragma once

// Doorbells: how a ring producer wakes a sleeping consumer.
//
//   ring()      producer: wake the consumer (eventfd write);
//   drain()     consumer, after waking: reset (eventfd read);
//   wait_fd()   consumer: the fd the poller sleeps on.

#include <cstdint>

#include "poller/eventfd.hpp"

namespace ipc {

// Cross-process: an eventfd shared by both processes (fork inheritance or
// SCM_RIGHTS). Must be non-blocking (EFD_NONBLOCK) for drain().
struct EventFdDoorbell {
    int fd = -1;

    void ring() noexcept { poller::eventfd_signal(fd); }
    void drain() noexcept { static_cast<void>(poller::eventfd_drain(fd)); }
    int wait_fd() const noexcept { return fd; }
};

// In-process, for tests under Twist: a fake eventfd of a poller::FakeBackend.
template <class FakeBackend>
struct FakeDoorbell {
    FakeBackend* backend = nullptr;
    int fd = -1;

    void ring() noexcept { backend->signal(fd); }
    void drain() noexcept { static_cast<void>(backend->drain(fd)); }
    int wait_fd() const noexcept { return fd; }
};

}  // namespace ipc
