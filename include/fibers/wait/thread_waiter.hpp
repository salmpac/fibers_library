#pragma once

// ThreadWaiter: a WaitNode front-end for a plain thread (not a fiber): the
// thread sleeps on an atomic word (C++20 atomic wait, a futex) until the
// node's wake. For callers outside fibers, e.g. the main thread waiting for
// a fiber::Future; never on a worker thread of a scheduler (it would block
// the worker, and with it every fiber queued there).
//
//   ThreadWaiter<Sync> waiter;           // on the waiting thread's stack
//   if (!core.wait_or_enqueue(waiter.node)) {
//       waiter.wait();                   // returns after the wake
//   }
//
// The waker's last touch: wake() stores NOTIFYING, notify_one()s, then
// stores DONE (release). The sleeper returns, and its frame (the node, the
// word) may go, only once it reads DONE (acquire), i.e. after the waker's
// last access; while it reads NOTIFYING it spins (the waker is inside
// notify_one, a few hundred ns at most). The same handshake as
// net::PollDesc's closer on a plain thread.
//
// Memory orders: the DONE store is a release after everything the waker did
// before the wake (WaitNode contract), read by the sleeper's acquire load.
// NOTIFYING carries no data (relaxed): it only makes the futex wait return.

#include <cstdint>

#include "fibers/sync/sync.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fibers::wait {

template <sync::ParkingSyncPolicy Sync = sync::StdSync>
class ThreadWaiter {
public:
    ThreadWaiter() noexcept : node{nullptr, this, &ThreadWaiter::wake} {}
    ThreadWaiter(const ThreadWaiter&) = delete;
    ThreadWaiter& operator=(const ThreadWaiter&) = delete;

    // Until the node's wake (which may already have happened). Once per
    // publication of the node.
    void wait() noexcept {
        for (;;) {
            const std::uint32_t v = flag_.load(std::memory_order_acquire);
            if (v == kDone) {
                break;
            }
            if (v == kIdle) {
                flag_.wait(kIdle, std::memory_order_acquire);
            } else {
                Sync::spin_hint();  // the waker is inside notify_one
            }
        }
        flag_.store(kIdle, std::memory_order_relaxed);  // the waker is gone: reusable
    }

    WaitNode node;

private:
    static constexpr std::uint32_t kIdle = 0;
    static constexpr std::uint32_t kNotifying = 1;
    static constexpr std::uint32_t kDone = 2;

    static void wake(void* self) noexcept {
        auto* w = static_cast<ThreadWaiter*>(self);
        w->flag_.store(kNotifying, std::memory_order_relaxed);
        w->flag_.notify_one();
        w->flag_.store(kDone, std::memory_order_release);  // last touch
    }

    typename Sync::template Atomic<std::uint32_t> flag_{kIdle};
};

}  // namespace fibers::wait
