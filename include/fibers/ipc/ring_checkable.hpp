#pragma once

// Consumer side of a ring as a poller::Checkable:
//   poll    pops up to kBatch items and hands them to the handler (which
//           typically turns them into tasks pushed into `out`);
//   arm     Consumer::arm(): sleeping = 1 + re-check; returns the doorbell fd,
//           or kArmWorkAppeared if items arrived meanwhile;
//   disarm  sleeping = 0, drain the doorbell.
//
// Register with Poller::add(rc.checkable()) (poller thread or while stopped).

#include <cstddef>

#include "fibers/ipc/doorbell.hpp"
#include "fibers/ipc/spsc_ring.hpp"
#include "fibers/poller/sources.hpp"
#include "fibers/poller/task_batch.hpp"

namespace fibers::ipc {

template <class T, sync::SyncPolicy Sync = sync::StdSync, class Doorbell = EventFdDoorbell,
          std::size_t kBatch = 32>
class RingCheckable {
public:
    using Handler = void (*)(void* context, const T* items, std::size_t count,
                             poller::TaskBatch& out) noexcept;

    RingCheckable(Ring<T, Sync> ring, Doorbell doorbell, Handler handler, void* context) noexcept
        : consumer_(ring), doorbell_(doorbell), handler_(handler), context_(context) {
        checkable_.memo = this;
        checkable_.poll = &poll;
        checkable_.arm = &arm;
        checkable_.disarm = &disarm;
    }

    RingCheckable(const RingCheckable&) = delete;
    RingCheckable& operator=(const RingCheckable&) = delete;

    poller::Checkable& checkable() noexcept { return checkable_; }
    Consumer<T, Sync>& consumer() noexcept { return consumer_; }
    Doorbell& doorbell() noexcept { return doorbell_; }

private:
    static std::size_t poll(void* memo, poller::TaskBatch& out) noexcept {
        auto* self = static_cast<RingCheckable*>(memo);
        const std::size_t n = self->consumer_.try_pop(self->buffer_, kBatch);
        if (n != 0) {
            self->handler_(self->context_, self->buffer_, n, out);
        }
        return n;
    }

    static int arm(void* memo) noexcept {
        auto* self = static_cast<RingCheckable*>(memo);
        return self->consumer_.arm() ? self->doorbell_.wait_fd() : poller::Checkable::kArmWorkAppeared;
    }

    static void disarm(void* memo) noexcept {
        auto* self = static_cast<RingCheckable*>(memo);
        self->consumer_.disarm();
        self->doorbell_.drain();
    }

    Consumer<T, Sync> consumer_;
    Doorbell doorbell_;
    Handler handler_;
    void* context_;
    poller::Checkable checkable_{};
    T buffer_[kBatch];
};

}  // namespace fibers::ipc
