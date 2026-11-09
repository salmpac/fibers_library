#pragma once

// PollerPool: several Pollers (reactor threads) behind one front, with
// round-robin placement of new work.
//
//   PollerPool pool(TaskSink::of(thread_pool), {.num_pollers = 2});
//   pool.start();
//   pool.schedule(timer);          // timer goes to the next poller
//   pool.cancel(timer);            // reaches the poller it was scheduled on
//   pool.next().watch(epollable);  // fds / Checkables: pick a poller, keep it
//
// Placement
//   next() advances a per-thread counter, so threads that create lots of
//   timers do not contend on one shared cache line; every thread starts at a
//   random poller (Sync::random_seed), so timers from different threads are
//   spread too. The counter lives in a thread_local reached only through a
//   [[gnu::noinline]] accessor with a compiler barrier (fibers migrate
//   between threads; see thread_pool::detail::thread_state()).
//
// Timers
//   schedule() / post() pick the poller; the timer remembers it
//   (BasicTimer::poller()), so cancel() needs no index. The timer contract is
//   the Poller's (timer.hpp): the callback runs exactly once on the poller
//   thread of the poller the timer was handed to.
//
// fds and Checkables
//   They stay on the poller they were registered with: unwatch / remove must
//   go to that same poller (see Poller's thread rules). Pick it with next()
//   (or at(i) for explicit placement) and keep the reference.
//
// Memory
//   The pollers are created in the constructor (one allocation of an array
//   of N pollers); nothing is allocated afterwards.

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <optional>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/poller/poller.hpp"
#include "fibers/sync/sync.hpp"

namespace fibers::poller {

struct PollerPoolOptions {
    std::size_t num_pollers = 1;
    PollerOptions poller{};  // options of every poller
};

namespace detail {

struct PollerPoolThreadState {
    std::uint64_t round_robin = 0;
    bool seeded = false;
};

template <class Sync>
[[gnu::noinline]] PollerPoolThreadState& poller_pool_thread_state() noexcept {
    asm volatile("" ::: "memory");
    PollerPoolThreadState& state = Sync::template thread_local_instance<PollerPoolThreadState>();
    if (!state.seeded) [[unlikely]] {
        state.round_robin = static_cast<std::uint64_t>(Sync::random_seed());
        state.seeded = true;
    }
    return state;
}

}  // namespace detail

template <class B = EpollBackend, sync::SyncPolicy Sync = sync::StdSync>
class PollerPool {
public:
    using PollerType = Poller<B, Sync>;
    using Timer = typename PollerType::Timer;

    // Every poller emits its tasks into `sink` and constructs its backend from
    // copies of `backend_args`. Does not start the threads.
    template <class... BackendArgs>
    explicit PollerPool(TaskSink sink, PollerPoolOptions options = {},
                        const BackendArgs&... backend_args) noexcept
        : size_(options.num_pollers) {
        if (size_ == 0) [[unlikely]] {
            fail::fail_fast("PollerPool: num_pollers must be > 0");
        }
        pollers_.reset(new (std::nothrow) std::optional<PollerType>[size_]);
        if (pollers_ == nullptr) [[unlikely]] {
            fail::fail_fast("PollerPool: out of memory");
        }
        for (std::size_t i = 0; i < size_; ++i) {
            pollers_[i].emplace(sink, options.poller, backend_args...);
        }
    }

    PollerPool(const PollerPool&) = delete;
    PollerPool& operator=(const PollerPool&) = delete;

    // Stops all pollers; each one then hands its pending timers back with
    // kCancelled (see ~Poller).
    ~PollerPool() { stop(); }

    void start() noexcept {
        for (std::size_t i = 0; i < size_; ++i) {
            pollers_[i]->start();
        }
    }

    // Stops and joins every reactor thread; idempotent. Not from a callback.
    void stop() noexcept {
        for (std::size_t i = 0; i < size_; ++i) {
            pollers_[i]->stop();
        }
    }

    std::size_t size() const noexcept { return size_; }

    PollerType& at(std::size_t index) noexcept {
        assert(index < size_);
        return *pollers_[index];
    }

    // The poller for the next piece of work from this thread (round robin).
    PollerType& next() noexcept {
        if (size_ == 1) {
            return *pollers_[0];
        }
        const std::uint64_t ticket = detail::poller_pool_thread_state<Sync>().round_robin++;
        return *pollers_[ticket % size_];
    }

    // ---- timers (any thread) ---------------------------------------------

    void schedule(Timer& timer) noexcept { next().schedule(timer); }

    void post(Timer& timer) noexcept { next().post(timer); }

    // Cancels on the poller the timer was last handed to. false: too late,
    // idle, or never scheduled.
    bool cancel(Timer& timer) noexcept {
        void* owner = timer.poller();
        if (owner == nullptr) {
            return false;
        }
        return static_cast<PollerType*>(owner)->cancel(timer);
    }

    // Sum over the pollers.
    PollerStats stats() const noexcept {
        PollerStats total{};
        for (std::size_t i = 0; i < size_; ++i) {
            const PollerStats s = pollers_[i]->stats();
            total.sleeps += s.sleeps;
            total.timer_wakeups += s.timer_wakeups;
            total.timers_fired += s.timers_fired;
            total.timers_cancelled += s.timers_cancelled;
            total.compactions += s.compactions;
        }
        return total;
    }

private:
    const std::size_t size_;
    std::unique_ptr<std::optional<PollerType>[]> pollers_;
};

}  // namespace fibers::poller
