#pragma once

// Poller: the reactor thread.
//
// One thread, started / stopped by the Poller object, that waits for
//   * Epollable  fds (epoll, edge-triggered by default),
//   * Timers     caller-owned nodes submitted from any thread through a
//                lock-free MPSC queue, kept in a private pairing heap,
//   * Checkable  polling sources (shared-memory rings, ...), polled in a loop
//                while they have work and put to sleep (arm) when idle,
// and turns events into Tasks for a TaskSink (normally a ThreadPool): callbacks
// push into a TaskBatch that is flushed with one submit_batch per iteration.
//
// Loop
//   No Checkables: block in the backend until the earliest timer deadline
//   (forever if there is none): idle CPU ~ 0.
//   Otherwise:
//     poll every Checkable; every os_check_interval rounds, or as soon as the
//     earliest deadline is due (one clock read per round), also run a
//     non-blocking backend wait + due timers;
//     after spin_budget rounds without work:
//       all Checkables armable: arm all (each re-checks its source);
//         any reports work -> disarm, keep polling;
//         else sleep until the earliest deadline (fds of the Checkables are
//         watched), then disarm all;
//       otherwise: sleep min(deadline, backoff); backoff doubles from
//         min_backoff up to max_backoff, resets on work.
//     spin_budget == kSpinForever: never sleep (dedicated core).
//
// Timer wakeup protocol (wake-if-earlier, Dekker with seq_cst on both sides)
//   sleep_until_ = deadline the poller sleeps until; kAwake (INT64_MIN) while
//   it is not sleeping, INT64_MAX when it sleeps without a deadline.
//
//   poller (before sleeping)          producer (schedule)
//     S1 sleep_until_ = X   seq_cst     P1 queue.push(node)   exchange, seq_cst
//     S2 queue non-empty?   seq_cst     P2 x = sleep_until_   seq_cst
//        yes: don't sleep                  if deadline < x and
//        no:  wait until X                    CAS(x -> kAwake): wake()
//
//   In the single total order of seq_cst operations either S2 sees P1 (the
//   poller drains the node and recomputes its deadline) or P2 sees S1 (the
//   producer wakes the poller if its deadline is earlier). The CAS lets only
//   one producer per sleep pay for the eventfd write; later ones see kAwake.
//   Producers with a later deadline never write: the poller wakes at X anyway
//   and drains them then.
//
// Stop: stop_ = true, then backend.wake() (sticky, like an eventfd counter),
// so a poller that is about to sleep returns immediately. The poller also
// re-reads stop_ right before every blocking wait: a non-blocking wait earlier
// in the same iteration (periodic fd check) may have drained the wake-up.
// (Found by the Twist simulator: lost stop -> deadlock in join.)
//
// Thread rules
//   any thread:    schedule, post, cancel, watch, stop/start (not from callbacks)
//   poller thread (inside callbacks) or while stopped:
//                  unwatch, add, remove
//   To unwatch / add / remove from another thread while running, post() a
//   timer whose callback does it (and signals completion as needed).
//
// Unwatch and lifetime: an Epollable may be destroyed once unwatch() returned
// on the poller thread; events of the current batch that still point at it are
// dropped (unwatch scrubs them).

#include <sys/epoll.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <utility>

#include "fibers/poller/backend.hpp"
#include "fibers/poller/epoll_backend.hpp"
#include "fibers/poller/sources.hpp"
#include "fibers/sync/sync.hpp"
#include "fibers/poller/task_batch.hpp"
#include "fibers/poller/timer.hpp"

namespace fibers::poller {

inline constexpr std::size_t kSpinForever = std::numeric_limits<std::size_t>::max();

// Default interest set of watch(): edge-triggered read + write + peer hangup.
inline constexpr std::uint32_t kDefaultEvents = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;

struct PollerOptions {
    // Empty polling rounds (over all Checkables) before trying to sleep.
    std::size_t spin_budget = 512;
    // Polling rounds between non-blocking checks of fds and timers.
    std::size_t os_check_interval = 64;
    // Sleep backoff when some Checkable cannot arm.
    Duration min_backoff = std::chrono::microseconds(50);
    Duration max_backoff = std::chrono::milliseconds(1);
};

// Counters for tests / monitoring (relaxed, approximate while running).
struct PollerStats {
    std::uint64_t sleeps = 0;         // blocking waits entered
    std::uint64_t timer_wakeups = 0;  // wake() calls by schedule() (earlier deadline)
    std::uint64_t timers_fired = 0;
    std::uint64_t timers_cancelled = 0;
    std::uint64_t compactions = 0;
};

template <class B = EpollBackend, sync::SyncPolicy Sync = sync::StdSync>
class Poller {
    static_assert(Backend<B>);

public:
    using Timer = BasicTimer<Sync>;
    using BackendType = B;

    // `backend_args` construct the backend in place. Does not start the thread.
    template <class... BackendArgs>
    explicit Poller(TaskSink sink, PollerOptions options = {}, BackendArgs&&... backend_args) noexcept
        : options_(options),
          backend_(std::forward<BackendArgs>(backend_args)...),
          batch_(sink) {}

    Poller(const Poller&) = delete;
    Poller& operator=(const Poller&) = delete;

    // Stops the thread, then hands every timer still owned by the poller back
    // with kCancelled (on the destroying thread) and flushes the batch.
    // Callbacks run from here must not schedule() again.
    ~Poller() {
        stop();
        destroying_ = true;
        drain_queue();
        heap_.remove_if([](Timer*) { return true; }, [this](Timer* timer) { release(timer); });
        batch_.flush();
    }

    // Starts the reactor thread. Fail fast if it cannot be created.
    void start() noexcept {
        assert(!thread_.has_value());
        stop_.store(false, std::memory_order_relaxed);
        thread_.emplace([this] { run(); });
    }

    // Stops and joins the reactor thread; idempotent. Timers, Checkables and
    // watched fds stay registered (start() resumes). Not from a callback.
    void stop() noexcept {
        if (!thread_.has_value()) {
            return;
        }
        stop_.store(true, std::memory_order_seq_cst);
        backend_.wake();
        thread_->join();
        thread_.reset();
    }

    TimePoint now() noexcept { return backend_.now(); }

    B& backend() noexcept { return backend_; }

    // ---- any thread -------------------------------------------------------

    // Registers e.fd with `events` and epoll_data.ptr = &e. 0 or errno.
    int watch(Epollable& e, std::uint32_t events = kDefaultEvents) noexcept {
        return backend_.watch(e.fd, events, &e);
    }

    // Hands an idle timer (deadline, memo, callback set) to the poller.
    void schedule(Timer& timer) noexcept {
        assert(timer.idle() && "schedule(): timer is still owned by the poller");
        assert(timer.callback != nullptr);
        timer.poller_ = this;
        timer.state_.store(Timer::kPending, std::memory_order_relaxed);  // published by push
        const std::int64_t deadline = timer.deadline.time_since_epoch().count();
        queue_.push(&timer);  // seq_cst exchange (P1)
        std::int64_t sleeping = sleep_until_.load(std::memory_order_seq_cst);  // P2
        while (deadline < sleeping) {
            if (sleep_until_.compare_exchange_weak(sleeping, kAwake, std::memory_order_seq_cst,
                                                   std::memory_order_seq_cst)) {
                timer_wakeups_.fetch_add(1, std::memory_order_relaxed);
                backend_.wake();
                break;
            }
        }
    }

    // Runs timer's callback (kFired) on the poller thread as soon as possible.
    void post(Timer& timer) noexcept {
        timer.deadline = TimePoint{};
        schedule(timer);
    }

    // true: the timer will not fire; kCancelled is delivered later (see
    // timer.hpp for when the node is released). false: too late, or idle.
    bool cancel(Timer& timer) noexcept {
        std::uint32_t expected = Timer::kPending;
        if (!timer.state_.compare_exchange_strong(expected, Timer::kCancelled,
                                                  std::memory_order_acq_rel,
                                                  std::memory_order_relaxed)) {
            return false;
        }
        cancels_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    // ---- poller thread (callbacks) or while stopped -----------------------

    int unwatch(Epollable& e) noexcept {
        scrub(&e);
        return backend_.unwatch(e.fd);
    }

    void add(Checkable& c) noexcept {
        assert(!c.hook.linked && c.poll != nullptr);
        c.hook.linked = true;
        c.hook.prev = tail_;
        c.hook.next = nullptr;
        (tail_ != nullptr ? tail_->hook.next : head_) = &c;
        tail_ = &c;
        if (c.arm == nullptr) {
            ++unarmable_;
        }
    }

    void remove(Checkable& c) noexcept {
        assert(c.hook.linked);
        if (iter_next_ == &c) {
            iter_next_ = c.hook.next;
        }
        if (c.hook.armed && c.disarm != nullptr) {
            c.disarm(c.memo);  // removed while the poller sleeps armed
        }
        if (c.hook.watched_fd >= 0) {
            scrub(tag_of(&c));
            static_cast<void>(backend_.unwatch(c.hook.watched_fd));
            c.hook.watched_fd = -1;
        }
        (c.hook.prev != nullptr ? c.hook.prev->hook.next : head_) = c.hook.next;
        (c.hook.next != nullptr ? c.hook.next->hook.prev : tail_) = c.hook.prev;
        c.hook = Checkable::Hook{};
        if (c.arm == nullptr) {
            --unarmable_;
        }
    }

    PollerStats stats() const noexcept {
        return PollerStats{
            .sleeps = sleeps_.load(std::memory_order_relaxed),
            .timer_wakeups = timer_wakeups_.load(std::memory_order_relaxed),
            .timers_fired = fired_.load(std::memory_order_relaxed),
            .timers_cancelled = cancelled_.load(std::memory_order_relaxed),
            .compactions = compactions_.load(std::memory_order_relaxed),
        };
    }

private:
    static constexpr std::int64_t kAwake = std::numeric_limits<std::int64_t>::min();
    static constexpr TimePoint kNoBlock = TimePoint::min();
    static constexpr TimePoint kNever = TimePoint::max();
    static constexpr std::size_t kMaxEvents = 128;

    // ---- reactor loop -----------------------------------------------------

    void run() noexcept {
        backend_.on_reactor_start();
        std::size_t idle_rounds = 0;
        std::size_t rounds = 0;
        Duration backoff = options_.min_backoff;
        while (!stop_.load(std::memory_order_acquire)) {
            if (head_ == nullptr) {
                service(kNever);  // pure reactor: sleep until the next deadline
                continue;
            }
            const std::size_t work = poll_all();
            if (++rounds >= options_.os_check_interval ||
                (next_deadline_ != kNever && next_deadline_ <= backend_.now())) {
                rounds = 0;
                service(kNoBlock);
            }
            batch_.flush();
            if (work != 0) {
                idle_rounds = 0;
                backoff = options_.min_backoff;
                continue;
            }
            if (idle_rounds < options_.spin_budget) {
                ++idle_rounds;
                Sync::spin_hint();
                continue;
            }
            idle_rounds = 0;
            rounds = 0;
            if (unarmable_ == 0) {
                switch (arm_all()) {
                    case Armed::kWork:
                        continue;
                    case Armed::kSleep:
                        service(kNever);
                        disarm_all();
                        continue;
                    case Armed::kUnsupported:
                        break;  // backoff sleep below
                }
            }
            service(backend_.now() + backoff);
            backoff = std::min(backoff * 2, options_.max_backoff);
        }
        batch_.flush();
    }

    // Waits in the backend until min(`until`, earliest timer) -- not at all
    // for kNoBlock -- then dispatches fd events and runs due timers.
    void service(TimePoint until) noexcept {
        drain_queue();
        TimePoint deadline = until == kNoBlock ? kNoBlock : std::min(until, next_deadline_);
        bool sleeping = false;
        if (deadline != kNoBlock) {
            sleep_until_.store(deadline.time_since_epoch().count(), std::memory_order_seq_cst);  // S1
            // S2. stop_ too: a non-blocking wait earlier in this iteration
            // may have consumed stop()'s wake() (it drains the eventfd).
            if (queue_.maybe_nonempty() || stop_.load(std::memory_order_seq_cst)) {
                sleep_until_.store(kAwake, std::memory_order_relaxed);
                deadline = kNoBlock;  // a timer arrived (or is in transit), or stopping
                Sync::spin_hint();
            } else {
                sleeping = true;
                sleeps_.fetch_add(1, std::memory_order_relaxed);
            }
        }
        const std::size_t count = backend_.wait(events_, kMaxEvents, deadline);
        if (sleeping) {
            sleep_until_.store(kAwake, std::memory_order_relaxed);
        }
        dispatch(count);
        drain_queue();
        fire_due();
        maybe_compact();
        batch_.flush();
    }

    void dispatch(std::size_t count) noexcept {
        dispatch_end_ = count;
        for (dispatch_pos_ = 0; dispatch_pos_ < dispatch_end_; ++dispatch_pos_) {
            void* tag = events_[dispatch_pos_].tag;
            if (tag == nullptr || is_checkable(tag)) {
                continue;  // scrubbed, or a Checkable's wake fd (the next poll round sees the work)
            }
            auto* e = static_cast<Epollable*>(tag);
            e->callback(e->memo, events_[dispatch_pos_].events, batch_);
        }
        dispatch_end_ = 0;
        dispatch_pos_ = 0;
    }

    // Drops not-yet-dispatched events of the current batch that point at `tag`.
    void scrub(void* tag) noexcept {
        for (std::size_t i = dispatch_pos_ + 1; i < dispatch_end_; ++i) {
            if (events_[i].tag == tag) {
                events_[i].tag = nullptr;
            }
        }
    }

    // ---- checkables -------------------------------------------------------

    enum class Armed { kSleep, kWork, kUnsupported };

    std::size_t poll_all() noexcept {
        std::size_t work = 0;
        for (Checkable* c = head_; c != nullptr; c = iter_next_) {
            iter_next_ = c->hook.next;
            work += c->poll(c->memo, batch_);
        }
        iter_next_ = nullptr;
        return work;
    }

    Armed arm_all() noexcept {
        Armed result = Armed::kSleep;
        Checkable* c = head_;
        for (; c != nullptr; c = c->hook.next) {
            const int fd = c->arm(c->memo);
            if (fd < 0) {
                result = fd == Checkable::kArmWorkAppeared ? Armed::kWork : Armed::kUnsupported;
                break;
            }
            if (fd != c->hook.watched_fd) {
                if (c->hook.watched_fd >= 0) {
                    static_cast<void>(backend_.unwatch(c->hook.watched_fd));
                }
                // Level-triggered: a signal that is not drained yet (e.g. a
                // stale wakeup) wakes us again rather than getting lost.
                if (backend_.watch(fd, EPOLLIN, tag_of(c)) != 0) {
                    fail::fail_fast("poller: cannot watch a Checkable's fd");
                }
                c->hook.watched_fd = fd;
            }
            c->hook.armed = true;
        }
        if (result != Armed::kSleep) {
            disarm_all();  // undo the arms that succeeded before `c`
        }
        return result;
    }

    void disarm_all() noexcept {
        for (Checkable* c = head_; c != nullptr; c = c->hook.next) {
            if (c->hook.armed) {
                c->hook.armed = false;
                if (c->disarm != nullptr) {
                    c->disarm(c->memo);
                }
            }
        }
    }

    static void* tag_of(Checkable* c) noexcept {
        return reinterpret_cast<void*>(reinterpret_cast<std::uintptr_t>(c) | 1u);
    }

    static bool is_checkable(void* tag) noexcept {
        return (reinterpret_cast<std::uintptr_t>(tag) & 1u) != 0;
    }

    // ---- timers -----------------------------------------------------------

    void drain_queue() noexcept {
        while (auto* node = queue_.pop()) {
            Timer* timer = static_cast<Timer*>(node);
            if (timer->state_.load(std::memory_order_acquire) == Timer::kCancelled) {
                release(timer);
                continue;
            }
            heap_.push(timer);
            next_deadline_ = std::min(next_deadline_, timer->deadline);
        }
    }

    void fire_due() noexcept {
        TimePoint now = kNoBlock;
        while (Timer* top = heap_.top()) {
            if (top->state_.load(std::memory_order_acquire) != Timer::kCancelled) {
                if (now == kNoBlock) {
                    now = backend_.now();
                }
                if (top->deadline > now) {
                    break;
                }
            }
            release(heap_.pop());
        }
        next_deadline_ = heap_.empty() ? kNever : heap_.top()->deadline;
    }

    // Hands the node back to its owner: kFired if still pending, kCancelled
    // otherwise (or unconditionally on destruction).
    void release(Timer* timer) noexcept {
        void* memo = timer->memo;
        const typename Timer::Callback callback = timer->callback;
        std::uint32_t expected = Timer::kPending;
        const bool fire = !stopping_for_good() &&
                          timer->state_.compare_exchange_strong(expected, Timer::kIdle,
                                                                std::memory_order_acq_rel,
                                                                std::memory_order_acquire);
        if (fire) {
            fired_.fetch_add(1, std::memory_order_relaxed);
            callback(memo, TimerEvent::kFired, batch_);
            return;
        }
        if (cancelled_estimate_ > 0) {
            --cancelled_estimate_;
        }
        cancelled_.fetch_add(1, std::memory_order_relaxed);
        timer->state_.store(Timer::kIdle, std::memory_order_release);
        callback(memo, TimerEvent::kCancelled, batch_);
    }

    void maybe_compact() noexcept {
        if (cancels_.load(std::memory_order_relaxed) != 0) {
            cancelled_estimate_ += cancels_.exchange(0, std::memory_order_relaxed);
        }
        if (cancelled_estimate_ == 0 || cancelled_estimate_ * 4 < heap_.size()) {
            return;
        }
        cancelled_estimate_ = 0;
        compactions_.fetch_add(1, std::memory_order_relaxed);
        heap_.remove_if(
            [](Timer* timer) {
                return timer->state_.load(std::memory_order_acquire) == Timer::kCancelled;
            },
            [this](Timer* timer) { release(timer); });
        next_deadline_ = heap_.empty() ? kNever : heap_.top()->deadline;
    }

    // In the destructor every remaining timer is delivered as kCancelled.
    bool stopping_for_good() const noexcept { return destroying_; }

    template <class T>
    using Atomic = typename Sync::template Atomic<T>;

    PollerOptions options_;
    B backend_;
    TaskBatch batch_;

    // Shared with producers.
    detail::MpscQueue<Sync> queue_;
    alignas(sync::kCacheLineSize) Atomic<std::int64_t> sleep_until_{kAwake};
    Atomic<std::size_t> cancels_{0};
    Atomic<bool> stop_{false};

    // Poller thread only.
    alignas(sync::kCacheLineSize) TimerHeap<Timer> heap_;
    TimePoint next_deadline_ = kNever;
    std::size_t cancelled_estimate_ = 0;
    Checkable* head_ = nullptr;
    Checkable* tail_ = nullptr;
    Checkable* iter_next_ = nullptr;
    std::size_t unarmable_ = 0;
    bool destroying_ = false;
    std::size_t dispatch_pos_ = 0;
    std::size_t dispatch_end_ = 0;
    ReadyEvent events_[kMaxEvents];

    Atomic<std::uint64_t> sleeps_{0};
    Atomic<std::uint64_t> timer_wakeups_{0};
    Atomic<std::uint64_t> fired_{0};
    Atomic<std::uint64_t> cancelled_{0};
    Atomic<std::uint64_t> compactions_{0};

    std::optional<typename Sync::Thread> thread_;
};

}  // namespace fibers::poller
