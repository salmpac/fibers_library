#pragma once

// PollDesc: readiness of one socket for fibers (Go's netpoller design).
//
// One PollDesc per fd, embedded in the object that owns the fd (Stream,
// Listener): no allocation. open() registers the fd ONCE, edge-triggered
// (EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET), on one poller picked with
// pollers().next(); the descriptor keeps a reference to that poller, and
// every readiness callback and the final unwatch run on its thread.
//
// Readiness words
// ---------------
// Two atomic words, rd_ and wr_ (one per direction), each holding
//
//   EMPTY  (0)       no readiness recorded, nobody waits
//   READY  (1)       an edge arrived that no waiter has consumed yet
//   CLOSED (2)       close() has begun: waits fail with ECANCELED (final)
//   IoWaiter*        the single waiter of this direction (aligned to 8,
//                    so never 0, 1 or 2)
//
// Waiter (io(dir, op), a fiber; op is the non-blocking syscall):
//
//   loop:
//     r = op()                            r != -EAGAIN: return r
//     cur = word                          relaxed load, then CAS loop:
//       READY  -> CAS(READY -> EMPTY)     relaxed; ok: an edge came after
//                                         (or around) the syscall: retry op
//       CLOSED -> return -ECANCELED
//       EMPTY  -> CAS(EMPTY -> &waiter)   release (publishes the waiter's
//                                         fields); ok: park()
//       waiter -> fail fast: one waiter per direction
//     after the wake: waiter.result == ECANCELED -> return -ECANCELED,
//                     else retry op
//
// Poller callback (events on the fd; on the owning poller's thread):
//
//   EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR | EPOLLPRI -> notify(rd_)
//   EPOLLOUT | EPOLLHUP | EPOLLERR                        -> notify(wr_)
//   notify(word): cur = word (relaxed), CAS loop:
//       CLOSED -> nothing
//       waiter -> CAS(waiter -> EMPTY)    acquire (reads the waiter's
//                                         fields); then waiter.result = 0
//                                         and push its Task into the batch
//       EMPTY / READY -> CAS(cur -> READY)  acquire (unused for these)
//
// Who consumes READY: the WAITER (its CAS READY -> EMPTY before parking).
// The waker consumes the waiter instead (waiter -> EMPTY): the woken waiter
// retries its syscall, which is what the edge was for, and does not find a
// stale READY that would cost it one more syscall per wait. The waker never
// writes READY over a waiter, so a word is never READY while somebody
// sleeps on it.
//
// Why no edge is lost. Every transition is an RMW on one word, so the
// waiter's arm CAS and the callback's CAS are totally ordered (the word's
// modification order), whatever their memory orders:
//   * callback first: the word is READY when the waiter tries to arm; the
//     arm CAS fails, the waiter consumes READY and retries the syscall, which
//     starts after the callback, i.e. after the epoll event was reported,
//     i.e. after the kernel made the fd ready: it sees the data.
//   * arm first: the callback's CAS finds the waiter and wakes it (the wake
//     may arrive before the fiber has left its stack: NOTIFIED absorbs it).
// The callback must use an RMW even when it finds READY (CAS READY ->
// READY): a plain load may return a stale READY that the waiter has already
// consumed, with the waiter's node already in the word (lost wakeup).
// A READY left over from an old edge only costs a spurious retry, and so
// does a wake for an edge whose data an earlier syscall already took: the
// loop simply re-arms.
//
// Memory orders of the words. The only data handed over through a word is
// the waiter itself (self, take, the checker probe): published by the arm
// CAS (release), read after the callback's or close's CAS / exchange
// (acquire). READY carries no data (readiness is kernel state; the retried
// syscall is ordered after the CAS by the program order and the kernel),
// so consuming READY is relaxed, and so is observing CLOSED. waiter.result
// is written by the waker before the wake; the fiber wake (state word,
// pool queue) orders it before the waiter's read.
//
// Lifetime: open / op references / close
// --------------------------------------
// refs_ = [ CLOSED bit 31 | number of operations in progress ].
//   open():  fields set, fd registered, then refs_ = 0 (release; it heads
//            the release sequence of every later RMW on refs_, so any
//            acquiring RMW that reads a later value sees the fields).
//   enter(): CAS loop count + 1 unless CLOSED (acquire: sees open()'s
//            fields). A failed enter() never touches the count, so nobody
//            but the operations that entered before close() decrements it.
//   leave(): fetch_sub(1, acq_rel); the one that brings CLOSED | 1 down to
//            CLOSED | 0 wakes the closer (see below).
// The fd stays open while any operation holds a reference: an operation
// racing with close() never reaches a closed (and maybe reused) fd number.
//
// close() (any fiber; or a thread that is not a worker of this scheduler):
//   1. CAS refs_: set CLOSED and take a reference of its own (fails with
//      EBADF if CLOSED is set already: never opened, closed, or closing).
//      acquire, like enter() (the poller and the Epollable); it publishes
//      nothing (closer_ goes out with the fetch_sub of step 3).
//   2. post() a timer to the OWNING poller and wait for its callback, which
//      (on the poller thread, where unwatch is allowed)
//        unwatch()s the fd (scrubbing its not-yet-dispatched events),
//        exchanges CLOSED into rd_ and wr_ (acquire) and wakes a waiter
//        found there with result ECANCELED,
//        then wakes the closer (the timer node and the closer's waiter
//        live on the closer's stack; the callback never touches them after
//        that wake).
//      No callback of this descriptor can run after that: the poller no
//      longer reports the fd, and it scrubbed the current batch.
//   3. Write closer_ = its waiter, drop its own reference (fetch_sub,
//      acq_rel: release publishes closer_ to the last leaver, acquire sees
//      every operation's work if the closer is last); if others are still
//      inside, wait: the last leave() wakes it.
//   4. Return: the caller closes the fd; the PollDesc may be reused (open())
//      or destroyed.
// Waking a parked waiter in 2 makes it return ECANCELED and leave(), so
// close() never waits for readiness that may never come. An operation that
// is between syscalls (not parked) finishes its current syscall and sees
// CLOSED at its next arm (or fails enter()). Each wait of the closer has
// exactly one waker (the timer callback, then the last leaver; the closer
// keeps its own reference until after the first wait, so no leave() can be
// last before step 3).
//
// The closer waits in a fiber by parking; on a plain thread on an atomic
// flag: the waker stores NOTIFYING, notify_one()s, then stores DONE
// (release); the thread returns (and its frame may go) only once it reads
// DONE (acquire), i.e. after the waker's last touch of the flag.
//
// Checked under Twist (tests/net/twist, FakeBackend fds standing in for
// sockets): weakening any of the release / acquire orders above to relaxed
// makes a test report a race, through IoWaiter::probe (the waiter handed
// over a word or to the last leaver), open_probe_ (open() -> enter() /
// close()) or a test cell written inside an operation and read after
// close(); dropping the READY record (the waker storing EMPTY) loses an
// edge and hangs a reader. The relaxed ones (READY consumption, observing
// CLOSED, NOTIFYING) carry no data.
//
// Socket errors. EPOLLERR (with or without EPOLLHUP) notifies BOTH words, so
// a parked reader and a parked writer both wake and retry. Before that the
// callback reads SO_ERROR (which clears it in the kernel) and keeps it in
// error_ (sticky until the next open()). The retried syscalls then report
// end of stream / EPIPE (the kernel's view once SO_ERROR is gone); the
// Stream maps those to error_after_shutdown(), so both get e.g. ECONNRESET
// (after any data still buffered has been read). An operation that is not
// parked and races with the callback may read SO_ERROR itself first; the
// first error recorded wins either way.
//
// The scheduler's pollers must be running while descriptors are closed
// (the unwatch is a posted timer). If the poller is destroyed first, the
// timer is delivered with kCancelled on the destroying thread while the
// poller is stopped, where unwatch is allowed too: same code.
//
// Deadlines (Go's SetReadDeadline / SetWriteDeadline)
// ---------------------------------------------------
// Per direction: `desired` (the deadline set by the user), one Timer node
// IN the descriptor (not on a fiber's stack: it lives as long as the
// socket and needs no cancellation), `armed_at` (when that node fires, or
// none), and an atomic `expired` flag. desired / armed_at and the node's
// scheduling are guarded by a small spin lock (set_deadline() on any
// thread, the timer callbacks on the poller thread; Go's pd.lock); the
// operations never take it.
//
//   set_deadline(t):  desired = t, expired = 0 (moving a deadline revives
//                     the direction), then
//                       none          -> nothing (a pending node finds no
//                                        deadline when it fires);
//                       t <= now      -> expire now (below);
//                       node idle     -> schedule it at t;
//                       node at <= t  -> nothing: it fires first and
//                                        re-arms for t (later deadlines,
//                                        the common per-request pattern,
//                                        cost no poller round trip);
//                       node at > t   -> post the control timer (once): on
//                                        the poller thread it withdraw()s
//                                        the node (Poller::withdraw: eager
//                                        removal from the heap) and re-arms
//                                        it at t.
//   node fires:       armed_at = none; re-evaluate the CURRENT desired:
//                     none -> idle; in the future -> re-arm; due -> expire.
//                     So a timer armed for an older deadline is harmless
//                     (no generation counter needed: the callback never acts
//                     on the deadline it was armed for, only on the current
//                     one, under the lock).
//   expire:           expired = 1 (seq_cst), then take the waiter out of the
//                     word (CAS waiter -> EMPTY, seq_cst) and wake it with
//                     result 0; the readiness callback and the expiry race
//                     on that one word, first CAS wins. The woken waiter
//                     re-checks `expired` at the top of its loop: still set
//                     -> ETIMEDOUT; cleared (the deadline moved meanwhile)
//                     -> it simply goes on waiting.
//   io():             every attempt starts with `expired` -> ETIMEDOUT.
//                     After a successful arm it re-reads `expired`
//                     (seq_cst): if set, it takes itself back out (CAS
//                     &waiter -> EMPTY) and returns ETIMEDOUT, or, if a
//                     waker took it first, parks to consume that wake.
//
// The arm / re-read pair against the expirer's store / take is a Dekker
// handshake on two words (the waiter's word and `expired`), so all four are
// seq_cst: in the single total order either the waiter's re-read sees the
// expiry or the expirer's load sees the waiter. (Two RMWs would not need
// it; the expirer's flag store is a plain store.) Under Twist's x86-TSO
// store buffering, a release-only expiry store loses a timeout
// (NetTwist.PastDeadlineRacesArm hangs); the waiter's side (an RMW, then a
// load) is ordered on x86 whatever the C++ order, so weakening it is not
// observable there -- it is needed by the C++ model (ARM, POWER).
//
// Why one node + a control node instead of one node per deadline: a Timer
// can only be rescheduled once its callback has run, and Poller::cancel()
// keeps referencing the node until its lazy removal. Later deadlines need
// no new node (re-arm at fire), earlier ones are handled by withdraw() on
// the poller thread, so two nodes per direction would not cover a third
// earlier move anyway, and the control node does, exactly.
//
// close() and timers: the close callback (poller thread) takes the lock,
// sets closing_ (set_deadline() then fails with ECANCELED and arms
// nothing) and withdraw()s every pending node (deadline timers and the
// control timer). After it no timer of this descriptor is in the poller:
// the PollDesc may be destroyed or reopened.

#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <cerrno>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <utility>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/poller/poller.hpp"
#include "fibers/poller/sources.hpp"
#include "fibers/poller/task_batch.hpp"
#include "fibers/queue/lock_free_spmc_queue.hpp"
#include "fibers/sync/spin_lock.hpp"
#include "fibers/sync/sync.hpp"
#include "fibers/thread_pool/task.hpp"

namespace fibers::net {

using thread_pool::Task;

// What PollDesc needs from a scheduler (sched::BasicScheduler): fibers
// (pool, stacks), parking atomics (close() from a plain thread), and a
// PollerPool whose pollers watch fds and run posted timers.
template <class S>
concept NetScheduler = fiber::FiberScheduler<S> && sync::ParkingSyncPolicy<typename S::SyncPolicy> &&
                       requires(S& s, typename S::Timer& timer, poller::Epollable& e) {
                           typename S::Poller;
                           { s.pollers().next() } -> std::same_as<typename S::Poller&>;
                           s.pollers().next().post(timer);
                           { s.pollers().next().watch(e, poller::kDefaultEvents) } -> std::same_as<int>;
                           { s.pollers().next().unwatch(e) } -> std::same_as<int>;
                       };

enum class Direction : std::uint8_t { kRead = 0, kWrite = 1 };

// One parked party (an I/O waiter, or the closer): what a waker needs to
// let it go on. take(self) is called exactly once per publication and
// returns the Task that resumes the party (empty if it resumes by itself:
// a fiber still on its stack, a plain thread); the waker submits it (the
// poller: into its TaskBatch). After take() the waker never touches the
// waiter again. `result` is written by the waker before take().
template <class Sync>
struct IoWaiter {
    using TakeFn = Task (*)(void* self) noexcept;
    using Probe = typename queue::detail::HbProbeOf<Sync>::type;

    void* self = nullptr;
    TakeFn take = nullptr;
    int result = 0;
    // Checker only (Twist simulator): written by the publisher before the
    // publishing RMW, read by the waker after its acquiring RMW, so a
    // missing release / acquire shows up as a race.
    [[no_unique_address]] Probe probe{};
};

template <NetScheduler Sched>
class BasicPollDesc {
public:
    using SyncPolicy = typename Sched::SyncPolicy;
    using Poller = typename Sched::Poller;
    using Timer = typename Sched::Timer;
    using Fiber = fiber::BasicFiber<Sched>;
    using Waiter = IoWaiter<SyncPolicy>;

    static constexpr std::uintptr_t kEmpty = 0;
    static constexpr std::uintptr_t kReady = 1;
    static constexpr std::uintptr_t kClosed = 2;

    static_assert(alignof(Waiter) >= 4, "waiter pointers must not collide with EMPTY / READY / CLOSED");

    BasicPollDesc() noexcept = default;
    BasicPollDesc(const BasicPollDesc&) = delete;
    BasicPollDesc& operator=(const BasicPollDesc&) = delete;

    // Registers `fd` (non-blocking already) with scheduler.pollers().next().
    // The descriptor must be closed (never opened, or close() returned) and
    // not used concurrently. 0 or the errno of the registration (the fd is
    // then not adopted; the descriptor stays closed).
    int open(Sched& scheduler, int fd) noexcept {
        if (refs_.load(std::memory_order_relaxed) != kClosedBit) [[unlikely]] {
            fail::fail_fast("net: PollDesc::open() on a descriptor that is open");
        }
        scheduler_ = &scheduler;
        poller_ = &scheduler.pollers().next();
        fd_ = fd;
        closer_ = nullptr;
        rd_.store(kEmpty, std::memory_order_relaxed);
        wr_.store(kEmpty, std::memory_order_relaxed);
        error_.store(0, std::memory_order_relaxed);
        for (int i = 0; i < 2; ++i) {
            Deadline& d = deadlines_[i];
            d.desc = this;
            d.dir = static_cast<Direction>(i);
            d.timer.memo = &d;
            d.timer.callback = &on_deadline_timer;
            d.desired = kNoDeadline;
            d.armed_at = kNoDeadline;
            d.expired.store(0, std::memory_order_relaxed);
        }
        control_.memo = this;
        control_.callback = &on_control_timer;
        control_pending_ = false;
        closing_ = false;
        deadlines_used_ = false;
        epollable_ = poller::Epollable{fd, this, &on_events};
        // Events can arrive from here on (the callback only touches rd_ /
        // wr_, initialised above; epoll_ctl orders them before the report).
        const int error = poller_->watch(epollable_, poller::kDefaultEvents);
        if (error != 0) {
            return error;
        }
        open_probe_.write();
        // release: the fields above to enter() / close() on other threads
        // (it heads the release sequence of every later RMW on refs_).
        refs_.store(0, std::memory_order_release);  // enter() may succeed now
        return 0;
    }

    // ---- operations ----------------------------------------------------------

    // Takes an operation reference. false: closed (or closing, or never
    // opened): the operation must fail (EBADF / ECANCELED) without touching
    // the fd. Any thread.
    bool enter() noexcept {
        std::uint32_t s = refs_.load(std::memory_order_relaxed);
        for (;;) {
            if ((s & kClosedBit) != 0) {
                return false;
            }
            // acquire: open()'s fields (fd, poller) before the store of 0.
            if (refs_.compare_exchange_weak(s, s + 1, std::memory_order_acquire, std::memory_order_relaxed)) {
                open_probe_.read();
                return true;
            }
        }
    }

    // Drops the reference of enter(). The last one out of a closing
    // descriptor wakes the closer.
    void leave() noexcept {
        // release: this operation's work (syscalls on the fd, reads of the
        // fields) happens before the closer's return; acquire: closer_ and
        // the closer's probe (published by the closer's own fetch_sub).
        const std::uint32_t s = refs_.fetch_sub(1, std::memory_order_acq_rel);
        if (s == (kClosedBit | 1)) {
            Sched& scheduler = *scheduler_;
            Waiter* closer = closer_;
            closer->probe.read();
            Task task = closer->take(closer->self);  // last touch of *this
            if (task) {
                scheduler.pool().submit(std::move(task));
            }
        }
    }

    // RAII enter() / leave().
    class Guard {
    public:
        explicit Guard(BasicPollDesc& desc) noexcept : desc_(&desc), entered_(desc.enter()) {}
        ~Guard() {
            if (entered_) {
                desc_->leave();
            }
        }
        Guard(const Guard&) = delete;
        Guard& operator=(const Guard&) = delete;
        explicit operator bool() const noexcept { return entered_; }

    private:
        BasicPollDesc* desc_;
        bool entered_;
    };

    // The fd; stable between enter() and leave().
    int fd() const noexcept { return fd_; }

    // Runs `op` (a non-blocking syscall: returns >= 0, or -errno) until it
    // returns something other than -EAGAIN, parking the calling fiber on
    // `dir` between attempts. Returns op's result, or -ECANCELED if the
    // descriptor is closed meanwhile. Between enter() and leave(). Must be
    // called from a fiber of Sched when it has to wait (fail fast otherwise).
    // At most one waiter per direction (fail fast on a second one).
    template <class Op>
    ssize_t io(Direction dir, Op&& op) noexcept {
        Word& word = word_of(dir);
        Deadline& deadline = deadline_of(dir);
        Waiter waiter;
        for (;;) {
            // An expired deadline fails at once, until it is moved.
            if (deadline.expired.load(std::memory_order_acquire) != 0) {
                return -ETIMEDOUT;
            }
            const ssize_t r = op();
            if (r != -EAGAIN) {
                return r;
            }
            if (waiter.self == nullptr) {
                Fiber* self = Fiber::current();
                if (self == nullptr) [[unlikely]] {
                    fail::fail_fast("net: an I/O operation would block outside a fiber");
                }
                waiter.self = self;
                waiter.take = &take_fiber;
            }
            switch (arm(word, waiter)) {
                case Arm::kRetry:
                    continue;
                case Arm::kClosed:
                    return -ECANCELED;
                case Arm::kArmed:
                    break;
            }
            // Dekker with expire(): arm (seq_cst RMW) then this load, against
            // expired = 1 (seq_cst) then its load of the word. Either we see
            // the expiry, or the expirer sees us in the word and wakes us.
            if (deadline.expired.load(std::memory_order_seq_cst) != 0) {
                std::uintptr_t mine = reinterpret_cast<std::uintptr_t>(&waiter);
                if (word.compare_exchange_strong(mine, kEmpty, std::memory_order_relaxed,
                                                 std::memory_order_relaxed)) {
                    return -ETIMEDOUT;  // nobody took us: nobody will wake us
                }
                // Taken by a waker meanwhile: its wake is on the way; park to
                // consume it (the loop then sees the expiry).
            }
            static_cast<Fiber*>(waiter.self)->park();
            if (waiter.result != 0) {
                return -waiter.result;
            }
        }
    }

    // ---- deadlines --------------------------------------------------------------

    // Sets the deadline of `dir` (see "Deadlines" in the top comment):
    // operations on that direction fail with ETIMEDOUT once it has passed
    // (a parked one is woken), until the deadline is moved. A deadline in the
    // past expires at once; TimePoint{} or TimePoint::max() means none.
    // Between enter() and leave(); any thread. 0, or ECANCELED if the
    // descriptor is being closed.
    int set_deadline(Direction dir, poller::TimePoint deadline) noexcept {
        Deadline& d = deadline_of(dir);
        if (deadline == poller::TimePoint{}) {
            deadline = kNoDeadline;
        }
        bool expire_now = false;
        {
            std::lock_guard guard(deadline_lock_);
            if (closing_) {
                return ECANCELED;
            }
            deadlines_used_ = true;
            d.desired = deadline;
            d.expired.store(0, std::memory_order_seq_cst);  // a moved deadline is not expired
            if (deadline == kNoDeadline) {
                // nothing: a pending timer finds no deadline when it fires
            } else if (deadline <= poller_->now()) {
                mark_expired(d);
                expire_now = true;
            } else if (d.armed_at == kNoDeadline) {
                arm_timer(d, deadline);
            } else if (d.armed_at > deadline) {
                // Earlier than the pending timer: only the poller thread can
                // take that one back; the control timer does it.
                if (!control_pending_) {
                    control_pending_ = true;
                    poller_->post(control_);
                }
            }
            // else: the pending timer fires first and re-arms for `deadline`.
        }
        if (expire_now) {
            if (Waiter* w = take_waiter(word_of(dir))) {
                Sched& scheduler = *scheduler_;
                w->probe.read();
                w->result = 0;  // it re-checks the expiry itself
                Task task = w->take(w->self);
                if (task) {
                    scheduler.pool().submit(std::move(task));
                }
            }
        }
        return 0;
    }

    // Racy snapshot (tests): whether `dir`'s deadline has expired.
    bool expired_approx(Direction dir) const noexcept {
        return deadlines_[static_cast<int>(dir)].expired.load(std::memory_order_relaxed) != 0;
    }

    // ---- close ----------------------------------------------------------------

    // See the top comment. 0, or EBADF if the descriptor is not open (never
    // opened, already closed, or another close() is in progress). From a
    // fiber, or from a thread that no fiber of this scheduler depends on (it
    // blocks the thread; not a worker of the scheduler, not a poller).
    // After 0 the caller owns the fd again (normally: ::close it).
    int close() noexcept {
        std::uint32_t s = refs_.load(std::memory_order_relaxed);
        for (;;) {
            if ((s & kClosedBit) != 0) {
                return EBADF;
            }
            // acquire, like enter(): open()'s fields (poller, epollable).
            // Nothing is published here: operations that see CLOSED in
            // enter() fail without reading anything, and closer_ is
            // published by the fetch_sub below.
            if (refs_.compare_exchange_weak(s, (s + 1) | kClosedBit, std::memory_order_acquire,
                                            std::memory_order_relaxed)) {
                open_probe_.read();
                break;
            }
        }
        ContextWaiter self;
        {
            CloseOp op{this, &self.waiter};
            Timer timer(&op, &on_close_timer);
            poller_->post(timer);
            self.wait();  // the callback has run: unwatched, waiters cancelled
        }
        closer_ = &self.waiter;
        self.waiter.probe.write();
        // release: publishes closer_ to the last leave(); acquire: if we are
        // last, every operation's work happens before our return.
        s = refs_.fetch_sub(1, std::memory_order_acq_rel);
        if ((s & kCountMask) != 1) {
            self.wait();  // the last leave() wakes us
        }
        return 0;
    }

    // ---- socket errors (EPOLLERR) ----------------------------------------------

    // The pending socket error (SO_ERROR) the poller took when it reported
    // EPOLLERR, 0 if none. Sticky until the next open(). See "Socket errors"
    // in the top comment. Any thread.
    int error() const noexcept { return error_.load(std::memory_order_acquire); }

    // After a syscall reported end of stream (0) or EPIPE on a socket: the
    // error that really ended it, if any (the poller's sticky one, else a
    // pending SO_ERROR that nobody has taken yet), 0 if it was an orderly
    // shutdown. Between enter() and leave().
    int error_after_shutdown() noexcept {
        if (const int e = error(); e != 0) {
            return e;
        }
        if (const int e = take_so_error(fd_); e != 0) {
            record_error(e);
            return error();
        }
        return error();  // the poller may have taken it meanwhile
    }

    // Racy snapshot: false between close() and the next open().
    bool is_open_approx() const noexcept {
        return (refs_.load(std::memory_order_relaxed) & kClosedBit) == 0;
    }

    // The poller the fd is registered with, and the scheduler (valid
    // between enter() and leave(), or for the owner while open).
    Poller* poller() const noexcept { return poller_; }
    Sched& scheduler() const noexcept { return *scheduler_; }

    // Racy snapshot of a word (tests).
    std::uintptr_t word_approx(Direction dir) const noexcept {
        return (dir == Direction::kRead ? rd_ : wr_).load(std::memory_order_relaxed);
    }

private:
    using Word = typename SyncPolicy::template Atomic<std::uintptr_t>;

    static constexpr std::uint32_t kClosedBit = std::uint32_t{1} << 31;
    static constexpr std::uint32_t kCountMask = kClosedBit - 1;

    enum class Arm : std::uint8_t { kArmed, kRetry, kClosed };

    Word& word_of(Direction dir) noexcept { return dir == Direction::kRead ? rd_ : wr_; }

    static bool is_waiter(std::uintptr_t value) noexcept { return value > kClosed; }

    // Waiter side: publish `waiter` in `word`, or report why not.
    static Arm arm(Word& word, Waiter& waiter) noexcept {
        waiter.result = 0;
        waiter.probe.write();
        std::uintptr_t cur = word.load(std::memory_order_relaxed);
        for (;;) {
            if (cur == kReady) {
                // relaxed: READY carries no data (see the top comment).
                if (word.compare_exchange_weak(cur, kEmpty, std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
                    return Arm::kRetry;
                }
                continue;
            }
            if (cur == kClosed) {
                return Arm::kClosed;
            }
            if (cur != kEmpty) [[unlikely]] {
                fail::fail_fast("net: two fibers wait on the same direction of one socket");
            }
            // release: the waiter's fields (and probe) to the waker;
            // seq_cst: the deadline Dekker in io().
            if (word.compare_exchange_weak(cur, reinterpret_cast<std::uintptr_t>(&waiter),
                                           std::memory_order_seq_cst, std::memory_order_relaxed)) {
                return Arm::kArmed;
            }
        }
    }

    // Hands `result` to a waiter taken out of a word and pushes its Task.
    static void wake(Waiter* waiter, int result, poller::TaskBatch& out) noexcept {
        waiter->probe.read();
        const typename Waiter::TakeFn take = waiter->take;
        void* const self = waiter->self;
        waiter->result = result;
        out.push(take(self));  // last touch of the waiter
    }

    // Poller side: an edge for `word`.
    static void notify(Word& word, poller::TaskBatch& out) noexcept {
        std::uintptr_t cur = word.load(std::memory_order_relaxed);
        for (;;) {
            if (cur == kClosed) {
                return;  // final; cannot happen after unwatch, harmless before
            }
            const std::uintptr_t next = is_waiter(cur) ? kEmpty : kReady;
            // acquire: the waiter's fields, published by its arm CAS. An RMW
            // even for READY -> READY: see "Why no edge is lost".
            if (word.compare_exchange_weak(cur, next, std::memory_order_acquire, std::memory_order_relaxed)) {
                break;
            }
        }
        if (is_waiter(cur)) {
            wake(reinterpret_cast<Waiter*>(cur), 0, out);
        }
    }

    static void cancel(Word& word, poller::TaskBatch& out) noexcept {
        // acquire: a waiter's fields, as in notify().
        const std::uintptr_t cur = word.exchange(kClosed, std::memory_order_acquire);
        if (is_waiter(cur)) {
            wake(reinterpret_cast<Waiter*>(cur), ECANCELED, out);
        }
    }

    // SO_ERROR (reading it clears it in the kernel); 0 if none or not a
    // socket.
    static int take_so_error(int fd) noexcept {
        int so_error = 0;
        socklen_t len = sizeof so_error;
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &len) != 0) {
            return 0;
        }
        return so_error;
    }

    // First error wins (a later SO_ERROR is a consequence of the first).
    void record_error(int e) noexcept {
        int expected = 0;
        // release / acquire with error(): the value is all that is handed
        // over; the waiters it concerns are woken after it (fiber wake).
        error_.compare_exchange_strong(expected, e, std::memory_order_release, std::memory_order_relaxed);
    }

    // Epollable callback (poller thread).
    static void on_events(void* memo, std::uint32_t events, poller::TaskBatch& out) noexcept {
        auto* self = static_cast<BasicPollDesc*>(memo);
        if ((events & EPOLLERR) != 0) {
            // Before the wakes below: both woken waiters (and later
            // operations) find the error that ended the connection, not the
            // EOF / EPIPE the kernel reports once SO_ERROR has been read.
            if (const int e = take_so_error(self->fd_); e != 0) {
                self->record_error(e);
            }
        }
        if ((events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR | EPOLLPRI)) != 0) {
            notify(self->rd_, out);
        }
        if ((events & (EPOLLOUT | EPOLLHUP | EPOLLERR)) != 0) {
            notify(self->wr_, out);
        }
    }

    struct CloseOp {
        BasicPollDesc* desc;
        Waiter* closer;
    };

    static constexpr poller::TimePoint kNoDeadline = poller::TimePoint::max();

    // One direction's deadline. desired / armed_at are guarded by
    // deadline_lock_; expired is read without it by the operations.
    struct Deadline {
        BasicPollDesc* desc = nullptr;
        Direction dir = Direction::kRead;
        Timer timer;                               // pending iff armed_at != kNoDeadline
        poller::TimePoint desired = kNoDeadline;  // the deadline set by the user
        poller::TimePoint armed_at = kNoDeadline;  // when `timer` fires
        typename SyncPolicy::template Atomic<std::uint32_t> expired{0};
    };

    Deadline& deadline_of(Direction dir) noexcept { return deadlines_[static_cast<int>(dir)]; }

    // Under deadline_lock_: schedules the direction's timer (idle) at `at`.
    void arm_timer(Deadline& d, poller::TimePoint at) noexcept {
        d.armed_at = at;
        d.timer.deadline = at;
        poller_->schedule(d.timer);
    }

    // The expirer's store; seq_cst: its half of the Dekker in io() (with the
    // load of the word in take_waiter()). One site for both expiry paths
    // (set_deadline() in the past, the timer), so the Twist test of one
    // checks the order of both.
    static void mark_expired(Deadline& d) noexcept { d.expired.store(1, std::memory_order_seq_cst); }

    // Takes the waiter out of `word` (nullptr if none). The load is seq_cst:
    // the expirer's half of the Dekker in io(); it also acquires the
    // waiter's fields (it reads the arm CAS's value), so the CAS needs no
    // order of its own beyond that (kept seq_cst: it is rare).
    // Twist: weakening the load and the CAS to relaxed races on the probe.
    static Waiter* take_waiter(Word& word) noexcept {
        std::uintptr_t cur = word.load(std::memory_order_seq_cst);
        while (is_waiter(cur)) {
            if (word.compare_exchange_weak(cur, kEmpty, std::memory_order_seq_cst, std::memory_order_seq_cst)) {
                return reinterpret_cast<Waiter*>(cur);
            }
        }
        return nullptr;
    }

    // Under deadline_lock_, poller thread: d's timer is idle (it has fired
    // or was withdrawn); act on the current deadline. true: expired now
    // (the caller takes and wakes the waiter after unlocking).
    bool reevaluate(Deadline& d) noexcept {
        if (d.desired == kNoDeadline) {
            return false;
        }
        if (d.desired > poller_->now()) {
            arm_timer(d, d.desired);
            return false;
        }
        if (d.expired.load(std::memory_order_relaxed) != 0) {
            return false;
        }
        mark_expired(d);
        return true;
    }

    void expire_into(Direction dir, poller::TaskBatch& out) noexcept {
        if (Waiter* w = take_waiter(word_of(dir))) {
            wake(w, 0, out);  // the waiter re-checks the expiry itself
        }
    }

    // A direction's deadline timer (poller thread; kCancelled only from
    // the poller's destructor).
    static void on_deadline_timer(void* memo, poller::TimerEvent event, poller::TaskBatch& out) noexcept {
        auto* d = static_cast<Deadline*>(memo);
        BasicPollDesc* desc = d->desc;
        bool fire = false;
        {
            std::lock_guard guard(desc->deadline_lock_);
            d->armed_at = kNoDeadline;
            if (desc->closing_ || event != poller::TimerEvent::kFired) {
                return;
            }
            fire = desc->reevaluate(*d);
        }
        if (fire) {
            desc->expire_into(d->dir, out);
        }
    }

    // The control timer (posted by set_deadline() when a deadline moved
    // EARLIER than its pending timer): withdraws that timer and re-arms it.
    static void on_control_timer(void* memo, poller::TimerEvent event, poller::TaskBatch& out) noexcept {
        auto* desc = static_cast<BasicPollDesc*>(memo);
        bool fire[2] = {false, false};
        {
            std::lock_guard guard(desc->deadline_lock_);
            desc->control_pending_ = false;
            if (desc->closing_ || event != poller::TimerEvent::kFired) {
                return;
            }
            for (int i = 0; i < 2; ++i) {
                Deadline& d = desc->deadlines_[i];
                if (d.armed_at != kNoDeadline && d.desired != kNoDeadline && d.armed_at > d.desired) {
                    static_cast<void>(desc->poller_->withdraw(d.timer));
                    d.armed_at = kNoDeadline;
                    fire[i] = desc->reevaluate(d);
                }
            }
        }
        for (int i = 0; i < 2; ++i) {
            if (fire[i]) {
                desc->expire_into(static_cast<Direction>(i), out);
            }
        }
    }

    // The close timer, on the owning poller (kFired), or on the thread that
    // destroys a stopped poller (kCancelled): unwatch is allowed in both.
    static void on_close_timer(void* memo, poller::TimerEvent event, poller::TaskBatch& out) noexcept {
        auto* op = static_cast<CloseOp*>(memo);
        BasicPollDesc* desc = op->desc;
        Waiter* closer = op->closer;
        static_cast<void>(desc->poller_->unwatch(desc->epollable_));
        {
            // No deadline timer may outlive the descriptor: take back the
            // pending ones (under the lock, so set_deadline() cannot arm a
            // new one after this; it sees closing_ instead).
            std::lock_guard guard(desc->deadline_lock_);
            desc->closing_ = true;
            if (desc->deadlines_used_ && event == poller::TimerEvent::kFired) {
                for (Deadline& d : desc->deadlines_) {
                    static_cast<void>(desc->poller_->withdraw(d.timer));
                    d.armed_at = kNoDeadline;
                }
                static_cast<void>(desc->poller_->withdraw(desc->control_));
                desc->control_pending_ = false;
            }
            // kCancelled: the poller is being destroyed and delivers every
            // pending timer itself (closing during the scheduler's
            // destruction is not supported for descriptors with deadlines).
        }
        cancel(desc->rd_, out);
        cancel(desc->wr_, out);
        // The closer was published to us by post() (the timer queue's
        // release / acquire); wake() reads its probe all the same.
        wake(closer, 0, out);  // last touch of op, timer and closer
    }

    // Waiter for the current context: a fiber parks, a plain thread sleeps
    // on an atomic. One-shot per wait(), reusable.
    class ContextWaiter {
    public:
        ContextWaiter() noexcept {
            Fiber* fiber = Fiber::current();
            if (fiber != nullptr) {
                waiter.self = fiber;
                waiter.take = &take_fiber;
            } else {
                waiter.self = this;
                waiter.take = &take_thread;
            }
        }

        void wait() noexcept {
            if (waiter.take == &take_fiber) {
                static_cast<Fiber*>(waiter.self)->park();
                return;
            }
            for (;;) {
                const std::uint32_t v = flag_.load(std::memory_order_acquire);
                if (v == kDone) {
                    break;
                }
                if (v == kIdle) {
                    flag_.wait(kIdle, std::memory_order_acquire);
                } else {
                    SyncPolicy::spin_hint();  // the waker is inside notify_one
                }
            }
            flag_.store(kIdle, std::memory_order_relaxed);  // the waker is gone
        }

        Waiter waiter;

    private:
        static constexpr std::uint32_t kIdle = 0;
        static constexpr std::uint32_t kNotifying = 1;
        static constexpr std::uint32_t kDone = 2;

        // kNotifying, notify, then kDone: the sleeper returns (and its
        // frame may go) only after kDone, i.e. after the waker's last touch.
        static Task take_thread(void* self) noexcept {
            auto* w = static_cast<ContextWaiter*>(self);
            w->flag_.store(kNotifying, std::memory_order_relaxed);  // no data: the sleeper spins on
            w->flag_.notify_one();
            w->flag_.store(kDone, std::memory_order_release);
            return Task{};
        }

        typename SyncPolicy::template Atomic<std::uint32_t> flag_{kIdle};
    };

    static Task take_fiber(void* self) noexcept {
        auto* fiber = static_cast<Fiber*>(self);
        return fiber->prepare_wake() ? fiber->task() : Task{};
    }

    Word rd_{kEmpty};
    Word wr_{kEmpty};
    typename SyncPolicy::template Atomic<int> error_{0};
    typename SyncPolicy::template Atomic<std::uint32_t> refs_{kClosedBit};
    int fd_ = -1;
    Sched* scheduler_ = nullptr;
    Poller* poller_ = nullptr;
    Waiter* closer_ = nullptr;
    poller::Epollable epollable_{};
    // Checker only: open()'s fields -> enter() / close() (see IoWaiter).
    [[no_unique_address]] typename Waiter::Probe open_probe_{};

    // Deadlines (see the top comment). All timers go to poller_.
    Deadline deadlines_[2];
    Timer control_;
    sync::SpinLock<SyncPolicy> deadline_lock_;
    bool control_pending_ = false;  // under deadline_lock_
    bool closing_ = false;          // under deadline_lock_
    bool deadlines_used_ = false;   // under deadline_lock_
};

}  // namespace fibers::net
