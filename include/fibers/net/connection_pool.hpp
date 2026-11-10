#pragma once

// ConnectionPool: a bounded pool of client connections for fibers (database,
// cache, upstream HTTP), with idle reuse, health checks, idle time /
// lifetime limits, acquire deadlines and a graceful close.
//
//   struct PgFactory {                              // makes connections
//       int open(PgConn& c, poller::TimePoint deadline) noexcept;   // connect + TLS + startup
//       bool check(PgConn& c) noexcept;             // optional: health check on checkout
//       void close(PgConn& c) noexcept;             // optional: graceful close (else c.close())
//   };
//   net::ConnectionPool<PgConn, PgFactory> pool(scheduler, PgFactory{...}, {.max_open = 16});
//   pool.start_reaper(10s);                         // optional
//   ... in a fiber:
//   net::ConnectionPool<PgConn, PgFactory>::Lease lease;
//   if (int e = pool.acquire(lease, scheduler.now() + 2s); e != 0) { ... ETIMEDOUT, ECANCELED, open's error }
//   if (!lease->query(...)) lease.mark_broken();    // protocol state unknown: do not reuse
//   // ~Lease: back to the pool (or closed if broken)
//   ...
//   pool.close();                                   // before destroying it, in a fiber
//
// Conn is any default-constructible, reusable connection object (e.g. a
// struct with a net::Connection / tls::Connection, a read buffer and
// protocol state). The pool owns max_open of them (an object pool sized
// once at construction: no allocation afterwards); Factory::open() opens a
// CLOSED one in place, and must leave it closed on failure; Factory::close()
// (or Conn::close()) closes it. A Conn object is reused for later
// connections, so open() must reset whatever protocol state it holds.
//
// Rules
// -----
//   * At most max_open connections exist at once: idle + leased + being
//     opened. An acquire that finds none idle and the limit reached waits
//     (FIFO) until a lease comes back (then that connection is handed to it
//     directly, or, if it was discarded, the right to open a new one), or
//     its deadline passes (ETIMEDOUT), or the pool is closed (ECANCELED).
//   * Idle connections are reused LIFO: the most recently returned one
//     first (warm: its TCP window, TLS session, server-side caches), so
//     the rest age at the tail and expire (max_idle_time) when load drops.
//   * Before an idle connection is handed out it is validated: lifetime
//     (max_lifetime since open), idle time (max_idle_time), invalidate()
//     epoch, and Factory::check() if the factory has one (only for
//     connections idle at least check_after_idle). A failed one is closed,
//     and the acquirer opens a new one in its place (keeping the capacity).
//   * A lease marked broken (mark_broken()) is closed on return instead of
//     being kept: after any I/O error or timeout mid-exchange the protocol
//     state is unknown. So is one returned past its lifetime, after
//     invalidate() or close(), or above max_idle.
//   * Expired idle connections are closed lazily (by acquire(), which
//     drops the expired tail of the idle list), by reap(), or by an
//     optional reaper fiber (start_reaper(interval)).
//   * close(): new acquires fail with ECANCELED, waiters wake with
//     ECANCELED, idle connections are closed, then close() waits until
//     every leased connection came back (and is closed) and the reaper has
//     exited. Destroy the pool only after that (or if it never had a
//     leased connection; destroying it with leases or waiters fails fast).
//
// Threads and fibers: acquire() and close() from fibers of Sched (they may
// wait, and Factory::open usually does I/O). A Lease may be returned from
// any fiber or thread. Factory calls run outside the pool's lock, in the
// calling fiber (open, check, close on acquire; close on return of a broken
// lease, in the returning fiber).
//
// Why not fiber::Semaphore for the bound: its acquire() can neither time
// out nor be cancelled, and the hand-off of a returned connection to the
// first waiter must happen in the same critical section that decides
// "nobody waits: idle it". So the count, the idle list and the FIFO of
// waiters are under one SpinLock (O(1) list operations, no waiting and no
// syscalls while held; Factory calls run outside it).
//
// Waiting with a deadline (the protocol)
// --------------------------------------
// A waiter is a node on the fiber's stack (+ a poller Timer when there is a
// deadline). Under the lock the acquirer schedules the timer, then queues
// the node, then unlocks and parks. The node is unlinked, and its result
// written, exactly once under the lock: by a resolver (a return handing
// over a connection or capacity, close()) or by the timer's callback
// (kFired: ETIMEDOUT). Whoever unlinks it wakes the fiber (after
// unlocking); a callback that finds the node unlinked already does nothing.
// After a resolver's wake the timer may still be pending, and the node (on
// the fiber's stack) must not go away while the poller references it.
// Poller::cancel() would not do: the poller lets go of a cancelled node
// only lazily (when it next wakes up for something else, maybe at the very
// deadline), so the fiber retires the timer itself: it posts a second node
// (`retire`, also on its stack) to the timer's poller and parks; that
// callback, on the poller thread, withdraw()s the timer if it is still
// pending (eager removal; if its callback has run already there is nothing
// left to do, both run on that one thread) and wakes the fiber. post()
// wakes a sleeping poller, so this costs one poller round trip, only for
// waits that were resolved before their deadline.

#include <cerrno>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <type_traits>
#include <utility>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/net/poll_desc.hpp"
#include "fibers/object_pool/sharded_pool.hpp"
#include "fibers/poller/sources.hpp"
#include "fibers/poller/task_batch.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/sync/spin_lock.hpp"

namespace fibers::net {

struct ConnectionPoolOptions {
    // Connections at once: idle + leased + being opened (>= 1).
    std::size_t max_open = 16;
    // Idle connections kept; one returned above this is closed.
    std::size_t max_idle = static_cast<std::size_t>(-1);
    // Close a connection idle this long (0: never).
    poller::Duration max_idle_time{0};
    // Close a connection this long after it was opened, at its next return
    // or checkout (0: never). Spreads reconnects (e.g. DNS changes,
    // server-side memory) without a thundering herd if you add jitter in
    // the factory.
    poller::Duration max_lifetime{0};
    // Factory::check() only for connections idle at least this long (0:
    // every checkout of an idle connection; Duration::max(): never).
    poller::Duration check_after_idle{0};
};

// A snapshot (stats()).
struct ConnectionPoolStats {
    std::size_t open = 0;     // idle + leased + being opened
    std::size_t idle = 0;
    std::size_t waiting = 0;  // fibers in acquire() waiting for a connection
    std::uint64_t opened = 0;        // Factory::open succeeded
    std::uint64_t open_failed = 0;   // Factory::open failed
    std::uint64_t reused = 0;        // an idle / returned connection handed out
    std::uint64_t closed = 0;        // connections the pool closed (any reason)
    std::uint64_t broken = 0;        // leases returned marked broken
    std::uint64_t expired = 0;       // closed for max_idle_time / max_lifetime
    std::uint64_t check_failed = 0;  // Factory::check said no
    std::uint64_t timeouts = 0;      // acquires that hit their deadline
};

// What the pool needs from a factory: open a closed Conn in place (0 or
// errno; on error the Conn must be closed again). `deadline`: the acquire's
// (TimePoint{}: none). Optional members, detected:
//   bool check(Conn&) noexcept   health check of an idle connection
//   void close(Conn&) noexcept   close (else Conn::close(), if any)
template <class F, class Conn>
concept ConnectionFactory = requires(F& f, Conn& c, poller::TimePoint deadline) {
    { f.open(c, deadline) } noexcept -> std::same_as<int>;
};

namespace detail {

// One connection object of a pool and its bookkeeping (from the pool's
// storage, reused). At namespace scope: a nested class's default member
// initializers are not usable (is_nothrow_default_constructible) before the
// enclosing class is complete.
template <class Conn>
struct PoolSlot {
    PoolSlot() noexcept = default;  // (time_point's constructor is not declared noexcept)

    Conn conn;
    PoolSlot* prev = nullptr;  // idle list (or a local chain: next only)
    PoolSlot* next = nullptr;
    poller::TimePoint opened_at{};
    poller::TimePoint idle_since{};
    std::uint64_t epoch = 0;
};

}  // namespace detail

template <class Conn, class Factory, NetScheduler Sched = sched::Scheduler>
    requires ConnectionFactory<Factory, Conn> && std::is_nothrow_default_constructible_v<Conn>
class BasicConnectionPool {
    using Slot = detail::PoolSlot<Conn>;

public:
    using Connection = Conn;
    using SyncPolicy = typename Sched::SyncPolicy;
    using Fiber = fiber::BasicFiber<Sched>;
    using Timer = typename Sched::Timer;

    // A leased connection: returns it to the pool when destroyed (or
    // reset()), or closes it if mark_broken() was called. Move-only; may be
    // moved to another fiber or thread.
    class Lease {
    public:
        Lease() noexcept = default;
        Lease(Lease&& other) noexcept
            : pool_(std::exchange(other.pool_, nullptr)),
              slot_(std::exchange(other.slot_, nullptr)),
              broken_(std::exchange(other.broken_, false)) {}

        Lease& operator=(Lease&& other) noexcept {
            if (this != &other) {
                reset();
                pool_ = std::exchange(other.pool_, nullptr);
                slot_ = std::exchange(other.slot_, nullptr);
                broken_ = std::exchange(other.broken_, false);
            }
            return *this;
        }

        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;

        ~Lease() { reset(); }

        explicit operator bool() const noexcept { return slot_ != nullptr; }
        Conn& operator*() const noexcept { return slot_->conn; }
        Conn* operator->() const noexcept { return &slot_->conn; }
        Conn* get() const noexcept { return slot_ != nullptr ? &slot_->conn : nullptr; }

        // Do not reuse: the pool closes the connection when it comes back.
        void mark_broken() noexcept { broken_ = true; }
        bool broken() const noexcept { return broken_; }

        // When Factory::open succeeded for this connection.
        poller::TimePoint opened_at() const noexcept { return slot_->opened_at; }

        // Returns the connection now (no-op if empty).
        void reset() noexcept {
            if (slot_ != nullptr) {
                BasicConnectionPool* pool = std::exchange(pool_, nullptr);
                Slot* slot = std::exchange(slot_, nullptr);
                const bool broken = std::exchange(broken_, false);
                pool->give_back(slot, broken);
            }
        }

    private:
        friend class BasicConnectionPool;

        Lease(BasicConnectionPool* pool, Slot* slot) noexcept : pool_(pool), slot_(slot) {}

        BasicConnectionPool* pool_ = nullptr;
        Slot* slot_ = nullptr;
        bool broken_ = false;
    };

    // Allocates the max_open connection objects (default-constructed,
    // closed). Fails fast if max_open == 0.
    BasicConnectionPool(Sched& scheduler, Factory factory, ConnectionPoolOptions options = {}) noexcept
        : scheduler_(scheduler),
          factory_(std::move(factory)),
          options_(normalize(options)),
          storage_(storage_options(options_)) {}

    BasicConnectionPool(const BasicConnectionPool&) = delete;
    BasicConnectionPool& operator=(const BasicConnectionPool&) = delete;

    // Closes the idle connections (Factory::close / Conn::close, on the
    // calling thread). Fails fast with leased connections, waiters or a
    // running reaper: close() first.
    ~BasicConnectionPool() {
        Slot* idle = nullptr;
        {
            std::lock_guard guard(lock_);
            if (open_ != idle_count_ || waiters_ != 0 || reaper_running_) [[unlikely]] {
                fail::fail_fast("net: ConnectionPool destroyed with leased connections, waiters or a reaper "
                                "(close() it first)");
            }
            idle = take_idle_locked();
            open_ = 0;
        }
        for (Slot* s = idle; s != nullptr;) {
            Slot* next = s->next;
            close_conn(*s);
            s = next;
        }
    }

    // ---- leasing -------------------------------------------------------------------

    // Leases a connection into `out` (which is reset first): an idle one if
    // there is a valid one, else a new one (Factory::open with `deadline`),
    // else waits for a return until `deadline` (TimePoint{}: no deadline).
    // 0, ETIMEDOUT (deadline passed while waiting; also at once when it has
    // already passed and nothing is free), ECANCELED (the pool is closed or
    // closing), or Factory::open's error. From a fiber of Sched when it has
    // to wait or open (fails fast otherwise).
    int acquire(Lease& out, poller::TimePoint deadline = {}) noexcept {
        out.reset();
        const bool timed = has_deadline(deadline);
        Slot* slot = nullptr;
        bool open = false;          // slot holds an open connection (to validate)
        Slot* dropped = nullptr;    // expired idle ones to close (chain via next)
        lock_.lock();
        if (closed_) {
            lock_.unlock();
            return ECANCELED;
        }
        const poller::TimePoint now = scheduler_.now();
        dropped = take_expired_tail_locked(now);
        if (idle_head_ != nullptr) {
            slot = pop_idle_head_locked();
            open = true;
        } else if (dropped != nullptr) {
            slot = dropped;  // reopen in place of an expired one
            dropped = dropped->next;
            open = true;     // validation closes it
        } else if (open_ < options_.max_open) {
            ++open_;  // ours: open a new one
        } else if (timed && deadline <= now) {
            lock_.unlock();
            counters_.timeouts.fetch_add(1, std::memory_order_relaxed);
            return ETIMEDOUT;
        } else {
            Fiber& self = Fiber::current_or_die("net: ConnectionPool::acquire() has to wait outside a fiber");
            Waiter w(this, &self, timed, false);
            if (timed) {
                w.timer.deadline = deadline;
                scheduler_.pollers().schedule(w.timer);  // its callback takes lock_ first
            }
            push_waiter_locked(w);
            lock_.unlock();
            park_waiter(self, w);
            if (w.result != 0) {
                if (w.result == ETIMEDOUT) {
                    counters_.timeouts.fetch_add(1, std::memory_order_relaxed);
                }
                return w.result;
            }
            slot = w.slot;  // a returned connection, or a slot to open (nullptr: take one)
            open = w.open;
            return finish_acquire(out, slot, open, deadline);
        }
        lock_.unlock();
        release_chain(dropped, /*expired=*/true);
        return finish_acquire(out, slot, open, deadline);
    }

    // acquire() with deadline now + timeout.
    int acquire_for(Lease& out, poller::Duration timeout) noexcept {
        return acquire(out, scheduler_.now() + timeout);
    }

    // ---- maintenance -------------------------------------------------------------------

    // Closes every expired idle connection now (max_idle_time,
    // max_lifetime). Returns how many. From a fiber (Factory::close may wait).
    std::size_t reap() noexcept {
        Slot* expired = nullptr;
        {
            std::lock_guard guard(lock_);
            if (closed_) {
                return 0;
            }
            const poller::TimePoint now = scheduler_.now();
            for (Slot* s = idle_tail_; s != nullptr;) {
                Slot* prev = s->prev;
                if (expired_at(*s, now)) {
                    unlink_idle_locked(*s);
                    s->next = expired;
                    expired = s;
                }
                s = prev;
            }
        }
        return release_chain(expired, /*expired=*/true);
    }

    // Starts a fiber that calls reap() every `interval` until close().
    // false: a reaper runs already, or the pool is closed.
    bool start_reaper(poller::Duration interval) noexcept {
        {
            std::lock_guard guard(lock_);
            if (closed_ || reaper_running_) {
                return false;
            }
            reaper_running_ = true;
        }
        fiber::go(scheduler_, [this, interval]() noexcept { reaper_loop(interval); });
        return true;
    }

    // Drops every connection made so far: idle ones are closed now, leased
    // ones when they come back (e.g. after a failover: the old primary's
    // connections must not be reused). From a fiber.
    void invalidate() noexcept {
        Slot* idle = nullptr;
        {
            std::lock_guard guard(lock_);
            ++epoch_;
            idle = take_idle_locked();
        }
        release_chain(idle, /*expired=*/false);
    }

    // See the top comment. 0, or EALREADY if the pool was closed (or is
    // being closed) by another call, which then does the waiting. From a
    // fiber when it has to wait.
    int close() noexcept {
        Slot* idle = nullptr;
        Waiter* wake = nullptr;  // waiters to wake after unlocking (chain via next)
        {
            std::lock_guard guard(lock_);
            if (closed_) {
                return EALREADY;
            }
            closed_ = true;
            while (waiter_head_ != nullptr) {
                Waiter* w = waiter_head_;
                static_cast<void>(resolve_locked(*w, ECANCELED, nullptr, false));
                w->next = wake;
                wake = w;
            }
            if (reaper_waiter_ != nullptr) {
                Waiter* w = reaper_waiter_;
                static_cast<void>(resolve_locked(*w, ECANCELED, nullptr, false));
                w->next = wake;
                wake = w;
            }
            idle = take_idle_locked();
        }
        while (wake != nullptr) {
            Waiter* next = wake->next;  // before the wake: the node goes with the fiber
            Fiber* fiber = wake->fiber;
            fiber->wake();
            wake = next;
        }
        release_chain(idle, /*expired=*/false);
        lock_.lock();
        if (open_ == 0 && !reaper_running_) {
            lock_.unlock();
            return 0;
        }
        Fiber& self = Fiber::current_or_die("net: ConnectionPool::close() has to wait outside a fiber");
        closer_ = &self;
        lock_.unlock();
        self.park();  // woken by the last release / the reaper's exit
        return 0;
    }

    ConnectionPoolStats stats() const noexcept {
        ConnectionPoolStats s;
        {
            std::lock_guard guard(lock_);
            s.open = open_;
            s.idle = idle_count_;
            s.waiting = waiters_;
        }
        s.opened = counters_.opened.load(std::memory_order_relaxed);
        s.open_failed = counters_.open_failed.load(std::memory_order_relaxed);
        s.reused = counters_.reused.load(std::memory_order_relaxed);
        s.closed = counters_.closed.load(std::memory_order_relaxed);
        s.broken = counters_.broken.load(std::memory_order_relaxed);
        s.expired = counters_.expired.load(std::memory_order_relaxed);
        s.check_failed = counters_.check_failed.load(std::memory_order_relaxed);
        s.timeouts = counters_.timeouts.load(std::memory_order_relaxed);
        return s;
    }

    const ConnectionPoolOptions& options() const noexcept { return options_; }
    Factory& factory() noexcept { return factory_; }
    Sched& scheduler() const noexcept { return scheduler_; }

private:
    using Counter = typename SyncPolicy::template Atomic<std::uint64_t>;

    // A fiber waiting in acquire() (or the reaper's sleep), on its stack.
    struct Waiter {
        Waiter(BasicConnectionPool* pool, Fiber* fiber, bool timed, bool reaper) noexcept
            : pool(pool),
              fiber(fiber),
              timer(this, &on_timer),
              retire(this, &on_retire),
              timed(timed),
              reaper(reaper) {}

        BasicConnectionPool* pool;
        Fiber* fiber;
        Waiter* prev = nullptr;
        Waiter* next = nullptr;
        Timer timer;   // the deadline
        Timer retire;  // posted to timer's poller to withdraw it (see the top comment)
        bool timed;
        bool reaper;  // the reaper's sleep: reaper_waiter_, not the FIFO
        bool linked = false;
        // Written under the lock by whoever unlinks the node, before the wake.
        bool fired = false;  // by the timer callback (the timer is done)
        int result = 0;
        Slot* slot = nullptr;
        bool open = false;
    };

    struct Counters {
        Counter opened{0};
        Counter open_failed{0};
        Counter reused{0};
        Counter closed{0};
        Counter broken{0};
        Counter expired{0};
        Counter check_failed{0};
        Counter timeouts{0};
    };

    static ConnectionPoolOptions normalize(ConnectionPoolOptions o) noexcept {
        if (o.max_open == 0) [[unlikely]] {
            fail::fail_fast("net: ConnectionPoolOptions::max_open must be at least 1");
        }
        return o;
    }

    // Exactly max_open objects, one shard, no per-thread caches (they could
    // strand objects on other threads and make acquire() grow the pool):
    // the pool never grows, since at most max_open slots are out at once.
    static object_pool::PoolOptions storage_options(const ConnectionPoolOptions& o) noexcept {
        object_pool::PoolOptions p;
        p.shards = 1;
        p.initial_objects = o.max_open;
        p.grow_chunk = 1;
        p.thread_cache_capacity = 0;
        return p;
    }

    static bool has_deadline(poller::TimePoint t) noexcept {
        return t != poller::TimePoint{} && t != poller::TimePoint::max();
    }

    bool lifetime_over(const Slot& s, poller::TimePoint now) const noexcept {
        return options_.max_lifetime.count() > 0 && now - s.opened_at >= options_.max_lifetime;
    }

    // An idle connection that must not be handed out any more.
    bool expired_at(const Slot& s, poller::TimePoint now) const noexcept {
        return (options_.max_idle_time.count() > 0 && now - s.idle_since >= options_.max_idle_time) ||
               lifetime_over(s, now);
    }

    // ---- Factory calls (outside the lock) ----------------------------------------------

    int open_conn(Conn& c, poller::TimePoint deadline) noexcept { return factory_.open(c, deadline); }

    void close_conn(Slot& s) noexcept {
        if constexpr (requires { { factory_.close(s.conn) } noexcept; }) {
            factory_.close(s.conn);
        } else if constexpr (requires { { s.conn.close() } noexcept; }) {
            static_cast<void>(s.conn.close());
        }
        counters_.closed.fetch_add(1, std::memory_order_relaxed);
    }

    bool check_conn(Slot& s, poller::TimePoint now) noexcept {
        if constexpr (requires { { factory_.check(s.conn) } noexcept -> std::same_as<bool>; }) {
            if (options_.check_after_idle != poller::Duration::max() &&
                now - s.idle_since >= options_.check_after_idle) {
                return factory_.check(s.conn);
            }
        }
        static_cast<void>(now);
        return true;
    }

    // ---- acquire's second half (no lock held) -------------------------------------------

    // `slot`: an idle / handed-over connection (open), a slot whose
    // connection is closed, or nullptr (take a fresh one); its capacity is
    // ours either way.
    int finish_acquire(Lease& out, Slot* slot, bool open, poller::TimePoint deadline) noexcept {
        if (open) {
            // (The epoch was checked under the lock: invalidate() empties
            // the idle list, give_back() hands over current ones only.)
            const poller::TimePoint now = scheduler_.now();
            bool valid = true;
            if (expired_at(*slot, now)) {
                counters_.expired.fetch_add(1, std::memory_order_relaxed);
                valid = false;
            }
            if (valid && !check_conn(*slot, now)) {
                counters_.check_failed.fetch_add(1, std::memory_order_relaxed);
                valid = false;
            }
            if (valid) {
                counters_.reused.fetch_add(1, std::memory_order_relaxed);
                out = Lease(this, slot);
                return 0;
            }
            close_conn(*slot);
        }
        if (slot == nullptr) {
            slot = storage_.acquire();
        }
        if (const int e = open_conn(slot->conn, deadline); e != 0) {
            counters_.open_failed.fetch_add(1, std::memory_order_relaxed);
            release_slot(slot);
            return e;
        }
        counters_.opened.fetch_add(1, std::memory_order_relaxed);
        slot->opened_at = scheduler_.now();
        {
            std::lock_guard guard(lock_);
            if (!closed_) {
                slot->epoch = epoch_;
                out = Lease(this, slot);
                return 0;
            }
        }
        close_conn(*slot);  // closed while we were opening it
        release_slot(slot);
        return ECANCELED;
    }

    // ---- returning -----------------------------------------------------------------------

    void give_back(Slot* slot, bool broken) noexcept {
        const poller::TimePoint now = scheduler_.now();
        if (broken) {
            counters_.broken.fetch_add(1, std::memory_order_relaxed);
        } else if (lifetime_over(*slot, now)) {
            counters_.expired.fetch_add(1, std::memory_order_relaxed);
        } else {
            Fiber* wake = nullptr;
            bool kept = false;
            slot->idle_since = now;  // also for a direct hand-off (validated by the taker)
            {
                std::lock_guard guard(lock_);
                if (!closed_ && slot->epoch == epoch_) {
                    if (waiter_head_ != nullptr) {
                        wake = resolve_locked(*waiter_head_, 0, slot, true);  // direct hand-off
                        kept = true;
                    } else if (idle_count_ < options_.max_idle) {
                        push_idle_head_locked(*slot);
                        kept = true;
                    }
                }
            }
            // Not a single access to *this past the unlock above (a closer
            // may be about to destroy the pool), only the fiber.
            if (kept) {
                if (wake != nullptr) {
                    wake->wake();
                }
                return;
            }
        }
        close_conn(*slot);
        release_slot(slot);
    }

    // A slot whose connection is closed (or was never opened): its capacity
    // goes to the first waiter (who opens a new connection in it), or back.
    void release_slot(Slot* slot) noexcept {
        Fiber* wake = nullptr;
        Fiber* closer = nullptr;
        {
            std::lock_guard guard(lock_);
            if (waiter_head_ != nullptr) {
                wake = resolve_locked(*waiter_head_, 0, slot, false);
            } else {
                storage_.release(slot);  // under the lock: before a closer can see open_ == 0
                --open_;
                closer = take_closer_if_done_locked();
            }
        }
        if (wake != nullptr) {
            wake->wake();
        }
        if (closer != nullptr) {
            closer->wake();  // last: close() may return and the pool go away
        }
    }

    // Closes a chain (via next) of slots and releases them. Returns how many.
    std::size_t release_chain(Slot* chain, bool expired) noexcept {
        std::size_t n = 0;
        while (chain != nullptr) {
            Slot* next = chain->next;
            if (expired) {
                counters_.expired.fetch_add(1, std::memory_order_relaxed);
            }
            close_conn(*chain);
            release_slot(chain);
            chain = next;
            ++n;
        }
        return n;
    }

    // ---- the reaper ------------------------------------------------------------------------

    void reaper_loop(poller::Duration interval) noexcept {
        Fiber& self = Fiber::current_or_die("net: ConnectionPool reaper outside a fiber");
        for (;;) {
            Waiter w(this, &self, true, true);
            {
                std::lock_guard guard(lock_);
                if (closed_) {
                    break;
                }
                w.timer.deadline = scheduler_.now() + interval;
                scheduler_.pollers().schedule(w.timer);
                w.linked = true;
                reaper_waiter_ = &w;
            }
            park_waiter(self, w);  // the timer fired, or close() resolved it
            if (w.result == ECANCELED) {
                break;
            }
            reap();
        }
        Fiber* closer = nullptr;
        {
            std::lock_guard guard(lock_);
            reaper_running_ = false;
            closer = take_closer_if_done_locked();
        }
        if (closer != nullptr) {
            closer->wake();
        }
    }

    // ---- waiters (lock held) -----------------------------------------------------------------

    void push_waiter_locked(Waiter& w) noexcept {
        w.prev = waiter_tail_;
        w.next = nullptr;
        (waiter_tail_ != nullptr ? waiter_tail_->next : waiter_head_) = &w;
        waiter_tail_ = &w;
        w.linked = true;
        ++waiters_;
    }

    void unlink_waiter_locked(Waiter& w) noexcept {
        if (w.reaper) {
            reaper_waiter_ = nullptr;
        } else {
            (w.prev != nullptr ? w.prev->next : waiter_head_) = w.next;
            (w.next != nullptr ? w.next->prev : waiter_tail_) = w.prev;
            --waiters_;
        }
        w.prev = w.next = nullptr;
        w.linked = false;
    }

    // Unlinks a linked waiter with its outcome. Returns the fiber, which the
    // caller wakes after unlocking.
    Fiber* resolve_locked(Waiter& w, int result, Slot* slot, bool open) noexcept {
        unlink_waiter_locked(w);
        w.result = result;
        w.slot = slot;
        w.open = open;
        return w.fiber;
    }

    // Parks until the waiter is resolved; then makes sure its timer is no
    // longer referenced by the poller (see the top comment).
    static void park_waiter(Fiber& self, Waiter& w) noexcept {
        self.park();  // one wake: from whoever unlinked the node
        if (w.timed && !w.fired) {
            auto* poller = static_cast<typename Sched::Poller*>(w.timer.poller());
            poller->post(w.retire);
            self.park();  // one wake: on_retire
        }
    }

    // The poller thread: a waiter's deadline. (kCancelled only when the
    // poller is destroyed with the timer pending: handled the same way.)
    static void on_timer(void* memo, poller::TimerEvent, poller::TaskBatch& out) noexcept {
        auto* w = static_cast<Waiter*>(memo);
        BasicConnectionPool* pool = w->pool;
        Fiber* fiber = nullptr;
        {
            std::lock_guard guard(pool->lock_);
            if (w->linked) {
                pool->unlink_waiter_locked(*w);
                w->result = ETIMEDOUT;
                w->fired = true;
                fiber = w->fiber;
            }
            // else: resolved already; the resolver wakes the fiber, which
            // then retires this (idle by now) timer.
        }
        // Not a single access to *w or *pool from here on.
        if (fiber != nullptr && fiber->prepare_wake()) {
            out.push(fiber->task());
        }
    }

    // The poller thread (the deadline timer's poller): the fiber was woken
    // by a resolver and waits for its timer to be let go of.
    static void on_retire(void* memo, poller::TimerEvent event, poller::TaskBatch& out) noexcept {
        auto* w = static_cast<Waiter*>(memo);
        Fiber* fiber = w->fiber;
        if (event == poller::TimerEvent::kFired) {
            // false: its callback has run already (on this thread); the node
            // is idle.
            static_cast<void>(static_cast<typename Sched::Poller*>(w->timer.poller())->withdraw(w->timer));
        }
        // kCancelled: the poller is being destroyed (stopped scheduler); it
        // drops every pending timer itself.
        if (fiber->prepare_wake()) {
            out.push(fiber->task());
        }
    }

    Fiber* take_closer_if_done_locked() noexcept {
        if (closer_ != nullptr && open_ == 0 && !reaper_running_) {
            return std::exchange(closer_, nullptr);
        }
        return nullptr;
    }

    // ---- the idle list (lock held; head = most recently returned) --------------------------

    void push_idle_head_locked(Slot& s) noexcept {
        s.prev = nullptr;
        s.next = idle_head_;
        (idle_head_ != nullptr ? idle_head_->prev : idle_tail_) = &s;
        idle_head_ = &s;
        ++idle_count_;
    }

    void unlink_idle_locked(Slot& s) noexcept {
        (s.prev != nullptr ? s.prev->next : idle_head_) = s.next;
        (s.next != nullptr ? s.next->prev : idle_tail_) = s.prev;
        s.prev = s.next = nullptr;
        --idle_count_;
    }

    Slot* pop_idle_head_locked() noexcept {
        Slot* s = idle_head_;
        unlink_idle_locked(*s);
        return s;
    }

    // The idle list as a chain (via next); the list is empty afterwards.
    Slot* take_idle_locked() noexcept {
        Slot* chain = idle_head_;
        for (Slot* s = chain; s != nullptr; s = s->next) {
            s->prev = nullptr;
        }
        idle_head_ = idle_tail_ = nullptr;
        idle_count_ = 0;
        return chain;
    }

    // Unlinks the expired connections at the tail (the longest idle) as a
    // chain (via next). Stops at the first valid one: idle time grows
    // towards the tail; a connection past its lifetime further up is caught
    // when it reaches the head or by reap().
    Slot* take_expired_tail_locked(poller::TimePoint now) noexcept {
        Slot* chain = nullptr;
        while (idle_tail_ != nullptr && expired_at(*idle_tail_, now)) {
            Slot* s = idle_tail_;
            unlink_idle_locked(*s);
            s->next = chain;
            chain = s;
        }
        return chain;
    }

    Sched& scheduler_;
    Factory factory_;
    const ConnectionPoolOptions options_;
    object_pool::DefaultPool<Slot, SyncPolicy> storage_;

    mutable sync::SpinLock<SyncPolicy> lock_;
    // Under lock_.
    std::size_t open_ = 0;
    std::size_t idle_count_ = 0;
    std::size_t waiters_ = 0;
    Slot* idle_head_ = nullptr;
    Slot* idle_tail_ = nullptr;
    Waiter* waiter_head_ = nullptr;
    Waiter* waiter_tail_ = nullptr;
    Waiter* reaper_waiter_ = nullptr;
    Fiber* closer_ = nullptr;
    std::uint64_t epoch_ = 0;
    bool closed_ = false;
    bool reaper_running_ = false;

    Counters counters_;
};

template <class Conn, class Factory>
using ConnectionPool = BasicConnectionPool<Conn, Factory, sched::Scheduler>;

}  // namespace fibers::net
