// net::ConnectionPool over a fake connection (exact control of opens,
// failures and health) and over real TCP connections (net::Connection to a
// local echo server).
//
// Fibers never use gtest assertions themselves: they record into atomics
// that the test thread checks after a Latch. Timing: no upper bounds that a
// busy machine could miss; deadlines only have to fire, not fire on time.

#include <gtest/gtest.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <thread>

#include "fibers/fiber/context.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/net/connection.hpp"
#include "fibers/net/connection_pool.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace net = fibers::net;
namespace this_fiber = fibers::fiber::this_fiber;
using fibers::poller::TimePoint;
using fibers::sched::Scheduler;
using net::ConnectionPoolOptions;
using namespace std::chrono_literals;

namespace {

class Latch {
public:
    explicit Latch(std::int64_t count) : count_(count) {}

    void count_down() noexcept { count_.fetch_sub(1, std::memory_order_acq_rel); }

    bool wait(std::chrono::milliseconds timeout = 120s) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (count_.load(std::memory_order_acquire) != 0) {
            if (std::chrono::steady_clock::now() > deadline) {
                return false;
            }
            std::this_thread::sleep_for(100us);
        }
        return true;
    }

private:
    std::atomic<std::int64_t> count_;
};

template <class Pred>
bool eventually(Pred pred, std::chrono::milliseconds timeout = 60s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

fibers::sched::SchedulerOptions options(std::size_t workers) {
    fibers::sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = 2;
    o.blocking.num_threads = 1;
    o.stacks.max_stacks = 1024;
    return o;
}

// ---- a fake connection ---------------------------------------------------------------

struct FakeConn {
    int id = 0;  // 1, 2, ... per successful open
    bool open = false;
    bool healthy = true;
};

struct FakeState {
    std::atomic<int> next_id{0};
    std::atomic<int> open_now{0};      // opened and not closed yet
    std::atomic<int> max_open_now{0};  // high-water mark of open_now
    std::atomic<int> opens{0};
    std::atomic<int> closes{0};
    std::atomic<int> checks{0};
    std::atomic<int> fail_opens{0};    // the next n opens fail with ECONNREFUSED
    std::atomic<bool> slow_open{false};
    std::atomic<int> bad_state{0};     // contract violations seen by the factory
};

// open / check / close, with check (health checks enabled).
struct FakeFactory {
    FakeState* s;

    int open(FakeConn& c, TimePoint) noexcept {
        if (c.open) {
            s->bad_state.fetch_add(1);  // the pool must hand us closed objects only
        }
        if (s->slow_open.load()) {
            this_fiber::sleep_for(2ms);
        }
        int n = s->fail_opens.load();
        while (n > 0 && !s->fail_opens.compare_exchange_weak(n, n - 1)) {
        }
        if (n > 0) {
            return ECONNREFUSED;
        }
        c.id = s->next_id.fetch_add(1) + 1;
        c.open = true;
        c.healthy = true;
        s->opens.fetch_add(1);
        const int now = s->open_now.fetch_add(1) + 1;
        int high = s->max_open_now.load();
        while (now > high && !s->max_open_now.compare_exchange_weak(high, now)) {
        }
        return 0;
    }

    bool check(FakeConn& c) noexcept {
        s->checks.fetch_add(1);
        return c.healthy;
    }

    void close(FakeConn& c) noexcept {
        if (!c.open) {
            s->bad_state.fetch_add(1);
        }
        c.open = false;
        s->closes.fetch_add(1);
        s->open_now.fetch_sub(1);
    }
};

// No check(): health checks off; Conn::close() is used.
struct PlainConn {
    bool open = false;
    int close() noexcept {
        open = false;
        return 0;
    }
};

struct PlainFactory {
    int open(PlainConn& c, TimePoint) noexcept {
        c.open = true;
        return 0;
    }
};

using Pool = net::ConnectionPool<FakeConn, FakeFactory>;
using Lease = Pool::Lease;

static_assert(net::ConnectionFactory<FakeFactory, FakeConn>);
static_assert(net::ConnectionFactory<PlainFactory, PlainConn>);

// Runs f() in a fiber and waits for it.
template <class F>
bool in_fiber(Scheduler& scheduler, F f) {
    Latch done(1);
    fiber::go(scheduler, [&]() noexcept {
        f();
        done.count_down();
    });
    return done.wait();
}

// ---- basics -----------------------------------------------------------------------

// Returned connections are reused most recent first (LIFO).
TEST(ConnectionPool, ReuseIsLifo) {
    Scheduler scheduler(options(2));
    FakeState state;
    Pool pool(scheduler, FakeFactory{&state}, ConnectionPoolOptions{.max_open = 4});
    int ids[3] = {};
    int again[3] = {};
    int errors = 0;
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept {
        {
            Lease a, b, c;
            errors += pool.acquire(a) + pool.acquire(b) + pool.acquire(c);
            ids[0] = a->id;
            ids[1] = b->id;
            ids[2] = c->id;
            a.reset();  // idle: a
            b.reset();  // idle: b, a
            c.reset();  // idle: c, b, a
        }
        Lease x, y, z;
        errors += pool.acquire(x) + pool.acquire(y) + pool.acquire(z);
        again[0] = x->id;
        again[1] = y->id;
        again[2] = z->id;
        x.reset();
        y.reset();
        z.reset();
        errors += pool.close();
    }));
    EXPECT_EQ(errors, 0);
    EXPECT_EQ(again[0], ids[2]);
    EXPECT_EQ(again[1], ids[1]);
    EXPECT_EQ(again[2], ids[0]);
    EXPECT_EQ(state.opens.load(), 3);
    EXPECT_EQ(state.open_now.load(), 0);
    EXPECT_EQ(state.bad_state.load(), 0);
    const net::ConnectionPoolStats s = pool.stats();
    EXPECT_EQ(s.opened, 3u);
    EXPECT_EQ(s.reused, 3u);
    EXPECT_EQ(s.open, 0u);
    EXPECT_EQ(s.closed, 3u);
}

// mark_broken(): closed on return, the next acquire opens a new one.
TEST(ConnectionPool, BrokenConnectionsAreDiscarded) {
    Scheduler scheduler(options(2));
    FakeState state;
    Pool pool(scheduler, FakeFactory{&state}, ConnectionPoolOptions{.max_open = 2});
    int first = 0;
    int second = 0;
    int errors = 0;
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept {
        Lease a;
        errors += pool.acquire(a);
        first = a->id;
        a.mark_broken();
        a.reset();
        Lease b;
        errors += pool.acquire(b);
        second = b->id;
        b.reset();
        errors += pool.close();
    }));
    EXPECT_EQ(errors, 0);
    EXPECT_NE(first, second);
    EXPECT_EQ(state.opens.load(), 2);
    EXPECT_EQ(state.closes.load(), 2);
    EXPECT_EQ(pool.stats().broken, 1u);
    EXPECT_EQ(state.bad_state.load(), 0);
}

// A failed health check closes the idle connection and opens a new one in
// its place; check_after_idle skips the check for recently used ones.
TEST(ConnectionPool, HealthCheckFailure) {
    Scheduler scheduler(options(2));
    FakeState state;
    Pool pool(scheduler, FakeFactory{&state}, ConnectionPoolOptions{.max_open = 1});
    int ids[3] = {};
    int errors = 0;
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept {
        Lease a;
        errors += pool.acquire(a);
        ids[0] = a->id;
        a.reset();
        errors += pool.acquire(a);  // checked, healthy: reused
        ids[1] = a->id;
        a->healthy = false;  // dies while idle
        a.reset();
        errors += pool.acquire(a);  // check fails: closed, reopened (max_open 1: in place)
        ids[2] = a->id;
        a.reset();
        errors += pool.close();
    }));
    EXPECT_EQ(errors, 0);
    EXPECT_EQ(ids[0], ids[1]);
    EXPECT_NE(ids[1], ids[2]);
    EXPECT_EQ(state.checks.load(), 2);
    EXPECT_EQ(pool.stats().check_failed, 1u);
    EXPECT_EQ(state.open_now.load(), 0);
    EXPECT_EQ(state.bad_state.load(), 0);

    FakeState lazy;
    Pool skip(scheduler, FakeFactory{&lazy}, ConnectionPoolOptions{.max_open = 1, .check_after_idle = 1h});
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept {
        Lease a;
        errors += skip.acquire(a);
        a.reset();
        errors += skip.acquire(a);
        a.reset();
        errors += skip.close();
    }));
    EXPECT_EQ(errors, 0);
    EXPECT_EQ(lazy.checks.load(), 0);
    EXPECT_EQ(lazy.opens.load(), 1);
}

// A factory without check() / close(): Conn::close() closes.
TEST(ConnectionPool, MinimalFactory) {
    Scheduler scheduler(options(1));
    net::ConnectionPool<PlainConn, PlainFactory> pool(scheduler, PlainFactory{}, ConnectionPoolOptions{.max_open = 1});
    bool open_while_leased = false;
    bool closed_after = false;
    PlainConn* conn = nullptr;
    int errors = 0;
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept {
        net::ConnectionPool<PlainConn, PlainFactory>::Lease a;
        errors += pool.acquire(a);
        conn = a.get();
        open_while_leased = a->open;
        a.mark_broken();
        a.reset();
        closed_after = !conn->open;
        errors += pool.close();
    }));
    EXPECT_EQ(errors, 0);
    EXPECT_TRUE(open_while_leased);
    EXPECT_TRUE(closed_after);
}

// Factory::open fails: the error comes back, the capacity is not lost.
TEST(ConnectionPool, OpenFailure) {
    Scheduler scheduler(options(2));
    FakeState state;
    Pool pool(scheduler, FakeFactory{&state}, ConnectionPoolOptions{.max_open = 1});
    state.fail_opens.store(2);
    int e1 = -1, e2 = -1, e3 = -1;
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept {
        Lease a;
        e1 = pool.acquire(a);
        e2 = pool.acquire(a);
        e3 = pool.acquire(a);  // max_open 1: would wait forever if a failed open leaked a slot
        a.reset();
        static_cast<void>(pool.close());
    }));
    EXPECT_EQ(e1, ECONNREFUSED);
    EXPECT_EQ(e2, ECONNREFUSED);
    EXPECT_EQ(e3, 0);
    EXPECT_EQ(pool.stats().open_failed, 2u);
    EXPECT_EQ(state.bad_state.load(), 0);
}

// ---- expiry ---------------------------------------------------------------------------

// max_idle_time: an idle connection past it is closed on the next acquire
// (lazily) instead of being handed out.
TEST(ConnectionPool, IdleExpiryOnAcquire) {
    Scheduler scheduler(options(2));
    FakeState state;
    Pool pool(scheduler, FakeFactory{&state}, ConnectionPoolOptions{.max_open = 4, .max_idle_time = 50ms});
    int first = 0;
    int second = 0;
    int errors = 0;
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept {
        Lease a, b;
        errors += pool.acquire(a) + pool.acquire(b);
        first = a->id;
        a.reset();
        b.reset();
        this_fiber::sleep_for(120ms);  // both idle past 50ms
        errors += pool.acquire(a);
        second = a->id;
        a.reset();
        errors += pool.close();
    }));
    EXPECT_EQ(errors, 0);
    EXPECT_NE(first, second);
    EXPECT_EQ(state.opens.load(), 3);
    EXPECT_EQ(pool.stats().expired, 2u);
    EXPECT_EQ(state.open_now.load(), 0);
    EXPECT_EQ(state.bad_state.load(), 0);
}

// reap() and the reaper fiber close expired idle connections without an
// acquire.
TEST(ConnectionPool, ReapAndReaper) {
    Scheduler scheduler(options(2));
    FakeState state;
    Pool pool(scheduler, FakeFactory{&state}, ConnectionPoolOptions{.max_open = 4, .max_idle_time = 30ms});
    std::size_t reaped = 0;
    int errors = 0;
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept {
        Lease a, b, c;
        errors += pool.acquire(a) + pool.acquire(b) + pool.acquire(c);
        a.reset();
        b.reset();
        c.reset();
        this_fiber::sleep_for(80ms);
        reaped = pool.reap();
    }));
    EXPECT_EQ(errors, 0);
    EXPECT_EQ(reaped, 3u);
    EXPECT_EQ(pool.stats().idle, 0u);
    EXPECT_EQ(state.open_now.load(), 0);

    EXPECT_TRUE(pool.start_reaper(10ms));
    EXPECT_FALSE(pool.start_reaper(10ms));  // one at a time
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept {
        Lease a, b;
        errors += pool.acquire(a) + pool.acquire(b);
    }));
    EXPECT_EQ(state.open_now.load(), 2);
    EXPECT_TRUE(eventually([&] { return state.open_now.load() == 0; }));  // the reaper got them
    EXPECT_EQ(pool.stats().idle, 0u);
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept { errors += pool.close(); }));  // stops the reaper
    EXPECT_EQ(errors, 0);
    EXPECT_FALSE(pool.start_reaper(10ms));  // closed
    EXPECT_EQ(state.bad_state.load(), 0);
}

// max_lifetime: closed on return once it has passed.
TEST(ConnectionPool, MaxLifetime) {
    Scheduler scheduler(options(2));
    FakeState state;
    Pool pool(scheduler, FakeFactory{&state}, ConnectionPoolOptions{.max_open = 2, .max_lifetime = 50ms});
    int errors = 0;
    std::size_t idle_after = 99;
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept {
        Lease a;
        errors += pool.acquire(a);
        this_fiber::sleep_for(100ms);
        a.reset();
        idle_after = pool.stats().idle;
        errors += pool.close();
    }));
    EXPECT_EQ(errors, 0);
    EXPECT_EQ(idle_after, 0u);
    EXPECT_EQ(pool.stats().expired, 1u);
    EXPECT_EQ(state.open_now.load(), 0);
}

// invalidate(): idle connections closed now, leased ones on return.
TEST(ConnectionPool, Invalidate) {
    Scheduler scheduler(options(2));
    FakeState state;
    Pool pool(scheduler, FakeFactory{&state}, ConnectionPoolOptions{.max_open = 4});
    int errors = 0;
    int open_after_invalidate = -1;
    std::size_t idle_after_return = 99;
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept {
        Lease a, b;
        errors += pool.acquire(a) + pool.acquire(b);
        a.reset();  // idle
        pool.invalidate();
        open_after_invalidate = state.open_now.load();  // b only
        b.reset();  // old epoch: closed
        idle_after_return = pool.stats().idle;
        errors += pool.acquire(a);  // a new one
        a.reset();
        errors += pool.close();
    }));
    EXPECT_EQ(errors, 0);
    EXPECT_EQ(open_after_invalidate, 1);
    EXPECT_EQ(idle_after_return, 0u);
    EXPECT_EQ(state.opens.load(), 3);
    EXPECT_EQ(state.open_now.load(), 0);
}

// ---- waiting ----------------------------------------------------------------------------

// All taken: acquire with a deadline fails with ETIMEDOUT (not before the
// deadline; at once if it has passed); without one it waits for the return
// and gets the very connection (direct hand-off).
TEST(ConnectionPool, AcquireDeadlineAndHandOff) {
    Scheduler scheduler(options(2));
    FakeState state;
    Pool pool(scheduler, FakeFactory{&state}, ConnectionPoolOptions{.max_open = 1});
    Latch done(2);
    std::atomic<int> timed_out{-100};
    std::atomic<std::int64_t> waited_ms{0};
    std::atomic<int> past{-100};
    std::atomic<int> held_id{0};
    std::atomic<int> handed_id{0};
    std::atomic<int> handed_error{-100};
    std::atomic<bool> holder_ready{false};
    std::atomic<bool> release{false};
    fiber::go(scheduler, [&]() noexcept {
        Lease a;
        if (pool.acquire(a) == 0) {
            held_id.store(a->id);
        }
        holder_ready.store(true);
        while (!release.load()) {
            this_fiber::sleep_for(1ms);
        }
        a.reset();
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        while (!holder_ready.load()) {
            this_fiber::sleep_for(1ms);
        }
        Lease b;
        const auto start = std::chrono::steady_clock::now();
        timed_out.store(pool.acquire(b, scheduler.now() + 100ms));
        waited_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)
                            .count());
        past.store(pool.acquire(b, scheduler.now() - 1ms));
        release.store(true);
        handed_error.store(pool.acquire(b));  // no deadline: the holder's connection
        if (b) {
            handed_id.store(b->id);
        }
        b.reset();
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(timed_out.load(), ETIMEDOUT);
    EXPECT_GE(waited_ms.load(), 90);
    EXPECT_EQ(past.load(), ETIMEDOUT);
    EXPECT_EQ(handed_error.load(), 0);
    EXPECT_EQ(handed_id.load(), held_id.load());
    EXPECT_EQ(state.opens.load(), 1);
    EXPECT_EQ(pool.stats().timeouts, 2u);
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept { static_cast<void>(pool.close()); }));
}

// A broken return hands the capacity (not the connection) to the waiter,
// which opens a new one.
TEST(ConnectionPool, BrokenReturnPassesCapacityToWaiter) {
    Scheduler scheduler(options(2));
    FakeState state;
    Pool pool(scheduler, FakeFactory{&state}, ConnectionPoolOptions{.max_open = 1});
    Latch done(2);
    std::atomic<int> first{0};
    std::atomic<int> second{0};
    std::atomic<int> error{-100};
    std::atomic<bool> holding{false};
    fiber::go(scheduler, [&]() noexcept {
        Lease a;
        if (pool.acquire(a) == 0) {
            first.store(a->id);
        }
        holding.store(true);
        while (pool.stats().waiting == 0) {
            this_fiber::sleep_for(1ms);
        }
        a.mark_broken();
        a.reset();
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        while (!holding.load()) {
            this_fiber::sleep_for(1ms);
        }
        Lease b;
        error.store(pool.acquire(b, scheduler.now() + 60s));
        if (b) {
            second.store(b->id);
        }
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(error.load(), 0);
    EXPECT_NE(first.load(), 0);
    EXPECT_NE(second.load(), first.load());
    EXPECT_EQ(state.max_open_now.load(), 1);
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept { static_cast<void>(pool.close()); }));
    EXPECT_EQ(state.open_now.load(), 0);
    EXPECT_EQ(state.bad_state.load(), 0);
}

// Many fibers on few workers: never more than max_open connections exist or
// are leased at once; everything is opened at most max_open times (no
// failures, no breaks); every connection is closed after close().
TEST(ConnectionPool, MaxOpenBoundUnderContention) {
    constexpr int kFibers = 64;
    constexpr int kRounds = 30;
    constexpr std::size_t kMaxOpen = 4;
    Scheduler scheduler(options(3));
    FakeState state;
    state.slow_open.store(true);  // opens overlap with acquires
    Pool pool(scheduler, FakeFactory{&state}, ConnectionPoolOptions{.max_open = kMaxOpen});
    Latch done(kFibers);
    std::atomic<int> leased{0};
    std::atomic<int> max_leased{0};
    std::atomic<int> errors{0};
    std::atomic<int> shared_use{0};  // two leases of one connection at once
    for (int i = 0; i < kFibers; ++i) {
        fiber::go(scheduler, [&, i]() noexcept {
            for (int r = 0; r < kRounds; ++r) {
                Lease lease;
                if (pool.acquire(lease) != 0) {
                    errors.fetch_add(1);
                    continue;
                }
                const int now = leased.fetch_add(1) + 1;
                int high = max_leased.load();
                while (now > high && !max_leased.compare_exchange_weak(high, now)) {
                }
                if (!lease->open || !lease->healthy) {
                    shared_use.fetch_add(1);
                }
                lease->healthy = false;  // "in use" marker: a second holder would see it
                if ((i + r) % 3 == 0) {
                    this_fiber::sleep_for(100us);
                } else {
                    this_fiber::yield();
                }
                lease->healthy = true;
                leased.fetch_sub(1);
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(errors.load(), 0);
    EXPECT_EQ(shared_use.load(), 0);
    EXPECT_LE(max_leased.load(), static_cast<int>(kMaxOpen));
    EXPECT_LE(state.max_open_now.load(), static_cast<int>(kMaxOpen));
    EXPECT_LE(state.opens.load(), static_cast<int>(kMaxOpen));
    int close_error = -1;
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept { close_error = pool.close(); }));
    EXPECT_EQ(close_error, 0);
    EXPECT_EQ(state.open_now.load(), 0);
    EXPECT_EQ(state.bad_state.load(), 0);
    EXPECT_EQ(pool.stats().open, 0u);
}

// Timed waiters racing with returns (grant vs deadline, the timer protocol):
// every acquire either gets a connection or ETIMEDOUT; nothing leaks or
// hangs, the bound holds.
TEST(ConnectionPool, ShortDeadlinesRaceWithReturns) {
    constexpr int kFibers = 48;
    constexpr int kRounds = 40;
    Scheduler scheduler(options(3));
    FakeState state;
    Pool pool(scheduler, FakeFactory{&state}, ConnectionPoolOptions{.max_open = 2});
    Latch done(kFibers);
    std::atomic<int> got{0};
    std::atomic<int> timeouts{0};
    std::atomic<int> other{0};
    std::atomic<int> leased{0};
    std::atomic<int> over{0};
    for (int i = 0; i < kFibers; ++i) {
        fiber::go(scheduler, [&, i]() noexcept {
            for (int r = 0; r < kRounds; ++r) {
                Lease lease;
                const int e = pool.acquire(lease, scheduler.now() + std::chrono::microseconds(50 + 37 * ((i + r) % 20)));
                if (e == ETIMEDOUT) {
                    timeouts.fetch_add(1);
                    continue;
                }
                if (e != 0) {
                    other.fetch_add(1);
                    continue;
                }
                got.fetch_add(1);
                if (leased.fetch_add(1) + 1 > 2) {
                    over.fetch_add(1);
                }
                this_fiber::yield();
                leased.fetch_sub(1);
                if ((i + r) % 7 == 0) {
                    lease.mark_broken();
                }
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(got.load() + timeouts.load(), kFibers * kRounds);
    EXPECT_EQ(other.load(), 0);
    EXPECT_EQ(over.load(), 0);
    EXPECT_GT(got.load(), 0);
    EXPECT_LE(state.max_open_now.load(), 2);
    int close_error = -1;
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept { close_error = pool.close(); }));
    EXPECT_EQ(close_error, 0);
    EXPECT_EQ(state.open_now.load(), 0);
    EXPECT_EQ(state.bad_state.load(), 0);
    const net::ConnectionPoolStats s = pool.stats();
    EXPECT_EQ(s.open, 0u);
    EXPECT_EQ(s.waiting, 0u);
    EXPECT_EQ(s.timeouts, static_cast<std::uint64_t>(timeouts.load()));
}

// close() with waiters (timed and untimed) and leased connections: waiters
// wake with ECANCELED, idle connections are closed at once, close() returns
// once the leases came back; later acquires fail with ECANCELED.
TEST(ConnectionPool, CloseWithWaitersAndLeases) {
    Scheduler scheduler(options(2));
    FakeState state;
    Pool pool(scheduler, FakeFactory{&state}, ConnectionPoolOptions{.max_open = 2});
    Latch holders(2);
    Latch waiters(2);
    Latch closer(1);
    std::atomic<int> held{0};
    std::atomic<bool> release{false};
    std::atomic<int> waiter_errors[2] = {-100, -100};
    std::atomic<int> close_error{-100};
    std::atomic<bool> close_returned{false};
    std::atomic<int> after_close{-100};
    std::atomic<int> second_close{-100};
    for (int i = 0; i < 2; ++i) {
        fiber::go(scheduler, [&]() noexcept {
            Lease a;
            if (pool.acquire(a) == 0) {
                held.fetch_add(1);
            }
            while (!release.load()) {
                this_fiber::sleep_for(1ms);
            }
            a.reset();
            holders.count_down();
        });
    }
    ASSERT_TRUE(eventually([&] { return held.load() == 2; }));
    for (int i = 0; i < 2; ++i) {
        fiber::go(scheduler, [&, i]() noexcept {
            Lease b;
            waiter_errors[i].store(i == 0 ? pool.acquire(b) : pool.acquire(b, scheduler.now() + 1h));
            waiters.count_down();
        });
    }
    ASSERT_TRUE(eventually([&] { return pool.stats().waiting == 2; }));
    fiber::go(scheduler, [&]() noexcept {
        close_error.store(pool.close());
        close_returned.store(true);
        Lease c;
        after_close.store(pool.acquire(c));
        closer.count_down();
    });
    ASSERT_TRUE(waiters.wait());
    EXPECT_EQ(waiter_errors[0].load(), ECANCELED);
    EXPECT_EQ(waiter_errors[1].load(), ECANCELED);
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept { second_close.store(pool.close()); }));
    EXPECT_EQ(second_close.load(), EALREADY);
    std::this_thread::sleep_for(20ms);
    EXPECT_FALSE(close_returned.load());  // two leases are still out
    EXPECT_EQ(state.open_now.load(), 2);
    release.store(true);
    ASSERT_TRUE(holders.wait());
    ASSERT_TRUE(closer.wait());
    EXPECT_EQ(close_error.load(), 0);
    EXPECT_EQ(after_close.load(), ECANCELED);
    EXPECT_EQ(state.open_now.load(), 0);
    EXPECT_EQ(pool.stats().open, 0u);
    EXPECT_EQ(state.bad_state.load(), 0);
}

// ---- contexts (fibers/fiber/context.hpp) ----------------------------------------------

// A waiting acquire under a context: cancel() ends it with ECANCELED, the
// context's deadline with ETIMEDOUT; the waiter leaves the FIFO and the
// capacity goes to the next one.
TEST(ConnectionPool, ContextInterruptsWaitingAcquire) {
    Scheduler scheduler(options(2));
    FakeState state;
    Pool pool(scheduler, FakeFactory{&state}, ConnectionPoolOptions{.max_open = 1});
    fibers::Context ctx = fibers::Context::cancellable();
    std::atomic<int> cancelled{-100};
    std::atomic<int> timed_out{-100};
    std::atomic<int> after{-100};
    std::atomic<bool> held{false};
    std::atomic<bool> release{false};
    Latch done(2);
    fiber::go(scheduler, [&]() noexcept {
        Lease a;
        if (pool.acquire(a) == 0) {
            held.store(true);
        }
        while (!release.load()) {
            this_fiber::sleep_for(1ms);
        }
        done.count_down();
    });
    fiber::go(scheduler, ctx, [&]() noexcept {
        while (!held.load()) {
            this_fiber::sleep_for(1ms);
        }
        Lease b;
        cancelled.store(pool.acquire(b));  // parks until ctx.cancel()
        {
            fibers::Context deadline = fibers::Context::with_timeout(30ms);
            fiber::ContextScope scope(deadline);
            timed_out.store(pool.acquire(b, scheduler.now() + 1h));  // the context's deadline is earlier
        }
        release.store(true);
        fiber::ContextScope none(nullptr);
        after.store(pool.acquire(b));  // the holder's connection, handed over
        done.count_down();
    });
    ASSERT_TRUE(eventually([&] { return ctx.has_dependants_approx(); }));
    EXPECT_EQ(pool.stats().waiting, 1u);
    ctx.cancel();
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(cancelled.load(), ECANCELED);
    EXPECT_EQ(timed_out.load(), ETIMEDOUT);
    EXPECT_EQ(after.load(), 0);
    EXPECT_EQ(pool.stats().waiting, 0u);
    EXPECT_EQ(state.opens.load(), 1);
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept { static_cast<void>(pool.close()); }));
}

// Under an ended context an acquire that would wait fails at once; one that
// finds an idle connection succeeds.
TEST(ConnectionPool, EndedContextFailsOnlyWaits) {
    Scheduler scheduler(options(2));
    FakeState state;
    Pool pool(scheduler, FakeFactory{&state}, ConnectionPoolOptions{.max_open = 1});
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept {
        Lease warm;
        static_cast<void>(pool.acquire(warm));  // one idle connection afterwards
    }));
    fibers::Context ctx = fibers::Context::cancellable();
    ctx.cancel();
    std::atomic<int> idle{-100};
    std::atomic<int> would_wait{-100};
    Latch done(1);
    fiber::go(scheduler, ctx, [&]() noexcept {
        Lease a;
        idle.store(pool.acquire(a));
        Lease b;
        would_wait.store(pool.acquire(b));
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(idle.load(), 0);
    EXPECT_EQ(would_wait.load(), ECANCELED);
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept { static_cast<void>(pool.close()); }));
}

// Returns race with cancellations: every waiter ends either with a
// connection or with ECANCELED, nothing is leaked or handed out twice.
TEST(ConnectionPool, ReturnsRaceWithCancels) {
    Scheduler scheduler(options(4));
    FakeState state;
    Pool pool(scheduler, FakeFactory{&state}, ConnectionPoolOptions{.max_open = 2});
    constexpr int kRounds = 100;
    constexpr int kWaiters = 4;
    std::atomic<int> errors{0};
    for (int round = 0; round < kRounds; ++round) {
        fibers::Context ctx = fibers::Context::cancellable();
        Latch done(kWaiters + 1);
        std::atomic<int> got{0};
        std::atomic<int> cancelled{0};
        for (int w = 0; w < kWaiters; ++w) {
            fiber::go(scheduler, ctx, [&, w]() noexcept {
                for (int i = 0; i < w; ++i) {
                    this_fiber::yield();
                }
                Lease l;
                const int e = pool.acquire(l);
                if (e == 0) {
                    got.fetch_add(1);
                    this_fiber::yield();  // hold it a moment: others wait
                } else if (e == ECANCELED) {
                    cancelled.fetch_add(1);
                } else {
                    errors.fetch_add(1);
                }
                done.count_down();
            });
        }
        fiber::go(scheduler, [&, round]() noexcept {
            for (int i = 0; i < round % 4; ++i) {
                this_fiber::yield();
            }
            ctx.cancel();
            done.count_down();
        });
        ASSERT_TRUE(done.wait()) << "round " << round;
        ASSERT_EQ(got.load() + cancelled.load(), kWaiters);
        ASSERT_FALSE(ctx.has_dependants_approx());
    }
    EXPECT_EQ(errors.load(), 0);
    EXPECT_EQ(pool.stats().waiting, 0u);
    EXPECT_LE(state.max_open_now.load(), 2);
    EXPECT_EQ(state.bad_state.load(), 0);
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept { static_cast<void>(pool.close()); }));
}

// Destroying a pool that still has a lease out fails fast.
TEST(ConnectionPoolDeathTest, DestroyWithLease) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            Scheduler scheduler(options(1));
            FakeState state;
            auto* pool = new Pool(scheduler, FakeFactory{&state}, ConnectionPoolOptions{.max_open = 1});
            auto* lease = new Lease;
            Latch done(1);
            fiber::go(scheduler, [&]() noexcept {
                static_cast<void>(pool->acquire(*lease));
                done.count_down();
            });
            static_cast<void>(done.wait());
            delete pool;
        },
        "leased connections");
}

// ---- real connections -----------------------------------------------------------------

// net::Connection to a local echo server through the pool: factory =
// connect with the io timeout; health check = still open.
struct EchoFactory {
    Scheduler* scheduler;
    net::Endpoint server;
    std::atomic<int>* connects;

    int open(net::Connection& c, TimePoint deadline) noexcept {
        c.set_io_timeout(30s);
        const int e = c.connect(*scheduler, server, deadline);
        if (e == 0) {
            connects->fetch_add(1);
            static_cast<void>(c.set_nodelay());
        }
        return e;
    }

    bool check(net::Connection& c) noexcept { return c.is_open_approx(); }
};

TEST(ConnectionPool, RealConnectionsToEchoServer) {
    constexpr int kClients = 16;
    constexpr int kRounds = 20;
    Scheduler scheduler(options(3));
    net::Listener listener;
    ASSERT_EQ(listener.listen(scheduler, net::Endpoint::loopback_v4(0)), 0);
    std::atomic<int> connects{0};
    std::atomic<int> accepted{0};
    std::atomic<bool> accept_loop_done{false};
    net::Connection server_conns[64];
    // Echo server: one fiber per connection.
    fiber::go(scheduler, [&]() noexcept {
        for (int i = 0; i < 64; ++i) {
            net::Connection& s = server_conns[i];
            if (s.accept(listener) != 0) {
                break;  // listener closed
            }
            accepted.fetch_add(1);
            fiber::go([&s]() noexcept {
                char buf[256];
                for (;;) {
                    const net::IoResult r = s.read_some(buf, sizeof buf);
                    if (!r || r.bytes == 0 || !s.write_all(buf, r.bytes)) {
                        break;
                    }
                }
                static_cast<void>(s.close());
            });
        }
        accept_loop_done.store(true);
    });
    using EchoPool = net::ConnectionPool<net::Connection, EchoFactory>;
    EchoPool pool(scheduler, EchoFactory{&scheduler, listener.local_endpoint(), &connects},
                  ConnectionPoolOptions{.max_open = 4});
    Latch done(kClients);
    std::atomic<int> ok{0};
    std::atomic<int> failed{0};
    for (int i = 0; i < kClients; ++i) {
        fiber::go(scheduler, [&, i]() noexcept {
            for (int r = 0; r < kRounds; ++r) {
                EchoPool::Lease lease;
                if (pool.acquire(lease, scheduler.now() + 60s) != 0) {
                    failed.fetch_add(1);
                    continue;
                }
                const std::string msg = "client " + std::to_string(i) + " round " + std::to_string(r);
                char buf[64];
                const bool good = lease->write_all(msg.data(), msg.size()) &&
                                  lease->read_exact(buf, msg.size()).bytes == msg.size() &&
                                  std::string_view(buf, msg.size()) == msg;
                if (good) {
                    ok.fetch_add(1);
                } else {
                    lease.mark_broken();
                    failed.fetch_add(1);
                }
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(ok.load(), kClients * kRounds);
    EXPECT_EQ(failed.load(), 0);
    EXPECT_LE(connects.load(), 4);
    int close_error = -1;
    ASSERT_TRUE(in_fiber(scheduler, [&]() noexcept { close_error = pool.close(); }));
    EXPECT_EQ(close_error, 0);
    static_cast<void>(listener.close());
    EXPECT_TRUE(eventually([&] { return accept_loop_done.load(); }));
    // The server fibers see end of stream and close their side.
    EXPECT_TRUE(eventually([&] {
        for (net::Connection& s : server_conns) {
            if (s.is_open_approx()) {
                return false;
            }
        }
        return true;
    }));
}

}  // namespace
