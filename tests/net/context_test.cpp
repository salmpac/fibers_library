// Network waits under a fiber::BasicContext (docs/cancellation.md): every
// PollDesc wait (read, write, accept, connect) ends with ECANCELED when the
// context is cancelled and ETIMEDOUT at its deadline; the effective
// deadline is min(socket deadline, context deadline). Real Scheduler,
// socketpairs and loopback TCP.
//
// No timing assertions: deadlines are generous and only outcomes are
// checked. "Parked under the context" is observed through
// has_dependants_approx() (an Interrupt is enlisted exactly while a wait is
// parked under it). Fibers record into atomics; the test thread checks.

#include <gtest/gtest.h>

#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <new>
#include <thread>
#include <vector>

#include "fibers/fiber/context.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/wait_group.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace net = fibers::net;
namespace this_fiber = fibers::fiber::this_fiber;
using fibers::Context;
using fibers::sched::Scheduler;
using fibers::wait::Error;
using net::Direction;
using net::Endpoint;
using net::IoResult;
using net::Listener;
using net::Stream;
using namespace std::chrono_literals;

namespace {

class Latch {
public:
    explicit Latch(std::int64_t count) : count_(count) {}
    void count_down() noexcept { count_.fetch_sub(1, std::memory_order_acq_rel); }
    bool wait(std::chrono::milliseconds timeout = 60s) {
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
bool eventually(Pred pred, std::chrono::milliseconds timeout = 30s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(50us);
    }
    return true;
}

fibers::sched::SchedulerOptions options(std::size_t workers, std::size_t pollers = 2) {
    fibers::sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = pollers;
    o.blocking.num_threads = 1;
    o.stacks.max_stacks = 4096;
    return o;
}

struct Pair {
    int fd[2] = {-1, -1};
    Pair() {
        if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fd) != 0) {
            fd[0] = fd[1] = -1;
        }
    }
};

// A socketpair whose first end is a Stream; the other end stays a raw fd.
struct StreamPair {
    Pair pair;
    Stream stream;
    explicit StreamPair(Scheduler& scheduler) {
        if (stream.adopt(scheduler, pair.fd[0]) != 0) {
            std::abort();
        }
    }
    ~StreamPair() {
        static_cast<void>(stream.close());
        ::close(pair.fd[1]);
    }
    void send_byte() const { static_cast<void>(::write(pair.fd[1], "x", 1)); }
};

// ---- read ------------------------------------------------------------------------

TEST(NetContext, CancelWakesParkedReader) {
    Scheduler scheduler(options(2));
    StreamPair p(scheduler);
    Context ctx = Context::cancellable();
    std::atomic<int> error{-1};
    Latch done(1);
    fiber::go(scheduler, ctx, [&]() noexcept {
        char c;
        error.store(p.stream.read_some(&c, 1).error);
        done.count_down();
    });
    ASSERT_TRUE(eventually([&] { return ctx.has_dependants_approx(); }));  // parked under ctx
    EXPECT_TRUE(ctx.cancel());
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(error.load(), ECANCELED);
    EXPECT_FALSE(ctx.has_dependants_approx());
    // The stream itself is fine: data that arrives later is read (outside
    // the context).
    p.send_byte();
    std::atomic<std::size_t> got{0};
    Latch again(1);
    fiber::go(scheduler, [&]() noexcept {
        char c;
        got.store(p.stream.read_some(&c, 1).bytes);
        again.count_down();
    });
    ASSERT_TRUE(again.wait());
    EXPECT_EQ(got.load(), 1u);
}

// Under an ended context a read that would wait fails at once, one that
// finds data does not wait and succeeds.
TEST(NetContext, EndedContextFailsOnlyWaits) {
    Scheduler scheduler(options(2));
    StreamPair p(scheduler);
    Context ctx = Context::cancellable();
    ctx.cancel();
    p.send_byte();
    std::atomic<std::size_t> first{0};
    std::atomic<int> first_error{-1};
    std::atomic<int> second_error{-1};
    Latch done(1);
    fiber::go(scheduler, ctx, [&]() noexcept {
        char c;
        const IoResult a = p.stream.read_some(&c, 1);  // data: no wait
        first.store(a.bytes);
        first_error.store(a.error);
        second_error.store(p.stream.read_some(&c, 1).error);  // would wait
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(first_error.load(), 0);
    EXPECT_EQ(first.load(), 1u);
    EXPECT_EQ(second_error.load(), ECANCELED);
}

TEST(NetContext, ContextDeadlineEndsRead) {
    Scheduler scheduler(options(2));
    StreamPair p(scheduler);
    Context ctx = Context::with_timeout(scheduler, 30ms);
    std::atomic<int> error{-1};
    Latch done(1);
    fiber::go(scheduler, ctx, [&]() noexcept {
        static_cast<void>(p.stream.set_read_deadline(scheduler.now() + 1h));  // later: the context's wins
        char c;
        error.store(p.stream.read_some(&c, 1).error);
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(error.load(), ETIMEDOUT);
    EXPECT_EQ(ctx.reason(), Error::kTimedOut);
    EXPECT_FALSE(p.stream.poll_desc().expired_approx(Direction::kRead));  // not the socket's deadline
}

// min(socket, context): an earlier socket deadline ends the read while the
// context lives on.
TEST(NetContext, EarlierSocketDeadlineWins) {
    Scheduler scheduler(options(2));
    StreamPair p(scheduler);
    Context ctx = Context::with_timeout(scheduler, 1h);
    std::atomic<int> error{-1};
    Latch done(1);
    fiber::go(scheduler, ctx, [&]() noexcept {
        static_cast<void>(p.stream.set_read_deadline(scheduler.now() + 30ms));
        char c;
        error.store(p.stream.read_some(&c, 1).error);
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(error.load(), ETIMEDOUT);
    EXPECT_FALSE(ctx.cancelled());
    EXPECT_TRUE(p.stream.poll_desc().expired_approx(Direction::kRead));
}

// ---- write, accept, connect --------------------------------------------------------

TEST(NetContext, CancelWakesBlockedWriter) {
    Scheduler scheduler(options(2));
    StreamPair p(scheduler);
    Context ctx = Context::cancellable();
    std::vector<char> big(16 << 20, 'b');
    std::atomic<int> error{-1};
    std::atomic<std::size_t> bytes{0};
    Latch done(1);
    fiber::go(scheduler, ctx, [&]() noexcept {
        const IoResult r = p.stream.write_all(big.data(), big.size());
        error.store(r.error);
        bytes.store(r.bytes);
        done.count_down();
    });
    ASSERT_TRUE(eventually([&] { return ctx.has_dependants_approx(); }));  // the buffer is full
    ctx.cancel();
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(error.load(), ECANCELED);
    EXPECT_LT(bytes.load(), big.size());
}

TEST(NetContext, CancelWakesAccept) {
    Scheduler scheduler(options(2));
    Listener listener;
    ASSERT_EQ(listener.listen(scheduler, Endpoint::loopback_v4(0)), 0);
    Context ctx = Context::cancellable();
    std::atomic<int> error{-1};
    Latch done(1);
    fiber::go(scheduler, ctx, [&]() noexcept {
        Stream s;
        error.store(listener.accept(s));
        done.count_down();
    });
    ASSERT_TRUE(eventually([&] { return ctx.has_dependants_approx(); }));
    ctx.cancel();
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(error.load(), ECANCELED);
    EXPECT_EQ(listener.close(), 0);
}

// A connect that hangs (a listener whose backlog is full drops SYNs on
// Linux) ends with the context's deadline; the half-open socket is gone.
TEST(NetContext, ContextDeadlineEndsConnect) {
    constexpr int kMaxAttempts = 64;
    Scheduler scheduler(options(2));
    Listener full;
    ASSERT_EQ(full.listen(scheduler, Endpoint::loopback_v4(0), net::ListenOptions{.backlog = 1}), 0);
    const Endpoint endpoint = full.local_endpoint();
    std::vector<std::unique_ptr<Stream>> filled;
    std::atomic<int> result{-1};
    std::atomic<bool> closed_after{false};
    std::atomic<int> reason{-1};
    Latch done(1);
    fiber::go(scheduler, [&]() noexcept {
        for (int i = 0; i < kMaxAttempts; ++i) {
            auto s = std::make_unique<Stream>();
            Context ctx = Context::with_timeout(100ms);
            int e;
            {
                fiber::ContextScope scope(ctx);
                e = s->connect(scheduler, endpoint);  // no deadline of its own
            }
            if (e == 0) {
                filled.push_back(std::move(s));
                continue;
            }
            result.store(e);
            closed_after.store(!s->is_open_approx());
            reason.store(ctx.reason() ? static_cast<int>(*ctx.reason()) : 0);
            break;
        }
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    for (auto& s : filled) {
        EXPECT_EQ(s->close(), 0);
    }
    EXPECT_EQ(full.close(), 0);
    if (result.load() != ETIMEDOUT) {
        GTEST_SKIP() << "no connect hung against a full backlog (result " << result.load() << ")";
    }
    EXPECT_TRUE(closed_after.load());
    EXPECT_EQ(reason.load(), static_cast<int>(Error::kTimedOut));
}

// ---- many waits, races ----------------------------------------------------------------

// Many fibers parked in reads under one context (spawned by a handler
// with wg.go, so they inherit it) all wake with ECANCELED; the handler's
// join is not interrupted and returns after them.
TEST(NetContext, ManyReadersOneCancel) {
    constexpr int kReaders = 200;
    Scheduler scheduler(options(4));
    std::vector<std::unique_ptr<StreamPair>> pairs;
    for (int i = 0; i < kReaders; ++i) {
        pairs.push_back(std::make_unique<StreamPair>(scheduler));
    }
    Context ctx = Context::cancellable();
    std::atomic<int> cancelled{0};
    std::atomic<int> parked{0};
    Latch done(1);
    fiber::go(scheduler, ctx, [&]() noexcept {
        fiber::WaitGroup wg;
        for (int i = 0; i < kReaders; ++i) {
            wg.go([&, i]() noexcept {
                parked.fetch_add(1);
                char c;
                if (pairs[i]->stream.read_some(&c, 1).error == ECANCELED) {
                    cancelled.fetch_add(1);
                }
            });
        }
        wg.wait();
        done.count_down();
    });
    ASSERT_TRUE(eventually([&] { return parked.load() == kReaders && ctx.has_dependants_approx(); }));
    ctx.cancel();
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(cancelled.load(), kReaders);
    EXPECT_FALSE(ctx.has_dependants_approx());
}

// Readiness and cancellation race on the same word: the read either gets
// the byte or ECANCELED, exactly once, and never hangs. Fibers yield first
// so they migrate between workers around the race.
TEST(NetContext, DataRacesWithCancel) {
    Scheduler scheduler(options(4));
    constexpr int kRounds = 300;
    int got_data = 0;
    int got_cancel = 0;
    for (int round = 0; round < kRounds; ++round) {
        StreamPair p(scheduler);
        Context ctx = Context::cancellable();
        std::atomic<int> error{-1};
        std::atomic<std::size_t> bytes{0};
        Latch done(3);
        fiber::go(scheduler, ctx, [&]() noexcept {
            for (int i = 0; i < round % 3; ++i) {
                this_fiber::yield();
            }
            char c;
            const IoResult r = p.stream.read_some(&c, 1);
            error.store(r.error);
            bytes.store(r.bytes);
            done.count_down();
        });
        fiber::go(scheduler, [&]() noexcept {
            p.send_byte();
            done.count_down();
        });
        fiber::go(scheduler, [&]() noexcept {
            for (int i = 0; i < round % 5; ++i) {
                this_fiber::yield();
            }
            ctx.cancel();
            done.count_down();
        });
        ASSERT_TRUE(done.wait()) << "round " << round;
        if (error.load() == 0) {
            ASSERT_EQ(bytes.load(), 1u);
            ++got_data;
        } else {
            ASSERT_EQ(error.load(), ECANCELED) << "round " << round;
            ++got_cancel;
        }
        ASSERT_FALSE(ctx.has_dependants_approx());
    }
    EXPECT_EQ(got_data + got_cancel, kRounds);
}

// A parent's cancellation reaches a reader parked under a child context
// (switched to with ContextScope), and a deadline of the child alone ends
// only the child's waits.
TEST(NetContext, ChildContexts) {
    Scheduler scheduler(options(2));
    StreamPair a(scheduler);
    StreamPair b(scheduler);
    Context root = Context::cancellable();
    std::atomic<int> child_deadline_error{-1};
    std::atomic<bool> root_alive_after{false};
    std::atomic<int> parent_cancel_error{-1};
    Latch first(1);
    Latch second(1);
    fiber::go(scheduler, root, [&]() noexcept {
        char c;
        {
            Context step = Context::child_of(root, scheduler.now() + 20ms);
            fiber::ContextScope scope(step);
            child_deadline_error.store(a.stream.read_some(&c, 1).error);
        }
        root_alive_after.store(!root.cancelled());
        first.count_down();
        Context child = Context::child_of(root);
        fiber::ContextScope scope(child);
        parent_cancel_error.store(b.stream.read_some(&c, 1).error);
        second.count_down();
    });
    ASSERT_TRUE(first.wait());
    EXPECT_EQ(child_deadline_error.load(), ETIMEDOUT);
    EXPECT_TRUE(root_alive_after.load());
    ASSERT_TRUE(eventually([&] { return root.has_dependants_approx(); }));  // the child is enlisted...
    std::this_thread::sleep_for(1ms);  // (...and very likely its reader parked: either way it ends)
    root.cancel();
    ASSERT_TRUE(second.wait());
    EXPECT_EQ(parent_cancel_error.load(), ECANCELED);
}

// ---- lifetimes ----------------------------------------------------------------------

TEST(NetContextDeathTest, DestroyWhileAFiberIsParkedUnderIt) {
    EXPECT_DEATH(
        {
            Scheduler scheduler(options(1));
            StreamPair p(scheduler);
            alignas(Context) unsigned char slot[sizeof(Context)];
            auto* ctx = ::new (slot) Context(Context::cancellable());
            fiber::go(scheduler, *ctx, [&]() noexcept {
                char c;
                static_cast<void>(p.stream.read_some(&c, 1));
            });
            while (!ctx->has_dependants_approx()) {
                std::this_thread::sleep_for(100us);
            }
            ctx->~Context();
        },
        "fiber waits under it");
}

}  // namespace
