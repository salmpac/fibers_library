// net::Connection (plaintext): per-operation timeouts, explicit deadlines,
// BufferedReader on top, connect / accept / reuse. The TLS switch is tested
// in tests/tls/tls_connection_test.cpp.
//
// Fibers never use gtest assertions themselves: they record into atomics
// that the test thread checks after a Latch. Timing: only lower bounds
// ("a timeout did not fire early") and generous upper bounds.

#include <gtest/gtest.h>

#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <thread>

#include "fibers/fiber/fiber.hpp"
#include "fibers/http/stream.hpp"
#include "fibers/net/buffered_reader.hpp"
#include "fibers/net/connection.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace net = fibers::net;
using fibers::sched::Scheduler;
using net::Connection;
using net::IoResult;
using net::Stream;
using namespace std::chrono_literals;

static_assert(net::ReadStream<Connection>);
static_assert(fibers::http::ByteStream<Connection>);
static_assert(!Connection::kHasTls);

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

fibers::sched::SchedulerOptions options(std::size_t workers) {
    fibers::sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = 2;
    o.blocking.num_threads = 1;
    o.stacks.max_stacks = 256;
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

std::chrono::nanoseconds since(std::chrono::steady_clock::time_point t) { return std::chrono::steady_clock::now() - t; }

// Echo, then the peer goes silent: the next read fails with ETIMEDOUT after
// the io timeout (not before), and the connection still works afterwards
// (the deadline moves with the next operation).
TEST(Connection, IoTimeoutFiresOnSilentPeer) {
    Scheduler scheduler(options(2));
    Pair pair;
    Stream peer;
    Connection conn;
    ASSERT_EQ(peer.adopt(scheduler, pair.fd[0]), 0);
    ASSERT_EQ(conn.adopt(scheduler, pair.fd[1]), 0);
    conn.set_io_timeout(200ms);
    EXPECT_EQ(conn.io_timeout(), 200ms);
    Latch done(1);
    std::atomic<int> echo_error{-100};
    std::atomic<int> timeout_error{-100};
    std::atomic<std::int64_t> waited_ms{0};
    std::atomic<int> after_error{-100};
    std::atomic<bool> peer_go{false};
    fiber::go(scheduler, [&]() noexcept {
        int e = 0;
        for (int i = 0; i < 10 && e == 0; ++i) {
            const std::string msg = "ping " + std::to_string(i);
            e = conn.write_all(msg.data(), msg.size()).error;
            char buf[32];
            if (e == 0) {
                const IoResult r = conn.read_exact(buf, msg.size());
                e = r.error != 0 ? r.error : (std::string_view(buf, r.bytes) == msg ? 0 : -1);
            }
        }
        echo_error.store(e);
        // The peer is silent now.
        const auto start = std::chrono::steady_clock::now();
        char byte;
        const IoResult r = conn.read_some(&byte, 1);
        waited_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(since(start)).count());
        timeout_error.store(r.error);
        // The next operation re-arms: the connection goes on.
        peer_go.store(true);
        const IoResult again = conn.read_some(&byte, 1);
        after_error.store(again.error != 0 ? again.error : (byte == 'x' ? 0 : -1));
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        char buf[32];
        for (int i = 0; i < 10; ++i) {
            const std::string msg = "ping " + std::to_string(i);
            const IoResult r = peer.read_exact(buf, msg.size());
            if (!r || peer.write_all(buf, r.bytes).error != 0) {
                return;
            }
        }
        while (!peer_go.load()) {
            fiber::this_fiber::sleep_for(5ms);
        }
        static_cast<void>(peer.write_all("x", 1));
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(echo_error.load(), 0);
    EXPECT_EQ(timeout_error.load(), ETIMEDOUT);
    EXPECT_GE(waited_ms.load(), 190);  // not early (clock granularity)
    EXPECT_EQ(after_error.load(), 0);
}

// Writes are bounded too: the peer never reads, the send buffer fills up,
// write_all fails with ETIMEDOUT and reports what it moved.
TEST(Connection, IoTimeoutBoundsWrites) {
    Scheduler scheduler(options(2));
    Pair pair;
    Connection conn;
    ASSERT_EQ(conn.adopt(scheduler, pair.fd[1]), 0);
    conn.set_io_timeout(100ms);
    Latch done(1);
    std::atomic<int> error{-100};
    std::atomic<std::size_t> moved{0};
    fiber::go(scheduler, [&]() noexcept {
        static char big[8 << 20];
        const IoResult r = conn.write_all(big, sizeof big);
        error.store(r.error);
        moved.store(r.bytes);
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(error.load(), ETIMEDOUT);
    EXPECT_GT(moved.load(), 0u);
    EXPECT_LT(moved.load(), std::size_t{8 << 20});
    ::close(pair.fd[0]);
}

// With an io timeout, an earlier explicit deadline wins; clearing it brings
// back the per-operation one; without an io timeout explicit deadlines are
// passed through as is.
TEST(Connection, ExplicitDeadlines) {
    Scheduler scheduler(options(2));
    Pair pair;
    Connection conn;
    ASSERT_EQ(conn.adopt(scheduler, pair.fd[1]), 0);
    Latch done(1);
    std::atomic<int> explicit_wins{-100};
    std::atomic<std::int64_t> explicit_ms{0};
    std::atomic<int> past{-100};
    std::atomic<int> no_timeout_explicit{-100};
    std::atomic<int> after_clear{-100};
    fiber::go(scheduler, [&]() noexcept {
        char byte;
        conn.set_io_timeout(60s);
        const auto start = std::chrono::steady_clock::now();
        static_cast<void>(conn.set_read_deadline(scheduler.now() + 100ms));
        explicit_wins.store(conn.read_some(&byte, 1).error);  // 100ms, not 60s
        explicit_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(since(start)).count());
        static_cast<void>(conn.set_read_deadline(scheduler.now() - 1s));  // in the past: at once
        past.store(conn.read_some(&byte, 1).error);
        conn.set_io_timeout(0ns);  // explicit deadlines only
        static_cast<void>(conn.set_read_deadline(scheduler.now() + 50ms));
        no_timeout_explicit.store(conn.read_some(&byte, 1).error);
        static_cast<void>(conn.set_read_deadline(fibers::poller::TimePoint{}));  // none
        conn.set_io_timeout(80ms);
        after_clear.store(conn.read_some(&byte, 1).error);  // the io timeout again
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(explicit_wins.load(), ETIMEDOUT);
    EXPECT_GE(explicit_ms.load(), 90);
    EXPECT_LT(explicit_ms.load(), 30'000);
    EXPECT_EQ(past.load(), ETIMEDOUT);
    EXPECT_EQ(no_timeout_explicit.load(), ETIMEDOUT);
    EXPECT_EQ(after_clear.load(), ETIMEDOUT);
    ::close(pair.fd[0]);
}

// BufferedReader over a Connection: every fill is one read_some with its
// own deadline (an inactivity timeout); a timeout keeps the partial line.
TEST(Connection, UnderBufferedReader) {
    Scheduler scheduler(options(2));
    Pair pair;
    Stream peer;
    Connection conn;
    ASSERT_EQ(peer.adopt(scheduler, pair.fd[0]), 0);
    ASSERT_EQ(conn.adopt(scheduler, pair.fd[1]), 0);
    conn.set_io_timeout(150ms);
    Latch done(2);
    std::atomic<int> lines_ok{0};
    std::atomic<int> partial_timeout{-100};
    std::atomic<int> resumed{-100};
    std::atomic<bool> send_rest{false};
    fiber::go(scheduler, [&]() noexcept {
        std::byte storage[64];
        net::BufferedReader reader(conn, storage);
        for (int i = 0; i < 100; ++i) {
            const net::LineResult l = reader.read_line();
            if (l && l.line == "line " + std::to_string(i) + "\n") {
                lines_ok.fetch_add(1);
            }
        }
        net::LineResult l = reader.read_line();  // "part" arrives, then silence
        partial_timeout.store(l.error);
        send_rest.store(true);
        l = reader.read_line();
        resumed.store(l.error != 0 ? l.error : (l.line == "partial\n" ? 0 : -1));
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        for (int i = 0; i < 100; ++i) {
            const std::string line = "line " + std::to_string(i) + "\n";
            static_cast<void>(peer.write_all(line.data(), line.size()));
        }
        static_cast<void>(peer.write_all("part", 4));
        while (!send_rest.load()) {
            fiber::this_fiber::sleep_for(5ms);
        }
        static_cast<void>(peer.write_all("ial\n", 4));
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(lines_ok.load(), 100);
    EXPECT_EQ(partial_timeout.load(), ETIMEDOUT);
    EXPECT_EQ(resumed.load(), 0);
}

// connect / accept over TCP, close, then reuse the same objects; the io
// timeout survives a reopen, explicit deadlines do not.
TEST(Connection, ConnectAcceptAndReuse) {
    Scheduler scheduler(options(2));
    net::Listener listener;
    ASSERT_EQ(listener.listen(scheduler, net::Endpoint::loopback_v4(0)), 0);
    const net::Endpoint ep = listener.local_endpoint();
    Connection client;
    Connection server;
    client.set_io_timeout(30s);
    Latch done(2);
    std::atomic<int> client_error{-100};
    std::atomic<int> server_error{-100};
    fiber::go(scheduler, [&]() noexcept {
        int e = 0;
        for (int round = 0; round < 3 && e == 0; ++round) {
            e = server.accept(listener);
            char buf[5];
            if (e == 0) {
                const IoResult r = server.read_exact(buf, 5);
                e = r.error != 0 ? r.error : (r.bytes == 5 ? 0 : -1);
            }
            if (e == 0) {
                e = server.write_all(buf, 5).error;
            }
            if (e == 0) {
                e = server.shutdown();
            }
            char byte;
            if (e == 0) {
                const IoResult r = server.read_some(&byte, 1);  // the client's FIN
                e = r.error != 0 ? r.error : (r.bytes == 0 ? 0 : -2);
            }
            static_cast<void>(server.close());
        }
        server_error.store(e);
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        int e = 0;
        for (int round = 0; round < 3 && e == 0; ++round) {
            e = client.connect(scheduler, ep);
            if (e == 0 && round == 0) {
                // Gone after the reopen of round 1 (a fresh open has none).
                e = client.set_read_deadline(scheduler.now() - 1s);
                char byte;
                if (e == 0 && client.read_some(&byte, 1).error != ETIMEDOUT) {
                    e = -3;
                }
                if (e == 0) {
                    e = client.set_read_deadline(fibers::poller::TimePoint{});
                }
            }
            if (e == 0) {
                e = client.write_all("hello", 5).error;
            }
            char buf[5];
            if (e == 0) {
                const IoResult r = client.read_exact(buf, 5);
                e = r.error != 0 ? r.error : (std::string_view(buf, 5) == "hello" ? 0 : -1);
            }
            if (e == 0) {
                char byte;
                const IoResult r = client.read_some(&byte, 1);  // the server's FIN
                e = r.error != 0 ? r.error : (r.bytes == 0 ? 0 : -2);
            }
            static_cast<void>(client.close());
            if (e == 0 && client.io_timeout() != 30s) {
                e = -4;
            }
        }
        client_error.store(e);
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(client_error.load(), 0);
    EXPECT_EQ(server_error.load(), 0);
}

// Operations on a closed connection fail with EBADF, with or without an io
// timeout; close() wakes a parked reader with ECANCELED.
TEST(Connection, ClosedAndCloseWakesReader) {
    Scheduler scheduler(options(2));
    Connection idle;
    char byte;
    idle.set_io_timeout(1s);
    EXPECT_EQ(idle.read_some(&byte, 1).error, EBADF);
    EXPECT_EQ(idle.write_all("x", 1).error, EBADF);
    EXPECT_EQ(idle.close(), EBADF);

    Pair pair;
    Connection conn;
    ASSERT_EQ(conn.adopt(scheduler, pair.fd[1]), 0);
    conn.set_io_timeout(60s);
    Latch done(1);
    std::atomic<bool> reading{false};
    std::atomic<int> error{-100};
    fiber::go(scheduler, [&]() noexcept {
        reading.store(true);
        char b;
        error.store(conn.read_some(&b, 1).error);
        done.count_down();
    });
    while (!reading.load()) {
        std::this_thread::sleep_for(1ms);
    }
    std::this_thread::sleep_for(20ms);  // most likely parked by now; either way ECANCELED or EBADF
    EXPECT_EQ(conn.close(), 0);
    ASSERT_TRUE(done.wait());
    EXPECT_TRUE(error.load() == ECANCELED || error.load() == EBADF) << error.load();
    ::close(pair.fd[0]);
}

}  // namespace
