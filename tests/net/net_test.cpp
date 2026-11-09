// net::Stream / net::Listener on a real Scheduler (StdSync, epoll pollers).
//
// Fibers never use gtest assertions themselves: they record into atomics
// that the test thread checks after a Latch.

#include <gtest/gtest.h>

#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "fibers/fiber/fiber.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace net = fibers::net;
namespace this_fiber = fibers::fiber::this_fiber;
using fibers::sched::Scheduler;
using net::Direction;
using net::Endpoint;
using net::IoResult;
using net::Listener;
using net::Stream;
using namespace std::chrono_literals;

namespace {

// See tests/fiber/fiber_test.cpp. Declare it BEFORE the scheduler.
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

bool parked(Stream& s, Direction dir) {
    return s.poll_desc().word_approx(dir) > net::BasicPollDesc<Scheduler>::kClosed;
}

std::uint8_t pattern(std::size_t i, std::size_t seed) {
    return static_cast<std::uint8_t>((i * 131 + seed * 7) & 0xFF);
}

// Echoes until end of stream or an error, then closes.
void echo(Stream& s, std::atomic<int>& errors) {
    char buf[4096];
    for (;;) {
        const IoResult r = s.read_some(buf, sizeof buf);
        if (!r) {
            errors.fetch_add(1);
            break;
        }
        if (r.bytes == 0) {
            break;
        }
        if (!s.write_all(buf, r.bytes)) {
            errors.fetch_add(1);
            break;
        }
    }
    if (s.close() != 0) {
        errors.fetch_add(1);
    }
}

// Sends messages of varying sizes, checks the echo.
void client_rounds(Stream& s, std::size_t rounds, std::size_t seed, std::atomic<int>& errors) {
    std::vector<std::uint8_t> out(8192);
    std::vector<std::uint8_t> in(8192);
    for (std::size_t round = 0; round < rounds; ++round) {
        const std::size_t size = (round * 997 + seed * 13) % out.size() + 1;
        for (std::size_t i = 0; i < size; ++i) {
            out[i] = pattern(i, seed + round);
        }
        if (!s.write_all(out.data(), size)) {
            errors.fetch_add(1);
            return;
        }
        const IoResult r = s.read_exact(in.data(), size);
        if (!r || r.bytes != size || std::memcmp(in.data(), out.data(), size) != 0) {
            errors.fetch_add(1);
            return;
        }
    }
}

struct Pair {
    int fd[2] = {-1, -1};
    Pair() {
        if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fd) != 0) {
            fd[0] = fd[1] = -1;
        }
    }
};

TEST(Net, SocketpairEcho) {
    Latch done(2);
    std::atomic<int> errors{0};
    Scheduler scheduler(options(2));
    Pair pair;
    Stream a;
    Stream b;
    ASSERT_EQ(a.adopt(scheduler, pair.fd[0]), 0);
    ASSERT_EQ(b.adopt(scheduler, pair.fd[1]), 0);
    fiber::go(scheduler, [&]() noexcept {
        echo(b, errors);
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        client_rounds(a, 200, 1, errors);
        if (a.shutdown(SHUT_WR) != 0) {
            errors.fetch_add(1);
        }
        char c;
        const IoResult eof = a.read_some(&c, 1);  // the echo side closes
        if (!eof || eof.bytes != 0) {
            errors.fetch_add(1);
        }
        if (a.close() != 0) {
            errors.fetch_add(1);
        }
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(errors.load(), 0);
    EXPECT_EQ(a.read_some(nullptr, 0).error, EBADF);
    EXPECT_EQ(a.close(), EBADF);
}

// Many connections over loopback TCP, a fiber per connection on both sides,
// several rounds each with varying sizes; workers and pollers > 1, so wakes
// cross threads.
TEST(Net, LoopbackManyConnections) {
    constexpr std::size_t kConnections = 300;
    constexpr std::size_t kRounds = 20;
    Latch clients(kConnections);
    Latch servers(kConnections + 1);
    std::atomic<int> errors{0};
    Scheduler scheduler(options(4, 2));
    Listener listener;
    ASSERT_EQ(listener.listen(scheduler, Endpoint::loopback_v4(0)), 0);
    const Endpoint endpoint = listener.local_endpoint();
    ASSERT_NE(endpoint.port(), 0);
    auto server_streams = std::make_unique<Stream[]>(kConnections);
    fiber::go(scheduler, [&]() noexcept {
        for (std::size_t i = 0; i < kConnections; ++i) {
            Stream& s = server_streams[i];
            if (listener.accept(s) != 0) {
                errors.fetch_add(1);
                break;
            }
            fiber::go<Scheduler>([&errors, &servers, stream = &s]() noexcept {
                echo(*stream, errors);
                servers.count_down();
            });
        }
        servers.count_down();
    });
    for (std::size_t c = 0; c < kConnections; ++c) {
        fiber::go(scheduler, [&, c]() noexcept {
            Stream s;
            const int error = s.connect(scheduler, endpoint);
            if (error != 0) {
                errors.fetch_add(1);
            } else {
                static_cast<void>(s.set_nodelay());
                client_rounds(s, kRounds, c, errors);
                if (s.close() != 0) {
                    errors.fetch_add(1);
                }
            }
            clients.count_down();
        });
    }
    ASSERT_TRUE(clients.wait());
    ASSERT_TRUE(servers.wait());
    EXPECT_EQ(errors.load(), 0);
    EXPECT_EQ(listener.close(), 0);
}

// read_some with nothing to read parks the fiber (the read word holds its
// waiter); a write by the peer wakes it. Then the same for write_all on a
// full send buffer.
TEST(Net, EagainParksUntilReady) {
    Latch done(1);
    std::atomic<std::size_t> got{0};
    std::atomic<int> errors{0};
    Scheduler scheduler(options(2));
    Pair pair;
    Stream a;
    ASSERT_EQ(a.adopt(scheduler, pair.fd[0]), 0);
    fiber::go(scheduler, [&]() noexcept {
        char buf[16];
        const IoResult r = a.read_some(buf, sizeof buf);
        got.store(r ? r.bytes : 0);
        done.count_down();
    });
    ASSERT_TRUE(eventually([&] { return parked(a, Direction::kRead); }));
    ASSERT_EQ(::write(pair.fd[1], "hello", 5), 5);
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(got.load(), 5u);

    // Fill the send buffer: the writer must park on the write word.
    constexpr std::size_t kTotal = 8 << 20;
    std::vector<char> big(kTotal, 'x');
    Latch written(1);
    fiber::go(scheduler, [&]() noexcept {
        const IoResult r = a.write_all(big.data(), big.size());
        if (!r || r.bytes != kTotal) {
            errors.fetch_add(1);
        }
        written.count_down();
    });
    ASSERT_TRUE(eventually([&] { return parked(a, Direction::kWrite); }));
    std::vector<char> sink(1 << 16);
    std::size_t drained = 0;
    while (drained < kTotal) {
        const ssize_t n = ::read(pair.fd[1], sink.data(), sink.size());
        ASSERT_GT(n, 0);
        drained += static_cast<std::size_t>(n);
    }
    ASSERT_TRUE(written.wait());
    EXPECT_EQ(errors.load(), 0);
    EXPECT_EQ(a.close(), 0);
    ::close(pair.fd[1]);
}

// A parked reader sees end of stream when the peer shuts down its write
// side (EPOLLRDHUP) and when the peer closes (EPOLLHUP).
TEST(Net, PeerShutdownAndCloseGiveEof) {
    Latch done(2);
    std::atomic<int> eofs{0};
    Scheduler scheduler(options(2));
    Pair p1;
    Pair p2;
    Stream a;
    Stream b;
    ASSERT_EQ(a.adopt(scheduler, p1.fd[0]), 0);
    ASSERT_EQ(b.adopt(scheduler, p2.fd[0]), 0);
    for (Stream* s : {&a, &b}) {
        fiber::go(scheduler, [&, s]() noexcept {
            char c;
            const IoResult r = s->read_some(&c, 1);
            if (r && r.bytes == 0) {
                eofs.fetch_add(1);
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(eventually([&] { return parked(a, Direction::kRead) && parked(b, Direction::kRead); }));
    ASSERT_EQ(::shutdown(p1.fd[1], SHUT_WR), 0);
    ::close(p2.fd[1]);
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(eofs.load(), 2);
    // Writing to a closed peer: EPIPE (MSG_NOSIGNAL, no SIGPIPE).
    Latch wrote(1);
    std::atomic<int> write_error{0};
    fiber::go(scheduler, [&]() noexcept {
        const IoResult r = b.write_all("x", 1);
        write_error.store(r.error);
        wrote.count_down();
    });
    ASSERT_TRUE(wrote.wait());
    EXPECT_EQ(write_error.load(), EPIPE);
    EXPECT_EQ(a.close(), 0);
    EXPECT_EQ(b.close(), 0);
    ::close(p1.fd[1]);
}

// close() while a fiber is parked in read_some: the reader gets ECANCELED,
// close() returns after it left. Closers: a plain thread, then a fiber.
TEST(Net, CloseWakesParkedReader) {
    Scheduler scheduler(options(2));
    for (int round = 0; round < 2; ++round) {
        const bool from_fiber = round == 1;
        Latch done(from_fiber ? 2 : 1);
        std::atomic<int> result{-1};
        std::atomic<int> close_result{-1};
        Pair pair;
        Stream a;
        ASSERT_EQ(a.adopt(scheduler, pair.fd[0]), 0);
        fiber::go(scheduler, [&]() noexcept {
            char c;
            result.store(a.read_some(&c, 1).error);
            done.count_down();
        });
        ASSERT_TRUE(eventually([&] { return parked(a, Direction::kRead); }));
        if (from_fiber) {
            fiber::go(scheduler, [&]() noexcept {
                close_result.store(a.close());
                done.count_down();
            });
        } else {
            close_result.store(a.close());
        }
        ASSERT_TRUE(done.wait());
        EXPECT_EQ(close_result.load(), 0);
        EXPECT_EQ(result.load(), ECANCELED);
        EXPECT_FALSE(a.is_open_approx());
        char c;
        EXPECT_EQ(a.read_some(&c, 1).error, EBADF);
        ::close(pair.fd[1]);
    }
}

// Reader and writer parked at once on one stream; close wakes both.
TEST(Net, CloseWakesReaderAndWriter) {
    Latch done(2);
    std::atomic<int> read_error{-1};
    std::atomic<int> write_error{-1};
    Scheduler scheduler(options(3));
    Pair pair;
    Stream a;
    ASSERT_EQ(a.adopt(scheduler, pair.fd[0]), 0);
    std::vector<char> big(8 << 20, 'y');
    fiber::go(scheduler, [&]() noexcept {
        char c;
        read_error.store(a.read_some(&c, 1).error);
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        write_error.store(a.write_all(big.data(), big.size()).error);
        done.count_down();
    });
    ASSERT_TRUE(eventually([&] { return parked(a, Direction::kRead) && parked(a, Direction::kWrite); }));
    EXPECT_EQ(a.close(), 0);
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(read_error.load(), ECANCELED);
    EXPECT_EQ(write_error.load(), ECANCELED);
    ::close(pair.fd[1]);
}

TEST(Net, ListenerCloseWakesAccept) {
    Latch done(1);
    std::atomic<int> result{-1};
    Scheduler scheduler(options(2));
    Listener listener;
    ASSERT_EQ(listener.listen(scheduler, Endpoint::loopback_v4(0)), 0);
    Stream s;
    fiber::go(scheduler, [&]() noexcept {
        result.store(listener.accept(s));
        done.count_down();
    });
    ASSERT_TRUE(eventually([&] { return listener.native_handle() >= 0; }));
    std::this_thread::sleep_for(20ms);  // let it park (not required for correctness)
    EXPECT_EQ(listener.close(), 0);
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(result.load(), ECANCELED);
    EXPECT_FALSE(s.is_open_approx());
}

TEST(Net, ConnectRefused) {
    Latch done(1);
    std::atomic<int> result{-1};
    Scheduler scheduler(options(2));
    Endpoint endpoint;
    {
        Listener listener;
        ASSERT_EQ(listener.listen(scheduler, Endpoint::loopback_v4(0)), 0);
        endpoint = listener.local_endpoint();
    }  // closed: nobody listens on that port any more
    Stream s;
    fiber::go(scheduler, [&]() noexcept {
        result.store(s.connect(scheduler, endpoint));
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(result.load(), ECONNREFUSED);
    EXPECT_FALSE(s.is_open_approx());
}

// 32 MB through loopback TCP with small socket buffers and a slow reader:
// write_all goes through many write-readiness rounds.
TEST(Net, LargeWriteSeveralRounds) {
    constexpr std::size_t kTotal = 32 << 20;
    Latch done(2);
    std::atomic<int> errors{0};
    std::atomic<bool> writer_parked{false};
    Scheduler scheduler(options(2));
    Listener listener;
    ASSERT_EQ(listener.listen(scheduler, Endpoint::loopback_v4(0)), 0);
    const Endpoint endpoint = listener.local_endpoint();
    Stream server;
    Stream client;
    std::vector<std::uint8_t> data(kTotal);
    for (std::size_t i = 0; i < kTotal; ++i) {
        data[i] = pattern(i, 5);
    }
    fiber::go(scheduler, [&]() noexcept {
        if (listener.accept(server) != 0) {
            errors.fetch_add(1);
            done.count_down();
            return;
        }
        std::vector<std::uint8_t> buf(1 << 15);
        std::size_t got = 0;
        std::size_t reads = 0;
        while (got < kTotal) {
            const IoResult r = server.read_some(buf.data(), buf.size());
            if (!r || r.bytes == 0) {
                errors.fetch_add(1);
                break;
            }
            for (std::size_t i = 0; i < r.bytes; ++i) {
                if (buf[i] != data[got + i]) {
                    errors.fetch_add(1);
                    break;
                }
            }
            got += r.bytes;
            if (++reads % 64 == 0) {
                this_fiber::sleep_for(100us);  // slow reader
            }
        }
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        if (client.connect(scheduler, endpoint) != 0) {
            errors.fetch_add(1);
            done.count_down();
            return;
        }
        const int small = 128 << 10;
        static_cast<void>(client.set_option(SOL_SOCKET, SO_SNDBUF, &small, sizeof small));
        const IoResult r = client.write_all(data.data(), data.size());
        if (!r || r.bytes != kTotal) {
            errors.fetch_add(1);
        }
        done.count_down();
    });
    // Sample the write word until both are done.
    ASSERT_TRUE(eventually([&] {
        if (client.is_open_approx() && parked(client, Direction::kWrite)) {
            writer_parked.store(true);
        }
        return writer_parked.load();
    }));
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(errors.load(), 0);
    EXPECT_TRUE(writer_parked.load());
    EXPECT_EQ(client.close(), 0);
    EXPECT_EQ(server.close(), 0);
    EXPECT_EQ(listener.close(), 0);
}

// One fiber reads while another writes on the same stream (one waiter per
// direction): full duplex against an echo peer.
TEST(Net, FullDuplexReaderAndWriterFibers) {
    constexpr std::size_t kTotal = 4 << 20;
    Latch done(3);
    std::atomic<int> errors{0};
    Scheduler scheduler(options(3));
    Pair pair;
    Stream a;
    Stream b;
    ASSERT_EQ(a.adopt(scheduler, pair.fd[0]), 0);
    ASSERT_EQ(b.adopt(scheduler, pair.fd[1]), 0);
    fiber::go(scheduler, [&]() noexcept {
        echo(b, errors);
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        std::vector<std::uint8_t> chunk(3000);
        std::size_t sent = 0;
        while (sent < kTotal) {
            const std::size_t n = std::min(chunk.size(), kTotal - sent);
            for (std::size_t i = 0; i < n; ++i) {
                chunk[i] = pattern(sent + i, 9);
            }
            if (!a.write_all(chunk.data(), n)) {
                errors.fetch_add(1);
                break;
            }
            sent += n;
        }
        static_cast<void>(a.shutdown(SHUT_WR));
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        std::vector<std::uint8_t> buf(5000);
        std::size_t got = 0;
        for (;;) {
            const IoResult r = a.read_some(buf.data(), buf.size());
            if (!r) {
                errors.fetch_add(1);
                break;
            }
            if (r.bytes == 0) {
                break;
            }
            for (std::size_t i = 0; i < r.bytes; ++i) {
                if (buf[i] != pattern(got + i, 9)) {
                    errors.fetch_add(1);
                    break;
                }
            }
            got += r.bytes;
        }
        if (got != kTotal) {
            errors.fetch_add(1);
        }
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(errors.load(), 0);
    EXPECT_EQ(a.close(), 0);
}

// Operations that need not wait work outside fibers.
TEST(Net, NonBlockingOpsOutsideFibers) {
    Scheduler scheduler(options(1));
    Pair pair;
    Stream a;
    ASSERT_EQ(a.adopt(scheduler, pair.fd[0]), 0);
    ASSERT_EQ(::write(pair.fd[1], "abc", 3), 3);
    char buf[8];
    const IoResult r = a.read_some(buf, sizeof buf);
    EXPECT_TRUE(r);
    EXPECT_EQ(r.bytes, 3u);
    EXPECT_TRUE(a.write_all("xyz", 3));
    EXPECT_EQ(::read(pair.fd[1], buf, sizeof buf), 3);
    EXPECT_EQ(a.close(), 0);
    ::close(pair.fd[1]);
}

// Re-open: a closed Stream accepts / adopts again.
TEST(Net, StreamReuse) {
    Scheduler scheduler(options(2));
    Stream a;
    for (int i = 0; i < 50; ++i) {
        Pair pair;
        ASSERT_EQ(a.adopt(scheduler, pair.fd[0]), 0);
        ASSERT_EQ(::write(pair.fd[1], "q", 1), 1);
        char c = 0;
        Latch done(1);
        fiber::go(scheduler, [&]() noexcept {
            static_cast<void>(a.read_some(&c, 1));
            done.count_down();
        });
        ASSERT_TRUE(done.wait());
        EXPECT_EQ(c, 'q');
        EXPECT_EQ(a.close(), 0);
        ::close(pair.fd[1]);
    }
}

int get_int_option(int fd, int level, int name) {
    int value = -1;
    socklen_t len = sizeof value;
    return ::getsockopt(fd, level, name, &value, &len) == 0 ? value : -1;
}

// Options set on the listener reach accepted connections (Linux inherits
// them), and the Stream setters work on a connected socket.
TEST(Net, TcpOptionsAndInheritance) {
    Latch done(2);
    std::atomic<int> errors{0};
    Scheduler scheduler(options(2));
    Listener listener;
    ASSERT_EQ(listener.listen(scheduler, Endpoint::loopback_v4(0)), 0);
    ASSERT_EQ(listener.set_nodelay(), 0);
    ASSERT_EQ(listener.set_keepalive(std::chrono::seconds(30), std::chrono::seconds(5), 4), 0);
    ASSERT_EQ(listener.set_user_timeout(std::chrono::milliseconds(7000)), 0);
    const Endpoint endpoint = listener.local_endpoint();
    Stream server;
    Stream client;
    fiber::go(scheduler, [&]() noexcept {
        if (listener.accept(server) != 0) {
            errors.fetch_add(1);
        }
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        if (client.connect(scheduler, endpoint) != 0) {
            errors.fetch_add(1);
        }
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    ASSERT_EQ(errors.load(), 0);
    const int fd = server.native_handle();
    EXPECT_EQ(get_int_option(fd, IPPROTO_TCP, TCP_NODELAY), 1);
    EXPECT_EQ(get_int_option(fd, SOL_SOCKET, SO_KEEPALIVE), 1);
    EXPECT_EQ(get_int_option(fd, IPPROTO_TCP, TCP_KEEPIDLE), 30);
    EXPECT_EQ(get_int_option(fd, IPPROTO_TCP, TCP_KEEPINTVL), 5);
    EXPECT_EQ(get_int_option(fd, IPPROTO_TCP, TCP_KEEPCNT), 4);
    EXPECT_EQ(get_int_option(fd, IPPROTO_TCP, TCP_USER_TIMEOUT), 7000);

    const int cfd = client.native_handle();
    EXPECT_EQ(get_int_option(cfd, SOL_SOCKET, SO_KEEPALIVE), 0);
    EXPECT_EQ(client.set_keepalive(std::chrono::seconds(11), std::chrono::seconds(3), 2), 0);
    EXPECT_EQ(client.set_user_timeout(std::chrono::milliseconds(1500)), 0);
    EXPECT_EQ(client.set_nodelay(), 0);
    EXPECT_EQ(get_int_option(cfd, SOL_SOCKET, SO_KEEPALIVE), 1);
    EXPECT_EQ(get_int_option(cfd, IPPROTO_TCP, TCP_KEEPIDLE), 11);
    EXPECT_EQ(get_int_option(cfd, IPPROTO_TCP, TCP_KEEPINTVL), 3);
    EXPECT_EQ(get_int_option(cfd, IPPROTO_TCP, TCP_KEEPCNT), 2);
    EXPECT_EQ(get_int_option(cfd, IPPROTO_TCP, TCP_USER_TIMEOUT), 1500);
    EXPECT_EQ(get_int_option(cfd, IPPROTO_TCP, TCP_NODELAY), 1);
    EXPECT_EQ(client.set_keepalive(std::chrono::seconds(0), std::chrono::seconds(0), 0), 0);
    EXPECT_EQ(get_int_option(cfd, SOL_SOCKET, SO_KEEPALIVE), 0);
    EXPECT_EQ(client.close(), 0);
    EXPECT_EQ(server.close(), 0);
    EXPECT_EQ(listener.close(), 0);
    EXPECT_EQ(client.set_nodelay(), EBADF);
}

// The peer resets the connection (close with SO_LINGER 0) while one fiber
// is parked in read_some and another in write_all (full send buffer): both
// wake with ECONNRESET (EPOLLERR notifies both directions; the poller keeps
// SO_ERROR), and so do later operations.
TEST(Net, PeerResetWakesReaderAndWriter) {
    Latch connected(2);
    Latch done(2);
    std::atomic<int> errors{0};
    std::atomic<int> read_error{-1};
    std::atomic<int> write_error{-1};
    Scheduler scheduler(options(3));
    Listener listener;
    ASSERT_EQ(listener.listen(scheduler, Endpoint::loopback_v4(0)), 0);
    const Endpoint endpoint = listener.local_endpoint();
    Stream server;
    Stream client;
    fiber::go(scheduler, [&]() noexcept {
        if (listener.accept(server) != 0) {
            errors.fetch_add(1);
        }
        connected.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        if (client.connect(scheduler, endpoint) != 0) {
            errors.fetch_add(1);
        }
        connected.count_down();
    });
    ASSERT_TRUE(connected.wait());
    ASSERT_EQ(errors.load(), 0);
    std::vector<char> big(32 << 20, 'z');
    fiber::go(scheduler, [&]() noexcept {
        char c;
        read_error.store(client.read_some(&c, 1).error);
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        write_error.store(client.write_all(big.data(), big.size()).error);
        done.count_down();
    });
    ASSERT_TRUE(eventually([&] { return parked(client, Direction::kRead) && parked(client, Direction::kWrite); }));
    const linger reset{1, 0};
    ASSERT_EQ(server.set_option(SOL_SOCKET, SO_LINGER, &reset, sizeof reset), 0);
    ASSERT_EQ(server.close(), 0);  // sends RST
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(read_error.load(), ECONNRESET);
    EXPECT_EQ(write_error.load(), ECONNRESET);
    char c;
    EXPECT_EQ(client.read_some(&c, 1).error, ECONNRESET);  // sticky
    EXPECT_EQ(client.write_all("x", 1).error, ECONNRESET);
    EXPECT_EQ(client.close(), 0);
    EXPECT_EQ(listener.close(), 0);
}

// ---- deadlines ------------------------------------------------------------------

using Clock = std::chrono::steady_clock;

// A parked reader times out at its deadline; the direction stays expired
// (even with data available) until the deadline is moved.
TEST(NetDeadline, ReadDeadlineWakesParkedReader) {
    Latch done(1);
    std::atomic<int> first{-1};
    std::atomic<std::int64_t> waited_ms{-1};
    Scheduler scheduler(options(2));
    Pair pair;
    Stream a;
    ASSERT_EQ(a.adopt(scheduler, pair.fd[0]), 0);
    fiber::go(scheduler, [&]() noexcept {
        const auto start = Clock::now();
        static_cast<void>(a.set_read_deadline(scheduler.now() + 50ms));
        char c;
        first.store(a.read_some(&c, 1).error);
        waited_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start).count());
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(first.load(), ETIMEDOUT);
    EXPECT_GE(waited_ms.load(), 49);
    EXPECT_LT(waited_ms.load(), 2000);
    EXPECT_TRUE(a.poll_desc().expired_approx(Direction::kRead));
    ASSERT_EQ(::write(pair.fd[1], "d", 1), 1);
    char c = 0;
    EXPECT_EQ(a.read_some(&c, 1).error, ETIMEDOUT) << "expired until moved, data or not";
    EXPECT_TRUE(a.write_all("w", 1)) << "the write direction is unaffected";
    ASSERT_EQ(a.set_read_deadline(fibers::poller::TimePoint{}), 0);  // cleared
    const IoResult r = a.read_some(&c, 1);
    EXPECT_TRUE(r);
    EXPECT_EQ(c, 'd');
    EXPECT_EQ(a.close(), 0);
    ::close(pair.fd[1]);
}

TEST(NetDeadline, PastDeadlineExpiresAtOnce) {
    Scheduler scheduler(options(1));
    Pair pair;
    Stream a;
    ASSERT_EQ(a.adopt(scheduler, pair.fd[0]), 0);
    ASSERT_EQ(::write(pair.fd[1], "p", 1), 1);
    ASSERT_EQ(a.set_read_deadline(scheduler.now() - 1ms), 0);
    char c;
    EXPECT_EQ(a.read_some(&c, 1).error, ETIMEDOUT);
    ASSERT_EQ(a.set_read_deadline(fibers::poller::TimePoint::max()), 0);
    EXPECT_EQ(a.read_some(&c, 1).bytes, 1u);
    EXPECT_EQ(a.close(), 0);
    EXPECT_EQ(a.set_read_deadline(scheduler.now()), EBADF);
    ::close(pair.fd[1]);
}

// Moving the deadline of a parked reader later keeps it waiting; moving it
// earlier than the pending timer (the control-timer path) fires on time.
TEST(NetDeadline, MovingTheDeadline) {
    Scheduler scheduler(options(2));
    for (const bool earlier : {false, true}) {
        Latch done(1);
        std::atomic<int> result{-1};
        std::atomic<std::int64_t> waited_ms{-1};
        Pair pair;
        Stream a;
        ASSERT_EQ(a.adopt(scheduler, pair.fd[0]), 0);
        const auto start = Clock::now();
        ASSERT_EQ(a.set_read_deadline(scheduler.now() + (earlier ? 10s : 60ms)), 0);
        fiber::go(scheduler, [&]() noexcept {
            char c;
            result.store(a.read_some(&c, 1).error);
            waited_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start).count());
            done.count_down();
        });
        ASSERT_TRUE(eventually([&] { return parked(a, Direction::kRead); }));
        ASSERT_EQ(a.set_read_deadline(scheduler.now() + (earlier ? 80ms : 400ms)), 0);
        ASSERT_TRUE(done.wait());
        EXPECT_EQ(result.load(), ETIMEDOUT);
        if (earlier) {
            EXPECT_GE(waited_ms.load(), 79);
            EXPECT_LT(waited_ms.load(), 3000) << "the earlier deadline, not the first one";
        } else {
            EXPECT_GE(waited_ms.load(), 399) << "the old deadline must not fire";
        }
        EXPECT_EQ(a.close(), 0);
        ::close(pair.fd[1]);
    }
}

// Data that arrives before the deadline is read; the deadline then still
// applies to later reads.
TEST(NetDeadline, DataBeforeDeadline) {
    Latch done(1);
    std::atomic<int> first{-1};
    std::atomic<int> second{-1};
    Scheduler scheduler(options(2));
    Pair pair;
    Stream a;
    ASSERT_EQ(a.adopt(scheduler, pair.fd[0]), 0);
    ASSERT_EQ(a.set_read_deadline(scheduler.now() + 300ms), 0);
    fiber::go(scheduler, [&]() noexcept {
        char c;
        const IoResult r = a.read_some(&c, 1);
        first.store(r ? static_cast<int>(r.bytes) : -r.error);
        second.store(a.read_some(&c, 1).error);
        done.count_down();
    });
    std::this_thread::sleep_for(20ms);
    ASSERT_EQ(::write(pair.fd[1], "x", 1), 1);
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(first.load(), 1);
    EXPECT_EQ(second.load(), ETIMEDOUT);
    EXPECT_EQ(a.close(), 0);
    ::close(pair.fd[1]);
}

// write_all against a full send buffer: ETIMEDOUT with the bytes written.
TEST(NetDeadline, WriteDeadline) {
    Latch done(1);
    std::atomic<int> error{-1};
    std::atomic<std::size_t> bytes{0};
    Scheduler scheduler(options(2));
    Pair pair;
    Stream a;
    ASSERT_EQ(a.adopt(scheduler, pair.fd[0]), 0);
    std::vector<char> big(16 << 20, 'b');
    fiber::go(scheduler, [&]() noexcept {
        static_cast<void>(a.set_write_deadline(scheduler.now() + 50ms));
        const IoResult r = a.write_all(big.data(), big.size());
        error.store(r.error);
        bytes.store(r.bytes);
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(error.load(), ETIMEDOUT);
    EXPECT_GT(bytes.load(), 0u);
    EXPECT_LT(bytes.load(), big.size());
    EXPECT_EQ(a.close(), 0);
    ::close(pair.fd[1]);
}

TEST(NetDeadline, AcceptDeadline) {
    Latch done(1);
    std::atomic<int> result{-1};
    Scheduler scheduler(options(2));
    Listener listener;
    ASSERT_EQ(listener.listen(scheduler, Endpoint::loopback_v4(0)), 0);
    Stream s;
    fiber::go(scheduler, [&]() noexcept {
        static_cast<void>(listener.set_accept_deadline(scheduler.now() + 30ms));
        result.store(listener.accept(s));
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(result.load(), ETIMEDOUT);
    EXPECT_FALSE(s.is_open_approx());
    EXPECT_EQ(listener.close(), 0);
}

// close() with armed deadline timers (one pending far in the future, one
// moved earlier through the control timer) takes them back from the
// poller: the Stream's memory is freed right after, and the pollers keep
// working (ASan would catch a timer node left behind). Reopening a closed
// stream starts without deadlines.
TEST(NetDeadline, CloseLeavesNoTimerBehind) {
    Scheduler scheduler(options(2, 1));
    for (int round = 0; round < 50; ++round) {
        Pair pair;
        auto s = std::make_unique<Stream>();
        ASSERT_EQ(s->adopt(scheduler, pair.fd[0]), 0);
        ASSERT_EQ(s->set_read_deadline(scheduler.now() + 1h), 0);
        ASSERT_EQ(s->set_write_deadline(scheduler.now() + 2h), 0);
        if (round % 2 == 0) {
            ASSERT_EQ(s->set_write_deadline(scheduler.now() + 30min), 0);  // control timer
        }
        ASSERT_EQ(s->close(), 0);
        s.reset();  // the timer nodes' memory goes away here
        ::close(pair.fd[1]);
    }
    Stream reused;
    for (int round = 0; round < 2; ++round) {
        Pair pair;
        ASSERT_EQ(reused.adopt(scheduler, pair.fd[0]), 0);
        ASSERT_FALSE(reused.poll_desc().expired_approx(Direction::kRead));
        ASSERT_EQ(reused.set_read_deadline(scheduler.now() - 1ms), 0);  // expired
        ASSERT_EQ(reused.close(), 0);
        ::close(pair.fd[1]);
    }
    // The pollers still run timers.
    Latch slept(1);
    fiber::go(scheduler, [&]() noexcept {
        this_fiber::sleep_for(5ms);
        slept.count_down();
    });
    ASSERT_TRUE(slept.wait());
}

// The per-request pattern: a later deadline before every read of an echo
// exchange (no timer per call: the pending one re-arms itself).
TEST(NetDeadline, DeadlinePerRequest) {
    Latch done(2);
    std::atomic<int> errors{0};
    Scheduler scheduler(options(2));
    Pair pair;
    Stream a;
    Stream b;
    ASSERT_EQ(a.adopt(scheduler, pair.fd[0]), 0);
    ASSERT_EQ(b.adopt(scheduler, pair.fd[1]), 0);
    fiber::go(scheduler, [&]() noexcept {
        char buf[64];
        for (;;) {
            static_cast<void>(b.set_read_deadline(scheduler.now() + 2s));
            const IoResult r = b.read_some(buf, sizeof buf);
            if (!r || r.bytes == 0) {
                if (!r) {
                    errors.fetch_add(1);
                }
                break;
            }
            if (!b.write_all(buf, r.bytes)) {
                errors.fetch_add(1);
                break;
            }
        }
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        char out[16] = "0123456789abcde";
        char in[16];
        for (int i = 0; i < 5000; ++i) {
            static_cast<void>(a.set_deadline(scheduler.now() + 2s));
            if (!a.write_all(out, sizeof out) || a.read_exact(in, sizeof in).bytes != sizeof in) {
                errors.fetch_add(1);
                break;
            }
        }
        static_cast<void>(a.shutdown(SHUT_WR));
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(errors.load(), 0);
    EXPECT_EQ(a.close(), 0);
    EXPECT_EQ(b.close(), 0);
}

using NetDeathTest = ::testing::Test;

TEST(NetDeathTest, WaitOutsideFiberFailsFast) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            Scheduler scheduler(options(1, 1));
            Pair pair;
            Stream a;
            if (a.adopt(scheduler, pair.fd[0]) == 0) {
                char c;
                static_cast<void>(a.read_some(&c, 1));
            }
        },
        "would block outside a fiber");
}

}  // namespace
