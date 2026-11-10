// http::Server and http::Client over real loopback sockets on a Scheduler:
// round trips (keep-alive reuse, chunked, large bodies), pipelining,
// 100-continue, timeouts (slowloris header timeout, idle, body, write),
// many connections on few workers, graceful shutdown.
//
// Fibers never use gtest assertions: they record into atomics / strings
// that the test thread checks after a Latch. Raw blocking sockets on the
// test thread play misbehaving clients.

#include <gtest/gtest.h>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "fibers/fiber/context.hpp"
#include "fibers/fiber/event.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/http/client.hpp"
#include "fibers/http/router.hpp"
#include "fibers/http/server.hpp"
#include "fibers/sched/scheduler.hpp"
#include "http_test_util.hpp"

namespace fiber = fibers::fiber;
namespace http = fibers::http;
namespace net = fibers::net;
using fibers::sched::Scheduler;
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

namespace {

class Latch {
public:
    explicit Latch(std::int64_t count) : count_(count) {}
    void count_down() noexcept { count_.fetch_sub(1, std::memory_order_acq_rel); }
    bool wait(std::chrono::milliseconds timeout = 60s) {
        const auto deadline = Clock::now() + timeout;
        while (count_.load(std::memory_order_acquire) > 0) {
            if (Clock::now() > deadline) {
                return false;
            }
            std::this_thread::sleep_for(200us);
        }
        return true;
    }

private:
    std::atomic<std::int64_t> count_;
};

template <class Pred>
bool eventually(Pred pred, std::chrono::milliseconds timeout = 30s) {
    const auto deadline = Clock::now() + timeout;
    while (!pred()) {
        if (Clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(200us);
    }
    return true;
}

fibers::sched::SchedulerOptions scheduler_options(std::size_t workers) {
    fibers::sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = 1;
    o.blocking.num_threads = 1;
    o.stacks.max_stacks = 4096;
    o.stacks.stack_size = 128 * 1024;
    return o;
}

// Scheduler, router, server; stops them in the right order.
struct Harness {
    explicit Harness(http::ServerOptions options = {}, std::size_t workers = 2)
        : scheduler(scheduler_options(workers)), server(scheduler, router.handler(), options) {}

    void start() {
        ASSERT_EQ(server.listen(net::Endpoint::loopback_v4(0)), 0);
        port = server.local_endpoint().port();
        ASSERT_EQ(server.start(), 0);
    }

    ~Harness() {
        static_cast<void>(server.stop(1s));
        scheduler.stop();
    }

    net::Endpoint endpoint() const { return net::Endpoint::loopback_v4(port); }

    Scheduler scheduler;
    http::Router router;
    http::Server server;
    std::uint16_t port = 0;
};

// ---- raw blocking client (test thread) -----------------------------------------

struct Raw {
    int fd = -1;

    explicit Raw(std::uint16_t port, std::chrono::milliseconds recv_timeout = 10s) {
        fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        sockaddr_in a{};
        a.sin_family = AF_INET;
        a.sin_port = htons(port);
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof a) != 0) {
            ::close(fd);
            fd = -1;
            return;
        }
        const int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        timeval tv{};
        tv.tv_sec = recv_timeout.count() / 1000;
        tv.tv_usec = (recv_timeout.count() % 1000) * 1000;
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    }
    ~Raw() {
        if (fd >= 0) {
            ::close(fd);
        }
    }

    bool send(const std::string& s) const {
        std::size_t done = 0;
        while (done < s.size()) {
            const ssize_t n = ::send(fd, s.data() + done, s.size() - done, MSG_NOSIGNAL);
            if (n <= 0) {
                return false;
            }
            done += static_cast<std::size_t>(n);
        }
        return true;
    }

    // Reads until `count` complete responses are buffered, EOF, an error or
    // the receive timeout. `eof` tells whether the peer closed.
    std::vector<http_test::ParsedResponse> read_responses(std::size_t count, bool* eof = nullptr,
                                                          std::vector<bool> heads = {}) {
        std::vector<http_test::ParsedResponse> rs;
        bool closed = false;
        for (;;) {
            bool ok = false;
            rs = http_test::parse_responses(data, ok, heads);
            if (ok && rs.size() >= count) {
                break;
            }
            char buf[65536];
            const ssize_t n = ::recv(fd, buf, sizeof buf, 0);
            if (n <= 0) {
                closed = n == 0;
                rs = http_test::parse_responses(data, ok, heads);
                break;
            }
            data.append(buf, static_cast<std::size_t>(n));
        }
        if (eof != nullptr) {
            *eof = closed;
        }
        return rs;
    }

    // Waits for the end of the stream: true if it came (EOF or reset) within
    // the receive timeout. Appends what arrives to `data`.
    bool wait_closed() {
        char buf[4096];
        for (;;) {
            const ssize_t n = ::recv(fd, buf, sizeof buf, 0);
            if (n == 0) {
                return true;
            }
            if (n < 0) {
                return errno == ECONNRESET;
            }
            data.append(buf, static_cast<std::size_t>(n));
        }
    }

    std::string data;
};

double seconds_since(Clock::time_point t0) { return std::chrono::duration<double>(Clock::now() - t0).count(); }

// ---- tests ----------------------------------------------------------------------

TEST(HttpServer, ClientRoundTrips) {
    Harness h;
    h.router.get("/hello", [](http::Request&, http::Response& r) noexcept { r.send("hello\n"); });
    h.router.post("/echo", [](http::Request& q, http::Response& r) noexcept {
        r.header("X-Len", std::to_string(q.body().size()));
        r.send(q.body());
    });
    h.router.get("/stream", [](http::Request& q, http::Response& r) noexcept {
        const int n = std::atoi(std::string(q.query("n").value_or("3")).c_str());
        r.begin_chunked();
        for (int i = 0; i < n; ++i) {
            r.write("chunk" + std::to_string(i) + "\n");
        }
    });
    h.start();

    std::atomic<int> failures{0};
    std::string log;
    Latch done(1);
    fiber::go(h.scheduler, [&]() noexcept {
        http::Client client(h.scheduler, h.endpoint());
        http::ClientResponse res;
        auto check = [&](bool ok, const char* what) {
            if (!ok) {
                failures.fetch_add(1);
                log += what;
                log += "; ";
            }
        };
        check(client.get("/hello", res) == 0 && res.status() == 200 && res.body() == "hello\n", "hello");
        check(res.header("content-length") == "6" && res.keep_alive(), "hello headers");
        for (int i = 0; i < 100; ++i) {
            check(client.get("/hello", res) == 0 && res.body() == "hello\n", "repeat");
        }
        check(client.connects() == 1, "keep-alive reuse");
        std::string big(200 * 1024, 'x');
        for (std::size_t i = 0; i < big.size(); i += 97) {
            big[i] = static_cast<char>('a' + i % 26);
        }
        check(client.post("/echo", big, res) == 0 && res.status() == 200 && res.body() == big, "echo big");
        check(res.header("X-Len") == std::to_string(big.size()), "echo len");
        check(client.get("/stream?n=50", res) == 0 && res.chunked() && res.body().size() > 300 &&
                  res.body().substr(0, 7) == "chunk0\n",
              "chunked");
        http::ClientRequest head;
        head.method = http::Method::kHead;
        head.target = "/hello";
        check(client.request(head, res) == 0 && res.status() == 200 && res.body().empty() &&
                  res.header("Content-Length") == "6",
              "head");
        check(client.get("/missing", res) == 0 && res.status() == 404, "404");
        http::ClientRequest del;
        del.method = http::Method::kDelete;
        del.target = "/hello";
        check(client.request(del, res) == 0 && res.status() == 405 && res.header("Allow") == "GET, HEAD", "405");
        check(client.connects() == 1, "still one connection");
        http::ClientRequest closing;
        closing.target = "/hello";
        closing.close = true;
        check(client.request(closing, res) == 0 && !res.keep_alive() && !client.connected(), "close");
        check(client.get("/hello", res) == 0 && client.connects() == 2, "reconnect");
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(failures.load(), 0) << log;
}

TEST(HttpServer, PipeliningAndExpectContinueOverSockets) {
    Harness h;
    h.router.get("/n/{i}", [](http::Request& q, http::Response& r) noexcept { r.send(q.param("i")); });
    h.router.put("/up", [](http::Request& q, http::Response& r) noexcept { r.send(q.body()); });
    h.start();
    Raw raw(h.port);
    ASSERT_GE(raw.fd, 0);
    std::string batch;
    for (int i = 0; i < 64; ++i) {
        batch += "GET /n/" + std::to_string(i) + " HTTP/1.1\r\nHost: x\r\n\r\n";
    }
    ASSERT_TRUE(raw.send(batch));
    auto rs = raw.read_responses(64);
    ASSERT_EQ(rs.size(), 64u);
    for (int i = 0; i < 64; ++i) {
        EXPECT_EQ(rs[i].body, std::to_string(i));
    }
    raw.data.clear();
    ASSERT_TRUE(raw.send("PUT /up HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\nContent-Length: 4\r\n\r\n"));
    rs = raw.read_responses(1);
    ASSERT_EQ(rs.size(), 1u);
    EXPECT_EQ(rs[0].status, 100);
    ASSERT_TRUE(raw.send("data"));
    rs = raw.read_responses(2);
    ASSERT_EQ(rs.size(), 2u);
    EXPECT_EQ(rs[1].status, 200);
    EXPECT_EQ(rs[1].body, "data");
}

TEST(HttpServer, SlowlorisHeaderTimeout) {
    http::ServerOptions o;
    o.header_timeout = 300ms;
    Harness h(o);
    h.router.get("/", [](http::Request&, http::Response& r) noexcept { r.send("ok"); });
    h.start();
    {
        // Part of a head, then nothing (even while trickling): 408, closed.
        Raw raw(h.port);
        const auto t0 = Clock::now();
        ASSERT_TRUE(raw.send("GET / HTTP/1.1\r\nHost: x\r\n"));
        std::this_thread::sleep_for(100ms);
        ASSERT_TRUE(raw.send("X-A: 1\r\n"));
        bool eof = false;
        const auto rs = raw.read_responses(1, &eof);
        const double dt = seconds_since(t0);
        ASSERT_EQ(rs.size(), 1u);
        EXPECT_EQ(rs[0].status, 408);
        EXPECT_GE(dt, 0.25);
        EXPECT_LT(dt, 5.0);
        EXPECT_TRUE(raw.wait_closed());
    }
    {
        // Connected, nothing sent: closed without a response.
        Raw raw(h.port);
        const auto t0 = Clock::now();
        EXPECT_TRUE(raw.wait_closed());
        EXPECT_EQ(raw.data, "");
        EXPECT_GE(seconds_since(t0), 0.25);
    }
    {
        // A keep-alive connection's next head gets header_timeout from its
        // first byte, not the idle timeout.
        Raw raw(h.port);
        ASSERT_TRUE(raw.send("GET / HTTP/1.1\r\nHost: x\r\n\r\n"));
        ASSERT_EQ(raw.read_responses(1).size(), 1u);
        raw.data.clear();
        const auto t0 = Clock::now();
        ASSERT_TRUE(raw.send("GET / HTTP/1.1\r\n"));
        const auto rs = raw.read_responses(1);
        ASSERT_EQ(rs.size(), 1u);
        EXPECT_EQ(rs[0].status, 408);
        EXPECT_LT(seconds_since(t0), 5.0);
    }
}

TEST(HttpServer, IdleAndBodyTimeouts) {
    http::ServerOptions o;
    o.idle_timeout = 300ms;
    o.body_timeout = 300ms;
    Harness h(o);
    h.router.get("/", [](http::Request&, http::Response& r) noexcept { r.send("ok"); });
    h.router.post("/", [](http::Request& q, http::Response& r) noexcept { r.send(q.body()); });
    h.start();
    {
        Raw raw(h.port);
        ASSERT_TRUE(raw.send("GET / HTTP/1.1\r\nHost: x\r\n\r\n"));
        ASSERT_EQ(raw.read_responses(1).size(), 1u);
        const auto t0 = Clock::now();
        EXPECT_TRUE(raw.wait_closed());
        const double dt = seconds_since(t0);
        EXPECT_GE(dt, 0.25);
        EXPECT_LT(dt, 5.0);
    }
    {
        Raw raw(h.port);
        const auto t0 = Clock::now();
        ASSERT_TRUE(raw.send("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\n\r\nabc"));
        const auto rs = raw.read_responses(1);
        ASSERT_EQ(rs.size(), 1u);
        EXPECT_EQ(rs[0].status, 408);
        EXPECT_GE(seconds_since(t0), 0.25);
    }
}

TEST(HttpServer, WriteTimeout) {
    http::ServerOptions o;
    o.write_timeout = 300ms;
    Harness h(o);
    std::atomic<int> write_error{0};
    std::atomic<std::int64_t> elapsed_ms{-1};
    h.router.get("/flood", [&](http::Request&, http::Response& r) noexcept {
        const auto t0 = Clock::now();
        const std::string block(256 * 1024, 'f');
        r.begin_chunked();
        for (int i = 0; i < 4000; ++i) {
            if (const int e = r.write(block); e != 0) {
                write_error.store(e);
                break;
            }
        }
        elapsed_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count());
    });
    h.start();
    Raw raw(h.port);
    const int small = 4096;
    ::setsockopt(raw.fd, SOL_SOCKET, SO_RCVBUF, &small, sizeof small);
    ASSERT_TRUE(raw.send("GET /flood HTTP/1.1\r\nHost: x\r\n\r\n"));
    // Never read: the server's writes block, then hit the write deadline.
    ASSERT_TRUE(eventually([&] { return elapsed_ms.load() >= 0; }, 30s));
    EXPECT_EQ(write_error.load(), ETIMEDOUT);
    EXPECT_LT(elapsed_ms.load(), 10000);
    EXPECT_TRUE(eventually([&] { return h.server.connections() == 0; }, 10s));
}

TEST(HttpServer, ManyConnectionsFewWorkers) {
    Harness h({}, 2);
    std::atomic<std::uint64_t> served{0};
    h.router.get("/c/{id}", [&](http::Request& q, http::Response& r) noexcept {
        served.fetch_add(1, std::memory_order_relaxed);
        r.send(q.param("id"));
    });
    h.start();
    constexpr int kClients = 300;
    constexpr int kRequests = 20;
    std::atomic<int> failures{0};
    Latch done(kClients);
    for (int c = 0; c < kClients; ++c) {
        fiber::go(h.scheduler, [&, c]() noexcept {
            http::Client client(h.scheduler, h.endpoint());
            http::ClientResponse res;
            const std::string target = "/c/" + std::to_string(c);
            for (int i = 0; i < kRequests; ++i) {
                if (client.get(target, res) != 0 || res.status() != 200 || res.body() != std::to_string(c)) {
                    failures.fetch_add(1);
                }
                if (i == 0) {
                    fiber::this_fiber::yield();  // let the others connect: many open at once
                }
            }
            if (client.connects() != 1) {
                failures.fetch_add(1);
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait(120s));
    EXPECT_EQ(failures.load(), 0);
    EXPECT_EQ(served.load(), static_cast<std::uint64_t>(kClients) * kRequests);
    EXPECT_EQ(h.server.accepted(), static_cast<std::uint64_t>(kClients));
}

TEST(HttpServer, GracefulShutdownLetsInFlightRequestsFinish) {
    http::ServerOptions o;
    o.idle_timeout = 60s;  // idle connections must be closed by stop(), not by the timeout
    Harness h(o);
    std::atomic<int> in_handler{0};
    h.router.get("/slow", [&](http::Request&, http::Response& r) noexcept {
        in_handler.fetch_add(1);
        fiber::this_fiber::sleep_for(300ms);
        r.send("slow done");
    });
    h.router.get("/fast", [](http::Request&, http::Response& r) noexcept { r.send("fast"); });
    h.start();

    // Idle keep-alive connections.
    std::vector<std::unique_ptr<Raw>> idle;
    for (int i = 0; i < 10; ++i) {
        idle.push_back(std::make_unique<Raw>(h.port));
        ASSERT_TRUE(idle.back()->send("GET /fast HTTP/1.1\r\nHost: x\r\n\r\n"));
        ASSERT_EQ(idle.back()->read_responses(1).size(), 1u);
    }
    // In-flight requests.
    std::vector<std::unique_ptr<Raw>> busy;
    for (int i = 0; i < 20; ++i) {
        busy.push_back(std::make_unique<Raw>(h.port));
        ASSERT_TRUE(busy.back()->send("GET /slow HTTP/1.1\r\nHost: x\r\n\r\n"));
    }
    ASSERT_TRUE(eventually([&] { return in_handler.load() == 20; }));

    const auto t0 = Clock::now();
    EXPECT_EQ(h.server.stop(5s), 0);
    const double dt = seconds_since(t0);
    EXPECT_LT(dt, 3.0);
    for (auto& raw : busy) {
        bool eof = false;
        const auto rs = raw->read_responses(1, &eof);
        ASSERT_EQ(rs.size(), 1u);
        EXPECT_EQ(rs[0].status, 200);
        EXPECT_EQ(rs[0].body, "slow done");
        EXPECT_EQ(rs[0].header("Connection"), "close");
        EXPECT_TRUE(eof || raw->wait_closed());
    }
    for (auto& raw : idle) {
        raw->data.clear();
        EXPECT_TRUE(raw->wait_closed());
        EXPECT_EQ(raw->data, "");
    }
    EXPECT_EQ(h.server.connections(), 0u);
    // The listener is closed.
    Raw late(h.port);
    EXPECT_LT(late.fd, 0);
}

TEST(HttpServer, ShutdownDeadlineCutsStalledRequests) {
    http::ServerOptions o;
    o.body_timeout = 60s;
    Harness h(o);
    h.router.post("/", [](http::Request& q, http::Response& r) noexcept { r.send(q.body()); });
    h.start();
    Raw raw(h.port);
    ASSERT_TRUE(raw.send("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1000\r\n\r\nabc"));
    ASSERT_TRUE(eventually([&] { return h.server.connections() == 1; }));
    std::this_thread::sleep_for(50ms);  // the connection waits for the body
    const auto t0 = Clock::now();
    EXPECT_EQ(h.server.stop(300ms), ETIMEDOUT);
    const double dt = seconds_since(t0);
    EXPECT_GE(dt, 0.25);
    EXPECT_LT(dt, 5.0);
    EXPECT_TRUE(raw.wait_closed());
}

TEST(HttpServer, StopFromAFiberAndIdempotent) {
    Harness h;
    h.router.get("/", [](http::Request&, http::Response& r) noexcept { r.send("ok"); });
    h.start();
    std::atomic<int> result{-1};
    Latch done(1);
    fiber::go(h.scheduler, [&]() noexcept {
        result.store(h.server.stop(1s));
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(result.load(), 0);
    EXPECT_EQ(h.server.stop(1s), 0);
}

TEST(HttpServer, ClientRetriesStaleKeepAliveConnection) {
    http::ServerOptions o;
    o.idle_timeout = 150ms;
    Harness h(o);
    h.router.get("/", [](http::Request&, http::Response& r) noexcept { r.send("ok"); });
    h.start();
    std::atomic<int> failures{0};
    std::atomic<std::uint64_t> connects{0};
    Latch done(1);
    fiber::go(h.scheduler, [&]() noexcept {
        http::Client client(h.scheduler, h.endpoint());
        http::ClientResponse res;
        if (client.get("/", res) != 0 || res.body() != "ok") {
            failures.fetch_add(1);
        }
        fiber::this_fiber::sleep_for(500ms);  // the server closes the idle connection
        if (client.get("/", res) != 0 || res.body() != "ok") {
            failures.fetch_add(1);
        }
        connects.store(client.connects());
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(failures.load(), 0);
    EXPECT_EQ(connects.load(), 2u);
}

// The HTTP/1.1 client under a context (fibers/fiber/context.hpp): a
// response that does not come in time ends with the context's deadline
// (ETIMEDOUT, the client's own timeout is longer); an ended context fails a
// request that would wait.
TEST(HttpServer, ClientUnderContext) {
    Harness h;
    fiber::Event release;
    h.router.get("/slow", [&](http::Request&, http::Response& r) noexcept {
        release.wait();
        r.send("late");
    });
    h.router.get("/", [](http::Request&, http::Response& r) noexcept { r.send("ok"); });
    h.start();
    std::atomic<int> timed_out{-100};
    std::atomic<int> reason{-100};
    std::atomic<int> after{-100};
    std::atomic<int> ended{-100};
    Latch done(1);
    fiber::go(h.scheduler, [&]() noexcept {
        {
            http::ClientOptions co;
            co.timeout = 60s;
            http::Client client(h.scheduler, h.endpoint(), co);
            http::ClientResponse res;
            fibers::Context ctx = fibers::Context::with_timeout(100ms);
            fiber::ContextScope scope(ctx);
            timed_out.store(client.get("/slow", res));
            reason.store(ctx.reason() ? static_cast<int>(*ctx.reason()) : 0);
        }
        release.set();
        http::Client client(h.scheduler, h.endpoint());
        http::ClientResponse res;
        after.store(client.get("/", res) == 0 && res.body() == "ok" ? 0 : -1);
        fibers::Context gone = fibers::Context::cancellable();
        gone.cancel();
        fiber::ContextScope scope(gone);
        ended.store(client.get("/", res));
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(timed_out.load(), ETIMEDOUT);
    EXPECT_EQ(reason.load(), static_cast<int>(fibers::wait::Error::kTimedOut));
    EXPECT_EQ(after.load(), 0);
    EXPECT_EQ(ended.load(), ECANCELED);
}

TEST(HttpServer, MaxConnections) {
    http::ServerOptions o;
    o.max_connections = 2;
    Harness h(o);
    h.router.get("/", [](http::Request&, http::Response& r) noexcept { r.send("ok"); });
    h.start();
    Raw a(h.port, 300ms);
    Raw b(h.port, 300ms);
    ASSERT_TRUE(a.send("GET / HTTP/1.1\r\n\r\n"));
    ASSERT_TRUE(b.send("GET / HTTP/1.1\r\n\r\n"));
    ASSERT_EQ(a.read_responses(1).size(), 1u);
    ASSERT_EQ(b.read_responses(1).size(), 1u);
    Raw c(h.port, 300ms);  // accepted by the kernel, not served yet
    ASSERT_TRUE(c.send("GET / HTTP/1.1\r\n\r\n"));
    EXPECT_EQ(c.read_responses(1).size(), 0u);
    ::close(a.fd);
    a.fd = -1;
    timeval tv{5, 0};
    ::setsockopt(c.fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    EXPECT_EQ(c.read_responses(1).size(), 1u);
}

}  // namespace
