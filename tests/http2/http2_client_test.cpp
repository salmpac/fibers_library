// fibers::http2 client: against MockH2Server (h2c and TLS; scripted GOAWAY,
// delays, kills, PING behaviour) and against nghttpd (h2c and TLS, static
// files, --echo-upload, -m), plus an indicative throughput number.
//
// Fibers never use gtest assertions themselves: they record results that
// the test thread checks after run_in_fiber() returned.

#include <gtest/gtest.h>

#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "fibers/fiber/context.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/wait_group.hpp"
#include "fibers/http2/client.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/tls/context.hpp"
#include "h2_test_util.hpp"

namespace fiber = fibers::fiber;
namespace http2 = fibers::http2;
namespace net = fibers::net;
namespace tls = fibers::tls;
using fibers::sched::Scheduler;
using namespace std::chrono_literals;
using h2test::MockH2Server;
using h2test::MockOptions;
using h2test::run_in_fiber;
using h2test::wait_until;

namespace {

std::string cert(const char* name) { return std::string(FIBERS_TLS_CERTS) + "/" + name; }

http2::ClientOptions h2c_options(std::uint16_t port) {
    http2::ClientOptions o;
    o.host = "127.0.0.1";
    o.port = port;
    o.endpoint = net::Endpoint::loopback_v4(port);
    o.request_timeout = 30s;
    return o;
}

int make_client_tls(tls::TlsContext& ctx, std::span<const std::string_view> alpn) {
    const std::string ca = cert("ca.pem");
    tls::ClientOptions o;
    o.ca_file = ca.c_str();
    o.alpn = alpn;
    return ctx.init_client(o);
}

constexpr std::string_view kH2[] = {"h2"};

struct Result {
    int error = -1;
    int status = 0;
    std::string body;
};

}  // namespace

// ---- basics -----------------------------------------------------------------------

TEST(Http2Mock, GetAndPost) {
    MockH2Server server;
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::Client client(scheduler, h2c_options(server.port()));
    Result get, post, missing;
    std::string authority;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response res;
        get.error = client.get("/status?code=202", res);
        get.status = res.status();
        authority = server.last_header(":authority");
        post.error = client.post("/echo", "hello http2", res);
        post.status = res.status();
        post.body = std::string(res.body());
        missing.error = client.get("/nope", res);
        missing.status = res.status();
        client.close();
    }));
    EXPECT_EQ(get.error, 0);
    EXPECT_EQ(get.status, 202);
    EXPECT_EQ(authority, "127.0.0.1:" + std::to_string(server.port()));
    EXPECT_EQ(post.error, 0);
    EXPECT_EQ(post.status, 200);
    EXPECT_EQ(post.body, "hello http2");
    EXPECT_EQ(missing.error, 0);
    EXPECT_EQ(missing.status, 404);
    EXPECT_EQ(client.stats().connects, 1u);
    EXPECT_EQ(server.connections.load(), 1);
}

TEST(Http2Mock, RequestHeadersSent) {
    MockH2Server server;
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::Client client(scheduler, h2c_options(server.port()));
    int error = -1;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        const http2::Header h[] = {{"authorization", "bearer abc"}, {"x-custom", "v1"}};
        http2::Request req;
        req.method = "PUT";
        req.path = "/echo?x=1";
        req.authority = "example.test";
        req.headers = h;
        req.body = "12345";
        http2::Response res;
        error = client.request(req, res);
        client.close();
    }));
    EXPECT_EQ(error, 0);
    EXPECT_EQ(server.last_header(":method"), "PUT");
    EXPECT_EQ(server.last_header(":path"), "/echo?x=1");
    EXPECT_EQ(server.last_header(":authority"), "example.test");
    EXPECT_EQ(server.last_header(":scheme"), "http");
    EXPECT_EQ(server.last_header("authorization"), "bearer abc");
    EXPECT_EQ(server.last_header("x-custom"), "v1");
    EXPECT_EQ(server.last_header("content-length"), "5");
    EXPECT_FALSE(server.last_header("user-agent").empty());
}

TEST(Http2Mock, InvalidRequests) {
    MockH2Server server;
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::Client client(scheduler, h2c_options(server.port()));
    std::vector<int> errors;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response res;
        auto with = [&](std::string_view name, std::string_view value) {
            const http2::Header h[] = {{name, value}};
            http2::Request req;
            req.path = "/echo";
            req.headers = h;
            return client.request(req, res);
        };
        errors.push_back(with("Upper", "x"));          // upper case name
        errors.push_back(with(":path", "/x"));         // pseudo-header
        errors.push_back(with("connection", "close"));  // connection-specific
        errors.push_back(with("x-bad", "a\r\nb"));     // CR LF in a value
        errors.push_back(with("te", "gzip"));          // te other than trailers
        std::vector<http2::Header> many(http2::kMaxRequestHeaders + 1, http2::Header{"x-a", "b"});
        http2::Request req;
        req.headers = many;
        errors.push_back(client.request(req, res));
        errors.push_back(with("x-good", "fine"));      // and the connection still works
        client.close();
    }));
    ASSERT_EQ(errors.size(), 7u);
    for (std::size_t i = 0; i + 1 < errors.size(); ++i) {
        EXPECT_EQ(errors[i], EINVAL) << i;
    }
    EXPECT_EQ(errors.back(), 0);
}

// ---- multiplexing -------------------------------------------------------------------

TEST(Http2Mock, ConcurrentStreamsOnOneConnection) {
    MockH2Server server;
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::Client client(scheduler, h2c_options(server.port()));
    constexpr int kFibers = 64;
    constexpr int kEach = 20;
    std::atomic<int> ok{0};
    std::atomic<int> bad{0};
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        fiber::WaitGroup wg;
        for (int f = 0; f < kFibers; ++f) {
            wg.add(1);
            fiber::go(scheduler, [&, f]() noexcept {
                http2::Response res;
                for (int i = 0; i < kEach; ++i) {
                    const std::string body = "fiber " + std::to_string(f) + " request " + std::to_string(i);
                    const std::string path = i % 2 == 0 ? "/echo" : "/delay?ms=1";
                    const int e = client.post(path, body, res);
                    const bool good = e == 0 && res.status() == 200 &&
                                      (i % 2 == 0 ? res.body() == body : res.body() == "delayed");
                    (good ? ok : bad).fetch_add(1);
                }
                wg.done();
            });
        }
        wg.wait();
        client.close();
    }));
    EXPECT_EQ(ok.load(), kFibers * kEach);
    EXPECT_EQ(bad.load(), 0);
    EXPECT_EQ(client.stats().connects, 1u);
    EXPECT_EQ(server.connections.load(), 1);
    EXPECT_GT(server.max_open_streams.load(), 1);
}

// Launches `n` concurrent requests to `path`; collects the errors.
std::vector<int> concurrent(Scheduler& scheduler, http2::Client& client, int n, const std::string& path) {
    std::vector<int> errors(static_cast<std::size_t>(n), -1);
    fiber::WaitGroup wg;
    for (int i = 0; i < n; ++i) {
        wg.add(1);
        fiber::go(scheduler, [&, i]() noexcept {
            http2::Response res;
            errors[static_cast<std::size_t>(i)] = client.get(path, res);
            wg.done();
        });
    }
    wg.wait();
    return errors;
}

int count(const std::vector<int>& v, int x) { return static_cast<int>(std::count(v.begin(), v.end(), x)); }

TEST(Http2Mock, MaxConcurrentStreamsQueues) {
    MockOptions mo;
    mo.max_concurrent_streams = 2;
    MockH2Server server(mo);
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::Client client(scheduler, h2c_options(server.port()));
    std::vector<int> errors;
    int warm = -1;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response res;
        warm = client.get("/echo", res);  // the server's SETTINGS are in
        errors = concurrent(scheduler, client, 10, "/delay?ms=50");
        client.close();
    }));
    EXPECT_EQ(warm, 0);
    EXPECT_EQ(count(errors, 0), 10);
    EXPECT_LE(server.max_open_streams.load(), 2);
    EXPECT_EQ(client.stats().connects, 1u);
}

TEST(Http2Mock, MaxConcurrentStreamsFails) {
    MockOptions mo;
    mo.max_concurrent_streams = 2;
    MockH2Server server(mo);
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::ClientOptions o = h2c_options(server.port());
    o.stream_limit = http2::StreamLimit::kFail;
    http2::Client client(scheduler, o);
    std::vector<int> errors;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response res;
        static_cast<void>(client.get("/echo", res));
        errors = concurrent(scheduler, client, 10, "/delay?ms=300");
        client.close();
    }));
    EXPECT_EQ(count(errors, 0), 2);
    EXPECT_EQ(count(errors, EBUSY), 8);
}

TEST(Http2Mock, BoundedQueue) {
    MockOptions mo;
    mo.max_concurrent_streams = 2;
    MockH2Server server(mo);
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::ClientOptions o = h2c_options(server.port());
    o.max_queued_requests = 3;
    http2::Client client(scheduler, o);
    std::vector<int> errors;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response res;
        static_cast<void>(client.get("/echo", res));
        errors = concurrent(scheduler, client, 10, "/delay?ms=300");
        client.close();
    }));
    EXPECT_EQ(count(errors, 0), 5);
    EXPECT_EQ(count(errors, EBUSY), 5);
}

// ---- deadlines --------------------------------------------------------------------

TEST(Http2Mock, DeadlineResetsStream) {
    MockH2Server server;
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::Client client(scheduler, h2c_options(server.port()));
    int timed_out = -1;
    int after = -1;
    std::chrono::milliseconds took{};
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response res;
        http2::Request req;
        req.path = "/hang";
        req.timeout = 100ms;
        const auto start = std::chrono::steady_clock::now();
        timed_out = client.request(req, res);
        took = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
        after = client.get("/echo", res);
    }));
    EXPECT_EQ(timed_out, ETIMEDOUT);
    EXPECT_GE(took, 95ms);
    EXPECT_LT(took, 3000ms);
    EXPECT_EQ(after, 0);
    EXPECT_TRUE(wait_until([&] { return server.rst_cancel.load() >= 1; }));
    EXPECT_GE(client.stats().streams_reset, 1u);
    EXPECT_EQ(client.stats().connects, 1u);
    bool drained = false;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        drained = h2test::fiber_wait_until([&] { return client.stream_records().first == 0; });
        client.close();
    }));
    EXPECT_TRUE(drained);
}

// A request made under a context (fibers/fiber/context.hpp): cancel()
// completes it with ECANCELED and resets its stream; the connection (shared,
// its fibers run under no context) serves the next request.
TEST(Http2Mock, ContextCancelResetsStream) {
    MockH2Server server;
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::Client client(scheduler, h2c_options(server.port()));
    ASSERT_TRUE(run_in_fiber(scheduler, [&] { ASSERT_EQ(client.connect(), 0); }));
    fibers::Context ctx = fibers::Context::cancellable();
    std::atomic<int> cancelled{-1};
    std::atomic<bool> done{false};
    fiber::go(scheduler, ctx, [&]() noexcept {
        http2::Response res;
        cancelled.store(client.get("/hang", res));
        done.store(true);
    });
    ASSERT_TRUE(wait_until([&] { return ctx.has_dependants_approx(); }));  // waiting for its stream
    ctx.cancel();
    ASSERT_TRUE(wait_until([&] { return done.load(); }));
    EXPECT_EQ(cancelled.load(), ECANCELED);
    EXPECT_TRUE(wait_until([&] { return server.rst_cancel.load() >= 1; }));
    int after = -1;
    int ended = -1;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response res;
        after = client.get("/echo", res);
        fibers::Context gone = fibers::Context::cancellable();
        gone.cancel();
        fiber::ContextScope scope(gone);
        ended = client.get("/echo", res);  // fails before sending anything
    }));
    EXPECT_EQ(after, 0);
    EXPECT_EQ(ended, ECANCELED);
    EXPECT_EQ(client.stats().connects, 1u);
    bool drained = false;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        drained = h2test::fiber_wait_until([&] { return client.stream_records().first == 0; });
        client.close();
    }));
    EXPECT_TRUE(drained);
}

// The context's deadline ends a request (ETIMEDOUT) whose own timeout is
// later; many such requests at once, half of them answered in time.
TEST(Http2Mock, ContextDeadlineEndsRequests) {
    MockH2Server server;
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::Client client(scheduler, h2c_options(server.port()));
    constexpr int kRequests = 20;
    std::atomic<int> ok{0};
    std::atomic<int> timed_out{0};
    std::atomic<int> other{0};
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        fibers::Context ctx = fibers::Context::with_timeout(200ms);
        fiber::ContextScope scope(ctx);
        fiber::WaitGroup wg;
        for (int i = 0; i < kRequests; ++i) {
            wg.go([&, i]() noexcept {  // inherits ctx
                http2::Response res;
                http2::Request req;
                req.path = i % 2 == 0 ? "/echo" : "/hang";
                req.timeout = 1h;
                const int e = client.request(req, res);
                (e == 0 ? ok : e == ETIMEDOUT ? timed_out : other).fetch_add(1);
            });
        }
        wg.wait();
    }));
    EXPECT_EQ(other.load(), 0);
    EXPECT_GE(timed_out.load(), kRequests / 2);
    EXPECT_EQ(ok.load() + timed_out.load(), kRequests);
    bool drained = false;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        drained = h2test::fiber_wait_until([&] { return client.stream_records().first == 0; });
        client.close();
    }));
    EXPECT_TRUE(drained);
}

TEST(Http2Mock, DeadlineOfQueuedRequest) {
    // One stream at a time: the second request waits in nghttp2's queue and
    // times out there (RST_STREAM before its HEADERS were ever sent).
    MockOptions mo;
    mo.max_concurrent_streams = 1;
    MockH2Server server(mo);
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::Client client(scheduler, h2c_options(server.port()));
    int first = -1;
    int second = -1;
    int third = -1;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response warm;
        static_cast<void>(client.get("/echo", warm));
        fiber::WaitGroup wg;
        wg.add(1);
        fiber::go(scheduler, [&]() noexcept {
            http2::Response res;
            first = client.get("/delay?ms=400", res);
            wg.done();
        });
        h2test::fiber_wait_until([&] { return server.open_streams.load() == 1; });
        http2::Response res;
        http2::Request req;
        req.path = "/echo";
        req.timeout = 100ms;
        second = client.request(req, res);
        wg.wait();
        third = client.get("/echo", res);
        client.close();
    }));
    EXPECT_EQ(first, 0);
    EXPECT_EQ(second, ETIMEDOUT);
    EXPECT_EQ(third, 0);
    EXPECT_LE(server.max_open_streams.load(), 1);
    EXPECT_EQ(server.requests.load(), 3);  // the cancelled one never reached the server
}

TEST(Http2Mock, ManyDeadlinesDoNotLeak) {
    MockH2Server server;
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::Client client(scheduler, h2c_options(server.port()));
    std::atomic<int> timeouts{0};
    std::atomic<int> oks{0};
    std::atomic<int> others{0};
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        fiber::WaitGroup wg;
        for (int f = 0; f < 32; ++f) {
            wg.add(1);
            fiber::go(scheduler, [&, f]() noexcept {
                http2::Response res;
                for (int i = 0; i < 10; ++i) {
                    http2::Request req;
                    // Some finish just before their deadline, some just after.
                    req.path = (f + i) % 2 == 0 ? "/delay?ms=20" : "/hang";
                    req.timeout = 20ms;
                    const int e = client.request(req, res);
                    (e == ETIMEDOUT ? timeouts : e == 0 ? oks : others).fetch_add(1);
                }
                wg.done();
            });
        }
        wg.wait();
    }));
    EXPECT_EQ(others.load(), 0);
    EXPECT_EQ(timeouts.load() + oks.load(), 320);
    EXPECT_GE(timeouts.load(), 160);
    bool drained = false;
    std::size_t pooled = 0;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        drained = h2test::fiber_wait_until([&] { return client.stream_records().first == 0; });
        pooled = client.stream_records().second;
        client.close();
    }));
    EXPECT_TRUE(drained);
    EXPECT_LE(pooled, 64u);  // records are reused, not leaked
    EXPECT_EQ(client.stats().connects, 1u);
}

// ---- bodies and limits ---------------------------------------------------------------

TEST(Http2Mock, LargeBodiesFlowControl) {
    MockH2Server server;  // 65535-byte windows on its side: the upload waits for WINDOW_UPDATEs
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::ClientOptions o = h2c_options(server.port());
    o.max_response_body_bytes = 16 << 20;
    o.initial_window_size = 65535;  // and so does our download
    o.connection_window_size = 65535;
    http2::Client client(scheduler, o);
    std::string upload(4 << 20, '\0');
    for (std::size_t i = 0; i < upload.size(); ++i) {
        upload[i] = static_cast<char>('A' + (i * 7) % 26);
    }
    int down = -1;
    int up = -1;
    std::size_t down_size = 0;
    bool down_ok = false;
    bool up_ok = false;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response res;
        down = client.get("/big?n=8388608", res);
        down_size = res.body().size();
        down_ok = true;
        for (std::size_t i = 0; i < res.body().size(); ++i) {
            if (res.body()[i] != static_cast<char>('a' + (8388608 - i) % 26)) {
                down_ok = false;
                break;
            }
        }
        up = client.post("/echo", upload, res);
        up_ok = res.body() == upload;
        client.close();
    }));
    EXPECT_EQ(down, 0);
    EXPECT_EQ(down_size, 8388608u);
    EXPECT_TRUE(down_ok);
    EXPECT_EQ(up, 0);
    EXPECT_TRUE(up_ok);
}

TEST(Http2Mock, ResponseLimits) {
    MockH2Server server;
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::ClientOptions o = h2c_options(server.port());
    o.max_response_body_bytes = 1 << 20;
    o.max_response_header_bytes = 8 * 1024;
    http2::Client client(scheduler, o);
    int body = -1;
    int header = -1;
    int after = -1;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response res;
        body = client.get("/big?n=3000000", res);
        header = client.get("/bigheader?n=20000", res);
        after = client.get("/echo", res);
        client.close();
    }));
    EXPECT_EQ(body, EMSGSIZE);
    EXPECT_EQ(header, EMSGSIZE);
    EXPECT_EQ(after, 0);
    EXPECT_GE(server.rst_cancel.load(), 1);
    EXPECT_EQ(client.stats().connects, 1u);
}

// ---- GOAWAY, connection loss, reconnect ------------------------------------------------

TEST(Http2Mock, GoawayMidFlight) {
    MockH2Server server;
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::Client client(scheduler, h2c_options(server.port()));
    std::vector<int> errors(4, -1);
    std::vector<std::int32_t> ids(4, 0);
    int goaway = -1;
    int fresh = -1;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        fiber::WaitGroup wg;
        for (int i = 0; i < 4; ++i) {
            wg.add(1);
            fiber::go(scheduler, [&, i]() noexcept {
                http2::Response res;
                errors[static_cast<std::size_t>(i)] = client.get("/delay?ms=300", res);
                wg.done();
            });
            // In order: stream ids 1, 3, 5, 7.
            h2test::fiber_wait_until([&] { return server.requests.load() == i + 1; });
        }
        http2::Response res;
        goaway = client.get("/goaway?last=3", res);  // stream 9: above last_stream_id
        wg.wait();
        fresh = client.get("/echo", res);
        client.close();
    }));
    EXPECT_EQ(errors[0], 0);       // stream 1 <= last: completes
    EXPECT_EQ(errors[1], 0);       // stream 3
    EXPECT_EQ(errors[2], EAGAIN);  // stream 5 > last: not processed, retryable
    EXPECT_EQ(errors[3], EAGAIN);
    EXPECT_EQ(goaway, EAGAIN);
    EXPECT_TRUE(http2::retryable(errors[2]));
    EXPECT_EQ(fresh, 0);  // on a new connection
    EXPECT_EQ(client.stats().goaways_received, 1u);
    EXPECT_EQ(client.stats().connects, 2u);
    EXPECT_EQ(server.connections.load(), 2);
}

TEST(Http2Mock, ServerKillFailsInFlightAndReconnects) {
    MockH2Server server;
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::Client client(scheduler, h2c_options(server.port()));
    std::vector<int> errors(5, -1);
    int after = -1;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        fiber::WaitGroup wg;
        for (int i = 0; i < 5; ++i) {
            wg.add(1);
            fiber::go(scheduler, [&, i]() noexcept {
                http2::Response res;
                http2::Request req;
                req.path = "/hang";
                req.timeout = std::chrono::nanoseconds(-1);  // none
                errors[static_cast<std::size_t>(i)] = client.request(req, res);
                wg.done();
            });
        }
        h2test::fiber_wait_until([&] { return server.open_streams.load() == 5; });
        server.kill_connections();
        wg.wait();
        http2::Response res;
        after = client.get("/echo", res);
        client.close();
    }));
    for (int e : errors) {
        EXPECT_EQ(e, ECONNRESET);
    }
    EXPECT_EQ(after, 0);
    EXPECT_EQ(client.stats().connects, 2u);
}

TEST(Http2Mock, ServerClosesConnection) {
    MockH2Server server;
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::Client client(scheduler, h2c_options(server.port()));
    int hang = -1;
    int close = -1;
    int after = -1;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        fiber::WaitGroup wg;
        wg.add(1);
        fiber::go(scheduler, [&]() noexcept {
            http2::Response res;
            hang = client.get("/hang", res);
            wg.done();
        });
        h2test::fiber_wait_until([&] { return server.open_streams.load() == 1; });
        http2::Response res;
        close = client.get("/close", res);
        wg.wait();
        after = client.get("/echo", res);
        client.close();
    }));
    EXPECT_EQ(hang, ECONNRESET);
    EXPECT_EQ(close, ECONNRESET);
    EXPECT_EQ(after, 0);
}

TEST(Http2Mock, ServerDownThenBack) {
    const std::uint16_t port = h2test::free_port();
    Scheduler scheduler(h2test::scheduler_options());
    http2::ClientOptions o = h2c_options(port);
    o.connect_timeout = 2s;
    http2::Client client(scheduler, o);
    int down = -1;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response res;
        down = client.get("/echo", res);
    }));
    EXPECT_EQ(down, ECONNREFUSED);
    MockH2Server server;
    ASSERT_TRUE(server.start(port));
    int up = -1;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response res;
        up = client.get("/echo", res);
        client.close();
    }));
    EXPECT_EQ(up, 0);
    EXPECT_EQ(client.stats().connect_failures, 1u);
    EXPECT_EQ(client.stats().connects, 1u);
}

// ---- PING keepalive -----------------------------------------------------------------

TEST(Http2Mock, PingKeepalive) {
    MockH2Server server;
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::ClientOptions o = h2c_options(server.port());
    o.ping_interval = 50ms;
    o.ping_timeout = 1s;
    http2::Client client(scheduler, o);
    int first = -1;
    int later = -1;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response res;
        first = client.get("/echo", res);
        fiber::this_fiber::sleep_for(500ms);
        later = client.get("/echo", res);
        client.close();
    }));
    EXPECT_EQ(first, 0);
    EXPECT_EQ(later, 0);
    EXPECT_GE(server.pings.load(), 4);
    EXPECT_GE(client.stats().ping_acks, 4u);
    EXPECT_EQ(client.stats().keepalive_failures, 0u);
    EXPECT_EQ(client.stats().connects, 1u);
}

TEST(Http2Mock, PingTimeoutDropsConnection) {
    MockOptions mo;
    mo.auto_ping_ack = false;  // a dead peer that still holds the TCP connection
    MockH2Server server(mo);
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::ClientOptions o = h2c_options(server.port());
    o.ping_interval = 50ms;
    o.ping_timeout = 100ms;
    http2::Client client(scheduler, o);
    int hang = -1;
    std::chrono::milliseconds took{};
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response res;
        http2::Request req;
        req.path = "/hang";
        req.timeout = 10s;
        const auto start = std::chrono::steady_clock::now();
        hang = client.request(req, res);
        took = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
        client.close();
    }));
    EXPECT_EQ(hang, ECONNRESET);
    EXPECT_LT(took, 5000ms);
    EXPECT_GE(client.stats().keepalive_failures, 1u);
}

// ---- close --------------------------------------------------------------------------

TEST(Http2Mock, CloseCancelsInFlight) {
    MockH2Server server;
    ASSERT_TRUE(server.start());
    Scheduler scheduler(h2test::scheduler_options());
    http2::Client client(scheduler, h2c_options(server.port()));
    int hang = -1;
    int after = -1;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        fiber::WaitGroup wg;
        wg.add(1);
        fiber::go(scheduler, [&]() noexcept {
            http2::Response res;
            hang = client.get("/hang", res);
            wg.done();
        });
        h2test::fiber_wait_until([&] { return server.open_streams.load() == 1; });
        client.close();
        wg.wait();
        http2::Response res;
        after = client.get("/echo", res);
    }));
    EXPECT_EQ(hang, ECANCELED);
    EXPECT_EQ(after, ECANCELED);
}

// ---- TLS ----------------------------------------------------------------------------

TEST(Http2MockTls, AlpnH2) {
    SSL_CTX* sctx = h2test::make_mock_tls(cert("server.pem"), cert("server.key"));
    ASSERT_NE(sctx, nullptr);
    MockOptions mo;
    mo.tls = sctx;
    MockH2Server server(mo);
    ASSERT_TRUE(server.start());
    tls::TlsContext ctx;
    ASSERT_EQ(make_client_tls(ctx, kH2), 0);
    Scheduler scheduler(h2test::scheduler_options());
    http2::ClientOptions o = h2c_options(server.port());
    o.host = "localhost";
    o.tls = &ctx;
    http2::Client client(scheduler, o);
    std::atomic<int> ok{0};
    std::string scheme;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        fiber::WaitGroup wg;
        for (int f = 0; f < 16; ++f) {
            wg.add(1);
            fiber::go(scheduler, [&, f]() noexcept {
                http2::Response res;
                for (int i = 0; i < 10; ++i) {
                    const std::string body(1000 + static_cast<std::size_t>(f * 100 + i), 'x');
                    if (client.post("/echo", body, res) == 0 && res.body() == body) {
                        ok.fetch_add(1);
                    }
                }
                wg.done();
            });
        }
        wg.wait();
        scheme = server.last_header(":scheme");
        client.close();
    }));
    server.stop();
    SSL_CTX_free(sctx);
    EXPECT_EQ(ok.load(), 160);
    EXPECT_EQ(scheme, "https");
    EXPECT_EQ(client.stats().connects, 1u);
}

TEST(Http2MockTls, NoAlpnIsRefused) {
    SSL_CTX* sctx = h2test::make_mock_tls(cert("server.pem"), cert("server.key"));
    ASSERT_NE(sctx, nullptr);
    SSL_CTX_set_alpn_select_cb(sctx, nullptr, nullptr);  // the server ignores ALPN
    MockOptions mo;
    mo.tls = sctx;
    MockH2Server server(mo);
    ASSERT_TRUE(server.start());
    tls::TlsContext ctx;
    ASSERT_EQ(make_client_tls(ctx, kH2), 0);
    Scheduler scheduler(h2test::scheduler_options());
    http2::ClientOptions o = h2c_options(server.port());
    o.host = "localhost";
    o.tls = &ctx;
    http2::Client client(scheduler, o);
    int error = -1;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response res;
        error = client.get("/echo", res);
        client.close();
    }));
    server.stop();
    SSL_CTX_free(sctx);
    EXPECT_EQ(error, ENOPROTOOPT);
}

TEST(Http2MockTls, BadCertificate) {
    SSL_CTX* sctx = h2test::make_mock_tls(cert("wronghost.pem"), cert("wronghost.key"));
    ASSERT_NE(sctx, nullptr);
    MockOptions mo;
    mo.tls = sctx;
    MockH2Server server(mo);
    ASSERT_TRUE(server.start());
    tls::TlsContext ctx;
    ASSERT_EQ(make_client_tls(ctx, kH2), 0);
    Scheduler scheduler(h2test::scheduler_options());
    http2::ClientOptions o = h2c_options(server.port());
    o.host = "localhost";
    o.tls = &ctx;
    http2::Client client(scheduler, o);
    int error = -1;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response res;
        error = client.get("/echo", res);
        client.close();
    }));
    server.stop();
    SSL_CTX_free(sctx);
    EXPECT_EQ(error, EPROTO);
    EXPECT_EQ(client.stats().connect_failures, 1u);
}

// ---- nghttpd ---------------------------------------------------------------------------

#ifdef FIBERS_NGHTTPD

namespace {

std::string make_big() {
    std::string big(8 << 20, '\0');
    for (std::size_t i = 0; i < big.size(); ++i) {
        big[i] = static_cast<char>('0' + (i * 31) % 75);
    }
    return big;
}

const std::string& big_file() {
    static const std::string data = make_big();
    return data;
}

// Written once per process into a temporary name and renamed: ctest runs
// the tests of this binary in parallel processes sharing the directory.
void put_file(const std::string& path, const std::string& data) {
    const std::string tmp = path + "." + std::to_string(::getpid());
    std::ofstream(tmp, std::ios::binary) << data;
    std::rename(tmp.c_str(), path.c_str());
}

const std::string& docroot() {
    static const std::string dir = [] {
        std::string d = std::string(FIBERS_HTTP2_TEST_DIR) + "/docroot";
        ::mkdir(d.c_str(), 0755);
        put_file(d + "/small.txt", "hello from nghttpd\n");
        put_file(d + "/big.bin", big_file());
        return d;
    }();
    return dir;
}

// nghttpd on 127.0.0.1:port (h2c unless tls), with extra args.
class Nghttpd {
public:
    bool start(bool tls, std::vector<std::string> extra = {}) {
        port_ = h2test::free_port();
        std::vector<std::string> args = {FIBERS_NGHTTPD, "-a", "127.0.0.1", "-d", docroot()};
        for (std::string& e : extra) {
            args.push_back(std::move(e));
        }
        if (!tls) {
            args.push_back("--no-tls");
        }
        args.push_back(std::to_string(port_));
        if (tls) {
            args.push_back(cert("server.key"));
            args.push_back(cert("server.pem"));
        }
        if (!child_.spawn(args)) {
            return false;
        }
        // Ours, not another process's server on a port free_port() handed
        // out twice: still alive once the port answers.
        return wait_until([&] { return h2test::port_open(port_) || !child_.alive(); }, 10000ms) &&
               child_.alive();
    }

    void kill() { child_.stop(); }
    std::uint16_t port() const { return port_; }

private:
    h2test::Child child_;
    std::uint16_t port_ = 0;
};

}  // namespace

TEST(Http2Nghttpd, H2cFilesAndUpload) {
    Nghttpd d;
    ASSERT_TRUE(d.start(false, {"--echo-upload"}));
    Scheduler scheduler(h2test::scheduler_options());
    http2::ClientOptions o = h2c_options(d.port());
    o.max_response_body_bytes = 16 << 20;
    http2::Client client(scheduler, o);
    int small = -1, big = -1, missing = -1, upload = -1;
    std::string small_body;
    bool big_ok = false;
    bool upload_ok = false;
    int missing_status = 0;
    std::string upload_data(3 << 20, 'u');
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response res;
        small = client.get("/small.txt", res);
        small_body = std::string(res.body());
        big = client.get("/big.bin", res);
        big_ok = res.body() == big_file();
        missing = client.get("/missing", res);
        missing_status = res.status();
        upload = client.post("/small.txt", upload_data, res);
        upload_ok = res.body() == upload_data;
        client.close();
    }));
    EXPECT_EQ(small, 0);
    EXPECT_EQ(small_body, "hello from nghttpd\n");
    EXPECT_EQ(big, 0);
    EXPECT_TRUE(big_ok);
    EXPECT_EQ(missing, 0);
    EXPECT_EQ(missing_status, 404);
    EXPECT_EQ(upload, 0);
    EXPECT_TRUE(upload_ok);
    EXPECT_EQ(client.stats().connects, 1u);
}

TEST(Http2Nghttpd, TlsConcurrentAndLarge) {
    Nghttpd d;
    ASSERT_TRUE(d.start(true, {"-w", "16", "-W", "18"}));  // small windows: flow control
    tls::TlsContext ctx;
    ASSERT_EQ(make_client_tls(ctx, kH2), 0);
    Scheduler scheduler(h2test::scheduler_options());
    http2::ClientOptions o = h2c_options(d.port());
    o.host = "localhost";
    o.tls = &ctx;
    o.max_response_body_bytes = 16 << 20;
    http2::Client client(scheduler, o);
    std::atomic<int> ok{0};
    std::atomic<int> big_ok{0};
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        fiber::WaitGroup wg;
        for (int f = 0; f < 4; ++f) {
            wg.add(1);
            fiber::go(scheduler, [&]() noexcept {
                http2::Response res;
                if (client.get("/big.bin", res) == 0 && res.body() == big_file()) {
                    big_ok.fetch_add(1);
                }
                wg.done();
            });
        }
        for (int f = 0; f < 50; ++f) {
            wg.add(1);
            fiber::go(scheduler, [&]() noexcept {
                http2::Response res;
                for (int i = 0; i < 10; ++i) {
                    if (client.get("/small.txt", res) == 0 && res.status() == 200) {
                        ok.fetch_add(1);
                    }
                }
                wg.done();
            });
        }
        wg.wait();
        client.close();
    }));
    EXPECT_EQ(big_ok.load(), 4);
    EXPECT_EQ(ok.load(), 500);
    EXPECT_EQ(client.stats().connects, 1u);
}

TEST(Http2Nghttpd, MaxConcurrentStreams) {
    Nghttpd d;
    ASSERT_TRUE(d.start(false, {"-m", "2"}));
    Scheduler scheduler(h2test::scheduler_options());
    http2::ClientOptions o = h2c_options(d.port());
    o.max_response_body_bytes = 16 << 20;
    http2::Client client(scheduler, o);
    std::vector<int> errors;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response res;
        static_cast<void>(client.get("/small.txt", res));
        errors = concurrent(scheduler, client, 20, "/big.bin");
        client.close();
    }));
    EXPECT_EQ(count(errors, 0), 20);
}

TEST(Http2Nghttpd, KillAndRestart) {
    Nghttpd d;
    ASSERT_TRUE(d.start(false, {"-w", "14"}));  // 16 KiB stream windows: the download takes many round trips
    Scheduler scheduler(h2test::scheduler_options());
    http2::ClientOptions o = h2c_options(d.port());
    o.max_response_body_bytes = 16 << 20;
    o.connect_timeout = 2s;
    http2::Client client(scheduler, o);
    std::vector<int> errors(8, -1);
    std::atomic<int> started{0};
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        fiber::WaitGroup wg;
        for (int i = 0; i < 8; ++i) {
            wg.add(1);
            fiber::go(scheduler, [&, i]() noexcept {
                http2::Response res;
                started.fetch_add(1);
                errors[static_cast<std::size_t>(i)] = client.get("/big.bin", res);
                wg.done();
            });
        }
        h2test::fiber_wait_until([&] { return started.load() == 8; });
        fiber::this_fiber::sleep_for(20ms);
        d.kill();
        wg.wait();
    }));
    int failed = 0;
    for (int e : errors) {
        EXPECT_TRUE(e == 0 || e == ECONNRESET) << e;
        failed += e == ECONNRESET ? 1 : 0;
    }
    EXPECT_GE(failed, 1);
    // Restart on the same port: the client reconnects lazily.
    h2test::Child again;
    ASSERT_TRUE(again.spawn({FIBERS_NGHTTPD, "-a", "127.0.0.1", "-d", docroot(), "--no-tls", std::to_string(d.port())}));
    ASSERT_TRUE(wait_until([&] { return h2test::port_open(d.port()); }, 10000ms));
    int after = -1;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response res;
        after = client.get("/small.txt", res);
        client.close();
    }));
    EXPECT_EQ(after, 0);
    EXPECT_EQ(client.stats().connects, 2u);
}

// An indicative number only (the machine is shared): GETs of a small file
// over one connection, many fibers.
TEST(Http2Load, NghttpdThroughput) {
    Nghttpd d;
    ASSERT_TRUE(d.start(false, {"-n", "2", "-m", "1000"}));
    Scheduler scheduler(h2test::scheduler_options());
    http2::Client client(scheduler, h2c_options(d.port()));
    constexpr int kFibers = 100;
    const int each = h2test::kSanitized ? 50 : 400;
    std::atomic<int> ok{0};
    std::chrono::duration<double> took{};
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Response warm;
        static_cast<void>(client.get("/small.txt", warm));
        const auto start = std::chrono::steady_clock::now();
        fiber::WaitGroup wg;
        for (int f = 0; f < kFibers; ++f) {
            wg.add(1);
            fiber::go(scheduler, [&]() noexcept {
                http2::Response res;
                for (int i = 0; i < each; ++i) {
                    if (client.get("/small.txt", res) == 0) {
                        ok.fetch_add(1, std::memory_order_relaxed);
                    }
                }
                wg.done();
            });
        }
        wg.wait();
        took = std::chrono::steady_clock::now() - start;
        client.close();
    }));
    EXPECT_EQ(ok.load(), kFibers * each);
    std::printf("[ indicative ] http2::Client -> nghttpd (h2c, 1 connection, %d fibers): %d requests in %.3f s = %.0f req/s\n",
                kFibers, ok.load(), took.count(), ok.load() / took.count());
}

#endif  // FIBERS_NGHTTPD
