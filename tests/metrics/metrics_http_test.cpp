// The /metrics endpoint (metrics::http_handler) over real loopback sockets:
// http::Server on a Scheduler, scraped with http::Client from a fiber.
//   - a small exposition goes out with Content-Length, a large one chunked;
//     either way the body is exactly Registry::text();
//   - HEAD carries no body;
//   - scrapes while fibers update the instruments (the server's own handler
//     counts requests, the built-in collectors are registered).
//
// Fibers never use gtest assertions: they record into atomics / strings
// that the test thread checks after the WaitGroup.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/wait_group.hpp"
#include "fibers/http/client.hpp"
#include "fibers/http/router.hpp"
#include "fibers/http/server.hpp"
#include "fibers/metrics/collectors.hpp"
#include "fibers/metrics/http.hpp"
#include "fibers/metrics/metrics.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace http = fibers::http;
namespace metrics = fibers::metrics;
namespace net = fibers::net;
using fibers::sched::Scheduler;
using namespace std::chrono_literals;

namespace {

fibers::sched::SchedulerOptions scheduler_options() {
    fibers::sched::SchedulerOptions o;
    o.pool.num_workers = 2;
    o.pollers.num_pollers = 1;
    o.blocking.num_threads = 1;
    o.stacks.max_stacks = 1024;
    o.stacks.stack_size = 64 * 1024;
    return o;
}

// Registry, scheduler, router, server; stops them in the right order (the
// server before the scheduler; the registry outlives both).
struct Harness {
    Harness() : scheduler(scheduler_options()), server(scheduler, router.handler()) {
        router.get("/metrics", metrics::http_handler(registry));
    }

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

    metrics::Registry registry{metrics::RegistryOptions{.shards = 2}};
    Scheduler scheduler;
    http::Router router;
    http::Server server;
    std::uint16_t port = 0;
};

// Runs `body(client, check)` in a fiber and waits for it.
template <class Body>
std::string run_client(Harness& h, Body body) {
    std::string log;
    fiber::WaitGroup wg;
    wg.go(h.scheduler, [&]() noexcept {
        http::Client client(h.scheduler, h.endpoint());
        auto check = [&](bool ok, std::string_view what) {
            if (!ok) {
                log += what;
                log += "; ";
            }
        };
        body(client, check);
    });
    wg.wait();
    return log;
}

TEST(MetricsHttp, SmallExpositionWithContentLength) {
    Harness h;
    h.registry.counter("small_total", "Small.").add(42);
    h.start();
    const std::string expected = h.registry.text();
    ASSERT_LT(expected.size(), metrics::kHttpBufferBytes);
    const std::string log = run_client(h, [&](http::Client& client, auto& check) {
        http::ClientResponse res;
        check(client.get("/metrics", res) == 0 && res.status() == 200, "get");
        check(!res.chunked(), "content-length framing");
        check(res.header("Content-Type") == metrics::kContentType, "content type");
        check(res.body() == expected, "body");
        http::ClientRequest head;
        head.method = http::Method::kHead;
        head.target = "/metrics";
        check(client.request(head, res) == 0 && res.status() == 200 && res.body().empty(), "head");
        check(res.header("Content-Length") == std::to_string(expected.size()), "head length");
    });
    EXPECT_EQ(log, "");
}

TEST(MetricsHttp, LargeExpositionChunked) {
    Harness h;
    auto family = h.registry.counter_family("large_total", "Large.", {"id", "padding"});
    for (int i = 0; i < 500; ++i) {
        family.with({std::to_string(i), std::string(40, static_cast<char>('a' + i % 26))}).add(
            static_cast<std::uint64_t>(i));
    }
    h.start();
    const std::string expected = h.registry.text();
    ASSERT_GT(expected.size(), 4 * metrics::kHttpBufferBytes);
    const std::string log = run_client(h, [&](http::Client& client, auto& check) {
        http::ClientResponse res;
        for (int i = 0; i < 3; ++i) {  // keep-alive after a chunked response
            check(client.get("/metrics", res) == 0 && res.status() == 200, "get");
            check(res.chunked(), "chunked framing");
            check(res.header("Content-Type") == metrics::kContentType, "content type");
            check(res.body() == expected, "body");
        }
        check(client.connects() == 1, "one connection");
    });
    EXPECT_EQ(log, "");
}

TEST(MetricsHttp, ScrapesWhileFibersUpdate) {
    Harness h;
    metrics::Counter requests = h.registry.counter("app_requests_total", "Requests.");
    metrics::Histogram latency =
        h.registry.histogram("app_latency_seconds", "Latency.", metrics::kLatencyBucketsUs, 1e6);
    metrics::register_scheduler(h.registry, h.scheduler);
    h.router.get("/work", [&](http::Request&, http::Response& res) noexcept {
        requests.inc();
        latency.observe(123);
        res.send("ok\n");
    });
    h.start();
    constexpr int kWorkers = 8;
    constexpr int kRequests = 50;
    std::atomic<int> failures{0};
    std::atomic<int> scrapes{0};
    fiber::WaitGroup wg;
    for (int c = 0; c < kWorkers; ++c) {
        wg.go(h.scheduler, [&]() noexcept {
            http::Client client(h.scheduler, h.endpoint());
            http::ClientResponse res;
            for (int i = 0; i < kRequests; ++i) {
                if (client.get("/work", res) != 0 || res.status() != 200) {
                    failures.fetch_add(1);
                }
            }
        });
    }
    wg.go(h.scheduler, [&]() noexcept {
        http::Client client(h.scheduler, h.endpoint());
        http::ClientResponse res;
        for (int i = 0; i < 20; ++i) {
            if (client.get("/metrics", res) != 0 || res.status() != 200 ||
                res.body().find("# TYPE fibers_spawned_total counter\n") == std::string_view::npos ||
                res.body().find("# TYPE app_latency_seconds histogram\n") == std::string_view::npos) {
                failures.fetch_add(1);
            }
            scrapes.fetch_add(1);
        }
    });
    wg.wait();
    EXPECT_EQ(failures.load(), 0);
    EXPECT_EQ(scrapes.load(), 20);
    EXPECT_EQ(requests.value(), std::uint64_t{kWorkers} * kRequests);
    EXPECT_EQ(latency.count(), std::uint64_t{kWorkers} * kRequests);
    const std::string text = h.registry.text();
    EXPECT_NE(text.find("app_requests_total 400\n"), std::string::npos) << text;
}

}  // namespace
