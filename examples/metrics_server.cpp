// A small REST service with Prometheus metrics (fibers::metrics):
//
//   GET    /items/{id}       -> 200 + the value, or 404
//   POST   /items/{id}       -> 201 (created) or 200 (replaced); body = value
//   DELETE /items/{id}       -> 204, or 404
//   GET    /hello            -> 200 "hello"
//   GET    /metrics          -> the Prometheus text exposition
//
//   metrics_server --port=8080 --workers=2 --worker-cpus=0,4 --pollers=1 --poller-cpus=2
//
//   curl -X POST --data 'hello' localhost:8080/items/a; curl localhost:8080/items/b
//   curl -s localhost:8080/metrics | promtool check metrics
//
// What is measured, per route (label route = the pattern, never the raw
// path: label values must come from a small fixed set):
//   app_http_requests_total{route,code}        counter, code = 2xx / 3xx / 4xx / 5xx
//   app_http_request_duration_seconds{route}   histogram, observed in µs
//   app_http_requests_in_flight                gauge
// plus the scheduler's own collectors (fibers_*: workers, parked workers,
// queues, pollers, blocking pool, stacks, fibers spawned / finished).
//
// Every instrument is created once at startup and kept by the handler
// wrapper (RouteMetrics), so a request costs a few relaxed atomics: no
// lookup, hashing or allocation. Stops gracefully on SIGINT / SIGTERM.

#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <string_view>

#include "fibers/fiber/mutex.hpp"
#include "fibers/http/router.hpp"
#include "fibers/metrics/collectors.hpp"
#include "fibers/metrics/http.hpp"
#include "fibers/metrics/metrics.hpp"
#include "http_common.hpp"

namespace fiber = fibers::fiber;
namespace http = fibers::http;
namespace metrics = fibers::metrics;

namespace {

// The instruments of one route.
struct RouteMetrics {
    metrics::Counter by_class[5];  // 1xx .. 5xx
    metrics::Histogram latency;
};

// The service's metric families.
class HttpMetrics {
public:
    explicit HttpMetrics(metrics::Registry& registry)
        : requests_(registry.counter_family("app_http_requests_total", "HTTP requests answered.",
                                            {"route", "code"})),
          latency_(registry.histogram_family("app_http_request_duration_seconds",
                                             "Time from routing to the end of the handler.", {"route"},
                                             metrics::kLatencyBucketsUs, 1e6)),
          in_flight_(registry.gauge("app_http_requests_in_flight", "Requests being handled.")) {}

    // At startup, once per route.
    RouteMetrics route(std::string_view pattern) {
        static constexpr std::string_view kClasses[5] = {"1xx", "2xx", "3xx", "4xx", "5xx"};
        RouteMetrics m;
        for (int i = 0; i < 5; ++i) {
            m.by_class[i] = requests_.with({pattern, kClasses[i]});
        }
        m.latency = latency_.with({pattern});
        return m;
    }

    // Wraps a handler: counts it, times it, tracks requests in flight.
    template <class F>
    auto instrument(std::string_view pattern, F handler) {
        return [this, m = route(pattern), handler](http::Request& req, http::Response& res) mutable noexcept {
            const auto start = std::chrono::steady_clock::now();
            in_flight_.inc();
            handler(req, res);
            const auto elapsed = std::chrono::steady_clock::now() - start;
            m.latency.observe(std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count());
            const int status_class = res.status() / 100;  // 1..9 (Response clamps to 100..999)
            m.by_class[status_class >= 1 && status_class <= 5 ? status_class - 1 : 4].inc();
            in_flight_.dec();
        };
    }

private:
    metrics::CounterFamily requests_;
    metrics::HistogramFamily latency_;
    metrics::Gauge in_flight_;
};

class Store {
public:
    void get(http::Request& req, http::Response& res) {
        std::string value;
        bool found = false;
        {
            std::lock_guard guard(mutex_);
            if (const auto it = items_.find(req.param("id")); it != items_.end()) {
                value = it->second;
                found = true;
            }
        }
        if (!found) {
            res.send(404, "no such item\n", "text/plain");
            return;
        }
        res.send(200, value, "application/octet-stream");
    }

    void put(http::Request& req, http::Response& res) {
        bool created = false;
        {
            std::lock_guard guard(mutex_);
            created = items_.insert_or_assign(std::string(req.param("id")), std::string(req.body())).second;
        }
        res.send(created ? 201 : 200, created ? "created\n" : "replaced\n", "text/plain");
    }

    void remove(http::Request& req, http::Response& res) {
        std::size_t erased = 0;
        {
            std::lock_guard guard(mutex_);
            if (const auto it = items_.find(req.param("id")); it != items_.end()) {
                items_.erase(it);
                erased = 1;
            }
        }
        if (erased == 0) {
            res.send(404, "no such item\n", "text/plain");
            return;
        }
        res.status(204);
    }

private:
    fiber::Mutex mutex_;
    std::map<std::string, std::string, std::less<>> items_;
};

}  // namespace

int main(int argc, char** argv) {
    examples::Args args(argc, argv);
    // Outlives the scheduler and the server (run_http_server stops both
    // before it returns), so no scrape can reach a destroyed collector.
    metrics::Registry registry;
    HttpMetrics http_metrics(registry);
    Store store;

    http::Router router;
    // std::string / std::map may throw bad_alloc; like the rest of the
    // library we treat out-of-memory as fatal (noexcept handlers).
    using http::Request;
    using http::Response;
    router.get("/hello", http_metrics.instrument("/hello", [](Request&, Response& res) noexcept {
        res.send("hello\n");
    }));
    router.get("/items/{id}", http_metrics.instrument("/items/{id}", [&](Request& req, Response& res) noexcept {
        store.get(req, res);
    }));
    router.post("/items/{id}", http_metrics.instrument("/items/{id}", [&](Request& req, Response& res) noexcept {
        store.put(req, res);
    }));
    router.del("/items/{id}", http_metrics.instrument("/items/{id}", [&](Request& req, Response& res) noexcept {
        store.remove(req, res);
    }));
    router.get("/metrics", metrics::http_handler(registry));

    // The scheduler is created inside run_http_server: register its
    // collectors once it exists.
    auto setup = [](fibers::sched::Scheduler& scheduler, void* context) noexcept {
        metrics::register_scheduler(*static_cast<metrics::Registry*>(context), scheduler);
    };
    return examples::run_http_server(args, router.handler(), "metrics_server", setup, &registry);
}
