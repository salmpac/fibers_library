// Prints a representative Prometheus exposition to stdout: every instrument
// kind with labels and escaping, and the built-in collectors of a live
// Scheduler. The metrics_promtool_check test pipes it into
// `promtool check metrics` (format + lint).

#include <chrono>
#include <cstdio>
#include <string>

#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/wait_group.hpp"
#include "fibers/metrics/collectors.hpp"
#include "fibers/metrics/metrics.hpp"
#include "fibers/sched/scheduler.hpp"

namespace metrics = fibers::metrics;
namespace fiber = fibers::fiber;

int main() {
    metrics::Registry registry({.shards = 4});
    auto requests = registry.counter_family("app_requests_total", "Requests handled, by channel and status.",
                                            {"channel", "status"});
    requests.with({"sms", "ok"}).add(3);
    requests.with({"email", "fail \"quoted\" \\ and\nnewline"}).inc();
    registry.gauge("app_in_flight_requests", "Requests in flight.\nSecond line with a \\ backslash.").set(-2);
    auto latency = registry.histogram_family("app_request_duration_seconds", "Request latency.", {"route"},
                                             metrics::kLatencyBucketsUs, 1e6);
    for (const char* route : {"/items", "/items/{id}"}) {
        metrics::Histogram h = latency.with({route});
        for (int us = 50; us < 20'000'000; us *= 3) {
            h.observe(us);
        }
    }
    registry.histogram("app_payload_size_bytes", "Payload sizes.", metrics::exponential_buckets(64, 4, 6))
        .observe(1000);

    fibers::sched::SchedulerOptions options;
    options.pool.num_workers = 2;
    options.stacks.max_stacks = 64;
    fibers::sched::Scheduler scheduler(options);
    metrics::register_scheduler(registry, scheduler);
    fiber::WaitGroup wg;
    for (int i = 0; i < 8; ++i) {
        wg.go(scheduler, []() noexcept { fiber::this_fiber::sleep_for(std::chrono::microseconds(100)); });
    }
    wg.wait();

    const std::string text = registry.text();
    std::fwrite(text.data(), 1, text.size(), stdout);
    scheduler.stop();
    return 0;
}
