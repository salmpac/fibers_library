// fibers::metrics: the exposition format (golden output: HELP / TYPE lines,
// escaping, histograms, ordering, collectors), the writer's buffering,
// instruments (histogram buckets, sharded sums), registration rules,
// concurrency (threads and migrating fibers updating while scrapes run:
// totals exact after join, counters monotonic, histogram _count == +Inf),
// and the built-in collectors on a live Scheduler.

#include <gtest/gtest.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/wait_group.hpp"
#include "fibers/metrics/collectors.hpp"
#include "fibers/metrics/metrics.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace metrics = fibers::metrics;
using fibers::sched::Scheduler;

namespace {

// The value of the first sample line that starts with `prefix` (name and
// labels, without the space); -1 if there is none.
double sample_value(const std::string& text, std::string_view prefix) {
    std::size_t pos = 0;
    while (pos < text.size()) {
        const std::size_t end = text.find('\n', pos);
        const std::string_view line(text.data() + pos, (end == std::string::npos ? text.size() : end) - pos);
        if (line.size() > prefix.size() && line.starts_with(prefix) && line[prefix.size()] == ' ') {
            return std::strtod(std::string(line.substr(prefix.size() + 1)).c_str(), nullptr);
        }
        if (end == std::string::npos) {
            break;
        }
        pos = end + 1;
    }
    return -1;
}

bool has_line(const std::string& text, std::string_view line) {
    const std::string whole = std::string(line) + "\n";
    return text.starts_with(whole) || text.find("\n" + whole) != std::string::npos;
}

void build_golden(metrics::Registry& r) {
    auto requests = r.counter_family("app_requests_total", "Requests handled.", {"channel", "status"});
    requests.with({"sms", "ok"}).add(3);
    requests.with({"email", "fail"}).inc();
    requests.with({"sms", "ok"}).inc();  // the same instrument again
    r.gauge("app_in_flight", "Help with \\ backslash\nand newline.").set(-5);
    r.gauge_family("app_escaped", "Escaping.", {"path"}).with({"a\"b\\c\nd"}).set(7);
    auto latency = r.histogram_family("app_latency_seconds", "Latency.", {"route"}, {1000, 5000, 100000}, 1e6);
    metrics::Histogram items = latency.with({"/items"});
    for (const std::int64_t us : {500, 1000, 1001, 200000}) {
        items.observe(us);
    }
    metrics::Histogram sizes = r.histogram("app_size_bytes", "Sizes.", {10, 100});
    sizes.observe(10);
    sizes.observe(-3);
    r.counter_family("app_unused_total", "No instruments: not exposed.", {"x"});
    r.add_collector(
        [](void*, metrics::Exposition& out) noexcept {
            out.family("app_build_info", "Build.", metrics::MetricType::kGauge);
            out.sample("app_build_info", {{"version", "1.0"}, {"commit", "c\"1"}}, 1);
            out.family("app_ratio", "Ratio.", metrics::MetricType::kGauge);
            out.sample("app_ratio", 0.25);
        },
        nullptr);
}

constexpr std::string_view kGolden =
    "# HELP app_requests_total Requests handled.\n"
    "# TYPE app_requests_total counter\n"
    "app_requests_total{channel=\"sms\",status=\"ok\"} 4\n"
    "app_requests_total{channel=\"email\",status=\"fail\"} 1\n"
    "# HELP app_in_flight Help with \\\\ backslash\\nand newline.\n"
    "# TYPE app_in_flight gauge\n"
    "app_in_flight -5\n"
    "# HELP app_escaped Escaping.\n"
    "# TYPE app_escaped gauge\n"
    "app_escaped{path=\"a\\\"b\\\\c\\nd\"} 7\n"
    "# HELP app_latency_seconds Latency.\n"
    "# TYPE app_latency_seconds histogram\n"
    "app_latency_seconds_bucket{route=\"/items\",le=\"0.001\"} 2\n"
    "app_latency_seconds_bucket{route=\"/items\",le=\"0.005\"} 3\n"
    "app_latency_seconds_bucket{route=\"/items\",le=\"0.1\"} 3\n"
    "app_latency_seconds_bucket{route=\"/items\",le=\"+Inf\"} 4\n"
    "app_latency_seconds_sum{route=\"/items\"} 0.202501\n"
    "app_latency_seconds_count{route=\"/items\"} 4\n"
    "# HELP app_size_bytes Sizes.\n"
    "# TYPE app_size_bytes histogram\n"
    "app_size_bytes_bucket{le=\"10\"} 2\n"
    "app_size_bytes_bucket{le=\"100\"} 2\n"
    "app_size_bytes_bucket{le=\"+Inf\"} 2\n"
    "app_size_bytes_sum 7\n"
    "app_size_bytes_count 2\n"
    "# HELP app_build_info Build.\n"
    "# TYPE app_build_info gauge\n"
    "app_build_info{version=\"1.0\",commit=\"c\\\"1\"} 1\n"
    "# HELP app_ratio Ratio.\n"
    "# TYPE app_ratio gauge\n"
    "app_ratio 0.25\n";

// ---- exposition format ----------------------------------------------------------

TEST(MetricsExposition, Golden) {
    metrics::Registry r({.shards = 4});
    build_golden(r);
    EXPECT_EQ(r.text(), kGolden);
    // Deterministic: a second scrape is the same.
    EXPECT_EQ(r.text(), kGolden);
}

TEST(MetricsExposition, StreamsThroughTinyBuffers) {
    metrics::Registry r({.shards = 2});
    build_golden(r);
    for (const std::size_t size : {1, 2, 7, 64, 333}) {
        std::string streamed;
        std::size_t calls = 0;
        auto sink = [&](std::string_view data) noexcept -> int {
            ++calls;
            EXPECT_LE(data.size(), size);
            streamed.append(data);
            return 0;
        };
        std::vector<char> buffer(size);
        metrics::Exposition out(buffer, metrics::Sink::ref(sink));
        r.write(out);
        EXPECT_EQ(out.flush(), 0);
        EXPECT_EQ(streamed, kGolden) << "buffer " << size;
        EXPECT_GE(calls, kGolden.size() / size);
    }
}

TEST(MetricsExposition, BufferWithoutSink) {
    metrics::Registry r({.shards = 1});
    build_golden(r);
    // Exactly fits: no error, nothing flushed.
    std::vector<char> exact(kGolden.size());
    metrics::Exposition fits(exact);
    r.write(fits);
    EXPECT_EQ(fits.error(), 0);
    EXPECT_FALSE(fits.flushed());
    EXPECT_EQ(fits.buffered(), kGolden);
    // One byte short: EMSGSIZE, sticky.
    std::vector<char> small(kGolden.size() - 1);
    metrics::Exposition overflow(small);
    r.write(overflow);
    EXPECT_EQ(overflow.error(), EMSGSIZE);
    EXPECT_EQ(overflow.flush(), EMSGSIZE);
}

TEST(MetricsExposition, SinkErrorIsSticky) {
    metrics::Registry r({.shards = 1});
    build_golden(r);
    std::size_t calls = 0;
    auto sink = [&](std::string_view) noexcept -> int {
        ++calls;
        return calls == 2 ? EPIPE : 0;
    };
    char buffer[16];
    metrics::Exposition out(buffer, metrics::Sink::ref(sink));
    r.write(out);
    EXPECT_EQ(out.flush(), EPIPE);
    EXPECT_EQ(calls, 2u);
}

TEST(MetricsExposition, Values) {
    char buffer[256];
    metrics::Exposition out(buffer);
    out.sample("a", std::numeric_limits<std::uint64_t>::max());
    out.sample("b", std::numeric_limits<std::int64_t>::min());
    out.sample("c", 1.5);
    out.sample("d", 1e-7);
    out.sample("e", std::numeric_limits<double>::infinity());
    out.sample("f", -std::numeric_limits<double>::infinity());
    out.sample("g", std::numeric_limits<double>::quiet_NaN());
    out.sample("h", 0.1f);
    out.sample("i", -0.0);
    EXPECT_EQ(out.buffered(),
              "a 18446744073709551615\n"
              "b -9223372036854775808\n"
              "c 1.5\n"
              "d 1e-07\n"
              "e +Inf\n"
              "f -Inf\n"
              "g NaN\n"
              "h 0.10000000149011612\n"
              "i -0\n");
}

TEST(MetricsExposition, NameRules) {
    using metrics::detail::valid_label_name;
    using metrics::detail::valid_metric_name;
    EXPECT_TRUE(valid_metric_name("a"));
    EXPECT_TRUE(valid_metric_name("_a:b_9"));
    EXPECT_TRUE(valid_metric_name(":x"));
    EXPECT_FALSE(valid_metric_name(""));
    EXPECT_FALSE(valid_metric_name("9a"));
    EXPECT_FALSE(valid_metric_name("a-b"));
    EXPECT_FALSE(valid_metric_name("a b"));
    EXPECT_TRUE(valid_label_name("a_1"));
    EXPECT_TRUE(valid_label_name("_a"));
    EXPECT_FALSE(valid_label_name("__a"));
    EXPECT_FALSE(valid_label_name("a:b"));
    EXPECT_FALSE(valid_label_name("1a"));
    EXPECT_FALSE(valid_label_name(""));
}

// ---- registry ------------------------------------------------------------------

TEST(MetricsRegistry, SameNameSameFamilyAndInstrument) {
    metrics::Registry r({.shards = 4});
    metrics::Counter a = r.counter("x_total", "X.");
    metrics::Counter b = r.counter("x_total", "X (help of a later registration is ignored).");
    a.add(2);
    b.inc();
    EXPECT_EQ(a.value(), 3u);
    auto f1 = r.gauge_family("g", "G.", {"k"});
    auto f2 = r.gauge_family("g", "G.", {"k"});
    f1.with({"v"}).set(4);
    EXPECT_EQ(f2.with({"v"}).value(), 4);
    const std::vector<std::int64_t> bounds{1, 2, 3};
    metrics::Histogram h1 = r.histogram("h", "H.", bounds, 10);
    metrics::Histogram h2 = r.histogram("h", "H.", bounds, 10);
    h1.observe(2);
    EXPECT_EQ(h2.count(), 1u);
    EXPECT_EQ(r.text().find("# TYPE x_total"), r.text().rfind("# TYPE x_total"));
}

TEST(MetricsRegistry, ShardsNormalised) {
    EXPECT_EQ(metrics::Registry({.shards = 3}).shards(), 4u);
    EXPECT_EQ(metrics::Registry({.shards = 1}).shards(), 1u);
    EXPECT_EQ(metrics::Registry({.shards = 100000}).shards(), metrics::kMaxShards);
    EXPECT_GE(metrics::Registry().shards(), 1u);
}

TEST(MetricsRegistry, ManyInstrumentsAcrossChunks) {
    // More cells than one chunk row holds, plus a histogram wider than a row.
    metrics::Registry r({.shards = 8});
    auto family = r.counter_family("c_total", "C.", {"i"});
    std::vector<metrics::Counter> counters;
    for (int i = 0; i < 100; ++i) {
        counters.push_back(family.with({std::to_string(i)}));
    }
    std::vector<std::int64_t> wide;
    for (std::int64_t b = 1; b <= 50; ++b) {
        wide.push_back(b * 10);
    }
    metrics::Histogram h = r.histogram("wide", "Wide.", wide);
    auto gauges = r.gauge_family("g", "G.", {"i"});
    for (int i = 0; i < 100; ++i) {
        counters[static_cast<std::size_t>(i)].add(static_cast<std::uint64_t>(i));
        gauges.with({std::to_string(i)}).set(-i);
        h.observe(i * 7);
    }
    const std::string text = r.text();
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(counters[static_cast<std::size_t>(i)].value(), static_cast<std::uint64_t>(i));
        EXPECT_EQ(sample_value(text, "c_total{i=\"" + std::to_string(i) + "\"}"), i);
        EXPECT_EQ(sample_value(text, "g{i=\"" + std::to_string(i) + "\"}"), -i);
    }
    EXPECT_EQ(h.count(), 100u);
    EXPECT_EQ(sample_value(text, "wide_count"), 100);
    EXPECT_EQ(sample_value(text, "wide_bucket{le=\"500\"}"), 72);  // i * 7 <= 500: i <= 71
}

TEST(MetricsRegistry, CollectorsCalledPerScrapeAndDropped) {
    struct State {
        int scrapes = 0;
        bool dropped = false;
    } state;
    {
        metrics::Registry r({.shards = 1});
        r.add_collector(
            [](void* c, metrics::Exposition& out) noexcept {
                auto& s = *static_cast<State*>(c);
                ++s.scrapes;
                out.family("scrapes_total", "Scrapes.", metrics::MetricType::kCounter);
                out.sample("scrapes_total", s.scrapes);
            },
            &state, [](void* c) noexcept { static_cast<State*>(c)->dropped = true; });
        EXPECT_TRUE(has_line(r.text(), "scrapes_total 1"));
        EXPECT_TRUE(has_line(r.text(), "scrapes_total 2"));
        EXPECT_FALSE(state.dropped);
    }
    EXPECT_TRUE(state.dropped);
}

TEST(MetricsRegistryDeathTest, ProgrammingErrorsFailFast) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    metrics::Registry r({.shards = 1});
    EXPECT_DEATH(r.counter("1bad", "x"), "invalid metric name");
    EXPECT_DEATH(r.counter_family("c_total", "x", {"__reserved"}), "invalid label name");
    EXPECT_DEATH(r.histogram_family("h", "x", {"le"}, {1}), "invalid label name");
    EXPECT_DEATH(r.counter_family("c_total", "x", {"a", "a"}), "duplicate label name");
    EXPECT_DEATH(r.histogram("h", "x", {2, 1}), "strictly ascending");
    EXPECT_DEATH(r.histogram("h", "x", {1, 2}, 0), "divisor");
    r.counter_family("c_total", "x", {"a"});
    EXPECT_DEATH(r.gauge("c_total", "x"), "registered again");
    EXPECT_DEATH(r.counter_family("c_total", "x", {"b"}), "registered again");
    EXPECT_DEATH(r.counter_family("c_total", "x", {"a"}).with({"1", "2"}), "wrong number of label values");
}

// ---- instruments ---------------------------------------------------------------

TEST(MetricsHistogram, Buckets) {
    metrics::Registry r({.shards = 2});
    // Few bounds (linear scan) and many (binary search): same rule, the first
    // bound >= value.
    for (const std::size_t n : {std::size_t{3}, std::size_t{20}}) {
        std::vector<std::int64_t> bounds;
        for (std::size_t i = 0; i < n; ++i) {
            bounds.push_back(static_cast<std::int64_t>(i) * 10);  // 0, 10, 20, ...
        }
        metrics::Histogram h = r.histogram("h" + std::to_string(n), "H.", bounds);
        const std::int64_t last = bounds.back();
        EXPECT_EQ(h.bucket_of(std::numeric_limits<std::int64_t>::min()), 0u);
        EXPECT_EQ(h.bucket_of(-1), 0u);
        EXPECT_EQ(h.bucket_of(0), 0u);
        EXPECT_EQ(h.bucket_of(1), 1u);
        EXPECT_EQ(h.bucket_of(10), 1u);
        EXPECT_EQ(h.bucket_of(11), 2u);
        EXPECT_EQ(h.bucket_of(last), n - 1);
        EXPECT_EQ(h.bucket_of(last + 1), n);
        EXPECT_EQ(h.bucket_of(std::numeric_limits<std::int64_t>::max()), n);
        for (std::int64_t v = -5; v <= last + 5; ++v) {
            h.observe(v);
        }
        std::uint64_t total = 0;
        for (std::size_t i = 0; i <= n; ++i) {
            total += h.bucket_count(i);
        }
        EXPECT_EQ(total, h.count());
        EXPECT_EQ(h.count(), static_cast<std::uint64_t>(last + 11));
        EXPECT_EQ(h.bucket_count(0), 6u);   // -5..0
        EXPECT_EQ(h.bucket_count(1), 10u);  // 1..10
        EXPECT_EQ(h.bucket_count(n), 5u);   // last+1..last+5
        std::int64_t sum = 0;
        for (std::int64_t v = -5; v <= last + 5; ++v) {
            sum += v;
        }
        EXPECT_EQ(h.sum(), sum);
    }
}

TEST(MetricsHistogram, EmptyBoundsOnlyInf) {
    metrics::Registry r({.shards = 1});
    metrics::Histogram h = r.histogram("only_inf", "H.", {});
    h.observe(5);
    h.observe(7);
    EXPECT_EQ(r.text(),
              "# HELP only_inf H.\n# TYPE only_inf histogram\n"
              "only_inf_bucket{le=\"+Inf\"} 2\nonly_inf_sum 12\nonly_inf_count 2\n");
}

TEST(MetricsHistogram, ExponentialAndLinearBuckets) {
    EXPECT_EQ(metrics::exponential_buckets(1, 2, 5), (std::vector<std::int64_t>{1, 2, 4, 8, 16}));
    EXPECT_EQ(metrics::exponential_buckets(1, 1.5, 4), (std::vector<std::int64_t>{1, 2, 3}));  // 1, 1.5, 2.25, 3.375
    EXPECT_EQ(metrics::linear_buckets(5, 5, 3), (std::vector<std::int64_t>{5, 10, 15}));
    metrics::Registry r({.shards = 1});
    metrics::Histogram h = r.histogram("lat_seconds", "L.", metrics::kLatencyBucketsUs, 1e6);
    h.observe(100);
    const std::string text = r.text();
    EXPECT_TRUE(has_line(text, "lat_seconds_bucket{le=\"0.0001\"} 1")) << text;
    EXPECT_TRUE(has_line(text, "lat_seconds_bucket{le=\"0.00025\"} 1")) << text;
    EXPECT_TRUE(has_line(text, "lat_seconds_bucket{le=\"10\"} 1")) << text;
    EXPECT_TRUE(has_line(text, "lat_seconds_sum 0.0001")) << text;
}

// ---- concurrency ---------------------------------------------------------------

// Checks a scrape against the previous one: counters never go down, a
// histogram's _count equals its +Inf bucket and never goes down.
struct ScrapeChecker {
    double last_counter = -1;  // sample_value() of a missing sample is -1
    double last_count = -1;
    int violations = 0;
    int scrapes = 0;

    void check(const std::string& text) {
        ++scrapes;
        const double counter = sample_value(text, "work_total");
        const double count = sample_value(text, "work_size_count");
        const double inf = sample_value(text, "work_size_bucket{le=\"+Inf\"}");
        if (counter < last_counter || count < last_count || count != inf) {
            ++violations;
        }
        last_counter = counter;
        last_count = count;
    }
};

TEST(MetricsConcurrency, ThreadsUpdateWhileScraping) {
    constexpr int kThreads = 8;
    constexpr int kOps = 20000;
    metrics::Registry r({.shards = 4});  // fewer shards than threads: rows are shared
    metrics::Counter counter = r.counter("work_total", "Work.");
    metrics::Histogram sizes = r.histogram("work_size", "Sizes.", {0, 1, 2, 4, 8});
    metrics::Gauge level = r.gauge("work_level", "Level.");
    std::atomic<bool> done{false};
    ScrapeChecker checker;
    std::thread scraper([&] {
        while (!done.load(std::memory_order_acquire)) {
            checker.check(r.text());
        }
    });
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < kOps; ++i) {
                counter.inc();
                sizes.observe((i + t) % 10);
                level.add(1);
                level.sub(1);
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    done.store(true, std::memory_order_release);
    scraper.join();
    EXPECT_EQ(checker.violations, 0);
    EXPECT_GT(checker.scrapes, 0);
    EXPECT_EQ(counter.value(), std::uint64_t{kThreads} * kOps);
    EXPECT_EQ(sizes.count(), std::uint64_t{kThreads} * kOps);
    EXPECT_EQ(level.value(), 0);
    // Each thread observes (i + t) % 10 for i in [0, kOps): kOps / 10 of each
    // value 0..9.
    constexpr std::uint64_t kEach = std::uint64_t{kThreads} * kOps / 10;
    EXPECT_EQ(sizes.bucket_count(0), kEach);      // 0
    EXPECT_EQ(sizes.bucket_count(1), kEach);      // 1
    EXPECT_EQ(sizes.bucket_count(2), kEach);      // 2
    EXPECT_EQ(sizes.bucket_count(3), 2 * kEach);  // 3, 4
    EXPECT_EQ(sizes.bucket_count(4), 4 * kEach);  // 5..8
    EXPECT_EQ(sizes.bucket_count(5), kEach);      // 9
    EXPECT_EQ(sizes.sum(), static_cast<std::int64_t>(kEach * 45));
    const std::string text = r.text();
    EXPECT_EQ(sample_value(text, "work_total"), kThreads * kOps);
    EXPECT_EQ(sample_value(text, "work_size_bucket{le=\"4\"}"), 5.0 * kEach);
}

fibers::sched::SchedulerOptions scheduler_options(std::size_t workers) {
    fibers::sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = 1;
    o.blocking.num_threads = 1;
    o.stacks.max_stacks = 512;
    o.stacks.stack_size = 64 * 1024;
    return o;
}

TEST(MetricsConcurrency, MigratingFibersUpdateWhileScraping) {
    constexpr int kFibers = 200;
    constexpr int kOps = 2000;
    metrics::Registry r({.shards = 2});
    auto family = r.counter_family("work_total", "Work.", {"kind"});
    metrics::Counter even = family.with({"even"});
    metrics::Counter odd = family.with({"odd"});
    metrics::Counter total = r.counter("work_total_all", "All work.");
    metrics::Histogram sizes = r.histogram("work_size", "Sizes.", {0, 1, 2, 4, 8});
    Scheduler scheduler(scheduler_options(4));
    std::atomic<bool> done{false};
    ScrapeChecker checker;
    std::thread scraper([&] {
        while (!done.load(std::memory_order_acquire)) {
            checker.check(r.text());
        }
    });
    fiber::WaitGroup wg;
    for (int f = 0; f < kFibers; ++f) {
        wg.go(scheduler, [&, f]() noexcept {
            for (int i = 0; i < kOps; ++i) {
                (i % 2 == 0 ? even : odd).inc();
                total.inc();
                sizes.observe((i + f) % 10);
                if (i % 100 == 0) {
                    fiber::this_fiber::yield();  // may continue on another worker
                }
            }
        });
    }
    wg.wait();
    done.store(true, std::memory_order_release);
    scraper.join();
    scheduler.stop();
    EXPECT_EQ(checker.violations, 0);
    EXPECT_EQ(even.value(), std::uint64_t{kFibers} * kOps / 2);
    EXPECT_EQ(odd.value(), std::uint64_t{kFibers} * kOps / 2);
    EXPECT_EQ(total.value(), std::uint64_t{kFibers} * kOps);
    EXPECT_EQ(sizes.count(), std::uint64_t{kFibers} * kOps);
    EXPECT_EQ(sizes.sum(), static_cast<std::int64_t>(std::uint64_t{kFibers} * kOps / 10 * 45));
    const auto counts = scheduler.fiber_counts();
    EXPECT_EQ(counts.spawned, static_cast<std::uint64_t>(kFibers));
    EXPECT_EQ(counts.finished, static_cast<std::uint64_t>(kFibers));
}

TEST(MetricsConcurrency, RegistrationWhileScraping) {
    constexpr int kChildren = 300;
    metrics::Registry r({.shards = 2});
    auto family = r.counter_family("reg_total", "Registered while scraping.", {"id"});
    std::atomic<bool> done{false};
    int scrapes = 0;
    std::thread scraper([&] {
        std::size_t last = 0;
        while (!done.load(std::memory_order_acquire)) {
            const std::string text = r.text();
            // Children only get added: a later scrape has at least as many.
            std::size_t lines = 0;
            for (const char c : text) {
                lines += c == '\n' ? 1 : 0;
            }
            EXPECT_GE(lines, last);
            last = lines;
            ++scrapes;
        }
    });
    std::vector<std::thread> registrars;
    for (int t = 0; t < 3; ++t) {
        registrars.emplace_back([&, t] {
            for (int i = 0; i < kChildren; ++i) {
                family.with({std::to_string(t * kChildren + i)}).inc();
                r.gauge("reg_gauge_" + std::to_string(t) + "_" + std::to_string(i), "G.").set(i);
            }
        });
    }
    for (auto& t : registrars) {
        t.join();
    }
    done.store(true, std::memory_order_release);
    scraper.join();
    const std::string text = r.text();
    for (int i = 0; i < 3 * kChildren; ++i) {
        ASSERT_EQ(sample_value(text, "reg_total{id=\"" + std::to_string(i) + "\"}"), 1) << i;
    }
    EXPECT_EQ(sample_value(text, "reg_gauge_2_299"), 299);
    EXPECT_GT(scrapes, 0);
}

// ---- built-in collectors -------------------------------------------------------

TEST(MetricsCollectors, Scheduler) {
    metrics::Registry r({.shards = 2});
    Scheduler scheduler(scheduler_options(2));
    metrics::register_scheduler(r, scheduler);
    metrics::register_thread_pool(r, scheduler.pool(), "rt");
    constexpr int kFibers = 10;
    fiber::WaitGroup wg;
    for (int i = 0; i < kFibers; ++i) {
        wg.go(scheduler, []() noexcept { fiber::this_fiber::sleep_for(std::chrono::milliseconds(1)); });
    }
    wg.wait();
    const std::string live = r.text();  // while running: only the shape
    EXPECT_TRUE(has_line(live, "# TYPE fibers_spawned_total counter")) << live;
    EXPECT_GE(sample_value(live, "fibers_spawned_total"), kFibers);
    scheduler.stop();  // joins everything: the counters are final

    const std::string text = r.text();
    EXPECT_TRUE(has_line(text, "fibers_spawned_total 10")) << text;
    EXPECT_TRUE(has_line(text, "fibers_finished_total 10")) << text;
    EXPECT_TRUE(has_line(text, "fibers_alive 0")) << text;
    EXPECT_TRUE(has_line(text, "fibers_workers 2")) << text;
    EXPECT_TRUE(has_line(text, "# TYPE fibers_workers_parked gauge")) << text;
    EXPECT_TRUE(has_line(text, "# TYPE fibers_worker_parks_total counter")) << text;
    EXPECT_TRUE(has_line(text, "fibers_worker_queued_tasks{worker=\"0\"} 0")) << text;
    EXPECT_TRUE(has_line(text, "fibers_worker_queued_tasks{worker=\"1\"} 0")) << text;
    EXPECT_TRUE(has_line(text, "fibers_pollers 1")) << text;
    // Each fiber's sleep_for timer fired on the only poller.
    EXPECT_EQ(sample_value(text, "fibers_poller_timers_fired_total{poller=\"0\"}"), kFibers) << text;
    EXPECT_GE(sample_value(text, "fibers_poller_sleeps_total{poller=\"0\"}"), 0) << text;
    EXPECT_GE(sample_value(text, "fibers_poller_timer_wakeups_total{poller=\"0\"}"), 0) << text;
    EXPECT_GE(sample_value(text, "fibers_poller_timers_cancelled_total{poller=\"0\"}"), 0) << text;
    EXPECT_GE(sample_value(text, "fibers_poller_timer_heap_compactions_total{poller=\"0\"}"), 0) << text;
    EXPECT_TRUE(has_line(text, "fibers_blocking_threads 1")) << text;
    EXPECT_GE(sample_value(text, "fibers_blocking_threads_sleeping"), 0) << text;
    EXPECT_GE(sample_value(text, "fibers_blocking_sleeps_total"), 0) << text;
    EXPECT_TRUE(has_line(text, "fibers_stacks_max 512")) << text;
    EXPECT_TRUE(has_line(text, "fibers_stack_size_bytes 65536")) << text;
    const double mapped = sample_value(text, "fibers_stacks_mapped");
    EXPECT_GE(mapped, 1) << text;
    EXPECT_LE(mapped, kFibers) << text;
    EXPECT_TRUE(has_line(text, "rt_workers 2")) << text;
}

TEST(MetricsCollectors, FiberCountsFromWorkersAndOutside) {
    Scheduler scheduler(scheduler_options(3));
    fiber::WaitGroup wg;
    // From outside (shared slot) and from fibers on the workers (own slots).
    for (int i = 0; i < 20; ++i) {
        wg.go(scheduler, [&wg]() noexcept {
            for (int j = 0; j < 5; ++j) {
                wg.go([]() noexcept {});
            }
        });
    }
    wg.wait();
    scheduler.stop();
    const auto counts = scheduler.fiber_counts();
    EXPECT_EQ(counts.spawned, 120u);
    EXPECT_EQ(counts.finished, 120u);
}

TEST(MetricsCollectors, DroppedFibersCountAsFinished) {
    Scheduler scheduler(scheduler_options(1));
    scheduler.stop();  // nothing runs any more: the fibers stay queued
    for (int i = 0; i < 5; ++i) {
        fiber::go(scheduler, []() noexcept {});
    }
    EXPECT_EQ(scheduler.fiber_counts().spawned, 5u);
    EXPECT_EQ(scheduler.fiber_counts().finished, 0u);
    // The destructor drops them and counts them as finished while the pool
    // is being destroyed (ASan: nothing of the pool is touched then).
}

TEST(MetricsCollectorsDeathTest, InvalidPrefix) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    metrics::Registry r({.shards = 1});
    Scheduler scheduler(scheduler_options(1));
    EXPECT_DEATH(metrics::register_scheduler(r, scheduler, "bad-prefix"), "invalid collector prefix");
    EXPECT_DEATH(metrics::register_scheduler(r, scheduler, std::string(65, 'a')), "invalid collector prefix");
}

}  // namespace
