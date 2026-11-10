// fibers::metrics over TwistSync under Twist: deterministic simulation
// (TWIST_SIM: Random and PCT schedulers) or real threads with fault
// injection (TWIST_FAULTY, with TSan in the clang-twist-faulty-tsan preset).
//
// The protocols checked:
//   * sharded cells: relaxed fetch_adds on per-shard rows (threads sharing a
//     row and not), summed by a concurrent scraper: every counter it reads
//     is monotonic, a histogram's count equals the sum of its buckets in the
//     same read, and the totals are exact after the joins;
//   * append-only publication: instruments (their label strings and zeroed
//     cells) and families registered while another thread scrapes; a scrape
//     that sees a node must see it fully built (TSan / the simulator's race
//     checks on the plain memory behind it), and the final scrape sees all.

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <string>
#include <string_view>

#include <twist/assist/assert.hpp>
#include <twist/cross.hpp>
#include <twist/ed/std/thread.hpp>

#if defined(__TWIST_SIM__)
#include <twist/sim.hpp>
#endif

#include "fibers/metrics/metrics.hpp"
#include "fibers/sync/twist_sync.hpp"

namespace metrics = fibers::metrics;

namespace {

using Sync = fibers::sync::TwistSync;
using Registry = metrics::BasicRegistry<Sync>;
using Counter = metrics::BasicCounter<Sync>;
using Histogram = metrics::BasicHistogram<Sync>;
using Thread = twist::ed::std::thread;

std::size_t env_or(const char* name, std::size_t fallback) {
    const char* value = std::getenv(name);
    return value != nullptr ? static_cast<std::size_t>(std::strtoull(value, nullptr, 10)) : fallback;
}

// Env: FIBERS_TWIST_RUNS (runs per scheduler / repetitions), FIBERS_TWIST_SEED.
void check(const std::function<void()>& body) {
#if defined(__TWIST_SIM__)
    const std::size_t runs = env_or("FIBERS_TWIST_RUNS", 5'000);
    const std::uint64_t seed = env_or("FIBERS_TWIST_SEED", 42);
    {
        twist::sim::sched::RandomScheduler random{{.seed = seed, .max_runs = runs}};
        const auto explored = twist::sim::Explore(random, body);
        if (explored.found) {
            twist::sim::Print(body, explored.found->schedule);
            FAIL() << "Random scheduler, after " << explored.sims << " runs:\n" << explored.found->result.std_err;
        }
    }
    {
        twist::sim::sched::PctScheduler pct{{.depth = 3, .seed = seed + 1, .max_runs = runs}};
        const auto explored = twist::sim::Explore(pct, body);
        if (explored.found) {
            twist::sim::Print(body, explored.found->schedule);
            FAIL() << "PCT scheduler, after " << explored.sims << " runs:\n" << explored.found->result.std_err;
        }
    }
#else
    const std::size_t runs = env_or("FIBERS_TWIST_RUNS", 50);
    for (std::size_t i = 0; i < runs; ++i) {
        twist::cross::Run(body);
    }
#endif
}

// A scrape into a buffer that holds everything (no sink).
std::string scrape(const Registry& registry) {
    char buffer[4096];
    metrics::Exposition out(buffer);
    registry.write(out);
    TWIST_ASSERT_M(out.error() == 0, "exposition fits");
    return std::string(out.buffered());
}

bool contains(const std::string& text, std::string_view what) { return text.find(what) != std::string::npos; }

// Updaters on two shards (three threads: two of them share a row) while a
// scraper reads the instruments twice and renders once.
void sharded_cells_scenario() {
    Registry registry({.shards = 2});
    Counter counter = registry.counter("c_total", "C.");
    Histogram histogram = registry.histogram("h", "H.", {1, 2});

    auto updater = [&](std::int64_t base) {
        for (std::int64_t i = 0; i < 2; ++i) {
            counter.inc();
            histogram.observe(base + i);
        }
    };
    Thread a(updater, 0);
    Thread b(updater, 1);
    Thread c(updater, 2);
    Thread scraper([&] {
        const std::uint64_t c1 = counter.value();
        const std::uint64_t n1 = histogram.count();
        const std::string text = scrape(registry);
        const std::uint64_t c2 = counter.value();
        const std::uint64_t n2 = histogram.count();
        TWIST_ASSERT_M(c1 <= c2 && c2 <= 6, "counter monotonic and bounded");
        TWIST_ASSERT_M(n1 <= n2 && n2 <= 6, "histogram count monotonic and bounded");
        TWIST_ASSERT_M(contains(text, "# TYPE h histogram\n"), "histogram rendered");
    });
    a.join();
    b.join();
    c.join();
    scraper.join();

    TWIST_ASSERT_M(counter.value() == 6, "exact counter total");
    TWIST_ASSERT_M(histogram.count() == 6, "exact histogram count");
    // Observed: 0, 1 | 1, 2 | 2, 3 -> buckets le=1: {0,1,1}, le=2: {2,2}, +Inf: {3}.
    TWIST_ASSERT_M(histogram.bucket_count(0) == 3, "bucket le=1");
    TWIST_ASSERT_M(histogram.bucket_count(1) == 2, "bucket le=2");
    TWIST_ASSERT_M(histogram.bucket_count(2) == 1, "bucket +Inf");
    TWIST_ASSERT_M(histogram.sum() == 9, "exact sum");
    const std::string text = scrape(registry);
    TWIST_ASSERT_M(contains(text, "c_total 6\n"), "final counter line");
    TWIST_ASSERT_M(contains(text, "h_bucket{le=\"+Inf\"} 6\nh_sum 9\nh_count 6\n"), "final histogram lines");
}

// Registration racing with a scrape: a new instrument of an existing family,
// a new family, and the same instrument obtained by two threads.
void registration_scenario() {
    Registry registry({.shards = 2});
    auto family = registry.counter_family("req_total", "R.", {"route"});
    family.with({"/a"}).inc();

    Thread registrar([&] {
        family.with({"/b"}).add(2);
        registry.gauge("late_gauge", "Late.").set(-3);
    });
    Thread twin([&] { family.with({"/b"}).add(5); });
    Thread scraper([&] {
        const std::string text = scrape(registry);
        TWIST_ASSERT_M(contains(text, "req_total{route=\"/a\"} 1\n"), "the first instrument is always there");
        // If the new instrument is visible at all, its labels are complete.
        if (contains(text, "/b")) {
            TWIST_ASSERT_M(contains(text, "req_total{route=\"/b\"} "), "published instrument fully built");
        }
        if (contains(text, "late_gauge")) {
            TWIST_ASSERT_M(contains(text, "# HELP late_gauge Late.\n# TYPE late_gauge gauge\nlate_gauge "),
                         "published family fully built");
        }
    });
    registrar.join();
    twin.join();
    scraper.join();

    const std::string text = scrape(registry);
    TWIST_ASSERT_M(contains(text, "req_total{route=\"/b\"} 7\n"), "one instrument for one label set");
    TWIST_ASSERT_M(contains(text, "late_gauge -3\n"), "late family");
}

TEST(MetricsTwist, ShardedCellsWithConcurrentScrape) { check(sharded_cells_scenario); }

TEST(MetricsTwist, RegistrationWhileScraping) { check(registration_scenario); }

}  // namespace
