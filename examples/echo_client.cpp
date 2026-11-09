// Load generator for the echo servers, on fibers.
//
//   echo_client --port=7000 --connections=100 --pipeline=16 --size=64
//               --duration=10 --warmup=2 [--rate=0] [--host=127.0.0.1]
//               [--workers=4 --worker-cpus=12-15 --pollers=1 --poller-cpus=9]
//               [--idle-spins=N]   (ThreadPoolOptions::idle_spins_before_park)
//
// Closed loop (--rate=0): every connection keeps `pipeline` requests of
// `size` bytes in flight: one fiber per connection sends `pipeline`
// requests, then sends one more for each response it reads. Latency is
// response time - send time.
//
// Open loop (--rate=R requests/s in total): every connection sends at R /
// connections requests/s on a fixed schedule (connection c is offset by a
// fraction of the interval), from a writer fiber; a reader fiber takes the
// responses. Latency of request k is measured from its INTENDED send time
// start + k * interval, not from when it was actually written, so a server
// (or client) that falls behind is charged for the whole delay (no
// coordinated omission; wrk2's method). `pipeline` is ignored.
//
// Responses completing inside [start + warmup, start + warmup + duration)
// are counted (rps) and recorded in a fixed-bucket log-linear histogram
// (examples/common.hpp; 64 shards of relaxed atomic counters, no allocation
// on the hot path). Connecting retries ECONNREFUSED a bounded number of
// times (--connect-retries, 20 ms apart), so the client can be started
// right after the server.
//
// Output: a human-readable summary and one line
//   RESULT mode=closed conns=.. pipeline=.. size=.. rate=.. rps=..
//          p50_us=.. p90_us=.. p99_us=.. p999_us=.. max_us=.. cpu=.. errors=..
// where cpu is the client's own CPU time (getrusage) per second of the
// measurement window, in cores.

#include <sys/resource.h>
#include <sys/socket.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "common.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace net = fibers::net;
namespace this_fiber = fibers::fiber::this_fiber;
using fibers::sched::Scheduler;
using examples::Histogram;
using examples::now_ns;

namespace {

constexpr std::size_t kShards = 64;
constexpr std::size_t kMaxSize = 16384;
constexpr std::size_t kMaxPipeline = 256;

struct Config {
    net::Endpoint endpoint;
    std::size_t connections = 1;
    std::size_t pipeline = 1;
    std::size_t size = 64;
    double rate = 0;  // total requests/s; 0 = closed loop
    int connect_retries = 250;
};

struct Shared {
    Config config;
    std::atomic<bool> started{false};
    std::int64_t go_ns = 0;  // published by `started`
    std::int64_t measure_from = 0;
    std::int64_t measure_until = 0;
    std::atomic<std::size_t> connected{0};
    std::atomic<std::size_t> failed{0};
    std::atomic<std::size_t> done{0};
    std::atomic<std::uint64_t> errors{0};
    std::atomic<std::uint64_t> counted{0};
    std::unique_ptr<Histogram[]> histograms = std::make_unique<Histogram[]>(kShards);

    void record(std::size_t connection, std::int64_t completed, std::int64_t latency) noexcept {
        if (completed >= measure_from && completed < measure_until) {
            histograms[connection % kShards].record(latency);
        }
    }
};

// Waits (sleeping) until main publishes the start time.
void wait_for_start(Shared& shared) {
    while (!shared.started.load(std::memory_order_acquire)) {
        this_fiber::sleep_for(std::chrono::milliseconds(1));
    }
}

bool connect_with_retries(Shared& shared, Scheduler& scheduler, net::Stream& s) {
    for (int attempt = 0;; ++attempt) {
        const int error = s.connect(scheduler, shared.config.endpoint);
        if (error == 0) {
            static_cast<void>(s.set_nodelay());
            // Close with RST: no TIME_WAIT pile-up across runs of 10k connections.
            const linger l{1, 0};
            static_cast<void>(s.set_option(SOL_SOCKET, SO_LINGER, &l, sizeof l));
            return true;
        }
        if (attempt >= shared.config.connect_retries || (error != ECONNREFUSED && error != EAGAIN &&
                                                         error != ETIMEDOUT && error != ECONNRESET)) {
            std::fprintf(stderr, "connect: %s\n", std::strerror(error));
            return false;
        }
        this_fiber::sleep_for(std::chrono::milliseconds(20));
    }
}

void closed_loop(Shared& shared, net::Stream& s, std::size_t index) {
    const Config& c = shared.config;
    char out[kMaxSize];
    char in[kMaxSize];
    std::memset(out, 'a' + static_cast<int>(index % 26), c.size);
    std::int64_t sent[kMaxPipeline];
    for (std::size_t i = 0; i < c.pipeline; ++i) {
        sent[i] = now_ns();
        if (!s.write_all(out, c.size)) {
            shared.errors.fetch_add(1, std::memory_order_relaxed);
            return;
        }
    }
    for (std::uint64_t k = 0;; ++k) {
        const net::IoResult r = s.read_exact(in, c.size);
        const std::int64_t t = now_ns();
        if (!r || r.bytes != c.size) {
            shared.errors.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        const std::size_t slot = k % c.pipeline;
        shared.record(index, t, t - sent[slot]);
        if (t >= shared.measure_until) {
            return;
        }
        sent[slot] = now_ns();
        if (!s.write_all(out, c.size)) {
            shared.errors.fetch_add(1, std::memory_order_relaxed);
            return;
        }
    }
}

struct OpenLoopConnection {
    Shared* shared;
    net::Stream* stream;
    std::size_t index;
    std::int64_t first;     // intended send time of request 0
    std::int64_t interval;  // ns between intended send times
    std::atomic<bool> writer_done{false};
};

void open_loop_writer(OpenLoopConnection& conn) {
    Shared& shared = *conn.shared;
    char out[kMaxSize];
    std::memset(out, 'a' + static_cast<int>(conn.index % 26), shared.config.size);
    for (std::uint64_t k = 0;; ++k) {
        const std::int64_t intended = conn.first + static_cast<std::int64_t>(k) * conn.interval;
        if (intended >= shared.measure_until) {
            break;
        }
        const std::int64_t now = now_ns();
        if (now < intended) {
            this_fiber::sleep_for(std::chrono::nanoseconds(intended - now));
        }
        if (!conn.stream->write_all(out, shared.config.size)) {
            shared.errors.fetch_add(1, std::memory_order_relaxed);
            break;
        }
    }
    // The server echoes what is left, sees end of stream and closes: the
    // reader then reads to the end.
    static_cast<void>(conn.stream->shutdown(SHUT_WR));
}

void open_loop_reader(OpenLoopConnection& conn) {
    Shared& shared = *conn.shared;
    char in[kMaxSize];
    for (std::uint64_t k = 0;; ++k) {
        const net::IoResult r = conn.stream->read_exact(in, shared.config.size);
        const std::int64_t t = now_ns();
        if (!r) {
            shared.errors.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        if (r.bytes != shared.config.size) {
            return;  // end of stream
        }
        const std::int64_t intended = conn.first + static_cast<std::int64_t>(k) * conn.interval;
        shared.record(conn.index, t, t - intended);
    }
}

double cpu_seconds() {
    rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    return static_cast<double>(usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) +
           static_cast<double>(usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) / 1e6;
}

void sleep_until_ns(std::int64_t t) {
    const std::int64_t now = now_ns();
    if (t > now) {
        std::this_thread::sleep_for(std::chrono::nanoseconds(t - now));
    }
}

}  // namespace

int main(int argc, char** argv) {
    examples::Args args(argc, argv);
    examples::raise_fd_limit();
    auto shared = std::make_unique<Shared>();
    Config& c = shared->config;
    const auto port = static_cast<std::uint16_t>(args.integer("port", 7000));
    if (!net::Endpoint::parse(args.string("host", "127.0.0.1"), port, c.endpoint)) {
        std::fprintf(stderr, "bad --host\n");
        return 2;
    }
    c.connections = static_cast<std::size_t>(std::max(1LL, args.integer("connections", 1)));
    c.pipeline = std::clamp<std::size_t>(static_cast<std::size_t>(args.integer("pipeline", 1)), 1, kMaxPipeline);
    c.size = std::clamp<std::size_t>(static_cast<std::size_t>(args.integer("size", 64)), 1, kMaxSize);
    c.rate = args.real("rate", 0);
    c.connect_retries = static_cast<int>(args.integer("connect-retries", 250));
    const double duration = args.real("duration", 10);
    const double warmup = args.real("warmup", 2);
    const bool open_loop = c.rate > 0;

    fibers::sched::SchedulerOptions options;
    options.pool.num_workers = static_cast<std::size_t>(args.integer("workers", 4));
    options.pool.cpus = args.cpus("worker-cpus");
    if (const long long spins = args.integer("idle-spins", -1); spins >= 0) {
        options.pool.idle_spins_before_park = static_cast<std::size_t>(spins);  // see ThreadPoolOptions
    }
    options.pollers.num_pollers = static_cast<std::size_t>(args.integer("pollers", 1));
    options.pollers.cpus = args.cpus("poller-cpus");
    options.blocking.num_threads = 1;
    options.blocking.cpus = args.cpus("worker-cpus");
    options.stacks.max_stacks = 2 * c.connections + 64;
    Scheduler scheduler(options);

    // Open loop: per-connection schedule.
    std::unique_ptr<OpenLoopConnection[]> open(open_loop ? new OpenLoopConnection[c.connections] : nullptr);
    auto streams = std::make_unique<net::Stream[]>(c.connections);

    // Connect in waves (the server's accept backlog is finite).
    constexpr std::size_t kWave = 512;
    for (std::size_t base = 0; base < c.connections; base += kWave) {
        const std::size_t end = std::min(c.connections, base + kWave);
        for (std::size_t i = base; i < end; ++i) {
            fiber::go(scheduler, [&shared, &scheduler, &streams, &open, open_loop, i]() noexcept {
                Shared& sh = *shared;
                net::Stream& s = streams[i];
                if (!connect_with_retries(sh, scheduler, s)) {
                    sh.failed.fetch_add(1);
                    sh.done.fetch_add(1);
                    return;
                }
                sh.connected.fetch_add(1);
                wait_for_start(sh);
                if (!open_loop) {
                    closed_loop(sh, s, i);
                } else {
                    OpenLoopConnection& conn = open[i];
                    fiber::go<Scheduler>([&conn]() noexcept {
                        open_loop_writer(conn);
                        conn.writer_done.store(true, std::memory_order_release);
                    });
                    open_loop_reader(conn);
                    while (!conn.writer_done.load(std::memory_order_acquire)) {
                        this_fiber::sleep_for(std::chrono::milliseconds(1));
                    }
                }
                static_cast<void>(s.close());
                sh.done.fetch_add(1);
            });
        }
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (shared->connected.load() + shared->failed.load() < end) {
            if (std::chrono::steady_clock::now() > deadline) {
                std::fprintf(stderr, "echo_client: connecting timed out\n");
                std::_Exit(1);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    if (shared->failed.load() != 0) {
        std::fprintf(stderr, "echo_client: %zu connections failed\n", shared->failed.load());
        std::_Exit(1);  // fibers are still waiting for the start: no orderly shutdown
    }

    const std::int64_t go = now_ns() + 5'000'000;
    shared->go_ns = go;
    shared->measure_from = go + static_cast<std::int64_t>(warmup * 1e9);
    shared->measure_until = shared->measure_from + static_cast<std::int64_t>(duration * 1e9);
    if (open_loop) {
        const double per_connection = c.rate / static_cast<double>(c.connections);
        const auto interval = static_cast<std::int64_t>(1e9 / per_connection);
        for (std::size_t i = 0; i < c.connections; ++i) {
            open[i].shared = shared.get();
            open[i].stream = &streams[i];
            open[i].index = i;
            open[i].interval = interval;
            open[i].first = go + static_cast<std::int64_t>(static_cast<double>(interval) *
                                                           static_cast<double>(i) /
                                                           static_cast<double>(c.connections));
        }
    }
    shared->started.store(true, std::memory_order_release);

    sleep_until_ns(shared->measure_from);
    const double cpu_from = cpu_seconds();
    sleep_until_ns(shared->measure_until);
    const double cpu_until = cpu_seconds();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (shared->done.load() < c.connections) {
        if (std::chrono::steady_clock::now() > deadline) {
            std::fprintf(stderr, "echo_client: %zu connections did not finish\n",
                         c.connections - shared->done.load());
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    std::vector<std::uint64_t> counts(Histogram::kBuckets, 0);
    for (std::size_t i = 0; i < kShards; ++i) {
        shared->histograms[i].merge_into(counts.data());
    }
    std::uint64_t total = 0;
    for (const std::uint64_t n : counts) {
        total += n;
    }
    const double rps = static_cast<double>(total) / duration;
    auto q = [&](double quantile) { return Histogram::quantile(counts.data(), total, quantile) / 1000.0; };
    const double cpu = (cpu_until - cpu_from) / duration;
    std::printf("%s loop: %zu connections, pipeline %zu, %zu-byte messages%s\n",
                open_loop ? "open" : "closed", c.connections, open_loop ? 0 : c.pipeline, c.size,
                open_loop ? "" : "");
    if (open_loop) {
        std::printf("  target rate %.0f req/s\n", c.rate);
    }
    std::printf("  %.0f req/s, latency us: p50 %.1f  p90 %.1f  p99 %.1f  p99.9 %.1f  max %.1f\n", rps, q(0.5),
                q(0.9), q(0.99), q(0.999), Histogram::max_value(counts.data()) / 1000.0);
    std::printf("  client cpu %.2f cores, errors %llu\n", cpu,
                static_cast<unsigned long long>(shared->errors.load()));
    std::printf("RESULT mode=%s conns=%zu pipeline=%zu size=%zu rate=%.0f rps=%.0f p50_us=%.1f p90_us=%.1f "
                "p99_us=%.1f p999_us=%.1f max_us=%.1f cpu=%.2f errors=%llu\n",
                open_loop ? "open" : "closed", c.connections, open_loop ? 0 : c.pipeline, c.size, c.rate, rps,
                q(0.5), q(0.9), q(0.99), q(0.999), Histogram::max_value(counts.data()) / 1000.0, cpu,
                static_cast<unsigned long long>(shared->errors.load()));
    std::fflush(stdout);
    // Close what may still be open, then stop (streams before the scheduler).
    for (std::size_t i = 0; i < c.connections; ++i) {
        static_cast<void>(streams[i].close());
    }
    scheduler.stop();
    return 0;
}
