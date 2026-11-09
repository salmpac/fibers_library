// TLS benchmarks: handshakes and echo traffic over TlsStream vs a plain
// net::Stream (StdSync, epoll pollers, loopback / socketpair). Certificates
// come from tests/tls/gen_certs.sh, generated at build time.
//
//   Handshake/K/W/C      full TLS 1.3 handshakes (default groups, i.e.
//                        X25519MLKEM768 on OpenSSL 3.5) over socketpairs,
//                        K = 0: EC P-256 server key, 1: RSA-2048; W workers,
//                        C handshakes in flight (client + server fiber each).
//                        items/s = handshakes (both sides on this machine).
//   Throughput<S>/N      one loopback TCP connection, the client writes N
//                        bytes per write_all, the server reads them;
//                        bytes/s. S = plain net::Stream or TlsStream.
//   PingPong<S>/N        one connection, N-byte request / N-byte echo, one
//                        at a time; per_op = round-trip time.
//
// Run pinned to CPUs 0-9: `taskset -c 0-9 tls_bench`; cpu_min / cpu_max are
// the CPUs fibers were seen on (sampled), cpu_main the benchmark thread's.

#include <benchmark/benchmark.h>

#include <sched.h>
#include <sys/socket.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "fibers/fiber/fiber.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/tls/context.hpp"
#include "fibers/tls/stream.hpp"

namespace fiber = fibers::fiber;
namespace net = fibers::net;
namespace sched = fibers::sched;
namespace tls = fibers::tls;
using fibers::sched::Scheduler;

namespace {

[[gnu::noinline]] int current_cpu() {
    asm volatile("" ::: "memory");
    return sched_getcpu();
}

struct CpuRange {
    std::atomic<int> min{1 << 20};
    std::atomic<int> max{-1};

    void sample() noexcept {
        const int cpu = current_cpu();
        int m = min.load(std::memory_order_relaxed);
        while (cpu < m && !min.compare_exchange_weak(m, cpu, std::memory_order_relaxed)) {
        }
        m = max.load(std::memory_order_relaxed);
        while (cpu > m && !max.compare_exchange_weak(m, cpu, std::memory_order_relaxed)) {
        }
    }

    void report(benchmark::State& state) const {
        state.counters["cpu_min"] = min.load();
        state.counters["cpu_max"] = max.load();
        state.counters["cpu_main"] = current_cpu();
    }
};

class Latch {
public:
    void reset(std::int64_t count) noexcept { count_.store(count, std::memory_order_relaxed); }

    void count_down() noexcept {
        if (count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            count_.notify_all();
        }
    }

    void wait() noexcept {
        for (std::int64_t v = count_.load(std::memory_order_acquire); v != 0;
             v = count_.load(std::memory_order_acquire)) {
            count_.wait(v, std::memory_order_acquire);
        }
    }

private:
    std::atomic<std::int64_t> count_{0};
};

sched::SchedulerOptions options(std::size_t workers) {
    sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = 1;
    o.blocking.num_threads = 1;
    o.stacks.stack_size = 64 * 1024;  // the default: enough for TLS (see stream.hpp)
    o.stacks.max_stacks = 4096;
    return o;
}

std::string cert(const char* name) { return std::string(FIBERS_TLS_CERTS) + "/" + name; }

bool make_server(tls::TlsContext& ctx, bool rsa) {
    const std::string chain = cert(rsa ? "server_rsa.pem" : "server.pem");
    const std::string key = cert(rsa ? "server_rsa.key" : "server.key");
    tls::ServerOptions o;
    o.cert_chain_file = chain.c_str();
    o.key_file = key.c_str();
    return ctx.init_server(o) == 0;
}

bool make_client(tls::TlsContext& ctx) {
    const std::string ca = cert("ca.pem");
    tls::ClientOptions o;
    o.ca_file = ca.c_str();
    return ctx.init_client(o) == 0;
}

benchmark::Counter per_op(double ops) {
    return benchmark::Counter(ops, benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

// ---- handshakes -------------------------------------------------------------------

void Handshake(benchmark::State& state) {
    const bool rsa = state.range(0) == 1;
    const auto workers = static_cast<std::size_t>(state.range(1));
    const auto concurrency = static_cast<std::size_t>(state.range(2));
    tls::TlsContext sctx;
    tls::TlsContext cctx;
    if (!make_server(sctx, rsa) || !make_client(cctx)) {
        state.SkipWithError("certificates");
        return;
    }
    Scheduler scheduler(options(workers));
    auto servers = std::make_unique<tls::TlsStream[]>(concurrency);
    auto clients = std::make_unique<tls::TlsStream[]>(concurrency);
    Latch latch;
    CpuRange cpus;
    std::atomic<int> errors{0};
    std::int64_t handshakes = 0;
    for (auto _ : state) {
        latch.reset(static_cast<std::int64_t>(2 * concurrency));
        for (std::size_t i = 0; i < concurrency; ++i) {
            int fd[2];
            if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fd) != 0 ||
                servers[i].transport().adopt(scheduler, fd[0]) != 0 ||
                clients[i].transport().adopt(scheduler, fd[1]) != 0 || servers[i].upgrade_server(sctx) != 0 ||
                clients[i].upgrade_client(cctx, "localhost") != 0) {
                state.SkipWithError("setup");
                return;
            }
            for (tls::TlsStream* s : {&servers[i], &clients[i]}) {
                fiber::go(scheduler, [&, s]() noexcept {
                    if (s->handshake() != 0) {
                        errors.fetch_add(1, std::memory_order_relaxed);
                    }
                    cpus.sample();
                    latch.count_down();
                });
            }
        }
        latch.wait();
        // Closed once both sides are done: a side that closes right after
        // its own handshake can fail the peer's last flight (EPIPE).
        for (std::size_t i = 0; i < concurrency; ++i) {
            static_cast<void>(servers[i].close());
            static_cast<void>(clients[i].close());
        }
        handshakes += static_cast<std::int64_t>(concurrency);
    }
    state.SetItemsProcessed(handshakes);
    state.counters["errors"] = errors.load();
    cpus.report(state);
}

// Baseline: the same handshakes with OpenSSL alone, both SSL objects in one
// thread joined by a BIO pair (no sockets, no fibers): what Handshake/K/1/1
// costs beyond OpenSSL's own work.
void HandshakeOpenSslOnly(benchmark::State& state) {
    const bool rsa = state.range(0) == 1;
    tls::TlsContext sctx;
    tls::TlsContext cctx;
    if (!make_server(sctx, rsa) || !make_client(cctx)) {
        state.SkipWithError("certificates");
        return;
    }
    int errors = 0;
    for (auto _ : state) {
        SSL* c = SSL_new(cctx.native_handle());
        SSL* s = SSL_new(sctx.native_handle());
        BIO* cb = nullptr;
        BIO* sb = nullptr;
        BIO_new_bio_pair(&cb, 0, &sb, 0);
        SSL_set_bio(c, cb, cb);
        SSL_set_bio(s, sb, sb);
        SSL_set_connect_state(c);
        SSL_set_tlsext_host_name(c, "localhost");
        SSL_set1_host(c, "localhost");
        SSL_set_accept_state(s);
        bool c_done = false;
        bool s_done = false;
        for (int round = 0; round < 16 && !(c_done && s_done); ++round) {
            c_done = c_done || SSL_do_handshake(c) == 1;
            s_done = s_done || SSL_do_handshake(s) == 1;
        }
        errors += c_done && s_done ? 0 : 1;
        ERR_clear_error();
        SSL_free(c);
        SSL_free(s);
    }
    state.SetItemsProcessed(state.iterations());
    state.counters["errors"] = errors;
}

BENCHMARK(HandshakeOpenSslOnly)->ArgNames({"rsa"})->Arg(0)->Arg(1);

BENCHMARK(Handshake)
    ->ArgNames({"rsa", "workers", "inflight"})
    ->Args({0, 1, 1})
    ->Args({1, 1, 1})
    ->Args({0, 1, 16})
    ->Args({0, 4, 64})
    ->Args({1, 4, 64})
    ->UseRealTime();

// ---- echo traffic: plain vs TLS ------------------------------------------------------

struct PlainSide {
    net::Stream stream;
    net::Stream& io() { return stream; }
};

struct TlsSide {
    tls::TlsStream stream;
    tls::TlsStream& io() { return stream; }
};

// One loopback TCP connection between a server and a client side, set up
// (and for TLS, handshaken) inside fibers.
template <class Side>
struct Connection {
    Side server;
    Side client;
    tls::TlsContext sctx;
    tls::TlsContext cctx;

    bool open(Scheduler& scheduler) {
        constexpr bool kTls = std::is_same_v<Side, TlsSide>;
        if (kTls && (!make_server(sctx, false) || !make_client(cctx))) {
            return false;
        }
        net::Listener listener;
        if (listener.listen(scheduler, net::Endpoint::loopback_v4(0)) != 0) {
            return false;
        }
        const net::Endpoint ep = listener.local_endpoint();
        Latch latch;
        latch.reset(2);
        std::atomic<int> errors{0};
        fiber::go(scheduler, [&]() noexcept {
            int e = 0;
            if constexpr (kTls) {
                e = server.stream.accept(listener, sctx);
                if (e == 0) {
                    e = server.stream.handshake();
                }
            } else {
                e = listener.accept(server.stream);
            }
            errors.fetch_add(e != 0 ? 1 : 0);
            latch.count_down();
        });
        fiber::go(scheduler, [&]() noexcept {
            int e = 0;
            if constexpr (kTls) {
                e = client.stream.connect(scheduler, ep, cctx, "localhost");
            } else {
                e = client.stream.connect(scheduler, ep);
            }
            if (e == 0) {
                e = client.stream.set_nodelay();
            }
            errors.fetch_add(e != 0 ? 1 : 0);
            latch.count_down();
        });
        latch.wait();
        static_cast<void>(listener.close());
        if constexpr (!kTls) {
            static_cast<void>(server.stream.set_nodelay());
        }
        return errors.load() == 0;
    }
};

template <class Side>
void Throughput(benchmark::State& state) {
    const auto chunk = static_cast<std::size_t>(state.range(0));
    constexpr std::size_t kPerIteration = 64 << 20;
    Scheduler scheduler(options(2));
    auto conn = std::make_unique<Connection<Side>>();
    if (!conn->open(scheduler)) {
        state.SkipWithError("connect");
        return;
    }
    std::vector<char> out(chunk, 'x');
    std::vector<char> in(chunk);
    Latch latch;
    CpuRange cpus;
    std::atomic<int> errors{0};
    for (auto _ : state) {
        latch.reset(2);
        fiber::go(scheduler, [&]() noexcept {
            for (std::size_t sent = 0; sent < kPerIteration; sent += chunk) {
                if (!conn->client.io().write_all(out.data(), chunk)) {
                    errors.fetch_add(1);
                    break;
                }
            }
            cpus.sample();
            latch.count_down();
        });
        fiber::go(scheduler, [&]() noexcept {
            for (std::size_t got = 0; got < kPerIteration; got += chunk) {
                if (conn->server.io().read_exact(in.data(), chunk).bytes != chunk) {
                    errors.fetch_add(1);
                    break;
                }
            }
            cpus.sample();
            latch.count_down();
        });
        latch.wait();
    }
    state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations() * kPerIteration));
    state.counters["errors"] = errors.load();
    cpus.report(state);
    static_cast<void>(conn->client.io().close());
    static_cast<void>(conn->server.io().close());
}

template <class Side>
void PingPong(benchmark::State& state) {
    const auto size = static_cast<std::size_t>(state.range(0));
    constexpr std::size_t kRounds = 1000;
    Scheduler scheduler(options(2));
    auto conn = std::make_unique<Connection<Side>>();
    if (!conn->open(scheduler)) {
        state.SkipWithError("connect");
        return;
    }
    Latch latch;
    CpuRange cpus;
    std::atomic<int> errors{0};
    for (auto _ : state) {
        latch.reset(2);
        fiber::go(scheduler, [&]() noexcept {
            char buf[16384];
            for (std::size_t i = 0; i < kRounds; ++i) {
                if (conn->server.io().read_exact(buf, size).bytes != size ||
                    !conn->server.io().write_all(buf, size)) {
                    errors.fetch_add(1);
                    break;
                }
            }
            cpus.sample();
            latch.count_down();
        });
        fiber::go(scheduler, [&]() noexcept {
            char buf[16384] = {};
            for (std::size_t i = 0; i < kRounds; ++i) {
                if (!conn->client.io().write_all(buf, size) || conn->client.io().read_exact(buf, size).bytes != size) {
                    errors.fetch_add(1);
                    break;
                }
            }
            cpus.sample();
            latch.count_down();
        });
        latch.wait();
    }
    state.counters["per_op"] = per_op(static_cast<double>(state.iterations() * kRounds));
    state.counters["errors"] = errors.load();
    cpus.report(state);
    static_cast<void>(conn->client.io().close());
    static_cast<void>(conn->server.io().close());
}

BENCHMARK_TEMPLATE(Throughput, PlainSide)->Arg(1024)->Arg(16384)->Arg(262144)->UseRealTime();
BENCHMARK_TEMPLATE(Throughput, TlsSide)->Arg(1024)->Arg(16384)->Arg(262144)->UseRealTime();
BENCHMARK_TEMPLATE(PingPong, PlainSide)->Arg(64)->Arg(4096)->UseRealTime();
BENCHMARK_TEMPLATE(PingPong, TlsSide)->Arg(64)->Arg(4096)->UseRealTime();

}  // namespace
