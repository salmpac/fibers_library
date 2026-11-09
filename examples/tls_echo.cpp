// TLS echo server and client on fibers (fibers::tls over net::Stream).
//
//   tls_echo --server --port=7443 --cert=server.pem --key=server.key
//            [--client-ca=ca.pem --require-client-cert] [--alpn=h2,http/1.1]
//            [--workers=2 --pollers=1 --stack-kb=64 --max-connections=10000]
//   tls_echo --client --host=127.0.0.1 --port=7443 --ca=ca.pem
//            [--server-name=localhost] [--cert=client.pem --key=client.key]
//            [--alpn=http/1.1] [--connections=16 --messages=1000 --size=64]
//            [--workers=2]
//
// The server runs until SIGINT / SIGTERM: one fiber per connection
// (handshake, then read_some -> write_all until close_notify), handshake
// failures are logged with OpenSSL's reason. The client opens
// `connections` connections at once, each does a handshake and `messages`
// request / echo round trips of `size` bytes, then closes cleanly
// (close_notify); it prints handshake and round-trip latency percentiles.
//
// Certificates: the build generates a test PKI next to this binary
// (tls_certs/: ca.pem, server.pem / .key with SANs localhost and
// 127.0.0.1, client.pem / .key), or make your own with the openssl CLI
// (see tests/tls/gen_certs.sh).

#include <signal.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "common.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/sync/spin_lock.hpp"
#include "fibers/tls/context.hpp"
#include "fibers/tls/stream.hpp"

namespace fiber = fibers::fiber;
namespace net = fibers::net;
namespace tls = fibers::tls;
using examples::Histogram;
using examples::now_ns;
using fibers::sched::Scheduler;

namespace {

// "h2,http/1.1" -> views into the argument (at most 8).
std::size_t split_alpn(const char* list, std::string_view (&out)[8]) {
    std::size_t n = 0;
    std::string_view rest = list != nullptr ? list : "";
    while (!rest.empty() && n < 8) {
        const std::size_t comma = rest.find(',');
        out[n++] = rest.substr(0, comma);
        rest = comma == std::string_view::npos ? std::string_view() : rest.substr(comma + 1);
    }
    return n;
}

fibers::sched::SchedulerOptions scheduler_options(const examples::Args& args, std::size_t max_fibers) {
    fibers::sched::SchedulerOptions o;
    o.pool.num_workers = static_cast<std::size_t>(args.integer("workers", 2));
    o.pool.cpus = args.cpus("worker-cpus");
    o.pollers.num_pollers = static_cast<std::size_t>(args.integer("pollers", 1));
    o.pollers.cpus = args.cpus("poller-cpus");
    o.blocking.num_threads = 1;
    o.stacks.stack_size = static_cast<std::size_t>(args.integer("stack-kb", 64)) * 1024;
    o.stacks.max_stacks = max_fibers + 16;
    return o;
}

// ---- server -----------------------------------------------------------------------

struct Server {
    explicit Server(std::size_t max_connections)
        : capacity(max_connections),
          streams(std::make_unique<tls::TlsStream[]>(max_connections)),
          free_slots(std::make_unique<std::size_t[]>(max_connections)),
          free_count(max_connections) {
        for (std::size_t i = 0; i < capacity; ++i) {
            free_slots[i] = capacity - 1 - i;
        }
    }

    tls::TlsStream* acquire() {
        std::lock_guard guard(lock);
        return free_count == 0 ? nullptr : &streams[free_slots[--free_count]];
    }

    void release(tls::TlsStream* s) {
        std::lock_guard guard(lock);
        free_slots[free_count++] = static_cast<std::size_t>(s - streams.get());
    }

    const std::size_t capacity;
    std::unique_ptr<tls::TlsStream[]> streams;
    std::unique_ptr<std::size_t[]> free_slots;
    std::size_t free_count;
    fibers::sync::SpinLock<> lock;
    std::atomic<std::size_t> active{0};
    std::atomic<std::uint64_t> accepted{0};
    std::atomic<std::uint64_t> failed_handshakes{0};
};

void serve(Server& server, tls::TlsStream& s) {
    static_cast<void>(s.set_nodelay());
    if (const int e = s.handshake(); e != 0) {
        char reason[256];
        std::fprintf(stderr, "tls_echo: handshake failed: %s: %s\n", std::strerror(e),
                     s.error_string(reason, sizeof reason));
        server.failed_handshakes.fetch_add(1, std::memory_order_relaxed);
        static_cast<void>(s.close());
        return;
    }
    char buf[16384];  // on the fiber's stack: 64 KiB leaves ~48 KiB for TLS (needs ~11)
    for (;;) {
        const net::IoResult r = s.read_some(buf, sizeof buf);
        if (!r || r.bytes == 0) {
            break;  // error, or the client's close_notify
        }
        if (!s.write_all(buf, r.bytes)) {
            break;
        }
    }
    static_cast<void>(s.shutdown());
    static_cast<void>(s.close());
}

int run_server(const examples::Args& args) {
    examples::raise_fd_limit();
    const std::size_t max_connections = static_cast<std::size_t>(args.integer("max-connections", 10000));
    std::string_view protos[8];
    const std::size_t nprotos = split_alpn(args.get("alpn"), protos);
    tls::ServerOptions so;
    so.cert_chain_file = args.string("cert", "tls_certs/server.pem");
    so.key_file = args.string("key", "tls_certs/server.key");
    so.client_ca_file = args.get("client-ca");
    if (so.client_ca_file != nullptr) {
        so.verify_client = args.get("require-client-cert") != nullptr ? tls::VerifyClient::kRequired
                                                                       : tls::VerifyClient::kOptional;
    }
    so.alpn = std::span<const std::string_view>(protos, nprotos);
    tls::TlsContext ctx;
    if (const int e = ctx.init_server(so); e != 0) {
        char reason[256];
        std::fprintf(stderr, "tls_echo: server context: %s: %s\n", std::strerror(e),
                     ctx.error_string(reason, sizeof reason));
        return 1;
    }

    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &signals, nullptr);

    auto server = std::make_unique<Server>(max_connections);
    const fibers::sched::SchedulerOptions options = scheduler_options(args, max_connections);
    Scheduler scheduler(options);
    net::Listener listener;
    net::Endpoint endpoint;
    const auto port = static_cast<std::uint16_t>(args.integer("port", 7443));
    if (!net::Endpoint::parse(args.string("host", "0.0.0.0"), port, endpoint)) {
        std::fprintf(stderr, "bad --host\n");
        return 2;
    }
    if (const int e = listener.listen(scheduler, endpoint); e != 0) {
        std::fprintf(stderr, "listen: %s\n", std::strerror(e));
        return 1;
    }
    std::printf("tls_echo server: port %u, %zu workers\n", listener.local_endpoint().port(),
                options.pool.num_workers);
    std::fflush(stdout);

    std::atomic<bool> accept_done{false};
    fiber::go(scheduler, [&]() noexcept {
        Server& srv = *server;
        for (;;) {
            tls::TlsStream* s = srv.acquire();
            if (s == nullptr) {
                fiber::this_fiber::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            const int e = s->accept(listener, ctx);
            if (e != 0) {
                srv.release(s);
                if (e == ECANCELED || e == EBADF) {
                    break;  // the listener was closed
                }
                fiber::this_fiber::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            srv.accepted.fetch_add(1, std::memory_order_relaxed);
            srv.active.fetch_add(1, std::memory_order_relaxed);
            fiber::go<Scheduler>([&srv, s]() noexcept {
                serve(srv, *s);
                srv.release(s);
                srv.active.fetch_sub(1, std::memory_order_release);
            });
        }
        accept_done.store(true, std::memory_order_release);
    });

    int signal_number = 0;
    sigwait(&signals, &signal_number);
    std::printf("tls_echo server: signal %d, %llu accepted, %llu failed handshakes\n", signal_number,
                static_cast<unsigned long long>(server->accepted.load()),
                static_cast<unsigned long long>(server->failed_handshakes.load()));
    static_cast<void>(listener.close());
    while (!accept_done.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    for (std::size_t i = 0; i < server->capacity; ++i) {
        static_cast<void>(server->streams[i].close());
    }
    for (int i = 0; i < 5000 && server->active.load(std::memory_order_acquire) != 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    scheduler.stop();
    return 0;
}

// ---- client -----------------------------------------------------------------------

int run_client(const examples::Args& args) {
    examples::raise_fd_limit();
    const auto connections = static_cast<std::size_t>(args.integer("connections", 16));
    const auto messages = static_cast<std::size_t>(args.integer("messages", 1000));
    const auto size = std::clamp<std::size_t>(static_cast<std::size_t>(args.integer("size", 64)), 1, 16384);
    std::string_view protos[8];
    const std::size_t nprotos = split_alpn(args.get("alpn"), protos);
    tls::ClientOptions co;
    co.ca_file = args.string("ca", "tls_certs/ca.pem");
    co.cert_chain_file = args.get("cert");
    co.key_file = args.get("key");
    co.alpn = std::span<const std::string_view>(protos, nprotos);
    tls::TlsContext ctx;
    if (const int e = ctx.init_client(co); e != 0) {
        char reason[256];
        std::fprintf(stderr, "tls_echo: client context: %s: %s\n", std::strerror(e),
                     ctx.error_string(reason, sizeof reason));
        return 1;
    }
    net::Endpoint endpoint;
    const char* host = args.string("host", "127.0.0.1");
    if (!net::Endpoint::parse(host, static_cast<std::uint16_t>(args.integer("port", 7443)), endpoint)) {
        std::fprintf(stderr, "bad --host (numeric address)\n");
        return 2;
    }
    const char* server_name = args.string("server-name", host);

    auto handshakes = std::make_unique<Histogram>();
    auto round_trips = std::make_unique<Histogram>();
    std::atomic<std::size_t> remaining{connections};
    std::atomic<std::uint64_t> errors{0};
    std::atomic<bool> printed{false};
    const std::int64_t start = now_ns();
    Scheduler scheduler(scheduler_options(args, connections));
    for (std::size_t c = 0; c < connections; ++c) {
        fiber::go(scheduler, [&]() noexcept {
            tls::TlsStream s;
            const std::int64_t t0 = now_ns();
            if (const int e = s.connect(scheduler, endpoint, ctx, server_name); e != 0) {
                if (!printed.exchange(true)) {
                    char reason[256];
                    std::fprintf(stderr, "tls_echo: connect / handshake: %s: %s\n", std::strerror(e),
                                 s.error_string(reason, sizeof reason));
                }
                errors.fetch_add(1);
            } else {
                handshakes->record(now_ns() - t0);
                if (!printed.exchange(true)) {
                    char subject[256];
                    std::printf("tls_echo client: %s %s, alpn \"%.*s\", server %s\n", s.version(), s.cipher(),
                                static_cast<int>(s.alpn().size()), s.alpn().data(),
                                s.peer_subject(subject, sizeof subject));
                }
                static_cast<void>(s.set_nodelay());
                char out[16384];
                char in[16384];
                std::memset(out, 'x', size);
                for (std::size_t m = 0; m < messages; ++m) {
                    const std::int64_t t = now_ns();
                    if (!s.write_all(out, size) || s.read_exact(in, size).bytes != size) {
                        errors.fetch_add(1);
                        break;
                    }
                    round_trips->record(now_ns() - t);
                }
                static_cast<void>(s.shutdown());
                char c1;
                static_cast<void>(s.read_some(&c1, 1));  // the server's close_notify
                static_cast<void>(s.close());
            }
            remaining.fetch_sub(1, std::memory_order_release);
        });
    }
    while (remaining.load(std::memory_order_acquire) != 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const double seconds = static_cast<double>(now_ns() - start) / 1e9;
    scheduler.stop();

    std::vector<std::uint64_t> hs(Histogram::kBuckets);
    std::vector<std::uint64_t> rt(Histogram::kBuckets);
    handshakes->merge_into(hs.data());
    round_trips->merge_into(rt.data());
    std::uint64_t nhs = 0;
    std::uint64_t nrt = 0;
    for (std::size_t i = 0; i < Histogram::kBuckets; ++i) {
        nhs += hs[i];
        nrt += rt[i];
    }
    std::printf("tls_echo client: %zu connections x %zu messages of %zu bytes in %.3f s, %llu errors\n",
                connections, messages, size, seconds, static_cast<unsigned long long>(errors.load()));
    std::printf("  handshake (connect + TLS)  p50 %.0f us  p99 %.0f us\n",
                Histogram::quantile(hs.data(), nhs, 0.5) / 1e3, Histogram::quantile(hs.data(), nhs, 0.99) / 1e3);
    std::printf("  round trip                 p50 %.1f us  p99 %.1f us  max %.1f us  (%.0f round trips/s)\n",
                Histogram::quantile(rt.data(), nrt, 0.5) / 1e3, Histogram::quantile(rt.data(), nrt, 0.99) / 1e3,
                Histogram::max_value(rt.data()) / 1e3, static_cast<double>(nrt) / seconds);
    return errors.load() == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
    examples::Args args(argc, argv);
    if (args.get("server") != nullptr) {
        return run_server(args);
    }
    if (args.get("client") != nullptr) {
        return run_client(args);
    }
    std::fprintf(stderr, "usage: tls_echo --server [options] | --client [options] (see the source)\n");
    return 2;
}
