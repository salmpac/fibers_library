// tls::Connection: plaintext -> TLS on the same socket (Postgres SSLRequest,
// pipelined STARTTLS with already_received), TLS from the first byte,
// per-operation timeouts during the handshake and afterwards, BufferedReader
// on top. Certificates: gen_certs.sh at build time (see CMakeLists.txt).
//
// Fibers never use gtest assertions themselves: they record into atomics
// that the test thread checks after a Latch.

#include <gtest/gtest.h>

#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <thread>

#include "fibers/fiber/fiber.hpp"
#include "fibers/http/stream.hpp"
#include "fibers/net/buffered_reader.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/tls/connection.hpp"
#include "fibers/tls/context.hpp"

namespace fiber = fibers::fiber;
namespace net = fibers::net;
namespace tls = fibers::tls;
using fibers::sched::Scheduler;
using net::IoResult;
using tls::Connection;
using tls::TlsContext;
using namespace std::chrono_literals;

static_assert(Connection::kHasTls);
static_assert(net::ReadStream<Connection>);
static_assert(fibers::http::ByteStream<Connection>);

namespace {

#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
constexpr bool kSanitized = true;
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
constexpr bool kSanitized = true;
#else
constexpr bool kSanitized = false;
#endif
#else
constexpr bool kSanitized = false;
#endif

constexpr std::size_t kStackSize = kSanitized ? 1024 * 1024 : 256 * 1024;

class Latch {
public:
    explicit Latch(std::int64_t count) : count_(count) {}

    void count_down() noexcept { count_.fetch_sub(1, std::memory_order_acq_rel); }

    bool wait(std::chrono::milliseconds timeout = 120s) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (count_.load(std::memory_order_acquire) != 0) {
            if (std::chrono::steady_clock::now() > deadline) {
                return false;
            }
            std::this_thread::sleep_for(100us);
        }
        return true;
    }

private:
    std::atomic<std::int64_t> count_;
};

fibers::sched::SchedulerOptions options(std::size_t workers) {
    fibers::sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = 2;
    o.blocking.num_threads = 1;
    o.stacks.stack_size = kStackSize;
    o.stacks.max_stacks = 64;
    return o;
}

std::string cert(const char* name) { return std::string(FIBERS_TLS_CERTS) + "/" + name; }

int make_server(TlsContext& ctx) {
    const std::string chain = cert("server.pem");
    const std::string key = cert("server.key");
    tls::ServerOptions o;
    o.cert_chain_file = chain.c_str();
    o.key_file = key.c_str();
    return ctx.init_server(o);
}

int make_client(TlsContext& ctx) {
    const std::string ca = cert("ca.pem");
    tls::ClientOptions o;
    o.ca_file = ca.c_str();
    return ctx.init_client(o);
}

struct Pair {
    int fd[2] = {-1, -1};
    Pair() {
        if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fd) != 0) {
            fd[0] = fd[1] = -1;
        }
    }
};

struct Contexts {
    TlsContext server;
    TlsContext client;
    bool ok = make_server(server) == 0 && make_client(client) == 0;
};

// Line echo over a BufferedReader: n lines, each echoed back. 0 or an error
// (negative: wrong data).
int echo_lines_client(Connection& c, int n) {
    std::byte storage[512];
    net::BufferedReader reader(c, storage);
    for (int i = 0; i < n; ++i) {
        const std::string line = "line " + std::to_string(i) + "\n";
        if (const IoResult w = c.write_all(line.data(), line.size()); !w) {
            return w.error;
        }
        const net::LineResult l = reader.read_line();
        if (!l) {
            return l.error;
        }
        if (l.line != line) {
            return -1;
        }
    }
    return 0;
}

// Until end of stream (close_notify).
int echo_lines_server(Connection& c, net::BufferedReader<Connection>& reader) {
    for (;;) {
        const net::LineResult l = reader.read_line();
        if (!l) {
            return l.error;
        }
        if (l.line.empty()) {
            return 0;
        }
        if (const IoResult w = c.write_all(l.line.data(), l.line.size()); !w) {
            return w.error;
        }
    }
}

// The Postgres flow: SSLRequest (8 bytes) in plaintext, 'S', then TLS on the
// same socket; both sides read the plaintext part through a BufferedReader,
// the client checks that nothing arrived behind the 'S' (CVE-2021-23222)
// and the server hands what it buffered (nothing here) to the upgrade.
TEST(TlsConnection, SslRequestUpgrade) {
    Contexts ctx;
    ASSERT_TRUE(ctx.ok);
    Scheduler scheduler(options(2));
    Pair pair;
    Connection server;
    Connection client;
    ASSERT_EQ(server.adopt(scheduler, pair.fd[0]), 0);
    ASSERT_EQ(client.adopt(scheduler, pair.fd[1]), 0);
    server.set_io_timeout(30s);
    client.set_io_timeout(30s);
    static constexpr unsigned char kSslRequest[8] = {0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f};
    Latch done(2);
    std::atomic<int> server_error{-100};
    std::atomic<int> client_error{-100};
    std::atomic<bool> server_tls{false};
    std::atomic<bool> client_tls_before{true};
    std::atomic<bool> client_tls{false};
    fiber::go(scheduler, [&]() noexcept {
        std::byte storage[1024];
        int e = 0;
        {
            net::BufferedReader plain(server, storage);
            unsigned char request[8];
            const IoResult r = plain.read_exact(request, sizeof request);
            e = r.error != 0 ? r.error : (std::memcmp(request, kSslRequest, 8) == 0 ? 0 : -1);
            if (e == 0) {
                e = server.write_all("S", 1).error;
            }
            if (e == 0) {
                e = server.start_tls_server(ctx.server, plain.take_buffered());
            }
        }
        server_tls.store(server.is_tls());
        if (e == 0) {
            net::BufferedReader reader(server, storage);
            e = echo_lines_server(server, reader);
        }
        server_error.store(e);
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        client_tls_before.store(client.is_tls());
        int e = client.write_all(kSslRequest, sizeof kSslRequest).error;
        std::byte storage[64];
        net::BufferedReader plain(client, storage);
        char answer = 0;
        if (e == 0) {
            const IoResult r = plain.read_exact(&answer, 1);
            e = r.error != 0 ? r.error : (answer == 'S' ? 0 : -1);
        }
        if (e == 0 && plain.buffered() != 0) {
            e = -2;  // injected plaintext: refuse
        }
        if (e == 0) {
            e = client.start_tls_client(ctx.client, "localhost");
        }
        client_tls.store(client.is_tls());
        if (e == 0) {
            e = echo_lines_client(client, 50);
        }
        if (e == 0) {
            e = client.shutdown();  // close_notify: the server reads end of stream
        }
        client_error.store(e);
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(server_error.load(), 0);
    EXPECT_EQ(client_error.load(), 0);
    EXPECT_FALSE(client_tls_before.load());
    EXPECT_TRUE(server_tls.load());
    EXPECT_TRUE(client_tls.load());
    EXPECT_EQ(client.start_tls_client(ctx.client, "localhost"), EALREADY);
}

// Pipelined STARTTLS: the client sends its ClientHello right behind the
// plaintext command; the server's BufferedReader reads past the line into
// it, and take_buffered() hands those bytes to start_tls_server.
TEST(TlsConnection, PipelinedStartTlsWithAlreadyReceived) {
    Contexts ctx;
    ASSERT_TRUE(ctx.ok);
    Scheduler scheduler(options(2));
    Pair pair;
    Connection server;
    Connection client;
    ASSERT_EQ(server.adopt(scheduler, pair.fd[0]), 0);
    ASSERT_EQ(client.adopt(scheduler, pair.fd[1]), 0);
    server.set_io_timeout(30s);
    client.set_io_timeout(30s);
    Latch done(2);
    std::atomic<int> server_error{-100};
    std::atomic<int> client_error{-100};
    std::atomic<std::size_t> handed_over{0};
    fiber::go(scheduler, [&]() noexcept {
        fiber::this_fiber::sleep_for(50ms);  // the ClientHello is queued behind the line
        std::byte storage[4096];
        int e = 0;
        {
            net::BufferedReader plain(server, storage);
            const net::LineResult l = plain.read_line();
            e = l.error != 0 ? l.error : (l.line == "STARTTLS\n" ? 0 : -1);
            if (e == 0) {
                const std::span<const std::byte> rest = plain.take_buffered();
                handed_over.store(rest.size());
                e = server.start_tls_server(ctx.server, rest);
            }
        }
        if (e == 0) {
            net::BufferedReader reader(server, storage);
            e = echo_lines_server(server, reader);
        }
        server_error.store(e);
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        int e = client.write_all("STARTTLS\n", 9).error;
        if (e == 0) {
            e = client.start_tls_client(ctx.client, "localhost");
        }
        if (e == 0) {
            e = echo_lines_client(client, 50);
        }
        if (e == 0) {
            e = client.shutdown();
        }
        client_error.store(e);
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(server_error.load(), 0);
    EXPECT_EQ(client_error.load(), 0);
    EXPECT_GT(handed_over.load(), 0u);  // the reader overshot into the ClientHello
}

// TLS from the first byte over TCP: connect_tls / accept + start_tls_server.
TEST(TlsConnection, TlsFromTheStart) {
    Contexts ctx;
    ASSERT_TRUE(ctx.ok);
    Scheduler scheduler(options(2));
    net::Listener listener;
    ASSERT_EQ(listener.listen(scheduler, net::Endpoint::loopback_v4(0)), 0);
    const net::Endpoint ep = listener.local_endpoint();
    Connection server;
    Connection client;
    client.set_io_timeout(30s);
    Latch done(2);
    std::atomic<int> server_error{-100};
    std::atomic<int> client_error{-100};
    std::atomic<bool> tls_both{false};
    fiber::go(scheduler, [&]() noexcept {
        int e = server.accept(listener);
        if (e == 0) {
            e = server.start_tls_server(ctx.server);
        }
        if (e == 0) {
            std::byte storage[512];
            net::BufferedReader reader(server, storage);
            e = echo_lines_server(server, reader);
        }
        if (e == 0) {
            e = server.shutdown();  // close_notify back
        }
        server_error.store(e);
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        int e = client.connect_tls(scheduler, ep, ctx.client, "localhost", scheduler.now() + 60s);
        tls_both.store(client.is_tls());
        if (e == 0) {
            e = echo_lines_client(client, 100);
        }
        if (e == 0) {
            e = client.shutdown();
        }
        if (e == 0) {
            char byte;
            const IoResult r = client.read_some(&byte, 1);  // the server's close_notify / FIN
            e = r.error != 0 && r.error != ECONNABORTED ? r.error : 0;
        }
        client_error.store(e);
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(server_error.load(), 0);
    EXPECT_EQ(client_error.load(), 0);
    EXPECT_TRUE(tls_both.load());
    EXPECT_STRNE(client.tls_stream().version(), "");
}

// The io timeout bounds the handshake (a server that never answers) and
// TLS reads afterwards (a silent peer); a failed start_tls closes the
// connection.
TEST(TlsConnection, IoTimeoutDuringAndAfterHandshake) {
    Contexts ctx;
    ASSERT_TRUE(ctx.ok);
    Scheduler scheduler(options(2));
    {
        Pair pair;
        Connection client;
        ASSERT_EQ(client.adopt(scheduler, pair.fd[1]), 0);
        client.set_io_timeout(150ms);
        Latch done(1);
        std::atomic<int> error{-100};
        std::atomic<bool> open_after{true};
        std::atomic<std::int64_t> waited_ms{0};
        fiber::go(scheduler, [&]() noexcept {
            const auto start = std::chrono::steady_clock::now();
            error.store(client.start_tls_client(ctx.client, "localhost"));  // nobody answers
            waited_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now() - start)
                                .count());
            open_after.store(client.is_open_approx());
            done.count_down();
        });
        ASSERT_TRUE(done.wait());
        EXPECT_EQ(error.load(), ETIMEDOUT);
        EXPECT_GE(waited_ms.load(), 140);
        EXPECT_FALSE(open_after.load());
        EXPECT_FALSE(client.is_tls());
        ::close(pair.fd[0]);
    }
    {
        Pair pair;
        Connection server;
        Connection client;
        ASSERT_EQ(server.adopt(scheduler, pair.fd[0]), 0);
        ASSERT_EQ(client.adopt(scheduler, pair.fd[1]), 0);
        client.set_io_timeout(150ms);
        Latch done(2);
        std::atomic<int> client_error{-100};
        std::atomic<int> read_error{-100};
        std::atomic<int> after{-100};
        std::atomic<bool> speak{false};
        fiber::go(scheduler, [&]() noexcept {
            if (server.start_tls_server(ctx.server) == 0) {
                while (!speak.load()) {
                    fiber::this_fiber::sleep_for(5ms);  // silent
                }
                static_cast<void>(server.write_all("x", 1));
                char byte;
                static_cast<void>(server.read_some(&byte, 1));  // until the client closes
            }
            done.count_down();
        });
        fiber::go(scheduler, [&]() noexcept {
            client_error.store(client.start_tls_client(ctx.client, "localhost"));
            char byte = 0;
            read_error.store(client.read_some(&byte, 1).error);  // silent server
            speak.store(true);
            const IoResult r = client.read_some(&byte, 1);  // TLS state intact after ETIMEDOUT
            after.store(r.error != 0 ? r.error : (byte == 'x' ? 0 : -1));
            static_cast<void>(client.close());
            done.count_down();
        });
        ASSERT_TRUE(done.wait());
        EXPECT_EQ(client_error.load(), 0);
        EXPECT_EQ(read_error.load(), ETIMEDOUT);
        EXPECT_EQ(after.load(), 0);
    }
}

// Certificate verification failure: EPROTO, the reason is available, the
// connection is closed.
TEST(TlsConnection, VerificationFailureCloses) {
    Contexts ctx;
    ASSERT_TRUE(ctx.ok);
    Scheduler scheduler(options(2));
    Pair pair;
    Connection server;
    Connection client;
    ASSERT_EQ(server.adopt(scheduler, pair.fd[0]), 0);
    ASSERT_EQ(client.adopt(scheduler, pair.fd[1]), 0);
    server.set_io_timeout(30s);
    client.set_io_timeout(30s);
    Latch done(2);
    std::atomic<int> client_error{-100};
    std::atomic<bool> open_after{true};
    std::string reason;
    fiber::go(scheduler, [&]() noexcept {
        static_cast<void>(server.start_tls_server(ctx.server));  // fails: the client aborts
        static_cast<void>(server.close());
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        client_error.store(client.start_tls_client(ctx.client, "wrong.example.com"));
        open_after.store(client.is_open_approx());
        char buf[256];
        reason = client.tls_stream().error_string(buf, sizeof buf);
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(client_error.load(), EPROTO);
    EXPECT_FALSE(open_after.load());
    EXPECT_NE(reason.find("verify"), std::string::npos) << reason;
}

}  // namespace
