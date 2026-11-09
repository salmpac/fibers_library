// tls::TlsStream / tls::TlsContext on a real Scheduler (StdSync, epoll
// pollers), with certificates generated at build time (gen_certs.sh).
//
// Fibers never use gtest assertions themselves: they record into atomics
// that the test thread checks after a Latch. Child processes (openssl
// s_client / s_server) are killed by their RAII owner, waits are bounded.

#include <gtest/gtest.h>

#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "fibers/fiber/fiber.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/tls/context.hpp"
#include "fibers/tls/stream.hpp"

extern char** environ;

namespace fiber = fibers::fiber;
namespace net = fibers::net;
namespace tls = fibers::tls;
using fibers::sched::Scheduler;
using net::Endpoint;
using net::IoResult;
using net::Listener;
using tls::TlsContext;
using tls::TlsStream;
using namespace std::chrono_literals;

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

// Sanitizers multiply frame sizes (OpenSSL itself is not instrumented, but
// our frames and the interceptors are).
constexpr std::size_t kStackSize = kSanitized ? 1024 * 1024 : 256 * 1024;

class Latch {
public:
    explicit Latch(std::int64_t count) : count_(count) {}

    void count_down() noexcept { count_.fetch_sub(1, std::memory_order_acq_rel); }

    bool wait(std::chrono::milliseconds timeout = 60s) {
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

template <class Pred>
bool eventually(Pred pred, std::chrono::milliseconds timeout = 30s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(50us);
    }
    return true;
}

fibers::sched::SchedulerOptions options(std::size_t workers, std::size_t pollers = 2,
                                        std::size_t stack_size = kStackSize) {
    fibers::sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = pollers;
    o.blocking.num_threads = 1;
    o.stacks.stack_size = stack_size;
    o.stacks.max_stacks = 2048;
    return o;
}

std::string cert(const char* name) { return std::string(FIBERS_TLS_CERTS) + "/" + name; }

bool exists(const std::string& path) { return ::access(path.c_str(), R_OK) == 0; }

struct ServerSpec {
    const char* cert = "server.pem";
    const char* key = "server.key";
    tls::VerifyClient verify_client = tls::VerifyClient::kNone;
    std::vector<std::string_view> alpn{};
    bool ignore_unexpected_eof = false;
    int max_version = 0;
};

struct ClientSpec {
    const char* ca = "ca.pem";
    const char* cert = nullptr;
    const char* key = nullptr;
    std::vector<std::string_view> alpn{};
    bool verify = true;
    bool ignore_unexpected_eof = false;
    int max_version = 0;
};

int make_server(TlsContext& ctx, const ServerSpec& spec = {}) {
    const std::string chain = cert(spec.cert);
    const std::string key = cert(spec.key);
    const std::string ca = cert("ca.pem");
    tls::ServerOptions o;
    o.cert_chain_file = chain.c_str();
    o.key_file = key.c_str();
    o.verify_client = spec.verify_client;
    o.client_ca_file = ca.c_str();
    o.alpn = spec.alpn;
    o.ignore_unexpected_eof = spec.ignore_unexpected_eof;
    o.max_version = spec.max_version;
    return ctx.init_server(o);
}

int make_client(TlsContext& ctx, const ClientSpec& spec = {}) {
    const std::string ca = cert(spec.ca);
    const std::string chain = spec.cert != nullptr ? cert(spec.cert) : std::string();
    const std::string key = spec.key != nullptr ? cert(spec.key) : std::string();
    tls::ClientOptions o;
    o.ca_file = ca.c_str();
    o.verify_peer = spec.verify;
    o.cert_chain_file = spec.cert != nullptr ? chain.c_str() : nullptr;
    o.key_file = spec.key != nullptr ? key.c_str() : nullptr;
    o.alpn = spec.alpn;
    o.ignore_unexpected_eof = spec.ignore_unexpected_eof;
    o.max_version = spec.max_version;
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

std::uint8_t pattern(std::size_t i, std::size_t seed) {
    return static_cast<std::uint8_t>((i * 131 + seed * 7 + (i >> 11)) & 0xFF);
}

// Echoes until end of stream or an error. Returns the error (0 at a clean
// end of stream).
int echo(TlsStream& s) {
    char buf[16384];
    for (;;) {
        const IoResult r = s.read_some(buf, sizeof buf);
        if (!r) {
            return r.error;
        }
        if (r.bytes == 0) {
            return 0;
        }
        if (const IoResult w = s.write_all(buf, r.bytes); !w) {
            return w.error;
        }
    }
}

// Sends messages of varying sizes, checks the echo. 0 or the first problem
// (an errno, or -1 for a data mismatch).
int client_rounds(TlsStream& s, std::size_t rounds, std::size_t seed, std::size_t max_size = 8192) {
    std::vector<std::uint8_t> out(max_size);
    std::vector<std::uint8_t> in(max_size);
    for (std::size_t round = 0; round < rounds; ++round) {
        const std::size_t size = (round * 997 + seed * 13) % out.size() + 1;
        for (std::size_t i = 0; i < size; ++i) {
            out[i] = pattern(i, seed + round);
        }
        if (const IoResult w = s.write_all(out.data(), size); !w) {
            return w.error;
        }
        const IoResult r = s.read_exact(in.data(), size);
        if (!r) {
            return r.error;
        }
        if (r.bytes != size || std::memcmp(in.data(), out.data(), size) != 0) {
            return -1;
        }
    }
    return 0;
}

// A connected TLS pair over a socketpair: `server` and `client` adopted and
// upgraded (no handshake yet).
struct Duo {
    Pair pair;
    int setup(Scheduler& scheduler, TlsStream& server, TlsContext& sctx, TlsStream& client, TlsContext& cctx,
              const char* name = "localhost") {
        if (pair.fd[0] < 0) {
            return errno;
        }
        if (int e = server.transport().adopt(scheduler, pair.fd[0]); e != 0) {
            return e;
        }
        if (int e = client.transport().adopt(scheduler, pair.fd[1]); e != 0) {
            return e;
        }
        if (int e = server.upgrade_server(sctx); e != 0) {
            return e;
        }
        return client.upgrade_client(cctx, name);
    }
};

// Handshake on both sides of a Duo, each in its own fiber; the results.
struct HandshakeResult {
    int server = -1;
    int client = -1;
};

HandshakeResult handshake_both(Scheduler& scheduler, TlsStream& server, TlsStream& client) {
    Latch done(2);
    std::atomic<int> se{-1};
    std::atomic<int> ce{-1};
    fiber::go(scheduler, [&]() noexcept {
        se.store(server.handshake());
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        ce.store(client.handshake());
        done.count_down();
    });
    if (!done.wait()) {
        return HandshakeResult{-2, -2};
    }
    return HandshakeResult{se.load(), ce.load()};
}

// ---- child processes (openssl CLI) -------------------------------------------

class Child {
public:
    Child() = default;
    Child(const Child&) = delete;
    Child& operator=(const Child&) = delete;

    ~Child() { stop(); }

    // stdin / stdout as pipes; stderr to /dev/null.
    bool spawn(const std::vector<std::string>& args) {
        int in[2];
        int out[2];
        if (::pipe2(in, O_CLOEXEC) != 0 || ::pipe2(out, O_CLOEXEC) != 0) {
            return false;
        }
        posix_spawn_file_actions_t actions;
        posix_spawn_file_actions_init(&actions);
        posix_spawn_file_actions_adddup2(&actions, in[0], 0);
        posix_spawn_file_actions_adddup2(&actions, out[1], 1);
        posix_spawn_file_actions_addopen(&actions, 2, "/dev/null", O_WRONLY, 0);
        std::vector<char*> argv;
        for (const std::string& a : args) {
            argv.push_back(const_cast<char*>(a.c_str()));
        }
        argv.push_back(nullptr);
        const int r = ::posix_spawn(&pid_, args[0].c_str(), &actions, nullptr, argv.data(), environ);
        posix_spawn_file_actions_destroy(&actions);
        ::close(in[0]);
        ::close(out[1]);
        in_ = in[1];
        out_ = out[0];
        if (r != 0) {
            pid_ = -1;
            return false;
        }
        return true;
    }

    bool write(std::string_view data) {
        return ::write(in_, data.data(), data.size()) == static_cast<ssize_t>(data.size());
    }

    // Reads stdout until `needle` shows up (true) or the timeout / EOF.
    bool read_until(std::string_view needle, std::chrono::milliseconds timeout = 20s) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (output_.find(needle) == std::string::npos) {
            const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - std::chrono::steady_clock::now());
            if (left.count() <= 0) {
                return false;
            }
            pollfd p{out_, POLLIN, 0};
            if (::poll(&p, 1, static_cast<int>(left.count())) <= 0) {
                return false;
            }
            char buf[4096];
            const ssize_t n = ::read(out_, buf, sizeof buf);
            if (n <= 0) {
                return false;
            }
            output_.append(buf, static_cast<std::size_t>(n));
        }
        return true;
    }

    const std::string& output() const { return output_; }

    void stop() {
        if (in_ >= 0) {
            ::close(in_);
            in_ = -1;
        }
        if (pid_ > 0) {
            ::kill(pid_, SIGKILL);
            int status = 0;
            ::waitpid(pid_, &status, 0);
            pid_ = -1;
        }
        if (out_ >= 0) {
            ::close(out_);
            out_ = -1;
        }
    }

private:
    pid_t pid_ = -1;
    int in_ = -1;
    int out_ = -1;
    std::string output_;
};

std::uint16_t free_port() {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    Endpoint e = Endpoint::loopback_v4(0);
    if (::bind(fd, e.address(), e.size) != 0) {
        ::close(fd);
        return 0;
    }
    sockaddr_in in{};
    socklen_t len = sizeof in;
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&in), &len);
    ::close(fd);
    return ntohs(in.sin_port);
}

// ---- tests ---------------------------------------------------------------------

TEST(Tls, ContextErrors) {
    TlsContext missing;
    tls::ServerOptions so;
    so.cert_chain_file = "/nonexistent/server.pem";
    so.key_file = "/nonexistent/server.key";
    EXPECT_EQ(missing.init_server(so), ENOENT);
    char buf[256];
    EXPECT_NE(std::strlen(missing.error_string(buf, sizeof buf)), 0u);
    EXPECT_NE(missing.last_error(), 0u);

    TlsContext mismatched;  // the key of another certificate
    const std::string chain = cert("server.pem");
    const std::string key = cert("client.key");
    so.cert_chain_file = chain.c_str();
    so.key_file = key.c_str();
    EXPECT_EQ(mismatched.init_server(so), EINVAL);

    TlsContext no_files;
    EXPECT_EQ(no_files.init_server(tls::ServerOptions{}), EINVAL);

    TlsContext bad_alpn;
    const std::string_view empty[] = {""};
    tls::ClientOptions co;
    co.alpn = empty;
    co.verify_peer = false;
    EXPECT_EQ(bad_alpn.init_client(co), EINVAL);

    TlsContext twice;
    EXPECT_EQ(make_client(twice), 0);
    EXPECT_EQ(make_client(twice), EINVAL);

    TlsContext ok;
    EXPECT_EQ(make_server(ok), 0);
    EXPECT_TRUE(ok.initialized());
    EXPECT_TRUE(ok.is_server());
}

TEST(Tls, UpgradeErrors) {
    Scheduler scheduler(options(1));
    TlsContext sctx;
    TlsContext cctx;
    ASSERT_EQ(make_server(sctx), 0);
    ASSERT_EQ(make_client(cctx), 0);
    TlsStream s;
    EXPECT_EQ(s.upgrade_server(sctx), ENOTCONN);  // transport not open
    Pair pair;
    ASSERT_EQ(s.transport().adopt(scheduler, pair.fd[0]), 0);
    EXPECT_EQ(s.upgrade_server(cctx), EINVAL);           // client context
    EXPECT_EQ(s.upgrade_client(sctx, "localhost"), EINVAL);
    EXPECT_EQ(s.upgrade_client(cctx, nullptr), EINVAL);  // verify-full needs a name
    std::vector<std::byte> big(64 * 1024);
    EXPECT_EQ(s.upgrade_server(sctx, big), EMSGSIZE);
    // Operations without a TLS session.
    Latch done(1);
    std::atomic<int> read_error{0};
    std::atomic<int> handshake_error{0};
    TlsStream fresh;
    fiber::go(scheduler, [&]() noexcept {
        char c;
        read_error.store(fresh.read_some(&c, 1).error);
        handshake_error.store(fresh.handshake());
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(read_error.load(), ENOTCONN);
    EXPECT_EQ(handshake_error.load(), ENOTCONN);
    EXPECT_EQ(s.close(), 0);
    ::close(pair.fd[1]);
}

// Handshake + echo over a socketpair; session details on both sides.
TEST(Tls, HandshakeAndEcho) {
    Scheduler scheduler(options(2));
    TlsContext sctx;
    TlsContext cctx;
    ASSERT_EQ(make_server(sctx), 0);
    ASSERT_EQ(make_client(cctx), 0);
    TlsStream server;
    TlsStream client;
    Duo duo;
    ASSERT_EQ(duo.setup(scheduler, server, sctx, client, cctx), 0);
    const HandshakeResult h = handshake_both(scheduler, server, client);
    ASSERT_EQ(h.server, 0);
    ASSERT_EQ(h.client, 0);
    EXPECT_STREQ(client.version(), "TLSv1.3");
    EXPECT_STREQ(server.version(), "TLSv1.3");
    EXPECT_NE(std::strlen(client.cipher()), 0u);
    EXPECT_EQ(client.verify_result(), X509_V_OK);
    char subject[256];
    EXPECT_STREQ(client.peer_subject(subject, sizeof subject), "/CN=localhost");
    EXPECT_EQ(server.peer_certificate(), nullptr);
    EXPECT_EQ(client.alpn(), "");

    Latch done(2);
    std::atomic<int> server_error{-100};
    std::atomic<int> client_error{-100};
    fiber::go(scheduler, [&]() noexcept {
        server_error.store(echo(server));
        static_cast<void>(server.shutdown());
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        int e = client_rounds(client, 100, 1);
        if (e == 0) {
            e = client.shutdown();  // close_notify: the server reads end of stream
        }
        if (e == 0) {
            char c;
            const IoResult r = client.read_some(&c, 1);  // the server's close_notify
            e = r.error != 0 ? r.error : static_cast<int>(r.bytes);
        }
        client_error.store(e);
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(server_error.load(), 0);
    EXPECT_EQ(client_error.load(), 0);
    EXPECT_EQ(server.close(), 0);
    EXPECT_EQ(client.close(), 0);
}

// The first read / write runs the handshake when handshake() was not called.
TEST(Tls, ImplicitHandshake) {
    Scheduler scheduler(options(2));
    TlsContext sctx;
    TlsContext cctx;
    ASSERT_EQ(make_server(sctx), 0);
    ASSERT_EQ(make_client(cctx), 0);
    TlsStream server;
    TlsStream client;
    Duo duo;
    ASSERT_EQ(duo.setup(scheduler, server, sctx, client, cctx), 0);
    Latch done(2);
    std::atomic<int> server_error{-100};
    std::atomic<int> client_error{-100};
    fiber::go(scheduler, [&]() noexcept {
        server_error.store(echo(server));
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        int e = client_rounds(client, 10, 2);
        if (e == 0) {
            e = client.shutdown();
        }
        client_error.store(e);
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(server_error.load(), 0);
    EXPECT_EQ(client_error.load(), 0);
    EXPECT_TRUE(client.handshake_done());
    EXPECT_TRUE(server.handshake_done());
}


// ---- verification, mTLS, ALPN ---------------------------------------------------

// One connection over a socketpair with the given configurations: both
// handshakes, then (if the server's succeeded) a close_notify from the
// server, and a read on the client (end of stream, or the error that the
// server's failure caused).
struct Outcome {
    int server = -100;
    int client = -100;
    int client_read = -100;  // error of the client's read after the handshake (0: clean EOF)
    long client_verify = 0;
    long server_verify = 0;
    std::string client_error;
    std::string server_error;
    std::string client_alpn;
    std::string server_alpn;
    std::string server_peer;
};

Outcome connect_once(const ServerSpec& sspec, const ClientSpec& cspec, const char* name = "localhost") {
    Outcome out;
    Scheduler scheduler(options(2));
    TlsContext sctx;
    TlsContext cctx;
    if (make_server(sctx, sspec) != 0 || make_client(cctx, cspec) != 0) {
        return out;
    }
    TlsStream server;
    TlsStream client;
    Duo duo;
    if (duo.setup(scheduler, server, sctx, client, cctx, name) != 0) {
        return out;
    }
    Latch done(2);
    Latch client_done(1);
    fiber::go(scheduler, [&]() noexcept {
        out.server = server.handshake();
        if (out.server == 0) {
            static_cast<void>(server.shutdown());
        }
        // Stay open until the client has read what we sent (an alert).
        while (!client_done.wait(0ms)) {
            fiber::this_fiber::sleep_for(1ms);
        }
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        out.client = client.handshake();
        if (out.client == 0) {
            char c;
            const IoResult r = client.read_some(&c, 1);
            out.client_read = r.error != 0 ? r.error : static_cast<int>(r.bytes);
        }
        client_done.count_down();
        done.count_down();
    });
    if (!done.wait()) {
        out.server = out.client = -2;
        return out;
    }
    char buf[512];
    out.client_verify = client.verify_result();
    out.server_verify = server.verify_result();
    out.client_error = client.error_string(buf, sizeof buf);
    out.server_error = server.error_string(buf, sizeof buf);
    out.client_alpn = std::string(client.alpn());
    out.server_alpn = std::string(server.alpn());
    out.server_peer = server.peer_subject(buf, sizeof buf);
    return out;
}

bool contains(const std::string& s, const char* needle) { return s.find(needle) != std::string::npos; }

TEST(Tls, VerifyIpAddressSan) {
    const Outcome ok = connect_once({}, {}, "127.0.0.1");
    EXPECT_EQ(ok.server, 0);
    EXPECT_EQ(ok.client, 0);
    EXPECT_EQ(ok.client_read, 0);
    const Outcome bad = connect_once({}, {}, "127.0.0.2");
    EXPECT_EQ(bad.client, EPROTO);
    EXPECT_EQ(bad.client_verify, X509_V_ERR_IP_ADDRESS_MISMATCH) << bad.client_error;
    EXPECT_NE(bad.server, 0);  // the client's alert (or its EOF), not a hang
}

TEST(Tls, VerifyUnknownCa) {
    ServerSpec s;
    s.cert = "untrusted.pem";
    s.key = "untrusted.key";
    const Outcome o = connect_once(s, {});
    EXPECT_EQ(o.client, EPROTO);
    EXPECT_EQ(o.client_verify, X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY) << o.client_error;
    EXPECT_TRUE(contains(o.client_error, "certificate verify failed")) << o.client_error;
    // The client's "unknown ca" alert reaches the server.
    EXPECT_EQ(o.server, EPROTO);
    EXPECT_TRUE(contains(o.server_error, "unknown ca")) << o.server_error;
}

TEST(Tls, VerifyHostnameMismatch) {
    ServerSpec s;
    s.cert = "wronghost.pem";
    s.key = "wronghost.key";
    const Outcome o = connect_once(s, {});
    EXPECT_EQ(o.client, EPROTO);
    EXPECT_EQ(o.client_verify, X509_V_ERR_HOSTNAME_MISMATCH) << o.client_error;
    EXPECT_EQ(o.server, EPROTO);
    // verify_peer = false accepts it (tests only).
    ClientSpec c;
    c.verify = false;
    const Outcome unchecked = connect_once(s, c, "localhost");
    EXPECT_EQ(unchecked.client, 0);
    EXPECT_EQ(unchecked.server, 0);
}

TEST(Tls, VerifyExpired) {
    if (!exists(cert("expired.pem"))) {
        GTEST_SKIP() << "the openssl CLI could not make an expired certificate (needs >= 3.4)";
    }
    ServerSpec s;
    s.cert = "expired.pem";
    s.key = "expired.key";
    const Outcome o = connect_once(s, {});
    EXPECT_EQ(o.client, EPROTO);
    EXPECT_EQ(o.client_verify, X509_V_ERR_CERT_HAS_EXPIRED) << o.client_error;
}

// mTLS required, no client certificate: the server's handshake fails; in
// TLS 1.3 the client's handshake has already completed by then, so the
// client sees the server's "certificate required" alert on its first read.
TEST(Tls, MtlsRequiredWithoutCertificate) {
    ServerSpec s;
    s.verify_client = tls::VerifyClient::kRequired;
    const Outcome o = connect_once(s, {});
    EXPECT_EQ(o.server, EPROTO);
    EXPECT_TRUE(contains(o.server_error, "certificate")) << o.server_error;
    if (o.client == 0) {
        EXPECT_EQ(o.client_read, EPROTO);
    } else {
        EXPECT_EQ(o.client, EPROTO);
    }
    // TLS 1.2: the client's handshake itself fails.
    s.max_version = TLS1_2_VERSION;
    const Outcome o12 = connect_once(s, {});
    EXPECT_EQ(o12.server, EPROTO);
    EXPECT_EQ(o12.client, EPROTO);
}

TEST(Tls, MtlsWithCertificate) {
    ServerSpec s;
    s.verify_client = tls::VerifyClient::kRequired;
    ClientSpec c;
    c.cert = "client.pem";
    c.key = "client.key";
    const Outcome o = connect_once(s, c);
    EXPECT_EQ(o.server, 0) << o.server_error;
    EXPECT_EQ(o.client, 0) << o.client_error;
    EXPECT_EQ(o.client_read, 0);
    EXPECT_EQ(o.server_peer, "/CN=fibers test client");
    EXPECT_EQ(o.server_verify, X509_V_OK);
    // A client certificate from an unknown CA is refused.
    c.cert = "untrusted.pem";
    c.key = "untrusted.key";
    const Outcome bad = connect_once(s, c);
    EXPECT_EQ(bad.server, EPROTO);
    EXPECT_EQ(bad.server_verify, X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY);
    // Optional: no certificate is fine.
    s.verify_client = tls::VerifyClient::kOptional;
    const Outcome optional = connect_once(s, {});
    EXPECT_EQ(optional.server, 0);
    EXPECT_EQ(optional.server_peer, "");
}

TEST(Tls, Alpn) {
    ServerSpec s;
    ClientSpec c;
    s.alpn = {"http/1.1"};
    c.alpn = {"h2", "http/1.1"};
    Outcome o = connect_once(s, c);
    EXPECT_EQ(o.server, 0);
    EXPECT_EQ(o.client_alpn, "http/1.1");
    EXPECT_EQ(o.server_alpn, "http/1.1");
    // The server's preference wins.
    s.alpn = {"h2", "http/1.1"};
    c.alpn = {"http/1.1", "h2"};
    o = connect_once(s, c);
    EXPECT_EQ(o.client_alpn, "h2");
    EXPECT_EQ(o.server_alpn, "h2");
    // No overlap: no_application_protocol, both fail.
    s.alpn = {"h2"};
    c.alpn = {"http/1.1"};
    o = connect_once(s, c);
    EXPECT_EQ(o.server, EPROTO);
    EXPECT_EQ(o.client, EPROTO);
    EXPECT_TRUE(contains(o.client_error, "no application protocol")) << o.client_error;
    // A server without ALPN ignores the client's offer.
    s.alpn = {};
    o = connect_once(s, c);
    EXPECT_EQ(o.server, 0);
    EXPECT_EQ(o.client_alpn, "");
}

// ---- bulk data, full duplex, many connections -------------------------------------

// Sends `total` pattern bytes in chunks of varying size.
int send_pattern(TlsStream& s, std::size_t total, std::size_t seed, std::size_t key_update_every = 0) {
    std::vector<std::uint8_t> buf(100000);
    std::size_t sent = 0;
    std::size_t next_update = key_update_every;
    std::size_t round = 0;
    while (sent < total) {
        const std::size_t n = std::min(total - sent, (round++ * 7919) % buf.size() + 1);
        for (std::size_t i = 0; i < n; ++i) {
            buf[i] = pattern(sent + i, seed);
        }
        const IoResult w = (round % 3 == 0) ? s.write_some(buf.data(), n) : s.write_all(buf.data(), n);
        if (!w) {
            return w.error;
        }
        sent += w.bytes;
        if (key_update_every != 0 && sent >= next_update) {
            if (const int e = s.key_update(true); e != 0) {
                return e;
            }
            next_update += key_update_every;
        }
    }
    return 0;
}

// Receives and checks `total` pattern bytes.
int receive_pattern(TlsStream& s, std::size_t total, std::size_t seed) {
    std::vector<std::uint8_t> buf(65536);
    std::size_t got = 0;
    while (got < total) {
        const IoResult r = s.read_some(buf.data(), std::min(buf.size(), total - got));
        if (!r) {
            return r.error;
        }
        if (r.bytes == 0) {
            return -1;  // early end of stream
        }
        for (std::size_t i = 0; i < r.bytes; ++i) {
            if (buf[i] != pattern(got + i, seed)) {
                return -2;
            }
        }
        got += r.bytes;
    }
    return 0;
}

TEST(Tls, LargeTransfersBothDirections) {
    constexpr std::size_t kTotal = 24 << 20;
    Scheduler scheduler(options(2));
    TlsContext sctx;
    TlsContext cctx;
    ASSERT_EQ(make_server(sctx), 0);
    ASSERT_EQ(make_client(cctx), 0);
    TlsStream server;
    TlsStream client;
    Duo duo;
    ASSERT_EQ(duo.setup(scheduler, server, sctx, client, cctx), 0);
    Latch done(2);
    std::atomic<int> server_error{-100};
    std::atomic<int> client_error{-100};
    fiber::go(scheduler, [&]() noexcept {
        int e = receive_pattern(server, kTotal, 1);  // client -> server first
        if (e == 0) {
            e = send_pattern(server, kTotal, 2);
        }
        server_error.store(e);
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        int e = send_pattern(client, kTotal, 1);
        if (e == 0) {
            e = receive_pattern(client, kTotal, 2);
        }
        client_error.store(e);
        done.count_down();
    });
    ASSERT_TRUE(done.wait(120s));
    EXPECT_EQ(server_error.load(), 0);
    EXPECT_EQ(client_error.load(), 0);
}

// One reader fiber and one writer fiber per side, both directions at once,
// with KeyUpdate requests in flight: a reader must answer the peer's
// request through the write side while its own writer is streaming. A
// half-duplex implementation deadlocks here (both writers block on full
// socket buffers while nobody reads).
TEST(Tls, FullDuplexWithKeyUpdates) {
    constexpr std::size_t kTotal = 32 << 20;
    Scheduler scheduler(options(4));
    TlsContext sctx;
    TlsContext cctx;
    ASSERT_EQ(make_server(sctx), 0);
    ASSERT_EQ(make_client(cctx), 0);
    TlsStream server;
    TlsStream client;
    Duo duo;
    ASSERT_EQ(duo.setup(scheduler, server, sctx, client, cctx), 0);
    const HandshakeResult h = handshake_both(scheduler, server, client);
    ASSERT_EQ(h.server, 0);
    ASSERT_EQ(h.client, 0);
    Latch done(4);
    std::atomic<int> errors[4] = {-100, -100, -100, -100};
    fiber::go(scheduler, [&]() noexcept {
        errors[0].store(send_pattern(server, kTotal, 3, 3 << 20));
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        errors[1].store(receive_pattern(server, kTotal, 4));
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        errors[2].store(send_pattern(client, kTotal, 4, 5 << 20));
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        errors[3].store(receive_pattern(client, kTotal, 3));
        done.count_down();
    });
    ASSERT_TRUE(done.wait(120s));
    for (auto& e : errors) {
        EXPECT_EQ(e.load(), 0);
    }
    // Nothing a reader produced (KeyUpdate answers) was left behind in the
    // outbound rings.
    EXPECT_EQ(BIO_wpending(SSL_get_wbio(server.native_handle())), 0);
    EXPECT_EQ(BIO_wpending(SSL_get_wbio(client.native_handle())), 0);
}

// Many connections over loopback TCP on two workers: a fiber per
// connection on both sides, handshakes interleaved with echo traffic.
TEST(Tls, ManyConnectionsFewWorkers) {
    constexpr std::size_t kConnections = 200;
    constexpr std::size_t kRounds = 10;
    Latch clients(kConnections);
    Latch servers(kConnections + 1);
    std::atomic<int> errors{0};
    std::atomic<int> first_error{0};
    Scheduler scheduler(options(2, 1));
    TlsContext sctx;
    TlsContext cctx;
    ASSERT_EQ(make_server(sctx), 0);
    ASSERT_EQ(make_client(cctx), 0);
    Listener listener;
    ASSERT_EQ(listener.listen(scheduler, Endpoint::loopback_v4(0)), 0);
    const Endpoint endpoint = listener.local_endpoint();
    auto server_streams = std::make_unique<TlsStream[]>(kConnections);
    auto fail = [&](int e) {
        errors.fetch_add(1);
        int expected = 0;
        first_error.compare_exchange_strong(expected, e);
    };
    fiber::go(scheduler, [&]() noexcept {
        for (std::size_t i = 0; i < kConnections; ++i) {
            TlsStream& s = server_streams[i];
            if (const int e = s.accept(listener, sctx); e != 0) {
                fail(e);
                break;
            }
            fiber::go<Scheduler>([&, stream = &s]() noexcept {
                if (const int e = echo(*stream); e != 0) {
                    fail(e);
                }
                static_cast<void>(stream->shutdown());
                static_cast<void>(stream->close());
                servers.count_down();
            });
        }
        servers.count_down();
    });
    for (std::size_t c = 0; c < kConnections; ++c) {
        fiber::go(scheduler, [&, c]() noexcept {
            TlsStream s;
            if (const int e = s.connect(scheduler, endpoint, cctx, "127.0.0.1"); e != 0) {
                fail(e);
            } else {
                static_cast<void>(s.set_nodelay());
                if (const int e = client_rounds(s, kRounds, c, 4096); e != 0) {
                    fail(e);
                }
                if (const int e = s.shutdown(); e != 0) {
                    fail(e);
                }
                char ch;
                if (const IoResult r = s.read_some(&ch, 1); !r || r.bytes != 0) {
                    fail(r.error != 0 ? r.error : -3);
                }
                static_cast<void>(s.close());
            }
            clients.count_down();
        });
    }
    ASSERT_TRUE(clients.wait(120s));
    ASSERT_TRUE(servers.wait(60s));
    EXPECT_EQ(errors.load(), 0) << "first error " << first_error.load();
    EXPECT_EQ(listener.close(), 0);
}

// ---- upgrade after plaintext (STARTTLS) ---------------------------------------------

TEST(Tls, UpgradeAfterPlaintext) {
    for (const bool pipelined : {false, true}) {
        SCOPED_TRACE(pipelined ? "pipelined ClientHello" : "OK first");
        Scheduler scheduler(options(2));
        TlsContext sctx;
        TlsContext cctx;
        ASSERT_EQ(make_server(sctx), 0);
        ASSERT_EQ(make_client(cctx), 0);
        TlsStream server;
        TlsStream client;
        Pair pair;
        ASSERT_EQ(server.transport().adopt(scheduler, pair.fd[0]), 0);
        ASSERT_EQ(client.transport().adopt(scheduler, pair.fd[1]), 0);
        Latch done(2);
        std::atomic<int> server_error{-100};
        std::atomic<int> client_error{-100};
        std::atomic<std::size_t> leftover{0};
        fiber::go(scheduler, [&]() noexcept {
            int e = 0;
            if (pipelined) {
                fiber::this_fiber::sleep_for(50ms);  // the ClientHello is queued behind the line
            }
            // A buffered line reader: it may read past the line.
            char buf[4096];
            std::size_t have = 0;
            const char* nl = nullptr;
            while (nl == nullptr && e == 0) {
                const IoResult r = server.transport().read_some(buf + have, sizeof buf - have);
                if (!r || r.bytes == 0) {
                    e = r.error != 0 ? r.error : -1;
                    break;
                }
                have += r.bytes;
                nl = static_cast<const char*>(std::memchr(buf, '\n', have));
            }
            if (e == 0 && std::string_view(buf, static_cast<std::size_t>(nl - buf)) != "STARTTLS") {
                e = -2;
            }
            if (e == 0 && !pipelined) {
                e = server.transport().write_all("OK\n", 3).error;
            }
            if (e == 0) {
                const std::size_t used = static_cast<std::size_t>(nl - buf) + 1;
                leftover.store(have - used);
                const auto rest = std::as_bytes(std::span<const char>(buf + used, have - used));
                e = server.upgrade_server(sctx, rest);
            }
            if (e == 0) {
                e = echo(server);
            }
            server_error.store(e);
            done.count_down();
        });
        fiber::go(scheduler, [&]() noexcept {
            int e = client.transport().write_all("STARTTLS\n", 9).error;
            if (e == 0 && !pipelined) {
                char ok[3];
                const IoResult r = client.transport().read_exact(ok, 3);
                e = r.error != 0 ? r.error : (r.bytes == 3 && std::memcmp(ok, "OK\n", 3) == 0 ? 0 : -1);
            }
            if (e == 0) {
                e = client.upgrade_client(cctx, "localhost");
            }
            if (e == 0) {
                e = client.handshake();
            }
            if (e == 0) {
                e = client_rounds(client, 20, 7);
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
        if (pipelined) {
            EXPECT_GT(leftover.load(), 0u);  // the server's reader overshot into the ClientHello
        }
    }
}

// ---- close_notify vs abrupt close -------------------------------------------------

// The server closes without close_notify: a truncation (ECONNABORTED), or
// end of stream with ignore_unexpected_eof.
TEST(Tls, AbruptCloseWithoutCloseNotify) {
    for (const bool ignore : {false, true}) {
        Scheduler scheduler(options(2));
        TlsContext sctx;
        TlsContext cctx;
        ASSERT_EQ(make_server(sctx), 0);
        ClientSpec c;
        c.ignore_unexpected_eof = ignore;
        ASSERT_EQ(make_client(cctx, c), 0);
        TlsStream server;
        TlsStream client;
        Duo duo;
        ASSERT_EQ(duo.setup(scheduler, server, sctx, client, cctx), 0);
        const HandshakeResult h = handshake_both(scheduler, server, client);
        ASSERT_EQ(h.server, 0);
        ASSERT_EQ(h.client, 0);
        Latch done(1);
        std::atomic<int> result{-100};
        std::atomic<int> again{-100};
        fiber::go(scheduler, [&]() noexcept {
            static_cast<void>(server.write_all("data", 4));
            static_cast<void>(server.close());  // no close_notify
            char buf[16];
            IoResult r = client.read_exact(buf, 4);  // the data still arrives
            if (r && r.bytes == 4) {
                r = client.read_some(buf, sizeof buf);
                result.store(r.error != 0 ? r.error : static_cast<int>(r.bytes));
                r = client.read_some(buf, sizeof buf);  // sticky
                again.store(r.error != 0 ? r.error : static_cast<int>(r.bytes));
            }
            done.count_down();
        });
        ASSERT_TRUE(done.wait());
        EXPECT_EQ(result.load(), ignore ? 0 : ECONNABORTED);
        EXPECT_EQ(again.load(), ignore ? 0 : ECONNABORTED);
        if (!ignore) {
            char buf[256];
            EXPECT_TRUE(contains(client.error_string(buf, sizeof buf), "unexpected eof"))
                << client.error_string(buf, sizeof buf);
        }
    }
}

// The peer resets the TCP connection in the middle of a record: the
// transport's ECONNRESET, and the partial record is never delivered.
TEST(Tls, PeerResetMidRecord) {
    Scheduler scheduler(options(2));
    TlsContext sctx;
    TlsContext cctx;
    ASSERT_EQ(make_server(sctx), 0);
    ASSERT_EQ(make_client(cctx), 0);
    Listener listener;
    ASSERT_EQ(listener.listen(scheduler, Endpoint::loopback_v4(0)), 0);
    const Endpoint endpoint = listener.local_endpoint();
    TlsStream server;
    TlsStream client;
    Latch done(2);
    std::atomic<int> result{-100};
    std::atomic<int> server_error{-100};
    Latch client_reading(1);
    fiber::go(scheduler, [&]() noexcept {
        int e = server.accept(listener, sctx);
        if (e == 0) {
            e = server.handshake();
        }
        if (e == 0) {
            e = server.write_all("whole", 5).error;
        }
        while (e == 0 && !client_reading.wait(0ms)) {
            fiber::this_fiber::sleep_for(1ms);
        }
        if (e == 0) {
            // An application-data record header announcing 1000 bytes, then 10.
            const unsigned char partial[15] = {0x17, 0x03, 0x03, 0x03, 0xe8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
            e = server.transport().write_all(partial, sizeof partial).error;
            fiber::this_fiber::sleep_for(20ms);
            const linger lg{1, 0};  // close -> RST
            static_cast<void>(server.transport().set_option(SOL_SOCKET, SO_LINGER, &lg, sizeof lg));
            static_cast<void>(server.close());
        }
        server_error.store(e);
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        int e = client.connect(scheduler, endpoint, cctx, "localhost");
        char buf[64];
        if (e == 0) {
            const IoResult r = client.read_exact(buf, 5);
            e = r.error;
        }
        client_reading.count_down();
        if (e == 0) {
            const IoResult r = client.read_some(buf, sizeof buf);
            e = r.error != 0 ? r.error : -static_cast<int>(r.bytes) - 1;
        }
        result.store(e);
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(server_error.load(), 0);
    EXPECT_EQ(result.load(), ECONNRESET);
    EXPECT_EQ(listener.close(), 0);
}

// ---- deadlines and close ------------------------------------------------------------

// A handshake against a silent peer times out (the transport's read
// deadline); after the peer shows up and the deadline is cleared, the same
// handshake resumes and completes: ETIMEDOUT is not sticky.
TEST(Tls, HandshakeDeadlineThenResume) {
    Scheduler scheduler(options(2));
    TlsContext sctx;
    TlsContext cctx;
    ASSERT_EQ(make_server(sctx), 0);
    ASSERT_EQ(make_client(cctx), 0);
    TlsStream server;
    TlsStream client;
    Duo duo;
    ASSERT_EQ(duo.setup(scheduler, server, sctx, client, cctx), 0);
    Latch first(1);
    std::atomic<int> timed_out{-100};
    std::atomic<long long> waited_ms{0};
    fiber::go(scheduler, [&]() noexcept {
        const auto start = std::chrono::steady_clock::now();
        static_cast<void>(client.set_read_deadline(scheduler.now() + 100ms));
        timed_out.store(client.handshake());
        waited_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() -
                                                                              start)
                            .count());
        first.count_down();
    });
    ASSERT_TRUE(first.wait());
    EXPECT_EQ(timed_out.load(), ETIMEDOUT);
    EXPECT_GE(waited_ms.load(), 90);
    EXPECT_FALSE(client.handshake_done());
    Latch second(2);
    std::atomic<int> se{-100};
    std::atomic<int> ce{-100};
    fiber::go(scheduler, [&]() noexcept {
        se.store(server.handshake());
        second.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        static_cast<void>(client.set_read_deadline(fibers::poller::TimePoint{}));
        ce.store(client.handshake());
        second.count_down();
    });
    ASSERT_TRUE(second.wait());
    EXPECT_EQ(se.load(), 0);
    EXPECT_EQ(ce.load(), 0);
}

TEST(Tls, ReadDeadlineThenResume) {
    Scheduler scheduler(options(2));
    TlsContext sctx;
    TlsContext cctx;
    ASSERT_EQ(make_server(sctx), 0);
    ASSERT_EQ(make_client(cctx), 0);
    TlsStream server;
    TlsStream client;
    Duo duo;
    ASSERT_EQ(duo.setup(scheduler, server, sctx, client, cctx), 0);
    ASSERT_EQ(handshake_both(scheduler, server, client).client, 0);
    Latch done(1);
    std::atomic<int> first{-100};
    std::atomic<int> second{-100};
    fiber::go(scheduler, [&]() noexcept {
        char buf[16];
        static_cast<void>(client.set_read_deadline(scheduler.now() + 50ms));
        first.store(client.read_some(buf, sizeof buf).error);
        static_cast<void>(client.set_read_deadline(fibers::poller::TimePoint{}));
        static_cast<void>(server.write_all("late", 4));
        const IoResult r = client.read_exact(buf, 4);
        second.store(r.error != 0 ? r.error : (std::memcmp(buf, "late", 4) == 0 ? 0 : -1));
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(first.load(), ETIMEDOUT);
    EXPECT_EQ(second.load(), 0);
}

// close() while a fiber is parked in a TLS read: ECANCELED.
TEST(Tls, CloseWakesParkedReader) {
    Scheduler scheduler(options(2));
    TlsContext sctx;
    TlsContext cctx;
    ASSERT_EQ(make_server(sctx), 0);
    ASSERT_EQ(make_client(cctx), 0);
    TlsStream server;
    TlsStream client;
    Duo duo;
    ASSERT_EQ(duo.setup(scheduler, server, sctx, client, cctx), 0);
    ASSERT_EQ(handshake_both(scheduler, server, client).client, 0);
    Latch done(1);
    std::atomic<int> result{-100};
    fiber::go(scheduler, [&]() noexcept {
        char buf[16];
        result.store(client.read_some(buf, sizeof buf).error);
        done.count_down();
    });
    ASSERT_TRUE(eventually([&] {
        return client.transport().poll_desc().word_approx(net::Direction::kRead) >
               net::BasicPollDesc<Scheduler>::kClosed;
    }));
    EXPECT_EQ(client.close(), 0);  // from this (non-fiber) thread
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(result.load(), ECANCELED);
}

// Writing after shutdown(): EPIPE; reading goes on (half-close).
TEST(Tls, WriteAfterShutdown) {
    Scheduler scheduler(options(2));
    TlsContext sctx;
    TlsContext cctx;
    ASSERT_EQ(make_server(sctx), 0);
    ASSERT_EQ(make_client(cctx), 0);
    TlsStream server;
    TlsStream client;
    Duo duo;
    ASSERT_EQ(duo.setup(scheduler, server, sctx, client, cctx), 0);
    ASSERT_EQ(handshake_both(scheduler, server, client).client, 0);
    Latch done(1);
    std::atomic<int> write_error{-100};
    std::atomic<int> server_eof{-100};
    std::atomic<int> read_back{-100};
    fiber::go(scheduler, [&]() noexcept {
        static_cast<void>(client.shutdown());
        write_error.store(client.write_all("x", 1).error);
        char buf[8];
        IoResult r = server.read_some(buf, sizeof buf);
        server_eof.store(r.error != 0 ? r.error : static_cast<int>(r.bytes));
        static_cast<void>(server.write_all("bye", 3));  // the server may still write
        r = client.read_exact(buf, 3);
        read_back.store(r.error != 0 ? r.error : (std::memcmp(buf, "bye", 3) == 0 ? 0 : -1));
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(write_error.load(), EPIPE);
    EXPECT_EQ(server_eof.load(), 0);
    EXPECT_EQ(read_back.load(), 0);
}

// ---- interop with the openssl CLI -----------------------------------------------------

// Our server (echo) against `openssl s_client`, with ALPN.
TEST(Tls, InteropServerWithOpensslClient) {
    Scheduler scheduler(options(2));
    TlsContext sctx;
    ServerSpec s;
    s.alpn = {"http/1.1"};
    ASSERT_EQ(make_server(sctx, s), 0);
    Listener listener;
    ASSERT_EQ(listener.listen(scheduler, Endpoint::loopback_v4(0)), 0);
    const std::string port = std::to_string(listener.local_endpoint().port());
    TlsStream server;
    Latch done(1);
    std::atomic<int> handshake{-100};
    std::string alpn;
    fiber::go(scheduler, [&]() noexcept {
        int e = server.accept(listener, sctx);
        if (e == 0) {
            e = server.handshake();
        }
        handshake.store(e);
        if (e == 0) {
            alpn = std::string(server.alpn());
            static_cast<void>(echo(server));  // until s_client is killed
        }
        static_cast<void>(server.close());
        done.count_down();
    });
    Child child;
    ASSERT_TRUE(child.spawn({FIBERS_OPENSSL_CLI, "s_client", "-connect", "127.0.0.1:" + port, "-CAfile",
                             cert("ca.pem"), "-verify_return_error", "-verify_hostname", "localhost", "-servername",
                             "localhost", "-alpn", "http/1.1", "-quiet"}));
    ASSERT_TRUE(child.write("hello from s_client\n"));
    EXPECT_TRUE(child.read_until("hello from s_client\n")) << child.output();
    child.stop();
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(handshake.load(), 0);
    EXPECT_EQ(alpn, "http/1.1");
    EXPECT_EQ(listener.close(), 0);
}

// Our client against `openssl s_server -rev` (reverses each line).
TEST(Tls, InteropClientWithOpensslServer) {
    const std::uint16_t port = free_port();
    ASSERT_NE(port, 0);
    Child child;
    ASSERT_TRUE(child.spawn({FIBERS_OPENSSL_CLI, "s_server", "-accept", "127.0.0.1:" + std::to_string(port),
                             "-cert", cert("server.pem"), "-key", cert("server.key"), "-rev", "-alpn",
                             "h2,http/1.1"}));
    ASSERT_TRUE(child.read_until("ACCEPT")) << child.output();
    Scheduler scheduler(options(2));
    TlsContext cctx;
    ClientSpec c;
    c.alpn = {"http/1.1"};
    ASSERT_EQ(make_client(cctx, c), 0);
    TlsStream client;
    Latch done(1);
    std::atomic<int> result{-100};
    std::string alpn;
    std::string reply;
    fiber::go(scheduler, [&]() noexcept {
        int e = client.connect(scheduler, Endpoint::loopback_v4(port), cctx, "localhost");
        if (e == 0) {
            alpn = std::string(client.alpn());
            e = client.write_all("hello\n", 6).error;
        }
        char buf[64];
        while (e == 0 && reply.find('\n') == std::string::npos) {
            const IoResult r = client.read_some(buf, sizeof buf);
            if (!r || r.bytes == 0) {
                e = r.error != 0 ? r.error : -1;
                break;
            }
            reply.append(buf, r.bytes);
        }
        static_cast<void>(client.shutdown());
        static_cast<void>(client.close());
        result.store(e);
        done.count_down();
    });
    ASSERT_TRUE(done.wait(30s));
    EXPECT_EQ(result.load(), 0);
    EXPECT_EQ(reply, "olleh\n");
    EXPECT_EQ(alpn, "http/1.1");
}


// ---- stack depth ------------------------------------------------------------------------

constexpr unsigned char kPaint = 0xA5;

struct StackWindow {
    std::uintptr_t low = 0;   // painted from here
    std::uintptr_t high = 0;  // up to here (below the caller's frame)
};

// The current fiber's free stack below the caller (fiber.hpp layout: the
// fiber object at the top of its page-aligned stack), painted.
[[gnu::noinline]] StackWindow paint_stack(std::size_t stack_size) {
    using Fiber = fiber::BasicFiber<Scheduler>;
    const Fiber* f = Fiber::current();
    const std::uintptr_t page = static_cast<std::uintptr_t>(::sysconf(_SC_PAGESIZE));
    const std::uintptr_t top = (reinterpret_cast<std::uintptr_t>(f) + sizeof(Fiber) + page - 1) & ~(page - 1);
    const std::uintptr_t bottom = top - ((stack_size + page - 1) & ~(page - 1));
    StackWindow w;
    w.low = bottom + 256;
    w.high = reinterpret_cast<std::uintptr_t>(__builtin_frame_address(0)) - 1024;
    for (auto* p = reinterpret_cast<volatile unsigned char*>(w.low);
         p < reinterpret_cast<volatile unsigned char*>(w.high); ++p) {
        *p = kPaint;
    }
    return w;
}

// Bytes below the caller's frame that something wrote since paint_stack()
// (an upper bound: the unpainted 1 KiB below the frame is counted).
[[gnu::noinline]] std::size_t used_depth(const StackWindow& w) {
    for (auto* p = reinterpret_cast<const volatile unsigned char*>(w.low);
         p < reinterpret_cast<const volatile unsigned char*>(w.high); ++p) {
        if (*p != kPaint) {
            return w.high - reinterpret_cast<std::uintptr_t>(p) + 1024;
        }
    }
    return 0;
}

struct Depths {
    std::size_t client_handshake = 0;
    std::size_t server_handshake = 0;
    std::size_t write = 0;
    std::size_t read = 0;
};

Depths measure_depths(const char* server_cert, const char* server_key, bool mtls) {
    constexpr std::size_t kStack = 512 * 1024;
    Depths d;
    Scheduler scheduler(options(2, 1, kStack));
    TlsContext sctx;
    TlsContext cctx;
    ServerSpec s;
    s.cert = server_cert;
    s.key = server_key;
    ClientSpec c;
    if (mtls) {
        s.verify_client = tls::VerifyClient::kRequired;
        c.cert = "client.pem";
        c.key = "client.key";
    }
    if (make_server(sctx, s) != 0 || make_client(cctx, c) != 0) {
        return d;
    }
    TlsStream server;
    TlsStream client;
    Duo duo;
    if (duo.setup(scheduler, server, sctx, client, cctx) != 0) {
        return d;
    }
    Latch done(2);
    fiber::go(scheduler, [&]() noexcept {
        StackWindow w = paint_stack(kStack);
        if (server.handshake() == 0) {
            d.server_handshake = used_depth(w);
            char buf[16384];
            w = paint_stack(kStack);
            if (server.read_exact(buf, sizeof buf).bytes == sizeof buf) {
                d.read = used_depth(w);
            }
        }
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        StackWindow w = paint_stack(kStack);
        if (client.handshake() == 0) {
            d.client_handshake = used_depth(w);
            static char buf[16384];
            w = paint_stack(kStack);
            if (client.write_all(buf, sizeof buf)) {
                d.write = used_depth(w);
            }
        }
        done.count_down();
    });
    if (!done.wait()) {
        return Depths{};
    }
    return d;
}

// How deep OpenSSL goes below the caller's frame (painted fiber stack). The
// figures back the default CommonOptions::stack_reserve and the stack size
// recommendation in stream.hpp; only meaningful without sanitizers.
TEST(Tls, StackDepth) {
    if (kSanitized) {
        GTEST_SKIP() << "sanitizers change frame sizes (and painting would touch poisoned stack)";
    }
    std::size_t deepest = 0;
    struct Case {
        const char* name;
        const char* cert;
        const char* key;
        bool mtls;
    };
    for (const Case& k : {Case{"ec", "server.pem", "server.key", false},
                          Case{"rsa2048", "server_rsa.pem", "server_rsa.key", false},
                          Case{"ec+mtls", "server.pem", "server.key", true}}) {
        const Depths d = measure_depths(k.cert, k.key, k.mtls);
        std::printf("stack depth [%s]: client handshake %zu, server handshake %zu, write 16K %zu, read 16K %zu\n",
                    k.name, d.client_handshake, d.server_handshake, d.write, d.read);
        EXPECT_GT(d.client_handshake, 0u);
        EXPECT_GT(d.server_handshake, 0u);
        EXPECT_GT(d.read, 0u);
        deepest = std::max({deepest, d.client_handshake, d.server_handshake, d.write, d.read});
    }
    RecordProperty("deepest", static_cast<int>(deepest));
    // The default reserve covers the deepest call with room to spare.
    EXPECT_LE(deepest * 3 / 2, tls::CommonOptions{}.stack_reserve);
}


// A fiber stack with less than stack_reserve left fails fast with a
// message instead of overflowing.
TEST(TlsDeathTest, StackReserveFailsFast) {
    if (kSanitized) {
        GTEST_SKIP() << "death tests with sanitizers and threads are unreliable";
    }
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            Scheduler scheduler(options(1, 1, 16 * 1024));  // < the default 24 KiB reserve
            TlsContext sctx;
            TlsContext cctx;
            if (make_server(sctx) != 0 || make_client(cctx) != 0) {
                std::abort();
            }
            TlsStream server;
            TlsStream client;
            Duo duo;
            if (duo.setup(scheduler, server, sctx, client, cctx) != 0) {
                std::abort();
            }
            Latch done(1);
            fiber::go(scheduler, [&]() noexcept {
                static_cast<void>(client.handshake());
                done.count_down();
            });
            static_cast<void>(done.wait(10s));
            std::_Exit(0);
        },
        "stack_reserve");
}

}  // namespace
