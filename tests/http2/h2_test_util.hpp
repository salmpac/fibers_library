#pragma once

// Test helpers for fibers::http2: a scheduler config, running a body in a
// fiber, child processes (nghttpd), and MockH2Server -- a small HTTP/2
// server over nghttp2 on plain threads (h2c or TLS with ALPN "h2") with
// scripted behaviour per :path:
//
//   /echo              200, the request body back
//   /delay?ms=N        200 "delayed" after N ms
//   /hang              never answers
//   /big?n=N           200 with N bytes (generated as flow control allows)
//   /status?code=N     status N, body {"reason":"Status<N>"}
//   /bigheader?n=N     200 with one header value of N bytes
//   /goaway?last=N     GOAWAY(last_stream_id = N), no response
//   /close             closes the connection abruptly (no response)
//   /3/device/<token>  APNs semantics (see respond_apns)
//
// Counters (atomics): connections, requests (header blocks complete),
// pings, rst_cancel / rst_any (RST_STREAM received), max_open_streams;
// the headers of the last request.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <nghttp2/nghttp2.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
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
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "fibers/fiber/event.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/sched/scheduler.hpp"

extern char** environ;

namespace h2test {

using namespace std::chrono_literals;

#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
inline constexpr bool kSanitized = true;
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
inline constexpr bool kSanitized = true;
#else
inline constexpr bool kSanitized = false;
#endif
#else
inline constexpr bool kSanitized = false;
#endif

inline fibers::sched::SchedulerOptions scheduler_options(std::size_t workers = 4) {
    fibers::sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = 2;
    o.blocking.num_threads = 1;
    o.stacks.stack_size = kSanitized ? 1024 * 1024 : 256 * 1024;
    o.stacks.max_stacks = 1024;
    return o;
}

// Runs `body` in a fiber of `scheduler` and waits (bounded) for it.
template <class F>
bool run_in_fiber(fibers::sched::Scheduler& scheduler, F&& body, std::chrono::seconds timeout = 120s) {
    std::atomic<bool> done{false};
    fibers::fiber::go(scheduler, [&]() noexcept {
        body();
        done.store(true, std::memory_order_release);
    });
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!done.load(std::memory_order_acquire)) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(200us);
    }
    return true;
}

template <class Pred>
bool wait_until(Pred&& pred, std::chrono::milliseconds timeout = 20000ms) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(500us);
    }
    return true;
}

// The same inside a fiber (sleeps the fiber, not the worker).
template <class Pred>
bool fiber_wait_until(Pred&& pred, std::chrono::milliseconds timeout = 20000ms) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        fibers::fiber::this_fiber::sleep_for(1ms);
    }
    return true;
}

inline std::uint16_t free_port() {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    sockaddr_in in{};
    in.sin_family = AF_INET;
    in.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&in), sizeof in) != 0) {
        ::close(fd);
        return 0;
    }
    socklen_t len = sizeof in;
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&in), &len);
    ::close(fd);
    return ntohs(in.sin_port);
}

inline bool port_open(std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    sockaddr_in in{};
    in.sin_family = AF_INET;
    in.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    in.sin_port = htons(port);
    const bool ok = ::connect(fd, reinterpret_cast<sockaddr*>(&in), sizeof in) == 0;
    ::close(fd);
    return ok;
}

// A child process (stdout / stderr to /dev/null), SIGKILLed on stop().
class Child {
public:
    Child() = default;
    Child(const Child&) = delete;
    Child& operator=(const Child&) = delete;
    ~Child() { stop(); }

    bool spawn(const std::vector<std::string>& args) {
        posix_spawn_file_actions_t actions;
        posix_spawn_file_actions_init(&actions);
        posix_spawn_file_actions_addopen(&actions, 1, "/dev/null", O_WRONLY, 0);
        posix_spawn_file_actions_addopen(&actions, 2, "/dev/null", O_WRONLY, 0);
        std::vector<char*> argv;
        for (const std::string& a : args) {
            argv.push_back(const_cast<char*>(a.c_str()));
        }
        argv.push_back(nullptr);
        const int r = ::posix_spawn(&pid_, args[0].c_str(), &actions, nullptr, argv.data(), environ);
        posix_spawn_file_actions_destroy(&actions);
        if (r != 0) {
            pid_ = -1;
            return false;
        }
        return true;
    }

    void stop() {
        if (pid_ > 0) {
            ::kill(pid_, SIGKILL);
            int status = 0;
            ::waitpid(pid_, &status, 0);
            pid_ = -1;
        }
    }

    // Still running (not exited, e.g. because its port was taken).
    bool alive() {
        if (pid_ <= 0) {
            return false;
        }
        int status = 0;
        if (::waitpid(pid_, &status, WNOHANG) == pid_) {
            pid_ = -1;
            return false;
        }
        return true;
    }

private:
    pid_t pid_ = -1;
};

// ---- MockH2Server ----------------------------------------------------------------

struct MockOptions {
    std::uint32_t max_concurrent_streams = 100;
    bool auto_ping_ack = true;
    SSL_CTX* tls = nullptr;  // server context (ALPN "h2" selected by the mock)
};

class MockH2Server {
public:
    explicit MockH2Server(MockOptions options = {}) : options_(options) {
        ::signal(SIGPIPE, SIG_IGN);  // SSL_write on a socket the client closed
    }
    ~MockH2Server() { stop(); }

    MockH2Server(const MockH2Server&) = delete;
    MockH2Server& operator=(const MockH2Server&) = delete;

    // Listens on 127.0.0.1:port (0: any free port).
    bool start(std::uint16_t port = 0) {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        const int one = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
        sockaddr_in in{};
        in.sin_family = AF_INET;
        in.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        in.sin_port = htons(port);
        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&in), sizeof in) != 0 || ::listen(listen_fd_, 64) != 0) {
            ::close(listen_fd_);
            listen_fd_ = -1;
            return false;
        }
        socklen_t len = sizeof in;
        ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&in), &len);
        port_ = ntohs(in.sin_port);
        stopping_.store(false);
        accept_thread_ = std::thread([this] { accept_loop(); });
        return true;
    }

    std::uint16_t port() const { return port_; }

    // Stops listening and closes every connection; joins all threads.
    void stop() {
        if (listen_fd_ < 0) {
            return;
        }
        stopping_.store(true);
        ::shutdown(listen_fd_, SHUT_RDWR);
        if (accept_thread_.joinable()) {
            accept_thread_.join();
        }
        ::close(listen_fd_);
        listen_fd_ = -1;
        kill_connections();
        std::vector<std::thread> threads;
        {
            std::lock_guard g(mu_);
            threads.swap(threads_);
        }
        for (std::thread& t : threads) {
            t.join();
        }
    }

    // Drops every open connection abruptly (the server keeps listening).
    void kill_connections() {
        std::lock_guard g(mu_);
        for (auto& [fd, killed] : live_) {
            killed->store(true);
            ::shutdown(fd, SHUT_RDWR);
        }
    }

    std::string last_header(const std::string& name) {
        std::lock_guard g(mu_);
        for (const auto& [n, v] : last_headers_) {
            if (n == name) {
                return v;
            }
        }
        return {};
    }

    std::string last_path() {
        std::lock_guard g(mu_);
        return last_path_;
    }

    std::atomic<int> connections{0};
    std::atomic<int> requests{0};
    std::atomic<int> pings{0};
    std::atomic<int> rst_cancel{0};
    std::atomic<int> rst_any{0};
    std::atomic<int> open_streams{0};
    std::atomic<int> max_open_streams{0};
    std::atomic<int> expired_once{0};  // APNs "expired-once" answered 403 so far

private:
    struct StreamData {
        std::string method;
        std::string path;
        std::string body;
        std::vector<std::pair<std::string, std::string>> headers;
        // response generation
        std::string out;
        std::size_t out_pos = 0;
        std::uint64_t gen_left = 0;  // /big: bytes still to generate
    };

    struct Delayed {
        std::chrono::steady_clock::time_point at;
        std::int32_t stream_id;
    };

    struct Conn {
        MockH2Server* server = nullptr;
        int fd = -1;
        SSL* ssl = nullptr;
        nghttp2_session* session = nullptr;
        std::map<std::int32_t, std::unique_ptr<StreamData>> streams;
        std::vector<Delayed> delayed;
        std::shared_ptr<std::atomic<bool>> killed;
        bool close_now = false;
    };

    void accept_loop() {
        for (;;) {
            const int fd = ::accept4(listen_fd_, nullptr, nullptr, SOCK_CLOEXEC);
            if (fd < 0) {
                if (stopping_.load()) {
                    return;
                }
                if (errno == EINTR || errno == ECONNABORTED) {
                    continue;
                }
                return;
            }
            const int one = 1;
            ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
            auto killed = std::make_shared<std::atomic<bool>>(false);
            std::lock_guard g(mu_);
            if (stopping_.load()) {
                ::close(fd);
                return;
            }
            live_.emplace_back(fd, killed);
            connections.fetch_add(1);
            threads_.emplace_back([this, fd, killed] { serve(fd, killed); });
        }
    }

    void forget(int fd) {
        std::lock_guard g(mu_);
        live_.erase(std::remove_if(live_.begin(), live_.end(), [fd](const auto& p) { return p.first == fd; }),
                    live_.end());
    }

    // ---- I/O (plain or TLS, non-blocking socket) ---------------------------------

    static long io_read(Conn& c, char* buf, std::size_t n) {
        if (c.ssl == nullptr) {
            const ssize_t r = ::read(c.fd, buf, n);
            if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                return -2;
            }
            return r;  // 0: EOF, -1: error
        }
        ERR_clear_error();
        const int r = SSL_read(c.ssl, buf, static_cast<int>(n));
        if (r > 0) {
            return r;
        }
        const int e = SSL_get_error(c.ssl, r);
        ERR_clear_error();
        if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) {
            return -2;
        }
        return e == SSL_ERROR_ZERO_RETURN ? 0 : -1;
    }

    // Writes everything (polls for POLLOUT); false on error / kill.
    static bool io_write_all(Conn& c, const char* data, std::size_t n) {
        std::size_t done = 0;
        while (done < n) {
            if (c.killed->load()) {
                return false;
            }
            long w;
            if (c.ssl == nullptr) {
                const ssize_t r = ::send(c.fd, data + done, n - done, MSG_NOSIGNAL);
                w = r >= 0 ? r : ((errno == EAGAIN || errno == EWOULDBLOCK) ? -2 : -1);
            } else {
                ERR_clear_error();
                const int r = SSL_write(c.ssl, data + done, static_cast<int>(n - done));
                if (r > 0) {
                    w = r;
                } else {
                    const int e = SSL_get_error(c.ssl, r);
                    w = (e == SSL_ERROR_WANT_WRITE || e == SSL_ERROR_WANT_READ) ? -2 : -1;
                }
                ERR_clear_error();
            }
            if (w == -1) {
                return false;
            }
            if (w == -2) {
                pollfd p{c.fd, POLLOUT, 0};
                ::poll(&p, 1, 50);
                continue;
            }
            done += static_cast<std::size_t>(w);
        }
        return true;
    }

    bool tls_accept(Conn& c) {
        c.ssl = SSL_new(options_.tls);
        SSL_set_fd(c.ssl, c.fd);
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        for (;;) {
            ERR_clear_error();
            const int r = SSL_accept(c.ssl);
            if (r == 1) {
                ERR_clear_error();
                return true;
            }
            const int e = SSL_get_error(c.ssl, r);
            ERR_clear_error();
            if ((e != SSL_ERROR_WANT_READ && e != SSL_ERROR_WANT_WRITE) || c.killed->load() ||
                std::chrono::steady_clock::now() > deadline) {
                return false;
            }
            pollfd p{c.fd, static_cast<short>(e == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT), 0};
            ::poll(&p, 1, 50);
        }
    }

    // ---- one connection ----------------------------------------------------------

    void serve(int fd, std::shared_ptr<std::atomic<bool>> killed) {
        ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL) | O_NONBLOCK);
        Conn c;
        c.server = this;
        c.fd = fd;
        c.killed = std::move(killed);
        if (options_.tls == nullptr || tls_accept(c)) {
            run(c);
        }
        if (c.session != nullptr) {
            nghttp2_session_del(c.session);
        }
        if (c.ssl != nullptr) {
            SSL_free(c.ssl);
        }
        open_streams.fetch_sub(static_cast<int>(c.streams.size()));
        forget(fd);
        ::close(fd);
    }

    void run(Conn& c) {
        nghttp2_session_callbacks* cbs = nullptr;
        nghttp2_session_callbacks_new(&cbs);
        nghttp2_session_callbacks_set_on_begin_headers_callback(cbs, &on_begin_headers);
        nghttp2_session_callbacks_set_on_header_callback(cbs, &on_header);
        nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cbs, &on_data);
        nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, &on_frame_recv);
        nghttp2_session_callbacks_set_on_stream_close_callback(cbs, &on_stream_close);
        nghttp2_option* opt = nullptr;
        nghttp2_option_new(&opt);
        nghttp2_option_set_no_auto_ping_ack(opt, options_.auto_ping_ack ? 0 : 1);
        nghttp2_session_server_new2(&c.session, cbs, &c, opt);
        nghttp2_option_del(opt);
        nghttp2_session_callbacks_del(cbs);
        const nghttp2_settings_entry iv[] = {{NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, options_.max_concurrent_streams}};
        nghttp2_submit_settings(c.session, NGHTTP2_FLAG_NONE, iv, 1);

        char buf[16384];
        for (;;) {
            if (c.killed->load() || c.close_now) {
                return;
            }
            // Due delayed responses.
            const auto now = std::chrono::steady_clock::now();
            int timeout_ms = 20;
            for (std::size_t i = 0; i < c.delayed.size();) {
                if (c.delayed[i].at <= now) {
                    const std::int32_t id = c.delayed[i].stream_id;
                    c.delayed.erase(c.delayed.begin() + static_cast<std::ptrdiff_t>(i));
                    if (auto it = c.streams.find(id); it != c.streams.end()) {
                        respond(c, id, 200, {}, "delayed");
                    }
                } else {
                    const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(c.delayed[i].at - now);
                    timeout_ms = std::min<int>(timeout_ms, static_cast<int>(left.count()) + 1);
                    ++i;
                }
            }
            // Send what nghttp2 has.
            for (;;) {
                const uint8_t* data = nullptr;
                const nghttp2_ssize n = nghttp2_session_mem_send2(c.session, &data);
                if (n < 0) {
                    return;
                }
                if (n == 0) {
                    break;
                }
                if (!io_write_all(c, reinterpret_cast<const char*>(data), static_cast<std::size_t>(n))) {
                    return;
                }
            }
            if (nghttp2_session_want_read(c.session) == 0 && nghttp2_session_want_write(c.session) == 0) {
                return;
            }
            if (c.ssl == nullptr || SSL_pending(c.ssl) == 0) {
                pollfd p{c.fd, POLLIN, 0};
                if (::poll(&p, 1, timeout_ms) <= 0) {
                    continue;
                }
            }
            for (;;) {
                const long r = io_read(c, buf, sizeof buf);
                if (r == -2) {
                    break;
                }
                if (r <= 0) {
                    return;
                }
                if (nghttp2_session_mem_recv2(c.session, reinterpret_cast<const uint8_t*>(buf),
                                              static_cast<std::size_t>(r)) < 0) {
                    return;
                }
                if (c.close_now) {
                    return;
                }
            }
        }
    }

    static Conn& conn(void* user) { return *static_cast<Conn*>(user); }

    static int on_begin_headers(nghttp2_session*, const nghttp2_frame* frame, void* user) {
        Conn& c = conn(user);
        if (frame->hd.type == NGHTTP2_HEADERS && frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
            c.streams[frame->hd.stream_id] = std::make_unique<StreamData>();
            const int open = c.server->open_streams.fetch_add(1) + 1;
            int max = c.server->max_open_streams.load();
            while (open > max && !c.server->max_open_streams.compare_exchange_weak(max, open)) {
            }
        }
        return 0;
    }

    static int on_header(nghttp2_session*, const nghttp2_frame* frame, const uint8_t* name, std::size_t namelen,
                         const uint8_t* value, std::size_t valuelen, uint8_t, void* user) {
        Conn& c = conn(user);
        auto it = c.streams.find(frame->hd.stream_id);
        if (it == c.streams.end()) {
            return 0;
        }
        const std::string n(reinterpret_cast<const char*>(name), namelen);
        const std::string v(reinterpret_cast<const char*>(value), valuelen);
        if (n == ":method") {
            it->second->method = v;
        } else if (n == ":path") {
            it->second->path = v;
        }
        it->second->headers.emplace_back(n, v);
        return 0;
    }

    static int on_data(nghttp2_session*, uint8_t, int32_t stream_id, const uint8_t* data, std::size_t len,
                       void* user) {
        Conn& c = conn(user);
        if (auto it = c.streams.find(stream_id); it != c.streams.end()) {
            it->second->body.append(reinterpret_cast<const char*>(data), len);
        }
        return 0;
    }

    static int on_frame_recv(nghttp2_session*, const nghttp2_frame* frame, void* user) {
        Conn& c = conn(user);
        switch (frame->hd.type) {
            case NGHTTP2_HEADERS:
                if (frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
                    c.server->requests.fetch_add(1);
                    auto it = c.streams.find(frame->hd.stream_id);
                    if (it != c.streams.end()) {
                        std::lock_guard g(c.server->mu_);
                        c.server->last_headers_ = it->second->headers;
                        c.server->last_path_ = it->second->path;
                    }
                }
                if ((frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0) {
                    c.server->handle(c, frame->hd.stream_id);
                }
                break;
            case NGHTTP2_DATA:
                if ((frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0) {
                    c.server->handle(c, frame->hd.stream_id);
                }
                break;
            case NGHTTP2_PING:
                if ((frame->hd.flags & NGHTTP2_FLAG_ACK) == 0) {
                    c.server->pings.fetch_add(1);
                }
                break;
            case NGHTTP2_RST_STREAM:
                c.server->rst_any.fetch_add(1);
                if (frame->rst_stream.error_code == NGHTTP2_CANCEL) {
                    c.server->rst_cancel.fetch_add(1);
                }
                break;
            default: break;
        }
        return 0;
    }

    static int on_stream_close(nghttp2_session*, int32_t stream_id, uint32_t, void* user) {
        Conn& c = conn(user);
        if (c.streams.erase(stream_id) != 0) {
            c.server->open_streams.fetch_sub(1);
        }
        return 0;
    }

    static std::uint64_t param(std::string_view path, std::string_view key) {
        const std::size_t q = path.find('?');
        if (q == std::string_view::npos) {
            return 0;
        }
        std::string_view query = path.substr(q + 1);
        while (!query.empty()) {
            const std::size_t amp = query.find('&');
            const std::string_view kv = query.substr(0, amp);
            if (kv.size() > key.size() && kv.substr(0, key.size()) == key && kv[key.size()] == '=') {
                return std::strtoull(std::string(kv.substr(key.size() + 1)).c_str(), nullptr, 10);
            }
            if (amp == std::string_view::npos) {
                break;
            }
            query.remove_prefix(amp + 1);
        }
        return 0;
    }

    static nghttp2_ssize read_out(nghttp2_session*, int32_t, uint8_t* buf, std::size_t length, uint32_t* flags,
                                  nghttp2_data_source* source, void*) {
        auto* s = static_cast<StreamData*>(source->ptr);
        if (s->gen_left > 0) {
            const std::size_t n = std::min<std::uint64_t>(length, s->gen_left);
            for (std::size_t i = 0; i < n; ++i) {
                buf[i] = static_cast<uint8_t>('a' + (s->gen_left - i) % 26);
            }
            s->gen_left -= n;
            if (s->gen_left == 0) {
                *flags |= NGHTTP2_DATA_FLAG_EOF;
            }
            return static_cast<nghttp2_ssize>(n);
        }
        const std::size_t n = std::min(length, s->out.size() - s->out_pos);
        std::memcpy(buf, s->out.data() + s->out_pos, n);
        s->out_pos += n;
        if (s->out_pos == s->out.size()) {
            *flags |= NGHTTP2_DATA_FLAG_EOF;
        }
        return static_cast<nghttp2_ssize>(n);
    }

    void respond(Conn& c, std::int32_t id, int status, std::vector<std::pair<std::string, std::string>> extra,
                 std::string body, std::uint64_t generate = 0) {
        auto it = c.streams.find(id);
        if (it == c.streams.end()) {
            return;
        }
        StreamData& s = *it->second;
        s.out = std::move(body);
        s.out_pos = 0;
        s.gen_left = generate;
        const std::string code = std::to_string(status);
        std::vector<nghttp2_nv> nv;
        auto add = [&nv](const std::string& n, const std::string& v) {
            nv.push_back(nghttp2_nv{reinterpret_cast<uint8_t*>(const_cast<char*>(n.data())),
                                    reinterpret_cast<uint8_t*>(const_cast<char*>(v.data())), n.size(), v.size(),
                                    NGHTTP2_NV_FLAG_NONE});
        };
        const std::string status_name = ":status";
        add(status_name, code);
        for (const auto& [n, v] : extra) {
            add(n, v);
        }
        nghttp2_data_provider2 prd{};
        prd.source.ptr = &s;
        prd.read_callback = &read_out;
        const bool has_body = !s.out.empty() || generate > 0;
        nghttp2_submit_response2(c.session, id, nv.data(), nv.size(), has_body ? &prd : nullptr);
    }

    void handle(Conn& c, std::int32_t id) {
        auto it = c.streams.find(id);
        if (it == c.streams.end()) {
            return;
        }
        StreamData& s = *it->second;
        const std::string_view path = s.path;
        const std::string_view route = path.substr(0, path.find('?'));
        if (route == "/echo") {
            respond(c, id, 200, {}, s.body);
        } else if (route == "/delay") {
            c.delayed.push_back({std::chrono::steady_clock::now() + std::chrono::milliseconds(param(path, "ms")), id});
        } else if (route == "/hang") {
            // never answers
        } else if (route == "/big") {
            respond(c, id, 200, {}, {}, param(path, "n"));
        } else if (route == "/status") {
            const int code = static_cast<int>(param(path, "code"));
            respond(c, id, code, {}, "{\"reason\":\"Status" + std::to_string(code) + "\"}");
        } else if (route == "/bigheader") {
            respond(c, id, 200, {{"x-big", std::string(param(path, "n"), 'h')}}, "ok");
        } else if (route == "/goaway") {
            nghttp2_submit_goaway(c.session, NGHTTP2_FLAG_NONE, static_cast<std::int32_t>(param(path, "last")),
                                  NGHTTP2_NO_ERROR, nullptr, 0);
        } else if (route == "/close") {
            c.close_now = true;
        } else if (route.substr(0, 10) == "/3/device/") {
            respond_apns(c, id, s, route.substr(10));
        } else {
            respond(c, id, 404, {}, "not found");
        }
    }

    static std::string header_of(const StreamData& s, std::string_view name) {
        for (const auto& [n, v] : s.headers) {
            if (n == name) {
                return v;
            }
        }
        return {};
    }

    // APNs: POST only; a bearer token required; the device token picks the
    // answer.
    void respond_apns(Conn& c, std::int32_t id, const StreamData& s, std::string_view token) {
        const std::string apns_id = [&] {
            std::string v = header_of(s, "apns-id");
            return v.empty() ? std::string("8f6c1b56-0000-4000-8000-000000000001") : v;
        }();
        auto reply = [&](int status, std::string reason, std::string extra = {}) {
            std::string body = reason.empty() ? std::string() : "{\"reason\":\"" + reason + "\"" + extra + "}";
            respond(c, id, status, {{"apns-id", apns_id}}, std::move(body));
        };
        if (s.method != "POST") {
            return reply(405, "MethodNotAllowed");
        }
        if (header_of(s, "authorization").rfind("bearer ", 0) != 0) {
            return reply(403, "MissingProviderToken");
        }
        if (header_of(s, "apns-topic").empty()) {
            return reply(400, "MissingTopic");
        }
        if (token == "bad") {
            return reply(400, "BadDeviceToken");
        }
        if (token == "gone") {
            return reply(410, "Unregistered", ",\"timestamp\":1700000000123");
        }
        if (token == "busy") {
            return reply(429, "TooManyRequests");
        }
        if (token == "boom") {
            return reply(500, "InternalServerError");
        }
        if (token == "down") {
            return reply(503, "ServiceUnavailable");
        }
        if (token == "big") {
            return reply(413, "PayloadTooLarge");
        }
        if (token == "expired-once" && expired_once.fetch_add(1) == 0) {
            return reply(403, "ExpiredProviderToken");
        }
        reply(200, "");
    }

    MockOptions options_;
    int listen_fd_ = -1;
    std::uint16_t port_ = 0;
    std::atomic<bool> stopping_{false};
    std::thread accept_thread_;
    std::mutex mu_;
    std::vector<std::thread> threads_;
    std::vector<std::pair<int, std::shared_ptr<std::atomic<bool>>>> live_;
    std::vector<std::pair<std::string, std::string>> last_headers_;
    std::string last_path_;
};

// A server SSL_CTX for the mock: cert / key PEM files, ALPN "h2" chosen.
inline SSL_CTX* make_mock_tls(const std::string& cert, const std::string& key) {
    SSL_CTX* ctx = SSL_CTX_new(TLS_server_method());
    if (SSL_CTX_use_certificate_chain_file(ctx, cert.c_str()) != 1 ||
        SSL_CTX_use_PrivateKey_file(ctx, key.c_str(), SSL_FILETYPE_PEM) != 1) {
        SSL_CTX_free(ctx);
        return nullptr;
    }
    SSL_CTX_set_alpn_select_cb(
        ctx,
        [](SSL*, const unsigned char** out, unsigned char* outlen, const unsigned char* in, unsigned int inlen,
           void*) -> int {
            static const unsigned char kH2[] = {2, 'h', '2'};
            unsigned char* selected = nullptr;
            if (SSL_select_next_proto(&selected, outlen, kH2, sizeof kH2, in, inlen) == OPENSSL_NPN_NEGOTIATED) {
                *out = selected;
                return SSL_TLSEXT_ERR_OK;
            }
            return SSL_TLSEXT_ERR_ALERT_FATAL;
        },
        nullptr);
    return ctx;
}

}  // namespace h2test
