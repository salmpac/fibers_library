#pragma once

// A small HTTP/1.1 client for fibers.
//
//   http::Client client(scheduler, net::Endpoint::loopback_v4(8080));   // numeric endpoint
//   http::ClientResponse res;
//   if (client.get("/hello", res) == 0 && res.status() == 200) { use(res.body()); }
//   client.request({.method = http::Method::kPost, .target = "/items", .body = json}, res);
//
// BasicClientConnection<S>: one HTTP/1.1 connection over any connected
// ByteStream S (plain TCP, TLS, ...): sends a request, reads the response
// (Content-Length, chunked, or until the server closes), keeps the
// connection for the next request when both sides allow it.
// BasicClient: the same over net::Stream, connecting to a numeric Endpoint
// on demand and reconnecting when the server closed the connection; a
// request on a reused connection that the server had closed meanwhile (no
// response byte at all) is retried once on a fresh connection if its method
// is idempotent.
//
// Not covered: connection pools, DNS, proxies, redirects, request body
// streaming, Expect: 100-continue (interim 1xx responses are skipped).
//
// Calls are from one fiber at a time (a connection carries one exchange at a
// time) and wait (from a fiber of the stream's scheduler). The client owns
// growable buffers (heap, reused across requests); ClientResponse owns its
// head and body (valid until the object is reused or destroyed).
//
// Errors (errno values, never exceptions): the stream's (ECONNREFUSED,
// ECONNRESET, ETIMEDOUT: the ClientOptions::timeout of the whole exchange
// passed, ...), EPROTO (malformed response), EMSGSIZE (a limit of
// ClientOptions exceeded), ENOTCONN (the connection cannot be reused; with
// BasicClient this does not happen). After any error the connection is not
// reusable.

#include <llhttp.h>

#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "fibers/http/message.hpp"
#include "fibers/http/stream.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/poller/sources.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fibers::http {

struct ClientOptions {
    // Response status line + headers.
    std::size_t max_header_bytes = 64 * 1024;
    std::size_t max_headers = 100;
    std::size_t max_body_bytes = 64 * 1024 * 1024;
    // The whole exchange (write the request, read the response).
    poller::Duration timeout = std::chrono::seconds(30);
    // Read buffer size.
    std::size_t read_buffer_bytes = 16 * 1024;
};

struct ClientRequest {
    Method method = Method::kGet;
    // Used instead of `method` when not empty (e.g. "PROPFIND").
    std::string_view method_name;
    std::string_view target = "/";
    // Host header; empty: the connection's default host.
    std::string_view host;
    std::span<const Header> headers;
    // Sent with Content-Length (also "Content-Length: 0" for POST / PUT /
    // PATCH without a body).
    std::string_view body;
    // Send "Connection: close" (the connection is not reused after).
    bool close = false;
};

class ClientResponse {
public:
    int status() const noexcept { return status_; }
    std::string_view reason() const noexcept { return reason_; }
    unsigned version_major() const noexcept { return major_; }
    unsigned version_minor() const noexcept { return minor_; }

    std::span<const Header> headers() const noexcept { return headers_; }
    std::string_view header(std::string_view name) const noexcept { return find_header(headers_, name); }

    std::string_view body() const noexcept { return body_; }
    // The body came chunked.
    bool chunked() const noexcept { return chunked_; }
    // The server keeps the connection open.
    bool keep_alive() const noexcept { return keep_alive_; }

private:
    template <ByteStream S>
    friend class BasicClientConnection;

    void reset() noexcept {
        status_ = 0;
        major_ = minor_ = 0;
        reason_ = {};
        reason_at_ = reason_len_ = 0;
        storage_.clear();
        spans_.clear();
        headers_.clear();
        body_.clear();
        chunked_ = false;
        keep_alive_ = false;
    }

    // Header bytes are appended to storage_ (pieces may straddle reads);
    // views are made once the head is complete (storage_ may reallocate
    // until then).
    struct Span {
        std::size_t name_at, name_len, value_at, value_len;
    };

    void finish_head() noexcept {
        headers_.clear();
        for (const Span& s : spans_) {
            std::string_view v(storage_.data() + s.value_at, s.value_len);
            while (!v.empty() && (v.back() == ' ' || v.back() == '\t')) {
                v.remove_suffix(1);
            }
            headers_.push_back(Header{std::string_view(storage_.data() + s.name_at, s.name_len), v});
        }
    }

    int status_ = 0;
    unsigned major_ = 0;
    unsigned minor_ = 0;
    std::string_view reason_;
    std::size_t reason_at_ = 0;
    std::size_t reason_len_ = 0;
    std::string storage_;
    std::vector<Span> spans_;
    std::vector<Header> headers_;
    std::string body_;
    bool chunked_ = false;
    bool keep_alive_ = false;
};

template <ByteStream S>
class BasicClientConnection {
public:
    // `stream` is connected (and stays owned by the caller). `default_host`
    // is sent as Host when a request names none.
    explicit BasicClientConnection(S& stream, ClientOptions options = {}, std::string_view default_host = "localhost")
        : stream_(&stream), options_(options), host_(default_host), in_(options.read_buffer_bytes) {}

    BasicClientConnection(const BasicClientConnection&) = delete;
    BasicClientConnection& operator=(const BasicClientConnection&) = delete;

    // Starts over on a new connection of the same stream object (after the
    // owner reconnected it).
    void reset() noexcept {
        reusable_ = true;
        responses_ = 0;
        in_len_ = 0;
    }

    // One exchange. 0 or errno (see the top comment).
    int request(const ClientRequest& req, ClientResponse& res) noexcept {
        res.reset();
        if (!reusable_) {
            return ENOTCONN;
        }
        reusable_ = false;  // until the exchange completes cleanly
        got_bytes_ = false;
        const poller::TimePoint deadline = std::chrono::steady_clock::now() + options_.timeout;
        static_cast<void>(stream_->set_write_deadline(deadline));
        static_cast<void>(stream_->set_read_deadline(deadline));
        if (const int e = send(req); e != 0) {
            return e;
        }
        head_request_ = req.method == Method::kHead && req.method_name.empty();
        if (const int e = receive(res); e != 0) {
            return e;
        }
        ++responses_;
        reusable_ = res.keep_alive_ && !req.close && in_len_ == 0;
        return 0;
    }

    int get(std::string_view target, ClientResponse& res) noexcept {
        ClientRequest req;
        req.target = target;
        return request(req, res);
    }

    // Whether the next request can go over this connection.
    bool reusable() const noexcept { return reusable_; }
    // Responses received on this connection.
    std::uint64_t responses() const noexcept { return responses_; }
    // The last failed exchange received no byte of a response (a stale
    // keep-alive connection, for instance).
    bool failed_before_response() const noexcept { return !got_bytes_; }

private:
    int send(const ClientRequest& req) noexcept {
        out_.clear();
        out_.append(req.method_name.empty() ? method_name(req.method) : req.method_name);
        out_.push_back(' ');
        out_.append(req.target.empty() ? std::string_view("/") : req.target);
        out_.append(" HTTP/1.1\r\nHost: ");
        out_.append(req.host.empty() ? std::string_view(host_) : req.host);
        out_.append("\r\n");
        for (const Header& h : req.headers) {
            if (!detail::is_token(h.name) || !detail::safe_field_value(h.value)) {
                return EINVAL;
            }
            out_.append(h.name);
            out_.append(": ");
            out_.append(h.value);
            out_.append("\r\n");
        }
        const bool wants_length = !req.body.empty() || req.method == Method::kPost || req.method == Method::kPut ||
                                  req.method == Method::kPatch;
        if (wants_length) {
            char digits[24];
            const auto r = std::to_chars(digits, digits + sizeof digits, req.body.size());
            out_.append("Content-Length: ");
            out_.append(digits, static_cast<std::size_t>(r.ptr - digits));
            out_.append("\r\n");
        }
        if (req.close) {
            out_.append("Connection: close\r\n");
        }
        out_.append("\r\n");
        if (req.body.size() <= 16 * 1024) {
            out_.append(req.body);
            return stream_->write_all(out_.data(), out_.size()).error;
        }
        if (const net::IoResult r = stream_->write_all(out_.data(), out_.size()); !r) {
            return r.error;
        }
        return stream_->write_all(req.body.data(), req.body.size()).error;
    }

    int receive(ClientResponse& res) noexcept {
        res_ = &res;
        llhttp_init(&parser_, HTTP_RESPONSE, &settings());
        parser_.data = this;
        error_ = 0;
        done_ = false;
        head_bytes_ = 0;
        std::size_t parse_pos = 0;
        for (;;) {
            if (parse_pos < in_len_) {
                const llhttp_errno_t err = llhttp_execute(&parser_, in_.data() + parse_pos, in_len_ - parse_pos);
                if (err == HPE_PAUSED) {
                    parse_pos = static_cast<std::size_t>(llhttp_get_error_pos(&parser_) - in_.data());
                } else if (err != HPE_OK) {
                    return error_ != 0 ? error_ : EPROTO;
                } else {
                    parse_pos = in_len_;
                }
                if (done_) {
                    // Bytes after the response (none expected): keep them,
                    // the connection is then not reused.
                    std::memmove(in_.data(), in_.data() + parse_pos, in_len_ - parse_pos);
                    in_len_ -= parse_pos;
                    res.keep_alive_ = res.keep_alive_ && llhttp_should_keep_alive(&parser_) != 0;
                    return 0;
                }
                // Everything consumed (header bytes were copied out).
                in_len_ = parse_pos = 0;
            }
            const net::IoResult r = stream_->read_some(in_.data() + in_len_, in_.size() - in_len_);
            if (!r) {
                return r.error;
            }
            if (r.bytes == 0) {
                if (got_bytes_ && llhttp_message_needs_eof(&parser_) != 0) {
                    const llhttp_errno_t err = llhttp_finish(&parser_);
                    if ((err == HPE_OK || err == HPE_PAUSED) && done_) {
                        res.keep_alive_ = false;
                        return 0;
                    }
                }
                return got_bytes_ ? EPROTO : ECONNRESET;
            }
            got_bytes_ = true;
            in_len_ += r.bytes;
        }
    }

    // ---- llhttp callbacks ---------------------------------------------------

    static BasicClientConnection& self(llhttp_t* p) noexcept { return *static_cast<BasicClientConnection*>(p->data); }

    int count_head(std::size_t n) noexcept {
        head_bytes_ += n;
        if (head_bytes_ > options_.max_header_bytes) {
            error_ = EMSGSIZE;
            return HPE_USER;
        }
        return 0;
    }

    static int on_status(llhttp_t* p, const char* at, std::size_t n) noexcept {
        BasicClientConnection& c = self(p);
        ClientResponse& r = *c.res_;
        if (r.reason_len_ == 0) {
            r.reason_at_ = r.storage_.size();
        }
        r.storage_.append(at, n);
        r.reason_len_ += n;
        return c.count_head(n);
    }

    static int on_header_field(llhttp_t* p, const char* at, std::size_t n) noexcept {
        BasicClientConnection& c = self(p);
        ClientResponse& r = *c.res_;
        if (c.in_body_) {
            return 0;
        }
        if (!c.field_open_) {
            if (r.spans_.size() == c.options_.max_headers) {
                c.error_ = EMSGSIZE;
                return HPE_USER;
            }
            r.spans_.push_back({r.storage_.size(), 0, 0, 0});
            c.field_open_ = true;
            c.value_open_ = false;
        }
        r.storage_.append(at, n);
        r.spans_.back().name_len += n;
        return c.count_head(n);
    }

    static int on_header_field_complete(llhttp_t* p) noexcept {
        self(p).field_open_ = false;
        return 0;
    }

    static int on_header_value(llhttp_t* p, const char* at, std::size_t n) noexcept {
        BasicClientConnection& c = self(p);
        ClientResponse& r = *c.res_;
        if (c.in_body_ || r.spans_.empty()) {
            return 0;
        }
        if (!c.value_open_) {
            r.spans_.back().value_at = r.storage_.size();
            c.value_open_ = true;
        }
        r.storage_.append(at, n);
        r.spans_.back().value_len += n;
        return c.count_head(n);
    }

    static int on_header_value_complete(llhttp_t* p) noexcept {
        self(p).value_open_ = false;
        return 0;
    }

    static int on_message_begin(llhttp_t* p) noexcept {
        BasicClientConnection& c = self(p);
        c.res_->reset();
        c.in_body_ = false;
        c.field_open_ = false;
        c.value_open_ = false;
        c.head_bytes_ = 0;
        return 0;
    }

    static int on_headers_complete(llhttp_t* p) noexcept {
        BasicClientConnection& c = self(p);
        ClientResponse& r = *c.res_;
        r.status_ = p->status_code;
        r.major_ = p->http_major;
        r.minor_ = p->http_minor;
        r.reason_ = std::string_view(r.storage_.data() + r.reason_at_, r.reason_len_);
        r.chunked_ = (p->flags & F_CHUNKED) != 0;
        r.finish_head();
        c.in_body_ = true;
        if ((p->flags & F_CONTENT_LENGTH) != 0 && p->content_length > c.options_.max_body_bytes) {
            c.error_ = EMSGSIZE;
            return -1;
        }
        // HEAD: the response has headers only (1 = no body).
        return c.head_request_ && p->status_code >= 200 ? 1 : 0;
    }

    static int on_body(llhttp_t* p, const char* at, std::size_t n) noexcept {
        BasicClientConnection& c = self(p);
        if (c.res_->body_.size() + n > c.options_.max_body_bytes) {
            c.error_ = EMSGSIZE;
            return HPE_USER;
        }
        c.res_->body_.append(at, n);
        return 0;
    }

    static int on_message_complete(llhttp_t* p) noexcept {
        BasicClientConnection& c = self(p);
        const int status = p->status_code;
        if (status >= 100 && status < 200 && status != 101) {
            return 0;  // interim response: the final one follows
        }
        c.res_->keep_alive_ = llhttp_should_keep_alive(p) != 0;
        c.done_ = true;
        return HPE_PAUSED;
    }

    static const llhttp_settings_t& settings() noexcept {
        static const llhttp_settings_t s = [] {
            llhttp_settings_t x;
            llhttp_settings_init(&x);
            x.on_message_begin = &on_message_begin;
            x.on_status = &on_status;
            x.on_header_field = &on_header_field;
            x.on_header_field_complete = &on_header_field_complete;
            x.on_header_value = &on_header_value;
            x.on_header_value_complete = &on_header_value_complete;
            x.on_headers_complete = &on_headers_complete;
            x.on_body = &on_body;
            x.on_message_complete = &on_message_complete;
            return x;
        }();
        return s;
    }

    S* stream_;
    ClientOptions options_;
    std::string host_;
    std::vector<char> in_;
    std::size_t in_len_ = 0;
    std::string out_;
    llhttp_t parser_{};
    ClientResponse* res_ = nullptr;
    std::size_t head_bytes_ = 0;
    std::uint64_t responses_ = 0;
    int error_ = 0;
    bool reusable_ = true;
    bool got_bytes_ = false;
    bool head_request_ = false;
    bool in_body_ = false;
    bool field_open_ = false;
    bool value_open_ = false;
    bool done_ = false;
};

// A client over plain TCP to one numeric endpoint (see the top comment).
template <net::NetScheduler Sched = sched::Scheduler>
class BasicClient {
public:
    using Stream = net::BasicStream<Sched>;

    BasicClient(Sched& scheduler, const net::Endpoint& peer, ClientOptions options = {})
        : scheduler_(scheduler), peer_(peer), connection_(stream_, options, host_of(peer)) {}

    BasicClient(const BasicClient&) = delete;
    BasicClient& operator=(const BasicClient&) = delete;

    // One exchange (connects first if needed). 0 or errno.
    int request(const ClientRequest& req, ClientResponse& res) noexcept {
        for (int attempt = 0;; ++attempt) {
            const bool reused = connected_ && connection_.responses() != 0;
            if (const int e = ensure_connected(); e != 0) {
                return e;
            }
            const int e = connection_.request(req, res);
            if (e == 0) {
                if (!connection_.reusable()) {
                    close();
                }
                return 0;
            }
            close();
            // A keep-alive connection the server closed just before our
            // request: retry once if that is safe.
            if (attempt == 0 && reused && connection_.failed_before_response() && idempotent(req) &&
                (e == ECONNRESET || e == EPIPE)) {
                continue;
            }
            return e;
        }
    }

    int get(std::string_view target, ClientResponse& res) noexcept {
        ClientRequest req;
        req.target = target;
        return request(req, res);
    }

    int post(std::string_view target, std::string_view body, ClientResponse& res,
             std::string_view content_type = "application/octet-stream") noexcept {
        const Header headers[] = {{"Content-Type", content_type}};
        ClientRequest req;
        req.method = Method::kPost;
        req.target = target;
        req.body = body;
        req.headers = headers;
        return request(req, res);
    }

    // Closes the connection (the next request opens a new one).
    void close() noexcept {
        if (connected_) {
            static_cast<void>(stream_.close());
            connected_ = false;
        }
    }

    bool connected() const noexcept { return connected_; }
    // TCP connections opened so far.
    std::uint64_t connects() const noexcept { return connects_; }

private:
    static std::string host_of(const net::Endpoint& e) {
        char text[64] = {};
        if (e.family() == AF_INET) {
            ::inet_ntop(AF_INET, &reinterpret_cast<const sockaddr_in*>(&e.storage)->sin_addr, text, sizeof text);
        } else if (e.family() == AF_INET6) {
            text[0] = '[';
            ::inet_ntop(AF_INET6, &reinterpret_cast<const sockaddr_in6*>(&e.storage)->sin6_addr, text + 1,
                        sizeof text - 2);
            std::strcat(text, "]");
        }
        std::string host(text);
        host += ':';
        host += std::to_string(e.port());
        return host;
    }

    static bool idempotent(const ClientRequest& req) noexcept {
        if (!req.method_name.empty()) {
            return false;
        }
        switch (req.method) {
            case Method::kGet:
            case Method::kHead:
            case Method::kPut:
            case Method::kDelete:
            case Method::kOptions:
            case Method::kTrace: return true;
            default: return false;
        }
    }

    int ensure_connected() noexcept {
        if (connected_) {
            return 0;
        }
        if (const int e = stream_.connect(scheduler_, peer_); e != 0) {
            return e;
        }
        static_cast<void>(stream_.set_nodelay(true));
        connected_ = true;
        ++connects_;
        connection_.reset();
        return 0;
    }

    Sched& scheduler_;
    net::Endpoint peer_;
    Stream stream_;
    BasicClientConnection<Stream> connection_;
    bool connected_ = false;
    std::uint64_t connects_ = 0;
};

using ClientConnection = BasicClientConnection<net::Stream>;
using Client = BasicClient<sched::Scheduler>;

}  // namespace fibers::http
