#pragma once

// http2::Request / http2::Response and the client's error codes.
//
// Errors (int, errno values; never exceptions). 0: a response arrived (any
// :status, also 4xx / 5xx). Otherwise:
//
//   ETIMEDOUT     the request's deadline passed. If the stream was open, it
//                 was reset with RST_STREAM(CANCEL); the server may or may
//                 not have processed it.
//   EAGAIN        the server did NOT process the request: refused before it
//                 was sent or processed (GOAWAY with a last_stream_id below
//                 its stream, RST_STREAM(REFUSED_STREAM), or the connection
//                 started going away just before it was sent). Safe to
//                 retry, also for non-idempotent requests (RFC 9113 8.7):
//                 retryable(EAGAIN) == true. The next request opens a new
//                 connection if needed.
//   EBUSY         too many streams: the peer's SETTINGS_MAX_CONCURRENT_STREAMS
//                 is reached and the client is configured to fail
//                 (StreamLimit::kFail), or the bounded queue behind it
//                 (ClientOptions::max_queued_requests) is full. Nothing was
//                 sent.
//   ECONNRESET    the connection was lost (transport error / end of stream,
//                 PING keepalive unanswered) while the request was in
//                 flight; it may have been processed.
//   ECONNABORTED  the server reset the stream (RST_STREAM other than
//                 REFUSED_STREAM): Response::reset_code() is its HTTP/2
//                 error code.
//   EPROTO        HTTP/2 protocol error on the connection (the connection
//                 is closed), a malformed response (no / bad :status), or
//                 the TLS peer did not negotiate "h2" via ALPN
//                 (ENOPROTOOPT, see below).
//   ENOPROTOOPT   TLS handshake done, but ALPN did not select "h2".
//   EMSGSIZE      the response exceeded max_response_header_bytes or
//                 max_response_body_bytes (the stream was reset with
//                 RST_STREAM(CANCEL)).
//   EINVAL        an invalid request: more than kMaxRequestHeaders headers,
//                 a pseudo-header among the headers, a name / value with
//                 bytes HTTP/2 forbids (CR, LF, NUL, upper case names, ...).
//   ECANCELED     the client was closed (close()) meanwhile / before.
//   anything else: connecting failed (net::resolve / connect / TLS errors as
//                 is: ECONNREFUSED, ENOENT for an unknown host, EPROTO for a
//                 failed TLS handshake, ETIMEDOUT for connect_timeout, ...).

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "fibers/poller/sources.hpp"

namespace fibers::http2 {

struct Header {
    std::string_view name;
    std::string_view value;
};

// Whether a request that failed with `error` may be sent again as is (the
// server did not process it).
constexpr bool retryable(int error) noexcept { return error == EAGAIN; }

// Headers per request (besides the pseudo-headers and content-length): the
// header array is built on the caller's stack.
inline constexpr std::size_t kMaxRequestHeaders = 64;

struct Request {
    std::string_view method = "GET";
    // :path (with the query).
    std::string_view path = "/";
    // :authority; empty: ClientOptions::authority (default host[:port]).
    std::string_view authority;
    // Lower-case names (HTTP/2); no pseudo-headers, no connection-specific
    // fields (connection, keep-alive, transfer-encoding, upgrade, ...).
    std::span<const Header> headers;
    // Sent as DATA (copied by nghttp2 into its frames as flow control
    // allows); must stay valid until request() returns, which it does
    // (request() returns only after the stream let go of it). A non-empty
    // body gets a content-length header unless `headers` has one.
    std::string_view body;
    // Relative timeout; zero: ClientOptions::request_timeout; negative:
    // none.
    poller::Duration timeout = poller::Duration::zero();
    // Absolute deadline (Scheduler::now()'s clock); takes precedence over
    // `timeout` when set (not TimePoint{}).
    poller::TimePoint deadline{};
};

// What came back. Reusable: request() clears it and keeps the capacity of
// its buffers (a reused Response does not allocate once warm).
class Response {
public:
    // :status (0 if the request failed).
    int status() const noexcept { return status_; }

    // Response headers, then trailers (lower-case names, in arrival order).
    // Valid until the Response is reused or destroyed.
    std::span<const Header> headers() const noexcept { return headers_; }

    // The first header named `name` (ASCII case-insensitive), "" if none.
    std::string_view header(std::string_view name) const noexcept {
        for (const Header& h : headers_) {
            if (equal_ci(h.name, name)) {
                return h.value;
            }
        }
        return {};
    }

    std::string_view body() const noexcept { return body_; }

    // ECONNABORTED: the server's RST_STREAM error code (RFC 9113 7).
    std::uint32_t reset_code() const noexcept { return reset_code_; }
    // The HTTP/2 stream the request used (0: none).
    std::int32_t stream_id() const noexcept { return stream_id_; }

    // Pre-sizes the buffers (optional).
    void reserve(std::size_t header_bytes, std::size_t body_bytes) noexcept {
        storage_.reserve(header_bytes);
        body_.reserve(body_bytes);
    }

    void clear() noexcept {
        status_ = 0;
        storage_.clear();
        spans_.clear();
        headers_.clear();
        body_.clear();
        reset_code_ = 0;
        stream_id_ = 0;
    }

private:
    friend struct ResponseWriter;

    struct Span {
        std::size_t name_at, name_len, value_at, value_len;
    };

    static bool equal_ci(std::string_view a, std::string_view b) noexcept {
        if (a.size() != b.size()) {
            return false;
        }
        for (std::size_t i = 0; i < a.size(); ++i) {
            char x = a[i];
            char y = b[i];
            x = (x >= 'A' && x <= 'Z') ? static_cast<char>(x + 32) : x;
            y = (y >= 'A' && y <= 'Z') ? static_cast<char>(y + 32) : y;
            if (x != y) {
                return false;
            }
        }
        return true;
    }

    int status_ = 0;
    std::string storage_;       // header names and values, back to back
    std::vector<Span> spans_;   // offsets into storage_ (it may reallocate while receiving)
    std::vector<Header> headers_;
    std::string body_;
    std::uint32_t reset_code_ = 0;
    std::int32_t stream_id_ = 0;
};

// The session's write access to a Response (internal). Allocation failure
// terminates (noexcept), like everywhere in the library.
struct ResponseWriter {
    static void set_status(Response& r, int status) noexcept { r.status_ = status; }
    static void set_stream_id(Response& r, std::int32_t id) noexcept { r.stream_id_ = id; }
    static void set_reset_code(Response& r, std::uint32_t code) noexcept { r.reset_code_ = code; }
    static std::size_t header_bytes(const Response& r) noexcept { return r.storage_.size(); }
    static void add_header(Response& r, std::string_view name, std::string_view value) noexcept {
        const std::size_t at = r.storage_.size();
        r.storage_.append(name);
        r.storage_.append(value);
        r.spans_.push_back(Response::Span{at, name.size(), at + name.size(), value.size()});
    }
    static std::size_t body_size(const Response& r) noexcept { return r.body_.size(); }
    static void append_body(Response& r, const void* data, std::size_t n) noexcept {
        r.body_.append(static_cast<const char*>(data), n);
    }
    // Builds the header views (storage_ does not change afterwards).
    static void finish(Response& r) noexcept {
        r.headers_.clear();
        r.headers_.reserve(r.spans_.size());
        for (const Response::Span& s : r.spans_) {
            r.headers_.push_back(Header{std::string_view(r.storage_.data() + s.name_at, s.name_len),
                                        std::string_view(r.storage_.data() + s.value_at, s.value_len)});
        }
    }
};

}  // namespace fibers::http2
