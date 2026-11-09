#pragma once

// http::ServerOptions: limits, timeouts and buffer sizes of the server.

#include <chrono>
#include <cstddef>
#include <cstdint>

#include "fibers/poller/sources.hpp"

namespace fibers::http {

struct ServerOptions {
    // ---- limits (violations get a response, then the connection closes) ----

    // Request line + header fields (+ the blank line). Exceeded: 414 if the
    // request line is not complete yet, else 431.
    std::size_t max_header_bytes = 8 * 1024;
    // Header fields per request. Exceeded: 431.
    std::size_t max_headers = 100;
    // Request body (Content-Length, or the sum of the chunks). Exceeded: 413
    // (for Content-Length before the body is read).
    std::size_t max_body_bytes = 1024 * 1024;
    // Query parameters Request::query() decodes; later ones are ignored.
    std::size_t max_query_params = 64;
    // Header lines a handler may add to one response (Response::header
    // returns false beyond).
    std::size_t max_response_header_bytes = 4 * 1024;

    // ---- timeouts (socket deadlines; see "Timeouts" in connection.hpp) ------

    // From accepting a connection, or from the first byte of a later request,
    // to the end of the request's headers (slowloris). Expired after some
    // bytes: 408, close. Before any byte: close.
    poller::Duration header_timeout = std::chrono::seconds(10);
    // Keep-alive: waiting for the next request. Expired: close.
    poller::Duration idle_timeout = std::chrono::seconds(60);
    // Waiting for more body bytes (an inactivity timeout: re-armed before
    // every read of the body). Expired: 408, close.
    poller::Duration body_timeout = std::chrono::seconds(30);
    // Each write of response data. Expired: close.
    poller::Duration write_timeout = std::chrono::seconds(30);
    // After an error response the server half-closes and drains what the
    // client still sends for at most this long, so that the client reads
    // the response instead of a reset.
    poller::Duration linger_timeout = std::chrono::milliseconds(500);

    // ---- buffers (per connection, allocated once per pooled connection) -----

    // Read buffer; at least max_header_bytes + 4 KiB. Holds the request head,
    // pipelined requests and bodies that fit; larger bodies (up to
    // max_body_bytes) go to a pooled body buffer.
    std::size_t read_buffer_bytes = 16 * 1024;
    // Write buffer (response heads, small bodies, pipelined responses); at
    // least max_response_header_bytes + 1 KiB.
    std::size_t write_buffer_bytes = 16 * 1024;

    // ---- server ------------------------------------------------------------

    // Connections served at once; the accept loop waits while at the limit.
    std::size_t max_connections = 10000;
    // TCP_NODELAY on accepted connections.
    bool tcp_nodelay = true;
};

namespace detail {

// The effective buffer sizes (minimums applied).
struct BufferSizes {
    std::size_t in;
    std::size_t out;
    std::size_t response_headers;
    std::size_t scratch;
    std::size_t headers;
    std::size_t query;

    static BufferSizes of(const ServerOptions& o) noexcept {
        BufferSizes s{};
        s.in = o.read_buffer_bytes < o.max_header_bytes + 4096 ? o.max_header_bytes + 4096 : o.read_buffer_bytes;
        s.response_headers = o.max_response_header_bytes;
        s.out = o.write_buffer_bytes < s.response_headers + 1024 ? s.response_headers + 1024 : o.write_buffer_bytes;
        s.scratch = o.max_header_bytes;
        s.headers = o.max_headers;
        s.query = o.max_query_params;
        return s;
    }
};

}  // namespace detail

}  // namespace fibers::http
