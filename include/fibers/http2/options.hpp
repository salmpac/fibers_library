#pragma once

// http2::ClientOptions: where to connect, limits, timeouts, buffers.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "fibers/net/socket.hpp"
#include "fibers/poller/sources.hpp"
#include "fibers/tls/context.hpp"

namespace fibers::http2 {

// What a request does when the peer's SETTINGS_MAX_CONCURRENT_STREAMS
// streams are open already.
enum class StreamLimit : std::uint8_t {
    // Wait in a bounded queue (inside nghttp2) for a stream to finish;
    // beyond max_queued_requests waiting: EBUSY.
    kQueue,
    // Fail at once with EBUSY.
    kFail,
};

struct ClientOptions {
    // ---- where ---------------------------------------------------------------

    // DNS name or numeric address: resolved (net::resolve) on every connect
    // unless `endpoint` is set; the TLS server name (SNI + certificate
    // check); the default :authority.
    std::string host = "localhost";
    std::uint16_t port = 443;
    // Connect here instead of resolving `host` (tests, pinned addresses).
    std::optional<net::Endpoint> endpoint;
    // TLS with ALPN "h2" when set: the context must be a client context
    // that offers "h2" (ClientOptions::alpn); a server that selects
    // anything else fails the connect with ENOPROTOOPT. nullptr: h2c with
    // prior knowledge (cleartext HTTP/2, RFC 9113 3.3; tests, sidecars).
    // Not owned; must outlive the client.
    tls::TlsContext* tls = nullptr;
    // :authority of requests that name none; empty: host, plus ":port"
    // unless it is the scheme's default (443 / 80).
    std::string authority;

    // ---- streams and limits ----------------------------------------------------

    StreamLimit stream_limit = StreamLimit::kQueue;
    // kQueue: requests waiting for a stream beyond the peer's limit.
    std::size_t max_queued_requests = 1024;
    // The peer's SETTINGS_MAX_CONCURRENT_STREAMS assumed until its SETTINGS
    // arrive (RFC 9113 says unlimited; 100 is what servers commonly send).
    std::uint32_t assumed_max_concurrent_streams = 100;
    // Response header block (names + values + 32 per field, RFC 9113
    // 6.5.2), also advertised as SETTINGS_MAX_HEADER_LIST_SIZE.
    std::size_t max_response_header_bytes = 16 * 1024;
    // Response body per stream (the Response grows up to this).
    std::size_t max_response_body_bytes = 1024 * 1024;
    // SETTINGS_INITIAL_WINDOW_SIZE (per-stream receive window) and the
    // connection receive window. nghttp2 sends WINDOW_UPDATEs as the data is
    // consumed (into the Response), so a stream never buffers more than its
    // window here, and a slow stream does not stall the others beyond the
    // connection window.
    std::uint32_t initial_window_size = 1u << 20;
    std::int32_t connection_window_size = 16 << 20;

    // ---- timeouts ------------------------------------------------------------

    // TCP connect + TLS handshake (also bounded by the request's deadline).
    poller::Duration connect_timeout = std::chrono::seconds(10);
    // Default per-request timeout (Request::timeout zero); zero: none.
    poller::Duration request_timeout = std::chrono::seconds(30);
    // PING keepalive: a PING every ping_interval while the connection is
    // open (zero: off); no ACK within ping_timeout -> the connection is
    // dropped (in-flight requests: ECONNRESET; the next one reconnects).
    poller::Duration ping_interval = poller::Duration::zero();
    poller::Duration ping_timeout = std::chrono::seconds(10);
    // close(): how long the GOAWAY may take to go out.
    poller::Duration close_flush_timeout = std::chrono::seconds(1);

    // ---- buffers (per connection, heap, allocated at connect) ----------------

    // Bytes per read from the transport.
    std::size_t read_buffer_bytes = 32 * 1024;
    // The writer serializes up to about this much per write_all.
    std::size_t write_chunk_bytes = 64 * 1024;

    // user-agent header ("" none).
    std::string user_agent = "fibers-http2/0.1";
};

}  // namespace fibers::http2
