#pragma once

// fibers::http: HTTP/1.1 server and client on fibers (CMake: FIBERS_HTTP,
// target fibers::http; parsing by llhttp).
//
//   stream.hpp      ByteStream: the transport concept (net::Stream, TLS later)
//   message.hpp     Method, Header, iequals, percent_decode, status_reason
//   request.hpp     Request (views into the connection's buffers)
//   response.hpp    Response (status, headers, send, chunked streaming)
//   router.hpp      Handler (type-erased), Router (method + path patterns)
//   options.hpp     ServerOptions (limits, timeouts, buffers)
//   connection.hpp  serve_connection<S>: one connection's request loop; ServerCore
//   server.hpp      Server: listener, accept loop, pooled connections, stop()
//   client.hpp      ClientConnection<S>, Client (keep-alive, chunked)

#include "fibers/http/client.hpp"
#include "fibers/http/connection.hpp"
#include "fibers/http/message.hpp"
#include "fibers/http/options.hpp"
#include "fibers/http/request.hpp"
#include "fibers/http/response.hpp"
#include "fibers/http/router.hpp"
#include "fibers/http/server.hpp"
#include "fibers/http/stream.hpp"
