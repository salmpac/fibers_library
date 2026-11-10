#pragma once

// fibers::http2: an HTTP/2 client over nghttp2 (TLS with ALPN "h2", or h2c
// with prior knowledge) and an APNs client on top of it.
//
//   client.hpp    http2::Client: one connection, multiplexed requests
//   message.hpp   Request, Response, error codes
//   options.hpp   ClientOptions
//   session.hpp   the nghttp2 session (internal)
//   jwt.hpp       ES256 JWT signing (Es256Signer), base64url
//   apns.hpp      ApnsClient: token auth, send(), response mapping

#include "fibers/http2/apns.hpp"
#include "fibers/http2/client.hpp"
#include "fibers/http2/jwt.hpp"
#include "fibers/http2/message.hpp"
#include "fibers/http2/options.hpp"
