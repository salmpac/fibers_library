#pragma once

// The Prometheus endpoint for an http::Router (needs fibers::http):
//
//   http::Router router;
//   router.get("/metrics", metrics::http_handler(registry));
//
// GET (and HEAD) answer 200 with Content-Type
// "text/plain; version=0.0.4; charset=utf-8". The exposition is rendered
// into a kHttpBufferBytes buffer on the handler's (fiber) stack: if it fits,
// it goes out with Content-Length; otherwise the response switches to
// chunked transfer and every full buffer is one chunk. So a scrape of any
// size allocates nothing and holds no lock (see Registry: scrapes run
// concurrently with updates and registration). The registry must outlive
// the server.

#include <cstddef>
#include <string_view>

#include "fibers/http/request.hpp"
#include "fibers/http/response.hpp"
#include "fibers/http/router.hpp"
#include "fibers/metrics/exposition.hpp"

namespace fibers::metrics {

inline constexpr std::string_view kContentType = "text/plain; version=0.0.4; charset=utf-8";

// Rendering buffer on the handler's stack (fiber stacks are 64 KiB by
// default).
inline constexpr std::size_t kHttpBufferBytes = 4096;

// Answers one request with `registry`'s exposition (any type with
// write(Exposition&) const).
template <class Registry>
void serve(const Registry& registry, http::Request&, http::Response& res) noexcept {
    struct Stream {
        http::Response* res;
        bool chunked = false;

        int write(std::string_view data) noexcept {
            if (!chunked) {
                res->status(200);
                static_cast<void>(res->header("Content-Type", kContentType));
                if (const int error = res->begin_chunked(); error != 0) {
                    return error;
                }
                chunked = true;
            }
            return res->write(data);
        }
    };
    Stream stream{&res};
    char buffer[kHttpBufferBytes];
    Exposition out(buffer, Sink(&stream, [](void* context, std::string_view data) noexcept -> int {
                       return static_cast<Stream*>(context)->write(data);
                   }));
    registry.write(out);
    if (!out.flushed()) {
        static_cast<void>(res.send(200, out.buffered(), kContentType));
        return;
    }
    if (out.flush() == 0) {
        static_cast<void>(res.end());
    }
    // else: the connection failed; the server closes it after the handler.
}

// serve() for `registry` as a Handler (refers to it).
template <class Registry>
http::Handler http_handler(const Registry& registry) noexcept {
    return http::Handler(const_cast<Registry*>(&registry), [](void* context, http::Request& req,
                                                              http::Response& res) noexcept {
        serve(*static_cast<const Registry*>(context), req, res);
    });
}

}  // namespace fibers::metrics
