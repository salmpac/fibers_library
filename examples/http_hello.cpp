// Hello-world HTTP server on fibers: GET /hello -> "Hello, World!\n".
//
//   http_hello --port=8080 --workers=1 --worker-cpus=0 --pollers=1 --poller-cpus=2
//              [--max-connections=20000 --stack-kb=64 --idle-spins=N --grace-ms=5000]
//
// The benchmark target of examples/README.md (wrk / wrk2, keep-alive).
// Stops gracefully on SIGINT / SIGTERM.

#include "fibers/http/router.hpp"
#include "http_common.hpp"

namespace http = fibers::http;

int main(int argc, char** argv) {
    examples::Args args(argc, argv);
    http::Router router;
    router.get("/hello", [](http::Request&, http::Response& res) noexcept {
        res.header("Content-Type", "text/plain");
        res.send("Hello, World!\n");
    });
    return examples::run_http_server(args, router.handler(), "http_hello");
}
