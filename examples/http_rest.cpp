// A tiny REST service on fibers: an in-memory key-value store behind a
// fiber::Mutex (it parks fibers, not worker threads).
//
//   GET    /items            -> 200, the ids, one per line
//   GET    /items/{id}       -> 200 + the value, or 404
//   POST   /items/{id}       -> 201 (created) or 200 (replaced); body = value
//   DELETE /items/{id}       -> 204, or 404
//
//   http_rest --port=8080 --workers=2 --worker-cpus=0,4 --pollers=1 --poller-cpus=2
//
//   curl -X POST --data 'hello' localhost:8080/items/a
//   curl localhost:8080/items/a
//   curl -X DELETE localhost:8080/items/a
//
// Handlers copy what they keep (the request is a view into the connection's
// buffer). Stops gracefully on SIGINT / SIGTERM.

#include <map>
#include <mutex>
#include <string>

#include "fibers/fiber/mutex.hpp"
#include "fibers/http/router.hpp"
#include "http_common.hpp"

namespace fiber = fibers::fiber;
namespace http = fibers::http;

namespace {

class Store {
public:
    void list(http::Response& res) {
        std::string out;
        {
            std::lock_guard guard(mutex_);
            for (const auto& [id, value] : items_) {
                out += id;
                out += '\n';
            }
        }
        res.send(200, out, "text/plain");
    }

    void get(http::Request& req, http::Response& res) {
        std::string value;
        bool found = false;
        {
            std::lock_guard guard(mutex_);
            if (const auto it = items_.find(std::string(req.param("id"))); it != items_.end()) {
                value = it->second;
                found = true;
            }
        }
        if (!found) {
            res.send(404, "no such item\n", "text/plain");
            return;
        }
        res.send(200, value, "application/octet-stream");
    }

    void put(http::Request& req, http::Response& res) {
        bool created = false;
        {
            std::lock_guard guard(mutex_);
            auto [it, inserted] = items_.insert_or_assign(std::string(req.param("id")), std::string(req.body()));
            created = inserted;
        }
        res.status(created ? 201 : 200);
        res.header("Location", req.path());
        res.send(created ? "created\n" : "replaced\n");
    }

    void remove(http::Request& req, http::Response& res) {
        std::size_t erased = 0;
        {
            std::lock_guard guard(mutex_);
            erased = items_.erase(std::string(req.param("id")));
        }
        if (erased == 0) {
            res.send(404, "no such item\n", "text/plain");
            return;
        }
        res.status(204);
    }

private:
    fiber::Mutex mutex_;
    std::map<std::string, std::string, std::less<>> items_;
};

}  // namespace

int main(int argc, char** argv) {
    examples::Args args(argc, argv);
    Store store;
    http::Router router;
    // std::string / std::map may throw bad_alloc; like the rest of the
    // library we treat out-of-memory as fatal (noexcept handlers).
    router.get("/items", [&](http::Request&, http::Response& res) noexcept { store.list(res); });
    router.get("/items/{id}", [&](http::Request& req, http::Response& res) noexcept { store.get(req, res); });
    router.post("/items/{id}", [&](http::Request& req, http::Response& res) noexcept { store.put(req, res); });
    router.del("/items/{id}", [&](http::Request& req, http::Response& res) noexcept { store.remove(req, res); });
    return examples::run_http_server(args, router.handler(), "http_rest");
}
