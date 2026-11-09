#pragma once

// http::Handler (a type-erased request handler) and http::Router (method +
// path pattern -> handler).
//
//   http::Router router;
//   router.get("/hello", [](http::Request&, http::Response& res) noexcept {
//       res.send("hello\n");
//   });
//   router.get("/flights/{id}", [&db](http::Request& req, http::Response& res) noexcept {
//       res.send(db.find(req.param("id")));
//   });
//   http::Server server(scheduler, router.handler());
//
// Handler: { void* context, void (*fn)(void*, Request&, Response&) noexcept }
// (no virtual functions). Handler::ref(f) refers to a callable that must
// outlive the server; a plain function pointer converts implicitly.
//
// Router: routes are added before the server starts (not thread-safe while
// serving; matching is read-only and runs on any number of workers). The
// router owns copies of the callables it is given (allocated when the route
// is added, never per request).
//
// Patterns: "/" separated segments, each literal or "{name}" (one whole
// segment, at most Request::kMaxPathParams per pattern, at most 32 segments).
// Literal segments are compared with the raw (not decoded) path, parameter
// values are percent-decoded. Trailing slashes are significant ("/a/" is not
// "/a"). When several patterns match, the most specific wins: the one whose
// first differing segment is literal ("/items/new" beats "/items/{id}").
//
// Dispatch: the matching pattern's handler for the method; HEAD falls back
// to GET (the response then carries no body). A path that matches no pattern
// gets 404 (or the not_found handler); one that matches with no handler for
// the method gets 405 with an Allow header.

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/http/message.hpp"
#include "fibers/http/request.hpp"
#include "fibers/http/response.hpp"

namespace fibers::http {

class Handler {
public:
    using Fn = void (*)(void* context, Request&, Response&) noexcept;
    using PlainFn = void (*)(Request&, Response&) noexcept;

    constexpr Handler() noexcept = default;
    constexpr Handler(void* context, Fn fn) noexcept : context_(context), fn_(fn) {}

    // A plain function (or a captureless lambda: +[](...) noexcept {...}).
    constexpr Handler(PlainFn f) noexcept : fn_(&call_plain), plain_(f) {}  // NOLINT: implicit on purpose

    // Refers to `f` (not copied): f must outlive every use of the handler.
    template <class F>
        requires std::is_nothrow_invocable_v<F&, Request&, Response&>
    static Handler ref(F& f) noexcept {
        return Handler(static_cast<void*>(std::addressof(f)), [](void* c, Request& q, Response& r) noexcept {
            (*static_cast<F*>(c))(q, r);
        });
    }

    void operator()(Request& req, Response& res) const noexcept {
        if (plain_ != nullptr) {
            plain_(req, res);
        } else {
            fn_(context_, req, res);
        }
    }

    explicit operator bool() const noexcept { return fn_ != nullptr; }

private:
    static void call_plain(void*, Request&, Response&) noexcept {}

    void* context_ = nullptr;
    Fn fn_ = nullptr;
    PlainFn plain_ = nullptr;
};

class Router {
public:
    Router() noexcept = default;
    Router(const Router&) = delete;
    Router& operator=(const Router&) = delete;

    ~Router() {
        for (const Owned& o : owned_) {
            o.destroy(o.object);
        }
    }

    // Adds a route; the router keeps a copy of `f`. false: invalid pattern
    // (must start with '/', "{name}" segments only whole, at most
    // Request::kMaxPathParams of them, at most 32 segments), or a handler
    // for this method and pattern exists already.
    template <class F>
        requires std::is_nothrow_invocable_v<std::decay_t<F>&, Request&, Response&>
    bool add(Method method, std::string_view pattern, F&& f) noexcept {
        return add_handler(method, pattern, own(std::forward<F>(f)));
    }

    // The same with a handler as is (not owned).
    bool add(Method method, std::string_view pattern, Handler h) noexcept { return add_handler(method, pattern, h); }

    template <class F>
    bool get(std::string_view pattern, F&& f) noexcept {
        return add(Method::kGet, pattern, std::forward<F>(f));
    }
    template <class F>
    bool post(std::string_view pattern, F&& f) noexcept {
        return add(Method::kPost, pattern, std::forward<F>(f));
    }
    template <class F>
    bool put(std::string_view pattern, F&& f) noexcept {
        return add(Method::kPut, pattern, std::forward<F>(f));
    }
    template <class F>
    bool patch(std::string_view pattern, F&& f) noexcept {
        return add(Method::kPatch, pattern, std::forward<F>(f));
    }
    template <class F>
    bool del(std::string_view pattern, F&& f) noexcept {
        return add(Method::kDelete, pattern, std::forward<F>(f));
    }

    // Handler for paths no pattern matches (default: 404 "Not Found").
    template <class F>
    void not_found(F&& f) noexcept {
        if constexpr (std::is_same_v<std::decay_t<F>, Handler>) {
            not_found_ = f;
        } else {
            not_found_ = own(std::forward<F>(f));
        }
    }

    // Dispatches one request (see the top comment).
    void handle(Request& req, Response& res) const noexcept {
        const Route* best = nullptr;
        for (const Route& route : routes_) {
            if (matches(route, req.path()) && (best == nullptr || route.param_mask < best->param_mask)) {
                best = &route;
            }
        }
        if (best == nullptr) {
            if (not_found_) {
                not_found_(req, res);
            } else {
                static_cast<void>(res.send(404, "Not Found\n", "text/plain"));
            }
            return;
        }
        Method m = req.method();
        Handler h = best->handlers[static_cast<std::size_t>(m)];
        if (!h && m == Method::kHead) {
            h = best->handlers[static_cast<std::size_t>(Method::kGet)];
        }
        if (!h) {
            method_not_allowed(*best, res);
            return;
        }
        if (!bind_params(*best, req)) {
            static_cast<void>(res.send(400, "Bad Request\n", "text/plain"));
            return;
        }
        h(req, res);
    }

    // This router as a Handler (refers to it: the router must outlive the
    // server).
    Handler handler() const noexcept {
        return Handler(const_cast<Router*>(this), [](void* self, Request& q, Response& r) noexcept {
            static_cast<const Router*>(self)->handle(q, r);
        });
    }

private:
    struct Segment {
        std::uint16_t offset;  // into Route::pattern
        std::uint16_t size;
        bool param;            // "{name}": offset/size give the name
    };

    struct Route {
        std::string pattern;
        std::vector<Segment> segments;
        std::uint32_t param_mask = 0;  // bit (31 - i): segment i is a parameter
        std::array<Handler, kMethodCount> handlers{};

        std::string_view text(const Segment& s) const noexcept { return {pattern.data() + s.offset, s.size}; }
    };

    struct Owned {
        void* object;
        void (*destroy)(void*) noexcept;
    };

    template <class F>
    Handler own(F&& f) noexcept {
        using Fn = std::decay_t<F>;
        if constexpr (std::is_convertible_v<Fn, Handler::PlainFn>) {
            return Handler(static_cast<Handler::PlainFn>(f));
        } else {
            Fn* copy = new (std::nothrow) Fn(std::forward<F>(f));
            if (copy == nullptr) {
                fail::fail_fast("http::Router: out of memory");
            }
            owned_.push_back(Owned{copy, [](void* p) noexcept { delete static_cast<Fn*>(p); }});
            return Handler::ref(*copy);
        }
    }

    bool add_handler(Method method, std::string_view pattern, Handler h) noexcept {
        Route route;
        if (!parse(pattern, route) || !h) {
            return false;
        }
        for (Route& existing : routes_) {
            if (existing.pattern == route.pattern) {
                Handler& slot = existing.handlers[static_cast<std::size_t>(method)];
                if (slot) {
                    return false;
                }
                slot = h;
                return true;
            }
        }
        route.handlers[static_cast<std::size_t>(method)] = h;
        routes_.push_back(std::move(route));
        return true;
    }

    static bool parse(std::string_view pattern, Route& out) noexcept {
        if (pattern.empty() || pattern.front() != '/' || pattern.size() > 0xFFFF) {
            return false;
        }
        out.pattern.assign(pattern);
        std::size_t params = 0;
        std::size_t pos = 1;
        for (;;) {
            const std::size_t slash = pattern.find('/', pos);
            const std::size_t end = slash == std::string_view::npos ? pattern.size() : slash;
            const std::string_view seg = pattern.substr(pos, end - pos);
            const std::size_t index = out.segments.size();
            if (index >= 32) {
                return false;
            }
            Segment s{static_cast<std::uint16_t>(pos), static_cast<std::uint16_t>(seg.size()), false};
            if (!seg.empty() && seg.front() == '{') {
                if (seg.size() < 3 || seg.back() != '}' ||
                    seg.substr(1, seg.size() - 2).find_first_of("{}") != std::string_view::npos ||
                    ++params > Request::kMaxPathParams) {
                    return false;
                }
                s = Segment{static_cast<std::uint16_t>(pos + 1), static_cast<std::uint16_t>(seg.size() - 2), true};
                out.param_mask |= std::uint32_t{1} << (31 - index);
            } else if (seg.find_first_of("{}") != std::string_view::npos) {
                return false;
            }
            out.segments.push_back(s);
            if (slash == std::string_view::npos) {
                break;
            }
            pos = slash + 1;
        }
        return true;
    }

    // Segment-wise match of the raw path (no allocation).
    static bool matches(const Route& route, std::string_view path) noexcept {
        if (path.empty() || path.front() != '/') {
            return false;
        }
        std::size_t pos = 1;
        const std::size_t n = route.segments.size();
        for (std::size_t i = 0; i < n; ++i) {
            const std::size_t slash = path.find('/', pos);
            const bool last = i + 1 == n;
            if (last != (slash == std::string_view::npos)) {
                return false;  // more or fewer path segments than pattern segments
            }
            const std::size_t end = last ? path.size() : slash;
            const std::string_view seg = path.substr(pos, end - pos);
            const Segment& s = route.segments[i];
            if (s.param ? seg.empty() : seg != route.text(s)) {
                return false;
            }
            pos = end + 1;
        }
        return true;
    }

    static bool bind_params(const Route& route, Request& req) noexcept {
        if (route.param_mask == 0) {
            return true;
        }
        const std::string_view path = req.path();
        std::size_t pos = 1;
        for (const Segment& s : route.segments) {
            std::size_t end = path.find('/', pos);
            if (end == std::string_view::npos) {
                end = path.size();
            }
            if (s.param) {
                const auto value = req.decode(path.substr(pos, end - pos), false);
                if (!value) {
                    return false;
                }
                req.params_[req.param_count_++] = PathParam{route.text(s), *value};
            }
            pos = end + 1;
        }
        return true;
    }

    static void method_not_allowed(const Route& route, Response& res) noexcept {
        char allow[96];
        std::size_t len = 0;
        auto add = [&](std::string_view m) noexcept {
            if (len + m.size() + 2 > sizeof allow) {
                return;
            }
            if (len != 0) {
                allow[len++] = ',';
                allow[len++] = ' ';
            }
            for (const char c : m) {
                allow[len++] = c;
            }
        };
        for (std::size_t i = 0; i < kMethodCount; ++i) {
            if (route.handlers[i] && static_cast<Method>(i) != Method::kOther) {
                add(method_name(static_cast<Method>(i)));
                if (static_cast<Method>(i) == Method::kGet && !route.handlers[static_cast<std::size_t>(Method::kHead)]) {
                    add("HEAD");
                }
            }
        }
        static_cast<void>(res.header("Allow", std::string_view(allow, len)));
        static_cast<void>(res.send(405, "Method Not Allowed\n", "text/plain"));
    }

    std::vector<Route> routes_;
    std::vector<Owned> owned_;
    Handler not_found_;
};

}  // namespace fibers::http
