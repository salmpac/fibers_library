#pragma once

// http::Request: a parsed request as the handler sees it.
//
// A view: every string_view points into the connection's buffers (the
// request line and headers into the read buffer, the body into the read
// buffer or a pooled body buffer, decoded parameters into a per-connection
// scratch area). Valid until the handler returns; copy what must outlive it.
// Nothing is allocated per request: the header array, the query table and
// the scratch area belong to the connection and are reused.
//
//   req.method()          Method::kGet, ...       req.method_name()  "GET"
//   req.target()          "/flights/42?seat=12%20A"   (raw request-target)
//   req.path()            "/flights/42"               (raw, not decoded)
//   req.param("id")       "42"         path parameter of the route, decoded
//   req.query("seat")     "12 A"       query parameter, decoded (optional)
//   req.header("Content-Type")         case-insensitive, first match
//   req.body()            the whole body (Content-Length or de-chunked)

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>

#include "fibers/http/message.hpp"

namespace fibers::http {

class Router;

namespace detail {
class ConnectionState;
}  // namespace detail

// A path parameter: name from the route pattern ("{id}"), decoded value.
struct PathParam {
    std::string_view name;
    std::string_view value;
};

// A query parameter, both parts percent-decoded ('+' is a space).
struct QueryParam {
    std::string_view name;
    std::string_view value;
};

class Request {
public:
    // At most this many {name} segments per route pattern.
    static constexpr std::size_t kMaxPathParams = 8;

    Request() noexcept = default;
    Request(const Request&) = delete;
    Request& operator=(const Request&) = delete;

    Method method() const noexcept { return method_; }
    // The method as sent ("GET", "PROPFIND", ...).
    std::string_view method_name() const noexcept { return method_name_; }

    // The raw request-target as sent.
    std::string_view target() const noexcept { return target_; }
    // The path of the target (origin-form up to '?'; for an absolute-form
    // target "http://host/p?q" it is "/p"), not percent-decoded.
    std::string_view path() const noexcept { return path_; }
    // The raw query string (after '?', without it; empty if none).
    std::string_view raw_query() const noexcept { return query_; }

    unsigned version_major() const noexcept { return major_; }
    unsigned version_minor() const noexcept { return minor_; }

    std::span<const Header> headers() const noexcept { return {headers_, header_count_}; }
    // First header field named `name` (case-insensitive); empty if absent
    // (use has_header() to tell an empty value from a missing one).
    std::string_view header(std::string_view name) const noexcept { return find_header(headers(), name); }
    bool has_header(std::string_view name) const noexcept {
        for (const Header& h : headers()) {
            if (iequals(h.name, name)) {
                return true;
            }
        }
        return false;
    }

    // The complete body (empty if none).
    std::string_view body() const noexcept { return body_; }

    // Whether the client allows the connection to be reused after this
    // request (HTTP/1.1 without "Connection: close", or HTTP/1.0 with
    // "Connection: keep-alive").
    bool keep_alive() const noexcept { return keep_alive_; }

    // Path parameter of the matched route ("{id}"), percent-decoded; empty
    // if there is none of that name.
    std::string_view param(std::string_view name) const noexcept {
        for (std::size_t i = 0; i < param_count_; ++i) {
            if (params_[i].name == name) {
                return params_[i].value;
            }
        }
        return {};
    }
    std::span<const PathParam> params() const noexcept { return {params_, param_count_}; }

    // First query parameter named `name` (compared after decoding); its
    // decoded value, or nullopt. The query string is parsed and decoded
    // once, on the first call (into the connection's scratch area; at most
    // ServerOptions::max_query_params parameters, later ones are ignored).
    std::optional<std::string_view> query(std::string_view name) noexcept {
        for (const QueryParam& q : query_params()) {
            if (q.name == name) {
                return q.value;
            }
        }
        return std::nullopt;
    }

    std::span<const QueryParam> query_params() noexcept {
        if (!query_parsed_) {
            parse_query();
        }
        return {query_table_, query_count_};
    }

private:
    friend class Router;
    friend class detail::ConnectionState;

    // Percent-decodes into the scratch area; nullopt if it is full (cannot
    // happen within the limits: path + query fit in max_header_bytes, which
    // is the scratch size).
    std::optional<std::string_view> decode(std::string_view in, bool form) noexcept {
        if (in.size() > scratch_cap_ - scratch_used_) {
            return std::nullopt;
        }
        char* out = scratch_ + scratch_used_;
        const std::size_t n = percent_decode(in, out, form);
        scratch_used_ += n;
        return std::string_view(out, n);
    }

    void parse_query() noexcept {
        query_parsed_ = true;
        std::string_view rest = query_;
        while (!rest.empty() && query_count_ < query_cap_) {
            const std::size_t amp = rest.find('&');
            const std::string_view pair = rest.substr(0, amp);
            rest = amp == std::string_view::npos ? std::string_view{} : rest.substr(amp + 1);
            if (pair.empty()) {
                continue;
            }
            const std::size_t eq = pair.find('=');
            const std::string_view raw_name = pair.substr(0, eq);
            const std::string_view raw_value = eq == std::string_view::npos ? std::string_view{} : pair.substr(eq + 1);
            const auto name = decode(raw_name, true);
            const auto value = decode(raw_value, true);
            if (!name || !value) {
                break;
            }
            query_table_[query_count_++] = QueryParam{*name, *value};
        }
    }

    // Splits the target into path and query.
    void split_target() noexcept {
        std::string_view t = target_;
        if (const std::size_t hash = t.find('#'); hash != std::string_view::npos) {
            t = t.substr(0, hash);
        }
        std::string_view path = t;
        if (const std::size_t q = t.find('?'); q != std::string_view::npos) {
            path = t.substr(0, q);
            query_ = t.substr(q + 1);
        } else {
            query_ = {};
        }
        // absolute-form: scheme "://" authority path
        if (!path.empty() && path.front() != '/' && path != "*") {
            if (const std::size_t scheme = path.find("://"); scheme != std::string_view::npos) {
                const std::string_view after = path.substr(scheme + 3);
                const std::size_t slash = after.find('/');
                path = slash == std::string_view::npos ? std::string_view("/") : after.substr(slash);
            }
        }
        path_ = path;
    }

    void reset() noexcept {
        method_ = Method::kOther;
        method_name_ = {};
        target_ = path_ = query_ = body_ = {};
        header_count_ = 0;
        param_count_ = 0;
        scratch_used_ = 0;
        query_count_ = 0;
        query_parsed_ = false;
        major_ = minor_ = 0;
        keep_alive_ = false;
    }

    Method method_ = Method::kOther;
    std::uint8_t major_ = 0;
    std::uint8_t minor_ = 0;
    bool keep_alive_ = false;
    bool query_parsed_ = false;
    std::string_view method_name_;
    std::string_view target_;
    std::string_view path_;
    std::string_view query_;
    std::string_view body_;
    const Header* headers_ = nullptr;
    std::size_t header_count_ = 0;

    PathParam params_[kMaxPathParams];
    std::size_t param_count_ = 0;

    char* scratch_ = nullptr;
    std::size_t scratch_cap_ = 0;
    std::size_t scratch_used_ = 0;

    QueryParam* query_table_ = nullptr;
    std::size_t query_cap_ = 0;
    std::size_t query_count_ = 0;
};

}  // namespace fibers::http
