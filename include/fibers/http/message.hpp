#pragma once

// HTTP/1.1 vocabulary shared by the server and the client: methods, header
// fields, status reason phrases, case-insensitive comparison, percent
// decoding, the IMF-fixdate of the Date header.
//
// Nothing here allocates; strings are std::string_view into buffers owned
// by a connection (see request.hpp / client.hpp for their lifetimes).

#include <time.h>

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace fibers::http {

// Request methods. The common ones get their own value (the router keeps a
// handler per value); every other method llhttp knows is kOther (its name is
// still available: Request::method_name()).
enum class Method : std::uint8_t {
    kGet,
    kHead,
    kPost,
    kPut,
    kDelete,
    kPatch,
    kOptions,
    kTrace,
    kConnect,
    kOther,
};

inline constexpr std::size_t kMethodCount = static_cast<std::size_t>(Method::kOther) + 1;

constexpr std::string_view method_name(Method m) noexcept {
    switch (m) {
        case Method::kGet: return "GET";
        case Method::kHead: return "HEAD";
        case Method::kPost: return "POST";
        case Method::kPut: return "PUT";
        case Method::kDelete: return "DELETE";
        case Method::kPatch: return "PATCH";
        case Method::kOptions: return "OPTIONS";
        case Method::kTrace: return "TRACE";
        case Method::kConnect: return "CONNECT";
        case Method::kOther: return "OTHER";
    }
    return "OTHER";
}

// One header field: views into the connection's buffer. Values have their
// surrounding whitespace removed.
struct Header {
    std::string_view name;
    std::string_view value;
};

constexpr char ascii_lower(char c) noexcept { return (c >= 'A' && c <= 'Z') ? static_cast<char>(c + ('a' - 'A')) : c; }

// ASCII case-insensitive equality (header names, tokens).
constexpr bool iequals(std::string_view a, std::string_view b) noexcept {
    if (a.size() != b.size()) {
        return false;
    }
    for (std::size_t i = 0; i < a.size(); ++i) {
        if (ascii_lower(a[i]) != ascii_lower(b[i])) {
            return false;
        }
    }
    return true;
}

// Whether a comma-separated header value (e.g. Connection) contains `token`
// (case-insensitive, whitespace around elements ignored).
constexpr bool has_token(std::string_view list, std::string_view token) noexcept {
    while (!list.empty()) {
        const std::size_t comma = list.find(',');
        std::string_view item = list.substr(0, comma);
        while (!item.empty() && (item.front() == ' ' || item.front() == '\t')) {
            item.remove_prefix(1);
        }
        while (!item.empty() && (item.back() == ' ' || item.back() == '\t')) {
            item.remove_suffix(1);
        }
        if (iequals(item, token)) {
            return true;
        }
        if (comma == std::string_view::npos) {
            break;
        }
        list.remove_prefix(comma + 1);
    }
    return false;
}

// First header named `name` (case-insensitive); empty view if absent.
template <class Headers>
constexpr std::string_view find_header(const Headers& headers, std::string_view name) noexcept {
    for (const Header& h : headers) {
        if (iequals(h.name, name)) {
            return h.value;
        }
    }
    return {};
}

namespace detail {

constexpr int hex_value(char c) noexcept {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

// Valid in a header field value we emit: no CR, LF or NUL (response
// splitting).
constexpr bool safe_field_value(std::string_view v) noexcept {
    for (const char c : v) {
        if (c == '\r' || c == '\n' || c == '\0') {
            return false;
        }
    }
    return true;
}

// RFC 9110 token characters (header names).
constexpr bool is_token(std::string_view v) noexcept {
    if (v.empty()) {
        return false;
    }
    for (const char c : v) {
        const auto u = static_cast<unsigned char>(c);
        const bool alnum = (u >= '0' && u <= '9') || (u >= 'a' && u <= 'z') || (u >= 'A' && u <= 'Z');
        if (!alnum && std::string_view("!#$%&'*+-.^_`|~").find(c) == std::string_view::npos) {
            return false;
        }
    }
    return true;
}

}  // namespace detail

// Percent-decodes `in` into `out` (which needs in.size() bytes; decoding
// never grows) and returns the decoded length. `form`: '+' decodes to a
// space (application/x-www-form-urlencoded, query strings). A '%' not
// followed by two hex digits is kept as is (lenient, like most servers).
// `out` may equal in.data() (in-place decoding).
constexpr std::size_t percent_decode(std::string_view in, char* out, bool form = false) noexcept {
    std::size_t n = 0;
    for (std::size_t i = 0; i < in.size(); ++i) {
        const char c = in[i];
        if (c == '%' && i + 2 < in.size()) {
            const int hi = detail::hex_value(in[i + 1]);
            const int lo = detail::hex_value(in[i + 2]);
            if (hi >= 0 && lo >= 0) {
                out[n++] = static_cast<char>(hi * 16 + lo);
                i += 2;
                continue;
            }
        }
        out[n++] = (form && c == '+') ? ' ' : c;
    }
    return n;
}

// Standard reason phrase of a status code ("" for unknown codes).
constexpr std::string_view status_reason(int status) noexcept {
    switch (status) {
        case 100: return "Continue";
        case 101: return "Switching Protocols";
        case 200: return "OK";
        case 201: return "Created";
        case 202: return "Accepted";
        case 203: return "Non-Authoritative Information";
        case 204: return "No Content";
        case 205: return "Reset Content";
        case 206: return "Partial Content";
        case 300: return "Multiple Choices";
        case 301: return "Moved Permanently";
        case 302: return "Found";
        case 303: return "See Other";
        case 304: return "Not Modified";
        case 307: return "Temporary Redirect";
        case 308: return "Permanent Redirect";
        case 400: return "Bad Request";
        case 401: return "Unauthorized";
        case 403: return "Forbidden";
        case 404: return "Not Found";
        case 405: return "Method Not Allowed";
        case 406: return "Not Acceptable";
        case 408: return "Request Timeout";
        case 409: return "Conflict";
        case 410: return "Gone";
        case 411: return "Length Required";
        case 412: return "Precondition Failed";
        case 413: return "Content Too Large";
        case 414: return "URI Too Long";
        case 415: return "Unsupported Media Type";
        case 416: return "Range Not Satisfiable";
        case 417: return "Expectation Failed";
        case 422: return "Unprocessable Content";
        case 426: return "Upgrade Required";
        case 428: return "Precondition Required";
        case 429: return "Too Many Requests";
        case 431: return "Request Header Fields Too Large";
        case 500: return "Internal Server Error";
        case 501: return "Not Implemented";
        case 502: return "Bad Gateway";
        case 503: return "Service Unavailable";
        case 504: return "Gateway Timeout";
        case 505: return "HTTP Version Not Supported";
        default: return "";
    }
}

// Status codes whose responses never carry a body (nor Content-Length for
// 1xx / 204).
constexpr bool status_has_no_body(int status) noexcept {
    return (status >= 100 && status < 200) || status == 204 || status == 304;
}

namespace detail {

// IMF-fixdate (RFC 9110 5.6.7), e.g. "Sun, 06 Nov 1994 08:49:37 GMT": 29
// characters, no locale, no allocation.
inline constexpr std::size_t kDateLength = 29;

inline void format_http_date(std::int64_t unix_seconds, char out[kDateLength]) noexcept {
    static constexpr char kDays[] = "SunMonTueWedThuFriSat";
    static constexpr char kMonths[] = "JanFebMarAprMayJunJulAugSepOctNovDec";
    const time_t t = static_cast<time_t>(unix_seconds);
    tm parts{};
    gmtime_r(&t, &parts);
    auto two = [](char* p, int v) noexcept {
        p[0] = static_cast<char>('0' + v / 10);
        p[1] = static_cast<char>('0' + v % 10);
    };
    const int year = parts.tm_year + 1900;
    char* p = out;
    p[0] = kDays[parts.tm_wday * 3];
    p[1] = kDays[parts.tm_wday * 3 + 1];
    p[2] = kDays[parts.tm_wday * 3 + 2];
    p[3] = ',';
    p[4] = ' ';
    two(p + 5, parts.tm_mday);
    p[7] = ' ';
    p[8] = kMonths[parts.tm_mon * 3];
    p[9] = kMonths[parts.tm_mon * 3 + 1];
    p[10] = kMonths[parts.tm_mon * 3 + 2];
    p[11] = ' ';
    two(p + 12, year / 100 % 100);
    two(p + 14, year % 100);
    p[16] = ' ';
    two(p + 17, parts.tm_hour);
    p[19] = ':';
    two(p + 20, parts.tm_min);
    p[22] = ':';
    two(p + 23, parts.tm_sec);
    p[25] = ' ';
    p[26] = 'G';
    p[27] = 'M';
    p[28] = 'T';
}

// Wall-clock seconds, cheaply (vDSO, coarse clock: the Date header has
// one-second resolution).
inline std::int64_t wall_seconds() noexcept {
    timespec ts{};
    clock_gettime(CLOCK_REALTIME_COARSE, &ts);
    return static_cast<std::int64_t>(ts.tv_sec);
}

// A per-connection Date cache: reformatted at most once per second.
class DateCache {
public:
    std::string_view get() noexcept {
        const std::int64_t now = wall_seconds();
        if (now != second_) {
            second_ = now;
            format_http_date(now, text_);
        }
        return {text_, kDateLength};
    }

private:
    std::int64_t second_ = -1;
    char text_[kDateLength] = {};
};

}  // namespace detail

}  // namespace fibers::http
