#pragma once

// Test helpers: a scripted in-memory ByteStream and a response parser.

#include <llhttp.h>

#include <cerrno>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "fibers/http/message.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/poller/sources.hpp"

namespace http_test {

// read_some() returns the script's pieces one per call (split further only
// if the caller's buffer is smaller), then the end: EOF, or ETIMEDOUT when
// end_error is set (a read deadline that fired). write_all() appends to
// `output`. Single-threaded: run serve_connection() on the test thread.
struct ScriptedStream {
    std::vector<std::string> pieces;
    std::size_t next = 0;
    std::size_t offset = 0;
    int end_error = 0;

    std::string output;
    std::size_t writes = 0;
    std::size_t reads = 0;
    std::size_t deadlines = 0;
    int write_error = 0;  // non-zero: every write fails with it
    bool shut = false;
    bool closed = false;

    fibers::net::IoResult read_some(void* buffer, std::size_t size) noexcept {
        ++reads;
        if (closed) {
            return {0, EBADF};
        }
        if (next == pieces.size()) {
            return {0, end_error};
        }
        const std::string& p = pieces[next];
        const std::size_t n = std::min(size, p.size() - offset);
        std::memcpy(buffer, p.data() + offset, n);
        offset += n;
        if (offset == p.size()) {
            ++next;
            offset = 0;
        }
        return {n, 0};
    }

    fibers::net::IoResult write_all(const void* data, std::size_t size) noexcept {
        ++writes;
        if (write_error != 0) {
            return {0, write_error};
        }
        output.append(static_cast<const char*>(data), size);
        return {size, 0};
    }

    int set_read_deadline(fibers::poller::TimePoint) noexcept {
        ++deadlines;
        return 0;
    }
    int set_write_deadline(fibers::poller::TimePoint) noexcept { return 0; }
    int shutdown() noexcept {
        shut = true;
        return 0;
    }
    int close() noexcept {
        closed = true;
        return 0;
    }
};

struct ParsedResponse {
    int status = 0;
    unsigned minor = 0;
    std::vector<std::pair<std::string, std::string>> headers;
    std::string body;
    bool chunked = false;

    std::string header(std::string_view name) const {
        for (const auto& [n, v] : headers) {
            if (fibers::http::iequals(n, name)) {
                return v;
            }
        }
        return {};
    }
    bool has(std::string_view name) const {
        for (const auto& [n, v] : headers) {
            if (fibers::http::iequals(n, name)) {
                return true;
            }
        }
        return false;
    }
};

// Parses a sequence of responses. `head_requests[i]`: response i answers a
// HEAD request (no body). Sets `ok` to false on a malformed stream (an
// incomplete last response included).
inline std::vector<ParsedResponse> parse_responses(std::string_view data, bool& ok,
                                                   const std::vector<bool>& head_requests = {}) {
    struct State {
        std::vector<ParsedResponse> out;
        ParsedResponse cur;
        std::string field;
        std::string value;
        bool in_value = false;
        const std::vector<bool>* heads = nullptr;
    } st;
    st.heads = &head_requests;
    llhttp_settings_t s;
    llhttp_settings_init(&s);
    s.on_header_field = [](llhttp_t* p, const char* at, std::size_t n) {
        auto& st = *static_cast<State*>(p->data);
        if (st.in_value) {
            st.cur.headers.emplace_back(st.field, st.value);
            st.field.clear();
            st.value.clear();
            st.in_value = false;
        }
        st.field.append(at, n);
        return 0;
    };
    s.on_header_value = [](llhttp_t* p, const char* at, std::size_t n) {
        auto& st = *static_cast<State*>(p->data);
        st.in_value = true;
        st.value.append(at, n);
        return 0;
    };
    s.on_headers_complete = [](llhttp_t* p) {
        auto& st = *static_cast<State*>(p->data);
        if (!st.field.empty()) {
            st.cur.headers.emplace_back(st.field, st.value);
        }
        st.field.clear();
        st.value.clear();
        st.in_value = false;
        st.cur.status = p->status_code;
        st.cur.minor = p->http_minor;
        st.cur.chunked = (p->flags & F_CHUNKED) != 0;
        const std::size_t index = st.out.size();
        const bool head = index < st.heads->size() && (*st.heads)[index];
        return head && p->status_code >= 200 ? 1 : 0;
    };
    s.on_body = [](llhttp_t* p, const char* at, std::size_t n) {
        static_cast<State*>(p->data)->cur.body.append(at, n);
        return 0;
    };
    s.on_message_complete = [](llhttp_t* p) {
        auto& st = *static_cast<State*>(p->data);
        st.out.push_back(std::move(st.cur));
        st.cur = ParsedResponse{};
        return 0;
    };
    llhttp_t parser;
    llhttp_init(&parser, HTTP_RESPONSE, &s);
    parser.data = &st;
    const llhttp_errno_t err = llhttp_execute(&parser, data.data(), data.size());
    ok = err == HPE_OK;
    if (ok) {
        const llhttp_errno_t fin = llhttp_finish(&parser);
        ok = fin == HPE_OK;
    }
    return std::move(st.out);
}

}  // namespace http_test
