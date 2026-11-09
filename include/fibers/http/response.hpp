#pragma once

// http::Response: what a handler answers with.
//
//   res.status(201);                              // default 200
//   res.header("Content-Type", "application/json");
//   res.send(R"({"id":42})");                     // head + body, done
//
//   res.begin_chunked();                          // or stream it
//   res.write("part 1"); res.write("part 2");     // one chunk each, flushed
//   res.end();                                    // last chunk (also implicit)
//
// The server adds Date, Content-Length (send) or Transfer-Encoding: chunked
// (begin_chunked), and Connection: close when the connection will close
// (the client asked for it, close() was called, the server is shutting down,
// or HTTP/1.0 without keep-alive; "Connection: keep-alive" for an HTTP/1.0
// client that asked for it). Framing headers are the server's: header()
// refuses Content-Length and Transfer-Encoding, and "Connection: close"
// from the handler is the same as close().
//
// send() is terminal and sends at once: the head and a small body are copied
// into the connection's write buffer (pipelined responses then share one
// write), a large body is written directly from the caller's memory. So the
// view passed to send() / write() only has to live through the call -- a
// string the handler owns, a static, anything; no copy of large bodies is
// made and nothing is kept after the call.
//
// HEAD requests: send() emits the head with the Content-Length of the body
// it was given and skips the body; write() sends nothing. 1xx, 204 and 304
// responses carry no body (and no Content-Length for 1xx / 204).
// HTTP/1.0 clients cannot take chunked responses: begin_chunked() then sends
// no framing header, write() sends raw data and the connection closes
// after the response (the end of the body is the end of the connection).
//
// A handler that sends nothing gets an empty response with its status
// (200). Errors: send / begin_chunked / write / end return 0 or the
// connection's errno (EPIPE, ETIMEDOUT, ...; the connection is then closed
// after the handler), EALREADY if the response was already sent, EINVAL for
// write() without begin_chunked().

#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "fibers/http/message.hpp"
#include "fibers/http/output.hpp"

namespace fibers::http {

namespace detail {
class ConnectionState;
}  // namespace detail

class Response {
public:
    Response() noexcept = default;
    Response(const Response&) = delete;
    Response& operator=(const Response&) = delete;

    // The status code (100..999; others become 500). Before the head is sent.
    void status(int code) noexcept {
        if (!started_) {
            status_ = (code >= 100 && code <= 999) ? code : 500;
        }
    }
    int status() const noexcept { return status_; }

    // Adds a header line. false: the head was already sent, the name is not
    // a token, the value contains CR / LF / NUL, it is a framing header
    // (Content-Length, Transfer-Encoding), or the handler's header space
    // (ServerOptions::max_response_header_bytes) is full.
    bool header(std::string_view name, std::string_view value) noexcept {
        if (started_ || !detail::is_token(name) || !detail::safe_field_value(value)) {
            return false;
        }
        if (iequals(name, "Content-Length") || iequals(name, "Transfer-Encoding")) {
            return false;
        }
        if (iequals(name, "Connection")) {
            if (has_token(value, "close")) {
                close_ = true;
            }
            return true;
        }
        const std::size_t need = name.size() + 2 + value.size() + 2;
        if (need > hdr_cap_ - hdr_len_) {
            return false;
        }
        char* p = hdr_ + hdr_len_;
        std::memcpy(p, name.data(), name.size());
        p += name.size();
        *p++ = ':';
        *p++ = ' ';
        std::memcpy(p, value.data(), value.size());
        p += value.size();
        *p++ = '\r';
        *p++ = '\n';
        hdr_len_ += need;
        if (iequals(name, "Date")) {
            has_date_ = true;
        }
        return true;
    }

    // Close the connection after this response.
    void close() noexcept { close_ = true; }

    // Sends the head and `body` (Content-Length framing). Terminal.
    int send(std::string_view body) noexcept {
        if (started_) {
            return EALREADY;
        }
        const bool no_body = status_has_no_body(status_);
        if (const int e = write_head(no_body ? Framing::kNone : Framing::kLength, body.size()); e != 0) {
            return e;
        }
        finished_ = true;
        if (no_body || head_request_ || body.empty()) {
            return io_->error();
        }
        return io_->append(body.data(), body.size());
    }

    // status(code), a Content-Type header if given, send(body).
    int send(int code, std::string_view body, std::string_view content_type = {}) noexcept {
        status(code);
        if (!content_type.empty()) {
            static_cast<void>(header("Content-Type", content_type));
        }
        return send(body);
    }

    // Sends the head with Transfer-Encoding: chunked (see the top comment
    // for HTTP/1.0 and HEAD). Then write() / end().
    int begin_chunked() noexcept {
        if (started_) {
            return EALREADY;
        }
        Framing f = Framing::kChunked;
        if (status_has_no_body(status_)) {
            f = Framing::kNone;
        } else if (http10_) {
            f = Framing::kClose;
            close_ = true;
        }
        return write_head(f, 0);
    }

    // One chunk, flushed (empty data: nothing is sent).
    int write(std::string_view data) noexcept {
        if (!started_ || finished_) {
            return started_ ? EALREADY : EINVAL;
        }
        if (data.empty() || framing_ == Framing::kNone || head_request_) {
            return io_->error();
        }
        if (framing_ == Framing::kChunked) {
            char line[24];
            const auto [end, ec] = std::to_chars(line, line + 16, data.size(), 16);
            static_cast<void>(ec);
            char* p = end;
            *p++ = '\r';
            *p++ = '\n';
            io_->append(line, static_cast<std::size_t>(p - line));
            io_->append(data.data(), data.size());
            io_->append("\r\n", 2);
        } else {
            io_->append(data.data(), data.size());
        }
        return io_->flush();
    }

    // The last chunk ("0\r\n\r\n"). Called after the handler if it did not.
    int end() noexcept {
        if (!started_) {
            return EINVAL;
        }
        if (finished_) {
            return EALREADY;
        }
        finished_ = true;
        if (framing_ == Framing::kChunked && !head_request_) {
            io_->append("0\r\n\r\n", 5);
        }
        return io_->error();
    }

    // Whether the head has been sent.
    bool started() const noexcept { return started_; }
    // Whether the response is complete (send() or end()).
    bool finished() const noexcept { return finished_; }

private:
    friend class detail::ConnectionState;

    enum class Framing : std::uint8_t { kLength, kChunked, kClose, kNone };

    // Per request, by the connection.
    void reset(detail::ConnectionIo* io, char* hdr, std::size_t hdr_cap, detail::DateCache* date, bool head_request,
               bool http10, bool client_keep_alive) noexcept {
        io_ = io;
        hdr_ = hdr;
        hdr_cap_ = hdr_cap;
        hdr_len_ = 0;
        date_ = date;
        status_ = 200;
        head_request_ = head_request;
        http10_ = http10;
        client_keep_alive_ = client_keep_alive;
        close_ = false;
        has_date_ = false;
        started_ = false;
        finished_ = false;
        keep_alive_ = false;
        framing_ = Framing::kNone;
    }

    // After the handler: completes the response. Returns whether the
    // connection may be reused.
    bool finish() noexcept {
        if (!started_) {
            static_cast<void>(send(std::string_view{}));
        } else if (!finished_) {
            static_cast<void>(end());
        }
        return keep_alive_ && io_->error() == 0;
    }

    int write_head(Framing framing, std::size_t length) noexcept {
        started_ = true;
        framing_ = framing;
        keep_alive_ = client_keep_alive_ && !close_ && framing != Framing::kClose && !io_->draining();
        // status line 9 + 3 + 1 + reason + 2; Date 6 + 29 + 2; Content-Length
        // 16 + 20 + 2; Transfer-Encoding 28; Connection 24; final CRLF 2.
        const std::string_view reason = status_reason(status_);
        const std::size_t bound = 15 + reason.size() + 37 + 38 + 28 + 24 + hdr_len_ + 2;
        char* const start = io_->reserve(bound);
        if (start == nullptr) {
            keep_alive_ = false;
            return io_->error() != 0 ? io_->error() : EMSGSIZE;
        }
        char* p = start;
        auto put = [&p](std::string_view s) noexcept {
            std::memcpy(p, s.data(), s.size());
            p += s.size();
        };
        put(http10_ ? "HTTP/1.0 " : "HTTP/1.1 ");
        p = std::to_chars(p, p + 3, status_).ptr;
        *p++ = ' ';
        put(reason);
        put("\r\n");
        if (!has_date_) {
            put("Date: ");
            put(date_->get());
            put("\r\n");
        }
        if (framing == Framing::kLength && !(status_ >= 100 && status_ < 200) && status_ != 204) {
            put("Content-Length: ");
            p = std::to_chars(p, p + 20, length).ptr;
            put("\r\n");
        } else if (framing == Framing::kChunked) {
            put("Transfer-Encoding: chunked\r\n");
        }
        if (!keep_alive_) {
            put("Connection: close\r\n");
        } else if (http10_) {
            put("Connection: keep-alive\r\n");
        }
        put(std::string_view(hdr_, hdr_len_));
        put("\r\n");
        io_->commit(static_cast<std::size_t>(p - start));
        return io_->error();
    }

    detail::ConnectionIo* io_ = nullptr;
    char* hdr_ = nullptr;
    std::size_t hdr_cap_ = 0;
    std::size_t hdr_len_ = 0;
    detail::DateCache* date_ = nullptr;
    int status_ = 200;
    Framing framing_ = Framing::kNone;
    bool head_request_ = false;
    bool http10_ = false;
    bool client_keep_alive_ = false;
    bool close_ = false;
    bool has_date_ = false;
    bool started_ = false;
    bool finished_ = false;
    bool keep_alive_ = false;
};

}  // namespace fibers::http
