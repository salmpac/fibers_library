#pragma once

// The server side of one HTTP/1.1 connection, over any ByteStream:
//
//   http::serve_connection(stream, state, core);   // in the connection's fiber
//
// reads requests, parses them with llhttp, calls core's handler, writes the
// responses, until the connection ends; then closes the stream. `state`
// (detail::ConnectionState) holds the per-connection buffers and may be
// reused for the next connection (the server pools them); `core`
// (ServerCore) is what all connections of a server share: options, handler,
// the pool of large-body buffers, the registry used by graceful shutdown.
//
// Parsing
// -------
// llhttp (Node.js's parser; strict mode, no lenient flags) does the
// tokenizing and the framing: request line, header fields, Content-Length,
// chunked bodies with extensions and trailers. It rejects the request
// smuggling vectors: Content-Length together with Transfer-Encoding,
// duplicate or malformed Content-Length, Transfer-Encoding with chunked not
// last, obs-fold, whitespace before the colon, bare CR / LF line ends. We add
// the server's policy on top: HTTP/1.0 with Transfer-Encoding and
// Transfer-Encoding without chunked are 400 (RFC 9112 6.1, 6.3); versions
// other than 1.0 / 1.1 are 505; CONNECT is 501.
//
// The read buffer is one contiguous array. A request always starts at
// offset 0 (leftover pipelined bytes are moved to the front after a
// request), llhttp is fed every byte once, in order, and the read buffer
// does not move while a request's head is parsed, so the callbacks' spans
// (method, target, header names and values, which llhttp may report in
// pieces when they straddle reads) are extended in place: views into the
// buffer, no copies. llhttp pauses after the head (on_headers_complete
// returns HPE_PAUSED) so that we can check the limits, answer
// "Expect: 100-continue" and choose where the body goes, and after the
// message (on_message_complete) so that pipelined requests are served one at
// a time. Trailer fields are parsed (and checked) but not exposed.
//
// Body: with Content-Length, in place after the head if it fits into the
// read buffer, else in a pooled body buffer of max_body_bytes. Chunked:
// llhttp hands over the data of each chunk (on_body); it is moved down in
// place over the chunk framing, and once the read buffer runs short it
// moves to a pooled body buffer. Either way the handler gets one contiguous
// string_view.
//
// Timeouts (socket deadlines on the stream, see ServerOptions)
// --------
//   new connection          read deadline = accept + header_timeout
//   waiting for a request   read deadline = now + idle_timeout (keep-alive)
//   first bytes of a head   if the head is not complete with them:
//                           read deadline = now + header_timeout
//   reading the body        read deadline = now + body_timeout, before
//                           every read (inactivity)
//   every flush             write deadline = now + write_timeout
// A later deadline than the pending one is cheap (a few atomics); an earlier
// one schedules a timer, so the header deadline is only set for heads that
// do not arrive in one read.
//
// Graceful shutdown (ServerCore::begin_shutdown): new connections and idle
// keep-alive connections are closed (an idle connection's read is cut by a
// deadline in the past); a connection in the middle of a request finishes it
// and answers with "Connection: close"; every deadline is clamped to the
// shutdown deadline (output.hpp), so I/O that is still going on then fails
// with ETIMEDOUT and the connection closes. The connection's phase word
// (kActive / kIdle / kClosing) and the draining flag are a Dekker pair: the
// connection stores kIdle and then reads draining, the server sets draining
// and then CASes kIdle -> kClosing, both seq_cst, so at least one of them
// notices the other.

#include <llhttp.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <new>
#include <span>
#include <string_view>

#include "fibers/http/message.hpp"
#include "fibers/http/options.hpp"
#include "fibers/http/output.hpp"
#include "fibers/http/request.hpp"
#include "fibers/http/response.hpp"
#include "fibers/http/router.hpp"
#include "fibers/http/stream.hpp"
#include "fibers/object_pool/pooled.hpp"
#include "fibers/object_pool/sharded_pool.hpp"
#include "fibers/sync/spin_lock.hpp"

namespace fibers::http {

namespace detail {

class ConnectionState;

// Registry entry of a live connection (in its ConnectionState).
struct ConnectionNode {
    enum Phase : std::uint8_t { kActive, kIdle, kClosing };

    ConnectionNode* prev = nullptr;
    ConnectionNode* next = nullptr;
    std::atomic<std::uint8_t> phase{kActive};
    StreamRef stream;
};

// A large-body buffer (max_body_bytes, allocated on first use, then reused).
struct BodyBlock {
    char* data = nullptr;

    BodyBlock() noexcept = default;
    BodyBlock(const BodyBlock&) = delete;
    BodyBlock& operator=(const BodyBlock&) = delete;
    ~BodyBlock() { ::operator delete(data, std::nothrow); }
};

using BodyPool = object_pool::DefaultPool<BodyBlock>;
// A body block on loan from the server's pool; returned when dropped.
using BodyLease = object_pool::Pooled<BodyPool>;

}  // namespace detail

// What all connections of one server share. Not movable (connections point
// to it).
class ServerCore {
public:
    ServerCore(Handler handler, ServerOptions options) noexcept
        : options_(options), handler_(handler), bodies_(body_pool_options()) {}

    ServerCore(const ServerCore&) = delete;
    ServerCore& operator=(const ServerCore&) = delete;

    const ServerOptions& options() const noexcept { return options_; }
    const Handler& handler() const noexcept { return handler_; }
    const detail::ShutdownSignal& signal() const noexcept { return signal_; }
    bool draining() const noexcept { return signal_.is_draining(); }

    // Starts the graceful shutdown (see connection.hpp's top comment):
    // publishes `deadline`, closes idle connections, clamps the deadlines of
    // active ones. Any thread; idempotent (the first deadline stays).
    void begin_shutdown(poller::TimePoint deadline) noexcept {
        if (!signal_.is_draining()) {
            signal_.publish(deadline);
        }
        deadline = signal_.deadline();
        std::lock_guard guard(lock_);
        for (detail::ConnectionNode* n = head_; n != nullptr; n = n->next) {
            std::uint8_t expected = detail::ConnectionNode::kIdle;
            if (n->phase.compare_exchange_strong(expected, detail::ConnectionNode::kClosing,
                                                 std::memory_order_seq_cst)) {
                // Waiting for a request: cut the read now.
                static_cast<void>(n->stream.set_read_deadline(detail::steady_now()));
            } else {
                static_cast<void>(n->stream.set_read_deadline(deadline));
                static_cast<void>(n->stream.set_write_deadline(deadline));
            }
        }
    }

    // Live connections (racy snapshot).
    std::size_t connections_approx() const noexcept { return count_.load(std::memory_order_relaxed); }

private:
    friend class detail::ConnectionState;

    static object_pool::PoolOptions body_pool_options() noexcept {
        object_pool::PoolOptions o;
        o.shards = 4;
        o.initial_objects = 0;
        o.grow_chunk = 4;
        o.thread_cache_capacity = 2;
        return o;
    }

    void attach(detail::ConnectionNode& node) noexcept {
        std::lock_guard guard(lock_);
        node.prev = nullptr;
        node.next = head_;
        if (head_ != nullptr) {
            head_->prev = &node;
        }
        head_ = &node;
        count_.fetch_add(1, std::memory_order_relaxed);
    }

    void detach(detail::ConnectionNode& node) noexcept {
        std::lock_guard guard(lock_);
        if (node.prev != nullptr) {
            node.prev->next = node.next;
        } else {
            head_ = node.next;
        }
        if (node.next != nullptr) {
            node.next->prev = node.prev;
        }
        node.prev = node.next = nullptr;
        count_.fetch_sub(1, std::memory_order_relaxed);
    }

    // Empty if out of memory.
    detail::BodyLease acquire_body() noexcept {
        detail::BodyLease b = bodies_.lease();
        if (b->data == nullptr) {
            b->data = static_cast<char*>(::operator new(options_.max_body_bytes + 1, std::nothrow));
            if (b->data == nullptr) {
                return {};  // b goes back to the pool
            }
        }
        return b;
    }

    const ServerOptions options_;
    const Handler handler_;
    detail::ShutdownSignal signal_;
    detail::BodyPool bodies_;
    sync::SpinLock<> lock_;
    detail::ConnectionNode* head_ = nullptr;
    std::atomic<std::size_t> count_{0};
};

namespace detail {

// Per-connection state: llhttp parser, buffers (one allocation, made on
// first use and kept while the object is reused), the request / response
// objects handed to handlers.
class ConnectionState {
public:
    ConnectionState() noexcept = default;
    ConnectionState(const ConnectionState&) = delete;
    ConnectionState& operator=(const ConnectionState&) = delete;

    ~ConnectionState() { ::operator delete(memory_, std::align_val_t{alignof(Header)}, std::nothrow); }

    // Serves the connection to its end, then closes the stream.
    void run(StreamRef stream, ServerCore& core) noexcept {
        core_ = &core;
        const ServerOptions& o = core.options();
        io_.bind(stream, nullptr, 0, &o, &core.signal_);
        // Fresh connection: the header timeout covers the wait for the first
        // request.
        static_cast<void>(io_.set_read_deadline(steady_now() + o.header_timeout));
        if (!prepare(o)) {
            static_cast<void>(stream.close());
            return;
        }
        io_.bind(stream, out_, sizes_.out, &o, &core.signal_);
        llhttp_init(&parser_, HTTP_REQUEST, &settings());
        parser_.data = this;
        in_len_ = 0;
        node_.stream = stream;
        node_.phase.store(ConnectionNode::kIdle, std::memory_order_seq_cst);
        core.attach(node_);
        End end = End::kQuiet;
        if (!core.draining()) {
            fresh_ = true;
            for (;;) {
                end = serve_one();
                if (end != End::kNone) {
                    break;
                }
                fresh_ = false;
            }
        }
        core.detach(node_);
        finish(end);
        static_cast<void>(stream.close());
        core_ = nullptr;
    }

private:
    // How a connection ends (kNone: go on with the next request).
    enum class End : std::uint8_t {
        kNone,
        kQuiet,    // idle timeout, peer closed, read error, shutdown: just close
        kClose,    // after a response with Connection: close: flush, half-close, close
        kLinger,   // after an error response or with unread input: flush,
                   // half-close, drain for linger_timeout, close
    };

    bool prepare(const ServerOptions& o) noexcept {
        const BufferSizes s = BufferSizes::of(o);
        if (memory_ != nullptr && s.in == sizes_.in && s.out == sizes_.out &&
            s.response_headers == sizes_.response_headers && s.scratch == sizes_.scratch &&
            s.headers == sizes_.headers && s.query == sizes_.query) {
            return true;
        }
        ::operator delete(memory_, std::align_val_t{alignof(Header)}, std::nothrow);
        memory_ = nullptr;
        auto round = [](std::size_t n) noexcept { return (n + alignof(Header) - 1) / alignof(Header) * alignof(Header); };
        const std::size_t headers_bytes = s.headers * sizeof(Header);
        const std::size_t query_bytes = s.query * sizeof(QueryParam);
        const std::size_t total = headers_bytes + query_bytes + round(s.in) + round(s.out) +
                                  round(s.response_headers) + round(s.scratch);
        char* m = static_cast<char*>(::operator new(total, std::align_val_t{alignof(Header)}, std::nothrow));
        if (m == nullptr) {
            return false;
        }
        memory_ = m;
        sizes_ = s;
        headers_ = ::new (m) Header[s.headers];
        m += headers_bytes;
        query_ = ::new (m) QueryParam[s.query];
        m += query_bytes;
        in_ = m;
        m += round(s.in);
        out_ = m;
        m += round(s.out);
        res_headers_ = m;
        m += round(s.response_headers);
        scratch_ = m;
        return true;
    }

    // ---- one request --------------------------------------------------------

    End serve_one() noexcept {
        begin_request();
        if (const End e = read_head(); e != End::kNone) {
            return e;
        }
        if (const End e = check_head(); e != End::kNone) {
            return e;
        }
        if (const End e = read_body(); e != End::kNone) {
            release_block();
            return e;
        }
        // The message is complete: parse_pos_ is its end.
        fill_request();
        res_.reset(&io_, res_headers_, sizes_.response_headers, &date_, req_.method_ == Method::kHead,
                   req_.minor_ == 0, req_.keep_alive_);
        core_->handler()(req_, res_);
        const bool keep_alive = res_.finish();
        release_block();
        // Leftover (pipelined) bytes to the front.
        const std::size_t leftover = in_len_ - parse_pos_;
        if (leftover != 0 && parse_pos_ != 0) {
            std::memmove(in_, in_ + parse_pos_, leftover);
        }
        in_len_ = leftover;
        parse_pos_ = 0;
        llhttp_resume(&parser_);
        if (!keep_alive) {
            return leftover != 0 ? End::kLinger : End::kClose;
        }
        return End::kNone;
    }

    void begin_request() noexcept {
        req_.reset();
        parse_pos_ = 0;
        header_count_ = 0;
        field_open_ = false;
        head_done_ = false;
        msg_done_ = false;
        in_body_ = false;
        url_done_ = false;
        error_status_ = 0;
        method_ = {};
        url_ = {};
        body_ = nullptr;
        body_len_ = 0;
        extra_bytes_ = 0;
        block_.reset();  // already returned by the previous request's end
        head_end_ = 0;
    }

    // Reads and parses until the head is complete. kNone: head complete,
    // parse_pos_ = head_end_.
    End read_head() noexcept {
        const ServerOptions& o = core_->options();
        // A fresh connection has its header deadline from run().
        bool header_deadline_set = fresh_;
        for (;;) {
            if (parse_pos_ < in_len_) {
                const llhttp_errno_t err = execute();
                if (head_done_) {
                    head_end_ = parse_pos_;
                    if (head_end_ > o.max_header_bytes) {
                        return respond_error(too_large_status());
                    }
                    return End::kNone;
                }
                if (err != HPE_OK) {
                    return respond_error(error_status(err));
                }
            }
            if (in_len_ > o.max_header_bytes) {
                return respond_error(too_large_status());
            }
            if (in_len_ != 0 && !header_deadline_set) {
                // Part of a head is here but not all of it: the rest has
                // header_timeout (heads that arrive in one read never pay
                // for this timer).
                header_deadline_set = true;
                static_cast<void>(io_.set_read_deadline(steady_now() + o.header_timeout));
            }
            const bool idle = in_len_ == 0;
            if (idle && !fresh_) {
                // Keep-alive wait: flush pipelined / pending responses first.
                if (io_.flush() != 0) {
                    return End::kQuiet;
                }
                static_cast<void>(io_.set_read_deadline(steady_now() + o.idle_timeout));
                node_.phase.store(ConnectionNode::kIdle, std::memory_order_seq_cst);
                if (core_->draining()) {
                    return End::kQuiet;
                }
            } else if (io_.flush() != 0) {
                return End::kQuiet;
            }
            const net::IoResult r = io_.stream().read_some(in_ + in_len_, sizes_.in - in_len_);
            if (!r || r.bytes == 0) {
                if (in_len_ != 0 && r.error == ETIMEDOUT) {
                    return respond_error(408);  // slowloris: part of a head, then nothing
                }
                return End::kQuiet;  // idle timeout, EOF, reset, shutdown
            }
            if (idle) {
                std::uint8_t expected = ConnectionNode::kIdle;
                if (!node_.phase.compare_exchange_strong(expected, ConnectionNode::kActive,
                                                         std::memory_order_seq_cst)) {
                    return End::kQuiet;  // the server closed us meanwhile
                }
            }
            in_len_ += r.bytes;
        }
    }

    // The server's checks between head and body; sets up the body.
    End check_head() noexcept {
        const ServerOptions& o = core_->options();
        const unsigned major = parser_.http_major;
        const unsigned minor = parser_.http_minor;
        if (major != 1 || minor > 1) {
            return respond_error(505);
        }
        if (parser_.method == HTTP_CONNECT) {
            return respond_error(501);
        }
        const bool chunked = (parser_.flags & F_CHUNKED) != 0;
        chunked_ = chunked;
        if ((parser_.flags & F_TRANSFER_ENCODING) != 0 && (!chunked || minor == 0)) {
            return respond_error(400);
        }
        if ((parser_.flags & F_CONTENT_LENGTH) != 0 && parser_.content_length > o.max_body_bytes) {
            return respond_error(413);
        }
        const bool has_body = chunked || parser_.content_length > 0;
        expect_continue_ = false;
        const std::string_view expect = find_header(std::span<const Header>(headers_, header_count_), "Expect");
        if (!expect.empty()) {
            if (!iequals(expect, "100-continue")) {
                return respond_error(417);
            }
            expect_continue_ = minor == 1 && has_body;
        }
        body_ = in_ + head_end_;
        if (!chunked && parser_.content_length > sizes_.in - head_end_) {
            if (!use_block()) {
                return respond_error(503);
            }
        }
        in_body_ = true;
        llhttp_resume(&parser_);
        return End::kNone;
    }

    // Reads and parses until the message is complete.
    End read_body() noexcept {
        const ServerOptions& o = core_->options();
        for (;;) {
            // Zero bytes are fine: llhttp then completes a body-less message.
            const llhttp_errno_t err = execute();
            if (msg_done_) {
                return End::kNone;
            }
            if (err != HPE_OK) {
                return respond_error(error_status(err));
            }
            // Everything was consumed: the raw region restarts after the body
            // (in place) or after the head (body in a block).
            in_len_ = parse_pos_ = head_end_ + (!block_ ? body_len_ : 0);
            if (!block_ && chunked_ && sizes_.in - in_len_ < sizes_.in / 8) {
                // A chunked body growing in place leaves too little room to
                // read into: move it to a block. (A Content-Length body is
                // only in place if it fits.)
                if (!use_block()) {
                    return respond_error(503);
                }
                in_len_ = parse_pos_ = head_end_;
            }
            if (expect_continue_) {
                expect_continue_ = false;
                static constexpr std::string_view kContinue = "HTTP/1.1 100 Continue\r\n\r\n";
                io_.append(kContinue);
            }
            if (io_.flush() != 0) {
                return End::kQuiet;
            }
            static_cast<void>(io_.set_read_deadline(steady_now() + o.body_timeout));
            const net::IoResult r = io_.stream().read_some(in_ + in_len_, sizes_.in - in_len_);
            if (!r || r.bytes == 0) {
                return r.error == ETIMEDOUT && !core_->draining() ? respond_error(408) : End::kQuiet;
            }
            in_len_ += r.bytes;
        }
    }

    // Moves the body so far into a pooled block; false if out of memory.
    bool use_block() noexcept {
        block_ = core_->acquire_body();
        if (!block_) {
            return false;
        }
        std::memcpy(block_->data, body_, body_len_);
        body_ = block_->data;
        return true;
    }

    void release_block() noexcept { block_.reset(); }

    // Feeds llhttp what is buffered after parse_pos_; advances parse_pos_ to
    // where it stopped (a pause, an error) or to in_len_.
    llhttp_errno_t execute() noexcept {
        llhttp_errno_t err = llhttp_execute(&parser_, in_ + parse_pos_, in_len_ - parse_pos_);
        if (err == HPE_OK) {
            parse_pos_ = in_len_;
        } else if (err == HPE_PAUSED) {
            // Only our two callbacks pause; head_done_ / msg_done_ say which.
            parse_pos_ = static_cast<std::size_t>(llhttp_get_error_pos(&parser_) - in_);
            err = HPE_OK;
        }
        return err;
    }

    // The head exceeds max_header_bytes: 414 if the request-target alone
    // does (or is not complete yet), else 431.
    int too_large_status() const noexcept {
        if (!url_done_ || url_.data() + url_.size() - in_ > static_cast<std::ptrdiff_t>(core_->options().max_header_bytes)) {
            return 414;
        }
        return 431;
    }

    int error_status(llhttp_errno_t err) const noexcept {
        // Our callbacks set error_status_ right before failing (llhttp
        // reports a failing *_complete callback as HPE_CB_*, not HPE_USER).
        if (error_status_ != 0) {
            return error_status_;
        }
        if (err == HPE_INVALID_VERSION) {
            // "HTTP/d.d" with a version we do not speak: 505; anything else
            // (garbage, a bare LF after the version) is malformed.
            const std::string_view line(in_, std::min<std::size_t>(in_len_, core_->options().max_header_bytes));
            const std::size_t at = line.find(" HTTP/");
            if (at != std::string_view::npos && at + 9 <= line.size()) {
                const char a = line[at + 6];
                const char dot = line[at + 7];
                const char b = line[at + 8];
                if (a >= '0' && a <= '9' && dot == '.' && b >= '0' && b <= '9' &&
                    (at + 9 == line.size() || line[at + 9] == '\r')) {
                    return 505;
                }
            }
        }
        return 400;
    }

    void fill_request() noexcept {
        req_.method_name_ = method_;
        req_.method_ = to_method(parser_.method);
        req_.target_ = url_;
        req_.split_target();
        req_.major_ = parser_.http_major;
        req_.minor_ = parser_.http_minor;
        req_.keep_alive_ = llhttp_should_keep_alive(&parser_) != 0;
        req_.headers_ = headers_;
        req_.header_count_ = header_count_;
        req_.body_ = std::string_view(body_ != nullptr ? body_ : in_, body_len_);
        req_.scratch_ = scratch_;
        req_.scratch_cap_ = sizes_.scratch;
        req_.query_table_ = query_;
        req_.query_cap_ = sizes_.query;
    }

    static Method to_method(std::uint8_t m) noexcept {
        switch (m) {
            case HTTP_GET: return Method::kGet;
            case HTTP_HEAD: return Method::kHead;
            case HTTP_POST: return Method::kPost;
            case HTTP_PUT: return Method::kPut;
            case HTTP_DELETE: return Method::kDelete;
            case HTTP_PATCH: return Method::kPatch;
            case HTTP_OPTIONS: return Method::kOptions;
            case HTTP_TRACE: return Method::kTrace;
            case HTTP_CONNECT: return Method::kConnect;
            default: return Method::kOther;
        }
    }

    // A minimal response with Connection: close; the connection then
    // lingers and closes.
    End respond_error(int status) noexcept {
        release_block();
        const std::string_view reason = status_reason(status);
        char body[64];
        std::size_t n = std::min(reason.size(), sizeof body - 1);
        std::memcpy(body, reason.data(), n);
        body[n++] = '\n';
        res_.reset(&io_, res_headers_, sizes_.response_headers, &date_, false, false, false);
        static_cast<void>(res_.send(status, std::string_view(body, n), "text/plain"));
        return End::kLinger;
    }

    void finish(End end) noexcept {
        const ServerOptions& o = core_->options();
        if (end == End::kQuiet) {
            io_.discard();
            return;
        }
        if (io_.flush() != 0) {
            return;
        }
        static_cast<void>(io_.stream().shutdown());
        if (end != End::kLinger) {
            return;
        }
        // Drain what the client still sends (bounded in time and bytes), so
        // that our close does not reset the connection before the client
        // has read the response.
        static_cast<void>(io_.set_read_deadline(steady_now() + o.linger_timeout));
        std::size_t drained = 0;
        while (drained < 256 * 1024) {
            const net::IoResult r = io_.stream().read_some(in_, sizes_.in);
            if (!r || r.bytes == 0) {
                break;
            }
            drained += r.bytes;
        }
    }

    // ---- llhttp callbacks ---------------------------------------------------

    static ConnectionState& self(llhttp_t* p) noexcept { return *static_cast<ConnectionState*>(p->data); }

    // Extends a span reported in pieces (pieces are adjacent in the buffer).
    static void extend(std::string_view& span, const char* at, std::size_t n) noexcept {
        if (span.data() == nullptr) {
            span = std::string_view(at, n);
        } else {
            span = std::string_view(span.data(), static_cast<std::size_t>(at + n - span.data()));
        }
    }

    static int on_method(llhttp_t* p, const char* at, std::size_t n) noexcept {
        extend(self(p).method_, at, n);
        return 0;
    }

    static int on_url(llhttp_t* p, const char* at, std::size_t n) noexcept {
        extend(self(p).url_, at, n);
        return 0;
    }

    static int on_url_complete(llhttp_t* p) noexcept {
        self(p).url_done_ = true;
        return 0;
    }

    static int on_version_complete(llhttp_t* p) noexcept {
        if (p->http_major != 1 || p->http_minor > 1) {
            self(p).error_status_ = 505;
            return HPE_USER;
        }
        return 0;
    }

    static int on_header_field(llhttp_t* p, const char* at, std::size_t n) noexcept {
        ConnectionState& c = self(p);
        if (c.in_body_) {
            return c.count_extra(n, 431);  // trailer fields: checked by llhttp, not kept
        }
        if (!c.field_open_) {
            if (c.header_count_ == c.sizes_.headers) {
                c.error_status_ = 431;
                return HPE_USER;
            }
            c.headers_[c.header_count_++] = Header{};
            c.field_open_ = true;
        }
        extend(c.headers_[c.header_count_ - 1].name, at, n);
        return 0;
    }

    static int on_header_field_complete(llhttp_t* p) noexcept {
        self(p).field_open_ = false;
        return 0;
    }

    static int on_header_value(llhttp_t* p, const char* at, std::size_t n) noexcept {
        ConnectionState& c = self(p);
        if (c.in_body_) {
            return c.count_extra(n, 431);
        }
        if (c.header_count_ == 0) {
            return 0;
        }
        extend(c.headers_[c.header_count_ - 1].value, at, n);
        return 0;
    }

    static int on_header_value_complete(llhttp_t* p) noexcept {
        ConnectionState& c = self(p);
        if (c.in_body_ || c.header_count_ == 0) {
            return 0;
        }
        std::string_view& v = c.headers_[c.header_count_ - 1].value;
        while (!v.empty() && (v.back() == ' ' || v.back() == '\t')) {
            v.remove_suffix(1);
        }
        return 0;
    }

    static int on_headers_complete(llhttp_t* p) noexcept {
        ConnectionState& c = self(p);
        c.head_done_ = true;
        // No protocol upgrades: an Upgrade request is served as a normal one
        // (CONNECT is refused in check_head).
        p->upgrade = 0;
        return HPE_PAUSED;
    }

    // Trailer fields and chunk extensions are not kept, but they count
    // against max_header_bytes (else a client could send them forever).
    int count_extra(std::size_t n, int status) noexcept {
        extra_bytes_ += n;
        if (extra_bytes_ > core_->options().max_header_bytes) {
            error_status_ = status;
            return HPE_USER;
        }
        return 0;
    }

    static int on_chunk_extension(llhttp_t* p, const char*, std::size_t n) noexcept {
        return self(p).count_extra(n, 400);
    }

    static int on_body(llhttp_t* p, const char* at, std::size_t n) noexcept {
        ConnectionState& c = self(p);
        if (c.body_len_ + n > c.core_->options().max_body_bytes) {
            c.error_status_ = 413;
            return HPE_USER;
        }
        char* dst = c.body_ + c.body_len_;
        if (dst != at) {
            std::memmove(dst, at, n);  // in place: dst <= at; block: disjoint
        }
        c.body_len_ += n;
        return 0;
    }

    static int on_message_complete(llhttp_t* p) noexcept {
        self(p).msg_done_ = true;
        return HPE_PAUSED;
    }

    static const llhttp_settings_t& settings() noexcept {
        static const llhttp_settings_t s = [] {
            llhttp_settings_t x;
            llhttp_settings_init(&x);
            x.on_method = &on_method;
            x.on_url = &on_url;
            x.on_url_complete = &on_url_complete;
            x.on_version_complete = &on_version_complete;
            x.on_header_field = &on_header_field;
            x.on_header_field_complete = &on_header_field_complete;
            x.on_header_value = &on_header_value;
            x.on_header_value_complete = &on_header_value_complete;
            x.on_headers_complete = &on_headers_complete;
            x.on_chunk_extension_name = &on_chunk_extension;
            x.on_chunk_extension_value = &on_chunk_extension;
            x.on_body = &on_body;
            x.on_message_complete = &on_message_complete;
            return x;
        }();
        return s;
    }

    // ---- state --------------------------------------------------------------

    ServerCore* core_ = nullptr;
    ConnectionIo io_;
    ConnectionNode node_;
    llhttp_t parser_{};

    // buffers (one allocation)
    void* memory_ = nullptr;
    BufferSizes sizes_{};
    char* in_ = nullptr;
    char* out_ = nullptr;
    char* res_headers_ = nullptr;
    char* scratch_ = nullptr;
    Header* headers_ = nullptr;
    QueryParam* query_ = nullptr;

    std::size_t in_len_ = 0;     // bytes in in_
    std::size_t parse_pos_ = 0;  // bytes of in_ fed to llhttp
    std::size_t head_end_ = 0;   // end of the current request's head

    // the current request
    std::string_view method_;
    std::string_view url_;
    std::size_t header_count_ = 0;
    char* body_ = nullptr;
    std::size_t body_len_ = 0;
    std::size_t extra_bytes_ = 0;  // trailer fields + chunk extensions
    BodyLease block_;
    int error_status_ = 0;
    bool fresh_ = false;
    bool field_open_ = false;
    bool head_done_ = false;
    bool msg_done_ = false;
    bool in_body_ = false;
    bool url_done_ = false;
    bool expect_continue_ = false;
    bool chunked_ = false;

    Request req_;
    Response res_;
    DateCache date_;
};

}  // namespace detail

// Serves one connection over `stream` (any ByteStream) in the calling fiber,
// to its end; closes the stream. `state` may be reused afterwards.
template <ByteStream S>
void serve_connection(S& stream, detail::ConnectionState& state, ServerCore& core) noexcept {
    state.run(detail::StreamRef::of(stream), core);
}

}  // namespace fibers::http
