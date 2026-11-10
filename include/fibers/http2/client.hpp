#pragma once

// An HTTP/2 client for fibers: one connection (TLS with ALPN "h2", or h2c
// with prior knowledge), many concurrent requests multiplexed on it as
// streams. Made for HTTP/2-only APIs such as Apple's push service (see
// apns.hpp).
//
//   tls::TlsContext ctx;
//   const std::string_view alpn[] = {"h2"};
//   tls::ClientOptions co;  co.alpn = alpn;  co.use_default_ca = true;
//   ctx.init_client(co);
//   http2::ClientOptions o;
//   o.host = "api.push.apple.com";  o.port = 443;  o.tls = &ctx;
//   http2::Client client(scheduler, o);
//   ... in fibers (any number at once):
//   const http2::Header h[] = {{"apns-topic", "com.example.app"}};
//   http2::Response res;
//   int e = client.request({.method = "POST", .path = "/3/device/...", .headers = h, .body = json}, res);
//   if (e == 0) { res.status(); res.header("apns-id"); res.body(); }
//   else if (http2::retryable(e)) { ... not processed: send again ... }
//   ... once, from a fiber, when done:
//   client.close();
//
// Connection structure
// --------------------
//   connect      lazy: the first request (or connect()) resolves `host`
//                (unless `endpoint` is set), connects (TCP, then TLS with
//                ALPN "h2" when ClientOptions::tls is set; both bounded by
//                connect_timeout and the request's deadline), and starts
//                two fibers. One connect at a time (a fiber::Mutex); the
//                callers that arrive meanwhile wait for it (bounded by its
//                connect_timeout, not by their own deadlines) and share the
//                connection.
//   reader fiber read_some(buffer) -> lock(mu) -> session.recv(): nghttp2
//                parses the frames, and its callbacks fill the callers'
//                Responses and complete their Oneshots -> unlock -> ring
//                the writer if nghttp2 has something to send (SETTINGS
//                ACK, WINDOW_UPDATE, PING ACK, ...).
//   writer fiber parked on a Doorbell; rung: lock(mu) -> session.fill(out)
//                (nghttp2 serializes frames, copying request bodies as flow
//                control allows) -> unlock -> write_all(out), until nothing
//                is pending. Also sends the keepalive PINGs.
//   callers      lock(mu) -> session.submit() -> unlock -> ring -> wait on
//                a fiber::Oneshot<int> in their own frame (no allocation).
//   The nghttp2 session (and everything next to it) is confined under ONE
//   fiber::Mutex per connection; socket reads and writes happen outside it
//   (a writer blocked on a full send buffer never stops the reader: HTTP/2
//   needs both directions to progress). The TLS stream supports exactly
//   this: one reader fiber and one writer fiber at a time.
//
// Deadlines (Request::timeout / deadline, ClientOptions::request_timeout):
// a poller timer per request (a node from the scheduler's TaskMemoryPool).
// Whoever comes first claims the request through a flag in the node: the
// connection (stream closed) or the timer, which completes the caller with
// ETIMEDOUT from the poller thread without touching the session. The woken
// caller then locks the connection mutex, detaches its stream and queues
// RST_STREAM(CANCEL), so the server learns of the cancellation and nothing
// references the caller's frame afterwards (session.hpp).
//
// Contexts (fibers/fiber/context.hpp, docs/cancellation.md): a request
// made by a fiber under a context fails at once with ECANCELED / ETIMEDOUT
// if the context has ended, and is interrupted when it ends: the caller
// enlists a hook in the context that claims the request through the same
// flag as the deadline timer (a DeadlineNode is made for that even without
// a deadline) and completes the caller with the context's error; the
// caller then resets the stream as after its own deadline. The connect,
// TLS handshake and socket I/O of a fresh connection run in the caller's
// fiber, so they are under its context too (the connect mutex that other
// callers wait on meanwhile is not cancellable: bounded by connect_timeout).
// The reader and writer fibers run under no context: one request's end
// never tears down the shared connection.
//
// SETTINGS_MAX_CONCURRENT_STREAMS: requests beyond the peer's limit wait
// inside nghttp2 for a stream (StreamLimit::kQueue; at most
// max_queued_requests of them, beyond: EBUSY) or fail with EBUSY at once
// (kFail). Flow control is nghttp2's (windows from ClientOptions;
// WINDOW_UPDATE as received data is consumed into the Responses).
//
// GOAWAY: the connection takes no new requests (the next one connects
// anew); streams above the GOAWAY's last_stream_id end with EAGAIN (not
// processed: retryable), the others finish normally, then the connection
// closes. Connection loss (read / write error, end of stream, protocol
// error, missing PING ACK): every request in flight ends with ECONNRESET
// (EPROTO for a protocol error; EAGAIN for those whose HEADERS were never
// sent); the next request reconnects. No automatic retries except one:
// a request whose connection stopped taking new streams before it could
// be submitted goes to a new connection (nothing was sent). Error codes:
// see message.hpp.
//
// Teardown (the reader fiber, once): the reader ends (transport error /
// end of stream / fatal protocol error / nothing left to do after a
// GOAWAY); after a protocol error it first lets the writer flush nghttp2's
// GOAWAY (bounded by close_flush_timeout); then, under the mutex, every
// live request is completed with the loss error and the session goes dead;
// the transport is shut down (the writer leaves a blocked write), the
// writer is waited for, the keepalive timer detached, the stream closed.
//
// Threads, lifetime: request() / get() / post() / connect() from fibers of
// the scheduler, any number at once. close() from a fiber, once: sends
// GOAWAY on every connection, ends the requests still in flight with
// ECANCELED and waits for the connection fibers; later requests fail with
// ECANCELED. Destroy the client only after close() and after every
// request() call has returned (connections reference the client; close()
// waits for them); destroying it with connections open fails fast.
//
// Memory. Per connection (heap, at connect): the connection object with
// its TLS stream, the read buffer (read_buffer_bytes), the writer's output
// buffer (about write_chunk_bytes), the nghttp2 session and its stream
// record pool (grows to the number of streams live at once, then reused).
// Per request: nothing of ours once warm -- the header array lives on the
// caller's stack, the deadline node comes from the TaskMemoryPool, the
// Response's buffers are reused if the caller reuses the Response. nghttp2
// allocates per stream (stream state, a copy of the request headers) and
// for HPACK; OpenSSL per connection.
//
// Stack: the reader and writer fibers run OpenSSL and nghttp2 on their
// stacks (TLS needs about 24 KiB free, see tls/stream.hpp "Stack"); the
// callers build the header array on theirs (kMaxRequestHeaders + 6 entries
// of 40 bytes). The scheduler's default 64 KiB stacks are enough.

#include <nghttp2/nghttp2.h>
#include <sys/socket.h>

#include <array>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/fiber/context.hpp"
#include "fibers/fiber/event.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/mutex.hpp"
#include "fibers/fiber/oneshot.hpp"
#include "fibers/fiber/wait_group.hpp"
#include "fibers/http2/message.hpp"
#include "fibers/http2/options.hpp"
#include "fibers/http2/session.hpp"
#include "fibers/net/resolver.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/poller/sources.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/sync/spin_lock.hpp"
#include "fibers/tls/connection.hpp"

namespace fibers::http2 {

namespace detail {

// A single-waiter, auto-reset wakeup for the writer fiber: ring() from
// anywhere (fibers, the poller thread), any number of times; wait()
// returns once per batch of rings.
//
//   word: 0 idle, 1 rung, else the parked fiber (aligned, never 0 / 1)
//   ring: idle / rung -> rung; fiber -> idle, then wake it
//   wait: rung -> idle, return; idle -> self, park (a ring that comes
//         before the fiber has left its stack is absorbed by its state
//         word: NOTIFIED)
//
// Every transition is an RMW on one word (acq_rel), so a ring and a wait are
// totally ordered and no ring is lost: what the ringer did before ring()
// happens-before the waiter's return. The data the writer acts on is under
// the connection mutex anyway.
template <class Sched>
class Doorbell {
public:
    using Fiber = fiber::BasicFiber<Sched>;

    void ring() noexcept {
        std::uintptr_t cur = word_.load(std::memory_order_relaxed);
        for (;;) {
            if (cur == kIdle || cur == kRung) {
                if (word_.compare_exchange_weak(cur, kRung, std::memory_order_acq_rel, std::memory_order_relaxed)) {
                    return;
                }
                continue;
            }
            if (word_.compare_exchange_weak(cur, kIdle, std::memory_order_acq_rel, std::memory_order_relaxed)) {
                reinterpret_cast<Fiber*>(cur)->wake();
                return;
            }
        }
    }

    // From the one waiting fiber.
    void wait() noexcept {
        std::uintptr_t cur = word_.load(std::memory_order_relaxed);
        for (;;) {
            if (cur == kRung) {
                if (word_.compare_exchange_weak(cur, kIdle, std::memory_order_acq_rel, std::memory_order_relaxed)) {
                    return;
                }
                continue;
            }
            if (cur != kIdle) {
                fail::fail_fast("http2: Doorbell has a second waiter");
            }
            Fiber& self = Fiber::current_or_die("http2: Doorbell::wait() outside a fiber");
            if (word_.compare_exchange_weak(cur, reinterpret_cast<std::uintptr_t>(&self), std::memory_order_acq_rel,
                                            std::memory_order_relaxed)) {
                self.park();  // the ring that takes us out of the word wakes us, once
                return;
            }
        }
    }

private:
    static constexpr std::uintptr_t kIdle = 0;
    static constexpr std::uintptr_t kRung = 1;
    typename Sched::SyncPolicy::template Atomic<std::uintptr_t> word_{kIdle};
};

inline bool is_connection_specific(std::string_view name) noexcept {
    return name == "connection" || name == "keep-alive" || name == "proxy-connection" ||
           name == "transfer-encoding" || name == "upgrade" || name == "host";
}

inline nghttp2_nv make_nv(std::string_view name, std::string_view value) noexcept {
    return nghttp2_nv{reinterpret_cast<uint8_t*>(const_cast<char*>(name.data())),
                      reinterpret_cast<uint8_t*>(const_cast<char*>(value.data())), name.size(), value.size(),
                      NGHTTP2_NV_FLAG_NONE};
}

}  // namespace detail

template <net::NetScheduler Sched = sched::Scheduler>
class BasicClient {
public:
    using Stream = tls::BasicConnection<Sched>;
    using SyncPolicy = typename Sched::SyncPolicy;

    BasicClient(Sched& scheduler, ClientOptions options) noexcept
        : scheduler_(scheduler), options_(std::move(options)) {
        if (options_.authority.empty()) {
            options_.authority = default_authority();
        }
        scheme_ = options_.tls != nullptr ? "https" : "http";
    }

    BasicClient(const BasicClient&) = delete;
    BasicClient& operator=(const BasicClient&) = delete;

    ~BasicClient() {
        std::lock_guard guard(lock_);
        if (head_ != nullptr) {
            fail::fail_fast("http2: Client destroyed with open connections (call close() first, from a fiber)");
        }
    }

    // One request (see the top comment); 0 or errno (message.hpp). From a
    // fiber of the scheduler.
    int request(const Request& req, Response& res) noexcept {
        res.clear();
        const poller::TimePoint deadline = deadline_of(req);
        if (has_deadline(deadline) && deadline <= scheduler_.now()) {
            return ETIMEDOUT;
        }
        if (const int e = fiber::this_fiber::context_error<Sched>(); e != 0) {
            return e;
        }
        std::array<nghttp2_nv, kMaxRequestHeaders + 6> nv;
        char length[24];
        std::size_t n = 0;
        if (const int e = build_headers(req, nv, n, length); e != 0) {
            return e;
        }
        for (int attempt = 0;; ++attempt) {
            int error = 0;
            Connection* conn = acquire(deadline, error);
            if (conn == nullptr) {
                return error;
            }
            bool retry = false;
            const int e = conn->exchange(nv.data(), n, req.body, res, deadline, retry);
            conn->unref();
            if (!retry || attempt == 2) {
                return e;
            }
            res.clear();
        }
    }

    int get(std::string_view path, Response& res) noexcept {
        Request req;
        req.path = path;
        return request(req, res);
    }

    int post(std::string_view path, std::string_view body, Response& res,
             std::string_view content_type = "application/octet-stream") noexcept {
        const Header headers[] = {{"content-type", content_type}};
        Request req;
        req.method = "POST";
        req.path = path;
        req.headers = headers;
        req.body = body;
        return request(req, res);
    }

    // Connects now if there is no usable connection (0 or errno), e.g. to
    // pay the handshake before the first request. `deadline` as Request's.
    int connect(poller::TimePoint deadline = {}) noexcept {
        if (!has_deadline(deadline)) {
            deadline = scheduler_.now() + options_.connect_timeout;
        }
        int error = 0;
        Connection* conn = acquire(deadline, error);
        if (conn != nullptr) {
            conn->unref();
        }
        return error;
    }

    // GOAWAY on every connection; in-flight requests end with ECANCELED;
    // waits for the connection fibers. From a fiber; idempotent.
    void close() noexcept {
        std::lock_guard connect_guard(connect_mu_);
        Connection* current = nullptr;
        {
            std::lock_guard guard(lock_);
            if (closed_) {
                return;
            }
            closed_ = true;
            current = std::exchange(current_, nullptr);
        }
        if (current != nullptr) {
            current->unref();
        }
        for (;;) {
            Connection* c = nullptr;
            {
                std::lock_guard guard(lock_);
                for (Connection* x = head_; x != nullptr; x = x->next) {
                    if (!x->close_requested) {
                        x->close_requested = true;
                        x->ref();
                        c = x;
                        break;
                    }
                }
            }
            if (c == nullptr) {
                break;
            }
            c->close_gracefully();
            c->unref();
        }
        if (ever_connected_) {
            connections_.wait();
        }
    }

    // A usable connection exists right now (approximate).
    bool connected() noexcept {
        std::lock_guard guard(lock_);
        return current_ != nullptr && current_->usable();
    }

    ClientStats stats() const noexcept { return counters_.snapshot(); }

    // Streams live on the current connection and the size of its record
    // pool (tests: no leak after cancellations). {0, 0} without one. From a
    // fiber (takes the connection mutex).
    std::pair<std::size_t, std::size_t> stream_records() noexcept {
        Connection* c = current_ref();
        if (c == nullptr) {
            return {0, 0};
        }
        std::pair<std::size_t, std::size_t> r = c->stream_records();
        c->unref();
        return r;
    }

    const ClientOptions& options() const noexcept { return options_; }
    Sched& scheduler() noexcept { return scheduler_; }

private:
    using Mutex = fiber::BasicMutex<Sched>;
    using CallT = detail::Call<Sched>;
    using DeadlineNode = detail::DeadlineNode<Sched>;
    using Counters = detail::Counters<SyncPolicy>;
    template <class T>
    using Atomic = typename SyncPolicy::template Atomic<T>;

    static bool has_deadline(poller::TimePoint t) noexcept {
        return t != poller::TimePoint{} && t != poller::TimePoint::max();
    }

    poller::TimePoint deadline_of(const Request& req) noexcept {
        if (req.deadline != poller::TimePoint{}) {
            return req.deadline;
        }
        if (req.timeout < poller::Duration::zero()) {
            return {};
        }
        const poller::Duration t = req.timeout > poller::Duration::zero() ? req.timeout : options_.request_timeout;
        return t > poller::Duration::zero() ? scheduler_.now() + t : poller::TimePoint{};
    }

    std::string default_authority() const {
        std::string a;
        const bool v6 = options_.host.find(':') != std::string::npos;
        if (v6) {
            a += '[';
        }
        a += options_.host;
        if (v6) {
            a += ']';
        }
        const std::uint16_t default_port = options_.tls != nullptr ? 443 : 80;
        if (options_.port != default_port) {
            a += ':';
            a += std::to_string(options_.port);
        }
        return a;
    }

    // Pseudo-headers, then the caller's (validated), content-length,
    // user-agent. nghttp2 copies them all at submit.
    int build_headers(const Request& req, std::array<nghttp2_nv, kMaxRequestHeaders + 6>& nv, std::size_t& n,
                      char (&length)[24]) const noexcept {
        if (req.headers.size() > kMaxRequestHeaders || req.method.empty() || req.path.empty()) {
            return EINVAL;
        }
        n = 0;
        nv[n++] = detail::make_nv(":method", req.method);
        nv[n++] = detail::make_nv(":scheme", scheme_);
        nv[n++] = detail::make_nv(":authority", req.authority.empty() ? std::string_view(options_.authority)
                                                                      : req.authority);
        nv[n++] = detail::make_nv(":path", req.path);
        bool has_length = false;
        bool has_agent = false;
        for (const Header& h : req.headers) {
            const auto* name = reinterpret_cast<const uint8_t*>(h.name.data());
            const auto* value = reinterpret_cast<const uint8_t*>(h.value.data());
            if (h.name.empty() || h.name[0] == ':' || nghttp2_check_header_name(name, h.name.size()) == 0 ||
                nghttp2_check_header_value_rfc9113(value, h.value.size()) == 0 ||
                detail::is_connection_specific(h.name) || (h.name == "te" && h.value != "trailers")) {
                return EINVAL;
            }
            has_length = has_length || h.name == "content-length";
            has_agent = has_agent || h.name == "user-agent";
            nv[n++] = detail::make_nv(h.name, h.value);
        }
        if (!req.body.empty() && !has_length) {
            const auto r = std::to_chars(length, length + sizeof length, req.body.size());
            nv[n++] = detail::make_nv("content-length", std::string_view(length, static_cast<std::size_t>(r.ptr - length)));
        }
        if (!has_agent && !options_.user_agent.empty()) {
            nv[n++] = detail::make_nv("user-agent", options_.user_agent);
        }
        return 0;
    }

    // ---- one connection ---------------------------------------------------------

    class Connection;

    // The keepalive timer of a connection: owned jointly by the connection
    // and a scheduled callback (refs), so the connection never waits for a
    // lazily delivered kCancelled. The callback rings the writer under
    // `lock` while `conn` is set; the teardown clears `conn` under it.
    struct KeepaliveNode {
        typename Sched::Timer timer;
        Atomic<std::uint32_t> refs{1};  // the connection; +1 per schedule
        sync::SpinLock<SyncPolicy> lock;
        Connection* conn = nullptr;

        void unref() noexcept {
            if (refs.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                delete this;
            }
        }

        // Poller thread.
        static void on_timer(void* memo, poller::TimerEvent event, poller::TaskBatch&) noexcept {
            auto* node = static_cast<KeepaliveNode*>(memo);
            if (event == poller::TimerEvent::kFired) {
                std::lock_guard guard(node->lock);
                if (node->conn != nullptr) {
                    node->conn->keepalive_due();
                }
            }
            node->unref();
        }
    };

    class Connection {
    public:
        explicit Connection(BasicClient& client) noexcept
            : client_(client),
              options_(client.options_),
              session_(client.options_, client.counters_),
              read_buffer_(std::make_unique<char[]>(client.options_.read_buffer_bytes)) {}

        Connection(const Connection&) = delete;
        Connection& operator=(const Connection&) = delete;

        bool usable() const noexcept { return usable_.load(std::memory_order_acquire); }
        void ref() noexcept { refs_.fetch_add(1, std::memory_order_relaxed); }
        void unref() noexcept {
            if (refs_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                delete this;
            }
        }

        // Session + TCP (+ TLS, ALPN "h2"). 0 or errno; on error nothing
        // is left open.
        int connect(poller::TimePoint deadline) noexcept {
            Sched& sched = client_.scheduler_;
            poller::TimePoint until = sched.now() + options_.connect_timeout;
            if (has_deadline(deadline) && deadline < until) {
                until = deadline;
            }
            if (const int e = session_.init(); e != 0) {
                return e;
            }
            int e = ENOENT;
            if (options_.endpoint) {
                e = connect_to(*options_.endpoint, until);
            } else {
                const net::ResolveResult r = net::resolve(sched, options_.host, options_.port);
                if (!r) {
                    return r.error;
                }
                for (const net::Endpoint& ep : r) {
                    e = connect_to(ep, until);
                    if (e == 0 || e == ETIMEDOUT || e == ENOPROTOOPT) {
                        break;
                    }
                }
            }
            if (e == 0) {
                static_cast<void>(stream_.set_nodelay(true));
            }
            return e;
        }

        // Spawns the writer (rung: the preface and our SETTINGS go out at
        // once) and the reader.
        void start() noexcept {
            Sched& sched = client_.scheduler_;
            if (options_.ping_interval > poller::Duration::zero()) {
                keepalive_ = new KeepaliveNode();
                keepalive_->conn = this;
                keepalive_->timer.memo = keepalive_;
                keepalive_->timer.callback = &KeepaliveNode::on_timer;
                next_ping_ = sched.now() + options_.ping_interval;
            }
            bell_.ring();
            fiber::go(sched, [this]() noexcept { run_writer(); });
            fiber::go(sched, [this]() noexcept { run_reader(); });
        }

        // One request on this connection, in the caller's fiber. retry: the
        // connection took no new streams (nothing was sent).
        int exchange(const nghttp2_nv* nv, std::size_t n, std::string_view body, Response& res,
                     poller::TimePoint deadline, bool& retry) noexcept {
            Sched& sched = client_.scheduler_;
            fiber::BasicContext<Sched>* ctx = fiber::this_fiber::context<Sched>();
            typename CallT::Oneshot done;
            CallT call;
            call.promise = done.promise();
            call.response = &res;
            const bool timed = has_deadline(deadline);
            if (timed || ctx != nullptr) {
                // Also without a deadline under a context: its claim flag
                // decides between the stream's completion and the context.
                call.deadline = DeadlineNode::create(sched, &call);
            }
            int e;
            bool want;
            {
                std::lock_guard guard(mu_);
                e = session_.submit(call, nv, n, body);
                want = session_.want_write();
                if (e == EAGAIN) {
                    usable_.store(false, std::memory_order_release);
                }
            }
            if (e != 0) {
                if (call.deadline != nullptr) {
                    call.deadline->unref();  // never scheduled: both references
                    call.deadline->unref();
                }
                call.promise.abandon();
                static_cast<void>(done.get());
                retry = e == EAGAIN;
                return e;
            }
            if (timed) {
                call.deadline->timer.deadline = deadline;
                sched.pollers().schedule(call.deadline->timer);
            }
            if (want) {
                bell_.ring();
            }
            wait::Result<int> r = wait_call(done, call, ctx);
            const int result = r ? *r : ECONNRESET;
            if (result == ETIMEDOUT || result == ECANCELED) {
                // The timer or the context completed us: the stream may
                // still be live and the session may still point at `call`.
                // Detach + reset. (An ECANCELED from the connection itself
                // has detached it already: cancel() is a no-op then.)
                {
                    std::lock_guard guard(mu_);
                    session_.cancel(call);
                    want = session_.want_write();
                }
                if (want) {
                    bell_.ring();
                }
                res.clear();
            }
            if (call.deadline != nullptr) {
                if (timed) {
                    // Not fired yet: take it back (the callback then only
                    // drops its reference, later).
                    static_cast<void>(sched.pollers().cancel(call.deadline->timer));
                } else {
                    call.deadline->unref();  // never scheduled: the timer's reference
                }
                call.deadline->unref();
            }
            return result;
        }

        // Waits for the call's completion; under a context, also for the
        // context's end (which then completes it, see the top comment).
        static wait::Result<int> wait_call(typename CallT::Oneshot& done, CallT& call,
                                           fiber::BasicContext<Sched>* ctx) noexcept {
            if (ctx == nullptr) {
                return done.get();
            }
            // After submit: the claim flag is shared with the session now.
            fiber::InterruptScope<Sched> scope(*ctx, &call, &on_context_end);
            return done.get();
        }  // ~scope: delist (waits for a canceller still inside on_context_end)

        // The context's hook (any thread, or inline in wait_call): claims
        // the call like the deadline timer does, and completes it.
        static void on_context_end(void* self, wait::Error reason) noexcept {
            auto* call = static_cast<CallT*>(self);
            if (call->deadline->claim()) {
                // The caller waits on its Oneshot: its frame is alive until
                // this set() completes it (our last touch of it).
                call->promise.set(fiber::errno_of(reason));
            }
        }

        // close(): GOAWAY, then down; in-flight requests get ECANCELED.
        void close_gracefully() noexcept {
            set_loss(ECANCELED);
            usable_.store(false, std::memory_order_release);
            {
                std::lock_guard guard(mu_);
                session_.terminate();
            }
            static_cast<void>(stream_.set_write_deadline(client_.scheduler_.now() + options_.close_flush_timeout));
            bell_.ring();
        }

        // Drops the connection now (any fiber): the reader sees the end of
        // the stream and tears down; in-flight requests get `error`.
        void abort(int error) noexcept {
            set_loss(error);
            usable_.store(false, std::memory_order_release);
            static_cast<void>(stream_.transport().shutdown(SHUT_RDWR));
        }

        std::pair<std::size_t, std::size_t> stream_records() noexcept {
            std::lock_guard guard(mu_);
            return {session_.live_streams(), session_.pooled_records()};
        }

        // Poller thread (KeepaliveNode::on_timer, under its lock).
        void keepalive_due() noexcept {
            keepalive_tick_.store(true, std::memory_order_release);
            bell_.ring();
        }

        // The client's list (under the client's lock_).
        Connection* prev = nullptr;
        Connection* next = nullptr;
        bool close_requested = false;

    private:
        enum class Close : std::uint8_t { kOpen, kFlushThenClose, kNow };

        int connect_to(const net::Endpoint& ep, poller::TimePoint until) noexcept {
            Sched& sched = client_.scheduler_;
            if (options_.tls == nullptr) {
                return stream_.connect(sched, ep, until);
            }
            if (const int e = stream_.connect_tls(sched, ep, *options_.tls, options_.host.c_str(), until); e != 0) {
                return e;
            }
            if (stream_.tls_stream().alpn() != "h2") {
                static_cast<void>(stream_.close());
                return ENOPROTOOPT;
            }
            return 0;
        }

        void set_loss(int error) noexcept {
            int expected = 0;
            loss_.compare_exchange_strong(expected, error, std::memory_order_relaxed);
        }

        void run_writer() noexcept {
            std::string out;
            out.reserve(options_.write_chunk_bytes + 32 * 1024);
            bool write_failed = false;
            bool shut = false;
            if (keepalive_ != nullptr) {
                schedule_keepalive(next_ping_);
            }
            for (;;) {
                bell_.wait();
                for (;;) {
                    if (keepalive_ != nullptr && keepalive_tick_.exchange(false, std::memory_order_acquire)) {
                        keepalive();
                    }
                    out.clear();
                    Close close;
                    bool finished;
                    {
                        std::lock_guard guard(mu_);
                        close = close_;
                        if (close != Close::kNow) {
                            session_.fill(out, options_.write_chunk_bytes);
                        }
                        finished = session_.finished();
                    }
                    if (close == Close::kNow) {
                        writer_exited_.set();
                        return;
                    }
                    if (!out.empty()) {
                        if (!write_failed && !stream_.write_all(out.data(), out.size())) {
                            write_failed = true;
                            set_loss(ECONNRESET);
                            static_cast<void>(stream_.transport().shutdown(SHUT_RDWR));  // the reader tears down
                        }
                        continue;  // more may be pending
                    }
                    if (close == Close::kFlushThenClose) {
                        writer_exited_.set();
                        return;
                    }
                    if (finished && !shut) {
                        // Nothing left to do (GOAWAY exchanged, every stream
                        // finished, all flushed): wake the reader with end
                        // of stream.
                        shut = true;
                        static_cast<void>(stream_.transport().shutdown(SHUT_RDWR));
                    }
                    break;
                }
            }
        }

        // The writer's keepalive step: send a PING when due, drop the
        // connection when the last one is unanswered for ping_timeout.
        void keepalive() noexcept {
            Sched& sched = client_.scheduler_;
            const poller::TimePoint now = sched.now();
            bool dead = false;
            {
                std::lock_guard guard(mu_);
                if (ping_outstanding_ && session_.ping_acks() != acks_at_ping_) {
                    ping_outstanding_ = false;
                }
                if (ping_outstanding_) {
                    dead = now >= ping_sent_ + options_.ping_timeout;
                } else if (now >= next_ping_) {
                    if (session_.ping()) {
                        ping_outstanding_ = true;
                        ping_sent_ = now;
                        acks_at_ping_ = session_.ping_acks();
                    }
                    next_ping_ = now + options_.ping_interval;
                }
            }
            if (dead) {
                Counters::bump(client_.counters_.keepalive_failures);
                abort(ECONNRESET);
                return;
            }
            // Next: the next PING, or the ACK deadline of the outstanding
            // one, whichever is first (a PING due while one is unanswered
            // waits for its ACK or its deadline).
            poller::TimePoint at = next_ping_;
            if (ping_outstanding_) {
                const poller::TimePoint ack_deadline = ping_sent_ + options_.ping_timeout;
                at = next_ping_ > now && next_ping_ < ack_deadline ? next_ping_ : ack_deadline;
            }
            schedule_keepalive(at);
        }

        // The writer only (the node is idle: never scheduled, or its
        // callback has run -- it set keepalive_tick_ before we got here).
        void schedule_keepalive(poller::TimePoint at) noexcept {
            keepalive_->refs.fetch_add(1, std::memory_order_relaxed);
            keepalive_->timer.deadline = at;
            client_.scheduler_.pollers().schedule(keepalive_->timer);
        }

        void run_reader() noexcept {
            bool fatal = false;
            for (;;) {
                const net::IoResult r = stream_.read_some(read_buffer_.get(), options_.read_buffer_bytes);
                if (!r || r.bytes == 0) {
                    set_loss(ECONNRESET);
                    break;
                }
                int rc;
                bool want;
                bool finished;
                {
                    std::lock_guard guard(mu_);
                    rc = session_.recv(read_buffer_.get(), r.bytes);
                    want = session_.want_write();
                    finished = session_.finished();
                    if (!session_.accepting()) {
                        usable_.store(false, std::memory_order_release);
                    }
                }
                if (want) {
                    bell_.ring();
                }
                if (rc != 0) {
                    fatal = true;
                    break;
                }
                if (finished) {
                    break;  // e.g. the peer's GOAWAY and no stream left
                }
            }
            teardown(fatal);
        }

        void teardown(bool fatal) noexcept {
            Sched& sched = client_.scheduler_;
            usable_.store(false, std::memory_order_release);
            if (fatal) {
                // Let the writer flush what nghttp2 queued (its GOAWAY).
                {
                    std::lock_guard guard(mu_);
                    close_ = Close::kFlushThenClose;
                }
                static_cast<void>(stream_.set_write_deadline(sched.now() + options_.close_flush_timeout));
                bell_.ring();
                writer_exited_.wait();
            }
            {
                std::lock_guard guard(mu_);
                close_ = Close::kNow;
                int loss = loss_.load(std::memory_order_relaxed);
                session_.lost(session_.loss_error(loss != 0 ? loss : ECONNRESET));
            }
            bell_.ring();
            static_cast<void>(stream_.transport().shutdown(SHUT_RDWR));
            writer_exited_.wait();
            if (keepalive_ != nullptr) {
                {
                    std::lock_guard guard(keepalive_->lock);
                    keepalive_->conn = nullptr;  // a callback running now has rung already / will not
                }
                static_cast<void>(sched.pollers().cancel(keepalive_->timer));
                keepalive_->unref();
                keepalive_ = nullptr;
            }
            static_cast<void>(stream_.close());
            BasicClient& client = client_;
            const bool was_current = client.unregister(this);
            if (was_current) {
                unref();  // the client's reference (current_)
            }
            unref();                      // the loops' reference: may destroy *this
            client.connections_.done();   // the last touch of the client
        }

        BasicClient& client_;
        const ClientOptions& options_;
        Stream stream_;
        Mutex mu_;  // session_, close_, and the Responses / Calls of live streams
        detail::Session<Sched> session_;
        detail::Doorbell<Sched> bell_;
        fiber::BasicEvent<Sched> writer_exited_;
        std::unique_ptr<char[]> read_buffer_;
        Close close_ = Close::kOpen;
        Atomic<bool> usable_{true};
        Atomic<int> loss_{0};  // the error in-flight requests get at teardown (first one wins)
        Atomic<std::uint32_t> refs_{1};  // the loops (teardown drops it)
        // keepalive (writer fiber; teardown after the writer exited)
        KeepaliveNode* keepalive_ = nullptr;
        Atomic<bool> keepalive_tick_{false};
        poller::TimePoint next_ping_{};
        poller::TimePoint ping_sent_{};
        std::uint64_t acks_at_ping_ = 0;
        bool ping_outstanding_ = false;
    };

    // ---- connections -------------------------------------------------------------

    // A usable connection with a reference for the caller, or nullptr with
    // `error` (closed: ECANCELED; or why connecting failed).
    Connection* acquire(poller::TimePoint deadline, int& error) noexcept {
        if (Connection* c = current_ref(); c != nullptr) {
            return c;
        }
        std::lock_guard connect_guard(connect_mu_);
        if (Connection* c = current_ref(); c != nullptr) {
            return c;
        }
        {
            std::lock_guard guard(lock_);
            if (closed_) {
                error = ECANCELED;
                return nullptr;
            }
        }
        auto* conn = new Connection(*this);
        if (const int e = conn->connect(deadline); e != 0) {
            delete conn;
            Counters::bump(counters_.connect_failures);
            error = e;
            return nullptr;
        }
        Counters::bump(counters_.connects);
        ever_connected_ = true;
        connections_.add(1);
        Connection* old;
        {
            std::lock_guard guard(lock_);
            conn->next = head_;
            if (head_ != nullptr) {
                head_->prev = conn;
            }
            head_ = conn;
            conn->ref();  // the client's (current_)
            conn->ref();  // the caller's
            old = std::exchange(current_, conn);
        }
        if (old != nullptr) {
            old->unref();
        }
        conn->start();
        return conn;
    }

    Connection* current_ref() noexcept {
        std::lock_guard guard(lock_);
        if (current_ != nullptr && current_->usable()) {
            current_->ref();
            return current_;
        }
        return nullptr;
    }

    // true: it was current_ (the caller drops that reference).
    bool unregister(Connection* conn) noexcept {
        std::lock_guard guard(lock_);
        if (conn->prev != nullptr) {
            conn->prev->next = conn->next;
        } else {
            head_ = conn->next;
        }
        if (conn->next != nullptr) {
            conn->next->prev = conn->prev;
        }
        if (current_ == conn) {
            current_ = nullptr;
            return true;
        }
        return false;
    }

    Sched& scheduler_;
    ClientOptions options_;
    std::string_view scheme_;
    Counters counters_;

    Mutex connect_mu_;               // one connect at a time; close()
    sync::SpinLock<SyncPolicy> lock_;  // current_, the list, closed_
    Connection* current_ = nullptr;
    Connection* head_ = nullptr;
    bool closed_ = false;
    bool ever_connected_ = false;    // under connect_mu_
    fiber::BasicWaitGroup<Sched> connections_;
};

using Client = BasicClient<sched::Scheduler>;

}  // namespace fibers::http2
