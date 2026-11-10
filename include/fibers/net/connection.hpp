#pragma once

// Connection: a byte stream with per-operation timeouts that starts as
// plaintext TCP and can switch to TLS in place on the same socket.
//
//   tls::Connection c;                                  // fibers/tls/connection.hpp
//   c.set_io_timeout(5s);                               // every read / write: now + 5s
//   c.connect(scheduler, endpoint);                     // the timeout bounds it too
//   c.write_all(ssl_request, 8);                        // Postgres SSLRequest, plaintext
//   char answer;  c.read_exact(&answer, 1);             // 'S'
//   c.start_tls_client(client_ctx, "db.example.com");   // handshake under the timeout
//   net::BufferedReader reader(c, storage);             // works on either phase
//
//   net::Connection plain;                              // no TLS (net only)
//   plain.adopt(scheduler, fd);  listener.accept(plain.transport());
//
// BasicConnection<Sched, Tls> holds ONE stream object: with Tls = NoTls a
// net::BasicStream, otherwise the TLS stream (tls::BasicTlsStream), which
// owns its socket (transport()). Until start_tls_* the connection reads and
// writes that transport directly; afterwards through TLS. So the upgrade
// moves nothing (the socket is registered with its poller in place, and a
// Stream cannot move anyway), and there is no virtual call or variant
// dispatch: one predictable branch on is_tls() per operation. The price of
// TLS capability is the TLS stream's footprint (a few hundred bytes: three
// fiber mutexes, pointers; OpenSSL's own state is allocated only by
// start_tls_*) also in connections that stay plaintext.
//
// The I/O surface is net::Stream's (read_some / read_exact / write_some /
// write_all -> IoResult, deadlines, shutdown, close), so it satisfies
// http::ByteStream and net::ReadStream: BufferedReader and the http code
// take it as is.
//
// Timeouts
// --------
// set_io_timeout(d), d > 0: every operation first sets its direction's
// deadline to now + d (one clock read and, since deadlines only move later,
// a few atomics: see "Deadlines" in poll_desc.hpp; a timer round trip only
// when the deadline moves earlier). The deadline covers the whole call: a
// write_all / read_exact of many bytes has d for all of them; through a
// BufferedReader, every fill is a read_some and gets its own d (an
// inactivity timeout). connect() without an explicit deadline and
// start_tls_* use now + d as well. d == 0 (the default): no automatic
// deadline.
// Explicit absolute deadlines (set_read_deadline / set_write_deadline /
// set_deadline, Scheduler::now()'s clock) still work: without an io timeout
// they are passed to the socket as is (Go semantics, sticky until moved);
// with one, every operation uses the earlier of the two. Errors are the
// stream's: ETIMEDOUT once a deadline passed (the connection stays usable;
// after a timeout in the middle of a protocol exchange it is usually not,
// see ConnectionPool's broken connections).
// With TLS a read may also have to write (a KeyUpdate answer, an alert), so
// reads arm the write deadline too. In full duplex this means a writer
// stuck on a peer that does not read is bounded by d after the last read
// that started, not after its own start.
//
// Plaintext -> TLS
// ----------------
// start_tls_client(ctx, server_name, already_received) and
// start_tls_server(ctx, already_received): upgrade_* + handshake on the
// open socket, under the io timeout (or an explicit deadline). On any error
// the connection is closed (its protocol state is gone). already_received:
// bytes a BufferedReader read past the plaintext part (take_buffered()).
// On the server side that is the client's pipelined ClientHello, normal.
// On the client side of a protocol where the server says nothing until the
// client's ClientHello (Postgres SSLRequest, STARTTLS) any such byte was
// not protected by TLS and may have been injected by a man in the middle
// (CVE-2021-23222): check reader.buffered() == 0 and refuse instead.
//
// Concurrency: as the underlying stream (one reader fiber + one writer
// fiber at a time; full duplex with TLS too). set_io_timeout, connect,
// adopt, accept and start_tls_* must not run concurrently with any other
// operation. Deadline setters: any thread, like Stream's.
//
// Lifetime: like net::Stream, neither copyable nor movable, reusable after
// close() (connect / adopt / accept again: plaintext again until
// start_tls_*). The destructor closes. No allocation in this code.

#include <sys/socket.h>

#include <algorithm>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <span>
#include <type_traits>

#include "fibers/net/socket.hpp"
#include "fibers/poller/sources.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fibers::net {

// BasicConnection's Tls parameter for plaintext-only connections.
struct NoTls {};

// What BasicConnection needs from a TLS stream (tls::BasicTlsStream<Sched>):
// it owns the socket (transport()), upgrades it in place and has
// net::Stream's I/O surface.
template <class T, class Sched>
concept TlsLayer = requires(T& t, void* buffer, const void* data, std::size_t size, int how) {
    { t.transport() } noexcept -> std::same_as<BasicStream<Sched>&>;
    { t.handshake() } noexcept -> std::same_as<int>;
    { t.read_some(buffer, size) } noexcept -> std::same_as<IoResult>;
    { t.read_exact(buffer, size) } noexcept -> std::same_as<IoResult>;
    { t.write_some(data, size) } noexcept -> std::same_as<IoResult>;
    { t.write_all(data, size) } noexcept -> std::same_as<IoResult>;
    { t.shutdown(how) } noexcept -> std::same_as<int>;
    { t.close() } noexcept -> std::same_as<int>;
};

template <NetScheduler Sched = sched::Scheduler, class Tls = NoTls>
    requires std::same_as<Tls, NoTls> || TlsLayer<Tls, Sched>
class BasicConnection {
public:
    using Stream = BasicStream<Sched>;
    using Listener = BasicListener<Sched>;
    using TlsStream = Tls;

    static constexpr bool kHasTls = !std::same_as<Tls, NoTls>;

    BasicConnection() noexcept = default;
    BasicConnection(const BasicConnection&) = delete;
    BasicConnection& operator=(const BasicConnection&) = delete;

    ~BasicConnection() { static_cast<void>(close()); }

    // ---- opening (the connection must be closed) ---------------------------------
    //
    // Every open starts plaintext, without explicit deadlines (the io
    // timeout stays).

    // TCP connect (net::Stream::connect). `deadline` (absolute): give up then
    // with ETIMEDOUT; TimePoint{}: now + io timeout if one is set, else none.
    // From a fiber of `scheduler`. 0 or errno; on error the connection stays
    // closed.
    int connect(Sched& scheduler, const Endpoint& peer, poller::TimePoint deadline = {}) noexcept {
        reset();
        if (!has_deadline(deadline) && io_timeout_.count() > 0) {
            deadline = scheduler.now() + io_timeout_;
        }
        return transport().connect(scheduler, peer, deadline);  // clears its deadline on success
    }

    // Takes over a connected socket (socketpair, an inherited fd). 0 or errno
    // (then the caller still owns fd).
    int adopt(Sched& scheduler, int fd) noexcept {
        reset();
        return transport().adopt(scheduler, fd);
    }

    // listener.accept() into this connection (plaintext; start_tls_server
    // for TLS, in the connection's own fiber).
    int accept(Listener& listener, Endpoint* peer = nullptr) noexcept {
        reset();
        return listener.accept(transport(), peer);
    }

    // ---- TLS (Tls != NoTls) ----------------------------------------------------------

    // connect() + start_tls_client(): TLS from the first byte. `deadline`
    // bounds both (TimePoint{}: the io timeout bounds each).
    template <class Ctx>
        requires kHasTls
    int connect_tls(Sched& scheduler, const Endpoint& peer, Ctx& ctx, const char* server_name,
                    poller::TimePoint deadline = {}) noexcept {
        if (const int e = connect(scheduler, peer, deadline); e != 0) {
            return e;
        }
        return start_tls_client(ctx, server_name, {}, deadline);
    }

    // Switches the open plaintext connection to TLS as the client:
    // upgrade_client + handshake. server_name: verified against the
    // certificate and sent as SNI (see TlsStream::upgrade_client).
    // already_received: see "Plaintext -> TLS" above. `deadline` bounds the
    // handshake (TimePoint{}: now + io timeout, or none). 0 or errno (EINVAL
    // for an unusable context; ETIMEDOUT; EPROTO with tls_stream().
    // error_string() for TLS failures); on error the connection is closed.
    template <class Ctx>
        requires kHasTls
    int start_tls_client(Ctx& ctx, const char* server_name, std::span<const std::byte> already_received = {},
                         poller::TimePoint deadline = {}) noexcept {
        if (tls_active_) {
            return EALREADY;
        }
        return finish_upgrade(layer_.upgrade_client(ctx, server_name, already_received), deadline);
    }

    // The same as the server (STARTTLS / SSLRequest servers, or TLS from the
    // first byte after accept()).
    template <class Ctx>
        requires kHasTls
    int start_tls_server(Ctx& ctx, std::span<const std::byte> already_received = {},
                         poller::TimePoint deadline = {}) noexcept {
        if (tls_active_) {
            return EALREADY;
        }
        return finish_upgrade(layer_.upgrade_server(ctx, already_received), deadline);
    }

    // TLS is active (start_tls_* succeeded since the last open).
    bool is_tls() const noexcept { return tls_active_; }

    // The TLS stream (alpn(), peer_certificate(), error_string(), ...).
    Tls& tls_stream() noexcept
        requires kHasTls
    {
        return layer_;
    }

    // ---- I/O (net::Stream's surface) ----------------------------------------------

    IoResult read_some(void* buffer, std::size_t size) noexcept {
        arm_read();
        if constexpr (kHasTls) {
            if (tls_active_) {
                return layer_.read_some(buffer, size);
            }
        }
        return transport().read_some(buffer, size);
    }

    IoResult read_exact(void* buffer, std::size_t size) noexcept {
        arm_read();
        if constexpr (kHasTls) {
            if (tls_active_) {
                return layer_.read_exact(buffer, size);
            }
        }
        return transport().read_exact(buffer, size);
    }

    IoResult write_some(const void* buffer, std::size_t size) noexcept {
        arm(Direction::kWrite);
        if constexpr (kHasTls) {
            if (tls_active_) {
                return layer_.write_some(buffer, size);
            }
        }
        return transport().write_some(buffer, size);
    }

    IoResult write_all(const void* buffer, std::size_t size) noexcept {
        arm(Direction::kWrite);
        if constexpr (kHasTls) {
            if (tls_active_) {
                return layer_.write_all(buffer, size);
            }
        }
        return transport().write_all(buffer, size);
    }

    // SHUT_WR (default) / SHUT_RDWR: with TLS, close_notify first (under the
    // write timeout). 0 or errno.
    int shutdown(int how = SHUT_WR) noexcept {
        if constexpr (kHasTls) {
            if (tls_active_) {
                if (how != SHUT_RD) {
                    arm(Direction::kWrite);
                }
                return layer_.shutdown(how);
            }
        }
        return transport().shutdown(how);
    }

    // Closes the socket (abrupt for TLS: shutdown() first for close_notify);
    // fibers parked in an operation wake with ECANCELED. 0 or EBADF. Any
    // thread.
    int close() noexcept { return transport().close(); }

    // ---- timeouts and deadlines -----------------------------------------------------

    // See "Timeouts" above. 0 turns it off (the explicit deadlines are put
    // back on the socket). Not concurrently with operations.
    void set_io_timeout(poller::Duration timeout) noexcept {
        io_timeout_ = timeout.count() > 0 ? timeout : poller::Duration::zero();
        if (transport().is_open_approx()) {
            static_cast<void>(restore_deadlines());
        }
    }

    template <class Rep, class Period>
    void set_io_timeout(std::chrono::duration<Rep, Period> timeout) noexcept {
        set_io_timeout(std::chrono::duration_cast<poller::Duration>(timeout));
    }

    poller::Duration io_timeout() const noexcept { return io_timeout_; }

    // Absolute deadlines (Stream::set_read_deadline's semantics; with an io
    // timeout the earlier of the two applies). TimePoint{}: none. Any
    // thread. 0, EBADF (not open) or ECANCELED (closing).
    int set_read_deadline(poller::TimePoint deadline) noexcept { return set_explicit(Direction::kRead, deadline); }
    int set_write_deadline(poller::TimePoint deadline) noexcept {
        return set_explicit(Direction::kWrite, deadline);
    }

    int set_deadline(poller::TimePoint deadline) noexcept {
        if (const int e = set_read_deadline(deadline); e != 0) {
            return e;
        }
        return set_write_deadline(deadline);
    }

    // ---- socket ----------------------------------------------------------------------

    // The socket: options, native_handle(), poll_desc(). Do not read or
    // write it directly while TLS is active.
    Stream& transport() noexcept {
        if constexpr (kHasTls) {
            return layer_.transport();
        } else {
            return layer_;
        }
    }

    int set_nodelay(bool on = true) noexcept { return transport().set_nodelay(on); }

    int set_keepalive(std::chrono::seconds idle, std::chrono::seconds interval, int count) noexcept {
        return transport().set_keepalive(idle, interval, count);
    }

    int set_user_timeout(std::chrono::milliseconds timeout) noexcept {
        return transport().set_user_timeout(timeout);
    }

    bool is_open_approx() noexcept { return transport().is_open_approx(); }
    int native_handle() noexcept { return transport().native_handle(); }

private:
    using Rep = poller::TimePoint::rep;
    using Atomic = typename Sched::SyncPolicy::template Atomic<Rep>;

    static constexpr Rep kNone = 0;  // TimePoint{}: no deadline

    static bool has_deadline(poller::TimePoint t) noexcept {
        return t != poller::TimePoint{} && t != poller::TimePoint::max();
    }

    static Rep encode(poller::TimePoint t) noexcept { return has_deadline(t) ? t.time_since_epoch().count() : kNone; }

    static poller::TimePoint decode(Rep r) noexcept {
        return r == kNone ? poller::TimePoint{} : poller::TimePoint(poller::TimePoint::duration(r));
    }

    // Before an open.
    void reset() noexcept {
        tls_active_ = false;
        explicit_of(Direction::kRead).store(kNone, std::memory_order_relaxed);
        explicit_of(Direction::kWrite).store(kNone, std::memory_order_relaxed);
    }

    Atomic& explicit_of(Direction dir) noexcept { return explicit_[static_cast<int>(dir)]; }

    // The earlier of now + io timeout and the explicit deadline.
    poller::TimePoint effective(Direction dir) noexcept {
        const poller::TimePoint by_timeout = transport().poll_desc().scheduler().now() + io_timeout_;
        const Rep e = explicit_of(dir).load(std::memory_order_relaxed);
        return e != kNone ? std::min(by_timeout, decode(e)) : by_timeout;
    }

    // Before an operation of `dir`: the per-operation deadline. Errors
    // (EBADF, ECANCELED) are left to the operation itself.
    void arm(Direction dir) noexcept {
        if (io_timeout_.count() > 0 && transport().is_open_approx()) {
            static_cast<void>(transport().set_deadline(dir, effective(dir)));
        }
    }

    void arm_read() noexcept {
        arm(Direction::kRead);
        if constexpr (kHasTls) {
            if (tls_active_) {
                arm(Direction::kWrite);  // the reader may write control records
            }
        }
    }

    int set_explicit(Direction dir, poller::TimePoint deadline) noexcept {
        explicit_of(dir).store(encode(deadline), std::memory_order_relaxed);
        if (io_timeout_.count() > 0 && transport().is_open_approx()) {
            return transport().set_deadline(dir, effective(dir));
        }
        return transport().set_deadline(dir, deadline);
    }

    // The explicit deadlines (or none) back on the socket, e.g. after a
    // deadline-bounded connect or handshake.
    int restore_deadlines() noexcept {
        Stream& s = transport();
        if (const int e = s.set_deadline(Direction::kRead, decode(explicit_of(Direction::kRead).load(
                                                               std::memory_order_relaxed)));
            e != 0) {
            return e;
        }
        return s.set_deadline(Direction::kWrite, decode(explicit_of(Direction::kWrite).load(std::memory_order_relaxed)));
    }

    // After upgrade_*: the handshake under `deadline` (or the io timeout).
    int finish_upgrade(int error, poller::TimePoint deadline) noexcept
        requires kHasTls
    {
        if (error == 0) {
            if (!has_deadline(deadline) && io_timeout_.count() > 0) {
                deadline = transport().poll_desc().scheduler().now() + io_timeout_;
            }
            if (has_deadline(deadline)) {
                error = transport().set_deadline(deadline);
            }
        }
        if (error == 0) {
            error = layer_.handshake();
        }
        if (error == 0) {
            error = restore_deadlines();
        }
        if (error != 0) {
            static_cast<void>(close());  // error_string() still describes a failed handshake
            return error;
        }
        tls_active_ = true;
        return 0;
    }

    std::conditional_t<kHasTls, Tls, Stream> layer_;
    poller::Duration io_timeout_{0};
    Atomic explicit_[2]{Atomic{kNone}, Atomic{kNone}};
    bool tls_active_ = false;
};

// Plaintext only (no OpenSSL needed). The TLS-capable one is
// tls::Connection (fibers/tls/connection.hpp).
using Connection = BasicConnection<sched::Scheduler>;

}  // namespace fibers::net
