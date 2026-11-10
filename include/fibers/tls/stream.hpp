#pragma once

// TlsStream: TLS over a net::Stream for fibers (OpenSSL 3, memory BIOs).
//
//   tls::TlsStream s;                                       // caller-owned, like net::Stream
//   if (int e = s.connect(scheduler, ep, client_ctx, "localhost"); e != 0) { ... }
//   s.write_all(req, n);
//   net::IoResult r = s.read_some(buf, sizeof buf);         // r.bytes == 0 && !r.error: close_notify
//   s.shutdown();                                           // close_notify, then TCP FIN
//   s.close();
//
//   // server: accept into the stream, then handshake in the connection's fiber
//   listener.accept(s.transport());  s.upgrade_server(server_ctx);  s.handshake();
//
//   // STARTTLS / Postgres SSLRequest: plaintext on transport() first, then
//   s.upgrade_client(ctx, "db.example.com");  s.handshake();
//
// The I/O surface is net::Stream's (read_some / read_exact / write_some /
// write_all -> net::IoResult, deadlines, shutdown, close), so code templated
// on the stream type takes either.
//
// The BIO pump
// ------------
// OpenSSL never sees the socket. The SSL object is bound to one half of a
// BIO pair (two ring buffers, allocated by OpenSSL per connection, sizes
// from the context's options); we own the other half ("net"). OpenSSL only
// ever reports WANT_READ / WANT_WRITE; our code moves ciphertext:
//
//   inbound   socket --read_some--> [net -> ssl ring] --SSL_read--> caller
//   outbound  caller --SSL_write--> [ssl -> net ring] --write_all--> socket
//
// Zero copy on our side: BIO_nwrite0 / BIO_nread0 hand out a pointer into
// the ring, and the socket syscall reads into / sends from it directly; the
// ring's bookkeeping (BIO_nwrite / BIO_nread) is committed afterwards.
// Parking happens only in the transport calls, on the net::Stream (so its
// deadlines and close() apply to the handshake and to every read and write).
//
// OpenSSL's error queue is thread-local, and a fiber may resume on another
// worker after any park. Rule: every OpenSSL call runs inside one critical
// section of ssl_mu_ that never parks: ERR_clear_error(), the call,
// SSL_get_error() and the error-queue read (into last_error_), then
// ERR_clear_error() again, all on one thread. The only park points are the
// transport calls and acquiring a mutex, and both happen with no OpenSSL
// state pending. errno is not involved: OpenSSL never does I/O here, and the
// transport returns its errors as values.
//
// Errors (net::IoResult::error / return values; never exceptions)
// ----------------------------------------------------------------
//   EPROTO        TLS failure: handshake failure, certificate verification
//                 failed (verify_result() != X509_V_OK says which check),
//                 fatal alert from the peer (e.g. "certificate required"),
//                 bad record MAC, ALPN mismatch. Sticky: the connection is
//                 dead; last_error() / error_string() give OpenSSL's reason.
//   ECONNABORTED  the transport ended without close_notify (a truncation;
//                 or the peer died mid-handshake). Sticky. With
//                 CommonOptions::ignore_unexpected_eof it is end of stream.
//   ENOTCONN      no TLS session set up (upgrade_* / connect / accept first).
//   EPIPE         write after shutdown() sent close_notify.
//   anything else comes from the transport as is: ETIMEDOUT (a deadline;
//                 NOT sticky -- the TLS state is intact, nothing was lost:
//                 move the deadline and call again), ECANCELED (close()d
//                 while waiting), EBADF (closed), ECONNRESET, EPIPE, ...
//   End of stream: read_some -> {0, 0} once the peer's close_notify arrived.
//   write_some / write_all report the plaintext bytes OpenSSL accepted; on a
//   transport error some of them may still be queued (encrypted) in the
//   ring: they go out with the next successful write / shutdown.
//
// Concurrency: one reader fiber + one writer fiber at a time (full duplex)
// ------------------------------------------------------------------------
// HTTP/1.1 is sequential, but HTTP/2 (and most proxies) need a reader and a
// writer running at once, so this is supported. Three fiber::Mutex:
//
//   read_mu_   the read role: read_some / read_exact; the only party that
//              parks on transport reads and fills the inbound ring. A second
//              reader waits for the first (no fail-fast).
//   write_mu_  the write role ("flusher"): write_*, shutdown, key_update;
//              the only party that parks on transport writes and drains the
//              outbound ring.
//   ssl_mu_    the SSL object and the ring bookkeeping; held only around
//              OpenSSL calls (never across a park).
//   Order: read_mu_ -> write_mu_ -> ssl_mu_. handshake() holds both roles.
//
// A reader can produce output too (TLS 1.3 KeyUpdate responses, which
// OpenSSL would otherwise defer to the next SSL_write -- never, for a
// read-only connection -- so the reader writes them at once; alerts).
// It never waits for the write role for that: it try_lock()s it and
// flushes only if nobody holds it; otherwise the holder flushes it. The
// holder releases the role with "unlock, re-check the ring under ssl_mu_,
// try_lock again if non-empty", so output appended while it held the role
// cannot be stranded (either the re-check sees it, or the appender's
// try_lock comes after the unlock). Writers flush whenever the ring has
// less than one record plus kControlRoom free, so a reader's control
// messages always fit (OpenSSL would otherwise report WANT_WRITE inside
// SSL_read and the reader would have to wait for the writer, which may be
// waiting for a peer that waits for us). Renegotiation is disabled (the
// context sets SSL_OP_NO_RENEGOTIATION), so SSL_write never needs to read:
// the inbound ring is touched only by the reader, which is why it may read
// from the socket into it outside ssl_mu_.
//
// The handshake runs implicitly on the first read / write if handshake()
// was not called (like Go's tls.Conn); a concurrent reader and writer wait
// for it on their roles. Call handshake() explicitly to get its error and
// to read alpn() / peer_subject() before any I/O.
//
// Lifetime: like net::Stream, neither copyable nor movable; reusable
// (close(), then connect / accept / upgrade_* again). close() only closes
// the transport (waking parked fibers with ECANCELED); the SSL object is
// freed by the next upgrade_* or the destructor, so a fiber still finishing
// an operation never touches freed memory. Operations wait (park) only
// inside fibers of Sched; the accessors may be called from anywhere once
// handshake() returned 0 (they read state captured at that point).
//
// Stack
// -----
// OpenSSL runs on the calling fiber's stack. Measured by painting the stack
// (tls_test StackDepth; OpenSSL 3.5.9, x86-64, bytes below the caller's
// frame, same in our release and debug builds):
//
//   client handshake   ~10.9 KiB   (EC P-256 or RSA-2048 server, with or
//   server handshake    ~7.8 KiB    without mTLS: the deepest path is the
//   write (16 KiB)      ~6.4 KiB    X25519MLKEM768 key share, not the
//   read (16 KiB)       ~1.8 KiB    signature)
//
// So the default 64 KiB fiber stack is enough for TLS plus a typical
// connection handler (e.g. a 16 KiB buffer on the stack): keep at least
// ~24 KiB free below the frame that calls into TlsStream, which is what
// CommonOptions::stack_reserve (default 24 KiB) enforces. Before every
// call into OpenSSL the stream checks that this much is left on the current
// fiber's stack and fails fast with a message if not (a frame bigger than
// the guard page could jump over it and corrupt memory silently). Sanitizer
// builds need several times more (the tests use 1 MiB stacks there).
//
// No allocation in this code; OpenSSL allocates internally per connection
// (SSL object, record buffers, the BIO pair).

#include <arpa/inet.h>
#include <netinet/in.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <span>
#include <string_view>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/mutex.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/tls/context.hpp"

namespace fibers::tls {

using net::IoResult;

template <net::NetScheduler Sched = sched::Scheduler>
class BasicTlsStream {
public:
    using Stream = net::BasicStream<Sched>;
    using Listener = net::BasicListener<Sched>;
    using Mutex = fiber::BasicMutex<Sched>;
    using Fiber = fiber::BasicFiber<Sched>;

    // Plaintext bytes per SSL_write (one record).
    static constexpr std::size_t kRecordPlaintext = 16384;

    BasicTlsStream() noexcept = default;
    BasicTlsStream(const BasicTlsStream&) = delete;
    BasicTlsStream& operator=(const BasicTlsStream&) = delete;

    ~BasicTlsStream() {
        static_cast<void>(transport_.close());
        reset();
    }

    // The underlying socket: connect / accept / adopt into it, and use it
    // directly for a plaintext phase before upgrade_*. Do not touch it while
    // TLS is active (except for socket options).
    Stream& transport() noexcept { return transport_; }

    // ---- setup -----------------------------------------------------------------

    // transport().connect + upgrade_client + handshake. On error the stream
    // is closed again. From a fiber of `scheduler`.
    int connect(Sched& scheduler, const net::Endpoint& peer, TlsContext& ctx, const char* server_name) noexcept {
        if (const int e = transport_.connect(scheduler, peer); e != 0) {
            return e;
        }
        int e = upgrade_client(ctx, server_name);
        if (e == 0) {
            e = handshake();
        }
        if (e != 0) {
            static_cast<void>(transport_.close());
        }
        return e;
    }

    // The same, with `deadline` (absolute, Scheduler::now()'s clock) for the
    // whole of it: the TCP connect (net::Stream::connect with a deadline)
    // and the handshake (both transport deadlines). ETIMEDOUT once it has
    // passed (at once, with no socket, if it already has); the stream is
    // closed again on any error. On success both deadlines are cleared.
    // TimePoint{} / TimePoint::max(): no deadline.
    int connect(Sched& scheduler, const net::Endpoint& peer, TlsContext& ctx, const char* server_name,
                poller::TimePoint deadline) noexcept {
        if (const int e = transport_.connect(scheduler, peer, deadline); e != 0) {
            return e;
        }
        const bool has_deadline = deadline != poller::TimePoint{} && deadline != poller::TimePoint::max();
        int e = has_deadline ? transport_.set_deadline(deadline) : 0;
        if (e == 0) {
            e = upgrade_client(ctx, server_name);
        }
        if (e == 0) {
            e = handshake();
        }
        if (e == 0 && has_deadline) {
            e = transport_.set_deadline(poller::TimePoint{});
        }
        if (e != 0) {
            static_cast<void>(transport_.close());  // error_string() still describes a failed handshake
        }
        return e;
    }

    // listener.accept(transport()) + upgrade_server. No handshake (run it in
    // the connection's own fiber, not in the accept loop).
    int accept(Listener& listener, TlsContext& ctx, net::Endpoint* peer = nullptr) noexcept {
        if (const int e = listener.accept(transport_, peer); e != 0) {
            return e;
        }
        const int e = upgrade_server(ctx);
        if (e != 0) {
            static_cast<void>(transport_.close());
        }
        return e;
    }

    // Starts TLS as the client on the connected transport (fresh connection
    // or after a plaintext exchange). server_name: verified against the
    // certificate (DNS name or IP literal; required when the context
    // verifies peers) and sent as SNI (DNS names). already_received:
    // ciphertext the caller read from the transport while it was plaintext
    // (a buffered reader that overshot); at most read_buffer_size bytes.
    // 0, EINVAL (bad arguments / context), ENOTCONN (transport not open),
    // EMSGSIZE (already_received too big), ENOMEM.
    // Not concurrently with any other operation on this stream.
    int upgrade_client(TlsContext& ctx, const char* server_name,
                       std::span<const std::byte> already_received = {}) noexcept {
        if (!ctx.initialized() || ctx.is_server()) {
            return EINVAL;
        }
        if (ctx.verifies_peer() && (server_name == nullptr || *server_name == '\0')) {
            return EINVAL;
        }
        if (const int e = setup(ctx, already_received); e != 0) {
            return e;
        }
        SSL_set_connect_state(ssl_);
        if (server_name != nullptr && *server_name != '\0') {
            if (const int e = set_server_name(ctx, server_name); e != 0) {
                reset();
                return e;
            }
        }
        return 0;
    }

    // Starts TLS as the server on the connected transport.
    int upgrade_server(TlsContext& ctx, std::span<const std::byte> already_received = {}) noexcept {
        if (!ctx.initialized() || !ctx.is_server()) {
            return EINVAL;
        }
        if (const int e = setup(ctx, already_received); e != 0) {
            return e;
        }
        SSL_set_accept_state(ssl_);
        return 0;
    }

    // Runs the handshake to completion (or returns 0 at once if done). The
    // transport's deadlines apply (ETIMEDOUT; the handshake can be resumed
    // after moving them). Errors: see the top comment.
    int handshake() noexcept {
        if (established_.load(std::memory_order_acquire)) {
            return 0;
        }
        check_stack();
        read_mu_.lock();
        write_mu_.lock();
        const int e = handshake_held();
        release_write_role();
        read_mu_.unlock();
        return e;
    }

    // ---- I/O (net::Stream's surface) ------------------------------------------

    // Up to `size` plaintext bytes (at most one record's worth). {0, 0}: the
    // peer sent close_notify.
    IoResult read_some(void* buffer, std::size_t size) noexcept {
        if (size == 0) {
            return IoResult{0, 0};
        }
        if (!established_.load(std::memory_order_acquire)) {
            if (const int e = handshake(); e != 0) {
                return IoResult{0, e};
            }
        }
        check_stack();
        read_mu_.lock();
        const IoResult r = read_held(buffer, size);
        read_mu_.unlock();
        return r;
    }

    // Exactly `size` bytes; a short count with error 0: the stream ended.
    IoResult read_exact(void* buffer, std::size_t size) noexcept {
        auto* p = static_cast<char*>(buffer);
        std::size_t done = 0;
        while (done < size) {
            const IoResult r = read_some(p + done, size - done);
            if (!r) {
                return IoResult{done, r.error};
            }
            if (r.bytes == 0) {
                break;
            }
            done += r.bytes;
        }
        return IoResult{done, 0};
    }

    // Encrypts as many whole records as the outbound ring takes (at least
    // one), then sends them all. Returns the plaintext bytes accepted.
    IoResult write_some(const void* buffer, std::size_t size) noexcept { return write(buffer, size, false); }

    // All `size` bytes (encrypted record by record, sent as the ring fills).
    IoResult write_all(const void* buffer, std::size_t size) noexcept { return write(buffer, size, true); }

    // SHUT_WR / SHUT_RDWR: sends close_notify (once; after the handshake),
    // waits until it is handed to the kernel, then shuts the transport down
    // the same way (the peer reads end of stream). SHUT_RD: transport only.
    // Reading goes on working after SHUT_WR (TLS 1.3 half-close). 0 or errno.
    int shutdown(int how = SHUT_WR) noexcept {
        int error = 0;
        if (how != SHUT_RD && ssl_ != nullptr) {
            check_stack();
            write_mu_.lock();
            for (int attempt = 0; attempt < 2; ++attempt) {
                bool again = false;
                {
                    std::lock_guard ssl_guard(ssl_mu_);
                    if (!established_.load(std::memory_order_relaxed) || fatal_ != 0 ||
                        (SSL_get_shutdown(ssl_) & SSL_SENT_SHUTDOWN) != 0) {
                        break;
                    }
                    ERR_clear_error();
                    const int r = SSL_shutdown(ssl_);
                    again = r < 0 && SSL_get_error(ssl_, r) == SSL_ERROR_WANT_WRITE;
                    ERR_clear_error();
                }
                error = flush_held();
                if (!again || error != 0) {
                    break;
                }
            }
            release_write_role();
        }
        const int e = transport_.shutdown(how);
        return error != 0 ? error : e;
    }

    // Closes the transport (fibers parked in an operation wake with
    // ECANCELED). Abrupt: no close_notify (call shutdown() first for a clean
    // close). The TLS state stays until the next upgrade_* / destruction.
    // 0 or EBADF. Any thread.
    int close() noexcept { return transport_.close(); }

    // TLS 1.3 KeyUpdate: new traffic keys for our direction; request_peer
    // asks the peer to update its own too (its reader answers through its
    // write side). 0 or errno.
    int key_update(bool request_peer = true) noexcept {
        if (!established_.load(std::memory_order_acquire)) {
            return ENOTCONN;
        }
        check_stack();
        write_mu_.lock();
        int error = 0;
        {
            std::lock_guard ssl_guard(ssl_mu_);
            ERR_clear_error();
            if (fatal_ != 0) {
                error = fatal_;
            } else if (SSL_key_update(ssl_, request_peer ? SSL_KEY_UPDATE_REQUESTED
                                                         : SSL_KEY_UPDATE_NOT_REQUESTED) != 1) {
                error = EPROTO;  // not TLS 1.3, or one is pending already
                last_error_ = ERR_peek_last_error();
            } else {
                // The KeyUpdate message is written by the next SSL_write or
                // SSL_do_handshake: do it now.
                ERR_clear_error();
                if (SSL_do_handshake(ssl_) != 1) {
                    const int r = SSL_get_error(ssl_, 0);
                    if (r != SSL_ERROR_WANT_READ && r != SSL_ERROR_WANT_WRITE) {
                        error = EPROTO;
                        last_error_ = ERR_peek_last_error();
                    }
                }
            }
            ERR_clear_error();
        }
        const int e = flush_held();
        release_write_role();
        return error != 0 ? error : e;
    }

    // ---- deadlines and options (forwarded to the transport) -------------------

    int set_read_deadline(poller::TimePoint deadline) noexcept { return transport_.set_read_deadline(deadline); }
    int set_write_deadline(poller::TimePoint deadline) noexcept { return transport_.set_write_deadline(deadline); }
    int set_deadline(poller::TimePoint deadline) noexcept { return transport_.set_deadline(deadline); }
    int set_nodelay(bool on = true) noexcept { return transport_.set_nodelay(on); }

    bool is_open_approx() const noexcept { return transport_.is_open_approx(); }

    // ---- after a successful handshake --------------------------------------------

    bool handshake_done() const noexcept { return established_.load(std::memory_order_acquire); }

    // Negotiated ALPN protocol ("" if none).
    std::string_view alpn() const noexcept {
        return std::string_view(reinterpret_cast<const char*>(alpn_), alpn_len_);
    }

    // X509_V_OK (0) or the verification error (X509_V_ERR_*), also after a
    // failed handshake. X509_verify_cert_error_string() names it.
    long verify_result() const noexcept { return verify_result_; }

    // The peer's certificate (nullptr: none sent; owned by the stream).
    X509* peer_certificate() const noexcept { return peer_cert_; }

    // "/CN=localhost/O=..." into buf ("" without a peer certificate).
    const char* peer_subject(char* buf, std::size_t size) const noexcept {
        if (size == 0) {
            return buf;
        }
        buf[0] = '\0';
        if (peer_cert_ != nullptr) {
            X509_NAME_oneline(X509_get_subject_name(peer_cert_), buf, static_cast<int>(size));
        }
        return buf;
    }

    const char* version() const noexcept { return version_ != nullptr ? version_ : ""; }  // "TLSv1.3"
    const char* cipher() const noexcept { return cipher_ != nullptr ? cipher_ : ""; }

    // ---- errors ----------------------------------------------------------------

    // The OpenSSL error code behind the last EPROTO / ECONNABORTED (0: none).
    // Read it from the fiber that got the error (or after synchronizing).
    unsigned long last_error() const noexcept { return last_error_; }

    // OpenSSL's reason, plus the verification error if there was one:
    // "error:0A000086:SSL routines::certificate verify failed (verify:
    // hostname mismatch)".
    const char* error_string(char* buf, std::size_t size) const noexcept {
        TlsContext::format_error(last_error_, buf, size);
        if (verify_result_ != X509_V_OK && size != 0) {
            const std::size_t used = std::strlen(buf);
            std::snprintf(buf + used, size - used, " (verify: %s)", X509_verify_cert_error_string(verify_result_));
        }
        return buf;
    }

    // The SSL object, for what is not wrapped (only between operations, or
    // read-only after the handshake).
    SSL* native_handle() const noexcept { return ssl_; }

private:
    enum class Step : unsigned char { kDone, kWantRead, kWantWrite, kEof, kFatal };

    // ---- setup / teardown ------------------------------------------------------------

    int setup(TlsContext& ctx, std::span<const std::byte> already_received) noexcept {
        if (!transport_.is_open_approx()) {
            return ENOTCONN;
        }
        reset();
        const CommonOptions& o = ctx.options();
        if (already_received.size() > o.read_buffer_size) {
            return EMSGSIZE;
        }
        ERR_clear_error();
        ssl_ = SSL_new(ctx.native_handle());
        BIO* ssl_side = nullptr;
        BIO* net_side = nullptr;
        // writebuf1: what SSL writes (outbound); writebuf2: what we write (inbound).
        if (ssl_ == nullptr || BIO_new_bio_pair(&ssl_side, o.write_buffer_size, &net_side, o.read_buffer_size) != 1) {
            last_error_ = ERR_peek_last_error();
            ERR_clear_error();
            reset();
            return ENOMEM;
        }
        SSL_set_bio(ssl_, ssl_side, ssl_side);  // the SSL owns its half
        ssl_bio_ = ssl_side;
        net_bio_ = net_side;
        ctx_ = &ctx;
        stack_reserve_ = o.stack_reserve;
        page_ = static_cast<std::size_t>(::sysconf(_SC_PAGESIZE));
        if (!already_received.empty() &&
            BIO_write(net_bio_, already_received.data(), static_cast<int>(already_received.size())) !=
                static_cast<int>(already_received.size())) {
            ERR_clear_error();
            reset();
            return ENOMEM;
        }
        return 0;
    }

    int set_server_name(TlsContext& ctx, const char* name) noexcept {
        in6_addr scratch;
        const bool ip = ::inet_pton(AF_INET, name, &scratch) == 1 || ::inet_pton(AF_INET6, name, &scratch) == 1;
        bool ok = true;
        if (!ip) {
            ok = SSL_set_tlsext_host_name(ssl_, name) == 1;  // SNI: DNS names only (RFC 6066)
        }
        if (ok && ctx.verifies_peer()) {
            if (ip) {
                ok = X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl_), name) == 1;
            } else {
                SSL_set_hostflags(ssl_, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
                ok = SSL_set1_host(ssl_, name) == 1;
            }
        }
        if (!ok) {
            last_error_ = ERR_peek_last_error();
            ERR_clear_error();
            return EINVAL;
        }
        return 0;
    }

    void reset() noexcept {
        if (ssl_ != nullptr) {
            SSL_free(ssl_);  // frees the SSL's half of the pair
            ssl_ = nullptr;
        }
        if (net_bio_ != nullptr) {
            BIO_free(net_bio_);
            net_bio_ = nullptr;
        }
        if (peer_cert_ != nullptr) {
            X509_free(peer_cert_);
            peer_cert_ = nullptr;
        }
        ssl_bio_ = nullptr;
        ctx_ = nullptr;
        established_.store(false, std::memory_order_relaxed);
        fatal_ = 0;
        last_error_ = 0;
        verify_result_ = X509_V_OK;
        alpn_ = nullptr;
        alpn_len_ = 0;
        version_ = nullptr;
        cipher_ = nullptr;
    }

    // ---- OpenSSL calls (ssl_mu_ held, same thread throughout) ------------------

    // After an OpenSSL call failed with `ret`: what to do next. Reads and
    // clears this thread's error queue (before anything can park).
    Step classify(int ret) noexcept {
        const int e = SSL_get_error(ssl_, ret);
        switch (e) {
            case SSL_ERROR_WANT_READ:
                ERR_clear_error();
                return Step::kWantRead;
            case SSL_ERROR_WANT_WRITE:
                ERR_clear_error();
                return Step::kWantWrite;
            case SSL_ERROR_ZERO_RETURN:
                ERR_clear_error();
                return Step::kEof;
            default:
                break;
        }
        const unsigned long code = ERR_peek_last_error();
        ERR_clear_error();
        int error = EPROTO;
        if (e == SSL_ERROR_SYSCALL ||
            (ERR_GET_LIB(code) == ERR_LIB_SSL && ERR_GET_REASON(code) == SSL_R_UNEXPECTED_EOF_WHILE_READING)) {
            error = ECONNABORTED;
        }
        last_error_ = code;
        fatal_ = error;
        verify_result_ = SSL_get_verify_result(ssl_);
        return Step::kFatal;
    }

    // The peer asked for a KeyUpdate (TLS 1.3, update_requested): OpenSSL
    // only notes it in SSL_read and writes our answer with the next
    // SSL_write. A connection whose application only reads would never
    // answer (tlsfuzzer's test-tls13-keyupdate waits for it and times out),
    // so the reader writes it now; the caller flushes it like any reader
    // output. ssl_mu_ held.
    void answer_key_update() noexcept {
        if (SSL_get_key_update_type(ssl_) == SSL_KEY_UPDATE_NONE) {
            return;
        }
        // What ssl3_write_bytes does before the next record: enter the state
        // machine with the pending update (not_requested: an answer), which
        // SSL_do_handshake then writes. SSL_key_update refuses while a write
        // is half done (BAD_WRITE_RETRY): the writer's retry sends it then.
        ERR_clear_error();
        if (SSL_key_update(ssl_, SSL_KEY_UPDATE_NOT_REQUESTED) == 1) {
            static_cast<void>(SSL_do_handshake(ssl_));  // 1, or WANT_WRITE: flushed and retried later
        }
        ERR_clear_error();
    }

    // At handshake completion: what the accessors return.
    void capture_session() noexcept {
        const unsigned char* proto = nullptr;
        unsigned int len = 0;
        SSL_get0_alpn_selected(ssl_, &proto, &len);  // lives as long as the SSL
        alpn_ = proto;
        alpn_len_ = proto != nullptr ? len : 0;
        peer_cert_ = SSL_get1_peer_certificate(ssl_);
        verify_result_ = SSL_get_verify_result(ssl_);
        version_ = SSL_get_version(ssl_);
        cipher_ = SSL_get_cipher_name(ssl_);
    }

    // ---- the pump (ssl_mu_ NOT held) -----------------------------------------------

    // Read role held: one transport read straight into the inbound ring.
    // 0 or the transport's error. End of stream shuts the ring's write side:
    // OpenSSL then reports close_notify-less EOF itself (ECONNABORTED, or
    // end of stream with ignore_unexpected_eof).
    int fill() noexcept {
        char* space = nullptr;
        int avail = 0;
        {
            std::lock_guard ssl_guard(ssl_mu_);
            avail = BIO_nwrite0(net_bio_, &space);
        }
        if (avail <= 0) [[unlikely]] {
            // OpenSSL asks for input only once it has drained the ring.
            fail::fail_fast("tls: inbound ring full while OpenSSL wants more input");
        }
        // Outside ssl_mu_: only the read role writes the inbound ring's free
        // space, and nobody else commits to it (see the top comment). The
        // ring is empty here (OpenSSL reports WANT_READ only when a read of
        // its half found nothing, and only we add data), so its offset is 0
        // and stays 0 until our BIO_nwrite: bss_bio resets the offset when a
        // read empties the ring, which cannot happen to an empty ring. So
        // the commit lands exactly where the bytes were written.
        const IoResult r = transport_.read_some(space, static_cast<std::size_t>(avail));
        std::lock_guard ssl_guard(ssl_mu_);
        if (r.bytes > 0) {
            BIO_nwrite(net_bio_, &space, static_cast<int>(r.bytes));
        } else if (r.error == 0) {
            BIO_shutdown_wr(net_bio_);
        }
        return r.error;
    }

    // Write role held: sends everything in the outbound ring. 0 or the
    // transport's error (what was not sent stays queued).
    int flush_held() noexcept {
        for (;;) {
            char* data = nullptr;
            int n = 0;
            {
                std::lock_guard ssl_guard(ssl_mu_);
                n = BIO_nread0(net_bio_, &data);
            }
            if (n <= 0) {
                return 0;
            }
            // Outside ssl_mu_: OpenSSL only appends behind the queued bytes.
            const IoResult r = transport_.write_all(data, static_cast<std::size_t>(n));
            if (r.bytes > 0) {
                std::lock_guard ssl_guard(ssl_mu_);
                BIO_nread(net_bio_, &data, static_cast<int>(r.bytes));
            }
            if (!r) {
                return r.error;
            }
        }
    }

    bool output_pending() noexcept {
        std::lock_guard ssl_guard(ssl_mu_);
        return BIO_ctrl_pending(net_bio_) > 0;
    }

    // Unlocks the write role without stranding output that someone (a
    // reader) appended while we held it.
    void release_write_role() noexcept {
        for (;;) {
            write_mu_.unlock();
            if (net_bio_ == nullptr || !output_pending() || !write_mu_.try_lock()) {
                return;  // nothing queued, or the new holder flushes it
            }
            if (flush_held() != 0) {
                write_mu_.unlock();
                return;
            }
        }
    }

    // A reader's output: flushed now if nobody holds the write role.
    void flush_if_free() noexcept {
        if (write_mu_.try_lock()) {
            static_cast<void>(flush_held());
            release_write_role();
        }
    }

    // ---- operations -------------------------------------------------------------------

    // Both roles held.
    int handshake_held() noexcept {
        for (;;) {
            Step step = Step::kDone;
            int error = 0;
            {
                std::lock_guard ssl_guard(ssl_mu_);
                if (ssl_ == nullptr) {
                    return ENOTCONN;
                }
                if (fatal_ != 0) {
                    return fatal_;
                }
                if (established_.load(std::memory_order_relaxed)) {
                    return 0;
                }
                ERR_clear_error();
                const int r = SSL_do_handshake(ssl_);
                if (r == 1) {
                    capture_session();
                    established_.store(true, std::memory_order_release);
                } else {
                    step = classify(r);
                    error = fatal_;
                }
            }
            switch (step) {
                case Step::kDone:
                    return flush_held();  // our last flight (Finished, tickets)
                case Step::kWantWrite:
                    if (const int e = flush_held(); e != 0) {
                        return e;
                    }
                    break;
                case Step::kWantRead:
                    // Our flight first: the peer waits for it.
                    if (const int e = flush_held(); e != 0) {
                        return e;
                    }
                    if (const int e = fill(); e != 0) {
                        return e;
                    }
                    break;
                case Step::kEof: {
                    // close_notify during the handshake.
                    std::lock_guard ssl_guard(ssl_mu_);
                    fatal_ = ECONNABORTED;
                    return ECONNABORTED;
                }
                case Step::kFatal:
                    static_cast<void>(flush_held());  // the alert we generated, best effort
                    return error;
            }
        }
    }

    // Read role held, handshake done.
    IoResult read_held(void* buffer, std::size_t size) noexcept {
        for (;;) {
            Step step = Step::kDone;
            std::size_t got = 0;
            int error = 0;
            bool output = false;
            {
                std::lock_guard ssl_guard(ssl_mu_);
                if (fatal_ != 0) {
                    return IoResult{0, fatal_};
                }
                ERR_clear_error();
                if (SSL_read_ex(ssl_, buffer, size, &got) != 1) {
                    step = classify(0);
                    error = fatal_;
                }
                if (step != Step::kFatal) {
                    answer_key_update();
                }
                output = BIO_ctrl_pending(net_bio_) > 0;
            }
            switch (step) {
                case Step::kDone:
                    if (output) {
                        flush_if_free();
                    }
                    return IoResult{got, 0};
                case Step::kEof:
                    return IoResult{0, 0};
                case Step::kFatal:
                    flush_if_free();  // the alert, best effort
                    return IoResult{0, error};
                case Step::kWantWrite: {
                    // Rare (control room exhausted): wait for the write role.
                    write_mu_.lock();
                    const int e = flush_held();
                    release_write_role();
                    if (e != 0) {
                        return IoResult{0, e};
                    }
                    break;
                }
                case Step::kWantRead:
                    if (output) {
                        flush_if_free();
                    }
                    if (const int e = fill(); e != 0) {
                        return IoResult{0, e};
                    }
                    break;
            }
        }
    }

    IoResult write(const void* buffer, std::size_t size, bool all) noexcept {
        if (size == 0) {
            return IoResult{0, 0};
        }
        if (!established_.load(std::memory_order_acquire)) {
            if (const int e = handshake(); e != 0) {
                return IoResult{0, e};
            }
        }
        check_stack();
        const auto* p = static_cast<const char*>(buffer);
        write_mu_.lock();
        std::size_t done = 0;
        int error = 0;
        bool fatal = false;
        while (done < size) {
            Step step = Step::kDone;
            {
                std::lock_guard ssl_guard(ssl_mu_);
                if (fatal_ != 0) {
                    error = fatal_;
                    fatal = true;
                    break;
                }
                if ((SSL_get_shutdown(ssl_) & SSL_SENT_SHUTDOWN) != 0) {
                    error = EPIPE;
                    break;
                }
                if (BIO_ctrl_get_write_guarantee(ssl_bio_) < TlsContext::kMinWriteBuffer) {
                    step = Step::kWantWrite;  // keep room for a reader's control messages
                } else {
                    std::size_t n = 0;
                    ERR_clear_error();
                    if (SSL_write_ex(ssl_, p + done, std::min(size - done, kRecordPlaintext), &n) == 1) {
                        done += n;
                    } else {
                        step = classify(0);
                        if (step == Step::kFatal) {
                            error = fatal_;
                            fatal = true;
                        } else if (step != Step::kWantWrite) {
                            error = EPROTO;  // WANT_READ / EOF: impossible without renegotiation
                        }
                    }
                }
            }
            if (error != 0) {
                break;
            }
            if (step == Step::kWantWrite) {
                if (!all && done > 0) {
                    break;  // write_some: what fitted
                }
                if (const int e = flush_held(); e != 0) {
                    error = e;
                    break;
                }
            }
        }
        if (error == 0 || fatal) {
            const int e = flush_held();
            if (error == 0) {
                error = e;
            }
        }
        release_write_role();
        return IoResult{done, error};
    }

    // ---- stack check -------------------------------------------------------------------

    void check_stack() noexcept {
        if (stack_reserve_ == 0) {
            return;
        }
        if constexpr (requires(Sched& s) { s.stacks().stack_size(); }) {
            const Fiber* f = Fiber::current();
            if (f == nullptr) {
                return;  // a plain thread: its own stack
            }
            // The fiber object sits at the top of its page-aligned stack
            // (fiber.hpp), whose usable size is stack_size rounded up to pages.
            const std::uintptr_t page = page_;
            const std::uintptr_t top = (reinterpret_cast<std::uintptr_t>(f) + sizeof(Fiber) + page - 1) & ~(page - 1);
            const std::uintptr_t usable = (f->scheduler().stacks().stack_size() + page - 1) & ~(page - 1);
            const std::uintptr_t bottom = top - usable;
            const auto sp = reinterpret_cast<std::uintptr_t>(__builtin_frame_address(0));
            if (sp <= bottom || sp > top) {
                return;  // not on the fiber's stack (e.g. a sanitizer's fake stack)
            }
            if (sp - bottom < stack_reserve_) [[unlikely]] {
                char message[192];
                std::snprintf(message, sizeof message,
                              "tls: %zu bytes of fiber stack left, CommonOptions::stack_reserve wants %zu "
                              "(raise StackPoolOptions::stack_size)",
                              static_cast<std::size_t>(sp - bottom), stack_reserve_);
                fail::fail_fast(message);
            }
        }
    }

    Stream transport_;
    Mutex read_mu_;
    Mutex write_mu_;
    Mutex ssl_mu_;

    // Set by upgrade_* (no operation running); read by the operations.
    SSL* ssl_ = nullptr;
    BIO* ssl_bio_ = nullptr;  // OpenSSL's half (owned by ssl_)
    BIO* net_bio_ = nullptr;  // ours
    const TlsContext* ctx_ = nullptr;
    std::size_t stack_reserve_ = 0;
    std::size_t page_ = 4096;

    // Under ssl_mu_.
    int fatal_ = 0;
    unsigned long last_error_ = 0;
    long verify_result_ = X509_V_OK;

    // Written once under ssl_mu_ before established_ (release).
    typename Sched::SyncPolicy::template Atomic<bool> established_{false};
    const unsigned char* alpn_ = nullptr;
    std::size_t alpn_len_ = 0;
    X509* peer_cert_ = nullptr;
    const char* version_ = nullptr;
    const char* cipher_ = nullptr;
};

using TlsStream = BasicTlsStream<sched::Scheduler>;

}  // namespace fibers::tls
