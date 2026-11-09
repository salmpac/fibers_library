#pragma once

// TlsContext: the shared, configured-once side of TLS (an SSL_CTX), OpenSSL 3.
//
//   tls::TlsContext client_ctx;
//   tls::ClientOptions co;
//   co.ca_file = "ca.pem";                       // or use_default_ca = true
//   const std::string_view protos[] = {"h2", "http/1.1"};
//   co.alpn = protos;
//   if (int e = client_ctx.init_client(co); e != 0) { ... client_ctx.error_string(...) ... }
//
//   tls::TlsContext server_ctx;
//   tls::ServerOptions so;
//   so.cert_chain_file = "server.pem";           // leaf first, then intermediates
//   so.key_file = "server.key";
//   so.client_ca_file = "ca.pem";                // optional: mTLS
//   so.verify_client = tls::VerifyClient::kRequired;
//   server_ctx.init_server(so);
//
// One context serves any number of connections (tls::TlsStream, stream.hpp)
// on any threads: OpenSSL's SSL_CTX is thread-safe once configured. Configure
// it once, before the first connection, then never change it (the init_*
// functions are not thread-safe against connections being made; they also
// read certificate files with blocking I/O, so call them at startup, not in
// a latency-sensitive fiber).
//
// Defaults (secure unless told otherwise):
//   * TLS 1.2 minimum (min_version), TLS 1.3 preferred, OpenSSL's default
//     cipher suites, no compression, no renegotiation (SSL_OP_NO_RENEGOTIATION:
//     a renegotiation would make SSL_write read from the network, which the
//     full-duplex design of TlsStream relies on never happening).
//   * client: verify-full. The peer's chain must verify against the CA
//     file / dir / system store, AND the name given to the stream
//     (TlsStream::upgrade_client / connect) must match the certificate (DNS
//     name or IP address SAN). The name is also sent as SNI (DNS names only).
//     verify_peer = false turns all of that off (tests only).
//   * server: certificate chain + key; client certificates are not
//     requested unless verify_client says so (mTLS with client_ca_file).
//   * ALPN: the client offers `alpn` in order; the server picks the first of
//     ITS list that the client offered. No overlap: the server fails the
//     handshake (no_application_protocol alert, RFC 7301), so both sides get
//     EPROTO. An empty server list ignores ALPN.
//   * Session tickets (TLS 1.3 / 1.2 stateless resumption) are on for
//     servers; clients do not cache sessions (no resumption by default).
//
// Errors: the init functions return 0 or an errno value (the errno of a
// file that could not be read, EINVAL for anything else: bad PEM, key that
// does not match the certificate, unknown option); the OpenSSL error code
// that caused it is kept (last_error() / error_string()). No exceptions.
//
// Lifetime: the context must outlive every TlsStream using it (the server's
// ALPN callback reads this object). Neither copyable nor movable.
//
// No allocation in this code; OpenSSL allocates internally (SSL_CTX, cert
// store), which is accepted.

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509_vfy.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <span>
#include <string_view>

namespace fibers::tls {

// What the server asks of clients (mTLS).
enum class VerifyClient : unsigned char {
    kNone,      // no client certificate requested
    kOptional,  // requested; verified if sent; a connection without one is accepted
    kRequired,  // requested and required: no (valid) certificate -> handshake error
};

// Fields every context has.
struct CommonOptions {
    // ALPN protocol ids, client: offered in this order; server: preference
    // order. Each 1..255 bytes; the wire list (1 length byte per id) must fit
    // TlsContext::kMaxAlpnWire bytes. Not retained (copied by init_*).
    std::span<const std::string_view> alpn{};

    // TLS1_2_VERSION or TLS1_3_VERSION. max_version 0 = the highest supported.
    int min_version = TLS1_2_VERSION;
    int max_version = 0;

    // Per-connection ciphertext buffers (the BIO pair between OpenSSL and the
    // socket, allocated by OpenSSL per stream). read: how much ciphertext one
    // socket read can take (bigger = fewer syscalls for bulk reads). write:
    // ciphertext queued before a send; at least kMinWriteBuffer (one full
    // record plus room for control messages, see stream.hpp).
    std::size_t read_buffer_size = 32 * 1024;
    std::size_t write_buffer_size = 48 * 1024;

    // Transport EOF without a close_notify: false (default) -> the read fails
    // with ECONNABORTED (a truncation is detectable); true -> plain end of
    // stream (many HTTP peers just close; SSL_OP_IGNORE_UNEXPECTED_EOF).
    bool ignore_unexpected_eof = false;

    // Bytes of fiber stack that must be left below the caller's frame when a
    // TlsStream calls into OpenSSL (handshake, read, write); less fails fast
    // with a message instead of running into the guard page (or past it: a
    // frame bigger than a page can skip it). 0 disables the check. The
    // default is about twice the deepest call measured (a client handshake,
    // ~11 KiB; tls_test StackDepth), see "Stack" in stream.hpp.
    std::size_t stack_reserve = 24 * 1024;
};

struct ClientOptions : CommonOptions {
    const char* ca_file = nullptr;  // PEM bundle of trusted CAs
    const char* ca_dir = nullptr;   // hashed directory (c_rehash / openssl rehash)
    bool use_default_ca = false;    // the system store (SSL_CTX_set_default_verify_paths)
    bool verify_peer = true;        // false: no chain or name checks at all (tests only)

    // Client certificate for mTLS (both or neither).
    const char* cert_chain_file = nullptr;
    const char* key_file = nullptr;
};

struct ServerOptions : CommonOptions {
    const char* cert_chain_file = nullptr;  // required: PEM, leaf first
    const char* key_file = nullptr;         // required: PEM private key

    const char* client_ca_file = nullptr;   // CAs for client certificates (needed unless kNone)
    VerifyClient verify_client = VerifyClient::kNone;

    bool session_tickets = true;
    std::size_t num_tickets = 2;            // TLS 1.3 tickets sent after each full handshake
};

class TlsContext {
public:
    static constexpr std::size_t kMaxAlpnWire = 128;
    // A max-size TLS 1.2 record (2^14 + 2048 expansion + 5 header) plus room
    // for control messages (alerts, KeyUpdate) produced by a reader.
    static constexpr std::size_t kMaxRecord = 16384 + 2048 + 5;
    static constexpr std::size_t kControlRoom = 1024;
    static constexpr std::size_t kMinWriteBuffer = kMaxRecord + kControlRoom;

    TlsContext() noexcept = default;
    TlsContext(const TlsContext&) = delete;
    TlsContext& operator=(const TlsContext&) = delete;

    ~TlsContext() {
        if (ctx_ != nullptr) {
            SSL_CTX_free(ctx_);
        }
    }

    // 0 or errno (see the top comment). Once per context.
    int init_client(const ClientOptions& o) noexcept {
        if (const int e = begin(o, /*server=*/false); e != 0) {
            return e;
        }
        if (o.verify_peer) {
            if (o.ca_file != nullptr || o.ca_dir != nullptr) {
                if (SSL_CTX_load_verify_locations(ctx_, o.ca_file, o.ca_dir) != 1) {
                    return fail();
                }
            }
            if (o.use_default_ca && SSL_CTX_set_default_verify_paths(ctx_) != 1) {
                return fail();
            }
            SSL_CTX_set_verify(ctx_, SSL_VERIFY_PEER, nullptr);
        } else {
            SSL_CTX_set_verify(ctx_, SSL_VERIFY_NONE, nullptr);
        }
        verify_peer_ = o.verify_peer;
        if ((o.cert_chain_file == nullptr) != (o.key_file == nullptr)) {
            return fail_with(EINVAL);
        }
        if (o.cert_chain_file != nullptr) {
            if (const int e = load_identity(o.cert_chain_file, o.key_file); e != 0) {
                return e;
            }
        }
        if (alpn_len_ != 0 && SSL_CTX_set_alpn_protos(ctx_, alpn_, static_cast<unsigned>(alpn_len_)) != 0) {
            return fail();
        }
        // No client session cache: nothing to resume, tickets are dropped.
        SSL_CTX_set_session_cache_mode(ctx_, SSL_SESS_CACHE_OFF);
        return 0;
    }

    int init_server(const ServerOptions& o) noexcept {
        if (o.cert_chain_file == nullptr || o.key_file == nullptr) {
            return fail_with(EINVAL);
        }
        if (const int e = begin(o, /*server=*/true); e != 0) {
            return e;
        }
        if (const int e = load_identity(o.cert_chain_file, o.key_file); e != 0) {
            return e;
        }
        if (o.verify_client != VerifyClient::kNone) {
            if (o.client_ca_file == nullptr) {
                return fail_with(EINVAL);
            }
            if (SSL_CTX_load_verify_locations(ctx_, o.client_ca_file, nullptr) != 1) {
                return fail();
            }
            // The CA names sent in the CertificateRequest (OpenSSL owns the list).
            STACK_OF(X509_NAME)* names = SSL_load_client_CA_file(o.client_ca_file);
            if (names == nullptr) {
                return fail();
            }
            SSL_CTX_set_client_CA_list(ctx_, names);
            int mode = SSL_VERIFY_PEER;
            if (o.verify_client == VerifyClient::kRequired) {
                mode |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
            }
            SSL_CTX_set_verify(ctx_, mode, nullptr);
        } else {
            SSL_CTX_set_verify(ctx_, SSL_VERIFY_NONE, nullptr);
        }
        // Resumed sessions with client certificates need a session id context.
        static constexpr unsigned char kSidCtx[] = "fibers::tls";
        if (SSL_CTX_set_session_id_context(ctx_, kSidCtx, sizeof kSidCtx - 1) != 1) {
            return fail();
        }
        if (!o.session_tickets) {
            SSL_CTX_set_options(ctx_, SSL_OP_NO_TICKET);
            SSL_CTX_set_num_tickets(ctx_, 0);
        } else {
            SSL_CTX_set_num_tickets(ctx_, o.num_tickets);
        }
        if (alpn_len_ != 0) {
            SSL_CTX_set_alpn_select_cb(ctx_, &select_alpn, this);
        }
        return 0;
    }

    bool is_server() const noexcept { return server_; }
    bool initialized() const noexcept { return ctx_ != nullptr && ready_; }
    bool verifies_peer() const noexcept { return verify_peer_; }
    const CommonOptions& options() const noexcept { return options_; }

    // The OpenSSL error that failed the last init_* (0: none).
    unsigned long last_error() const noexcept { return last_error_; }

    // Human-readable form of last_error() into buf (always terminated).
    const char* error_string(char* buf, std::size_t size) const noexcept {
        return format_error(last_error_, buf, size);
    }

    // For anything not covered above (ciphers, groups, keylog callback, ...).
    // Configure before the first connection.
    SSL_CTX* native_handle() const noexcept { return ctx_; }

    // "error:0A000086:SSL routines::certificate verify failed" (or "no error").
    static const char* format_error(unsigned long code, char* buf, std::size_t size) noexcept {
        if (size == 0) {
            return buf;
        }
        if (code == 0) {
            std::snprintf(buf, size, "no error");
        } else {
            ERR_error_string_n(code, buf, size);
        }
        return buf;
    }

    // An OpenSSL error code -> errno (see the top comment).
    static int errno_of(unsigned long code) noexcept {
        if (code != 0 && ERR_SYSTEM_ERROR(code)) {
            const int e = ERR_GET_REASON(code);
            return e != 0 ? e : EINVAL;
        }
        return EINVAL;
    }

private:
    int begin(const CommonOptions& o, bool server) noexcept {
        ERR_clear_error();
        if (ctx_ != nullptr) {
            return fail_with(EINVAL);  // once per context
        }
        if (o.write_buffer_size < kMinWriteBuffer || o.read_buffer_size < 1024) {
            return fail_with(EINVAL);
        }
        if (const int e = encode_alpn(o.alpn); e != 0) {
            return fail_with(e);
        }
        ctx_ = SSL_CTX_new(server ? TLS_server_method() : TLS_client_method());
        if (ctx_ == nullptr) {
            return fail();
        }
        server_ = server;
        options_ = o;
        options_.alpn = {};  // not retained; the wire form is in alpn_
        if (SSL_CTX_set_min_proto_version(ctx_, o.min_version) != 1 ||
            SSL_CTX_set_max_proto_version(ctx_, o.max_version) != 1) {
            return fail();
        }
        std::uint64_t opts = SSL_OP_NO_COMPRESSION | SSL_OP_NO_RENEGOTIATION;
        if (o.ignore_unexpected_eof) {
            opts |= SSL_OP_IGNORE_UNEXPECTED_EOF;
        }
        SSL_CTX_set_options(ctx_, opts);
        // SSL_write may return after one record (TlsStream writes in records
        // and flushes between them), and a retried write may pass the same
        // bytes from another address (a caller's buffer is not pinned).
        SSL_CTX_set_mode(ctx_, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
        ready_ = true;
        return 0;
    }

    int load_identity(const char* chain, const char* key) noexcept {
        if (SSL_CTX_use_certificate_chain_file(ctx_, chain) != 1 ||
            SSL_CTX_use_PrivateKey_file(ctx_, key, SSL_FILETYPE_PEM) != 1 ||
            SSL_CTX_check_private_key(ctx_) != 1) {
            return fail();
        }
        return 0;
    }

    int encode_alpn(std::span<const std::string_view> protos) noexcept {
        std::size_t n = 0;
        for (const std::string_view p : protos) {
            if (p.empty() || p.size() > 255 || n + 1 + p.size() > kMaxAlpnWire) {
                return EINVAL;
            }
            alpn_[n++] = static_cast<unsigned char>(p.size());
            std::memcpy(alpn_ + n, p.data(), p.size());
            n += p.size();
        }
        alpn_len_ = n;
        return 0;
    }

    // Server: our preference order first (SSL_select_next_proto's first list).
    static int select_alpn(SSL*, const unsigned char** out, unsigned char* out_len, const unsigned char* in,
                           unsigned int in_len, void* arg) noexcept {
        auto* self = static_cast<TlsContext*>(arg);
        unsigned char* selected = nullptr;
        if (SSL_select_next_proto(&selected, out_len, self->alpn_, static_cast<unsigned>(self->alpn_len_), in,
                                  in_len) != OPENSSL_NPN_NEGOTIATED) {
            return SSL_TLSEXT_ERR_ALERT_FATAL;  // no_application_protocol
        }
        *out = selected;
        return SSL_TLSEXT_ERR_OK;
    }

    // Drains this thread's error queue: last_error_ = the newest error; the
    // result is the errno of a system error in it (a file that could not be
    // opened is reported below the SSL-level error), else EINVAL.
    int fail() noexcept {
        int error = EINVAL;
        last_error_ = 0;
        while (const unsigned long code = ERR_get_error()) {
            last_error_ = code;
            if (ERR_SYSTEM_ERROR(code) && error == EINVAL) {
                error = errno_of(code);
            }
        }
        ready_ = false;
        return error;
    }

    int fail_with(int error) noexcept {
        ERR_clear_error();
        ready_ = false;
        return error;
    }

    SSL_CTX* ctx_ = nullptr;
    bool server_ = false;
    bool ready_ = false;
    bool verify_peer_ = true;
    unsigned long last_error_ = 0;
    CommonOptions options_{};
    unsigned char alpn_[kMaxAlpnWire]{};
    std::size_t alpn_len_ = 0;
};

}  // namespace fibers::tls
