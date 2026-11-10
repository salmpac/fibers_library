#pragma once

// ES256 JSON Web Tokens (RFC 7515 / 7518 / 7519) for token-based provider
// authentication (APNs: header {"alg":"ES256","kid":<key id>}, claims
// {"iss":<team id>,"iat":<seconds>}), signed with OpenSSL 3 from an EC
// P-256 private key in PEM (Apple's .p8 files are PKCS#8 PEM).
//
//   http2::Es256Signer signer;
//   if (signer.load_file("AuthKey_ABC123DEFG.p8") != 0) { ... }
//   char jwt[http2::Es256Signer::kMaxJwt];
//   std::size_t n = signer.sign_jwt("ABC123DEFG", "DEF123GHIJ", unix_seconds, jwt, sizeof jwt);
//
// The signature is JOSE's raw r || s (2 x 32 bytes), not OpenSSL's DER.
// OpenSSL's error queue is thread-local: every function here clears it,
// makes its calls and reads it (last_error()) before returning, without
// parking in between, so a fiber that migrates to another worker later
// never sees a stale queue. Signing never waits. One signer signs for one
// party at a time (last_error() is a plain field); the key itself is
// read-only once loaded.
//
// No allocation here besides OpenSSL's own (the key, a digest context and
// the DER signature per sign_jwt).

#include <openssl/bio.h>
#include <openssl/bn.h>
#include <openssl/ec.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>

#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>

namespace fibers::http2 {

// base64url without padding (RFC 4648 5). `out` has room for
// base64url_size(n) bytes; returns the length written.
constexpr std::size_t base64url_size(std::size_t n) noexcept { return (n * 4 + 2) / 3; }

inline std::size_t base64url_encode(const void* data, std::size_t n, char* out) noexcept {
    static constexpr char kAlphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    const auto* p = static_cast<const unsigned char*>(data);
    std::size_t o = 0;
    std::size_t i = 0;
    for (; i + 3 <= n; i += 3) {
        const std::uint32_t v = (std::uint32_t{p[i]} << 16) | (std::uint32_t{p[i + 1]} << 8) | p[i + 2];
        out[o++] = kAlphabet[(v >> 18) & 63];
        out[o++] = kAlphabet[(v >> 12) & 63];
        out[o++] = kAlphabet[(v >> 6) & 63];
        out[o++] = kAlphabet[v & 63];
    }
    if (n - i == 1) {
        const std::uint32_t v = std::uint32_t{p[i]} << 16;
        out[o++] = kAlphabet[(v >> 18) & 63];
        out[o++] = kAlphabet[(v >> 12) & 63];
    } else if (n - i == 2) {
        const std::uint32_t v = (std::uint32_t{p[i]} << 16) | (std::uint32_t{p[i + 1]} << 8);
        out[o++] = kAlphabet[(v >> 18) & 63];
        out[o++] = kAlphabet[(v >> 12) & 63];
        out[o++] = kAlphabet[(v >> 6) & 63];
    }
    return o;
}

// Decodes base64url (with or without padding) into `out`. false on a bad
// character or length.
inline bool base64url_decode(std::string_view in, std::string& out) {
    auto value = [](char c) -> int {
        if (c >= 'A' && c <= 'Z') return c - 'A';
        if (c >= 'a' && c <= 'z') return c - 'a' + 26;
        if (c >= '0' && c <= '9') return c - '0' + 52;
        if (c == '-' || c == '+') return 62;
        if (c == '_' || c == '/') return 63;
        return -1;
    };
    while (!in.empty() && in.back() == '=') {
        in.remove_suffix(1);
    }
    if (in.size() % 4 == 1) {
        return false;
    }
    out.clear();
    std::uint32_t acc = 0;
    int bits = 0;
    for (const char c : in) {
        const int v = value(c);
        if (v < 0) {
            return false;
        }
        acc = (acc << 6) | static_cast<std::uint32_t>(v);
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out.push_back(static_cast<char>((acc >> bits) & 0xff));
        }
    }
    return true;
}

class Es256Signer {
public:
    // Upper bound of a JWT with key / team ids of up to kMaxId bytes.
    static constexpr std::size_t kMaxId = 64;
    static constexpr std::size_t kMaxJwt = 512;

    Es256Signer() noexcept = default;
    Es256Signer(const Es256Signer&) = delete;
    Es256Signer& operator=(const Es256Signer&) = delete;
    ~Es256Signer() { EVP_PKEY_free(key_); }

    // Loads the private key from PEM text (PKCS#8 "PRIVATE KEY", or SEC1
    // "EC PRIVATE KEY"). 0, or EINVAL (not a P-256 private key; last_error()).
    int load_pem(std::string_view pem) noexcept {
        ERR_clear_error();
        BIO* bio = BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()));
        EVP_PKEY* key = bio != nullptr ? PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr) : nullptr;
        BIO_free(bio);
        return adopt(key);
    }

    // The same from a file (e.g. AuthKey_<kid>.p8). Blocking file I/O: call
    // it at startup. 0, the errno of the open, or EINVAL.
    int load_file(const char* path) noexcept {
        std::FILE* f = std::fopen(path, "rb");
        if (f == nullptr) {
            return errno != 0 ? errno : ENOENT;
        }
        char buf[8192];
        const std::size_t n = std::fread(buf, 1, sizeof buf, f);
        std::fclose(f);
        const int e = load_pem(std::string_view(buf, n));
        OPENSSL_cleanse(buf, sizeof buf);
        return e;
    }

    bool loaded() const noexcept { return key_ != nullptr; }
    EVP_PKEY* native_handle() const noexcept { return key_; }

    // Writes the JWT into out[0, cap); returns its length, 0 on failure
    // (ids longer than kMaxId or with characters that need JSON escaping,
    // `cap` too small, no key, OpenSSL error: last_error()).
    std::size_t sign_jwt(std::string_view key_id, std::string_view team_id, std::int64_t issued_at, char* out,
                         std::size_t cap) noexcept {
        if (key_ == nullptr || !plain_id(key_id) || !plain_id(team_id)) {
            return 0;
        }
        char header[32 + kMaxId];
        const int hn = std::snprintf(header, sizeof header, R"({"alg":"ES256","kid":"%.*s"})",
                                     static_cast<int>(key_id.size()), key_id.data());
        char claims[48 + kMaxId];
        const int cn = std::snprintf(claims, sizeof claims, R"({"iss":"%.*s","iat":%lld})",
                                     static_cast<int>(team_id.size()), team_id.data(),
                                     static_cast<long long>(issued_at));
        if (hn <= 0 || cn <= 0) {
            return 0;
        }
        const std::size_t need = base64url_size(static_cast<std::size_t>(hn)) + 1 +
                                 base64url_size(static_cast<std::size_t>(cn)) + 1 + base64url_size(64);
        if (need > cap) {
            return 0;
        }
        std::size_t n = base64url_encode(header, static_cast<std::size_t>(hn), out);
        out[n++] = '.';
        n += base64url_encode(claims, static_cast<std::size_t>(cn), out + n);
        unsigned char sig[64];
        if (!sign(out, n, sig)) {
            return 0;
        }
        out[n++] = '.';
        n += base64url_encode(sig, sizeof sig, out + n);
        return n;
    }

    // ES256 over `data`: raw r || s. false on failure (last_error()).
    bool sign(const void* data, std::size_t size, unsigned char (&rs)[64]) noexcept {
        ERR_clear_error();
        bool ok = false;
        EVP_MD_CTX* ctx = EVP_MD_CTX_new();
        unsigned char der[80];
        std::size_t der_len = sizeof der;
        if (ctx != nullptr && EVP_DigestSignInit(ctx, nullptr, EVP_sha256(), nullptr, key_) == 1 &&
            EVP_DigestSign(ctx, der, &der_len, static_cast<const unsigned char*>(data), size) == 1) {
            const unsigned char* p = der;
            ECDSA_SIG* es = d2i_ECDSA_SIG(nullptr, &p, static_cast<long>(der_len));
            if (es != nullptr) {
                const BIGNUM* r = nullptr;
                const BIGNUM* s = nullptr;
                ECDSA_SIG_get0(es, &r, &s);
                ok = BN_bn2binpad(r, rs, 32) == 32 && BN_bn2binpad(s, rs + 32, 32) == 32;
                ECDSA_SIG_free(es);
            }
        }
        EVP_MD_CTX_free(ctx);
        capture_error();
        return ok;
    }

    // The OpenSSL error of the last failure (0: none).
    unsigned long last_error() const noexcept { return last_error_; }

private:
    static bool plain_id(std::string_view id) noexcept {
        if (id.empty() || id.size() > kMaxId) {
            return false;
        }
        for (const char c : id) {
            if (c == '"' || c == '\\' || static_cast<unsigned char>(c) < 0x20) {
                return false;
            }
        }
        return true;
    }

    int adopt(EVP_PKEY* key) noexcept {
        if (key == nullptr || EVP_PKEY_get_base_id(key) != EVP_PKEY_EC || EVP_PKEY_get_bits(key) != 256) {
            EVP_PKEY_free(key);
            capture_error();
            return EINVAL;
        }
        EVP_PKEY_free(key_);
        key_ = key;
        ERR_clear_error();
        return 0;
    }

    void capture_error() noexcept {
        const unsigned long e = ERR_peek_last_error();
        if (e != 0) {
            last_error_ = e;
        }
        ERR_clear_error();
    }

    EVP_PKEY* key_ = nullptr;
    unsigned long last_error_ = 0;
};

}  // namespace fibers::http2
