#pragma once

// A small Apple Push Notification service client over http2::Client, with
// token-based authentication (ES256 JWT from a .p8 key).
//
//   tls::TlsContext ctx;                      // client, use_default_ca, alpn = {"h2"}
//   http2::ApnsOptions o;
//   o.team_id = "DEF123GHIJ";  o.key_id = "ABC123DEFG";  o.key_file = "AuthKey_ABC123DEFG.p8";
//   o.http2.tls = &ctx;                       // host / port default to api.push.apple.com:443
//   http2::ApnsClient apns(scheduler, o);
//   if (apns.init() != 0) { ... bad key ... }
//   ... in fibers:
//   http2::Notification n;  n.topic = "com.example.app";
//   http2::ApnsResult r = apns.send(device_token_hex, R"({"aps":{"alert":"hi"}})", n);
//   switch (r.status) {
//     case http2::ApnsStatus::kSuccess:       ... r.apns_id() ...
//     case http2::ApnsStatus::kUnregistered:  ... forget the token (r.timestamp_ms) ...
//     case http2::ApnsStatus::kTooManyRequests / kServerError: ... back off, retry ...
//     case http2::ApnsStatus::kTransportError: ... r.error; http2::retryable(r.error) ...
//     ...
//   }
//   apns.close();                             // from a fiber
//
// Requests: POST /3/device/<token> with authorization: bearer <jwt>,
// apns-topic, apns-push-type, and optionally apns-priority,
// apns-expiration, apns-collapse-id, apns-id; the body is the JSON payload
// (Apple's limits: 4 KiB, 5 KiB for VoIP). Responses: 200 + apns-id, or a
// status with {"reason":"..."} (+ "timestamp" for 410), mapped to
// ApnsStatus (Apple's "Handling notification responses from APNs").
//
// The provider token is cached and re-signed every token_refresh (default
// 50 minutes: Apple rejects tokens older than an hour and refreshes more
// often than every 20 minutes with TooManyProviderTokenUpdates). A 403
// ExpiredProviderToken / InvalidProviderToken forces a new token, and the
// notification is sent once more with it (it was rejected, not delivered).
// One fiber signs at a time (a fiber::Mutex); the others keep using the
// current token meanwhile or wait for the first one. Signing does not park
// between OpenSSL calls (jwt.hpp).
//
// Memory: send() builds its headers and path on the stack; the Response is
// the caller's scratch (send(..., Response&) reuses its buffers) or a local
// one (a few small allocations per call).

#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>
#include <string_view>

#include "fibers/fiber/mutex.hpp"
#include "fibers/http2/client.hpp"
#include "fibers/http2/jwt.hpp"
#include "fibers/sync/spin_lock.hpp"

namespace fibers::http2 {

enum class ApnsStatus : std::uint8_t {
    kSuccess,             // 200: accepted (apns_id())
    kBadDeviceToken,      // 400 BadDeviceToken: the token is invalid (or for the other environment)
    kBadRequest,          // other 400 (BadTopic, PayloadEmpty, BadPriority, ...): reason()
    kProviderTokenError,  // 403 ExpiredProviderToken / InvalidProviderToken / MissingProviderToken
    kForbidden,           // other 403 (BadCertificate, TopicDisallowed, ...)
    kUnregistered,        // 410 Unregistered / ExpiredToken: stop sending to this token (timestamp_ms)
    kPayloadTooLarge,     // 413
    kTooManyRequests,     // 429 (TooManyRequests, TooManyProviderTokenUpdates): back off
    kServerError,         // 500 InternalServerError, 503 ServiceUnavailable / Shutdown: retry later
    kOtherStatus,         // anything else (404 BadPath, 405 BadMethod, ...): http_status, reason()
    kTransportError,      // no response: error (errno; retryable(error): not delivered, resend)
};

const char* to_string(ApnsStatus s) noexcept;

struct ApnsResult {
    ApnsStatus status = ApnsStatus::kTransportError;
    int http_status = 0;
    int error = 0;               // kTransportError: the errno (message.hpp)
    std::int64_t timestamp_ms = 0;  // 410: when the token stopped being valid (ms since the epoch)

    std::string_view apns_id() const noexcept { return {apns_id_, apns_id_len_}; }
    std::string_view reason() const noexcept { return {reason_, reason_len_}; }

    char apns_id_[64] = {};
    std::size_t apns_id_len_ = 0;
    char reason_[64] = {};
    std::size_t reason_len_ = 0;
};

struct Notification {
    // apns-topic: the app's bundle id (+ ".voip", ".complication", ...).
    std::string_view topic;
    // apns-push-type: alert, background, voip, complication, fileprovider,
    // mdm, liveactivity, location, pushtotalk, widgets.
    std::string_view push_type = "alert";
    // apns-priority: 10, 5 or 1; 0: not sent (APNs: 10).
    int priority = 0;
    // apns-expiration: UNIX seconds (0: deliver once or never); < 0: not sent.
    std::int64_t expiration = -1;
    // apns-collapse-id (up to 64 bytes); empty: not sent.
    std::string_view collapse_id;
    // apns-id (a UUID, echoed back); empty: APNs assigns one.
    std::string_view apns_id;
    // As Request::timeout (zero: ClientOptions::request_timeout).
    poller::Duration timeout = poller::Duration::zero();
};

struct ApnsOptions {
    std::string team_id;  // "iss"
    std::string key_id;   // "kid"
    // The .p8 key: a file, or its PEM text (key_pem wins when set).
    std::string key_file;
    std::string key_pem;
    // false: api.sandbox.push.apple.com (development tokens).
    bool production = true;
    // Connection settings. host "" (default here): api[.sandbox].push.apple.com.
    // Set `tls` (ALPN "h2", a CA store that trusts Apple's servers).
    ClientOptions http2 = default_http2();
    // Re-sign the provider token after this long.
    std::chrono::seconds token_refresh = std::chrono::minutes(50);
    // The clock for "iat" (seconds since the epoch); nullptr: system_clock.
    std::int64_t (*unix_now)() noexcept = nullptr;

    static ClientOptions default_http2() {
        ClientOptions o;
        o.host.clear();
        o.port = 443;
        o.ping_interval = std::chrono::minutes(5);
        return o;
    }
};

namespace detail {

// The string value of "key" in a flat JSON object ({"reason":"BadDeviceToken"}).
inline std::string_view json_string(std::string_view json, std::string_view key) noexcept {
    for (std::size_t at = json.find('"'); at != std::string_view::npos; at = json.find('"', at + 1)) {
        if (json.compare(at + 1, key.size(), key) != 0 || at + 1 + key.size() >= json.size() ||
            json[at + 1 + key.size()] != '"') {
            continue;
        }
        std::size_t p = at + key.size() + 2;
        while (p < json.size() && (json[p] == ' ' || json[p] == ':')) {
            ++p;
        }
        if (p >= json.size() || json[p] != '"') {
            return {};
        }
        const std::size_t end = json.find('"', p + 1);
        return end == std::string_view::npos ? std::string_view{} : json.substr(p + 1, end - p - 1);
    }
    return {};
}

// The integer value of "key" (0 if none).
inline std::int64_t json_int(std::string_view json, std::string_view key) noexcept {
    for (std::size_t at = json.find('"'); at != std::string_view::npos; at = json.find('"', at + 1)) {
        if (json.compare(at + 1, key.size(), key) != 0 || at + 1 + key.size() >= json.size() ||
            json[at + 1 + key.size()] != '"') {
            continue;
        }
        std::size_t p = at + key.size() + 2;
        while (p < json.size() && (json[p] == ' ' || json[p] == ':')) {
            ++p;
        }
        std::int64_t v = 0;
        std::from_chars(json.data() + p, json.data() + json.size(), v);
        return v;
    }
    return 0;
}

inline void copy_into(char (&dst)[64], std::size_t& len, std::string_view src) noexcept {
    len = src.size() < sizeof dst ? src.size() : sizeof dst - 1;
    if (len != 0) {
        std::memcpy(dst, src.data(), len);
    }
    dst[len] = '\0';
}

}  // namespace detail

inline const char* to_string(ApnsStatus s) noexcept {
    switch (s) {
        case ApnsStatus::kSuccess: return "Success";
        case ApnsStatus::kBadDeviceToken: return "BadDeviceToken";
        case ApnsStatus::kBadRequest: return "BadRequest";
        case ApnsStatus::kProviderTokenError: return "ProviderTokenError";
        case ApnsStatus::kForbidden: return "Forbidden";
        case ApnsStatus::kUnregistered: return "Unregistered";
        case ApnsStatus::kPayloadTooLarge: return "PayloadTooLarge";
        case ApnsStatus::kTooManyRequests: return "TooManyRequests";
        case ApnsStatus::kServerError: return "ServerError";
        case ApnsStatus::kOtherStatus: return "OtherStatus";
        case ApnsStatus::kTransportError: return "TransportError";
    }
    return "?";
}

// Maps an APNs response (status, body, apns-id header) to an ApnsResult.
inline ApnsResult map_apns_response(int http_status, std::string_view body, std::string_view apns_id) noexcept {
    ApnsResult r;
    r.http_status = http_status;
    detail::copy_into(r.apns_id_, r.apns_id_len_, apns_id);
    const std::string_view reason = detail::json_string(body, "reason");
    detail::copy_into(r.reason_, r.reason_len_, reason);
    switch (http_status) {
        case 200: r.status = ApnsStatus::kSuccess; break;
        case 400: r.status = reason == "BadDeviceToken" ? ApnsStatus::kBadDeviceToken : ApnsStatus::kBadRequest; break;
        case 403:
            r.status = (reason == "ExpiredProviderToken" || reason == "InvalidProviderToken" ||
                        reason == "MissingProviderToken")
                           ? ApnsStatus::kProviderTokenError
                           : ApnsStatus::kForbidden;
            break;
        case 410:
            r.status = ApnsStatus::kUnregistered;
            r.timestamp_ms = detail::json_int(body, "timestamp");
            break;
        case 413: r.status = ApnsStatus::kPayloadTooLarge; break;
        case 429: r.status = ApnsStatus::kTooManyRequests; break;
        default: r.status = http_status >= 500 ? ApnsStatus::kServerError : ApnsStatus::kOtherStatus; break;
    }
    return r;
}

template <net::NetScheduler Sched = sched::Scheduler>
class BasicApnsClient {
public:
    // Bearer token: "bearer " + the JWT.
    static constexpr std::size_t kMaxAuthorization = 8 + Es256Signer::kMaxJwt;
    // Device tokens are 64 hex digits today; Apple may make them longer.
    static constexpr std::size_t kMaxDeviceToken = 200;

    BasicApnsClient(Sched& scheduler, ApnsOptions options) noexcept
        : options_(prepare(std::move(options))), client_(scheduler, options_.http2) {}

    BasicApnsClient(const BasicApnsClient&) = delete;
    BasicApnsClient& operator=(const BasicApnsClient&) = delete;

    // Loads the key and signs a first token (blocking file I/O when
    // key_file is used: at startup). 0, the file's errno, or EINVAL (not an
    // EC P-256 private key, or bad team / key ids).
    int init() noexcept {
        const int e = options_.key_pem.empty() ? signer_.load_file(options_.key_file.c_str())
                                               : signer_.load_pem(options_.key_pem);
        if (e != 0) {
            return e;
        }
        return sign_new_token(unix_now()) ? 0 : EINVAL;
    }

    // Sends one notification (see the top comment). From a fiber.
    ApnsResult send(std::string_view device_token, std::string_view payload, const Notification& n,
                    Response& scratch) noexcept {
        ApnsResult result;
        if (device_token.empty() || device_token.size() > kMaxDeviceToken) {
            result.error = EINVAL;
            return result;
        }
        char path[16 + kMaxDeviceToken];
        std::memcpy(path, "/3/device/", 10);
        std::memcpy(path + 10, device_token.data(), device_token.size());
        char priority[8];
        char expiration[24];
        for (int attempt = 0;; ++attempt) {
            char authorization[kMaxAuthorization];
            std::uint64_t generation = 0;
            const std::size_t auth_len = current_authorization(authorization, generation);
            if (auth_len == 0) {
                result.error = EINVAL;  // no key (init() not called / failed)
                return result;
            }
            Header headers[8];
            std::size_t h = 0;
            headers[h++] = {"authorization", std::string_view(authorization, auth_len)};
            if (!n.topic.empty()) {
                headers[h++] = {"apns-topic", n.topic};
            }
            if (!n.push_type.empty()) {
                headers[h++] = {"apns-push-type", n.push_type};
            }
            if (n.priority > 0) {
                const auto r = std::to_chars(priority, priority + sizeof priority, n.priority);
                headers[h++] = {"apns-priority", std::string_view(priority, static_cast<std::size_t>(r.ptr - priority))};
            }
            if (n.expiration >= 0) {
                const auto r = std::to_chars(expiration, expiration + sizeof expiration, n.expiration);
                headers[h++] = {"apns-expiration",
                                std::string_view(expiration, static_cast<std::size_t>(r.ptr - expiration))};
            }
            if (!n.collapse_id.empty()) {
                headers[h++] = {"apns-collapse-id", n.collapse_id};
            }
            if (!n.apns_id.empty()) {
                headers[h++] = {"apns-id", n.apns_id};
            }
            headers[h++] = {"content-type", "application/json"};
            Request req;
            req.method = "POST";
            req.path = std::string_view(path, 10 + device_token.size());
            req.headers = std::span<const Header>(headers, h);
            req.body = payload;
            req.timeout = n.timeout;
            const int e = client_.request(req, scratch);
            if (e != 0) {
                result = ApnsResult{};
                result.error = e;
                return result;
            }
            result = map_apns_response(scratch.status(), scratch.body(), scratch.header("apns-id"));
            if (result.status == ApnsStatus::kProviderTokenError && attempt == 0 &&
                result.reason() != "MissingProviderToken") {
                force_refresh(generation);  // rejected, not delivered: once more with a new token
                continue;
            }
            return result;
        }
    }

    ApnsResult send(std::string_view device_token, std::string_view payload, const Notification& n) noexcept {
        Response scratch;
        return send(device_token, payload, n, scratch);
    }

    // The provider token in use now (tests / logging).
    std::string current_token() noexcept {
        char buf[kMaxAuthorization];
        std::uint64_t generation = 0;
        const std::size_t n = current_authorization(buf, generation);
        return n > 7 ? std::string(buf + 7, n - 7) : std::string();
    }

    // Tokens signed so far.
    std::uint64_t tokens_signed() noexcept {
        std::lock_guard guard(token_lock_);
        return tokens_signed_;
    }

    void close() noexcept { client_.close(); }

    BasicClient<Sched>& http2() noexcept { return client_; }
    const ApnsOptions& options() const noexcept { return options_; }

private:
    static ApnsOptions prepare(ApnsOptions o) {
        if (o.http2.host.empty()) {
            o.http2.host = o.production ? "api.push.apple.com" : "api.sandbox.push.apple.com";
        }
        return o;
    }

    std::int64_t unix_now() const noexcept {
        if (options_.unix_now != nullptr) {
            return options_.unix_now();
        }
        return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

    // Copies "bearer <jwt>" into `out` (NUL-terminated), re-signing first
    // if the token is due; `generation` identifies the token. Its length, 0
    // without a token.
    std::size_t current_authorization(char (&out)[kMaxAuthorization], std::uint64_t& generation) noexcept {
        const std::int64_t now = unix_now();
        if (const std::size_t n = copy_token(out, generation, now, true); n != 0) {
            return n;
        }
        std::lock_guard guard(refresh_mu_);  // one signer; may park (no OpenSSL state pending)
        if (const std::size_t n = copy_token(out, generation, now, true); n != 0) {
            return n;  // another fiber re-signed meanwhile
        }
        static_cast<void>(sign_new_token(now));
        return copy_token(out, generation, now, false);
    }

    // The token, if there is one and (require_fresh) it is not due.
    std::size_t copy_token(char (&out)[kMaxAuthorization], std::uint64_t& generation, std::int64_t now,
                           bool require_fresh) noexcept {
        std::lock_guard guard(token_lock_);
        if (token_len_ == 0) {
            return 0;
        }
        if (require_fresh && (stale_ || now - issued_at_ >= options_.token_refresh.count() || now < issued_at_)) {
            return 0;
        }
        std::memcpy(out, "bearer ", 7);
        std::memcpy(out + 7, token_, token_len_);
        out[7 + token_len_] = '\0';
        generation = tokens_signed_;
        return 7 + token_len_;
    }

    // Under refresh_mu_ (or in init()).
    bool sign_new_token(std::int64_t now) noexcept {
        char jwt[Es256Signer::kMaxJwt];
        const std::size_t n = signer_.sign_jwt(options_.key_id, options_.team_id, now, jwt, sizeof jwt);
        if (n == 0) {
            return false;
        }
        std::lock_guard guard(token_lock_);
        std::memcpy(token_, jwt, n);
        token_len_ = n;
        issued_at_ = now;
        stale_ = false;
        ++tokens_signed_;
        return true;
    }

    // The token of `generation` was rejected: re-sign before the next use
    // (unless that happened already).
    void force_refresh(std::uint64_t generation) noexcept {
        std::lock_guard guard(token_lock_);
        if (generation == tokens_signed_) {
            stale_ = true;
        }
    }

    ApnsOptions options_;
    BasicClient<Sched> client_;
    Es256Signer signer_;
    fiber::BasicMutex<Sched> refresh_mu_;
    sync::SpinLock<typename Sched::SyncPolicy> token_lock_;  // the fields below
    char token_[Es256Signer::kMaxJwt] = {};
    std::size_t token_len_ = 0;
    std::int64_t issued_at_ = 0;
    std::uint64_t tokens_signed_ = 0;
    bool stale_ = false;
};

using ApnsClient = BasicApnsClient<sched::Scheduler>;

}  // namespace fibers::http2
