// fibers::http2 APNs helper: base64url, ES256 JWTs (decoded and verified
// with the public key), the response mapping, and ApnsClient against the
// mock APNs of MockH2Server (h2c and TLS): headers sent, status mapping,
// token caching / refresh, the ExpiredProviderToken retry.

#include <gtest/gtest.h>

#include <openssl/core_names.h>
#include <openssl/ec.h>
#include <openssl/evp.h>
#include <openssl/pem.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "fibers/http2/apns.hpp"
#include "fibers/http2/jwt.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/tls/context.hpp"
#include "h2_test_util.hpp"

namespace http2 = fibers::http2;
namespace net = fibers::net;
namespace tls = fibers::tls;
using fibers::sched::Scheduler;
using namespace std::chrono_literals;
using h2test::MockH2Server;
using h2test::MockOptions;
using h2test::run_in_fiber;

namespace {

// A fresh EC P-256 key: {private PKCS#8 PEM, public SPKI PEM}.
struct TestKey {
    std::string private_pem;
    std::string public_pem;
};

std::string bio_string(BIO* bio) {
    char* data = nullptr;
    const long n = BIO_get_mem_data(bio, &data);
    return std::string(data, static_cast<std::size_t>(n));
}

TestKey make_key(const char* curve = "P-256") {
    TestKey k;
    EVP_PKEY* key = EVP_EC_gen(curve);
    BIO* priv = BIO_new(BIO_s_mem());
    PEM_write_bio_PrivateKey(priv, key, nullptr, nullptr, 0, nullptr, nullptr);  // PKCS#8, like a .p8
    k.private_pem = bio_string(priv);
    BIO_free(priv);
    BIO* pub = BIO_new(BIO_s_mem());
    PEM_write_bio_PUBKEY(pub, key);
    k.public_pem = bio_string(pub);
    BIO_free(pub);
    EVP_PKEY_free(key);
    return k;
}

std::vector<std::string> split(std::string_view jwt) {
    std::vector<std::string> parts;
    std::size_t at = 0;
    for (;;) {
        const std::size_t dot = jwt.find('.', at);
        parts.emplace_back(jwt.substr(at, dot == std::string_view::npos ? std::string_view::npos : dot - at));
        if (dot == std::string_view::npos) {
            break;
        }
        at = dot + 1;
    }
    return parts;
}

// Verifies the JWT's ES256 signature (raw r || s) with the public key.
bool verify_jwt(std::string_view jwt, const std::string& public_pem) {
    const std::size_t last_dot = jwt.rfind('.');
    if (last_dot == std::string_view::npos) {
        return false;
    }
    std::string rs;
    if (!http2::base64url_decode(jwt.substr(last_dot + 1), rs) || rs.size() != 64) {
        return false;
    }
    ECDSA_SIG* sig = ECDSA_SIG_new();
    ECDSA_SIG_set0(sig, BN_bin2bn(reinterpret_cast<const unsigned char*>(rs.data()), 32, nullptr),
                   BN_bin2bn(reinterpret_cast<const unsigned char*>(rs.data()) + 32, 32, nullptr));
    unsigned char* der = nullptr;
    const int der_len = i2d_ECDSA_SIG(sig, &der);
    ECDSA_SIG_free(sig);
    BIO* bio = BIO_new_mem_buf(public_pem.data(), static_cast<int>(public_pem.size()));
    EVP_PKEY* pub = PEM_read_bio_PUBKEY(bio, nullptr, nullptr, nullptr);
    BIO_free(bio);
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    const bool ok = pub != nullptr && EVP_DigestVerifyInit(ctx, nullptr, EVP_sha256(), nullptr, pub) == 1 &&
                    EVP_DigestVerify(ctx, der, static_cast<std::size_t>(der_len),
                                     reinterpret_cast<const unsigned char*>(jwt.data()), last_dot) == 1;
    EVP_MD_CTX_free(ctx);
    EVP_PKEY_free(pub);
    OPENSSL_free(der);
    return ok;
}

std::string cert(const char* name) { return std::string(FIBERS_TLS_CERTS) + "/" + name; }

std::atomic<std::int64_t> g_now{1'800'000'000};
std::int64_t fake_now() noexcept { return g_now.load(); }

http2::ApnsOptions apns_options(std::uint16_t port, const TestKey& key) {
    http2::ApnsOptions o;
    o.team_id = "TEAM123456";
    o.key_id = "KEY1234567";
    o.key_pem = key.private_pem;
    o.http2.host = "127.0.0.1";
    o.http2.port = port;
    o.http2.endpoint = net::Endpoint::loopback_v4(port);
    o.http2.ping_interval = std::chrono::nanoseconds(0);
    o.unix_now = &fake_now;
    return o;
}

}  // namespace

// ---- base64url / JWT ------------------------------------------------------------------

TEST(Jwt, Base64Url) {
    const std::pair<std::string_view, std::string_view> cases[] = {
        {"", ""}, {"f", "Zg"}, {"fo", "Zm8"}, {"foo", "Zm9v"}, {"foob", "Zm9vYg"}, {"fooba", "Zm9vYmE"},
        {"foobar", "Zm9vYmFy"}, {"\xfb\xff\xbf", "-_-_"},
    };
    for (const auto& [plain, encoded] : cases) {
        char out[32];
        const std::size_t n = http2::base64url_encode(plain.data(), plain.size(), out);
        EXPECT_EQ(std::string_view(out, n), encoded);
        EXPECT_EQ(n, http2::base64url_size(plain.size()));
        std::string back;
        ASSERT_TRUE(http2::base64url_decode(encoded, back));
        EXPECT_EQ(back, plain);
    }
    std::string out;
    EXPECT_FALSE(http2::base64url_decode("a", out));
    EXPECT_FALSE(http2::base64url_decode("ab$d", out));
}

TEST(Jwt, Es256SignedAndVerified) {
    const TestKey key = make_key();
    http2::Es256Signer signer;
    ASSERT_EQ(signer.load_pem(key.private_pem), 0);
    char jwt[http2::Es256Signer::kMaxJwt];
    const std::size_t n = signer.sign_jwt("KEY1234567", "TEAM123456", 1'700'000'000, jwt, sizeof jwt);
    ASSERT_GT(n, 0u);
    const std::string_view token(jwt, n);
    const std::vector<std::string> parts = split(token);
    ASSERT_EQ(parts.size(), 3u);
    std::string header, claims;
    ASSERT_TRUE(http2::base64url_decode(parts[0], header));
    ASSERT_TRUE(http2::base64url_decode(parts[1], claims));
    EXPECT_EQ(header, R"({"alg":"ES256","kid":"KEY1234567"})");
    EXPECT_EQ(claims, R"({"iss":"TEAM123456","iat":1700000000})");
    EXPECT_TRUE(verify_jwt(token, key.public_pem));

    // Another key does not verify it; a modified token does not verify.
    EXPECT_FALSE(verify_jwt(token, make_key().public_pem));
    std::string tampered(token);
    tampered[parts[0].size() + 2] = tampered[parts[0].size() + 2] == 'A' ? 'B' : 'A';
    EXPECT_FALSE(verify_jwt(tampered, key.public_pem));

    // ECDSA is randomized: two signatures differ, both verify.
    char jwt2[http2::Es256Signer::kMaxJwt];
    const std::size_t n2 = signer.sign_jwt("KEY1234567", "TEAM123456", 1'700'000'000, jwt2, sizeof jwt2);
    EXPECT_NE(std::string_view(jwt2, n2), token);
    EXPECT_TRUE(verify_jwt(std::string_view(jwt2, n2), key.public_pem));
}

TEST(Jwt, BadKeysAndIds) {
    http2::Es256Signer signer;
    EXPECT_EQ(signer.load_pem("not a key"), EINVAL);
    EXPECT_EQ(signer.load_pem(make_key("P-384").private_pem), EINVAL);  // ES256 needs P-256
    EXPECT_EQ(signer.load_file("/nonexistent/AuthKey.p8"), ENOENT);
    char jwt[http2::Es256Signer::kMaxJwt];
    EXPECT_EQ(signer.sign_jwt("K", "T", 1, jwt, sizeof jwt), 0u);  // no key
    ASSERT_EQ(signer.load_pem(make_key().private_pem), 0);
    EXPECT_EQ(signer.sign_jwt("K\"", "T", 1, jwt, sizeof jwt), 0u);  // needs JSON escaping
    EXPECT_EQ(signer.sign_jwt("", "T", 1, jwt, sizeof jwt), 0u);
    EXPECT_EQ(signer.sign_jwt("K", "T", 1, jwt, 20), 0u);  // no room
    EXPECT_GT(signer.sign_jwt("K", "T", 1, jwt, sizeof jwt), 0u);
}

// ---- response mapping ----------------------------------------------------------------

TEST(Apns, ResponseMapping) {
    using S = http2::ApnsStatus;
    struct Case {
        int status;
        std::string_view body;
        S expected;
    } cases[] = {
        {200, "", S::kSuccess},
        {400, R"({"reason":"BadDeviceToken"})", S::kBadDeviceToken},
        {400, R"({"reason":"BadTopic"})", S::kBadRequest},
        {403, R"({"reason":"ExpiredProviderToken"})", S::kProviderTokenError},
        {403, R"({"reason":"InvalidProviderToken"})", S::kProviderTokenError},
        {403, R"({"reason":"BadCertificate"})", S::kForbidden},
        {404, R"({"reason":"BadPath"})", S::kOtherStatus},
        {410, R"({"reason":"Unregistered","timestamp":1700000000123})", S::kUnregistered},
        {410, R"({"reason":"ExpiredToken"})", S::kUnregistered},
        {413, R"({"reason":"PayloadTooLarge"})", S::kPayloadTooLarge},
        {429, R"({"reason":"TooManyRequests"})", S::kTooManyRequests},
        {500, R"({"reason":"InternalServerError"})", S::kServerError},
        {503, R"({"reason":"ServiceUnavailable"})", S::kServerError},
    };
    for (const Case& c : cases) {
        const http2::ApnsResult r = http2::map_apns_response(c.status, c.body, "id-1");
        EXPECT_EQ(r.status, c.expected) << c.status << " " << c.body;
        EXPECT_EQ(r.http_status, c.status);
        EXPECT_EQ(r.apns_id(), "id-1");
    }
    const http2::ApnsResult gone =
        http2::map_apns_response(410, R"({"reason": "Unregistered", "timestamp": 1700000000123})", "");
    EXPECT_EQ(gone.reason(), "Unregistered");
    EXPECT_EQ(gone.timestamp_ms, 1700000000123);
    EXPECT_STREQ(http2::to_string(http2::ApnsStatus::kUnregistered), "Unregistered");
}

// ---- ApnsClient against the mock ---------------------------------------------------------

TEST(Apns, SendAndStatuses) {
    MockH2Server server;
    ASSERT_TRUE(server.start());
    const TestKey key = make_key();
    Scheduler scheduler(h2test::scheduler_options());
    http2::ApnsClient apns(scheduler, apns_options(server.port(), key));
    ASSERT_EQ(apns.init(), 0);
    std::vector<http2::ApnsResult> results;
    std::string auth, topic, push_type, priority, expiration, collapse, path, method;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Notification n;
        n.topic = "com.example.app";
        n.push_type = "alert";
        n.priority = 10;
        n.expiration = 1'800'003'600;
        n.collapse_id = "group-1";
        n.apns_id = "123e4567-e89b-12d3-a456-426614174000";
        http2::Response scratch;
        const std::string device = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        results.push_back(apns.send(device, R"({"aps":{"alert":"hello"}})", n, scratch));
        auth = server.last_header("authorization");
        topic = server.last_header("apns-topic");
        push_type = server.last_header("apns-push-type");
        priority = server.last_header("apns-priority");
        expiration = server.last_header("apns-expiration");
        collapse = server.last_header("apns-collapse-id");
        path = server.last_path();
        method = server.last_header(":method");
        http2::Notification plain;
        plain.topic = "com.example.app";
        for (const char* token : {"bad", "gone", "busy", "boom", "down", "big"}) {
            results.push_back(apns.send(token, "{}", plain, scratch));
        }
        http2::Notification no_topic;
        results.push_back(apns.send(device, "{}", no_topic));
        apns.close();
    }));
    ASSERT_EQ(results.size(), 8u);
    using S = http2::ApnsStatus;
    EXPECT_EQ(results[0].status, S::kSuccess);
    EXPECT_EQ(results[0].http_status, 200);
    EXPECT_EQ(results[0].apns_id(), "123e4567-e89b-12d3-a456-426614174000");
    EXPECT_EQ(method, "POST");
    EXPECT_EQ(path, "/3/device/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
    EXPECT_EQ(topic, "com.example.app");
    EXPECT_EQ(push_type, "alert");
    EXPECT_EQ(priority, "10");
    EXPECT_EQ(expiration, "1800003600");
    EXPECT_EQ(collapse, "group-1");
    ASSERT_EQ(auth.rfind("bearer ", 0), 0u);
    EXPECT_TRUE(verify_jwt(std::string_view(auth).substr(7), key.public_pem));
    std::string claims;
    ASSERT_TRUE(http2::base64url_decode(split(std::string_view(auth).substr(7))[1], claims));
    EXPECT_EQ(claims, R"({"iss":"TEAM123456","iat":)" + std::to_string(g_now.load()) + "}");

    EXPECT_EQ(results[1].status, S::kBadDeviceToken);
    EXPECT_EQ(results[1].reason(), "BadDeviceToken");
    EXPECT_EQ(results[2].status, S::kUnregistered);
    EXPECT_EQ(results[2].timestamp_ms, 1700000000123);
    EXPECT_EQ(results[3].status, S::kTooManyRequests);
    EXPECT_EQ(results[4].status, S::kServerError);
    EXPECT_EQ(results[4].http_status, 500);
    EXPECT_EQ(results[5].status, S::kServerError);
    EXPECT_EQ(results[5].http_status, 503);
    EXPECT_EQ(results[6].status, S::kPayloadTooLarge);
    EXPECT_EQ(results[7].status, S::kBadRequest);
    EXPECT_EQ(results[7].reason(), "MissingTopic");
    EXPECT_EQ(apns.http2().stats().connects, 1u);
}

TEST(Apns, TokenCachedAndRefreshed) {
    MockH2Server server;
    ASSERT_TRUE(server.start());
    const TestKey key = make_key();
    g_now.store(1'800'000'000);
    Scheduler scheduler(h2test::scheduler_options());
    http2::ApnsClient apns(scheduler, apns_options(server.port(), key));
    ASSERT_EQ(apns.init(), 0);
    std::vector<std::string> tokens;
    std::vector<std::uint64_t> signed_count;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Notification n;
        n.topic = "com.example.app";
        auto send = [&] {
            static_cast<void>(apns.send("abcd", "{}", n));
            tokens.push_back(server.last_header("authorization"));
            signed_count.push_back(apns.tokens_signed());
        };
        send();
        send();                              // cached
        g_now.fetch_add(49 * 60);
        send();                              // still cached at 49 minutes
        g_now.fetch_add(2 * 60);
        send();                              // 51 minutes: re-signed
        send();
        apns.close();
    }));
    ASSERT_EQ(tokens.size(), 5u);
    EXPECT_EQ(tokens[0], tokens[1]);
    EXPECT_EQ(tokens[1], tokens[2]);
    EXPECT_NE(tokens[2], tokens[3]);
    EXPECT_EQ(tokens[3], tokens[4]);
    EXPECT_EQ(signed_count, (std::vector<std::uint64_t>{1, 1, 1, 2, 2}));
    std::string claims;
    ASSERT_TRUE(http2::base64url_decode(split(std::string_view(tokens[3]).substr(7))[1], claims));
    EXPECT_EQ(claims, R"({"iss":"TEAM123456","iat":)" + std::to_string(g_now.load()) + "}");
    EXPECT_TRUE(verify_jwt(std::string_view(tokens[3]).substr(7), key.public_pem));
}

TEST(Apns, ExpiredProviderTokenRetriedOnce) {
    MockH2Server server;
    ASSERT_TRUE(server.start());
    const TestKey key = make_key();
    Scheduler scheduler(h2test::scheduler_options());
    http2::ApnsClient apns(scheduler, apns_options(server.port(), key));
    ASSERT_EQ(apns.init(), 0);
    http2::ApnsResult r;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Notification n;
        n.topic = "com.example.app";
        r = apns.send("expired-once", "{}", n);
        apns.close();
    }));
    EXPECT_EQ(r.status, http2::ApnsStatus::kSuccess);
    EXPECT_EQ(server.expired_once.load(), 2);  // a 403, then the retry
    EXPECT_EQ(apns.tokens_signed(), 2u);
    EXPECT_EQ(server.requests.load(), 2);
}

TEST(Apns, ConcurrentSendsShareOneConnection) {
    MockH2Server server;
    ASSERT_TRUE(server.start());
    const TestKey key = make_key();
    Scheduler scheduler(h2test::scheduler_options());
    http2::ApnsClient apns(scheduler, apns_options(server.port(), key));
    ASSERT_EQ(apns.init(), 0);
    std::atomic<int> ok{0};
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        fibers::fiber::WaitGroup wg;
        for (int f = 0; f < 32; ++f) {
            wg.add(1);
            fibers::fiber::go(scheduler, [&]() noexcept {
                http2::Notification n;
                n.topic = "com.example.app";
                http2::Response scratch;
                for (int i = 0; i < 20; ++i) {
                    if (apns.send("feedface", R"({"aps":{"badge":1}})", n, scratch).status ==
                        http2::ApnsStatus::kSuccess) {
                        ok.fetch_add(1);
                    }
                }
                wg.done();
            });
        }
        wg.wait();
        apns.close();
    }));
    EXPECT_EQ(ok.load(), 640);
    EXPECT_EQ(apns.http2().stats().connects, 1u);
    EXPECT_EQ(apns.tokens_signed(), 1u);
}

TEST(Apns, OverTls) {
    SSL_CTX* sctx = h2test::make_mock_tls(cert("server.pem"), cert("server.key"));
    ASSERT_NE(sctx, nullptr);
    MockOptions mo;
    mo.tls = sctx;
    MockH2Server server(mo);
    ASSERT_TRUE(server.start());
    tls::TlsContext ctx;
    const std::string ca = cert("ca.pem");
    const std::string_view alpn[] = {"h2"};
    tls::ClientOptions co;
    co.ca_file = ca.c_str();
    co.alpn = alpn;
    ASSERT_EQ(ctx.init_client(co), 0);
    const TestKey key = make_key();
    http2::ApnsOptions o = apns_options(server.port(), key);
    o.http2.host = "localhost";
    o.http2.tls = &ctx;
    Scheduler scheduler(h2test::scheduler_options());
    http2::ApnsClient apns(scheduler, o);
    ASSERT_EQ(apns.init(), 0);
    http2::ApnsResult ok;
    http2::ApnsResult gone;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Notification n;
        n.topic = "com.example.app";
        n.push_type = "background";
        n.priority = 5;
        ok = apns.send("cafe", R"({"aps":{"content-available":1}})", n);
        gone = apns.send("gone", "{}", n);
        apns.close();
    }));
    server.stop();
    SSL_CTX_free(sctx);
    EXPECT_EQ(ok.status, http2::ApnsStatus::kSuccess);
    EXPECT_EQ(gone.status, http2::ApnsStatus::kUnregistered);
}

TEST(Apns, TransportErrors) {
    const TestKey key = make_key();
    Scheduler scheduler(h2test::scheduler_options());
    http2::ApnsOptions o = apns_options(h2test::free_port(), key);  // nobody listens
    http2::ApnsClient apns(scheduler, o);
    ASSERT_EQ(apns.init(), 0);
    http2::ApnsResult r;
    http2::ApnsResult empty;
    ASSERT_TRUE(run_in_fiber(scheduler, [&] {
        http2::Notification n;
        n.topic = "com.example.app";
        r = apns.send("abcd", "{}", n);
        empty = apns.send("", "{}", n);
        apns.close();
    }));
    EXPECT_EQ(r.status, http2::ApnsStatus::kTransportError);
    EXPECT_EQ(r.error, ECONNREFUSED);
    EXPECT_EQ(empty.error, EINVAL);

    http2::ApnsOptions bad = apns_options(1, key);
    bad.key_pem = "garbage";
    http2::ApnsClient no_key(scheduler, bad);
    EXPECT_EQ(no_key.init(), EINVAL);
}
