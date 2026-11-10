// Sends push notifications through Apple's push service (APNs) with
// fibers::http2's ApnsClient: token-based auth (ES256 JWT from a .p8 key),
// HTTP/2 over TLS (ALPN "h2"), N notifications multiplexed on one
// connection.
//
//   apns_send --team=DEF123GHIJ --key-id=ABC123DEFG --key=AuthKey_ABC123DEFG.p8 \
//             --topic=com.example.app --token=<64 hex digits> \
//             [--sandbox] [--payload='{"aps":{"alert":"hello"}}'] [--push-type=alert]
//             [--priority=10] [--count=1] [--concurrency=1]
//             [--host=api.push.apple.com --port=443] [--ca=<PEM bundle>] (default: the system store)
//
// Prints one line per distinct result (status, reason, count) and the
// apns-id of the first accepted notification.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/wait_group.hpp"
#include "fibers/http2/apns.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/tls/context.hpp"

namespace fiber = fibers::fiber;
namespace http2 = fibers::http2;
namespace tls = fibers::tls;

namespace {

const char* arg(int argc, char** argv, const char* name, const char* fallback = nullptr) {
    const std::size_t n = std::strlen(name);
    for (int i = 1; i < argc; ++i) {
        if (std::strncmp(argv[i], name, n) == 0 && argv[i][n] == '=') {
            return argv[i] + n + 1;
        }
        if (std::strcmp(argv[i], name) == 0) {
            return "";
        }
    }
    return fallback;
}

}  // namespace

int main(int argc, char** argv) {
    const char* team = arg(argc, argv, "--team");
    const char* key_id = arg(argc, argv, "--key-id");
    const char* key = arg(argc, argv, "--key");
    const char* topic = arg(argc, argv, "--topic");
    const char* token = arg(argc, argv, "--token");
    if (team == nullptr || key_id == nullptr || key == nullptr || topic == nullptr || token == nullptr) {
        std::fprintf(stderr,
                     "usage: apns_send --team=T --key-id=K --key=AuthKey.p8 --topic=bundle.id --token=HEX "
                     "[--sandbox] [--payload=JSON] [--push-type=alert] [--priority=10] [--count=1] "
                     "[--concurrency=1] [--host=H --port=P] [--ca=PEM]\n");
        return 2;
    }
    const std::string payload = arg(argc, argv, "--payload", R"({"aps":{"alert":"hello from fibers"}})");
    const int count = std::atoi(arg(argc, argv, "--count", "1"));
    const int concurrency = std::max(1, std::atoi(arg(argc, argv, "--concurrency", "1")));

    tls::TlsContext ctx;
    const std::string_view alpn[] = {"h2"};
    tls::ClientOptions co;
    co.alpn = alpn;
    co.ca_file = arg(argc, argv, "--ca");
    co.use_default_ca = co.ca_file == nullptr;
    char err[256];
    if (ctx.init_client(co) != 0) {
        std::fprintf(stderr, "TLS context: %s\n", ctx.error_string(err, sizeof err));
        return 1;
    }

    http2::ApnsOptions o;
    o.team_id = team;
    o.key_id = key_id;
    o.key_file = key;
    o.production = arg(argc, argv, "--sandbox") == nullptr;
    o.http2.tls = &ctx;
    if (const char* host = arg(argc, argv, "--host"); host != nullptr) {
        o.http2.host = host;
    }
    o.http2.port = static_cast<std::uint16_t>(std::atoi(arg(argc, argv, "--port", "443")));

    fibers::sched::SchedulerOptions so;
    so.pool.num_workers = 2;
    so.pollers.num_pollers = 1;
    so.blocking.num_threads = 1;
    fibers::sched::Scheduler scheduler(so);
    http2::ApnsClient apns(scheduler, o);
    if (const int e = apns.init(); e != 0) {
        std::fprintf(stderr, "key %s: %s\n", key, std::strerror(e));
        return 1;
    }

    std::mutex mu;
    std::map<std::string, int> results;
    std::string first_id;
    std::atomic<int> next{0};
    std::atomic<bool> done{false};
    const auto start = std::chrono::steady_clock::now();
    fiber::go(scheduler, [&]() noexcept {
        fiber::WaitGroup wg;
        for (int w = 0; w < concurrency; ++w) {
            wg.add(1);
            fiber::go(scheduler, [&]() noexcept {
                http2::Notification n;
                n.topic = topic;
                n.push_type = arg(argc, argv, "--push-type", "alert");
                n.priority = std::atoi(arg(argc, argv, "--priority", "0"));
                http2::Response scratch;
                while (next.fetch_add(1) < count) {
                    const http2::ApnsResult r = apns.send(token, payload, n, scratch);
                    std::string line = http2::to_string(r.status);
                    line += r.status == http2::ApnsStatus::kTransportError
                                ? std::string(" (") + std::strerror(r.error) + ")"
                                : " " + std::to_string(r.http_status) + " " + std::string(r.reason());
                    std::lock_guard g(mu);
                    ++results[line];
                    if (r.status == http2::ApnsStatus::kSuccess && first_id.empty()) {
                        first_id = std::string(r.apns_id());
                    }
                }
                wg.done();
            });
        }
        wg.wait();
        apns.close();
        done.store(true);
    });
    while (!done.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const double s = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    for (const auto& [line, n] : results) {
        std::printf("%6d  %s\n", n, line.c_str());
    }
    if (!first_id.empty()) {
        std::printf("apns-id of the first accepted: %s\n", first_id.c_str());
    }
    std::printf("%d notifications in %.3f s\n", count, s);
    return results.size() == 1 && results.begin()->first.rfind("Success", 0) == 0 ? 0 : 1;
}
