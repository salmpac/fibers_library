// net::resolve: numeric fast path, getaddrinfo on the blocking pool
// (localhost, unknown hosts), family filters, many concurrent lookups on few
// workers.
//
// Fibers never use gtest assertions themselves: they record into atomics /
// plain results that the test thread checks after a Latch. No test depends
// on the network: "localhost" comes from /etc/hosts (nss-files) or
// nss-myhostname, ".invalid" is reserved (RFC 6761) and fails without a
// DNS round trip on resolvers that know it, quickly enough elsewhere.

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <thread>

#include "fibers/fiber/blocking.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/net/resolver.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace net = fibers::net;
using fibers::sched::Scheduler;
using net::AddressFamily;
using net::Endpoint;
using net::ResolveOptions;
using net::ResolveResult;
using namespace std::chrono_literals;

namespace {

class Latch {
public:
    explicit Latch(std::int64_t count) : count_(count) {}

    void count_down() noexcept { count_.fetch_sub(1, std::memory_order_acq_rel); }

    bool wait(std::chrono::milliseconds timeout = 120s) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (count_.load(std::memory_order_acquire) != 0) {
            if (std::chrono::steady_clock::now() > deadline) {
                return false;
            }
            std::this_thread::sleep_for(100us);
        }
        return true;
    }

private:
    std::atomic<std::int64_t> count_;
};

fibers::sched::SchedulerOptions options(std::size_t workers, std::size_t blocking_threads = 2) {
    fibers::sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = 1;
    o.blocking.num_threads = blocking_threads;
    o.stacks.max_stacks = 1024;
    return o;
}

bool is_loopback(const Endpoint& e) {
    if (e.family() == AF_INET) {
        const auto* in = reinterpret_cast<const sockaddr_in*>(&e.storage);
        return (ntohl(in->sin_addr.s_addr) >> 24) == 127;
    }
    if (e.family() == AF_INET6) {
        const auto* in6 = reinterpret_cast<const sockaddr_in6*>(&e.storage);
        return IN6_IS_ADDR_LOOPBACK(&in6->sin6_addr) != 0;
    }
    return false;
}

// Runs f() in a fiber of `scheduler` and waits for it.
template <class F>
void in_fiber(Scheduler& scheduler, F f) {
    Latch done(1);
    fiber::go(scheduler, [&]() noexcept {
        f();
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
}

TEST(Resolver, NumericV4) {
    const ResolveResult r = net::resolve("127.0.0.1", 8080);  // outside a fiber: inline
    ASSERT_TRUE(r) << r.error;
    ASSERT_EQ(r.size(), 1u);
    EXPECT_EQ(r[0].family(), AF_INET);
    EXPECT_EQ(r[0].port(), 8080);
    EXPECT_EQ(r[0].size, sizeof(sockaddr_in));
    EXPECT_TRUE(is_loopback(r[0]));
}

TEST(Resolver, NumericV6) {
    const ResolveResult r = net::resolve("::1", 443);
    ASSERT_TRUE(r) << r.error;
    ASSERT_EQ(r.size(), 1u);
    EXPECT_EQ(r[0].family(), AF_INET6);
    EXPECT_EQ(r[0].port(), 443);
    EXPECT_EQ(r[0].size, sizeof(sockaddr_in6));
    EXPECT_TRUE(is_loopback(r[0]));
}

TEST(Resolver, NumericFamilyMismatch) {
    ResolveResult r = net::resolve("::1", 1, ResolveOptions{.family = AddressFamily::kV4});
    EXPECT_EQ(r.error, ENOENT);
    EXPECT_TRUE(r.empty());
    r = net::resolve("10.1.2.3", 1, ResolveOptions{.family = AddressFamily::kV6});
    EXPECT_EQ(r.error, ENOENT);
    r = net::resolve("10.1.2.3", 1, ResolveOptions{.family = AddressFamily::kV4});
    EXPECT_TRUE(r);
}

// Numeric hosts never reach the blocking pool: with its only thread
// blocked, numeric lookups from a fiber still complete (a pooled call would
// queue behind the blocker until it is released).
TEST(Resolver, NumericFastPathSkipsThePool) {
    Scheduler scheduler(options(2, 1));
    std::atomic<bool> release{false};
    std::atomic<bool> blocking{false};
    Latch blocker_done(1);
    fiber::go(scheduler, [&]() noexcept {
        fiber::this_fiber::blocking([&]() noexcept {
            blocking.store(true);
            while (!release.load()) {
                std::this_thread::sleep_for(1ms);
            }
        });
        blocker_done.count_down();
    });
    while (!blocking.load()) {
        std::this_thread::sleep_for(1ms);
    }
    std::atomic<bool> ok{false};
    Latch done(1);
    fiber::go(scheduler, [&]() noexcept {
        ok.store(net::resolve(scheduler, "192.0.2.7", 9).size() == 1 &&
                 net::resolve(scheduler, "2001:db8::1", 9).size() == 1);
        done.count_down();
    });
    const bool finished = done.wait(60s);
    release.store(true);
    ASSERT_TRUE(blocker_done.wait());
    ASSERT_TRUE(finished);
    EXPECT_TRUE(ok.load());
}

TEST(Resolver, BadArguments) {
    EXPECT_EQ(net::resolve("", 80).error, EINVAL);
    EXPECT_EQ(net::resolve(std::string_view("a\0b", 3), 80).error, EINVAL);
    EXPECT_EQ(net::resolve("localhost", 80, ResolveOptions{.max_results = 0}).error, EINVAL);
    const std::string long_name(NI_MAXHOST, 'a');
    EXPECT_EQ(net::resolve(long_name, 80).error, ENAMETOOLONG);
}

TEST(Resolver, LocalhostOnTheBlockingPool) {
    Scheduler scheduler(options(2));
    ResolveResult any;
    ResolveResult v4;
    in_fiber(scheduler, [&]() noexcept {
        any = net::resolve(scheduler, "localhost", 5432);
        v4 = net::resolve(scheduler, "localhost", 5432, ResolveOptions{.family = AddressFamily::kV4});
    });
    ASSERT_TRUE(any) << any.error << " " << ::gai_strerror(any.gai_error);
    ASSERT_GE(any.size(), 1u);
    for (const Endpoint& e : any) {
        EXPECT_TRUE(is_loopback(e));
        EXPECT_EQ(e.port(), 5432);
    }
    ASSERT_TRUE(v4) << v4.error;
    ASSERT_GE(v4.size(), 1u);
    for (const Endpoint& e : v4) {
        EXPECT_EQ(e.family(), AF_INET);
        EXPECT_TRUE(is_loopback(e));
    }
}

// max_results and the capacity bound the count.
TEST(Resolver, ResultLimits) {
    const ResolveResult limited = net::resolve("localhost", 1, ResolveOptions{.max_results = 1});
    ASSERT_TRUE(limited) << limited.error;
    EXPECT_EQ(limited.size(), 1u);
    const net::BasicResolveResult<1> one = net::resolve<Scheduler, 1>("localhost", 1);
    ASSERT_TRUE(one) << one.error;
    EXPECT_EQ(one.size(), 1u);
}

TEST(Resolver, UnknownHost) {
    Scheduler scheduler(options(1));
    ResolveResult r;
    in_fiber(scheduler, [&]() noexcept { r = net::resolve(scheduler, "no-such-host.invalid", 80); });
    EXPECT_FALSE(r);
    EXPECT_TRUE(r.empty());
    // ENOENT normally; EAGAIN if the system resolver is unreachable.
    EXPECT_TRUE(r.error == ENOENT || r.error == EAGAIN) << r.error << " " << ::gai_strerror(r.gai_error);
    EXPECT_NE(r.gai_error, 0);
}

TEST(Resolver, GaiErrorMapping) {
    using net::detail::gai_to_errno;
    EXPECT_EQ(gai_to_errno(0, 0), 0);
    EXPECT_EQ(gai_to_errno(EAI_NONAME, 0), ENOENT);
    EXPECT_EQ(gai_to_errno(EAI_AGAIN, 0), EAGAIN);
    EXPECT_EQ(gai_to_errno(EAI_FAIL, 0), EIO);
    EXPECT_EQ(gai_to_errno(EAI_MEMORY, 0), ENOMEM);
    EXPECT_EQ(gai_to_errno(EAI_FAMILY, 0), EAFNOSUPPORT);
    EXPECT_EQ(gai_to_errno(EAI_SYSTEM, EMFILE), EMFILE);
    EXPECT_EQ(gai_to_errno(EAI_SYSTEM, 0), EIO);
}

// Many lookups from many fibers on 2 workers and 2 blocking threads: the
// fibers park (the workers stay free), every lookup completes.
TEST(Resolver, ManyConcurrentOnFewWorkers) {
    constexpr int kFibers = 200;
    Scheduler scheduler(options(2, 2));
    Latch done(kFibers);
    std::atomic<int> ok{0};
    std::atomic<int> numeric_ok{0};
    std::atomic<int> first_error{0};
    for (int i = 0; i < kFibers; ++i) {
        fiber::go(scheduler, [&, i]() noexcept {
            const ResolveResult r = net::resolve(scheduler, "localhost", static_cast<std::uint16_t>(1000 + i));
            if (r && !r.empty() && r[0].port() == 1000 + i && is_loopback(r[0])) {
                ok.fetch_add(1);
            } else {
                int expected = 0;
                first_error.compare_exchange_strong(expected, r.error != 0 ? r.error : -1);
            }
            if (net::resolve(scheduler, "127.0.0.1", 1)) {
                numeric_ok.fetch_add(1);
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(ok.load(), kFibers) << "first error " << first_error.load();
    EXPECT_EQ(numeric_ok.load(), kFibers);
}

}  // namespace
