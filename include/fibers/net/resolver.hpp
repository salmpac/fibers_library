#pragma once

// Name resolution for fibers: host name -> Endpoints (getaddrinfo on the
// scheduler's BlockingPool).
//
//   net::ResolveResult r = net::resolve(scheduler, "db.example.com", 5432);
//   if (!r) { ... r.error (errno value), r.gai_error (EAI_*), gai_strerror ... }
//   for (const net::Endpoint& ep : r) {           // getaddrinfo's order (RFC 6724)
//       if (stream.connect(scheduler, ep, deadline) == 0) break;
//   }
//
//   net::resolve(scheduler, "::1", 80);           // numeric: no pool, no syscall
//   net::resolve(scheduler, "example.com", 443, {.family = net::AddressFamily::kV4});
//
// Numeric hosts ("127.0.0.1", "::1") are parsed in place (inet_pton) and
// never reach the pool. Anything else is getaddrinfo(), run through
// this_fiber::blocking: the calling fiber parks, a BlockingPool thread makes
// the call (NSS: /etc/hosts, DNS, ...), the fiber resumes with the result.
// Outside a fiber of Sched the call runs inline and blocks the calling
// thread (this_fiber::blocking's rule).
//
// No deadline, no cancellation: getaddrinfo() cannot be interrupted, so a
// resolve runs to completion on the blocking pool (bounded only by the
// system resolver's own timeouts and retries, resolv.conf: timeout:5
// attempts:2 by default, per server). A caller that must not wait that long
// has no way out of the call yet; each slow lookup also holds one blocking
// thread, so many concurrent lookups of a dead DNS queue up behind
// BlockingPoolOptions::num_threads. An asynchronous resolver (c-ares on the
// poller) is the future option for deadlines and cancellation.
//
// Results: at most N endpoints (BasicResolveResult<N>, default 8) in an array
// inside the result, i.e. on the caller's stack; nothing is allocated by this
// code (getaddrinfo itself allocates its list, freed before returning).
// Only SOCK_STREAM addresses are asked for (one entry per address, not one
// per socket type), each with `port` filled in.
//
// Errors (ResolveResult::error; gai_error keeps the EAI_* code, 0 for
// errors detected here):
//   EINVAL        empty host, a host with a NUL byte, bad options
//   ENAMETOOLONG  host longer than NI_MAXHOST - 1
//   ENOENT        no such host / no address of the requested family
//                 (EAI_NONAME, EAI_NODATA, EAI_ADDRFAMILY; also a numeric
//                 address of the other family)
//   EAGAIN        temporary resolver failure (EAI_AGAIN: DNS timed out or
//                 SERVFAIL); retrying later may work
//   EIO           permanent resolver failure (EAI_FAIL) or unknown EAI code
//   ENOMEM        EAI_MEMORY
//   EAFNOSUPPORT  EAI_FAMILY
//   errno         EAI_SYSTEM (the errno of the failed call)

#include <netdb.h>
#include <sys/socket.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>

#include "fibers/fiber/blocking.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fibers::net {

enum class AddressFamily : std::uint8_t {
    kAny,  // IPv4 and IPv6 (AF_UNSPEC)
    kV4,   // AF_INET only
    kV6,   // AF_INET6 only
};

struct ResolveOptions {
    AddressFamily family = AddressFamily::kAny;
    // At most this many endpoints (also capped by the result's capacity N).
    std::size_t max_results = static_cast<std::size_t>(-1);
    // AI_ADDRCONFIG: only families the host has a non-loopback address of
    // (no AAAA queries on an IPv4-only host). Off by default: with it, a host
    // without a configured address of a family cannot resolve "localhost"
    // to that family's loopback either.
    bool addrconfig = false;
};

// Up to N endpoints, or an error. Iterable (const Endpoint&).
template <std::size_t N>
struct BasicResolveResult {
    static_assert(N > 0, "net: BasicResolveResult needs room for an endpoint");

    Endpoint endpoints[N];
    std::size_t count = 0;
    int error = 0;      // 0 or an errno value (see the top comment)
    int gai_error = 0;  // the EAI_* code behind `error` (0: none / not from getaddrinfo)

    explicit operator bool() const noexcept { return error == 0; }

    static constexpr std::size_t capacity() noexcept { return N; }
    std::size_t size() const noexcept { return count; }
    bool empty() const noexcept { return count == 0; }
    const Endpoint& operator[](std::size_t i) const noexcept { return endpoints[i]; }
    const Endpoint* begin() const noexcept { return endpoints; }
    const Endpoint* end() const noexcept { return endpoints + count; }
    std::span<const Endpoint> span() const noexcept { return {endpoints, count}; }
};

using ResolveResult = BasicResolveResult<8>;

namespace detail {

// EAI_* -> errno (see the top comment). `sys_errno`: errno right after the
// call, for EAI_SYSTEM.
inline int gai_to_errno(int gai, int sys_errno) noexcept {
    switch (gai) {
        case 0:
            return 0;
        case EAI_NONAME:
#ifdef EAI_NODATA
        case EAI_NODATA:
#endif
#ifdef EAI_ADDRFAMILY
        case EAI_ADDRFAMILY:
#endif
            return ENOENT;
        case EAI_AGAIN:
            return EAGAIN;
        case EAI_FAIL:
            return EIO;
        case EAI_MEMORY:
            return ENOMEM;
        case EAI_FAMILY:
            return EAFNOSUPPORT;
        case EAI_SERVICE:
        case EAI_SOCKTYPE:
        case EAI_BADFLAGS:
            return EINVAL;
        case EAI_OVERFLOW:
            return ENAMETOOLONG;
        case EAI_SYSTEM:
            return sys_errno != 0 ? sys_errno : EIO;
        default:
            return EIO;
    }
}

inline int to_af(AddressFamily family) noexcept {
    switch (family) {
        case AddressFamily::kV4:
            return AF_INET;
        case AddressFamily::kV6:
            return AF_INET6;
        default:
            return AF_UNSPEC;
    }
}

inline void set_port(Endpoint& e, std::uint16_t port) noexcept {
    if (e.family() == AF_INET) {
        reinterpret_cast<sockaddr_in*>(&e.storage)->sin_port = htons(port);
    } else if (e.family() == AF_INET6) {
        reinterpret_cast<sockaddr_in6*>(&e.storage)->sin6_port = htons(port);
    }
}

// What getaddrinfo() returned, on the fiber's stack (written by the pool
// thread, read after the wake).
struct GaiCall {
    const char* host;
    addrinfo hints;
    addrinfo* list = nullptr;
    int rc = 0;
    int sys_errno = 0;
};

}  // namespace detail

// Resolves `host` to at most N endpoints with `port`. From a fiber of
// Sched (getaddrinfo then runs on its BlockingPool), or inline anywhere
// else. Numeric addresses return at once. See the top comment.
template <fiber::BlockingScheduler Sched = sched::Scheduler, std::size_t N = 8>
BasicResolveResult<N> resolve(std::string_view host, std::uint16_t port, ResolveOptions options = {}) noexcept {
    BasicResolveResult<N> out;
    const std::size_t limit = options.max_results < N ? options.max_results : N;
    if (host.empty() || limit == 0 || host.find('\0') != std::string_view::npos) {
        out.error = EINVAL;
        return out;
    }
    char name[NI_MAXHOST];
    if (host.size() >= sizeof name) {
        out.error = ENAMETOOLONG;
        return out;
    }
    std::memcpy(name, host.data(), host.size());
    name[host.size()] = '\0';

    // Numeric fast path: no pool round trip, no NSS.
    const int want = detail::to_af(options.family);
    Endpoint numeric;
    if (Endpoint::parse(name, port, numeric)) {
        if (want != AF_UNSPEC && numeric.family() != want) {
            out.error = ENOENT;  // what getaddrinfo reports (EAI_ADDRFAMILY / EAI_NONAME)
            out.gai_error = EAI_NONAME;
            return out;
        }
        out.endpoints[0] = numeric;
        out.count = 1;
        return out;
    }

    detail::GaiCall call{name, addrinfo{}};
    call.hints.ai_family = want;
    call.hints.ai_socktype = SOCK_STREAM;
    call.hints.ai_protocol = IPPROTO_TCP;
    call.hints.ai_flags = options.addrconfig ? AI_ADDRCONFIG : 0;
    fiber::this_fiber::blocking<Sched>([&call]() noexcept {
        errno = 0;
        call.rc = ::getaddrinfo(call.host, nullptr, &call.hints, &call.list);
        call.sys_errno = errno;  // the pool thread's errno, for EAI_SYSTEM
    });
    if (call.rc != 0) {
        out.error = detail::gai_to_errno(call.rc, call.sys_errno);
        out.gai_error = call.rc;
        return out;
    }
    for (const addrinfo* ai = call.list; ai != nullptr && out.count < limit; ai = ai->ai_next) {
        if ((ai->ai_family != AF_INET && ai->ai_family != AF_INET6) || ai->ai_addr == nullptr ||
            ai->ai_addrlen > sizeof(sockaddr_storage)) {
            continue;
        }
        Endpoint& e = out.endpoints[out.count];
        e = Endpoint{};
        std::memcpy(&e.storage, ai->ai_addr, ai->ai_addrlen);
        e.size = static_cast<socklen_t>(ai->ai_addrlen);
        detail::set_port(e, port);
        ++out.count;
    }
    ::freeaddrinfo(call.list);
    if (out.count == 0) {
        out.error = ENOENT;  // only addresses of other families / socket types
        out.gai_error = EAI_NONAME;
    }
    return out;
}

// The same; `scheduler` names the scheduler type (the call must come from
// one of its fibers to run on its pool; see above).
template <std::size_t N = 8, fiber::BlockingScheduler Sched>
BasicResolveResult<N> resolve(Sched& scheduler, std::string_view host, std::uint16_t port,
                              ResolveOptions options = {}) noexcept {
    static_cast<void>(scheduler);
    return resolve<Sched, N>(host, port, options);
}

}  // namespace fibers::net
