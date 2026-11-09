#pragma once

// TCP (and any stream socket) for fibers: Listener, Stream, Endpoint.
//
//   net::Listener listener;
//   listener.listen(scheduler, net::Endpoint::loopback_v4(port));
//   fiber::go(scheduler, [&]() noexcept {
//       for (;;) {
//           net::Stream& s = ...;               // caller-owned, e.g. a pool
//           if (listener.accept(s) != 0) break;
//           fiber::go([&s]() noexcept {         // one fiber per connection
//               char buf[4096];
//               for (;;) {
//                   net::IoResult r = s.read_some(buf, sizeof buf);
//                   if (!r || r.bytes == 0) break;            // error / EOF
//                   if (!s.write_all(buf, r.bytes)) break;
//               }
//               s.close();
//           });
//       }
//   });
//
// Every socket is non-blocking and registered once (edge-triggered) with a
// poller of the scheduler (net::BasicPollDesc, poll_desc.hpp, embedded: no
// allocation). An operation first tries the syscall; only on EAGAIN does the
// calling fiber park until the poller reports readiness. So:
//   * operations that do not have to wait work from any thread;
//   * an operation that has to wait outside a fiber of Sched fails fast;
//   * at most one fiber waits per direction (one reader, one writer at a
//     time; a second parked reader fails fast);
//   * errors are returned (errno values), never thrown. ECANCELED: the
//     socket was closed while the operation waited. A connection that the
//     peer reset, or that keepalive / TCP_USER_TIMEOUT gave up on, fails
//     both directions with that error (ECONNRESET, ETIMEDOUT, ...): EPOLLERR
//     wakes the parked reader and writer, and the end of stream / EPIPE the
//     kernel then reports is mapped back to the error (see "Socket errors"
//     in poll_desc.hpp).
//
// Objects are neither copyable nor movable (the poller holds their address
// while they are open). A closed object can be opened again (accept into it,
// connect, adopt). close() unregisters the fd on its poller's thread, wakes
// a fiber parked in an operation on it (ECANCELED), waits for operations in
// progress to leave, then closes the fd (see poll_desc.hpp). The destructor
// closes an open object. Close every socket while the scheduler is running.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "fibers/net/poll_desc.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fibers::net {

// Result of a read / write: bytes transferred and 0 or an errno value.
// read_some: bytes == 0 with error == 0 is end of stream. The *_all / *_exact
// forms report how much was transferred before an error, too.
struct IoResult {
    std::size_t bytes = 0;
    int error = 0;

    explicit operator bool() const noexcept { return error == 0; }
};

// An IPv4 / IPv6 socket address (a sockaddr_storage by value).
struct Endpoint {
    sockaddr_storage storage{};
    socklen_t size = 0;

    static Endpoint ipv4(std::uint32_t host_order_address, std::uint16_t port) noexcept {
        Endpoint e;
        auto* in = reinterpret_cast<sockaddr_in*>(&e.storage);
        in->sin_family = AF_INET;
        in->sin_port = htons(port);
        in->sin_addr.s_addr = htonl(host_order_address);
        e.size = sizeof(sockaddr_in);
        return e;
    }

    static Endpoint loopback_v4(std::uint16_t port) noexcept { return ipv4(INADDR_LOOPBACK, port); }
    static Endpoint any_v4(std::uint16_t port) noexcept { return ipv4(INADDR_ANY, port); }

    static Endpoint loopback_v6(std::uint16_t port) noexcept {
        Endpoint e;
        auto* in6 = reinterpret_cast<sockaddr_in6*>(&e.storage);
        in6->sin6_family = AF_INET6;
        in6->sin6_port = htons(port);
        in6->sin6_addr = in6addr_loopback;
        e.size = sizeof(sockaddr_in6);
        return e;
    }

    // A numeric IPv4 or IPv6 address ("127.0.0.1", "::1"). false: not one.
    static bool parse(const char* address, std::uint16_t port, Endpoint& out) noexcept {
        out = Endpoint{};
        auto* in = reinterpret_cast<sockaddr_in*>(&out.storage);
        if (::inet_pton(AF_INET, address, &in->sin_addr) == 1) {
            in->sin_family = AF_INET;
            in->sin_port = htons(port);
            out.size = sizeof(sockaddr_in);
            return true;
        }
        out = Endpoint{};
        auto* in6 = reinterpret_cast<sockaddr_in6*>(&out.storage);
        if (::inet_pton(AF_INET6, address, &in6->sin6_addr) == 1) {
            in6->sin6_family = AF_INET6;
            in6->sin6_port = htons(port);
            out.size = sizeof(sockaddr_in6);
            return true;
        }
        return false;
    }

    int family() const noexcept { return storage.ss_family; }

    std::uint16_t port() const noexcept {
        if (storage.ss_family == AF_INET) {
            return ntohs(reinterpret_cast<const sockaddr_in*>(&storage)->sin_port);
        }
        if (storage.ss_family == AF_INET6) {
            return ntohs(reinterpret_cast<const sockaddr_in6*>(&storage)->sin6_port);
        }
        return 0;
    }

    const sockaddr* address() const noexcept { return reinterpret_cast<const sockaddr*>(&storage); }
};

namespace detail {

inline int set_nonblocking(int fd) noexcept {
    const int flags = ::fcntl(fd, F_GETFL);
    if (flags < 0) {
        return errno;
    }
    if ((flags & O_NONBLOCK) == 0 && ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
        return errno;
    }
    return 0;
}

// -errno on failure (PollDesc::io convention), else the result.
inline ssize_t sys(ssize_t r) noexcept { return r >= 0 ? r : -static_cast<ssize_t>(errno); }

template <class Desc>
int set_option(Desc& desc, int level, int name, const void* value, socklen_t size) noexcept {
    typename Desc::Guard guard(desc);
    if (!guard) {
        return EBADF;
    }
    return ::setsockopt(desc.fd(), level, name, value, size) == 0 ? 0 : errno;
}

template <class Desc>
int set_nodelay(Desc& desc, bool on) noexcept {
    const int value = on ? 1 : 0;
    return set_option(desc, IPPROTO_TCP, TCP_NODELAY, &value, sizeof value);
}

template <class Desc>
int set_keepalive(Desc& desc, std::chrono::seconds idle, std::chrono::seconds interval, int count) noexcept {
    const int on = idle.count() > 0 ? 1 : 0;
    if (const int e = set_option(desc, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof on); e != 0 || on == 0) {
        return e;
    }
    const int idle_s = static_cast<int>(idle.count());
    const int interval_s = static_cast<int>(interval.count());
    if (const int e = set_option(desc, IPPROTO_TCP, TCP_KEEPIDLE, &idle_s, sizeof idle_s); e != 0) {
        return e;
    }
    if (const int e = set_option(desc, IPPROTO_TCP, TCP_KEEPINTVL, &interval_s, sizeof interval_s); e != 0) {
        return e;
    }
    return set_option(desc, IPPROTO_TCP, TCP_KEEPCNT, &count, sizeof count);
}

template <class Desc>
int set_user_timeout(Desc& desc, std::chrono::milliseconds timeout) noexcept {
    const unsigned value = static_cast<unsigned>(timeout.count());
    return set_option(desc, IPPROTO_TCP, TCP_USER_TIMEOUT, &value, sizeof value);
}

}  // namespace detail

template <NetScheduler Sched = sched::Scheduler>
class BasicStream {
public:
    using PollDesc = BasicPollDesc<Sched>;

    BasicStream() noexcept = default;
    BasicStream(const BasicStream&) = delete;
    BasicStream& operator=(const BasicStream&) = delete;

    ~BasicStream() { static_cast<void>(close()); }

    // Takes ownership of a connected stream socket (e.g. from socketpair):
    // makes it non-blocking and registers it. 0 or errno (then the caller
    // still owns the fd). The stream must be closed.
    int adopt(Sched& scheduler, int fd) noexcept {
        if (const int error = detail::set_nonblocking(fd); error != 0) {
            return error;
        }
        return desc_.open(scheduler, fd);
    }

    // Connects to `peer` (non-blocking connect, then waits for write
    // readiness and reads SO_ERROR). From a fiber of `scheduler` (it waits).
    // 0 or errno (ECONNREFUSED, ETIMEDOUT, ...); on error the stream stays
    // closed.
    int connect(Sched& scheduler, const Endpoint& peer) noexcept {
        const int fd = ::socket(peer.family(), SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (fd < 0) {
            return errno;
        }
        if (::connect(fd, peer.address(), peer.size) != 0 && errno != EINPROGRESS) {
            const int error = errno;
            ::close(fd);
            return error;
        }
        // Registered after connect(): in SYN_SENT the socket reports nothing,
        // and EPOLL_CTL_ADD reports a connection that completed meanwhile.
        if (const int error = desc_.open(scheduler, fd); error != 0) {
            ::close(fd);
            return error;
        }
        int error = 0;
        {
            typename PollDesc::Guard guard(desc_);  // cannot fail: nobody else has it
            const ssize_t r = desc_.io(Direction::kWrite, [this, fd]() noexcept -> ssize_t {
                // A failed connect is reported with EPOLLERR, and the poller
                // takes SO_ERROR into the descriptor's sticky error then.
                if (const int e = desc_.error(); e != 0) {
                    return -static_cast<ssize_t>(e);
                }
                int so_error = 0;
                socklen_t len = sizeof so_error;
                if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &len) != 0) {
                    return -static_cast<ssize_t>(errno);
                }
                if (so_error != 0) {
                    return so_error == EINPROGRESS || so_error == EALREADY ? -EAGAIN : -so_error;
                }
                // SO_ERROR == 0 also while still connecting (a spurious or
                // early wakeup): connected only if there is a peer.
                sockaddr_storage addr;
                socklen_t addr_len = sizeof addr;
                if (::getpeername(fd, reinterpret_cast<sockaddr*>(&addr), &addr_len) == 0) {
                    return 0;
                }
                return errno == ENOTCONN ? -EAGAIN : -static_cast<ssize_t>(errno);
            });
            error = r < 0 ? static_cast<int>(-r) : 0;
        }
        if (error != 0) {
            static_cast<void>(close());
        }
        return error;
    }

    // One read: bytes > 0, or 0 at end of stream (error 0). Waits while
    // nothing is available.
    IoResult read_some(void* buffer, std::size_t size) noexcept {
        typename PollDesc::Guard guard(desc_);
        if (!guard) {
            return IoResult{0, EBADF};
        }
        const int fd = desc_.fd();
        const ssize_t r = desc_.io(Direction::kRead, [fd, buffer, size]() noexcept {
            return detail::sys(::read(fd, buffer, size));
        });
        if (r == 0 && size != 0) {
            // End of stream, or the connection was reset / timed out (the
            // poller took SO_ERROR on EPOLLERR): report the error then.
            return IoResult{0, desc_.error_after_shutdown()};
        }
        return to_result(r);
    }

    // Reads exactly `size` bytes. A short count with error 0 means the
    // stream ended first.
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

    // One write (MSG_NOSIGNAL: a closed peer gives EPIPE, not SIGPIPE).
    // Waits while the send buffer is full.
    IoResult write_some(const void* buffer, std::size_t size) noexcept {
        typename PollDesc::Guard guard(desc_);
        if (!guard) {
            return IoResult{0, EBADF};
        }
        const int fd = desc_.fd();
        const ssize_t r = desc_.io(Direction::kWrite, [fd, buffer, size]() noexcept {
            return detail::sys(::send(fd, buffer, size, MSG_NOSIGNAL));
        });
        return to_result(map_epipe(r));
    }

    // Writes all `size` bytes (several write-readiness rounds if needed).
    IoResult write_all(const void* buffer, std::size_t size) noexcept {
        typename PollDesc::Guard guard(desc_);
        if (!guard) {
            return IoResult{0, EBADF};
        }
        const int fd = desc_.fd();
        const auto* p = static_cast<const char*>(buffer);
        std::size_t done = 0;
        while (done < size) {
            const ssize_t r = map_epipe(desc_.io(Direction::kWrite, [fd, p, done, size]() noexcept {
                return detail::sys(::send(fd, p + done, size - done, MSG_NOSIGNAL));
            }));
            if (r < 0) {
                return IoResult{done, static_cast<int>(-r)};
            }
            done += static_cast<std::size_t>(r);
        }
        return IoResult{done, 0};
    }

    // ::shutdown (SHUT_RD / SHUT_WR / SHUT_RDWR). A fiber parked in
    // read_some wakes up (end of stream) through the poller. 0 or errno.
    int shutdown(int how = SHUT_WR) noexcept {
        typename PollDesc::Guard guard(desc_);
        if (!guard) {
            return EBADF;
        }
        return ::shutdown(desc_.fd(), how) == 0 ? 0 : errno;
    }

    // ---- TCP options (0 or errno) ------------------------------------------

    // TCP_NODELAY: send small segments at once (no Nagle).
    int set_nodelay(bool on = true) noexcept { return detail::set_nodelay(desc_, on); }

    // TCP keepalive: probe an idle connection after `idle`, every
    // `interval`, give up after `count` unanswered probes (then pending and
    // later operations fail with ETIMEDOUT). idle == 0 turns it off.
    int set_keepalive(std::chrono::seconds idle, std::chrono::seconds interval, int count) noexcept {
        return detail::set_keepalive(desc_, idle, interval, count);
    }

    // TCP_USER_TIMEOUT: fail the connection (ETIMEDOUT) when sent data
    // stays unacknowledged for this long; 0 = the system default.
    int set_user_timeout(std::chrono::milliseconds timeout) noexcept {
        return detail::set_user_timeout(desc_, timeout);
    }

    // setsockopt on the open socket.
    int set_option(int level, int name, const void* value, socklen_t size) noexcept {
        return detail::set_option(desc_, level, name, value, size);
    }

    // See the top comment and BasicPollDesc::close(). 0, or EBADF if not
    // open; the fd's own close error is returned otherwise.
    int close() noexcept {
        const int fd = desc_.fd();
        if (desc_.close() != 0) {
            return EBADF;
        }
        return ::close(fd) == 0 ? 0 : errno;
    }

    bool is_open_approx() const noexcept { return desc_.is_open_approx(); }

    // The fd while open (for getsockopt and the like; do not close it).
    int native_handle() const noexcept { return desc_.fd(); }

    PollDesc& poll_desc() noexcept { return desc_; }

private:
    template <NetScheduler>
    friend class BasicListener;

    // EPIPE after a reset / timeout: the error that ended the connection.
    ssize_t map_epipe(ssize_t r) noexcept {
        if (r == -EPIPE) {
            if (const int e = desc_.error_after_shutdown(); e != 0) {
                return -static_cast<ssize_t>(e);
            }
        }
        return r;
    }

    static IoResult to_result(ssize_t r) noexcept {
        return r < 0 ? IoResult{0, static_cast<int>(-r)} : IoResult{static_cast<std::size_t>(r), 0};
    }

    PollDesc desc_;
};

struct ListenOptions {
    int backlog = 1024;
    bool reuse_address = true;  // SO_REUSEADDR
    bool reuse_port = false;    // SO_REUSEPORT (several listeners on one port)
    bool v6_only = false;       // IPV6_V6ONLY for an IPv6 endpoint
};

template <NetScheduler Sched = sched::Scheduler>
class BasicListener {
public:
    using PollDesc = BasicPollDesc<Sched>;
    using Stream = BasicStream<Sched>;

    BasicListener() noexcept = default;
    BasicListener(const BasicListener&) = delete;
    BasicListener& operator=(const BasicListener&) = delete;

    ~BasicListener() { static_cast<void>(close()); }

    // socket + bind + listen + register; any thread. 0 or errno. Port 0
    // picks a free port (see local_endpoint()).
    int listen(Sched& scheduler, const Endpoint& endpoint, ListenOptions options = {}) noexcept {
        const int fd = ::socket(endpoint.family(), SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (fd < 0) {
            return errno;
        }
        const int one = 1;
        const int v6_only = options.v6_only ? 1 : 0;
        if ((options.reuse_address && ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one) != 0) ||
            (options.reuse_port && ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof one) != 0) ||
            (endpoint.family() == AF_INET6 &&
             ::setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &v6_only, sizeof v6_only) != 0) ||
            ::bind(fd, endpoint.address(), endpoint.size) != 0 || ::listen(fd, options.backlog) != 0) {
            const int error = errno;
            ::close(fd);
            return error;
        }
        if (const int error = desc_.open(scheduler, fd); error != 0) {
            ::close(fd);
            return error;
        }
        return 0;
    }

    // Takes ownership of a listening socket. 0 or errno.
    int adopt(Sched& scheduler, int fd) noexcept {
        if (const int error = detail::set_nonblocking(fd); error != 0) {
            return error;
        }
        return desc_.open(scheduler, fd);
    }

    // Accepts one connection into `out` (which must be closed), registered
    // with the next poller of the calling thread's round robin. Waits while
    // there is none. `peer` (optional) receives the peer's address.
    // 0 or errno (ECANCELED: the listener was closed meanwhile; EMFILE /
    // ENFILE: out of fds, the connection stays queued).
    int accept(Stream& out, Endpoint* peer = nullptr) noexcept {
        typename PollDesc::Guard guard(desc_);
        if (!guard) {
            return EBADF;
        }
        const int fd = desc_.fd();
        Endpoint scratch;
        Endpoint& addr = peer != nullptr ? *peer : scratch;
        const ssize_t r = desc_.io(Direction::kRead, [fd, &addr]() noexcept {
            addr.size = sizeof addr.storage;  // value-result: reset on every attempt
            return detail::sys(::accept4(fd, reinterpret_cast<sockaddr*>(&addr.storage), &addr.size,
                                         SOCK_NONBLOCK | SOCK_CLOEXEC));
        });
        if (r < 0) {
            return static_cast<int>(-r);
        }
        const int conn = static_cast<int>(r);
        if (const int error = out.desc_.open(desc_.scheduler(), conn); error != 0) {
            ::close(conn);
            return error;
        }
        return 0;
    }

    // ---- TCP options of accepted connections (0 or errno) ------------------
    //
    // Set on the listening socket; Linux copies them into every connection
    // accepted afterwards (sk_clone: SO_KEEPALIVE and the keepalive timing,
    // TCP_NODELAY, TCP_USER_TIMEOUT are all inherited; net_test checks it),
    // so accept() needs no extra syscalls. Same meaning as on Stream.
    int set_nodelay(bool on = true) noexcept { return detail::set_nodelay(desc_, on); }

    int set_keepalive(std::chrono::seconds idle, std::chrono::seconds interval, int count) noexcept {
        return detail::set_keepalive(desc_, idle, interval, count);
    }

    int set_user_timeout(std::chrono::milliseconds timeout) noexcept {
        return detail::set_user_timeout(desc_, timeout);
    }

    int set_option(int level, int name, const void* value, socklen_t size) noexcept {
        return detail::set_option(desc_, level, name, value, size);
    }

    // The bound address (e.g. the port chosen for port 0). Any thread, while
    // open.
    Endpoint local_endpoint() const noexcept {
        Endpoint e;
        e.size = sizeof e.storage;
        if (::getsockname(desc_.fd(), reinterpret_cast<sockaddr*>(&e.storage), &e.size) != 0) {
            return Endpoint{};
        }
        return e;
    }

    // Wakes a fiber parked in accept() (ECANCELED), then closes. 0 or EBADF.
    int close() noexcept {
        const int fd = desc_.fd();
        if (desc_.close() != 0) {
            return EBADF;
        }
        return ::close(fd) == 0 ? 0 : errno;
    }

    bool is_open_approx() const noexcept { return desc_.is_open_approx(); }
    int native_handle() const noexcept { return desc_.fd(); }

private:
    PollDesc desc_;
};

using Stream = BasicStream<sched::Scheduler>;
using Listener = BasicListener<sched::Scheduler>;

}  // namespace fibers::net
