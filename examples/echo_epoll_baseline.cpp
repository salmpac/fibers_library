// Reference echo server without fibers: one thread per core, each with its
// own epoll instance and its own SO_REUSEPORT listener (the kernel spreads
// connections over the listeners), edge-triggered, read -> send in place.
//
//   echo_epoll_baseline --port=7000 --threads=1 --cpus=0 [--buffer=16384]
//
// A connection's state is a small struct; when send() cannot take
// everything, the rest is kept in a per-connection buffer and reading stops
// until EPOLLOUT drains it (the same back-pressure as the fiber server's
// write_all). Runs until SIGINT / SIGTERM (then exits at once).

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "common.hpp"
#include "fibers/sync/affinity.hpp"
#include "fibers/sync/sync.hpp"

namespace {

struct Conn {
    int fd = -1;
    std::unique_ptr<char[]> pending;  // allocated on the first short send
    std::size_t pending_off = 0;
    std::size_t pending_len = 0;
};

int make_listener(std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        return -1;
    }
    const int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof one);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr) != 0 || ::listen(fd, 4096) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

void close_conn(Conn* c) {
    ::close(c->fd);  // also removes it from the epoll set
    delete c;
}

// false: the connection was closed.
bool flush(Conn* c) {
    while (c->pending_len != 0) {
        const ssize_t w = ::send(c->fd, c->pending.get() + c->pending_off, c->pending_len, MSG_NOSIGNAL);
        if (w < 0) {
            if (errno == EAGAIN) {
                return true;
            }
            close_conn(c);
            return false;
        }
        c->pending_off += static_cast<std::size_t>(w);
        c->pending_len -= static_cast<std::size_t>(w);
    }
    return true;
}

void on_readable(Conn* c, char* buf, std::size_t size) {
    if (!flush(c)) {
        return;
    }
    if (c->pending_len != 0) {
        return;  // wait for EPOLLOUT
    }
    for (;;) {
        const ssize_t n = ::read(c->fd, buf, size);
        if (n == 0) {
            close_conn(c);
            return;
        }
        if (n < 0) {
            if (errno == EAGAIN) {
                return;
            }
            close_conn(c);
            return;
        }
        std::size_t off = 0;
        while (off < static_cast<std::size_t>(n)) {
            const ssize_t w = ::send(c->fd, buf + off, static_cast<std::size_t>(n) - off, MSG_NOSIGNAL);
            if (w < 0) {
                if (errno != EAGAIN) {
                    close_conn(c);
                    return;
                }
                if (!c->pending) {
                    c->pending = std::make_unique<char[]>(size);
                }
                c->pending_off = 0;
                c->pending_len = static_cast<std::size_t>(n) - off;
                std::memcpy(c->pending.get(), buf + off, c->pending_len);
                return;  // EPOLLOUT resumes
            }
            off += static_cast<std::size_t>(w);
        }
    }
}

void run_thread(std::size_t index, int cpu, std::uint16_t port, std::size_t buffer) {
    fibers::sync::pin_current_thread<fibers::sync::StdSync>(cpu, "echo_epoll_baseline");
    const int listener = make_listener(port);
    const int epoll = ::epoll_create1(EPOLL_CLOEXEC);
    if (listener < 0 || epoll < 0) {
        std::fprintf(stderr, "thread %zu: listener / epoll: %s\n", index, std::strerror(errno));
        std::_Exit(1);
    }
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.ptr = nullptr;  // the listener
    ::epoll_ctl(epoll, EPOLL_CTL_ADD, listener, &ev);
    std::vector<char> buf(buffer);
    epoll_event events[256];
    for (;;) {
        const int n = ::epoll_wait(epoll, events, 256, -1);
        for (int i = 0; i < n; ++i) {
            if (events[i].data.ptr == nullptr) {
                for (;;) {
                    const int fd = ::accept4(listener, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
                    if (fd < 0) {
                        break;  // EAGAIN (or a transient error: the next edge retries)
                    }
                    const int one = 1;
                    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
                    auto* c = new Conn;
                    c->fd = fd;
                    epoll_event cev{};
                    cev.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;
                    cev.data.ptr = c;
                    if (::epoll_ctl(epoll, EPOLL_CTL_ADD, fd, &cev) != 0) {
                        close_conn(c);
                    }
                }
                continue;
            }
            on_readable(static_cast<Conn*>(events[i].data.ptr), buf.data(), buf.size());
        }
    }
}

}  // namespace

int main(int argc, char** argv) {
    examples::Args args(argc, argv);
    examples::raise_fd_limit();
    const auto port = static_cast<std::uint16_t>(args.integer("port", 7000));
    const std::size_t threads = static_cast<std::size_t>(args.integer("threads", 1));
    const fibers::sync::CpuList cpus = args.cpus("cpus");
    const std::size_t buffer = static_cast<std::size_t>(args.integer("buffer", 16384));

    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &signals, nullptr);

    std::vector<std::thread> pool;
    for (std::size_t i = 0; i < threads; ++i) {
        pool.emplace_back(run_thread, i, cpus.for_thread(i), port, buffer);
    }
    std::printf("echo_epoll_baseline: port %u, %zu threads\n", port, threads);
    std::fflush(stdout);
    int signal_number = 0;
    sigwait(&signals, &signal_number);
    std::_Exit(0);
}
