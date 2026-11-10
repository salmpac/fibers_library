#pragma once

// http::Server: an HTTP/1.1 server on fibers over plain TCP (net::Stream).
//
//   http::Router router;
//   router.get("/hello", [](http::Request&, http::Response& res) noexcept { res.send("hello\n"); });
//
//   http::Server server(scheduler, router.handler());     // + ServerOptions
//   server.listen(net::Endpoint::any_v4(8080));
//   server.start();                       // the accept loop, in a fiber
//   ...
//   server.stop(std::chrono::seconds(5)); // graceful, from a thread or a fiber
//
// One fiber per connection (keep-alive, pipelining; see connection.hpp for
// parsing, limits and timeouts). Connection objects (the stream, the parser,
// the read / write buffers) come from a sharded object pool: allocated when
// the pool grows, then reused, so serving a request allocates nothing in the
// server's own code. Large request bodies use a pooled body buffer.
//
// Graceful shutdown, stop(grace):
//   1. the server publishes the shutdown deadline (now + grace); idle
//      keep-alive connections are closed at once, connections in the middle
//      of a request finish it and answer with "Connection: close";
//   2. the listener is closed, the accept loop ends;
//   3. it waits for every connection fiber (a fiber::WaitGroup). I/O still
//      going on at the deadline fails (every socket deadline is clamped to
//      it), so the wait ends soon after the deadline unless a handler blocks
//      on something other than its connection.
// stop() returns 0 if everything ended before the deadline, ETIMEDOUT if
// connections had to be cut. It may be called from a fiber of the scheduler
// (it parks) -- but not from a handler of this server, which would wait for
// itself -- or from a thread outside the scheduler (it polls every
// millisecond). Idempotent; later calls wait for the first one.
//
// The server is single-use (start once, stop once). Stop it before the
// scheduler stops. The scheduler's stack pool must have room for
// max_connections fibers (StackPoolOptions::max_stacks) plus the server's
// own two (accept loop, shutdown).

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>

#include "fibers/fiber/event.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/wait_group.hpp"
#include "fibers/http/connection.hpp"
#include "fibers/http/options.hpp"
#include "fibers/http/router.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/object_pool/pooled.hpp"
#include "fibers/object_pool/sharded_pool.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fibers::http {

template <net::NetScheduler Sched = sched::Scheduler>
class BasicServer {
public:
    using Stream = net::BasicStream<Sched>;
    using Listener = net::BasicListener<Sched>;

    BasicServer(Sched& scheduler, Handler handler, ServerOptions options = {}) noexcept
        : scheduler_(scheduler), core_(handler, options), pool_(pool_options()) {}

    BasicServer(const BasicServer&) = delete;
    BasicServer& operator=(const BasicServer&) = delete;

    // Stops (grace 0) if still running: the scheduler must still run then.
    ~BasicServer() {
        if (started_.load(std::memory_order_acquire)) {
            static_cast<void>(stop(poller::Duration::zero()));
        }
    }

    // socket + bind + listen (port 0: a free port, see local_endpoint()).
    // 0 or errno.
    int listen(const net::Endpoint& endpoint, net::ListenOptions options = {}) noexcept {
        if (const int e = listener_.listen(scheduler_, endpoint, options); e != 0) {
            return e;
        }
        if (core_.options().tcp_nodelay) {
            static_cast<void>(listener_.set_nodelay(true));  // inherited by accepted sockets
        }
        return 0;
    }

    net::Endpoint local_endpoint() const noexcept { return listener_.local_endpoint(); }

    // Starts the accept loop (a fiber). 0, or EINVAL if not listening or
    // already started / stopped.
    int start() noexcept {
        if (!listener_.is_open_approx() || stop_called_.load(std::memory_order_acquire) ||
            started_.exchange(true, std::memory_order_acq_rel)) {
            return EINVAL;
        }
        fiber::go(scheduler_, [this]() noexcept { accept_loop(); });
        return 0;
    }

    // Graceful shutdown (see the top comment). 0 or ETIMEDOUT.
    int stop(poller::Duration grace = std::chrono::seconds(10)) noexcept {
        if (stop_called_.exchange(true, std::memory_order_acq_rel)) {
            wait_stopped();
            return stop_result_;
        }
        if (!started_.load(std::memory_order_acquire)) {
            static_cast<void>(listener_.close());
            stopped_.set();
            done_.store(true, std::memory_order_release);
            return 0;
        }
        if (fiber::BasicFiber<Sched>::current() != nullptr) {
            shutdown(grace);
        } else {
            fiber::go(scheduler_, [this, grace]() noexcept { shutdown(grace); });
        }
        wait_stopped();
        return stop_result_;
    }

    // Live connections (racy snapshot).
    std::size_t connections() const noexcept { return active_.load(std::memory_order_relaxed); }
    // Connections accepted so far.
    std::uint64_t accepted() const noexcept { return accepted_.load(std::memory_order_relaxed); }

    ServerCore& core() noexcept { return core_; }

private:
    struct Connection {
        Stream stream;
        detail::ConnectionState state;
    };

    static object_pool::PoolOptions pool_options() noexcept {
        object_pool::PoolOptions o;
        o.shards = 8;
        o.initial_objects = 0;
        o.grow_chunk = 64;
        o.thread_cache_capacity = 8;
        return o;
    }

    void accept_loop() noexcept {
        const std::size_t limit = core_.options().max_connections;
        for (;;) {
            if (core_.draining()) {
                break;
            }
            if (active_.load(std::memory_order_relaxed) >= limit) {
                fiber::this_fiber::sleep_for<Sched>(std::chrono::milliseconds(1));
                continue;
            }
            auto c = pool_.lease();
            const int error = listener_.accept(c->stream);
            if (error != 0) {
                if (error == EMFILE || error == ENFILE || error == ENOBUFS || error == ENOMEM) {
                    fiber::this_fiber::sleep_for<Sched>(std::chrono::milliseconds(10));
                    continue;
                }
                if (error == ECONNABORTED || error == EINTR || error == EPROTO || error == EPERM) {
                    continue;
                }
                break;  // ECANCELED / EBADF: the listener was closed
            }
            active_.fetch_add(1, std::memory_order_relaxed);
            accepted_.fetch_add(1, std::memory_order_relaxed);
            wg_.go(scheduler_, [this, c = std::move(c)]() mutable noexcept {
                serve_connection(c->stream, c->state, core_);
                c.reset();  // back in the pool before the connection stops counting
                active_.fetch_sub(1, std::memory_order_relaxed);
            });
        }
        accept_done_.set();
    }

    void shutdown(poller::Duration grace) noexcept {
        const poller::TimePoint deadline = scheduler_.now() + grace;
        core_.begin_shutdown(deadline);
        static_cast<void>(listener_.close());
        accept_done_.wait();
        // Connections spawned meanwhile check the draining flag themselves;
        // this pass catches those that went idle in between, cheaply.
        core_.begin_shutdown(deadline);
        wg_.wait();
        stop_result_ = scheduler_.now() > deadline ? ETIMEDOUT : 0;
        stopped_.set();
        // Last touch of the server by this fiber (see wait_stopped).
        done_.store(true, std::memory_order_release);
    }

    void wait_stopped() noexcept {
        if (fiber::BasicFiber<Sched>::current() != nullptr) {
            stopped_.wait();
            while (!done_.load(std::memory_order_acquire)) {
                fiber::this_fiber::yield<Sched>();  // a few instructions away
            }
            return;
        }
        while (!done_.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    Sched& scheduler_;
    ServerCore core_;
    Listener listener_;
    object_pool::DefaultPool<Connection> pool_;
    fiber::BasicWaitGroup<Sched> wg_;
    fiber::BasicEvent<Sched> accept_done_;
    fiber::BasicEvent<Sched> stopped_;
    std::atomic<bool> started_{false};
    std::atomic<bool> stop_called_{false};
    std::atomic<bool> done_{false};
    std::atomic<std::size_t> active_{0};
    std::atomic<std::uint64_t> accepted_{0};
    int stop_result_ = 0;
};

using Server = BasicServer<sched::Scheduler>;

}  // namespace fibers::http
