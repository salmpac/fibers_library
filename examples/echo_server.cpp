// Echo server on fibers: a listener, an accept loop in a fiber, one fiber
// per connection (read_some -> write_all until end of stream).
//
//   echo_server --port=7000 --workers=1 --pollers=1 --blocking=1
//               --worker-cpus=0 --poller-cpus=2 --blocking-cpus=4
//               [--max-connections=20000] [--buffer=16384] [--host=0.0.0.0]
//               [--stack-kb=64] [--idle-spins=N]
//
// Runs until SIGINT / SIGTERM, then closes the listener and every open
// connection (their fibers wake up with ECANCELED), waits for the fibers and
// stops the scheduler. Connections live in a fixed array (no allocation per
// connection): a free list under a spin lock hands out slots, leased as
// object_pool::Pooled handles that return them when dropped.

#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <memory>
#include <mutex>
#include <thread>

#include "common.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/object_pool/pooled.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/sync/spin_lock.hpp"

namespace fiber = fibers::fiber;
namespace net = fibers::net;
using fibers::sched::Scheduler;

namespace {

// A pool of connection slots (object_pool::LeasablePool).
struct Server {
    using value_type = net::Stream;

    explicit Server(std::size_t max_connections)
        : capacity(max_connections),
          streams(std::make_unique<net::Stream[]>(max_connections)),
          free_slots(std::make_unique<std::size_t[]>(max_connections)),
          free_count(max_connections) {
        for (std::size_t i = 0; i < capacity; ++i) {
            free_slots[i] = capacity - 1 - i;
        }
    }

    // nullptr when all slots are taken.
    net::Stream* acquire() noexcept {
        std::lock_guard guard(lock);
        if (free_count == 0) {
            return nullptr;
        }
        return &streams[free_slots[--free_count]];
    }

    void release(net::Stream* s) noexcept {
        std::lock_guard guard(lock);
        free_slots[free_count++] = static_cast<std::size_t>(s - streams.get());
    }

    const std::size_t capacity;
    std::unique_ptr<net::Stream[]> streams;
    std::unique_ptr<std::size_t[]> free_slots;
    std::size_t free_count;
    fibers::sync::SpinLock<> lock;
    std::atomic<std::size_t> active{0};
    std::atomic<std::uint64_t> accepted{0};
    std::atomic<bool> stopping{false};
    std::size_t buffer = 16384;
};

void serve(Server& server, net::Stream& s) {
    static_cast<void>(s.set_nodelay());
    // The buffer lives on the fiber's stack (64 KiB by default).
    char buf[16384];
    const std::size_t size = std::min(server.buffer, sizeof buf);
    for (;;) {
        const net::IoResult r = s.read_some(buf, size);
        if (!r || r.bytes == 0) {
            break;
        }
        if (!s.write_all(buf, r.bytes)) {
            break;
        }
    }
    static_cast<void>(s.close());
}

}  // namespace

int main(int argc, char** argv) {
    examples::Args args(argc, argv);
    examples::raise_fd_limit();
    const auto port = static_cast<std::uint16_t>(args.integer("port", 7000));
    const std::size_t max_connections = static_cast<std::size_t>(args.integer("max-connections", 20000));

    fibers::sched::SchedulerOptions options;
    options.pool.num_workers = static_cast<std::size_t>(args.integer("workers", 1));
    options.pool.cpus = args.cpus("worker-cpus");
    if (const long long spins = args.integer("idle-spins", -1); spins >= 0) {
        options.pool.idle_spins_before_park = static_cast<std::size_t>(spins);  // see ThreadPoolOptions
    }
    options.pollers.num_pollers = static_cast<std::size_t>(args.integer("pollers", 1));
    options.pollers.cpus = args.cpus("poller-cpus");
    options.blocking.num_threads = static_cast<std::size_t>(args.integer("blocking", 1));
    options.blocking.cpus = args.cpus("blocking-cpus");
    options.stacks.stack_size = static_cast<std::size_t>(args.integer("stack-kb", 64)) * 1024;
    options.stacks.max_stacks = max_connections + 16;

    // Block the signals before any thread starts: only main receives them.
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &signals, nullptr);

    auto server = std::make_unique<Server>(max_connections);
    server->buffer = static_cast<std::size_t>(args.integer("buffer", 16384));
    Scheduler scheduler(options);
    net::Listener listener;
    net::Endpoint endpoint;
    if (!net::Endpoint::parse(args.string("host", "0.0.0.0"), port, endpoint)) {
        std::fprintf(stderr, "bad --host\n");
        return 2;
    }
    net::ListenOptions listen_options;
    listen_options.backlog = 4096;
    if (const int error = listener.listen(scheduler, endpoint, listen_options); error != 0) {
        std::fprintf(stderr, "listen: %s\n", std::strerror(error));
        return 1;
    }
    std::printf("echo_server: port %u, %zu workers, %zu pollers, %zu blocking, max %zu connections\n",
                listener.local_endpoint().port(), options.pool.num_workers, options.pollers.num_pollers,
                options.blocking.num_threads, max_connections);
    std::fflush(stdout);

    std::atomic<bool> accept_done{false};
    fiber::go(scheduler, [&]() noexcept {
        Server& srv = *server;
        for (;;) {
            auto s = fibers::object_pool::lease(srv);
            if (!s) {
                std::fprintf(stderr, "echo_server: out of connection slots\n");
                fiber::this_fiber::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            const int error = listener.accept(*s);
            if (error != 0) {
                if (error == EMFILE || error == ENFILE || error == ENOBUFS || error == ENOMEM) {
                    fiber::this_fiber::sleep_for(std::chrono::milliseconds(10));
                    continue;
                }
                if (error == ECONNABORTED || error == EINTR) {
                    continue;
                }
                break;  // ECANCELED: the listener was closed
            }
            srv.accepted.fetch_add(1, std::memory_order_relaxed);
            srv.active.fetch_add(1, std::memory_order_relaxed);
            fiber::go<Scheduler>([&srv, s = std::move(s)]() mutable noexcept {
                serve(srv, *s);
                s.reset();  // the slot is free before `active` drops
                srv.active.fetch_sub(1, std::memory_order_release);
            });
        }
        accept_done.store(true, std::memory_order_release);
    });

    int signal_number = 0;
    sigwait(&signals, &signal_number);
    std::printf("echo_server: signal %d, %llu connections accepted, shutting down\n", signal_number,
                static_cast<unsigned long long>(server->accepted.load()));
    server->stopping.store(true);
    static_cast<void>(listener.close());
    while (!accept_done.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    // Close what is still open: parked readers wake with ECANCELED, close
    // themselves (EBADF, ignored) and return their slots.
    for (std::size_t i = 0; i < server->capacity; ++i) {
        static_cast<void>(server->streams[i].close());
    }
    for (int i = 0; i < 5000 && server->active.load(std::memory_order_acquire) != 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    scheduler.stop();
    return 0;
}
