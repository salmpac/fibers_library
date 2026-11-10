#pragma once

// Shared main() of the HTTP examples: scheduler placement from the command
// line (like echo_server), an http::Server on a handler, graceful shutdown
// on SIGINT / SIGTERM.
//
//   --port=8080 --host=0.0.0.0
//   --workers=1 --worker-cpus=0 --pollers=1 --poller-cpus=2
//   --blocking=1 --blocking-cpus=4 [--idle-spins=N]
//   --max-connections=20000 --stack-kb=64 --grace-ms=5000

#include <signal.h>

#include <chrono>
#include <cstdio>
#include <cstring>

#include "common.hpp"
#include "fibers/http/router.hpp"
#include "fibers/http/server.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/sched/scheduler.hpp"

namespace examples {

// `setup(scheduler, setup_context)`, if given, runs once the scheduler
// exists, before the server starts (e.g. to register metrics collectors on
// it; the scheduler is stopped and destroyed before this function returns).
using ServerSetup = void (*)(fibers::sched::Scheduler& scheduler, void* context) noexcept;

inline int run_http_server(const Args& args, fibers::http::Handler handler, const char* name,
                           ServerSetup setup = nullptr, void* setup_context = nullptr) {
    namespace http = fibers::http;
    namespace net = fibers::net;
    raise_fd_limit();
    const auto port = static_cast<std::uint16_t>(args.integer("port", 8080));
    const auto max_connections = static_cast<std::size_t>(args.integer("max-connections", 20000));

    fibers::sched::SchedulerOptions options;
    options.pool.num_workers = static_cast<std::size_t>(args.integer("workers", 1));
    options.pool.cpus = args.cpus("worker-cpus");
    if (const long long spins = args.integer("idle-spins", -1); spins >= 0) {
        options.pool.idle_spins_before_park = static_cast<std::size_t>(spins);
    }
    options.pollers.num_pollers = static_cast<std::size_t>(args.integer("pollers", 1));
    options.pollers.cpus = args.cpus("poller-cpus");
    options.blocking.num_threads = static_cast<std::size_t>(args.integer("blocking", 1));
    options.blocking.cpus = args.cpus("blocking-cpus");
    options.stacks.stack_size = static_cast<std::size_t>(args.integer("stack-kb", 64)) * 1024;
    options.stacks.max_stacks = max_connections + 16;

    http::ServerOptions server_options;
    server_options.max_connections = max_connections;

    // Block the signals before any thread starts: only main receives them.
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &signals, nullptr);

    fibers::sched::Scheduler scheduler(options);
    if (setup != nullptr) {
        setup(scheduler, setup_context);
    }
    int status = 0;
    {
        http::Server server(scheduler, handler, server_options);
        net::Endpoint endpoint;
        if (!net::Endpoint::parse(args.string("host", "0.0.0.0"), port, endpoint)) {
            std::fprintf(stderr, "bad --host\n");
            return 2;
        }
        net::ListenOptions listen_options;
        listen_options.backlog = 4096;
        if (const int error = server.listen(endpoint, listen_options); error != 0) {
            std::fprintf(stderr, "listen: %s\n", std::strerror(error));
            return 1;
        }
        server.start();
        std::printf("%s: http://%s:%u/ (%zu workers, %zu pollers, max %zu connections)\n", name,
                    args.string("host", "0.0.0.0"), server.local_endpoint().port(), options.pool.num_workers,
                    options.pollers.num_pollers, max_connections);
        std::fflush(stdout);

        int signal_number = 0;
        sigwait(&signals, &signal_number);
        const auto grace = std::chrono::milliseconds(args.integer("grace-ms", 5000));
        std::printf("%s: signal %d, %llu connections accepted, shutting down (grace %lld ms)\n", name,
                    signal_number, static_cast<unsigned long long>(server.accepted()),
                    static_cast<long long>(grace.count()));
        std::fflush(stdout);
        status = server.stop(grace) == 0 ? 0 : 3;
    }
    scheduler.stop();
    return status;
}

}  // namespace examples
