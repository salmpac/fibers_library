// fibers::Context: a deadline (or a cancel) for a whole tree of fibers.
//
//   context_deadline [--workers=4] [--fast=4] [--slow=3] [--deadline-ms=300]
//
// What it is for: a request comes in with a budget (say 300 ms). Its
// handler fans out to backends over the network; some answer quickly, some
// hang. When the budget is spent, every network wait in the handler's tree
// of fibers returns ETIMEDOUT, so the handler gathers what it has, returns
// normally (RAII closes the sockets) and answers -- instead of waiting for
// the slowest backend. A client that goes away is the same thing with
// ctx.cancel() and ECANCELED.
//
//   Context::with_timeout(sched, d)  a root that ends at now + d (kTimedOut)
//   Context::cancellable()           a root that ends by cancel() (kCancelled)
//   Context::child_of(parent, t)     ends with the parent, or at its own t
//   fiber::go(sched, ctx, f)         f runs under ctx
//   fiber::go(f), wg.go(f)           (in a fiber) the child inherits the context
//   fiber::ContextScope s(ctx)       this fiber runs under ctx for a scope
//   this_fiber::cancelled()          the cooperative check (one load)
//
// Key rules (docs/cancellation.md):
//   * Cooperative: nothing is killed. Cancellable waits are the network
//     ones -- socket read / write / accept / connect (also TLS, HTTP/1.1 and
//     HTTP/2 client calls) and ConnectionPool::acquire. They return
//     ECANCELED / ETIMEDOUT. An operation that does not have to wait (data
//     already there) still succeeds.
//   * Not cancellable: Mutex, Event, WaitGroup, Oneshot / Future,
//     Semaphore, sleep_for, blocking / file ops, resolve. CPU loops check
//     this_fiber::cancelled(); waits on those primitives end when the
//     network-bound fibers they wait for end.
//   * wg.wait() is a join and deliberately NOT interrupted: the children
//     reference the handler's frame; under the same context they end
//     promptly anyway.
//   * The context outlives every fiber under it: create it on the
//     handler's stack, join the children, then return. Destroying it while
//     a fiber is parked under it fails fast.
//   * Effective deadline of a socket wait = min(socket deadline, context
//     deadline).
//
// The program: a toy backend server (fibers, no context) answers "f" lines
// at once and never answers "s" lines (it waits until the client hangs
// up). Part 1: a handler with a deadline calls --fast fast and --slow slow
// backends in parallel plus a CPU-bound fiber; the fast calls succeed, the
// slow ones and the CPU loop end at the deadline, and the handler returns
// soon after it. Part 2: the same handler under Context::cancellable();
// main cancels it once the slow calls are waiting ("the client went
// away"): ECANCELED. Part 3: a per-step budget inside a request with a
// child context and ContextScope.

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>

#include "../common.hpp"
#include "fibers/fiber/context.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/wait_group.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace net = fibers::net;
namespace this_fiber = fibers::fiber::this_fiber;
using fibers::Context;
using fibers::sched::Scheduler;
using namespace std::chrono_literals;

namespace {

double ms_since(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
}

// ---- the backend --------------------------------------------------------------

// One connection: "f\n" -> "ok\n"; "s\n" -> no answer, wait for the
// client to hang up (it does when its context ends).
void serve(net::Stream& s) {
    char buf[64];
    const net::IoResult r = s.read_some(buf, sizeof buf);
    if (r && r.bytes > 0 && buf[0] == 'f') {
        static_cast<void>(s.write_all("ok\n", 3));
    } else {
        while (s.read_some(buf, sizeof buf).bytes > 0) {
        }
    }
    static_cast<void>(s.close());
}

struct Backend {
    Scheduler& scheduler;
    net::Listener listener;
    fiber::WaitGroup fibers;  // the accept loop + one per connection

    explicit Backend(Scheduler& s) : scheduler(s) {}

    bool start() {
        if (listener.listen(scheduler, net::Endpoint::loopback_v4(0)) != 0) {
            return false;
        }
        // An explicit scheduler: these fibers run under no context.
        fibers.go(scheduler, [this]() noexcept {
            for (;;) {
                auto* conn = new net::Stream;
                if (listener.accept(*conn) != 0) {
                    delete conn;
                    return;  // closed
                }
                fibers.go(scheduler, [conn]() noexcept {
                    serve(*conn);
                    delete conn;
                });
            }
        });
        return true;
    }

    void stop() {
        static_cast<void>(listener.close());  // the accept loop ends with ECANCELED
        fibers.wait();
    }
};

// ---- the handler -----------------------------------------------------------------

// One backend call in the caller's fiber, under its context: connect,
// send the request line, read the answer. 0 or errno.
int call(Scheduler& scheduler, const net::Endpoint& backend, char kind) {
    net::Stream s;
    if (const int e = s.connect(scheduler, backend); e != 0) {
        return e;
    }
    const char line[2] = {kind, '\n'};
    int error = 0;
    if (const net::IoResult w = s.write_all(line, 2); !w) {
        error = w.error;
    } else {
        char buf[16];
        const net::IoResult r = s.read_some(buf, sizeof buf);  // ETIMEDOUT / ECANCELED when ctx ends
        error = r ? (r.bytes > 0 ? 0 : ECONNRESET) : r.error;
    }
    static_cast<void>(s.close());
    return error;
}

struct Outcome {
    std::atomic<int> ok{0};
    std::atomic<int> timed_out{0};
    std::atomic<int> cancelled{0};
    std::atomic<int> other{0};
    std::atomic<int> waiting{0};  // calls that sent their request
    std::atomic<bool> cpu_stopped{false};

    void count(int e) {
        (e == 0 ? ok : e == ETIMEDOUT ? timed_out : e == ECANCELED ? cancelled : other).fetch_add(1);
    }
};

// Runs under the request's context (set by whoever spawned it): fans out,
// joins, returns. Nothing here mentions the context except the check in
// the CPU loop.
void handle(Scheduler& scheduler, const net::Endpoint& backend, int fast, int slow, Outcome& out) {
    fiber::WaitGroup wg;
    for (int i = 0; i < fast + slow; ++i) {
        const char kind = i < fast ? 'f' : 's';
        wg.go([&, kind]() noexcept {  // inherits the context
            if (kind == 's') {
                out.waiting.fetch_add(1);
            }
            out.count(call(scheduler, backend, kind));
        });
    }
    wg.go([&]() noexcept {  // CPU-bound work: checks cooperatively
        unsigned long x = 0;
        while (!this_fiber::cancelled()) {
            for (int i = 0; i < 10'000; ++i) {
                x = x * 6364136223846793005ul + 1442695040888963407ul;
            }
            this_fiber::yield();
        }
        out.cpu_stopped.store(x != 1);  // (uses x)
    });
    wg.wait();  // a join: not interrupted by the context
}

}  // namespace

int main(int argc, char** argv) {
    const examples::Args args(argc, argv);
    const int workers = static_cast<int>(args.integer("workers", 4));
    const int fast = static_cast<int>(args.integer("fast", 4));
    const int slow = static_cast<int>(args.integer("slow", 3));
    const auto deadline = std::chrono::milliseconds(args.integer("deadline-ms", 300));

    fibers::sched::SchedulerOptions options;
    options.pool.num_workers = static_cast<std::size_t>(workers);
    Scheduler scheduler(options);
    Backend backend(scheduler);
    if (!backend.start()) {
        std::fprintf(stderr, "listen failed\n");
        return 1;
    }
    const net::Endpoint endpoint = backend.listener.local_endpoint();
    bool good = true;

    // ---- part 1: a deadline ----------------------------------------------------------
    {
        Outcome out;
        std::atomic<bool> done{false};
        const auto start = std::chrono::steady_clock::now();
        // Created by main (a plain thread) with the scheduler; it lives on
        // main's stack until the handler has returned.
        Context ctx = Context::with_timeout(scheduler, deadline);
        fiber::go(scheduler, ctx, [&]() noexcept {
            handle(scheduler, endpoint, fast, slow, out);
            done.store(true);
        });
        while (!done.load()) {
            std::this_thread::sleep_for(1ms);
        }
        const double took = ms_since(start);
        std::printf("part 1 (deadline %lld ms): %d ok, %d timed out, %d other, cpu loop stopped: %s; "
                    "handler returned after %.0f ms; reason: %s\n",
                    static_cast<long long>(deadline.count()), out.ok.load(), out.timed_out.load(),
                    out.other.load(), out.cpu_stopped.load() ? "yes" : "no", took,
                    ctx.reason() ? fibers::wait::to_string(*ctx.reason()) : "live");
        good = good && out.ok.load() == fast && out.timed_out.load() == slow && out.other.load() == 0 &&
               out.cpu_stopped.load() && ctx.reason() == fibers::wait::Error::kTimedOut;
    }

    // ---- part 2: the client went away (cancel) ------------------------------------------
    {
        Outcome out;
        std::atomic<bool> done{false};
        Context ctx = Context::cancellable();
        fiber::go(scheduler, ctx, [&]() noexcept {
            handle(scheduler, endpoint, fast, slow, out);
            done.store(true);
        });
        // Cancel once every slow call has sent its request and the fast
        // ones are done (any moment would do; this makes the counts exact).
        while (out.waiting.load() < slow || out.ok.load() < fast) {
            std::this_thread::sleep_for(1ms);
        }
        std::this_thread::sleep_for(5ms);
        ctx.cancel();
        while (!done.load()) {
            std::this_thread::sleep_for(1ms);
        }
        std::printf("part 2 (cancel): %d ok, %d cancelled, %d other, cpu loop stopped: %s\n", out.ok.load(),
                    out.cancelled.load(), out.other.load(), out.cpu_stopped.load() ? "yes" : "no");
        // A slow call that had not parked in its read yet may also see the
        // cancel at its connect / write: ECANCELED either way.
        good = good && out.ok.load() == fast && out.cancelled.load() == slow && out.other.load() == 0 &&
               out.cpu_stopped.load();
    }

    // ---- part 3: a per-step budget inside a request ----------------------------------------
    {
        std::atomic<int> step{-1};
        std::atomic<int> next{-1};
        std::atomic<bool> done{false};
        Context request = Context::with_timeout(scheduler, 10s);
        fiber::go(scheduler, request, [&]() noexcept {
            {
                // At most 50 ms for this lookup, within the request's budget.
                Context lookup = Context::child_of(*this_fiber::context(), scheduler.now() + 50ms);
                fiber::ContextScope scope(lookup);
                step.store(call(scheduler, endpoint, 's'));  // ETIMEDOUT after 50 ms
            }
            next.store(call(scheduler, endpoint, 'f'));  // the request itself is still live
            done.store(true);
        });
        while (!done.load()) {
            std::this_thread::sleep_for(1ms);
        }
        std::printf("part 3 (child context): step %s, next call %s, request live: %s\n",
                    step.load() == ETIMEDOUT ? "timed out" : std::strerror(step.load()),
                    next.load() == 0 ? "ok" : std::strerror(next.load()), request.cancelled() ? "no" : "yes");
        good = good && step.load() == ETIMEDOUT && next.load() == 0 && !request.cancelled();
    }  // ~request: takes its pending timer back from the poller

    backend.stop();
    scheduler.stop();
    std::printf("%s\n", good ? "OK" : "FAILED");
    return good ? 0 : 1;
}
