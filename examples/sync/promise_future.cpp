// One-shot request / response between fibers: fiber::Oneshot<T> and
// fiber::make_promise_future<T>().
//
//   promise_future [--workers=2]
//
// What they are for: one party asks, another (a fiber, a plain thread, a
// blocking-pool call, a poller callback) answers exactly once. The consumer
// waits with get(); the producer completes with promise.set(value), or
// breaks the promise by dropping it unset. The result is a
// fiber::Result<T>: a value, or Error::kBroken. No exceptions.
//
// Two flavours, same Promise type on the producer side:
//
//   fiber::Oneshot<T>               the state lives in the consumer's frame:
//                                   zero allocation. Rule: the Oneshot must
//                                   outlive the producer's set() / drop, so
//                                   the consumer must observe the completion
//                                   (get(), or try_get() / ready() seeing it)
//                                   before leaving the scope; destroying it
//                                   while the promise is pending fails fast.
//   make_promise_future<T>(sched)   the state is a pooled block (scheduler's
//                                   TaskMemoryPool), freed by whichever side
//                                   finishes last. The consumer may walk away
//                                   (drop / detach() the Future) at any time.
//
// Hot path: set() is one atomic exchange on the state word plus, if the
// consumer is parked, one wake (a submit to a worker queue). get() checks the
// state word; if pending, it puts a node on the fiber's stack and parks.
// get() outside a fiber blocks the calling thread instead (fine for main or
// any plain thread, never for a plain callback running on a worker).
//
// Consumer API: get() waits and consumes; try_get() never waits and returns
// std::optional<Result<T>> (nullopt: not yet); ready() peeks. Both get() and
// a successful try_get() consume the result: call them once.

#include <chrono>
#include <cstdio>
#include <string>
#include <thread>

#include "../common.hpp"
#include "fibers/fiber/blocking.hpp"
#include "fibers/fiber/event.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/future.hpp"
#include "fibers/fiber/oneshot.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace this_fiber = fibers::fiber::this_fiber;
using fibers::sched::Scheduler;
using namespace std::chrono_literals;

namespace {

void print(const char* what, const fiber::Result<int>& r) {
    if (r) {
        std::printf("  %-44s -> %d\n", what, *r);
    } else {
        std::printf("  %-44s -> error: %s\n", what, fiber::to_string(r.error()));
    }
}

}  // namespace

int main(int argc, char** argv) {
    const examples::Args args(argc, argv);
    fibers::sched::SchedulerOptions options;
    options.pool.num_workers = static_cast<std::size_t>(args.integer("workers", 2));
    options.blocking.num_threads = 1;
    Scheduler scheduler(options);

    std::thread legacy;  // (c): a plain thread, joined by main at the end

    fiber::Oneshot<int> exit_code;  // main waits on it: get() blocks a plain thread
    fiber::go(scheduler, [&, done = exit_code.promise()]() mutable noexcept {
        int failures = 0;

        // ---- (a) Oneshot on the consumer's stack: zero allocation ----
        {
            fiber::Oneshot<int> reply;  // in this frame, must outlive the producer
            fiber::go([promise = reply.promise()]() mutable noexcept {
                this_fiber::sleep_for(1ms);  // "compute"
                promise.set(6 * 7);
            });
            const fiber::Result<int> r = reply.get();  // parks until set()
            print("(a) Oneshot, producer fiber", r);
            failures += r && *r == 42 ? 0 : 1;
        }  // fine to leave: get() has observed the completion

        // ---- (b) pooled Promise / Future: the consumer may walk away ----
        {
            auto [promise, future] = fiber::make_promise_future<std::string>(scheduler);
            // set() never throws: "pong" becomes a std::string at the call
            // site (set(T value) takes it by value), then it is moved in.
            fiber::go([p = std::move(promise)]() mutable noexcept { p.set("pong"); });
            fiber::Result<std::string> r = future.get();  // consumes; the Future is empty afterwards
            std::printf("  %-44s -> %s (future.valid() = %d)\n", "(b) Future, get()", r ? r->c_str() : "?",
                        future.valid());
            failures += r && *r == "pong" ? 0 : 1;
        }
        {
            // A slow producer and an impatient consumer: poll with
            // try_get(), give up after ~3 ms and drop the Future. The
            // producer's later set() still succeeds; it destroys the value
            // and frees the block itself.
            fiber::Event producer_done;
            auto [promise, future] = fiber::make_promise_future<std::string>(scheduler);
            fiber::go([p = std::move(promise), &producer_done]() mutable noexcept {
                this_fiber::sleep_for(10ms);
                p.set("too late");  // the consumer has gone: no leak, no use-after-free
                producer_done.set();
            });
            int polls = 0;
            for (; polls < 3; ++polls) {
                if (std::optional<fiber::Result<std::string>> r = future.try_get()) {
                    break;  // not this time: the producer needs 10 ms
                }
                this_fiber::sleep_for(1ms);
            }
            future.detach();  // or just let it go out of scope
            producer_done.wait();
            std::printf("  %-44s -> gave up after %d polls; the producer's set() was harmless\n",
                        "(b) Future, try_get() then detach()", polls);
        }

        // ---- (c) producers that are not fibers ----
        {
            // A plain std::thread completes a Oneshot the fiber waits on.
            fiber::Oneshot<int> reply;
            legacy = std::thread([promise = reply.promise()]() mutable noexcept {
                std::this_thread::sleep_for(2ms);  // a plain thread may block
                promise.set(7);
            });
            const fiber::Result<int> r = reply.get();  // the fiber parks, its worker does not
            print("(c) Oneshot, producer on a std::thread", r);
            failures += r && *r == 7 ? 0 : 1;
        }
        {
            // A fiber hands slow, blocking work to the BlockingPool and
            // completes another fiber's Future from there.
            auto [promise, future] = fiber::make_promise_future<int>(scheduler);
            fiber::go([p = std::move(promise)]() mutable noexcept {
                this_fiber::blocking([&p]() noexcept {
                    std::this_thread::sleep_for(2ms);  // runs on a blocking-pool thread
                    p.set(1234);                       // set() from any thread
                });
            });
            const fiber::Result<int> r = future.get();
            print("(c) Future, set() inside this_fiber::blocking", r);
            failures += r && *r == 1234 ? 0 : 1;
        }

        // ---- (d) a broken promise is an error, not a hang ----
        {
            fiber::Oneshot<int> reply;
            fiber::go([promise = reply.promise()]() mutable noexcept {
                const bool valid_request = false;
                if (!valid_request) {
                    return;  // promise destroyed unset -> Error::kBroken
                }
                promise.set(0);
            });
            const fiber::Result<int> r = reply.get();
            print("(d) Oneshot, promise dropped unset", r);
            failures += !r && r.error() == fiber::Error::kBroken ? 0 : 1;

            auto [promise, future] = fiber::make_promise_future<int>(scheduler);
            promise.abandon();  // explicit; the same as destroying it
            const std::optional<fiber::Result<int>> t = future.try_get();
            print("(d) Future, abandon() then try_get()", *t);
            failures += t && !*t ? 0 : 1;
        }
        done.set(failures);
    });

    const fiber::Result<int> failures = exit_code.get();
    legacy.join();
    scheduler.stop();
    std::printf("promise_future: %d failure(s)\n", failures ? *failures : -1);
    return failures && *failures == 0 ? 0 : 1;
}
