// OneshotCore / OneshotState / BasicPromise over plain WaitNodes, and their
// stackful front-ends fiber::Oneshot and the pooled fiber::Promise /
// fiber::Future (StdSync).
//
// Fibers never use gtest assertions themselves: they record into atomics
// (or plain data ordered by the primitive under test) that the test thread
// checks after joining them (test-local Latch).

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "fibers/fiber/blocking.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/future.hpp"
#include "fibers/fiber/oneshot.hpp"
#include "fibers/object_pool/pool_allocator.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/wait/oneshot_core.hpp"
#include "fibers/wait/result.hpp"
#include "fibers/wait/thread_waiter.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fiber = fibers::fiber;
namespace sched = fibers::sched;
namespace fw = fibers::wait;
namespace this_fiber = fibers::fiber::this_fiber;
using fibers::fiber::Fiber;
using fibers::sched::Scheduler;
using fibers::wait::OneshotCore;
using fibers::wait::OneshotState;
using fibers::wait::OneshotStatus;
using fibers::wait::WaitNode;
using namespace std::chrono_literals;

namespace {

// Joins fibers from a non-fiber thread. Declare it BEFORE the scheduler, so
// the workers (that may still be inside count_down) are joined first.
class Latch {
public:
    explicit Latch(std::int64_t count) : count_(count) {}

    void count_down(std::int64_t n = 1) noexcept { count_.fetch_sub(n, std::memory_order_acq_rel); }

    // false on timeout (the test fails instead of hanging).
    bool wait(std::chrono::milliseconds timeout = 30s) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (count_.load(std::memory_order_acquire) != 0) {
            if (std::chrono::steady_clock::now() > deadline) {
                return false;
            }
            std::this_thread::sleep_for(100us);
        }
        return true;
    }

    std::int64_t remaining() const noexcept { return count_.load(); }

private:
    std::atomic<std::int64_t> count_;
};

sched::SchedulerOptions options(std::size_t workers, std::size_t pollers = 2) {
    sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = pollers;
    o.blocking.num_threads = 2;
    o.stacks.stack_size = 64 * 1024;
    o.stacks.max_stacks = 8192;
    o.task_memory.initial_blocks = 64;
    return o;
}

// A plain waiter for the core: counts its wakes and poisons its node inside
// the wake (as if it disappeared with a resumed fiber).
struct Waiter {
    WaitNode node;
    int wakes = 0;

    Waiter() noexcept { node = WaitNode{nullptr, this, &Waiter::wake}; }

    static void wake(void* self) noexcept {
        auto* w = static_cast<Waiter*>(self);
        ++w->wakes;
        w->node.self = reinterpret_cast<void*>(std::uintptr_t{0xdead0});
    }
};

// Counts live instances: every value constructed is destroyed exactly once.
struct Tracked {
    static inline std::atomic<int> live{0};
    int value = 0;

    explicit Tracked(int v) noexcept : value(v) { live.fetch_add(1, std::memory_order_relaxed); }
    Tracked(Tracked&& other) noexcept : value(other.value) { live.fetch_add(1, std::memory_order_relaxed); }
    Tracked& operator=(Tracked&&) = delete;
    ~Tracked() { live.fetch_sub(1, std::memory_order_relaxed); }
};

// A PoolAllocator that counts live blocks (exact leak / double-free check).
struct CountingAllocator {
    std::atomic<std::int64_t> live{0};
    std::atomic<std::int64_t> total{0};

    CountingAllocator() = default;
    CountingAllocator(CountingAllocator&& other) noexcept : live(other.live.load()), total(other.total.load()) {}

    void* allocate(std::size_t bytes, std::size_t alignment) noexcept {
        live.fetch_add(1, std::memory_order_relaxed);
        total.fetch_add(1, std::memory_order_relaxed);
        return ::operator new(bytes, std::align_val_t{alignment});
    }
    void deallocate(void* p, std::size_t bytes, std::size_t alignment) noexcept {
        if (live.fetch_sub(1, std::memory_order_relaxed) <= 0) {
            fibers::fail::fail_fast("test: CountingAllocator: more frees than allocations");
        }
        ::operator delete(p, bytes, std::align_val_t{alignment});
    }
};
static_assert(fibers::object_pool::PoolAllocator<CountingAllocator>);

// p.set(args...) is a valid call.
template <class P, class... Args>
concept CanSet = requires(P& p, Args&&... args) { p.set(std::forward<Args>(args)...); };

// A pooled state over a counting release hook, for the core tests.
template <class T>
struct PooledState {
    std::atomic<int> released{0};
    alignas(OneshotState<T>) std::byte block[sizeof(OneshotState<T>)];

    OneshotState<T>* make() noexcept {
        return ::new (static_cast<void*>(block)) OneshotState<T>(
            [](void* ctx, void*) noexcept { static_cast<PooledState*>(ctx)->released.fetch_add(1); }, this);
    }
};

// ------------------------------------------------------------------ Result

TEST(Result, ValueAndError) {
    fw::Result<int> ok(std::in_place, 7);
    EXPECT_TRUE(ok.has_value());
    EXPECT_TRUE(static_cast<bool>(ok));
    EXPECT_EQ(*ok, 7);
    EXPECT_EQ(ok.value(), 7);
    fw::Result<int> bad = fw::Error::kBroken;
    EXPECT_FALSE(bad);
    EXPECT_EQ(bad.error(), fw::Error::kBroken);
    EXPECT_STREQ(fw::to_string(bad.error()), "broken promise");
    fw::Result<std::unique_ptr<int>> moved(std::in_place, std::make_unique<int>(3));
    std::unique_ptr<int> p = *std::move(moved);
    EXPECT_EQ(*p, 3);
}

TEST(ResultDeathTest, WrongAccess) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            fw::Result<int> bad = fw::Error::kBroken;
            static_cast<void>(bad.value());
        },
        "value\\(\\) on an error");
    EXPECT_DEATH(
        {
            fw::Result<int> ok(std::in_place, 1);
            static_cast<void>(ok.error());
        },
        "error\\(\\) on a value");
}

TEST(Result, Void) {
    fw::Result<void> ok;
    EXPECT_TRUE(ok.has_value());
    EXPECT_TRUE(static_cast<bool>(ok));
    ok.value();  // checks only
    *ok;
    const fw::Result<void> in_place(std::in_place);
    EXPECT_TRUE(in_place);
    fw::Result<void> bad = fw::Error::kBroken;
    EXPECT_FALSE(bad);
    EXPECT_EQ(bad.error(), fw::Error::kBroken);
    bad = ok;  // copyable, like Result<T>
    EXPECT_TRUE(bad);
    static_assert(std::is_same_v<fw::Result<void>::value_type, void>);
    static_assert(std::is_same_v<fiber::Result<void>, fw::Result<void>>);
}

TEST(ResultDeathTest, VoidWrongAccess) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            fw::Result<void> bad = fw::Error::kBroken;
            bad.value();
        },
        "value\\(\\) on an error");
    EXPECT_DEATH(
        {
            fw::Result<void> ok;
            static_cast<void>(ok.error());
        },
        "error\\(\\) on a value");
}

// --------------------------------------------------------------- OneshotCore

TEST(OneshotCore, SetBeforeWait) {
    OneshotState<int> state;
    fw::BasicPromise<int> promise(&state);
    EXPECT_EQ(state.status(), OneshotStatus::kPending);
    promise.set(42);
    EXPECT_FALSE(promise.valid());
    EXPECT_EQ(state.status(), OneshotStatus::kReady);
    Waiter w;
    EXPECT_EQ(state.wait_or_enqueue(w.node), OneshotStatus::kReady);  // final: does not wait
    EXPECT_EQ(w.wakes, 0);
    fw::Result<int> r = state.take(state.status());
    ASSERT_TRUE(r);
    EXPECT_EQ(*r, 42);
}

TEST(OneshotCore, WaitBeforeSet) {
    OneshotState<int> state;
    fw::BasicPromise<int> promise(&state);
    Waiter w;
    EXPECT_EQ(state.wait_or_enqueue(w.node), OneshotStatus::kPending);
    EXPECT_TRUE(state.has_waiter_approx());
    EXPECT_EQ(state.status(), OneshotStatus::kPending);
    promise.set(7);
    EXPECT_EQ(w.wakes, 1);
    EXPECT_FALSE(state.has_waiter_approx());
    EXPECT_EQ(state.status(), OneshotStatus::kReady);
    fw::Result<int> r = state.take(OneshotStatus::kReady);
    EXPECT_EQ(*r, 7);
}

TEST(OneshotCore, BrokenPromiseWakesWaiter) {
    OneshotState<int> state;
    Waiter w;
    {
        fw::BasicPromise<int> promise(&state);
        EXPECT_EQ(state.wait_or_enqueue(w.node), OneshotStatus::kPending);
    }  // destroyed unset: broken
    EXPECT_EQ(w.wakes, 1);
    EXPECT_EQ(state.status(), OneshotStatus::kBroken);
    fw::Result<int> r = state.take(OneshotStatus::kBroken);
    EXPECT_FALSE(r);
    EXPECT_EQ(r.error(), fw::Error::kBroken);
}

TEST(OneshotCore, AbandonAndMoves) {
    OneshotState<int> a;
    OneshotState<int> b;
    fw::BasicPromise<int> pa(&a);
    fw::BasicPromise<int> pb(&b);
    fw::BasicPromise<int> moved(std::move(pa));
    EXPECT_FALSE(pa.valid());
    EXPECT_TRUE(moved.valid());
    moved = std::move(pb);  // breaks a (the promise it held)
    EXPECT_EQ(a.status(), OneshotStatus::kBroken);
    EXPECT_EQ(b.status(), OneshotStatus::kPending);
    moved.abandon();
    EXPECT_EQ(b.status(), OneshotStatus::kBroken);
    moved.abandon();  // empty: no-op
}

TEST(OneshotCore, DetachBeforeCompletionProducerFrees) {
    Tracked::live = 0;
    PooledState<Tracked> pooled;
    OneshotState<Tracked>* state = pooled.make();
    fw::BasicPromise<Tracked> promise(state);
    EXPECT_EQ(state->detach(), OneshotStatus::kPending);
    EXPECT_EQ(pooled.released.load(), 0);
    promise.set(5);  // the consumer has gone: the producer destroys and frees
    EXPECT_EQ(pooled.released.load(), 1);
    EXPECT_EQ(Tracked::live.load(), 0);
}

TEST(OneshotCore, DetachAfterCompletionConsumerFrees) {
    Tracked::live = 0;
    PooledState<Tracked> pooled;
    OneshotState<Tracked>* state = pooled.make();
    fw::BasicPromise<Tracked> promise(state);
    promise.set(5);
    EXPECT_EQ(Tracked::live.load(), 1);
    EXPECT_EQ(pooled.released.load(), 0);
    EXPECT_EQ(state->detach(), OneshotStatus::kReady);  // the caller frees
    state->destroy_value();
    state->release();
    EXPECT_EQ(pooled.released.load(), 1);
    EXPECT_EQ(Tracked::live.load(), 0);
}

TEST(OneshotCore, DetachThenAbandon) {
    PooledState<int> pooled;
    OneshotState<int>* state = pooled.make();
    fw::BasicPromise<int> promise(state);
    EXPECT_EQ(state->detach(), OneshotStatus::kPending);
    promise.abandon();
    EXPECT_EQ(pooled.released.load(), 1);
}

// T = void: the same protocol, the slot holds an empty tag; set() takes no
// arguments, take() returns a Result<void>.
TEST(OneshotCore, Void) {
    {
        OneshotState<void> state;
        fw::BasicPromise<void> promise(&state);
        Waiter w;
        EXPECT_EQ(state.wait_or_enqueue(w.node), OneshotStatus::kPending);
        promise.set();
        EXPECT_FALSE(promise.valid());
        EXPECT_EQ(w.wakes, 1);
        fw::Result<void> r = state.take(state.status());
        EXPECT_TRUE(r);
    }
    {
        OneshotState<void> state;
        { fw::BasicPromise<void> promise(&state); }  // broken
        fw::Result<void> r = state.take(state.status());
        ASSERT_FALSE(r);
        EXPECT_EQ(r.error(), fw::Error::kBroken);
    }
    {  // pooled, consumer detached first: the producer frees
        PooledState<void> pooled;
        OneshotState<void>* state = pooled.make();
        fw::BasicPromise<void> promise(state);
        EXPECT_EQ(state->detach(), OneshotStatus::kPending);
        promise.set();
        EXPECT_EQ(pooled.released.load(), 1);
    }
    static_assert(!CanSet<fw::BasicPromise<void>, int>);
    static_assert(CanSet<fw::BasicPromise<void>>);
}

// How set() builds the value: by value at the call site (a literal, a copy,
// a braced list), or in place when that construction is noexcept.
TEST(OneshotCore, SetErgonomics) {
    {
        fiber::Oneshot<std::string> o;
        o.promise().set("literal");  // const char[8] -> std::string at the call site
        EXPECT_EQ(*o.get(), "literal");
    }
    {
        const std::string text = "copied, not moved";
        fiber::Oneshot<std::string> o;
        o.promise().set(text);  // a copy at the call site (may throw there, not in set())
        EXPECT_EQ(*o.get(), text);
    }
    {
        struct Point {
            int x;
            int y;
        };
        fiber::Oneshot<Point> o;
        o.promise().set({3, 4});  // a braced list builds the by-value argument
        const fiber::Result<Point> r = o.get();
        EXPECT_EQ(r->x + r->y, 7);
    }
    {
        // In place (noexcept construction): no move at all; by value: one
        // move into the state.
        struct Counted {
            int value = 0;
            int* moves = nullptr;
            Counted(int v, int* m) noexcept : value(v), moves(m) {}
            Counted(Counted&& other) noexcept : value(other.value), moves(other.moves) { ++*moves; }
        };
        int moves = 0;
        {
            fiber::Oneshot<Counted> o;
            o.promise().set(1, &moves);
            EXPECT_EQ(moves, 0);
            EXPECT_EQ(o.get()->value, 1);
        }
        moves = 0;
        {
            fiber::Oneshot<Counted> o;
            o.promise().set(Counted(2, &moves));
            EXPECT_EQ(moves, 1);
        }
    }
    {
        CountingAllocator alloc;
        auto [promise, future] = fiber::make_promise_future<std::string>(alloc);
        promise.set("pooled");
        EXPECT_EQ(*future.get(), "pooled");
    }
    // set() itself is always noexcept (building a by-value argument is the
    // call site's part of the expression). A throwing, explicit-only
    // construction is not accepted silently: that overload exists but
    // static_asserts (tests/oneshot/set_rejects.cpp checks the message).
    static_assert(CanSet<fiber::Promise<std::string>, const char (&)[3]>);
    static_assert(CanSet<fiber::Promise<std::string>, std::string&>);
    static_assert(CanSet<fiber::Promise<std::string>, std::string>);
    static_assert(noexcept(std::declval<fiber::Promise<std::string>&>().set(std::declval<std::string>())));
}

TEST(OneshotCoreDeathTest, Misuse) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            OneshotCore<> core;
            static_cast<void>(core.complete(OneshotStatus::kReady));
            static_cast<void>(core.complete(OneshotStatus::kBroken));
        },
        "completed twice");
    EXPECT_DEATH(
        {
            OneshotState<int> state;
            fw::BasicPromise<int> promise(&state);
            promise.set(1);
            promise.set(2);
        },
        "empty Promise");
    EXPECT_DEATH(
        {
            OneshotCore<> core;
            Waiter a;
            Waiter b;
            static_cast<void>(core.wait_or_enqueue(a.node));
            static_cast<void>(core.wait_or_enqueue(b.node));
        },
        "one consumer");
    EXPECT_DEATH(
        {
            OneshotCore<> core;
            Waiter a;
            static_cast<void>(core.wait_or_enqueue(a.node));
            static_cast<void>(core.detach());
        },
        "enqueued waiter");
    EXPECT_DEATH(
        {
            OneshotCore<> core;
            static_cast<void>(core.detach());
            static_cast<void>(core.detach());
        },
        "detached twice");
    EXPECT_DEATH(
        {
            Waiter a;
            OneshotCore<> core;
            static_cast<void>(core.wait_or_enqueue(a.node));
        },
        "enqueued waiter");
}

// Runs producer(round) on a second thread and consumer(round) on this one,
// started together (the producer spins on the round counter), so the two
// sides of each round really race.
template <class Producer, class Consumer>
void race_rounds(int rounds, Producer producer, Consumer consumer) {
    std::atomic<int> started{0};
    std::thread thread([&] {
        for (int round = 0; round < rounds; ++round) {
            while (started.load(std::memory_order_acquire) <= round) {
                fibers::sync::cpu_relax();
            }
            producer(round);
        }
    });
    for (int round = 0; round < rounds; ++round) {
        started.store(round + 1, std::memory_order_release);
        // Jitter: the producer needs a cache-line transfer to see the start,
        // so without a delay the consumer would nearly always come first.
        for (int i = (round * 7) % 24; i > 0; --i) {
            fibers::sync::cpu_relax();
        }
        consumer(round);
    }
    thread.join();
}

// Producer and consumer threads on plain nodes (the consumer spins on its
// wake flag); payload written before set() is seen after the result.
TEST(OneshotCore, ThreadsStress) {
    constexpr int kRounds = 50'000;
    struct SpinWaiter {
        WaitNode node;
        std::atomic<int> wakes{0};
        SpinWaiter() noexcept { node = WaitNode{nullptr, this, &SpinWaiter::wake}; }
        static void wake(void* self) noexcept {
            static_cast<SpinWaiter*>(self)->wakes.fetch_add(1, std::memory_order_release);
        }
    };
    struct Round {
        OneshotState<int> state;
        fw::BasicPromise<int> promise{&state};
        int payload = 0;
        SpinWaiter waiter;
    };
    auto rounds = std::make_unique<Round[]>(kRounds);
    int bad = 0;
    int waited = 0;
    race_rounds(
        kRounds,
        [&](int round) {
            Round& r = rounds[static_cast<std::size_t>(round)];
            r.payload = round;
            if (round % 5 == 0) {
                r.promise.abandon();
            } else {
                r.promise.set(round);
            }
        },
        [&](int round) {
            Round& r = rounds[static_cast<std::size_t>(round)];
            OneshotStatus s = r.state.wait_or_enqueue(r.waiter.node);
            if (s == OneshotStatus::kPending) {
                ++waited;
                while (r.waiter.wakes.load(std::memory_order_acquire) == 0) {
                    fibers::sync::cpu_relax();
                }
                s = r.state.status();
            }
            fw::Result<int> result = r.state.take(s);
            if (r.payload != round || (round % 5 == 0) == result.has_value() || (result && *result != round)) {
                ++bad;
            }
        });
    for (int round = 0; round < kRounds; ++round) {
        if (rounds[static_cast<std::size_t>(round)].waiter.wakes.load() > 1) {
            ++bad;
        }
    }
    EXPECT_EQ(bad, 0);
    std::printf("waited in %d of %d rounds\n", waited, kRounds);
}

// The detach / complete race: exactly one side frees, every value is
// destroyed exactly once.
TEST(OneshotCore, DetachRaceStress) {
    constexpr int kRounds = 50'000;
    struct Round {
        PooledState<Tracked> pooled;
        OneshotState<Tracked>* state = pooled.make();
        fw::BasicPromise<Tracked> promise{state};
    };
    Tracked::live = 0;
    auto rounds = std::make_unique<Round[]>(kRounds);
    int producer_freed = 0;
    race_rounds(
        kRounds,
        [&](int round) {
            Round& r = rounds[static_cast<std::size_t>(round)];
            if (round % 3 == 0) {
                r.promise.abandon();
            } else {
                r.promise.set(round);
            }
        },
        [&](int round) {
            Round& r = rounds[static_cast<std::size_t>(round)];
            const OneshotStatus s = r.state->detach();
            if (s != OneshotStatus::kPending) {
                if (s == OneshotStatus::kReady) {
                    r.state->destroy_value();
                }
                r.state->release();
            }
            producer_freed += s == OneshotStatus::kPending ? 1 : 0;
        });
    for (int round = 0; round < kRounds; ++round) {
        ASSERT_EQ(rounds[static_cast<std::size_t>(round)].pooled.released.load(), 1) << "round " << round;
    }
    EXPECT_EQ(Tracked::live.load(), 0);
    std::printf("producer freed in %d of %d rounds\n", producer_freed, kRounds);
}

TEST(ThreadWaiter, BlocksUntilSet) {
    for (int round = 0; round < 200; ++round) {
        OneshotState<int> state;
        fw::BasicPromise<int> promise(&state);
        std::thread producer([&] {
            if (round % 2 == 0) {
                std::this_thread::sleep_for(50us);
            }
            promise.set(round);
        });
        fw::ThreadWaiter<> waiter;
        if (state.wait_or_enqueue(waiter.node) == OneshotStatus::kPending) {
            waiter.wait();
        }
        EXPECT_EQ(*state.take(state.status()), round);
        producer.join();
    }
}

// ------------------------------------------------------- Oneshot (no fibers)

TEST(Oneshot, OnPlainThreads) {
    Tracked::live = 0;
    {
        fiber::Oneshot<Tracked> o;
        fiber::Promise<Tracked> p = o.promise();
        EXPECT_FALSE(o.ready());
        EXPECT_FALSE(o.try_get().has_value());
        std::thread producer([&] {
            std::this_thread::sleep_for(1ms);
            p.set(11);
        });
        fw::Result<Tracked> r = o.get();  // blocks this thread (not a worker)
        producer.join();
        ASSERT_TRUE(r);
        EXPECT_EQ(r->value, 11);
        EXPECT_TRUE(o.consumed());
    }
    {
        fiber::Oneshot<Tracked> o;
        o.promise().set(1);  // the temporary Promise is set, then destroyed (no-op)
        EXPECT_TRUE(o.ready());
        EXPECT_EQ(Tracked::live.load(), 1);
    }  // never taken: destroyed with the Oneshot
    EXPECT_EQ(Tracked::live.load(), 0);
    {
        fiber::Oneshot<int> o;
        { auto p = o.promise(); }
        std::optional<fw::Result<int>> r = o.try_get();
        ASSERT_TRUE(r.has_value());
        EXPECT_EQ(r->error(), fw::Error::kBroken);
    }
    {
        fiber::Oneshot<int> unused;  // promise() never taken: destroyable
    }
}

TEST(Oneshot, VoidOnPlainThreads) {
    {
        fiber::Oneshot<void> done;
        fiber::Promise<void> p = done.promise();
        int payload = 0;  // written before set(), read after get()
        std::thread producer([&] {
            std::this_thread::sleep_for(1ms);
            payload = 5;
            p.set();
        });
        const fiber::Result<void> r = done.get();  // blocks this thread
        producer.join();
        EXPECT_TRUE(r);
        EXPECT_EQ(payload, 5);
    }
    {
        fiber::Oneshot<void> done;
        done.promise().abandon();
        std::optional<fiber::Result<void>> r = done.try_get();
        ASSERT_TRUE(r.has_value());
        ASSERT_FALSE(*r);
        EXPECT_EQ(r->error(), fw::Error::kBroken);
    }
    {
        fiber::Oneshot<void> done;
        done.promise().set();  // never taken: fine
        EXPECT_TRUE(done.ready());
    }
}

TEST(OneshotDeathTest, Misuse) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            fiber::Promise<int> p;
            {
                fiber::Oneshot<int> o;
                p = o.promise();
            }
        },
        "Promise is still outstanding");
    EXPECT_DEATH(
        {
            fiber::Oneshot<int> o;
            o.promise().set(1);
            static_cast<void>(o.get());
            static_cast<void>(o.get());
        },
        "already consumed");
    EXPECT_DEATH(
        {
            fiber::Oneshot<int> o;
            static_cast<void>(o.get());
        },
        "before promise");
    EXPECT_DEATH(
        {
            fiber::Oneshot<int> o;
            auto p1 = o.promise();
            auto p2 = o.promise();
        },
        "called twice");
}

// ------------------------------------------------ Future (no fibers)

TEST(Future, LifetimesWithCountingAllocator) {
    Tracked::live = 0;
    CountingAllocator alloc;
    {  // set, then get
        auto [promise, future] = fiber::make_promise_future<Tracked>(alloc);
        EXPECT_EQ(alloc.live.load(), 1);
        promise.set(3);
        EXPECT_TRUE(future.ready());
        fw::Result<Tracked> r = future.get();
        EXPECT_EQ(r->value, 3);
        EXPECT_FALSE(future.valid());
        EXPECT_EQ(alloc.live.load(), 0);  // freed by get()
    }
    {  // detach before set: the producer frees
        auto [promise, future] = fiber::make_promise_future<Tracked>(alloc);
        future.detach();
        EXPECT_FALSE(future.valid());
        EXPECT_EQ(alloc.live.load(), 1);
        promise.set(4);
        EXPECT_EQ(alloc.live.load(), 0);
    }
    {  // set, then the Future is dropped untaken: the consumer frees
        auto [promise, future] = fiber::make_promise_future<Tracked>(alloc);
        promise.set(5);
        EXPECT_EQ(Tracked::live.load(), 1);
    }
    {  // broken, then try_get
        auto [promise, future] = fiber::make_promise_future<Tracked>(alloc);
        EXPECT_FALSE(future.try_get().has_value());
        promise.abandon();
        std::optional<fw::Result<Tracked>> r = future.try_get();
        ASSERT_TRUE(r.has_value());
        EXPECT_FALSE(*r);
        EXPECT_FALSE(future.valid());
    }
    {  // both dropped unset (the Future first: detach; then the Promise: abandon + free)
        auto pair = fiber::make_promise_future<Tracked>(alloc);
        fiber::Promise<Tracked> promise = std::move(pair.promise);
        fiber::Future<Tracked> future = std::move(pair.future);
    }
    {  // move-assigning over a live Future detaches it
        auto a = fiber::make_promise_future<int>(alloc);
        auto b = fiber::make_promise_future<int>(alloc);
        a.future = std::move(b.future);
        a.promise.set(1);  // a's consumer has gone: freed here
        EXPECT_EQ(alloc.live.load(), 1);
        b.promise.set(2);
        EXPECT_EQ(*a.future.get(), 2);
        EXPECT_EQ(alloc.live.load(), 0);
    }
    EXPECT_EQ(alloc.live.load(), 0);
    EXPECT_EQ(alloc.total.load(), 7);
    EXPECT_EQ(Tracked::live.load(), 0);
}

TEST(Future, Void) {
    CountingAllocator alloc;
    {  // set, then get
        auto [promise, future] = fiber::make_promise_future<void>(alloc);
        promise.set();
        EXPECT_TRUE(future.ready());
        const fiber::Result<void> r = future.get();
        EXPECT_TRUE(r);
        EXPECT_FALSE(future.valid());
    }
    {  // detach before set: the producer frees
        auto [promise, future] = fiber::make_promise_future<void>(alloc);
        future.detach();
        promise.set();
    }
    {  // broken
        auto [promise, future] = fiber::make_promise_future<void>(alloc);
        promise.abandon();
        const fiber::Result<void> r = future.get();
        ASSERT_FALSE(r);
        EXPECT_EQ(r.error(), fw::Error::kBroken);
    }
    {  // set, then the Future dropped untaken
        auto [promise, future] = fiber::make_promise_future<void>(alloc);
        promise.set();
    }
    EXPECT_EQ(alloc.live.load(), 0);
    EXPECT_EQ(alloc.total.load(), 4);

    // From the scheduler's TaskMemoryPool, completed by a fiber, waited for
    // by this (plain) thread.
    Scheduler scheduler(options(2));
    for (int i = 0; i < 100; ++i) {
        auto [promise, future] = fiber::make_promise_future<void>(scheduler);
        int payload = 0;
        fiber::go(scheduler, [&payload, i, p = std::move(promise)]() mutable noexcept {
            payload = i + 1;
            p.set();
        });
        EXPECT_TRUE(future.get());
        EXPECT_EQ(payload, i + 1);
    }
}

TEST(Future, TaskMemoryPoolBlocks) {
    Scheduler scheduler(options(1));
    struct Big {
        char bytes[400];
    };
    static_assert(sched::TaskMemoryPool<>::block_size_for(sizeof(OneshotState<Big>)) == 512);
    const std::size_t cap64 = scheduler.task_memory().capacity<64>();
    const std::size_t cap512 = scheduler.task_memory().capacity<512>();
    for (int i = 0; i < 10'000; ++i) {
        auto [p1, f1] = fiber::make_promise_future<int>(scheduler);
        auto [p2, f2] = fiber::make_promise_future<Big>(scheduler);
        if (i % 2 == 0) {
            p1.set(i);
            EXPECT_EQ(*f1.get(), i);
        } else {
            f1.detach();
        }
        p2.set(Big{});
    }
    // No leak: the pools never had to grow.
    EXPECT_EQ(scheduler.task_memory().capacity<64>(), cap64);
    EXPECT_EQ(scheduler.task_memory().capacity<512>(), cap512);
}

TEST(FutureDeathTest, Misuse) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            CountingAllocator alloc;
            auto pair = fiber::make_promise_future<int>(alloc);
            pair.promise.set(1);
            static_cast<void>(pair.future.get());
            static_cast<void>(pair.future.get());
        },
        "empty Future");
    EXPECT_DEATH(
        {
            fiber::Future<int> future;
            static_cast<void>(future.try_get());
        },
        "empty Future");
}

// ------------------------------------------------------------------ fibers

// Request / response chain between two fibers, both directions through
// Oneshots on the fibers' stacks: the client sends {n, reply promise} via
// the server's current request Oneshot; the server answers with {n + 1,
// promise of its next request Oneshot}.
struct Request;
struct Reply {
    int value = 0;
    fiber::Promise<Request> next;  // where to send the next request
};
struct Request {
    int value = 0;
    fiber::Promise<Reply> reply;
};

TEST(OneshotFibers, PingPongChain) {
    constexpr int kRounds = 20'000;
    Latch done(2);
    std::atomic<int> bad{0};
    std::atomic<int> server_seen{0};
    Scheduler scheduler(options(4));
    fiber::Promise<Request> first;
    fiber::Oneshot<fiber::Promise<Request>> handshake;  // the server's first request promise
    fiber::Promise<fiber::Promise<Request>> handshake_promise = handshake.promise();
    fiber::go(scheduler, [&]() noexcept {  // server
        std::optional<fiber::Oneshot<Request>> slots[2];
        slots[0].emplace();
        handshake_promise.set(slots[0]->promise());
        for (int i = 0;; ++i) {
            fiber::Oneshot<Request>& cur = *slots[i % 2];
            fw::Result<Request> rq = cur.get();
            if (!rq) {
                break;  // the client has gone: broken
            }
            server_seen.fetch_add(1, std::memory_order_relaxed);
            fiber::Oneshot<Request>& next = slots[(i + 1) % 2].emplace();
            rq->reply.set(Reply{rq->value + 1, next.promise()});
        }
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {  // client
        fiber::Promise<Request> to_server = *handshake.get();
        for (int i = 0; i < kRounds; ++i) {
            fiber::Oneshot<Reply> reply;
            to_server.set(Request{i, reply.promise()});
            fw::Result<Reply> r = reply.get();
            if (!r || r->value != i + 1) {
                bad.fetch_add(1);
                break;
            }
            to_server = std::move(r->next);
        }
        done.count_down();  // to_server dies with the frame: the server sees a broken request
    });
    ASSERT_TRUE(done.wait(60s));
    EXPECT_EQ(bad.load(), 0);
    EXPECT_EQ(server_seen.load(), kRounds);
}

// Many client fibers on 8 workers, each sending requests to freshly spawned
// server fibers, alternating Oneshot (on the client's stack) and the pooled
// pair; a share of the servers abandon (broken) instead of answering.
TEST(OneshotFibers, ManyPairsManyWorkers) {
    constexpr int kClients = 64;
    constexpr int kRequests = 300;
    Latch done(kClients);
    std::atomic<int> bad{0};
    std::atomic<int> broken{0};
    CountingAllocator alloc;
    {
        Scheduler scheduler(options(8));
        for (int c = 0; c < kClients; ++c) {
            fiber::go(scheduler, [&, c]() noexcept {
                for (int i = 0; i < kRequests; ++i) {
                    const int expect = c * 100'000 + i;
                    const bool fail = (i % 17) == 3;
                    auto serve = [expect, fail](fiber::Promise<int> p) noexcept {
                        return [expect, fail, p = std::move(p)]() mutable noexcept {
                            if (expect % 3 == 0) {
                                this_fiber::yield();
                            }
                            if (!fail) {
                                p.set(expect);
                            }  // else: p dies unset -> broken
                        };
                    };
                    fw::Result<int> r = fw::Error::kBroken;
                    if (i % 2 == 0) {
                        fiber::Oneshot<int> reply;
                        fiber::go(serve(reply.promise()));
                        r = reply.get();
                    } else if (i % 4 == 1) {
                        auto [promise, future] = fiber::make_promise_future<int>(scheduler);
                        fiber::go(serve(std::move(promise)));
                        r = future.get();
                    } else {
                        auto [promise, future] = fiber::make_promise_future<int>(alloc);
                        fiber::go(serve(std::move(promise)));
                        r = future.get();
                    }
                    if (fail) {
                        broken.fetch_add(1);
                        if (r || r.error() != fw::Error::kBroken) {
                            bad.fetch_add(1);
                        }
                    } else if (!r || *r != expect) {
                        bad.fetch_add(1);
                    }
                }
                done.count_down();
            });
        }
        ASSERT_TRUE(done.wait(60s)) << done.remaining() << " clients stuck";
    }
    EXPECT_EQ(bad.load(), 0);
    EXPECT_GT(broken.load(), 0);
    EXPECT_EQ(alloc.live.load(), 0);
}

TEST(OneshotFibers, ProducerOnPlainThread) {
    constexpr int kRounds = 500;
    Latch done(1);
    std::atomic<int> bad{0};
    std::atomic<fiber::Promise<int>*> mailbox{nullptr};
    Scheduler scheduler(options(2));
    std::thread producer([&] {
        for (int i = 0; i < kRounds; ++i) {
            fiber::Promise<int>* p = nullptr;
            while ((p = mailbox.exchange(nullptr, std::memory_order_acquire)) == nullptr) {
                std::this_thread::yield();
            }
            if (i % 3 == 0) {
                std::this_thread::sleep_for(20us);  // let the fiber park
            }
            p->set(i);
        }
    });
    fiber::go(scheduler, [&]() noexcept {
        for (int i = 0; i < kRounds; ++i) {
            fiber::Oneshot<int> reply;
            fiber::Promise<int> p = reply.promise();
            mailbox.store(&p, std::memory_order_release);
            if (*reply.get() != i) {
                bad.fetch_add(1);
            }
        }
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    producer.join();
    EXPECT_EQ(bad.load(), 0);
}

// Oneshot<void> as a "done" signal between fibers: the consumer parks
// until the producer fiber's set().
TEST(OneshotFibers, VoidSignal) {
    constexpr int kRounds = 2000;
    Latch done(1);
    std::atomic<int> bad{0};
    Scheduler scheduler(options(4));
    fiber::go(scheduler, [&]() noexcept {
        for (int i = 0; i < kRounds; ++i) {
            fiber::Oneshot<void> finished;
            int value = 0;
            fiber::go([&value, i, p = finished.promise()]() mutable noexcept {
                if (i % 2 == 0) {
                    this_fiber::yield();
                }
                value = i;
                p.set();
            });
            if (!finished.get() || value != i) {
                bad.fetch_add(1);
            }
        }
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(bad.load(), 0);
}

// A plain thread waits on a Future that fibers complete.
TEST(OneshotFibers, ConsumerOnPlainThread) {
    Scheduler scheduler(options(2));
    for (int i = 0; i < 200; ++i) {
        auto [promise, future] = fiber::make_promise_future<int>(scheduler);
        fiber::go(scheduler, [i, p = std::move(promise)]() mutable noexcept {
            if (i % 2 == 0) {
                this_fiber::sleep_for(20us);
            }
            p.set(i);
        });
        EXPECT_EQ(*future.get(), i);  // blocks the test thread
    }
}

TEST(OneshotFibers, ProducerInPollerTimer) {
    constexpr int kTimers = 32;
    struct Ctx {
        fiber::Promise<int> promise;
        int payload = 0;
        int id = 0;
    };
    Latch done(kTimers);
    std::atomic<int> bad{0};
    auto ctxs = std::make_unique<Ctx[]>(kTimers);
    std::vector<std::unique_ptr<Scheduler::Timer>> timers;
    for (int i = 0; i < kTimers; ++i) {
        timers.push_back(std::make_unique<Scheduler::Timer>(
            &ctxs[static_cast<std::size_t>(i)],
            [](void* memo, fibers::poller::TimerEvent, fibers::poller::TaskBatch&) noexcept {
                auto* c = static_cast<Ctx*>(memo);
                c->payload = c->id * 2;
                c->promise.set(c->id);  // wakes the fiber from the poller thread
            }));
    }
    Scheduler scheduler(options(4));
    for (int i = 0; i < kTimers; ++i) {
        fiber::go(scheduler, [&, i]() noexcept {
            Ctx& c = ctxs[static_cast<std::size_t>(i)];
            c.id = i;
            fiber::Oneshot<int> o;
            c.promise = o.promise();
            Scheduler::Timer& timer = *timers[static_cast<std::size_t>(i)];
            timer.deadline = scheduler.now() + std::chrono::microseconds(100 * (i % 5));
            scheduler.pollers().schedule(timer);
            fw::Result<int> r = o.get();
            if (!r || *r != i || c.payload != i * 2) {
                bad.fetch_add(1);
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(bad.load(), 0);
}

// The value is produced by a call on the blocking pool: set() runs on a
// blocking thread, the consumer is another fiber.
TEST(OneshotFibers, ProducerViaBlocking) {
    constexpr int kRounds = 300;
    Latch done(1);
    std::atomic<int> bad{0};
    std::atomic<int> on_blocking_thread{0};
    Scheduler scheduler(options(2));
    fiber::go(scheduler, [&]() noexcept {
        for (int i = 0; i < kRounds; ++i) {
            fiber::Oneshot<int> reply;
            fiber::go([&on_blocking_thread, &scheduler, i, p = reply.promise()]() mutable noexcept {
                this_fiber::blocking([&]() noexcept {
                    if (!scheduler.in_worker_thread()) {
                        on_blocking_thread.fetch_add(1, std::memory_order_relaxed);
                    }
                    p.set(i * 3);
                });
            });
            if (*reply.get() != i * 3) {
                bad.fetch_add(1);
            }
        }
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(bad.load(), 0);
    EXPECT_EQ(on_blocking_thread.load(), kRounds);
}

// The consumer drops the pooled Future before set (detach): the producer
// frees. Counted allocator (exact) and TaskMemoryPool (no growth); ASan
// checks for use-after-free.
TEST(OneshotFibers, ConsumerDetachesBeforeSet) {
    constexpr int kRounds = 3000;
    Tracked::live = 0;
    CountingAllocator alloc;
    Latch done(1);
    std::atomic<int> producers{0};
    {
        Scheduler scheduler(options(4));
        fiber::go(scheduler, [&]() noexcept {
            for (int i = 0; i < kRounds; ++i) {
                auto run = [&producers, i](fiber::Promise<Tracked> p) noexcept {
                    return [&producers, i, p = std::move(p)]() mutable noexcept {
                        if (i % 2 == 0) {
                            this_fiber::sleep_for(10us);
                        }
                        p.set(i);
                        producers.fetch_add(1, std::memory_order_release);
                    };
                };
                if (i % 2 == 0) {
                    auto [promise, future] = fiber::make_promise_future<Tracked>(alloc);
                    fiber::go(run(std::move(promise)));
                    if (i % 4 == 0) {
                        this_fiber::yield();
                    }
                }  // future dropped: detached, mostly before set
                else {
                    auto [promise, future] = fiber::make_promise_future<Tracked>(scheduler);
                    fiber::go(run(std::move(promise)));
                    future.detach();
                }
            }
            done.count_down();
        });
        ASSERT_TRUE(done.wait());
        while (producers.load(std::memory_order_acquire) != kRounds) {
            std::this_thread::sleep_for(100us);
        }
    }
    EXPECT_EQ(alloc.live.load(), 0);
    EXPECT_EQ(alloc.total.load(), kRounds / 2);
    EXPECT_EQ(Tracked::live.load(), 0);
}

// The NOTIFIED path, deterministically: the consumer publishes its node and
// stays on its stack until the producer's set() has returned, so the wake
// finds the fiber RUNNING; park() returns at once.
TEST(OneshotFibers, ConsumerWokenWhileStillSwitchingOut) {
    constexpr int kRounds = 100;
    struct Round {
        OneshotState<int> state;
        fw::BasicPromise<int> promise{&state};
        std::atomic<bool> woken{false};
    };
    Latch done(1);
    std::atomic<Round*> current{nullptr};
    std::atomic<Fiber*> consumer{nullptr};
    std::atomic<int> bad{0};
    int notified_seen = 0;
    auto rounds = std::make_unique<Round[]>(kRounds);
    Scheduler scheduler(options(2));
    fiber::go(scheduler, [&]() noexcept {
        Fiber& self = *Fiber::current();
        consumer.store(&self);
        for (int i = 0; i < kRounds; ++i) {
            Round& r = rounds[static_cast<std::size_t>(i)];
            WaitNode node = self.wait_node();
            if (r.state.wait_or_enqueue(node) == OneshotStatus::kPending) {
                current.store(&r, std::memory_order_release);
                while (!r.woken.load(std::memory_order_acquire)) {
                    fibers::sync::cpu_relax();
                }
                self.park();  // returns at once: the wake already happened
            } else {
                bad.fetch_add(1);
            }
            if (*r.state.take(r.state.status()) != i) {
                bad.fetch_add(1);
            }
        }
        done.count_down();
    });
    for (int i = 0; i < kRounds; ++i) {
        Round* r = nullptr;
        while ((r = current.exchange(nullptr, std::memory_order_acquire)) == nullptr) {
            std::this_thread::yield();
        }
        r->promise.set(i);
        if (consumer.load()->state_approx() == (Fiber::kRunning | Fiber::kNotified)) {
            ++notified_seen;
        }
        r->woken.store(true, std::memory_order_release);
    }
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(bad.load(), 0);
    EXPECT_EQ(notified_seen, kRounds);
}

}  // namespace
