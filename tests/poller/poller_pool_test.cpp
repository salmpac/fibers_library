// PollerPool: round-robin placement over several pollers, cancel through the
// owning poller, fds on a chosen poller.

#include <gtest/gtest.h>

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <thread>
#include <vector>

#include "fibers/poller/eventfd.hpp"
#include "fibers/poller/fake_backend.hpp"
#include "fibers/poller/poller_pool.hpp"

namespace poller = fibers::poller;

namespace {

using namespace std::chrono_literals;
using poller::Epollable;
using poller::Task;
using poller::TaskBatch;
using poller::TaskSink;
using poller::TimerEvent;
using Clock = std::chrono::steady_clock;
using Pool = poller::PollerPool<>;
using Timer = Pool::Timer;

// Runs submitted tasks right away (on the poller thread).
struct InlineSink {
    void submit_batch(Task* tasks, std::size_t count) noexcept {
        for (std::size_t i = 0; i < count; ++i) {
            std::move(tasks[i]).run();
        }
    }
};

template <class Pred>
bool wait_until(Pred pred, std::chrono::milliseconds timeout = 10s) {
    const auto deadline = Clock::now() + timeout;
    while (!pred()) {
        if (Clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(100us);
    }
    return true;
}

struct Counters {
    std::atomic<std::size_t> fired{0};
    std::atomic<std::size_t> cancelled{0};
};

void count_event(void* memo, TimerEvent event, TaskBatch&) noexcept {
    auto* c = static_cast<Counters*>(memo);
    (event == TimerEvent::kFired ? c->fired : c->cancelled).fetch_add(1);
}

}  // namespace

TEST(PollerPool, RoundRobinSpreadsTimersEvenly) {
    constexpr std::size_t kPollers = 4;
    constexpr std::size_t kPerPoller = 50;
    InlineSink sink;
    Counters counters;
    std::vector<std::unique_ptr<Timer>> timers;
    Pool pool(TaskSink::of(sink), {.num_pollers = kPollers});
    pool.start();
    for (std::size_t i = 0; i < kPollers * kPerPoller; ++i) {
        auto& t = *timers.emplace_back(std::make_unique<Timer>(&counters, &count_event));
        t.deadline = pool.at(0).now() + 1ms;
        pool.schedule(t);
    }
    ASSERT_TRUE(wait_until([&] { return counters.fired.load() == kPollers * kPerPoller; }));
    pool.stop();
    // Consecutive tickets of one thread: exactly kPerPoller per poller.
    for (std::size_t i = 0; i < kPollers; ++i) {
        EXPECT_EQ(pool.at(i).stats().timers_fired, kPerPoller) << "poller " << i;
    }
    EXPECT_EQ(pool.stats().timers_fired, kPollers * kPerPoller);
}

TEST(PollerPool, TimerRemembersItsPoller) {
    InlineSink sink;
    Counters counters;
    Timer t(&counters, &count_event);
    EXPECT_EQ(t.poller(), nullptr);
    Pool pool(TaskSink::of(sink), {.num_pollers = 3});
    EXPECT_FALSE(pool.cancel(t));  // never scheduled
    t.deadline = pool.at(0).now() + 1h;
    pool.schedule(t);
    bool found = false;
    for (std::size_t i = 0; i < pool.size(); ++i) {
        found = found || t.poller() == &pool.at(i);
    }
    EXPECT_TRUE(found);
    EXPECT_TRUE(pool.cancel(t));
    EXPECT_FALSE(pool.cancel(t));  // already cancelled
    pool.start();
    ASSERT_TRUE(wait_until([&] { return counters.cancelled.load() == 1; }));
    pool.stop();
    EXPECT_EQ(counters.fired.load(), 0u);
}

TEST(PollerPool, CancelReachesTheOwningPoller) {
    constexpr std::size_t kTimers = 400;
    InlineSink sink;
    Counters counters;
    std::vector<std::unique_ptr<Timer>> timers;
    {
        Pool pool(TaskSink::of(sink), {.num_pollers = 4});
        pool.start();
        for (std::size_t i = 0; i < kTimers; ++i) {
            auto& t = *timers.emplace_back(std::make_unique<Timer>(&counters, &count_event));
            t.deadline = pool.at(0).now() + 1h;
            pool.schedule(t);
        }
        std::size_t cancelled = 0;
        for (auto& t : timers) {
            cancelled += pool.cancel(*t) ? 1 : 0;
        }
        EXPECT_EQ(cancelled, kTimers);
        // ~PollerPool hands every node back with kCancelled.
    }
    EXPECT_EQ(counters.cancelled.load(), kTimers);
    EXPECT_EQ(counters.fired.load(), 0u);
    for (auto& t : timers) {
        EXPECT_TRUE(t->idle());
    }
}

TEST(PollerPool, ManyThreadsEachTimerFiresOnce) {
    constexpr std::size_t kThreads = 4;
    constexpr std::size_t kPerThread = 2000;
    InlineSink sink;
    Counters counters;
    std::vector<std::unique_ptr<Timer>> timers(kThreads * kPerThread);
    for (auto& t : timers) {
        t = std::make_unique<Timer>(&counters, &count_event);
    }
    Pool pool(TaskSink::of(sink), {.num_pollers = 3});
    pool.start();
    std::vector<std::thread> threads;
    for (std::size_t th = 0; th < kThreads; ++th) {
        threads.emplace_back([&, th] {
            for (std::size_t i = 0; i < kPerThread; ++i) {
                Timer& t = *timers[th * kPerThread + i];
                t.deadline = pool.at(0).now() + std::chrono::microseconds(i % 500);
                pool.schedule(t);
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    ASSERT_TRUE(wait_until([&] { return counters.fired.load() == kThreads * kPerThread; }));
    pool.stop();
    EXPECT_EQ(counters.cancelled.load(), 0u);
    std::size_t used = 0;
    for (std::size_t i = 0; i < pool.size(); ++i) {
        used += pool.at(i).stats().timers_fired > 0 ? 1 : 0;
    }
    EXPECT_EQ(used, pool.size());
}

TEST(PollerPool, FdsOnChosenPollers) {
    constexpr std::size_t kPollers = 3;
    InlineSink sink;
    struct Source {
        int fd = poller::make_eventfd();
        std::atomic<std::size_t> edges{0};
        Epollable e{fd, this, [](void* memo, std::uint32_t events, TaskBatch&) noexcept {
                        auto* self = static_cast<Source*>(memo);
                        if ((events & EPOLLIN) == 0) return;
                        poller::eventfd_drain(self->fd);
                        self->edges.fetch_add(1);
                    }};
        ~Source() { close(fd); }
    };
    std::vector<std::unique_ptr<Source>> sources;
    Pool pool(TaskSink::of(sink), {.num_pollers = kPollers});
    std::vector<Pool::PollerType*> owners;
    for (std::size_t i = 0; i < 2 * kPollers; ++i) {
        auto& s = *sources.emplace_back(std::make_unique<Source>());
        Pool::PollerType& p = pool.next();
        ASSERT_EQ(p.watch(s.e), 0);
        owners.push_back(&p);
    }
    pool.start();
    for (auto& s : sources) {
        poller::eventfd_signal(s->fd);
    }
    ASSERT_TRUE(wait_until([&] {
        for (auto& s : sources) {
            if (s->edges.load() == 0) return false;
        }
        return true;
    }));
    pool.stop();
    // Round robin from one thread: every poller got two sources.
    for (std::size_t i = 0; i < kPollers; ++i) {
        std::size_t n = 0;
        for (auto* o : owners) {
            n += o == &pool.at(i) ? 1 : 0;
        }
        EXPECT_EQ(n, 2u) << "poller " << i;
    }
    for (std::size_t i = 0; i < sources.size(); ++i) {
        EXPECT_EQ(owners[i]->unwatch(sources[i]->e), 0);  // stopped: allowed from here
    }
}

TEST(PollerPool, SinglePollerIsPlainPoller) {
    InlineSink sink;
    Counters counters;
    Timer t(&counters, &count_event);
    Pool pool(TaskSink::of(sink));
    EXPECT_EQ(pool.size(), 1u);
    EXPECT_EQ(&pool.next(), &pool.at(0));
    pool.start();
    pool.post(t);
    ASSERT_TRUE(wait_until([&] { return counters.fired.load() == 1; }));
    pool.stop();
}

TEST(PollerPool, FakeBackend) {
    InlineSink sink;
    Counters counters;
    Timer a(&counters, &count_event), b(&counters, &count_event);
    poller::PollerPool<poller::FakeBackend<>> pool(TaskSink::of(sink), {.num_pollers = 2});
    pool.start();
    a.deadline = pool.at(0).now() + 1ms;
    b.deadline = pool.at(1).now() + 1ms;
    pool.schedule(a);
    pool.schedule(b);
    EXPECT_NE(a.poller(), b.poller());
    ASSERT_TRUE(wait_until([&] { return counters.fired.load() == 2; }));
    pool.stop();
}
