// Poller with the real OS backend (epoll / eventfd / CLOCK_MONOTONIC).

#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include "fibers/object_pool/sharded_pool.hpp"
#include "fibers/poller/eventfd.hpp"
#include "fibers/poller/fake_backend.hpp"
#include "fibers/poller/poller.hpp"
#include "fibers/thread_pool/thread_pool.hpp"

namespace poller = fibers::poller;

namespace {

using namespace std::chrono_literals;
using poller::Checkable;
using poller::Epollable;
using poller::Task;
using poller::TaskBatch;
using poller::TaskSink;
using poller::TimerEvent;
using poller::TimePoint;
using Clock = std::chrono::steady_clock;

// Runs submitted tasks right away (on the poller thread).
struct InlineSink {
    std::atomic<std::size_t> batches{0};
    void submit_batch(Task* tasks, std::size_t count) noexcept {
        batches.fetch_add(1, std::memory_order_relaxed);
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

// CPU time (user + system) of the whole process.
std::chrono::microseconds process_cpu() {
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    auto tv = [](const timeval& t) {
        return std::chrono::seconds(t.tv_sec) + std::chrono::microseconds(t.tv_usec);
    };
    return tv(usage.ru_utime) + tv(usage.ru_stime);
}

// CPU used by the process while the calling thread sleeps for `window`.
std::chrono::microseconds idle_cpu(std::chrono::milliseconds window = 200ms) {
    const auto before = process_cpu();
    std::this_thread::sleep_for(window);
    return process_cpu() - before;
}

#if defined(__SANITIZE_THREAD__) || defined(__SANITIZE_ADDRESS__)
constexpr bool kSanitized = true;
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer) || __has_feature(address_sanitizer)
constexpr bool kSanitized = true;
#else
constexpr bool kSanitized = false;
#endif
#else
constexpr bool kSanitized = false;
#endif

// Generous bounds: tests run in parallel on a noisy desktop and under sanitizers.
constexpr auto kIdleCpuBound = 20ms;  // of a 200 ms window
constexpr auto kLatenessBound = kSanitized ? 100ms : 20ms;

std::atomic<std::size_t>& as_counter(void* memo) { return *static_cast<std::atomic<std::size_t>*>(memo); }

void count_task(void* memo) noexcept { as_counter(memo).fetch_add(1); }

using Poller = poller::Poller<>;
using Timer = poller::Timer;

// An eventfd watched by the poller; each edge counts and records the mask.
struct EventFdSource {
    int fd = poller::make_eventfd();
    std::atomic<std::size_t> edges{0};
    std::atomic<std::uint32_t> last_events{0};
    Epollable epollable{fd, this, [](void* memo, std::uint32_t events, TaskBatch& out) noexcept {
                            auto* self = static_cast<EventFdSource*>(memo);
                            // EPOLLOUT-only edges: the eventfd is (still) writable.
                            if ((events & EPOLLIN) == 0) return;
                            self->last_events.store(events);
                            poller::eventfd_drain(self->fd);
                            out.push(Task(&self->edges, &count_task, nullptr));
                        }};
    ~EventFdSource() { close(fd); }
};

}  // namespace

TEST(Poller, EventFdReadinessRunsCallback) {
    InlineSink sink;
    Poller p(TaskSink::of(sink));
    EventFdSource source;
    ASSERT_EQ(p.watch(source.epollable), 0);
    p.start();
    for (std::size_t i = 1; i <= 5; ++i) {
        poller::eventfd_signal(source.fd);
        ASSERT_TRUE(wait_until([&] { return source.edges.load() >= i; })) << i;
    }
    EXPECT_NE(source.last_events.load() & EPOLLIN, 0u);
    p.stop();
    EXPECT_EQ(p.unwatch(source.epollable), 0);
}

TEST(Poller, PipeReadiness) {
    InlineSink sink;
    Poller p(TaskSink::of(sink));
    int fds[2];
    ASSERT_EQ(pipe2(fds, O_NONBLOCK | O_CLOEXEC), 0);
    struct Reader {
        int fd;
        std::atomic<std::size_t> bytes{0};
    } reader{fds[0]};
    Epollable e{fds[0], &reader, [](void* memo, std::uint32_t, TaskBatch&) noexcept {
                    auto* r = static_cast<Reader*>(memo);
                    char buf[64];
                    for (;;) {  // edge-triggered: read until EAGAIN
                        const ssize_t n = read(r->fd, buf, sizeof(buf));
                        if (n <= 0) break;
                        r->bytes.fetch_add(static_cast<std::size_t>(n));
                    }
                }};
    ASSERT_EQ(p.watch(e, EPOLLIN | EPOLLET), 0);
    p.start();
    ASSERT_EQ(write(fds[1], "abc", 3), 3);
    ASSERT_TRUE(wait_until([&] { return reader.bytes.load() == 3; }));
    ASSERT_EQ(write(fds[1], "defgh", 5), 5);
    ASSERT_TRUE(wait_until([&] { return reader.bytes.load() == 8; }));
    p.stop();
    close(fds[0]);
    close(fds[1]);
}

// Echo server on one end of a socketpair: readiness -> Task on a ThreadPool,
// which reads until EAGAIN and writes back (the fiber pattern, minus fibers).
TEST(Poller, SocketpairEchoThroughThreadPool) {
    fibers::thread_pool::ThreadPool<> pool(fibers::thread_pool::ThreadPoolOptions{.num_workers = 1});
    Poller p(TaskSink::of(pool));
    int sv[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sv), 0);
    struct Server {
        int fd;
        std::atomic<std::size_t> echoed{0};
        static void echo(void* memo) noexcept {
            auto* s = static_cast<Server*>(memo);
            char buf[256];
            for (;;) {
                const ssize_t n = read(s->fd, buf, sizeof(buf));
                if (n <= 0) break;  // EAGAIN: wait for the next edge
                ssize_t off = 0;
                while (off < n) {
                    const ssize_t w = write(s->fd, buf + off, static_cast<std::size_t>(n - off));
                    if (w > 0) off += w;
                }
                s->echoed.fetch_add(static_cast<std::size_t>(n));
            }
        }
    } server{sv[1]};
    Epollable e{sv[1], &server, [](void* memo, std::uint32_t events, TaskBatch& out) noexcept {
                    if (events & EPOLLIN) out.push(Task(memo, &Server::echo, nullptr));
                }};
    ASSERT_EQ(p.watch(e), 0);
    p.start();

    std::string received;
    for (const char* message : {"hello", "poller", "echo!"}) {
        const std::size_t len = std::strlen(message);
        ASSERT_EQ(write(sv[0], message, len), static_cast<ssize_t>(len));
        std::string got;
        ASSERT_TRUE(wait_until([&] {
            char buf[64];
            const ssize_t n = read(sv[0], buf, sizeof(buf));
            if (n > 0) got.append(buf, static_cast<std::size_t>(n));
            return got.size() >= len;
        }));
        EXPECT_EQ(got, message);
    }
    p.stop();
    pool.stop();
    close(sv[0]);
    close(sv[1]);
}

namespace {

struct OrderLog {
    std::mutex mutex;
    std::vector<std::pair<int, std::chrono::nanoseconds>> fired;  // (index, lateness)
    std::atomic<std::size_t> cancelled{0};
};

struct OrderedTimer {
    Timer timer;
    OrderLog* log = nullptr;
    int index = 0;
    static void callback(void* memo, TimerEvent event, TaskBatch&) noexcept {
        auto* self = static_cast<OrderedTimer*>(memo);
        if (event == TimerEvent::kCancelled) {
            self->log->cancelled.fetch_add(1);
            return;
        }
        const auto lateness = Clock::now() - self->timer.deadline;  // node is ours again
        std::lock_guard lock(self->log->mutex);
        self->log->fired.emplace_back(self->index, lateness);
    }
};

}  // namespace

TEST(Poller, TimersFireInDeadlineOrderWithBoundedLateness) {
    InlineSink sink;
    constexpr int kTimers = 60;
    OrderLog log;
    std::vector<OrderedTimer> timers(kTimers);  // timers outlive the poller that owns them
    Poller p(TaskSink::of(sink));
    p.start();
    std::vector<int> offsets_ms(kTimers);
    for (int i = 0; i < kTimers; ++i) offsets_ms[i] = 5 + i;  // distinct deadlines 5..64 ms
    std::shuffle(offsets_ms.begin(), offsets_ms.end(), std::mt19937(7));
    const TimePoint base = p.now();
    for (int i = 0; i < kTimers; ++i) {
        auto& t = timers[i];
        t.log = &log;
        t.index = offsets_ms[i];
        t.timer.memo = &t;
        t.timer.callback = &OrderedTimer::callback;
        t.timer.deadline = base + std::chrono::milliseconds(offsets_ms[i]);
        p.schedule(t.timer);
    }
    ASSERT_TRUE(wait_until([&] {
        std::lock_guard lock(log.mutex);
        return log.fired.size() == kTimers;
    }));
    p.stop();
    std::vector<std::chrono::nanoseconds> lateness;
    for (std::size_t i = 0; i < log.fired.size(); ++i) {
        EXPECT_EQ(log.fired[i].first, 5 + static_cast<int>(i)) << "fired out of order";
        EXPECT_GE(log.fired[i].second.count(), 0) << "fired early";
        lateness.push_back(log.fired[i].second);
    }
    std::sort(lateness.begin(), lateness.end());
    const auto median = lateness[lateness.size() / 2];
    const auto max = lateness.back();
    std::printf("[ lateness ] median %.1f us, max %.1f us over %d timers\n", median.count() / 1e3,
                max.count() / 1e3, kTimers);
    RecordProperty("lateness_median_ns", std::to_string(median.count()));
    RecordProperty("lateness_max_ns", std::to_string(max.count()));
    EXPECT_LT(median, kLatenessBound);
    for (auto& t : timers) EXPECT_TRUE(t.timer.idle());
}

TEST(Poller, EarlierDeadlineWakesSleepingPoller) {
    InlineSink sink;
    OrderLog log;
    OrderedTimer far, near, later;  // outlive the poller
    Poller p(TaskSink::of(sink));
    p.start();
    for (auto* t : {&far, &near}) {
        t->log = &log;
        t->timer.memo = t;
        t->timer.callback = &OrderedTimer::callback;
    }
    far.index = 1;
    near.index = 2;
    far.timer.deadline = p.now() + 1h;
    p.schedule(far.timer);
    std::this_thread::sleep_for(20ms);  // the poller now sleeps until +1h
    const auto wakeups_before = p.stats().timer_wakeups;
    near.timer.deadline = p.now() + 5ms;
    p.schedule(near.timer);
    ASSERT_TRUE(wait_until([&] {
        std::lock_guard lock(log.mutex);
        return !log.fired.empty();
    }, 2s));
    EXPECT_EQ(log.fired[0].first, 2);
    EXPECT_LT(log.fired[0].second, kLatenessBound);
    EXPECT_GE(p.stats().timer_wakeups, wakeups_before + 1);
    // A later deadline does not wake it.
    later.log = &log;
    later.timer.memo = &later;
    later.timer.callback = &OrderedTimer::callback;
    later.timer.deadline = p.now() + 2h;
    std::this_thread::sleep_for(10ms);
    const auto wakeups = p.stats().timer_wakeups;
    p.schedule(later.timer);
    std::this_thread::sleep_for(10ms);
    EXPECT_EQ(p.stats().timer_wakeups, wakeups);
    EXPECT_TRUE(p.cancel(far.timer));
    EXPECT_TRUE(p.cancel(later.timer));
    p.stop();
}  // ~Poller hands both back with kCancelled

TEST(Poller, Cancellation) {
    InlineSink sink;
    OrderLog log;
    OrderedTimer a, b, c, poke;  // outlive the poller
    Poller p(TaskSink::of(sink));
    for (auto* t : {&a, &b, &c, &poke}) {
        t->log = &log;
        t->timer.memo = t;
        t->timer.callback = &OrderedTimer::callback;
    }
    a.index = 1;
    b.index = 2;
    c.index = 3;
    poke.index = 4;
    // Cancelled while still in the MPSC queue (poller not running yet).
    c.timer.deadline = p.now() + 10ms;
    p.schedule(c.timer);
    EXPECT_TRUE(p.cancel(c.timer));
    EXPECT_FALSE(p.cancel(c.timer)) << "second cancel";
    p.start();
    ASSERT_TRUE(wait_until([&] { return log.cancelled.load() == 1; }));
    EXPECT_TRUE(c.timer.idle());

    // Cancelled while in the heap: delivered by compaction on the next iteration.
    a.timer.deadline = p.now() + 1h;
    b.timer.deadline = p.now() + 30ms;
    p.schedule(a.timer);
    p.schedule(b.timer);
    std::this_thread::sleep_for(5ms);
    EXPECT_TRUE(p.cancel(a.timer));
    p.post(poke.timer);  // wake it up so it notices
    ASSERT_TRUE(wait_until([&] { return log.cancelled.load() == 2; }, 2s));
    EXPECT_TRUE(a.timer.idle());
    ASSERT_TRUE(wait_until([&] {
        std::lock_guard lock(log.mutex);
        return log.fired.size() == 2;  // poke, b
    }));
    EXPECT_FALSE(p.cancel(b.timer)) << "already fired";
    p.stop();
    std::lock_guard lock(log.mutex);
    for (auto& [index, lateness] : log.fired) {
        EXPECT_NE(index, 1);
        EXPECT_NE(index, 3);
    }
    EXPECT_GE(p.stats().compactions, 1u);
}

// TimerHeap::erase (behind Poller::withdraw) keeps the heap consistent:
// random pushes, pops and erases against a sorted reference.
TEST(TimerHeap, EraseAnywhere) {
    std::mt19937_64 rng(7);
    constexpr std::size_t kNodes = 2000;
    std::vector<Timer> nodes(kNodes);
    poller::TimerHeap<Timer> heap;
    std::vector<std::size_t> in;  // indices in the heap
    auto deadline_of = [&](std::size_t i) { return nodes[i].deadline; };
    for (int step = 0; step < 20000; ++step) {
        const int op = static_cast<int>(rng() % 7);
        if (op == 6 && !in.empty()) {
            // The compaction path: remove_if re-melds the survivors.
            const std::uint64_t mod = 5 + rng() % 7;
            const std::uint64_t rem = rng() % mod;
            auto doomed = [&](const Timer* t) {
                return static_cast<std::uint64_t>(t->deadline.time_since_epoch().count()) % mod == rem;
            };
            std::size_t removed = 0;
            heap.remove_if([&](Timer* t) { return doomed(t); }, [&](Timer*) { ++removed; });
            const std::size_t before = in.size();
            std::erase_if(in, [&](std::size_t i) { return doomed(&nodes[i]); });
            ASSERT_EQ(before - in.size(), removed);
            for (std::size_t i : in) {
                ASSERT_TRUE(poller::TimerHeap<Timer>::contains(&nodes[i]));
            }
            ASSERT_EQ(heap.size(), in.size());
            continue;
        }
        if (op <= 1 || in.empty()) {
            std::size_t i = rng() % kNodes;
            if (poller::TimerHeap<Timer>::contains(&nodes[i])) {
                continue;
            }
            nodes[i].deadline = TimePoint(std::chrono::nanoseconds(rng() % 1000));
            heap.push(&nodes[i]);
            in.push_back(i);
        } else if (op <= 3) {
            Timer* top = heap.pop();
            const auto it = std::min_element(in.begin(), in.end(),
                                              [&](std::size_t a, std::size_t b) { return deadline_of(a) < deadline_of(b); });
            ASSERT_EQ(top->deadline, deadline_of(*it));
            in.erase(std::find(in.begin(), in.end(), static_cast<std::size_t>(top - nodes.data())));
        } else {
            const std::size_t k = rng() % in.size();
            heap.erase(&nodes[in[k]]);
            ASSERT_FALSE(poller::TimerHeap<Timer>::contains(&nodes[in[k]]));
            in.erase(in.begin() + static_cast<std::ptrdiff_t>(k));
        }
        ASSERT_EQ(heap.size(), in.size());
    }
    TimePoint last = TimePoint::min();
    while (!heap.empty()) {
        Timer* top = heap.pop();
        ASSERT_GE(top->deadline, last);
        last = top->deadline;
    }
}

namespace {

struct WithdrawProbe {
    Poller* poller = nullptr;
    Timer* target = nullptr;
    std::atomic<int> result{-1};
};

void count_event(void* memo, TimerEvent, TaskBatch&) noexcept { as_counter(memo).fetch_add(1); }

}  // namespace

// withdraw() from a callback takes a pending timer back: it never fires and
// the destructor does not hand it back either.
TEST(Poller, WithdrawFromCallback) {
    InlineSink sink;
    std::atomic<std::size_t> far_events{0};
    Timer far;
    Timer remover;
    WithdrawProbe probe;
    {
        Poller p(TaskSink::of(sink));
        probe.poller = &p;
        probe.target = &far;
        far.memo = &far_events;
        far.callback = &count_event;
        far.deadline = p.now() + 1h;
        p.start();
        p.schedule(far);
        remover.memo = &probe;
        remover.callback = [](void* memo, TimerEvent, TaskBatch&) noexcept {
            auto* pr = static_cast<WithdrawProbe*>(memo);
            const bool first = pr->poller->withdraw(*pr->target);
            const bool second = pr->poller->withdraw(*pr->target);
            pr->result.store((first ? 1 : 0) | (second ? 2 : 0));
        };
        p.post(remover);
        ASSERT_TRUE(wait_until([&] { return probe.result.load() != -1; }));
        EXPECT_EQ(probe.result.load(), 1) << "withdrawn once, then idle";
        EXPECT_TRUE(far.idle());
        p.stop();
    }
    EXPECT_EQ(far_events.load(), 0u) << "a withdrawn timer gets no callback, not even kCancelled";
}

// withdraw() of a timer that has already fired, and of one that was never
// scheduled: false, and no second callback.
TEST(Poller, WithdrawFiredOrNeverScheduled) {
    InlineSink sink;
    std::atomic<std::size_t> events{0};
    Timer fired;
    Timer never;
    Timer remover;
    WithdrawProbe probe[2];
    Poller p(TaskSink::of(sink));
    p.start();
    fired.memo = &events;
    fired.callback = &count_event;
    p.post(fired);
    ASSERT_TRUE(wait_until([&] { return events.load() == 1; }));
    for (int i = 0; i < 2; ++i) {
        probe[i].poller = &p;
        probe[i].target = i == 0 ? &fired : &never;
        remover.memo = &probe[i];
        remover.callback = [](void* memo, TimerEvent, TaskBatch&) noexcept {
            auto* pr = static_cast<WithdrawProbe*>(memo);
            pr->result.store(pr->poller->withdraw(*pr->target) ? 1 : 0);
        };
        ASSERT_TRUE(wait_until([&] { return remover.idle(); }));
        p.post(remover);
        ASSERT_TRUE(wait_until([&] { return probe[i].result.load() != -1; }));
        EXPECT_EQ(probe[i].result.load(), 0);
    }
    p.stop();
    EXPECT_EQ(events.load(), 1u);
}

namespace {

struct InTransitProbe {
    Poller* poller = nullptr;
    Timer* target = nullptr;
    std::atomic<int> withdrawn{0};
    std::atomic<int> failed{0};
    std::atomic<int> runs{0};
};

}  // namespace

// withdraw() of a timer scheduled just before (still in the MPSC queue,
// maybe behind other threads' pushes that are in transit) while producers
// keep scheduling far timers: it must find the node (spinning until the
// queue links it) every time.
TEST(Poller, WithdrawWhileOthersSchedule) {
    InlineSink sink;
    std::atomic<std::size_t> far_events{0};
    constexpr int kProducers = 3;
    constexpr int kPerProducer = 3000;
    constexpr int kRounds = 2000;
    std::vector<Timer> far(kProducers * kPerProducer);
    Timer target;
    Timer remover;
    std::atomic<std::size_t> target_events{0};
    InTransitProbe probe;
    {
        Poller p(TaskSink::of(sink));
        p.start();
        probe.poller = &p;
        probe.target = &target;
        target.memo = &target_events;
        target.callback = &count_event;
        remover.memo = &probe;
        remover.callback = [](void* memo, TimerEvent, TaskBatch&) noexcept {
            auto* pr = static_cast<InTransitProbe*>(memo);
            (pr->poller->withdraw(*pr->target) ? pr->withdrawn : pr->failed).fetch_add(1);
            pr->runs.fetch_add(1);
        };
        std::vector<std::thread> producers;
        for (int t = 0; t < kProducers; ++t) {
            producers.emplace_back([&, t] {
                for (int i = 0; i < kPerProducer; ++i) {
                    Timer& node = far[static_cast<std::size_t>(t * kPerProducer + i)];
                    node.memo = &far_events;
                    node.callback = &count_event;
                    node.deadline = p.now() + 1h;
                    p.schedule(node);
                }
            });
        }
        for (int round = 0; round < kRounds; ++round) {
            target.deadline = p.now() + 1h;
            p.schedule(target);
            p.post(remover);  // its callback withdraws `target`
            ASSERT_TRUE(wait_until([&] { return probe.runs.load() == round + 1 && remover.idle(); }));
            ASSERT_TRUE(target.idle());
        }
        for (auto& t : producers) {
            t.join();
        }
        p.stop();
    }  // ~Poller hands the far timers back (kCancelled), not `target`
    EXPECT_EQ(probe.withdrawn.load(), kRounds);
    EXPECT_EQ(probe.failed.load(), 0);
    EXPECT_EQ(target_events.load(), 0u);
    EXPECT_EQ(far_events.load(), static_cast<std::size_t>(kProducers * kPerProducer));
}

namespace {

// Timer nodes from an object pool, returned to it by their callback.
struct PooledTimer {
    Timer timer;
    void* pool = nullptr;  // TimerPool*
    std::atomic<std::size_t>* fired = nullptr;
    std::atomic<std::size_t>* cancelled = nullptr;
    static void callback(void* memo, TimerEvent event, TaskBatch&) noexcept;
};

using TimerPool = fibers::object_pool::DefaultPool<PooledTimer>;

void PooledTimer::callback(void* memo, TimerEvent event, TaskBatch&) noexcept {
    auto* self = static_cast<PooledTimer*>(memo);
    (event == TimerEvent::kFired ? self->fired : self->cancelled)->fetch_add(1);
    static_cast<TimerPool*>(self->pool)->release(self);  // the node is ours again
}

}  // namespace

TEST(Poller, ManyTimersFromManyThreads) {
    InlineSink sink;
    Poller p(TaskSink::of(sink));
    p.start();
    TimerPool pool;
    std::atomic<std::size_t> fired{0}, cancelled{0}, cancel_ok{0}, cancel_late{0};
    constexpr std::size_t kThreads = 8;
    constexpr std::size_t kPerThread = 3000;
    std::vector<std::thread> threads;
    for (std::size_t t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            std::mt19937 rng(static_cast<unsigned>(t));
            for (std::size_t i = 0; i < kPerThread; ++i) {
                PooledTimer* node = pool.acquire();
                node->pool = &pool;
                node->fired = &fired;
                node->cancelled = &cancelled;
                node->timer.memo = node;
                node->timer.callback = &PooledTimer::callback;
                node->timer.deadline = p.now() + std::chrono::microseconds(rng() % 2000);
                // Not cancelled: once scheduled, the node may fire, go back
                // to the pool and be reused by another thread at any moment.
                p.schedule(node->timer);
            }
            // Far-future timers (cannot fire before we cancel them), all
            // cancelled; reclaimed by compaction or on destruction.
            for (std::size_t i = 0; i < kPerThread / 10; ++i) {
                PooledTimer* node = pool.acquire();
                node->pool = &pool;
                node->fired = &fired;
                node->cancelled = &cancelled;
                node->timer.memo = node;
                node->timer.callback = &PooledTimer::callback;
                node->timer.deadline = p.now() + 1h;
                p.schedule(node->timer);
                (p.cancel(node->timer) ? cancel_ok : cancel_late).fetch_add(1);
            }
        });
    }
    for (auto& thread : threads) thread.join();
    constexpr std::size_t kNear = kThreads * kPerThread;
    constexpr std::size_t kFar = kThreads * (kPerThread / 10);
    ASSERT_TRUE(wait_until([&] { return fired.load() == kNear; }, 20s)) << fired.load();
    EXPECT_EQ(cancel_ok.load(), kFar);
    EXPECT_EQ(cancel_late.load(), 0u);
    // Cancelled far timers get reclaimed by compaction / on destruction.
    p.stop();
    EXPECT_EQ(fired.load(), kNear);
    std::printf("[ timers ] %zu fired, %zu cancelled reclaimed while running, %llu compactions\n",
                fired.load(), cancelled.load(),
                static_cast<unsigned long long>(p.stats().compactions));
}

TEST(Poller, StopWhileSleeping) {
    InlineSink sink;
    for (int variant = 0; variant < 3; ++variant) {
        Timer far(nullptr, [](void*, TimerEvent, TaskBatch&) noexcept {});  // outlives p
        Poller p(TaskSink::of(sink));
        int fd = poller::make_eventfd();
        Checkable armable{&fd, [](void*, TaskBatch&) noexcept -> std::size_t { return 0; },
                          [](void* memo) noexcept { return *static_cast<int*>(memo); }, nullptr};
        if (variant == 1) {
            far.deadline = p.now() + 1h;
            p.schedule(far);
        }
        if (variant == 2) p.add(armable);
        p.start();
        std::this_thread::sleep_for(20ms);
        const auto start = Clock::now();
        p.stop();
        EXPECT_LT(Clock::now() - start, 1s) << "variant " << variant;
        EXPECT_GE(p.stats().sleeps, 1u) << "variant " << variant;
        if (variant == 2) p.remove(armable);
        close(fd);
    }
}

TEST(Poller, IdleCpuWithOnlyEpoll) {
    InlineSink sink;
    Poller p(TaskSink::of(sink));
    EventFdSource source;
    ASSERT_EQ(p.watch(source.epollable), 0);
    p.start();
    std::this_thread::sleep_for(10ms);
    const auto cpu = idle_cpu();
    std::printf("[ idle cpu ] epoll only: %lld us / 200 ms\n", static_cast<long long>(cpu.count()));
    EXPECT_LT(cpu, kIdleCpuBound);
    poller::eventfd_signal(source.fd);
    EXPECT_TRUE(wait_until([&] { return source.edges.load() == 1; }));
    p.stop();
}

namespace {

// A Checkable implementing the arm protocol by hand (what ipc rings do):
// producer: pending++ ; fence ; if (sleeping) eventfd write
// consumer arm: sleeping = 1 ; re-check pending.
struct ArmableSource {
    int fd = poller::make_eventfd();
    std::atomic<std::size_t> pending{0};
    std::atomic<int> sleeping{0};
    std::atomic<std::size_t> handled{0};
    std::atomic<std::size_t> polls{0};
    std::atomic<std::size_t> arms{0};
    std::atomic<std::size_t> rings{0};

    Checkable checkable{this, &poll, &arm, &disarm};

    ~ArmableSource() { close(fd); }

    void produce() {
        pending.fetch_add(1, std::memory_order_release);
        std::atomic_thread_fence(std::memory_order_seq_cst);
        if (sleeping.load(std::memory_order_seq_cst) != 0) {
            rings.fetch_add(1);
            poller::eventfd_signal(fd);
        }
    }

    static std::size_t poll(void* memo, TaskBatch&) noexcept {
        auto* self = static_cast<ArmableSource*>(memo);
        self->polls.fetch_add(1, std::memory_order_relaxed);
        const std::size_t n = self->pending.exchange(0, std::memory_order_acquire);
        self->handled.fetch_add(n);
        return n;
    }
    static int arm(void* memo) noexcept {
        auto* self = static_cast<ArmableSource*>(memo);
        self->arms.fetch_add(1, std::memory_order_relaxed);
        self->sleeping.store(1, std::memory_order_seq_cst);
        if (self->pending.load(std::memory_order_seq_cst) != 0) {
            self->sleeping.store(0, std::memory_order_relaxed);
            return Checkable::kArmWorkAppeared;
        }
        return self->fd;
    }
    static void disarm(void* memo) noexcept {
        auto* self = static_cast<ArmableSource*>(memo);
        self->sleeping.store(0, std::memory_order_relaxed);
        poller::eventfd_drain(self->fd);
    }
};

}  // namespace

TEST(Poller, IdleCpuWithArmableCheckables) {
    InlineSink sink;
    Poller p(TaskSink::of(sink), poller::PollerOptions{.spin_budget = 256});
    ArmableSource a, b;
    p.add(a.checkable);
    p.add(b.checkable);
    p.start();
    std::this_thread::sleep_for(20ms);
    const auto polls_before = a.polls.load();
    const auto cpu = idle_cpu();
    const auto polls = a.polls.load() - polls_before;
    std::printf("[ idle cpu ] 2 armable checkables: %lld us / 200 ms, %zu polls\n",
                static_cast<long long>(cpu.count()), polls);
    EXPECT_LT(cpu, kIdleCpuBound);
    EXPECT_LT(polls, 1000u) << "should be asleep, not polling";
    // Wake through the eventfd, many times.
    for (std::size_t i = 1; i <= 50; ++i) {
        (i % 2 ? a : b).produce();
        ASSERT_TRUE(wait_until([&] { return a.handled.load() + b.handled.load() == i; }, 5s)) << i;
        if (i % 10 == 0) std::this_thread::sleep_for(5ms);  // let it fall asleep again
    }
    EXPECT_GT(a.rings.load() + b.rings.load(), 0u) << "never woken through the eventfd";
    p.stop();
    p.remove(a.checkable);
    p.remove(b.checkable);
}

TEST(Poller, NonArmableCheckableBacksOff) {
    InlineSink sink;
    Poller p(TaskSink::of(sink), poller::PollerOptions{.spin_budget = 16,
                                                       .min_backoff = 50us,
                                                       .max_backoff = 2ms});
    ArmableSource source;
    source.checkable.arm = nullptr;  // cannot sleep on anything
    source.checkable.disarm = nullptr;
    p.add(source.checkable);
    p.start();
    std::this_thread::sleep_for(20ms);
    const auto polls_before = source.polls.load();
    const auto sleeps_before = p.stats().sleeps;
    const auto cpu = idle_cpu();
    const auto polls = source.polls.load() - polls_before;
    const auto sleeps = p.stats().sleeps - sleeps_before;
    std::printf("[ backoff ] %zu polls, %llu sleeps, %lld us cpu / 200 ms\n", polls,
                static_cast<unsigned long long>(sleeps), static_cast<long long>(cpu.count()));
    // ~200 ms / 2 ms = 100 sleeps of 17 polls each at the cap.
    EXPECT_GT(sleeps, 30u);
    EXPECT_LT(sleeps, 400u);
    EXPECT_GT(polls, 30u);
    EXPECT_LT(polls, 20'000u);
    EXPECT_LT(cpu, 100ms);
    // Work is still noticed within about max_backoff.
    const auto start = Clock::now();
    source.produce();
    ASSERT_TRUE(wait_until([&] { return source.handled.load() == 1; }, 2s));
    EXPECT_LT(Clock::now() - start, kSanitized ? 200ms : 50ms);
    p.stop();
    p.remove(source.checkable);
}

TEST(Poller, SpinForeverStillRunsTimersAndFds) {
    InlineSink sink;
    std::atomic<std::size_t> fired{0};
    Timer t(&fired, [](void* memo, TimerEvent e, TaskBatch&) noexcept {
        if (e == TimerEvent::kFired) as_counter(memo).fetch_add(1);
    });
    Poller p(TaskSink::of(sink), poller::PollerOptions{.spin_budget = poller::kSpinForever,
                                                       .os_check_interval = 32});
    ArmableSource source;
    p.add(source.checkable);
    EventFdSource fd_source;
    ASSERT_EQ(p.watch(fd_source.epollable), 0);
    p.start();
    t.deadline = p.now() + 2ms;
    p.schedule(t);
    poller::eventfd_signal(fd_source.fd);
    source.produce();
    EXPECT_TRUE(wait_until([&] { return fired.load() == 1; }, 2s));
    EXPECT_TRUE(wait_until([&] { return fd_source.edges.load() == 1; }, 2s));
    EXPECT_TRUE(wait_until([&] { return source.handled.load() == 1; }, 2s));
    p.stop();
    EXPECT_EQ(p.stats().sleeps, 0u);
    p.remove(source.checkable);
}

// Two fds ready in the same epoll batch; whichever callback runs first
// unwatches the other one, whose event must then be dropped.
TEST(Poller, UnwatchFromCallbackScrubsPendingEvents) {
    InlineSink sink;
    Poller p(TaskSink::of(sink));
    struct Pair {
        Poller* poller;
        Epollable e[2];
        int fds[2];
        std::atomic<std::size_t> calls{0};
    } pair{};
    pair.poller = &p;
    for (int i = 0; i < 2; ++i) {
        pair.fds[i] = poller::make_eventfd();
        pair.e[i] = Epollable{pair.fds[i], &pair, [](void* memo, std::uint32_t, TaskBatch&) noexcept {
                                  auto* self = static_cast<Pair*>(memo);
                                  if (self->calls.fetch_add(1) == 0) {
                                      for (auto& e : self->e) self->poller->unwatch(e);
                                  }
                              }};
        poller::eventfd_signal(pair.fds[i]);
        ASSERT_EQ(p.watch(pair.e[i]), 0);
    }
    p.start();
    ASSERT_TRUE(wait_until([&] { return pair.calls.load() >= 1; }));
    std::this_thread::sleep_for(20ms);
    p.stop();
    EXPECT_EQ(pair.calls.load(), 1u);
    for (int fd : pair.fds) close(fd);
}

// post(): run something on the poller thread, e.g. register a Checkable while
// the poller is running.
TEST(Poller, PostRegistersCheckableAtRuntime) {
    InlineSink sink;
    Poller p(TaskSink::of(sink));
    p.start();
    std::this_thread::sleep_for(10ms);
    ArmableSource source;
    struct Ctx {
        Poller* p;
        ArmableSource* source;
        std::atomic<int> step{0};
    } ctx{&p, &source};
    Timer add(&ctx, [](void* memo, TimerEvent, TaskBatch&) noexcept {
        auto* c = static_cast<Ctx*>(memo);
        c->p->add(c->source->checkable);
        c->step.store(1);
    });
    p.post(add);
    ASSERT_TRUE(wait_until([&] { return ctx.step.load() == 1; }));
    source.produce();
    ASSERT_TRUE(wait_until([&] { return source.handled.load() == 1; }));
    Timer remove(&ctx, [](void* memo, TimerEvent, TaskBatch&) noexcept {
        auto* c = static_cast<Ctx*>(memo);
        c->p->remove(c->source->checkable);
        c->step.store(2);
    });
    p.post(remove);
    ASSERT_TRUE(wait_until([&] { return ctx.step.load() == 2; }));
    p.stop();
}

TEST(Poller, BatchIsFlushedOncePerIteration) {
    InlineSink sink;
    Poller p(TaskSink::of(sink));
    std::atomic<std::size_t> ran{0};
    constexpr int kTimers = 10;
    struct Node {
        Timer timer;
        std::atomic<std::size_t>* ran;
    } nodes[kTimers];
    const TimePoint deadline = p.now();  // all due at once
    for (auto& n : nodes) {
        n.ran = &ran;
        n.timer.memo = &n;
        n.timer.deadline = deadline;
        n.timer.callback = [](void* memo, TimerEvent, TaskBatch& out) noexcept {
            out.push(Task(static_cast<Node*>(memo)->ran, &count_task, nullptr));
        };
        p.schedule(n.timer);
    }
    p.start();
    ASSERT_TRUE(wait_until([&] { return ran.load() == kTimers; }));
    p.stop();
    EXPECT_EQ(sink.batches.load(), 1u) << "ten tasks, one submit_batch";
}

TEST(Poller, DestructorReleasesPendingTimers) {
    InlineSink sink;
    std::atomic<std::size_t> cancelled{0};
    Timer a(&cancelled, [](void* memo, TimerEvent e, TaskBatch&) noexcept {
        if (e == TimerEvent::kCancelled) as_counter(memo).fetch_add(1);
    });
    Timer b(&cancelled, a.callback);
    {
        Poller p(TaskSink::of(sink));
        a.deadline = p.now() + 1h;
        b.deadline = p.now() + 2h;
        p.schedule(a);
        p.start();
        p.schedule(b);
        std::this_thread::sleep_for(5ms);
    }
    EXPECT_EQ(cancelled.load(), 2u);
    EXPECT_TRUE(a.idle());
    EXPECT_TRUE(b.idle());
}

// The protocol logic with the fake backend, on real threads (StdSync).
TEST(Poller, FakeBackendTimersAndFds) {
    InlineSink sink;
    std::atomic<std::size_t> fired{0}, edges{0};
    poller::Timer t(&fired, [](void* memo, TimerEvent, TaskBatch&) noexcept { as_counter(memo).fetch_add(1); });
    poller::Timer t2(&fired, t.callback);  // both outlive the poller
    poller::Poller<poller::FakeBackend<>> p(TaskSink::of(sink));
    const int fd = p.backend().create_event();
    Epollable e{fd, &edges, [](void* memo, std::uint32_t, TaskBatch&) noexcept { as_counter(memo).fetch_add(1); }};
    ASSERT_EQ(p.watch(e), 0);
    p.start();
    t.deadline = p.now() + 1h;
    p.schedule(t);
    std::this_thread::sleep_for(5ms);
    t2.deadline = p.now() + 1ms;
    p.schedule(t2);
    ASSERT_TRUE(wait_until([&] { return fired.load() == 1; }));
    p.backend().signal(fd);
    ASSERT_TRUE(wait_until([&] { return edges.load() == 1; }));
    EXPECT_TRUE(p.cancel(t));
    p.stop();
}
