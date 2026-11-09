// CpuList parsing and thread pinning of ThreadPool, PollerPool, BlockingPool.

#include <gtest/gtest.h>
#include <sched.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "fibers/blocking/blocking_pool.hpp"
#include "fibers/blocking/ops.hpp"
#include "fibers/poller/poller_pool.hpp"
#include "fibers/sync/affinity.hpp"
#include "fibers/thread_pool/thread_pool.hpp"

namespace {

using fibers::sync::CpuList;
using namespace std::chrono_literals;

// CPUs this process may run on (the test pins only to those).
std::vector<int> allowed_cpus() {
    cpu_set_t set;
    CPU_ZERO(&set);
    std::vector<int> cpus;
    if (sched_getaffinity(0, sizeof set, &set) == 0) {
        for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
            if (CPU_ISSET(cpu, &set)) {
                cpus.push_back(cpu);
            }
        }
    }
    return cpus;
}

bool wait_for(const std::atomic<int>& value, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (value.load() != expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(100us);
    }
    return true;
}

TEST(CpuList, Parse) {
    CpuList list;
    ASSERT_TRUE(CpuList::parse("0,2,4-6,9", list));
    ASSERT_EQ(list.size(), 6u);
    EXPECT_EQ(list[0], 0);
    EXPECT_EQ(list[1], 2);
    EXPECT_EQ(list[2], 4);
    EXPECT_EQ(list[4], 6);
    EXPECT_EQ(list[5], 9);
    EXPECT_EQ(list.for_thread(7), 2);  // 7 % 6 = 1
    ASSERT_TRUE(CpuList::parse("", list));
    EXPECT_TRUE(list.empty());
    EXPECT_EQ(list.for_thread(3), -1);
    EXPECT_FALSE(CpuList::parse("3-1", list));
    EXPECT_FALSE(CpuList::parse("a", list));
    EXPECT_FALSE(CpuList::parse("1;2", list));
    EXPECT_FALSE(CpuList::parse("0-200", list));  // more than kCapacity
    const CpuList init{3, 1};
    EXPECT_EQ(init.size(), 2u);
    EXPECT_EQ(init.for_thread(1), 1);
}

TEST(Affinity, ThreadPoolWorkers) {
    const std::vector<int> cpus = allowed_cpus();
    ASSERT_FALSE(cpus.empty());
    const int cpu = cpus.back();
    fibers::thread_pool::ThreadPoolOptions options;
    options.num_workers = 2;
    options.cpus = CpuList{cpu};
    fibers::thread_pool::ThreadPool<> pool(options);
    std::atomic<int> done{0};
    std::atomic<int> wrong{0};
    struct Probe {
        std::atomic<int>* done;
        std::atomic<int>* wrong;
        int cpu;
    } probe{&done, &wrong, cpu};
    for (int i = 0; i < 64; ++i) {
        pool.submit(fibers::thread_pool::Task(
            &probe,
            [](void* memo) noexcept {
                auto* p = static_cast<Probe*>(memo);
                if (sched_getcpu() != p->cpu) {
                    p->wrong->fetch_add(1);
                }
                p->done->fetch_add(1);
            },
            [](void*) noexcept {}));
    }
    ASSERT_TRUE(wait_for(done, 64));
    EXPECT_EQ(wrong.load(), 0);
    pool.stop();
}

struct TimerMemo {
    std::atomic<int>* seen;
    std::atomic<int>* fired;
};

TEST(Affinity, PollerPoolAndBlockingPool) {
    const std::vector<int> cpus = allowed_cpus();
    ASSERT_FALSE(cpus.empty());
    const int first = cpus.front();
    const int second = cpus.size() > 1 ? cpus[1] : cpus.front();

    fibers::thread_pool::ThreadPoolOptions sink_options;
    sink_options.num_workers = 1;
    fibers::thread_pool::ThreadPool<> sink_pool(sink_options);
    fibers::poller::PollerPoolOptions poller_options;
    poller_options.num_pollers = 2;
    poller_options.cpus = CpuList{first, second};
    fibers::poller::PollerPool<> pollers(fibers::poller::TaskSink::of(sink_pool), poller_options);
    pollers.start();
    std::atomic<int> seen[2] = {-1, -1};
    std::atomic<int> fired{0};
    TimerMemo memos[2] = {{&seen[0], &fired}, {&seen[1], &fired}};
    fibers::poller::Timer timers[2];
    for (std::size_t i = 0; i < 2; ++i) {
        timers[i].memo = &memos[i];
        timers[i].callback = [](void* memo, fibers::poller::TimerEvent, fibers::poller::TaskBatch&) noexcept {
            auto* m = static_cast<TimerMemo*>(memo);
            m->seen->store(sched_getcpu());
            m->fired->fetch_add(1);
        };
        pollers.at(i).post(timers[i]);
    }
    ASSERT_TRUE(wait_for(fired, 2));
    EXPECT_EQ(seen[0].load(), first);
    EXPECT_EQ(seen[1].load(), second);
    pollers.stop();

    fibers::blocking::BlockingPoolOptions blocking_options;
    blocking_options.num_threads = 2;
    blocking_options.cpus = CpuList{second};
    fibers::blocking::BlockingPool<> blocking(blocking_options);
    std::atomic<int> where{-1};
    std::atomic<int> ran{0};
    fibers::blocking::CallOp op([&]() noexcept {
        where.store(sched_getcpu());
        ran.store(1);
    });
    blocking.submit(op);
    ASSERT_TRUE(wait_for(ran, 1));
    EXPECT_EQ(where.load(), second);
    blocking.stop();
    sink_pool.stop();
}

}  // namespace
