// Stackful coroutines (StdSync: MachineContext).

#include <gtest/gtest.h>

#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <cfenv>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "fibers/coro/coroutine.hpp"
#include "fibers/stack/stack.hpp"
#include "fibers/stack/stack_pool.hpp"

namespace coro = fibers::coro;
using coro::Coroutine;
using coro::ResumeResult;
using coro::Status;
using coro::SuspendContext;
using fibers::stack::Stack;

namespace {

// The running thread's id, re-read after every switch. std::this_thread::
// get_id() is pthread_self(), which glibc declares const: called directly in
// a fiber body, the compiler may hoist it out of a loop across a switch and
// keep reporting the thread the fiber started on. Same rule as the library's
// thread-local accessors: never inlined, never treated as pure.
[[gnu::noinline]] std::thread::id current_thread_id() {
    asm volatile("" ::: "memory");
    return std::this_thread::get_id();
}

std::vector<int> g_trace;  // only for the free-function body below

void free_body(SuspendContext ctx) noexcept {
    g_trace.push_back(1);
    ctx.suspend();
    g_trace.push_back(2);
}

TEST(Coroutine, FunctionBody) {
    g_trace.clear();
    Stack stack = Stack::allocate();
    Coroutine co(stack.view(), free_body);
    EXPECT_FALSE(co.started());
    EXPECT_TRUE(g_trace.empty());  // nothing runs before resume()
    ResumeResult r = co.resume();
    EXPECT_EQ(r.status, Status::kSuspended);
    EXPECT_FALSE(r.awaiter);
    EXPECT_EQ(g_trace, std::vector<int>{1});
    EXPECT_FALSE(co.done());
    r = co.resume();
    EXPECT_TRUE(r.completed());
    EXPECT_TRUE(co.done());
    EXPECT_EQ(g_trace, (std::vector<int>{1, 2}));
}

TEST(Coroutine, CaptureLessLambda) {
    Stack stack = Stack::allocate();
    Coroutine co(stack.view(), [](SuspendContext ctx) noexcept {
        for (int i = 0; i < 3; ++i) {
            ctx.suspend();
        }
    });
    int suspends = 0;
    while (!co.resume().completed()) {
        ++suspends;
    }
    EXPECT_EQ(suspends, 3);
}

TEST(Coroutine, LvalueCallableIsReferenced) {
    Stack stack = Stack::allocate();
    int steps = 0;
    auto body = [&steps](SuspendContext ctx) noexcept {
        for (int i = 0; i < 5; ++i) {
            ++steps;
            ctx.suspend();
        }
    };
    Coroutine co(stack.view(), body);
    for (int i = 1; i <= 5; ++i) {
        co.resume();
        EXPECT_EQ(steps, i);
    }
    EXPECT_TRUE(co.resume().completed());
}

// The awaiter is handed to the resumer, which acts on it after the
// coroutine's stack has stopped running.
struct RecordingAwaiter {
    const Stack* stack = nullptr;
    int calls = 0;
    bool called_off_stack = false;

    void await_suspend() noexcept {
        ++calls;
        const auto* frame = static_cast<const std::byte*>(__builtin_frame_address(0));
        const auto view = stack->view();
        called_off_stack = frame < view.data() || frame >= view.data() + view.size();
    }
};

TEST(Coroutine, AwaiterReturnedToResumer) {
    Stack stack = Stack::allocate();
    RecordingAwaiter* seen = nullptr;
    auto body = [&](SuspendContext ctx) noexcept {
        RecordingAwaiter awaiter{&stack};  // lives on the coroutine's stack
        seen = &awaiter;
        ctx.suspend(awaiter);
        EXPECT_EQ(awaiter.calls, 1);  // acted on before we were resumed
        EXPECT_TRUE(awaiter.called_off_stack);
    };
    Coroutine co(stack.view(), body);
    ResumeResult r = co.resume();
    ASSERT_EQ(r.status, Status::kSuspended);
    ASSERT_TRUE(r.awaiter);
    EXPECT_EQ(r.awaiter.address(), seen);
    r.awaiter.await_suspend();
    r = co.resume();
    EXPECT_TRUE(r.completed());
    EXPECT_FALSE(r.awaiter);
}

TEST(Coroutine, Nested) {
    Stack outer_stack = Stack::allocate();
    Stack inner_stack = Stack::allocate();
    std::string trace;
    auto inner = [&](SuspendContext ctx) noexcept {
        trace += 'a';
        ctx.suspend();
        trace += 'c';
    };
    auto outer = [&](SuspendContext ctx) noexcept {
        Coroutine co(inner_stack.view(), inner);
        co.resume();
        trace += 'b';
        ctx.suspend();
        co.resume();
        trace += 'd';
    };
    Coroutine co(outer_stack.view(), outer);
    co.resume();
    EXPECT_EQ(trace, "ab");
    co.resume();
    EXPECT_EQ(trace, "abcd");
    EXPECT_TRUE(co.done());
}

struct Counted {
    static inline int alive = 0;
    static inline int calls = 0;
    std::unique_ptr<int> payload = std::make_unique<int>(7);  // move-only

    Counted() noexcept { ++alive; }
    Counted(Counted&& other) noexcept : payload(std::move(other.payload)) { ++alive; }
    ~Counted() { --alive; }

    void operator()(SuspendContext ctx) noexcept {
        ++calls;
        ctx.suspend();
        *payload += 1;
    }
};

TEST(MakeCoroutine, ClosureOnTopOfStackDestroyedAtCompletion) {
    Counted::alive = 0;
    Counted::calls = 0;
    Stack stack = Stack::allocate();
    {
        Counted f;
        auto co = coro::make_coroutine(stack.view(), std::move(f));
        EXPECT_EQ(Counted::alive, 2);  // f (moved-from) and the closure
        co.resume();
        EXPECT_EQ(Counted::calls, 1);
        EXPECT_EQ(Counted::alive, 2);
        EXPECT_TRUE(co.resume().completed());
        EXPECT_EQ(Counted::alive, 1);  // closure gone at completion
    }
    EXPECT_EQ(Counted::alive, 0);
}

TEST(MakeCoroutine, NeverStartedDestroysClosure) {
    Counted::alive = 0;
    Stack stack = Stack::allocate();
    {
        auto co = coro::make_coroutine(stack.view(), Counted{});
        EXPECT_EQ(Counted::alive, 1);
    }
    EXPECT_EQ(Counted::alive, 0);
}

TEST(MakeCoroutine, ClosureIsAtTheTopOfTheStack) {
    Stack stack = Stack::allocate();
    const std::byte* closure_at = nullptr;
    struct Probe {
        const std::byte** out;
        void operator()(SuspendContext) noexcept { *out = reinterpret_cast<const std::byte*>(this); }
    };
    auto co = coro::make_coroutine(stack.view(), Probe{&closure_at});
    co.resume();
    const auto view = stack.view();
    EXPECT_GE(closure_at, view.data() + view.size() - 64);
    EXPECT_LT(closure_at, view.data() + view.size());
}

// Rounding mode set inside a coroutine does not leak into the resumer.
TEST(Coroutine, FloatingPointControlIsPerCoroutine) {
    Stack stack = Stack::allocate();
    auto co = coro::make_coroutine(stack.view(), [](SuspendContext ctx) noexcept {
        std::fesetround(FE_UPWARD);
        ctx.suspend();
        EXPECT_EQ(std::fegetround(), FE_UPWARD);
        std::fesetround(FE_TONEAREST);
    });
    co.resume();
    EXPECT_EQ(std::fegetround(), FE_TONEAREST);
    co.resume();
}

// Many short-lived coroutines on stacks from a pool.
TEST(Coroutine, ManyOnPooledStacks) {
    fibers::stack::StackPool<> pool({.stack_size = 16 * 1024, .max_stacks = 64});
    std::uint64_t sum = 0;
    for (int i = 0; i < 10'000; ++i) {
        Stack* stack = pool.acquire();
        {
            auto co = coro::make_coroutine(stack->view(), [&sum, i](SuspendContext ctx) noexcept {
                sum += static_cast<std::uint64_t>(i);
                ctx.suspend();
                sum += 1;
            });
            while (!co.resume().completed()) {
            }
        }
        pool.release(stack);
    }
    EXPECT_EQ(sum, 10'000ull * 9'999 / 2 + 10'000);
}

// Coroutines migrate: a tiny scheduler (locked deque) run by several
// threads. Each coroutine records the threads it ran on; the awaiter
// re-enqueues the coroutine only after it has suspended (the AwaitSuspend
// pattern: before that, another thread could resume a running coroutine).
TEST(Coroutine, MigratesBetweenThreads) {
    constexpr int kThreads = 4;
    constexpr int kCoroutines = 16;
    constexpr int kSteps = 500;

    struct Task;
    struct Scheduler {
        std::mutex lock;
        std::deque<Task*> queue;
        std::atomic<int> completed{0};

        void push(Task* t) {
            std::lock_guard guard(lock);
            queue.push_back(t);
        }
        Task* pop() {
            std::lock_guard guard(lock);
            if (queue.empty()) return nullptr;
            Task* t = queue.front();
            queue.pop_front();
            return t;
        }
    };
    struct Reschedule {
        Scheduler* scheduler;
        Task* task;
        void await_suspend() noexcept { scheduler->push(task); }
    };
    struct Task {
        Scheduler* scheduler = nullptr;
        Stack stack = Stack::allocate(32 * 1024);
        std::set<std::thread::id> threads;  // touched only by the coroutine
        std::uint64_t counter = 0;
        std::unique_ptr<Coroutine> co;
        std::thread::id last_resumer;  // touched only by the thread holding the task

        void body(SuspendContext ctx) noexcept {
            for (int i = 0; i < kSteps; ++i) {
                threads.insert(current_thread_id());
                ++counter;
                Reschedule r{scheduler, this};
                ctx.suspend(r);
            }
        }
    };

    Scheduler scheduler;
    std::vector<std::unique_ptr<Task>> tasks;
    for (int i = 0; i < kCoroutines; ++i) {
        auto task = std::make_unique<Task>();
        task->scheduler = &scheduler;
        Task* raw = task.get();
        task->co = std::make_unique<Coroutine>(
            raw->stack.view(),
            coro::Routine::erased(raw, [](void* self, SuspendContext ctx) noexcept {
                static_cast<Task*>(self)->body(ctx);
            }));
        scheduler.push(raw);
        tasks.push_back(std::move(task));
    }

    // Migration is forced, not left to chance: a thread never resumes the
    // coroutine it resumed last (it puts it back and yields). Otherwise, on a
    // loaded machine, one thread can do all 8'000 hand-overs (well under a
    // millisecond) inside a single time slice and nothing migrates.
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&] {
            const std::thread::id me = std::this_thread::get_id();
            while (scheduler.completed.load() < kCoroutines) {
                Task* task = scheduler.pop();
                if (task == nullptr) {
                    std::this_thread::yield();
                    continue;
                }
                if (task->last_resumer == me) {
                    scheduler.push(task);
                    std::this_thread::yield();
                    continue;
                }
                task->last_resumer = me;  // handed between threads via the queue's lock
                ResumeResult r = task->co->resume();
                if (r.completed()) {
                    scheduler.completed.fetch_add(1);
                } else {
                    r.awaiter.await_suspend();  // off the coroutine's stack
                }
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    std::size_t migrated = 0;
    for (auto& task : tasks) {
        EXPECT_TRUE(task->co->done());
        EXPECT_EQ(task->counter, std::uint64_t{kSteps});
        migrated += task->threads.size() > 1 ? 1 : 0;
    }
    // Every resume happens on a different thread than the previous one.
    EXPECT_EQ(migrated, std::size_t{kCoroutines});
}

// --- fail-fast conditions ---

TEST(CoroutineDeathTest, DestroyStartedNotCompleted) {
    EXPECT_DEATH(
        {
            Stack stack = Stack::allocate();
            Coroutine co(stack.view(), [](SuspendContext ctx) noexcept { ctx.suspend(); });
            co.resume();
        },
        "started but has not completed");
}

TEST(CoroutineDeathTest, ResumeCompleted) {
    EXPECT_DEATH(
        {
            Stack stack = Stack::allocate();
            Coroutine co(stack.view(), [](SuspendContext) noexcept {});
            co.resume();
            co.resume();
        },
        "resume\\(\\) of a completed coroutine");
}

[[gnu::noinline]] std::size_t recurse(std::size_t depth) {
    volatile char frame[512];
    frame[0] = static_cast<char>(depth);
    if (depth == 0) {
        return frame[0];
    }
    return recurse(depth - 1) + static_cast<std::size_t>(frame[0]);
}

volatile std::size_t g_sink = 0;
const std::byte* g_guard_begin = nullptr;
const std::byte* g_guard_end = nullptr;

// SIGSEGV handler on an alternate stack (the faulting stack is exhausted):
// exit code 3 if the fault address is in the guard page, 4 otherwise.
void on_segv(int, siginfo_t* info, void*) {
    const auto* address = static_cast<const std::byte*>(info->si_addr);
    if (address >= g_guard_begin && address < g_guard_end) {
        static constexpr char kMessage[] = "fault in the guard page\n";
        [[maybe_unused]] auto written = ::write(2, kMessage, sizeof(kMessage) - 1);
        ::_exit(3);
    }
    ::_exit(4);
}

void overflow_coroutine_stack() {
    static std::byte alt_stack[64 * 1024];
    stack_t ss{};
    ss.ss_sp = alt_stack;
    ss.ss_size = sizeof(alt_stack);
    ::sigaltstack(&ss, nullptr);
    struct sigaction action {};
    action.sa_sigaction = &on_segv;
    action.sa_flags = SA_SIGINFO | SA_ONSTACK;
    ::sigaction(SIGSEGV, &action, nullptr);

    Stack stack = Stack::allocate(16 * 1024);
    g_guard_begin = stack.mapping();
    g_guard_end = stack.mapping() + stack.guard_size();
    Coroutine co(stack.view(), [](SuspendContext) noexcept { g_sink = recurse(1u << 20); });
    co.resume();
}

// Running off the end of a coroutine's stack faults in its guard page.
TEST(CoroutineDeathTest, StackOverflowHitsGuardPage) {
    EXPECT_EXIT(overflow_coroutine_stack(), testing::ExitedWithCode(3), "fault in the guard page");
}

}  // namespace
