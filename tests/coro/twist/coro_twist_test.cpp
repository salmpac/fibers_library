// Coroutines over TwistSync under Twist: every context switch goes through
// TwistContext (an adapter over twist::ed::sure::ExecutionContext), so the
// simulator / fault injector knows which user fiber runs on which thread.
//
// TWIST_SIM:    each scenario is explored with the Random and PCT schedulers
//               of the deterministic simulator.
// TWIST_FAULTY: each scenario runs several times on real threads with fault
//               injection (pairs well with TSan: the clang-twist-faulty-tsan
//               preset).
//
// The scenarios hand a suspended coroutine from one thread to another, the
// way a fiber scheduler will: what the coroutine wrote before suspending
// (twist::assist::Shared cells, checked for happens-before) must be visible
// to the thread that resumes it, and nothing may resume it before it has
// actually switched out.

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>

#include <twist/assist/assert.hpp>
#include <twist/assist/shared.hpp>
#include <twist/cross.hpp>
#include <twist/ed/std/atomic.hpp>
#include <twist/ed/std/thread.hpp>

#if defined(__TWIST_SIM__)
#include <twist/sim.hpp>
#endif

#include "fibers/context/twist_context.hpp"
#include "fibers/coro/coroutine.hpp"
#include "fibers/stack/stack.hpp"
#include "fibers/sync/twist_sync.hpp"

namespace coro = fibers::coro;

namespace {

using Sync = fibers::sync::TwistSync;
using Coroutine = coro::BasicCoroutine<Sync>;
using SuspendContext = coro::BasicSuspendContext<Sync>;
using fibers::stack::Stack;

// Twist's own frames (and, in simulation, the stack reserve it checks for
// overflow) run on the coroutine's stack too.
constexpr std::size_t kStackSize = 256 * 1024;

std::size_t env_or(const char* name, std::size_t fallback) {
    const char* value = std::getenv(name);
    return value != nullptr ? static_cast<std::size_t>(std::strtoull(value, nullptr, 10)) : fallback;
}

// Env: FIBERS_TWIST_RUNS (runs per scheduler / repetitions), FIBERS_TWIST_SEED.
void check(const std::function<void()>& body) {
#if defined(__TWIST_SIM__)
    const std::size_t runs = env_or("FIBERS_TWIST_RUNS", 10'000);
    const std::uint64_t seed = env_or("FIBERS_TWIST_SEED", 42);
    {
        twist::sim::sched::RandomScheduler random{{.seed = seed, .max_runs = runs}};
        const auto explored = twist::sim::Explore(random, body);
        if (explored.found) {
            twist::sim::Print(body, explored.found->schedule);
            FAIL() << "Random scheduler, after " << explored.sims << " runs:\n"
                   << explored.found->result.std_err;
        }
    }
    {
        twist::sim::sched::PctScheduler pct{{.depth = 3, .seed = seed + 1, .max_runs = runs}};
        const auto explored = twist::sim::Explore(pct, body);
        if (explored.found) {
            twist::sim::Print(body, explored.found->schedule);
            FAIL() << "PCT scheduler, after " << explored.sims << " runs:\n"
                   << explored.found->result.std_err;
        }
    }
#else
    const std::size_t runs = env_or("FIBERS_TWIST_RUNS", 50);
    for (std::size_t i = 0; i < runs; ++i) {
        twist::cross::Run(body);
    }
#endif
}

// Single-threaded sanity check of the adapter: resume / suspend / complete.
TEST(CoroTwist, ResumeSuspendComplete) {
    check([] {
        Stack stack = Stack::allocate(kStackSize);
        int steps = 0;
        auto body = [&steps](SuspendContext ctx) noexcept {
            for (int i = 0; i < 3; ++i) {
                ++steps;
                ctx.suspend();
            }
        };
        Coroutine co(stack.view(), body);
        for (int i = 1; i <= 3; ++i) {
            TWIST_ASSERT_M(!co.resume().completed(), "suspended");
            TWIST_ASSERT_M(steps == i, "one step per resume");
        }
        TWIST_ASSERT_M(co.resume().completed(), "completed");
    });
}

// A coroutine handed between two threads through an atomic and resumed by
// them alternately. The coroutine writes a Shared cell before each suspend;
// the next resumer (on the other thread) reads it.
TEST(CoroTwist, AlternateThroughAtomic) {
    check([] {
        constexpr int kSteps = 4;
        Stack stack = Stack::allocate(kStackSize);
        twist::assist::Shared<int> cell{0};
        auto body = [&cell](SuspendContext ctx) noexcept {
            for (int i = 1; i <= kSteps; ++i) {
                cell.Write(i);
                ctx.suspend();
                TWIST_ASSERT_M(cell.Read() == -i, "resumer's write visible");
            }
        };
        Coroutine co(stack.view(), body);
        twist::ed::std::atomic<int> turn{0};

        auto resumer = [&](int parity) {
            // Resumes step 0, 2, 4 (parity 0) or 1, 3 (parity 1).
            for (int step = parity; step <= kSteps; step += 2) {
                while (turn.load(std::memory_order_acquire) != step) {
                    Sync::spin_hint();
                }
                if (step > 0) {
                    TWIST_ASSERT_M(cell.Read() == step, "coroutine's write visible");
                    cell.Write(-step);
                }
                const bool completed = co.resume().completed();
                TWIST_ASSERT_M(completed == (step == kSteps), "completes on the last step");
                turn.store(step + 1, std::memory_order_release);
            }
        };
        twist::ed::std::thread other(resumer, 1);
        resumer(0);
        other.join();
        TWIST_ASSERT_M(co.done(), "done");
    });
}

// The AwaitSuspend pattern: the coroutine suspends with an awaiter that
// publishes it to the other thread. The resumer runs the awaiter only after
// resume() returned, i.e. once the coroutine has switched out, so the other
// thread can never resume a coroutine that is still running.
struct HandOver {
    twist::ed::std::atomic<Coroutine*>* slot;
    Coroutine* self;
    void await_suspend() noexcept { slot->store(self, std::memory_order_release); }
};

TEST(CoroTwist, AwaiterHandsOverAfterSuspend) {
    check([] {
        constexpr int kHops = 4;
        Stack stack = Stack::allocate(kStackSize);
        twist::ed::std::atomic<Coroutine*> slots[2] = {nullptr, nullptr};
        twist::assist::Shared<int> cell{0};
        Coroutine* self = nullptr;
        auto body = [&](SuspendContext ctx) noexcept {
            for (int hop = 0; hop < kHops; ++hop) {
                cell.Write(cell.Read() + 1);
                HandOver awaiter{&slots[(hop + 1) % 2], self};
                ctx.suspend(awaiter);
            }
        };
        Coroutine co(stack.view(), body);
        self = &co;

        // Thread i runs the coroutine when it shows up in slots[i]: resumes
        // number i, i + 2, ... of kHops + 1 in total (the last completes it).
        auto driver = [&](int i) {
            for (int resume = i; resume <= kHops; resume += 2) {
                Coroutine* c = nullptr;
                while ((c = slots[i].exchange(nullptr, std::memory_order_acquire)) == nullptr) {
                    Sync::spin_hint();
                }
                TWIST_ASSERT_M(cell.Read() == resume, "previous hop's write visible");
                const coro::ResumeResult r = c->resume();
                TWIST_ASSERT_M(r.completed() == (resume == kHops), "completes after the last hop");
                if (!r.completed()) {
                    TWIST_ASSERT_M(static_cast<bool>(r.awaiter), "suspended with the hand-over awaiter");
                    r.awaiter.await_suspend();  // publishes to the other thread
                }
            }
        };
        slots[0].store(&co, std::memory_order_relaxed);
        twist::ed::std::thread other(driver, 1);
        driver(0);
        other.join();
        TWIST_ASSERT_M(co.done(), "done");
        TWIST_ASSERT_M(cell.Read() == kHops, "every hop ran");
    });
}

// make_coroutine under Twist: closure at the top of the stack, resumed on
// another thread.
TEST(CoroTwist, MakeCoroutineOnAnotherThread) {
    check([] {
        Stack stack = Stack::allocate(kStackSize);
        twist::assist::Shared<int> cell{0};
        auto co = coro::make_coroutine<Sync>(stack.view(), [&cell](SuspendContext ctx) noexcept {
            cell.Write(1);
            ctx.suspend();
            cell.Write(cell.Read() + 1);
        });
        co.resume();
        twist::ed::std::thread other([&] { TWIST_ASSERT_M(co.resume().completed(), "completed"); });
        other.join();
        TWIST_ASSERT_M(cell.Read() == 2, "both halves ran");
    });
}

}  // namespace
