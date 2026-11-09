// MachineContext: the raw switch (no coroutine layer on top).

#include <gtest/gtest.h>

#include <atomic>
#include <cfenv>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

#include "fibers/context/context.hpp"
#include "test_stack.hpp"

namespace context = fibers::context;
using context::MachineContext;
using context::Trampoline;
using context_test::TestStack;

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

// A context that runs `body(self)` and then exits to `exit_target`.
struct Runner {
    MachineContext ctx;
    MachineContext* exit_target = nullptr;
    void (*body)(Runner&) = nullptr;
    void* arg = nullptr;

    void setup(context::StackView stack) {
        ctx.setup(stack, Trampoline{this, &run});
    }

    static void run(void* self) noexcept {
        auto& runner = *static_cast<Runner*>(self);
        runner.body(runner);
        runner.ctx.exit_to(*runner.exit_target);
    }
};

TEST(MachineContext, PingPong) {
    TestStack stack;
    MachineContext main;
    Runner runner;
    runner.exit_target = &main;
    int steps = 0;
    runner.arg = &steps;
    runner.body = [](Runner& self) {
        int& n = *static_cast<int*>(self.arg);
        for (int i = 0; i < 100; ++i) {
            ++n;
            self.ctx.switch_to(*self.exit_target);
        }
    };
    runner.setup(stack.view());
    for (int i = 0; i < 100; ++i) {
        main.switch_to(runner.ctx);
        EXPECT_EQ(steps, i + 1);
    }
    main.switch_to(runner.ctx);  // runs to the exit
    EXPECT_EQ(steps, 100);
}

// Many live values across switches: callee-saved registers survive (in
// optimised builds the compiler keeps these in rbx, rbp, r12-r15).
[[gnu::noinline]] std::uint64_t mix(MachineContext& self, MachineContext& other, std::uint64_t seed) {
    std::uint64_t a = seed, b = seed * 3, c = seed * 5, d = seed * 7, e = seed * 11, f = seed * 13;
    for (int i = 0; i < 10; ++i) {
        self.switch_to(other);
        a += b ^ c;
        b += c ^ d;
        c += d ^ e;
        d += e ^ f;
        e += f ^ a;
        f += a ^ b;
    }
    return a ^ b ^ c ^ d ^ e ^ f;
}

std::uint64_t mix_reference(std::uint64_t seed) {
    std::uint64_t a = seed, b = seed * 3, c = seed * 5, d = seed * 7, e = seed * 11, f = seed * 13;
    for (int i = 0; i < 10; ++i) {
        a += b ^ c;
        b += c ^ d;
        c += d ^ e;
        d += e ^ f;
        e += f ^ a;
        f += a ^ b;
    }
    return a ^ b ^ c ^ d ^ e ^ f;
}

TEST(MachineContext, CalleeSavedRegistersSurvive) {
    TestStack stack;
    MachineContext main;
    Runner runner;
    runner.exit_target = &main;
    std::uint64_t result = 0;
    runner.arg = &result;
    runner.body = [](Runner& self) {
        *static_cast<std::uint64_t*>(self.arg) = mix(self.ctx, *self.exit_target, 0x1234567);
    };
    runner.setup(stack.view());
    const std::uint64_t mine = mix(main, runner.ctx, 0x89abcdef);
    main.switch_to(runner.ctx);  // its 11th round
    EXPECT_EQ(mine, mix_reference(0x89abcdef));
    EXPECT_EQ(result, mix_reference(0x1234567));
}

// The entry frame is aligned like after a call: 16-byte aligned locals need
// no realignment, and SSE spills (movaps) do not fault.
[[gnu::noinline]] void aligned_local_address(std::uintptr_t* out) {
    alignas(16) volatile long double x = 1.0L;
    *out = reinterpret_cast<std::uintptr_t>(&x);
}

TEST(MachineContext, StackIsAbiAligned) {
    TestStack stack;
    MachineContext main;
    Runner runner;
    runner.exit_target = &main;
    std::uintptr_t address = 1;
    runner.arg = &address;
    runner.body = [](Runner& self) { aligned_local_address(static_cast<std::uintptr_t*>(self.arg)); };
    runner.setup(stack.view());
    main.switch_to(runner.ctx);
    EXPECT_EQ(address % 16, 0u);
#if !FIBERS_CONTEXT_ASAN  // ASan may put the local on its fake stack
    const auto* bottom = stack.view().data();
    EXPECT_GT(address, reinterpret_cast<std::uintptr_t>(bottom));
    EXPECT_LT(address, reinterpret_cast<std::uintptr_t>(bottom + stack.size()));
#endif
}

// Rounding mode (MXCSR and the x87 control word) is per context.
TEST(MachineContext, FloatingPointControlIsPerContext) {
    ASSERT_EQ(std::fegetround(), FE_TONEAREST);
    TestStack stack;
    MachineContext main;
    Runner runner;
    runner.exit_target = &main;
    int seen = -1;
    runner.arg = &seen;
    runner.body = [](Runner& self) {
        int& seen = *static_cast<int*>(self.arg);
        std::fesetround(FE_DOWNWARD);
        self.ctx.switch_to(*self.exit_target);
        seen = std::fegetround();
        std::fesetround(FE_TONEAREST);
    };
    runner.setup(stack.view());
    main.switch_to(runner.ctx);
    EXPECT_EQ(std::fegetround(), FE_TONEAREST);  // not leaked into main
    main.switch_to(runner.ctx);
    EXPECT_EQ(seen, FE_DOWNWARD);  // restored for the fiber
    EXPECT_EQ(std::fegetround(), FE_TONEAREST);
}

// Symmetric transfer: main -> a -> b -> a -> main.
TEST(MachineContext, SymmetricSwitches) {
    TestStack stack_a;
    TestStack stack_b;
    MachineContext main;
    Runner a;
    Runner b;
    std::vector<int> trace;
    a.exit_target = &main;
    b.exit_target = &a.ctx;
    a.arg = &b;
    b.arg = &trace;
    a.body = [](Runner& self) {
        auto& b = *static_cast<Runner*>(self.arg);
        static_cast<std::vector<int>*>(b.arg)->push_back(1);
        self.ctx.switch_to(b.ctx);  // b exits back to us
        static_cast<std::vector<int>*>(b.arg)->push_back(3);
    };
    b.body = [](Runner& self) { static_cast<std::vector<int>*>(self.arg)->push_back(2); };
    a.setup(stack_a.view());
    b.setup(stack_b.view());
    main.switch_to(a.ctx);
    EXPECT_EQ(trace, (std::vector<int>{1, 2, 3}));
}

// A stack can be set up again after its context exited.
TEST(MachineContext, ReuseStackAfterExit) {
    TestStack stack;
    MachineContext main;
    int runs = 0;
    for (int i = 0; i < 1000; ++i) {
        Runner runner;
        runner.exit_target = &main;
        runner.arg = &runs;
        runner.body = [](Runner& self) { ++*static_cast<int*>(self.arg); };
        runner.setup(stack.view());
        main.switch_to(runner.ctx);
    }
    EXPECT_EQ(runs, 1000);
}

// A context suspended on one thread continues on another. Thread hand-over
// through an atomic (release / acquire).
TEST(MachineContext, ContinuesOnAnotherThread) {
    TestStack stack;
    struct Shared {
        Runner runner;
        MachineContext caller;  // whoever runs the fiber at the moment
        std::vector<std::thread::id> threads;
    } shared;
    shared.runner.exit_target = &shared.caller;
    shared.runner.arg = &shared;
    shared.runner.body = [](Runner& self) {
        auto& shared = *static_cast<Shared*>(self.arg);
        for (int i = 0; i < 4; ++i) {
            shared.threads.push_back(current_thread_id());
            self.ctx.switch_to(shared.caller);
        }
    };
    shared.runner.setup(stack.view());

    std::atomic<int> turn{0};
    constexpr int kSteps = 5;  // 4 switches back + the exit
    auto worker = [&](int parity) {
        for (int step = parity; step < kSteps; step += 2) {
            while (turn.load(std::memory_order_acquire) != step) {
                std::this_thread::yield();
            }
            shared.caller.switch_to(shared.runner.ctx);
            turn.store(step + 1, std::memory_order_release);
        }
    };
    std::thread t0(worker, 0);
    std::thread t1(worker, 1);
    t0.join();
    t1.join();
    ASSERT_EQ(shared.threads.size(), 4u);
    EXPECT_NE(shared.threads[0], shared.threads[1]);
    EXPECT_EQ(shared.threads[0], shared.threads[2]);
    EXPECT_EQ(shared.threads[1], shared.threads[3]);
}

TEST(MachineContextDeathTest, StackTooSmall) {
    alignas(16) std::byte tiny[64];
    EXPECT_DEATH(
        {
            MachineContext ctx;
            ctx.setup(context::StackView{tiny, sizeof(tiny)}, Trampoline{nullptr, nullptr});
        },
        "stack too small");
}

}  // namespace

namespace second_tu {
int run_three_switches();  // second_tu.cpp
}

namespace {

// The same inline asm functions used from two translation units.
TEST(MachineContext, SecondTranslationUnit) {
    EXPECT_EQ(second_tu::run_three_switches(), 3);
}

}  // namespace
