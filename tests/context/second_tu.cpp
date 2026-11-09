// A second translation unit using the header-only context switch: the naked
// asm functions are inline (weak COMDAT), so both TUs link into one binary.

#include "fibers/context/context.hpp"
#include "test_stack.hpp"

namespace second_tu {

namespace {

struct Counter {
    fibers::context::MachineContext self;
    fibers::context::MachineContext* back = nullptr;
    int count = 0;

    static void run(void* arg) noexcept {
        auto& c = *static_cast<Counter*>(arg);
        for (;;) {
            if (++c.count == 3) {
                c.self.exit_to(*c.back);
            }
            c.self.switch_to(*c.back);
        }
    }
};

}  // namespace

// Switches into a fresh context three times; returns how often it ran.
int run_three_switches() {
    context_test::TestStack stack;
    fibers::context::MachineContext main;
    Counter counter;
    counter.back = &main;
    counter.self.setup(stack.view(), fibers::context::Trampoline{&counter, &Counter::run});
    for (int i = 0; i < 3; ++i) {
        main.switch_to(counter.self);
    }
    return counter.count;
}

}  // namespace second_tu
