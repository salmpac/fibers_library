#pragma once

// TwistContext: the ExecutionContext of TwistSync, an adapter over
// twist::ed::sure::ExecutionContext. Under Twist every user-level context
// switch must go through Twist (the simulator runs threads as its own fibers
// and has to know which one runs; fault injection tracks the current fiber),
// so with TwistSync the coroutine layer switches through this instead of
// MachineContext. Twist builds the sure library with the same sanitizer
// flags, and its sanitizer layer annotates the switches.
//
// Needs the `twist` and `sure` CMake targets (FIBERS_TWIST=ON; Twist links
// sure privately in fault-injection mode, so link `sure` yourself).
//
// The one place in the library with a virtual function: sure's trampoline
// interface (::sure::ITrampoline) is virtual, and this adapter implements it.
// It is confined here; our own Trampoline stays a plain function pointer.

#include <twist/ed/sure/context.hpp>

#include "fibers/context/context.hpp"
#include "fibers/context/stack_view.hpp"
#include "fibers/context/trampoline.hpp"
#include "fibers/fail/fail_fast.hpp"

namespace fibers::context {

class TwistContext final : private ::sure::ITrampoline {
public:
    TwistContext() noexcept = default;

    TwistContext(const TwistContext&) = delete;
    TwistContext& operator=(const TwistContext&) = delete;
    TwistContext(TwistContext&&) = delete;
    TwistContext& operator=(TwistContext&&) = delete;

    ~TwistContext() = default;

    void setup(StackView stack, Trampoline trampoline) noexcept {
        trampoline_ = trampoline;
        ctx_.Setup(stack, this);
    }

    void switch_to(TwistContext& target) noexcept { ctx_.SwitchTo(target.ctx_); }

    [[noreturn]] void exit_to(TwistContext& target) noexcept { ctx_.ExitTo(target.ctx_); }

private:
    // ::sure::ITrampoline
    void Run() noexcept override {
        trampoline_.run(trampoline_.self);
        fail::fail_fast("context: trampoline returned (it must exit_to another context)");
    }

    twist::ed::sure::ExecutionContext ctx_;
    Trampoline trampoline_;
};

static_assert(SwitchableContext<TwistContext>);

}  // namespace fibers::context
