#pragma once

// Execution contexts: the machine-level switch under the coroutine layer.
//
// A context type C (SwitchableContext) is Lipovsky's sure::ExecutionContext
// in snake_case, without virtual functions:
//
//   C ctx;                          empty: a place to save the running
//                                   context into, not yet a target
//   ctx.setup(stack, trampoline)    prepare ctx to run
//                                   trampoline.run(trampoline.self) on `stack`
//                                   when first switched to
//   ctx.switch_to(target)           save the running context into ctx and
//                                   continue target; returns when someone
//                                   switches back to ctx (maybe on another
//                                   thread)
//   ctx.exit_to(target)             [[noreturn]]: leave the running context
//                                   for good (last act of a trampoline)
//
// All noexcept; non-copyable and non-movable (contexts are referred to by
// address while suspended). switch_to / exit_to must be called on the context
// that represents the code running right now.
//
// Which type: the Sync policy decides (Sync::ExecutionContext, see
// sync::ContextSyncPolicy), so that Twist sees user-level switches:
//   StdSync   -> MachineContext (machine_context.hpp, our x86-64 switch)
//   TwistSync -> TwistContext   (twist_context.hpp, over twist::ed::sure)

#include <concepts>
#include <type_traits>

#include "fibers/context/machine_context.hpp"
#include "fibers/context/stack_view.hpp"
#include "fibers/context/trampoline.hpp"

namespace fibers::context {

template <class C>
concept SwitchableContext =
    std::is_nothrow_default_constructible_v<C> && !std::is_copy_constructible_v<C> &&
    !std::is_move_constructible_v<C> &&
    requires(C& ctx, C& target, StackView stack, Trampoline trampoline) {
        { ctx.setup(stack, trampoline) } noexcept;
        { ctx.switch_to(target) } noexcept;
        { ctx.exit_to(target) } noexcept;
    };

static_assert(SwitchableContext<MachineContext>);

}  // namespace fibers::context
