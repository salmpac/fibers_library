#pragma once

// Stackful coroutines: a routine running on its own stack that can suspend
// itself and be resumed, by the same or another thread.
//
//   void body(coro::SuspendContext ctx) noexcept {
//       ...; ctx.suspend(); ...; ctx.suspend(awaiter); ...
//   }
//   coro::Coroutine co(stack.view(), body);  // non-owning routine
//   coro::ResumeResult r = co.resume();      // runs until suspend / completion
//   if (r.awaiter) r.awaiter.await_suspend(); // act on it off the coroutine's stack
//   co.done();
//
//   auto co2 = coro::make_coroutine(stack.view(), [&](coro::SuspendContext ctx) noexcept {...});
//                                            // closure moved to the top of the stack
//
// No allocation, no thread_local, no virtual functions, no exceptions.
//
// Passing information out on suspend (Lipovsky's AwaitSuspend pattern):
// ctx.suspend(awaiter) stores a type-erased reference to the awaiter in the
// Coroutine object (not in a thread_local), switches out, and resume()
// returns it to the resumer. The resumer acts on it after the coroutine's
// stack has stopped running, which is what makes "enqueue me into a mutex /
// reschedule me" safe: by the time another thread can resume the coroutine,
// it is no longer running anywhere. The awaiter object itself typically
// lives in the suspended coroutine's frame; it stays valid until the
// coroutine is resumed again. The coroutine layer never calls it and knows
// nothing about schedulers: the resumer (a fiber layer) does.
//
// An Awaiter is anything with `void await_suspend() noexcept` (a concept, no
// base class). It has no parameters on purpose: an awaiter is constructed
// inside the coroutine, which can put into it whatever the resumer needs
// (e.g. a fiber handle), and a layer above that wants a richer call wraps its
// own awaiter in a small adapter on the suspending frame. Erased as
// {void* self, void (*fn)(void*) noexcept} (AwaiterRef), like
// thread_pool::Task: one indirect call, no vtable.
//
// Threads: a coroutine may be resumed on another thread than the one it last
// suspended on. The hand-over between the threads (the suspend has returned
// on thread A before resume() starts on thread B) must be synchronised by the
// caller, e.g. through a release/acquire atomic or a queue; given that, the
// coroutine's stack and this object are handed over with it. Coroutine
// bodies must not keep addresses of thread_local variables across suspend(),
// and must not trust the optimiser with "what thread am I on" either:
// std::this_thread::get_id() is pthread_self(), which glibc declares const,
// so a call in a loop around suspend() can be hoisted and keep returning the
// thread the coroutine started on (seen in our tests at -O2). Read such
// things through a [[gnu::noinline]] function with an asm memory barrier
// (as thread_pool::detail::thread_state() does).
//
// Lifetime: a Coroutine is neither copyable nor movable (its contexts are
// referred to by address while suspended), so it can be embedded in a larger
// object (a fiber header) or returned by make_coroutine (guaranteed copy
// elision). Destroying one that started but has not completed is a
// fail_fast: without exceptions its stack cannot be unwound.
//
// Sync policy: the context switch comes from Sync::ExecutionContext
// (sync::ContextSyncPolicy), so under TwistSync Twist sees every switch.
// Coroutine / SuspendContext / Routine are the StdSync instances of the
// Basic* templates.

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "fibers/context/context.hpp"
#include "fibers/context/stack_view.hpp"
#include "fibers/context/trampoline.hpp"
#include "fibers/fail/fail_fast.hpp"
#include "fibers/sync/sync.hpp"

namespace fibers::coro {

using context::StackView;

template <class A>
concept Awaiter = requires(A& awaiter) {
    { awaiter.await_suspend() } noexcept;
};

// Type-erased reference to an Awaiter (not owning). Empty when the coroutine
// suspended without one (ctx.suspend()) or completed.
class AwaiterRef {
public:
    AwaiterRef() noexcept = default;

    template <Awaiter A>
    explicit AwaiterRef(A& awaiter) noexcept
        : self_(const_cast<void*>(static_cast<const void*>(std::addressof(awaiter)))),
          fn_(&call<A>) {}

    explicit operator bool() const noexcept { return fn_ != nullptr; }

    // Calls awaiter.await_suspend(). Must not be empty.
    void await_suspend() const noexcept { fn_(self_); }

    // The awaiter object (nullptr if empty), for a resumer that knows its type.
    void* address() const noexcept { return self_; }

private:
    template <class A>
    static void call(void* self) noexcept {
        static_cast<A*>(self)->await_suspend();
    }

    void* self_ = nullptr;
    void (*fn_)(void*) noexcept = nullptr;
};

enum class Status { kSuspended, kCompleted };

struct ResumeResult {
    Status status = Status::kSuspended;
    AwaiterRef awaiter;  // empty unless suspended with ctx.suspend(awaiter)

    bool completed() const noexcept { return status == Status::kCompleted; }
};

template <sync::ContextSyncPolicy Sync>
class BasicCoroutine;

// Handed to the routine: the only way to suspend the running coroutine.
// A small copyable handle; valid while the coroutine runs, only on its stack.
template <sync::ContextSyncPolicy Sync>
class BasicSuspendContext {
public:
    // Switches back to the resumer; resume() returns {kSuspended, empty}.
    void suspend() noexcept;

    // Switches back to the resumer; resume() returns {kSuspended, awaiter}.
    // `awaiter` must stay alive until the resumer is done with it (usually it
    // lives on this stack, so: until the coroutine is resumed).
    template <Awaiter A>
    void suspend(A& awaiter) noexcept;

private:
    friend class BasicCoroutine<Sync>;
    explicit BasicSuspendContext(BasicCoroutine<Sync>* coroutine) noexcept
        : coroutine_(coroutine) {}

    BasicCoroutine<Sync>* coroutine_;
};

// The body of a coroutine, not owning: {void* self, void (*fn)(void*, ctx)
// noexcept}. Made from
//   - a function `void(SuspendContext) noexcept` (pointer or captureless
//     lambda),
//   - an lvalue callable with `operator()(SuspendContext) noexcept`, which
//     must outlive the coroutine's run (it is referred to, not copied).
// Rvalue callables with state are rejected (they would dangle): keep them in
// a variable, or hand them to make_coroutine.
template <sync::ContextSyncPolicy Sync>
class BasicRoutine {
public:
    using Context = BasicSuspendContext<Sync>;
    using Function = void(Context) noexcept;

    BasicRoutine(Function* function) noexcept  // NOLINT: implicit on purpose
        : target_{.function = function}, call_(&call_function) {}

    // Captureless lambdas (also rvalues): through their function pointer.
    template <class F>
        requires std::convertible_to<F, Function*> && (!std::same_as<std::decay_t<F>, Function*>)
    BasicRoutine(F&& f) noexcept  // NOLINT: implicit on purpose
        : BasicRoutine(static_cast<Function*>(f)) {}

    // Lvalue callables: referred to, not copied.
    template <class F>
        requires(!std::same_as<std::remove_cv_t<F>, BasicRoutine>) &&
                (!std::convertible_to<F&, Function*>) && std::is_nothrow_invocable_v<F&, Context>
    BasicRoutine(F& f) noexcept  // NOLINT: implicit on purpose
        : target_{.object = const_cast<void*>(static_cast<const void*>(std::addressof(f)))},
          call_(&call_object<F>) {}

    template <class F>
        requires(!std::same_as<std::remove_cvref_t<F>, BasicRoutine>) &&
                (!std::is_lvalue_reference_v<F>) && (!std::convertible_to<F, Function*>)
    BasicRoutine(F&&) = delete;  // would dangle: use a named callable or make_coroutine

    // Erased form: `call(self, ctx)`.
    static BasicRoutine erased(void* self, void (*call)(void*, Context) noexcept) noexcept {
        return BasicRoutine(self, call);
    }

    void operator()(Context ctx) const noexcept { call_(target_, ctx); }

private:
    union Target {
        void* object;
        Function* function;
        struct Erased {
            void* self;
            void (*call)(void*, Context) noexcept;
        } erased;
    };

    BasicRoutine(void* self, void (*call)(void*, Context) noexcept) noexcept
        : target_{.erased = {self, call}}, call_(&call_erased) {}

    static void call_function(const Target& target, Context ctx) noexcept {
        target.function(ctx);
    }
    template <class F>
    static void call_object(const Target& target, Context ctx) noexcept {
        (*static_cast<F*>(target.object))(ctx);
    }
    static void call_erased(const Target& target, Context ctx) noexcept {
        target.erased.call(target.erased.self, ctx);
    }

    Target target_;
    void (*call_)(const Target&, Context) noexcept;
};

template <sync::ContextSyncPolicy Sync = sync::StdSync>
class BasicCoroutine {
    using ExecutionContext = typename Sync::ExecutionContext;
    static_assert(context::SwitchableContext<ExecutionContext>,
                  "Sync::ExecutionContext: include its header (fibers/context/...)");

public:
    using SuspendContext = BasicSuspendContext<Sync>;
    using Routine = BasicRoutine<Sync>;
    // Called with `drop_arg` instead of running if the coroutine is
    // destroyed before it was ever resumed (make_coroutine: destroys the
    // closure).
    using Drop = void (*)(void* drop_arg) noexcept;

    // Prepares `routine` to run on `stack` (not owned: it must outlive the
    // coroutine's run). Nothing runs until the first resume().
    BasicCoroutine(StackView stack, Routine routine) noexcept
        : BasicCoroutine(stack, routine, nullptr, nullptr) {}

    BasicCoroutine(StackView stack, Routine routine, Drop drop, void* drop_arg) noexcept
        : routine_(routine), drop_(drop), drop_arg_(drop_arg) {
        coroutine_ctx_.setup(stack, context::Trampoline{this, &trampoline});
    }

    BasicCoroutine(const BasicCoroutine&) = delete;
    BasicCoroutine& operator=(const BasicCoroutine&) = delete;
    BasicCoroutine(BasicCoroutine&&) = delete;
    BasicCoroutine& operator=(BasicCoroutine&&) = delete;

    ~BasicCoroutine() {
        if (state_ == State::kRunning || state_ == State::kSuspended) [[unlikely]] {
            fail::fail_fast("coro: destroying a coroutine that started but has not completed");
        }
        if (state_ == State::kCreated && drop_ != nullptr) {
            drop_(drop_arg_);
        }
    }

    // Runs the coroutine until it suspends or completes. Not on a completed
    // coroutine, not from inside itself (both fail_fast).
    ResumeResult resume() noexcept {
        if (state_ == State::kCompleted || state_ == State::kRunning) [[unlikely]] {
            fail::fail_fast(state_ == State::kCompleted ? "coro: resume() of a completed coroutine"
                                                        : "coro: resume() of a running coroutine");
        }
        state_ = State::kRunning;
        caller_ctx_.switch_to(coroutine_ctx_);
        // Back on the resumer's stack: the coroutine suspended or completed.
        return ResumeResult{state_ == State::kCompleted ? Status::kCompleted : Status::kSuspended,
                            std::exchange(awaiter_, AwaiterRef{})};
    }

    bool done() const noexcept { return state_ == State::kCompleted; }
    bool started() const noexcept { return state_ != State::kCreated; }

private:
    friend class BasicSuspendContext<Sync>;

    enum class State : std::uint8_t { kCreated, kRunning, kSuspended, kCompleted };

    // On the coroutine's stack.
    void suspend(AwaiterRef awaiter) noexcept {
        awaiter_ = awaiter;
        state_ = State::kSuspended;
        coroutine_ctx_.switch_to(caller_ctx_);
        // Resumed (maybe on another thread): resume() set kRunning.
    }

    // Bottom of the coroutine's stack.
    static void trampoline(void* self) noexcept {
        auto* coroutine = static_cast<BasicCoroutine*>(self);
        coroutine->routine_(SuspendContext(coroutine));
        coroutine->state_ = State::kCompleted;
        coroutine->coroutine_ctx_.exit_to(coroutine->caller_ctx_);
    }

    ExecutionContext coroutine_ctx_;
    ExecutionContext caller_ctx_;  // the resumer, saved by each resume()
    Routine routine_;
    Drop drop_;
    void* drop_arg_;
    AwaiterRef awaiter_;
    State state_ = State::kCreated;
};

template <sync::ContextSyncPolicy Sync>
void BasicSuspendContext<Sync>::suspend() noexcept {
    coroutine_->suspend(AwaiterRef{});
}

template <sync::ContextSyncPolicy Sync>
template <Awaiter A>
void BasicSuspendContext<Sync>::suspend(A& awaiter) noexcept {
    coroutine_->suspend(AwaiterRef(awaiter));
}

using Coroutine = BasicCoroutine<sync::StdSync>;
using SuspendContext = BasicSuspendContext<sync::StdSync>;
using Routine = BasicRoutine<sync::StdSync>;

namespace detail {

// Leaves at least this much of the stack below a closure placed by
// make_coroutine (the guard page catches the rest).
inline constexpr std::size_t kMinStackBelowClosure = 1024;

template <class Closure, class Context>
void run_and_destroy(void* closure, Context ctx) noexcept {
    Closure& f = *static_cast<Closure*>(closure);
    f(ctx);
    f.~Closure();  // its lifetime ends at completion, on the coroutine's stack
}

template <class Closure>
void destroy(void* closure) noexcept {
    static_cast<Closure*>(closure)->~Closure();
}

}  // namespace detail

// A coroutine running `f`, moved (or copied) to the top of `stack`: no
// allocation, and the closure lives exactly as long as the run (destroyed
// on the coroutine's stack when f returns, or by ~Coroutine if never
// resumed). The coroutine uses the rest of the stack below the closure.
template <sync::ContextSyncPolicy Sync = sync::StdSync, class F>
    requires std::is_nothrow_invocable_v<std::decay_t<F>&, BasicSuspendContext<Sync>> &&
             std::is_nothrow_constructible_v<std::decay_t<F>, F&&>
BasicCoroutine<Sync> make_coroutine(StackView stack, F&& f) noexcept {
    using Closure = std::decay_t<F>;
    constexpr std::size_t kAlign = alignof(Closure) > 16 ? alignof(Closure) : 16;
    const auto begin = reinterpret_cast<std::uintptr_t>(stack.data());
    const std::uintptr_t end = begin + stack.size();
    if (stack.size() < sizeof(Closure) + kAlign + detail::kMinStackBelowClosure) [[unlikely]] {
        fail::fail_fast("coro: make_coroutine: stack too small for the closure");
    }
    const std::uintptr_t at = (end - sizeof(Closure)) & ~(std::uintptr_t{kAlign} - 1);
    Closure* closure = ::new (reinterpret_cast<void*>(at)) Closure(std::forward<F>(f));
    const StackView below{stack.data(), static_cast<std::size_t>(at - begin)};
    return BasicCoroutine<Sync>(
        below,
        BasicRoutine<Sync>::erased(closure, &detail::run_and_destroy<Closure, BasicSuspendContext<Sync>>),
        &detail::destroy<Closure>, closure);
}

}  // namespace fibers::coro
