#pragma once

// MachineContext: our own user-level context switch (x86-64 System V).
// The ExecutionContext of StdSync (see SwitchableContext in context.hpp for
// the contract shared with the Twist adapter).
//
// What is saved. A switch is an ordinary function call to switch_stacks(), so
// the compiler already treats every caller-saved register (rax, rcx, rdx,
// rsi, rdi, r8-r11, xmm0-15, flags) as clobbered and spills what it needs.
// Saved are exactly the rest of the SysV ABI's callee-preserved state:
//   rbx, rbp, r12-r15, rsp, the return address,
//   MXCSR control bits and the x87 control word (the ABI makes both
//   callee-saved: a fiber that changes rounding mode must not leak it into
//   the code it switches to).
// They are pushed onto the stack being left; the context object holds only
// that stack's pointer (sp_).
//
// The switch continues the target with `pop rcx; jmp *rcx` rather than
// `ret` (rcx is caller-saved, free to clobber). A `ret` to another stack's
// return address mispredicts on the return stack buffer every time; the
// indirect jump is predicted by the branch target predictor, which learns
// the usual ping-pong pattern. Measured round trip (two switches) on an
// i5-14400F P-core: 20 ns with ret, 6.4 ns with pop + jmp.
//
// Saved frame (sp_ points at its first word; addresses grow down the list):
//   sp_ + 0   x87 control word (low 16 bits)
//   sp_ + 8   MXCSR (low 32 bits)
//   sp_ + 16  r15, r14, r13, r12, rbx, rbp
//   sp_ + 64  return address
//
// A fresh context (setup) gets a forged frame of the same shape whose
// "return address" is entry_stub, with r12 = this context and r13 = &entry.
// The first switch to it jumps to entry_stub, which moves r12 into rdi
// and jumps (not calls) to entry(MachineContext*). Above the forged frame is
// a zero word that entry sees as its return address, so debuggers and
// unwinders stop there; it also makes rsp at entry 8 mod 16, as after a call.
// The registers start zeroed (rbp = 0 ends frame-pointer chains), MXCSR and
// the x87 control word are those of the thread calling setup().
//
// Header-only assembly: switch_stacks and entry_stub are [[gnu::naked]]
// inline functions. Inline functions are emitted as weak COMDAT symbols, so
// any number of translation units link; naked means no prologue/epilogue and
// no sanitizer instrumentation, the body is exactly our asm. GCC additionally
// gets noipa: with -fipa-ra it would otherwise trust the (empty) register
// usage it sees in the naked body and keep values in caller-saved registers
// across the call. (Clang has no interprocedural register allocation on
// x86-64 by default.) The asm is AT&T syntax: -masm=intel is not supported.
//
// Sanitizers (only compiled in under them, see sanitizer.hpp):
//   ASan: __sanitizer_start_switch_fiber before and
//         __sanitizer_finish_switch_fiber after every switch, so ASan knows
//         the stack it runs on (bounds of a context that was never set up, e.g.
//         the thread's own stack, are learned from the finish call of the
//         context it switched to). setup() unpoisons the whole stack before
//         it writes the forged frame: a reused stack still carries redzones
//         of frames that never returned (entry() is the topmost frame, so
//         its redzones sit right where the forged frame goes).
//   TSan: a TSan fiber per set-up context (__tsan_create_fiber), switched to
//         right before every machine switch, destroyed by the context exited
//         to (or by the destructor if the context never ran to its exit).
//         Switches synchronise (flags 0): what ran before a switch
//         happens-before what runs after it, as on one thread.
//
// Threads. A context may be switched to from a different thread than the one
// it last ran on (fibers migrate): nothing here is thread-local, and the
// machine switch itself does not care which thread executes it. The caller
// provides the happens-before between the threads (e.g. the scheduler's
// queue). Code running in a context must not cache addresses of thread_local
// variables across a switch (see thread_pool::detail::thread_state()).
//
// CET shadow stacks (x86 SHSTK). A forged return address, or returning on a
// different stack than the call was made on, is a control-protection fault
// when a shadow stack is active. Linux enables one for user space only if
// the CPU supports it, the kernel has it, the executable and every library
// are marked SHSTK-compatible (-fcf-protection) and glibc is told to (e.g.
// GLIBC_TUNABLES=glibc.cpu.hwcaps=SHSTK on glibc 2.39+; off by default). The
// nix toolchain does not mark binaries, but other distributions' compilers
// do, so setup() checks at run time: RDSSPQ is a NOP that leaves 0 when no
// shadow stack is active, and returns its pointer otherwise; then setup()
// fails fast with a clear message instead of crashing on the first switch.
// Supporting shadow stacks (a shadow stack per context, RSTORSSP /
// SAVEPREVSSP on switch) is future work.

#if !defined(__x86_64__)
#error "fibers::context::MachineContext is implemented for x86-64 (System V) only"
#endif

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>

#include "fibers/context/sanitizer.hpp"
#include "fibers/context/stack_view.hpp"
#include "fibers/context/trampoline.hpp"
#include "fibers/fail/fail_fast.hpp"

#if defined(__clang__)
#define FIBERS_CONTEXT_NAKED gnu::naked, gnu::noinline
#else
#define FIBERS_CONTEXT_NAKED gnu::naked, gnu::noinline, gnu::noipa
#endif

namespace fibers::context {

namespace detail {

// switch_stacks(void** save_sp /* rdi */, void* load_sp /* rsi */):
// pushes the callee-saved state, stores rsp to *save_sp, loads load_sp into
// rsp, pops the state saved there and jumps to its saved return address.
[[FIBERS_CONTEXT_NAKED]] inline void switch_stacks(void** /*save_sp*/, void* /*load_sp*/) noexcept {
    asm("pushq %rbp\n\t"
        "pushq %rbx\n\t"
        "pushq %r12\n\t"
        "pushq %r13\n\t"
        "pushq %r14\n\t"
        "pushq %r15\n\t"
        "subq $16, %rsp\n\t"
        "stmxcsr 8(%rsp)\n\t"
        "fnstcw (%rsp)\n\t"
        "movq %rsp, (%rdi)\n\t"
        "movq %rsi, %rsp\n\t"
        "ldmxcsr 8(%rsp)\n\t"
        "fldcw (%rsp)\n\t"
        "addq $16, %rsp\n\t"
        "popq %r15\n\t"
        "popq %r14\n\t"
        "popq %r13\n\t"
        "popq %r12\n\t"
        "popq %rbx\n\t"
        "popq %rbp\n\t"
        "popq %rcx\n\t"
        "jmpq *%rcx\n\t");
}

// First instructions of a fresh context: entry(r12) via r13, with the zero
// word above as entry's return address.
[[FIBERS_CONTEXT_NAKED]] inline void entry_stub() noexcept {
    asm("movq %r12, %rdi\n\t"
        "jmpq *%r13\n\t");
}

// Fails fast if a CET shadow stack is active (see the header comment).
inline void check_no_shadow_stack() noexcept {
    std::uint64_t ssp = 0;
    asm volatile("rdsspq %0" : "+r"(ssp));
    if (ssp != 0) [[unlikely]] {
        fail::fail_fast(
            "context: a CET shadow stack is active, which fibers::context does not support "
            "(build without -fcf-protection=return/full, or run with "
            "GLIBC_TUNABLES=glibc.cpu.x86_shstk=off)");
    }
}

}  // namespace detail

#undef FIBERS_CONTEXT_NAKED

class MachineContext {
public:
    // Smallest stack setup() accepts: the forged frame and some room. Real
    // stacks are much larger; overflow is caught by the guard page of
    // fibers/stack, not here.
    static constexpr std::size_t kMinStackSize = 256;

    // An empty context: a place to save the running context into
    // (switch_to), not a target until it has been saved into or set up.
    MachineContext() noexcept = default;

    MachineContext(const MachineContext&) = delete;
    MachineContext& operator=(const MachineContext&) = delete;
    MachineContext(MachineContext&&) = delete;
    MachineContext& operator=(MachineContext&&) = delete;

    ~MachineContext() {
#if FIBERS_CONTEXT_TSAN
        if (tsan_owned_) {
            __tsan_destroy_fiber(tsan_fiber_);
        }
#endif
    }

    // Prepares this context to run trampoline.run(trampoline.self) on
    // `stack` when first switched to. The stack must stay valid until the
    // context has exited (or is never switched to). Must not be called on the
    // running context.
    void setup(StackView stack, Trampoline trampoline) noexcept {
        detail::check_no_shadow_stack();
        if (stack.size() < kMinStackSize) [[unlikely]] {
            fail::fail_fast("context: stack too small");
        }
        trampoline_ = trampoline;

#if FIBERS_CONTEXT_ASAN
        // Before the forged frame is written: a reused stack still carries
        // the redzones of the previous context's frames, which never
        // returned (entry() and the trampoline below it exit_to instead),
        // right where the forged frame goes.
        __asan_unpoison_memory_region(stack.data(), stack.size());
        asan_stack_bottom_ = stack.data();
        asan_stack_size_ = stack.size();
        asan_fake_stack_ = nullptr;  // first entry: no fake stack to restore
        asan_from_ = nullptr;
#endif

        std::uint32_t mxcsr = 0;
        std::uint16_t x87_cw = 0;
        asm volatile("stmxcsr %0" : "=m"(mxcsr));
        asm volatile("fnstcw %0" : "=m"(x87_cw));

        const auto top =
            reinterpret_cast<std::uintptr_t>(stack.data() + stack.size()) & ~std::uintptr_t{15};
        const std::uint64_t frame[kFrameWords] = {
            x87_cw,
            mxcsr,
            0,                                                     // r15
            0,                                                     // r14
            reinterpret_cast<std::uint64_t>(&entry),               // r13
            reinterpret_cast<std::uint64_t>(this),                 // r12
            0,                                                     // rbx
            0,                                                     // rbp: end of frame chain
            reinterpret_cast<std::uint64_t>(&detail::entry_stub),  // "return address"
            0,                                                     // entry's return address
        };
        // top is 16-aligned, the frame 80 bytes: after the final pop of
        // switch_stacks rsp = top - 8, i.e. 8 mod 16 as at a function entry.
        static_assert(sizeof(frame) % 16 == 0);
        void* sp = reinterpret_cast<void*>(top - sizeof(frame));
        std::memcpy(sp, frame, sizeof(frame));
        sp_ = sp;

#if FIBERS_CONTEXT_TSAN
        if (tsan_owned_) {
            __tsan_destroy_fiber(tsan_fiber_);
        }
        tsan_fiber_ = __tsan_create_fiber(0);
        tsan_owned_ = true;
#endif
    }

    // Saves the running context into *this and continues `target` (a set-up
    // context, or one saved by an earlier switch_to). Returns when some
    // context switches back to *this, possibly on another thread.
    void switch_to(MachineContext& target) noexcept {
        before_switch(target);
        detail::switch_stacks(&sp_, target.sp_);
        after_switch();
    }

    // Leaves the running context for good and continues `target`. The last
    // thing a trampoline does; after it the stack may be reused.
    [[noreturn]] void exit_to(MachineContext& target) noexcept {
#if FIBERS_CONTEXT_ASAN
        target.asan_from_ = nullptr;  // this context's bounds are of no use any more
        // nullptr fake-stack slot: ASan frees this context's fake stack.
        __sanitizer_start_switch_fiber(nullptr, target.asan_stack_bottom_, target.asan_stack_size_);
#endif
#if FIBERS_CONTEXT_TSAN
        // We cannot destroy the fiber we run on: the target does it.
        target.tsan_exited_ = tsan_fiber_;
        tsan_owned_ = false;
        __tsan_switch_to_fiber(target.tsan_fiber_, 0);
#endif
        detail::switch_stacks(&sp_, target.sp_);
        fail::fail_fast("context: switched to a context that has exited");
    }

private:
    static constexpr std::size_t kFrameWords = 10;

    [[noreturn]] static void entry(MachineContext* self) noexcept {
        self->after_switch();
        self->trampoline_.run(self->trampoline_.self);
        fail::fail_fast("context: trampoline returned (it must exit_to another context)");
    }

    void before_switch([[maybe_unused]] MachineContext& target) noexcept {
#if FIBERS_CONTEXT_ASAN
        target.asan_from_ = this;
        __sanitizer_start_switch_fiber(&asan_fake_stack_, target.asan_stack_bottom_,
                                       target.asan_stack_size_);
#endif
#if FIBERS_CONTEXT_TSAN
        // For a set-up context this is its own fiber; for a thread-side one
        // the fiber of whatever is running (the thread's, or an outer fiber).
        tsan_fiber_ = __tsan_get_current_fiber();
        // Immediately before the switch: later accesses count for the target.
        __tsan_switch_to_fiber(target.tsan_fiber_, 0);
#endif
    }

    // First thing after control arrives in *this (from switch_to or exit_to
    // of another context, or the very first entry).
    void after_switch() noexcept {
#if FIBERS_CONTEXT_ASAN
        MachineContext* from = std::exchange(asan_from_, nullptr);
        __sanitizer_finish_switch_fiber(asan_fake_stack_,
                                        from != nullptr ? &from->asan_stack_bottom_ : nullptr,
                                        from != nullptr ? &from->asan_stack_size_ : nullptr);
#endif
#if FIBERS_CONTEXT_TSAN
        if (tsan_exited_ != nullptr) {
            __tsan_destroy_fiber(std::exchange(tsan_exited_, nullptr));
        }
#endif
    }

    void* sp_ = nullptr;  // saved stack pointer (frame layout above)
    Trampoline trampoline_;

#if FIBERS_CONTEXT_ASAN
    const void* asan_stack_bottom_ = nullptr;  // lowest address of this context's stack
    std::size_t asan_stack_size_ = 0;
    void* asan_fake_stack_ = nullptr;          // saved by start_switch, restored by finish
    MachineContext* asan_from_ = nullptr;      // set by whoever switches to us
#endif
#if FIBERS_CONTEXT_TSAN
    void* tsan_fiber_ = nullptr;   // fiber to switch to when targeting this context
    void* tsan_exited_ = nullptr;  // fiber of a context that exited to us
    bool tsan_owned_ = false;      // tsan_fiber_ created by setup() and not yet exited
#endif
};

}  // namespace fibers::context
