#pragma once

namespace fibers::context {

// What a freshly set up context runs: run(self) on the context's stack.
//
// Type-erased by hand (an object pointer and a plain function pointer, like
// thread_pool::Task), not by a virtual base: the library has no vtables.
//
// run must never return: the last thing it does is exit_to() another context
// (returning is a fail_fast). It is noexcept: there is nothing to unwind into
// at the bottom of a context's stack.
struct Trampoline {
    void* self = nullptr;
    void (*run)(void* self) noexcept = nullptr;
};

}  // namespace fibers::context
