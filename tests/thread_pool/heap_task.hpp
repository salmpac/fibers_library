#pragma once

// Test-only convenience: wraps a callable into a heap-allocated Task.
// The pool itself never allocates; real users bring their own memory
// (object pools, fiber structs, ...).

#include <type_traits>
#include <utility>

#include "fibers/thread_pool/task.hpp"

namespace fibers::thread_pool::test {

template <class F>
Task heap_task(F&& f) {
    using Fn = std::decay_t<F>;
    return Task(
        new Fn(std::forward<F>(f)),
        [](void* memo) noexcept {
            auto* fn = static_cast<Fn*>(memo);
            (*fn)();
            delete fn;
        },
        [](void* memo) noexcept { delete static_cast<Fn*>(memo); });
}

}  // namespace fibers::thread_pool::test
