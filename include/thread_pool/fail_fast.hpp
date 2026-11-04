#pragma once

// Fail-fast policy of the library: no exceptions are thrown or caught.
//
// Resource acquisition (threads, memory, OS primitives) happens in noexcept
// functions, so a failure there (std::bad_alloc, std::system_error from a
// thread or mutex) terminates the process via std::terminate, which reports
// the exception. Conditions the library detects itself go through fail_fast().

#include <cstdio>
#include <cstdlib>

namespace thread_pool {

[[noreturn]] inline void fail_fast(const char* message) noexcept {
    std::fputs("fibers_library: fatal: ", stderr);
    std::fputs(message, stderr);
    std::fputc('\n', stderr);
    std::abort();
}

}  // namespace thread_pool
