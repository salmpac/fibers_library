#pragma once

// Which sanitizer the including translation unit is built with. Context
// switches must be announced to ASan and TSan (see machine_context.hpp); the
// annotations are compiled in only under the corresponding sanitizer.
//
//   FIBERS_CONTEXT_ASAN   1 under -fsanitize=address, else 0
//   FIBERS_CONTEXT_TSAN   1 under -fsanitize=thread, else 0
//
// GCC defines __SANITIZE_ADDRESS__ / __SANITIZE_THREAD__; clang has
// __has_feature (newer clang defines the GCC macros as well).

#if defined(__SANITIZE_ADDRESS__)
#define FIBERS_CONTEXT_ASAN 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
#define FIBERS_CONTEXT_ASAN 1
#endif
#endif
#ifndef FIBERS_CONTEXT_ASAN
#define FIBERS_CONTEXT_ASAN 0
#endif

#if defined(__SANITIZE_THREAD__)
#define FIBERS_CONTEXT_TSAN 1
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define FIBERS_CONTEXT_TSAN 1
#endif
#endif
#ifndef FIBERS_CONTEXT_TSAN
#define FIBERS_CONTEXT_TSAN 0
#endif

#if FIBERS_CONTEXT_ASAN
#include <sanitizer/asan_interface.h>
#include <sanitizer/common_interface_defs.h>
#endif
#if FIBERS_CONTEXT_TSAN
#include <sanitizer/tsan_interface.h>
#endif
