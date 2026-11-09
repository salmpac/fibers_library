cmake_minimum_required(VERSION 3.25)
project(fibers_library VERSION 0.1.0 LANGUAGES CXX)

include(GNUInstallDirs)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# No C++20 modules here. Scanning would also run clang-scan-deps directly,
# bypassing the nix cc-wrapper (and thus libc/libstdc++ include paths); this
# matters for fetched deps such as fmt that opt into CMP0155.
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)

option(FIBERS_BUILD_TESTS "Build unit tests" ${PROJECT_IS_TOP_LEVEL})
option(FIBERS_BUILD_BENCHMARKS "Build benchmarks (needs google benchmark)" OFF)
option(FIBERS_TWIST "Fetch Twist and build concurrency tests with TwistSync" OFF)
option(FIBERS_LINK_COMPILE_COMMANDS
    "Symlink compile_commands.json from the build dir into the source dir (for clangd)" OFF)
set(FIBERS_SANITIZE "" CACHE STRING
    "Sanitizers to enable, passed to -fsanitize= (e.g. thread, address,undefined)")

if(PROJECT_IS_TOP_LEVEL AND NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

# Under nix the cc-wrapper injects dependency include dirs (gtest, ...) through
# NIX_CFLAGS_COMPILE, so CMake mistakes them for compiler built-ins and omits
# them from compile commands. Un-mark them so compile_commands.json is complete
# for tools that do not see the nix environment (clangd).
if(DEFINED ENV{NIX_CFLAGS_COMPILE})
    separate_arguments(_fibers_nix_flags UNIX_COMMAND "$ENV{NIX_CFLAGS_COMPILE}")
    set(_fibers_next_is_dir OFF)
    foreach(_flag IN LISTS _fibers_nix_flags)
        if(_fibers_next_is_dir)
            list(REMOVE_ITEM CMAKE_CXX_IMPLICIT_INCLUDE_DIRECTORIES "${_flag}")
            set(_fibers_next_is_dir OFF)
        elseif(_flag STREQUAL "-isystem")
            set(_fibers_next_is_dir ON)
        endif()
    endforeach()
endif()

if(FIBERS_SANITIZE)
    add_compile_options(-fsanitize=${FIBERS_SANITIZE} -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=${FIBERS_SANITIZE})
endif()

find_package(Threads REQUIRED)

if(FIBERS_TWIST)
    include(cmake/Twist.cmake)
endif()

# Header-only components: include/fibers/<component>/*.hpp, namespace
# fibers::<component>, one INTERFACE target fibers::<component> each. All of
# them share the include dir include/ (#include "fibers/<component>/x.hpp");
# a component links the components it uses.
function(fibers_add_component name)
    add_library(fibers_${name} INTERFACE)
    add_library(fibers::${name} ALIAS fibers_${name})
    set_target_properties(fibers_${name} PROPERTIES EXPORT_NAME ${name})
    target_include_directories(fibers_${name} INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
    target_compile_features(fibers_${name} INTERFACE cxx_std_20)
    target_link_libraries(fibers_${name} INTERFACE ${ARGN})
    install(TARGETS fibers_${name} EXPORT fibersTargets)
endfunction()

# Sync policy (StdSync / TwistSync, policy concepts), cpu_relax, SpinLock.
fibers_add_component(sync Threads::Threads)
# fail_fast(): the library never throws.
fibers_add_component(fail)
# SPSC / SPMC / sharded / slow queues. (LockFreeSPMCQueue's Task slot uses
# fibers/thread_pool/task.hpp, a dependency-free header.)
fibers_add_component(queue fibers::sync)
# Work-stealing thread pool and Task.
fibers_add_component(thread_pool fibers::sync fibers::queue)
# Sharded object pools.
fibers_add_component(object_pool fibers::sync fibers::fail)
# Reactor thread (epoll, timers, polling sources); hands out Tasks.
fibers_add_component(poller fibers::sync fibers::fail fibers::thread_pool)
# Shared-memory SPSC rings for other processes, polled by the poller.
fibers_add_component(ipc fibers::sync fibers::poller)
# Blocking-operations pool (spawn_blocking), ops with continuations, RWF_NOWAIT.
fibers_add_component(blocking fibers::sync fibers::fail fibers::thread_pool)
# Execution contexts: our x86-64 context switch (MachineContext, StdSync) and
# the Twist adapter (TwistContext, TwistSync; needs the twist and sure targets).
fibers_add_component(context fibers::fail)
# Fiber stacks: mmap'ed with a guard page; StackPool on the sharded object pool.
fibers_add_component(stack fibers::context fibers::fail fibers::object_pool fibers::sync)
# Stackful coroutines over Sync::ExecutionContext.
fibers_add_component(coro fibers::context fibers::fail fibers::sync)
# Waiter-agnostic cores of waiting primitives over WaitNode (MutexCore).
fibers_add_component(wait fibers::sync fibers::fail)
# Scheduler { ThreadPool, Poller, BlockingPool, StackPool, TaskMemoryPool },
# TaskHeader, plain-callback tasks.
fibers_add_component(sched fibers::sync fibers::fail fibers::thread_pool fibers::poller
    fibers::blocking fibers::stack fibers::object_pool)
# Stackful fibers on a Scheduler (go, this_fiber, state machine) and their
# primitives (Mutex).
fibers_add_component(fiber fibers::sched fibers::coro fibers::wait fibers::context)

install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT fibersTargets
    NAMESPACE fibers::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/fibers)
install(FILES cmake/fibersConfig.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/fibers)

if(FIBERS_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if(FIBERS_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

if(FIBERS_LINK_COMPILE_COMMANDS AND PROJECT_IS_TOP_LEVEL
   AND NOT CMAKE_BINARY_DIR STREQUAL CMAKE_SOURCE_DIR)
    file(CREATE_LINK
        ${CMAKE_BINARY_DIR}/compile_commands.json
        ${CMAKE_SOURCE_DIR}/compile_commands.json
        SYMBOLIC)
endif()
