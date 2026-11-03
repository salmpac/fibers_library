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

# Header-only work-stealing thread pool: include/thread_pool, namespace thread_pool.
add_library(fibers_thread_pool INTERFACE)
add_library(fibers::thread_pool ALIAS fibers_thread_pool)
set_target_properties(fibers_thread_pool PROPERTIES EXPORT_NAME thread_pool)
target_include_directories(fibers_thread_pool INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_compile_features(fibers_thread_pool INTERFACE cxx_std_20)
target_link_libraries(fibers_thread_pool INTERFACE Threads::Threads)

install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(TARGETS fibers_thread_pool EXPORT fibersTargets)
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
