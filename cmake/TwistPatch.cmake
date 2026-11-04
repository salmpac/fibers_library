# Applies the local Twist patches to FetchContent's Twist checkout (the
# current directory). Run as its PATCH_COMMAND by cmake/Twist.cmake:
#
#   cmake -DGIT_EXECUTABLE=git -DPATCH_DIR=<dir with *.patch>
#         -DPATCHES_HASH=<hash of the patch set> -P TwistPatch.cmake
#
# Idempotent: the applied patch set is recorded in a stamp file, so a re-run
# (re-configure, patch step invalidated) with the same patches does nothing,
# and a different patch set (PATCHES_HASH is part of the command, so
# FetchContent re-runs the step) first resets the checkout to the pinned
# commit. Only FetchContent's own clone is touched: with a
# FETCHCONTENT_SOURCE_DIR_TWIST override (nix) the patch step does not run.

set(_stamp "${CMAKE_CURRENT_SOURCE_DIR}/.fibers-twist-patches")
if(EXISTS "${_stamp}")
    file(READ "${_stamp}" _applied)
    if(_applied STREQUAL PATCHES_HASH)
        message(STATUS "TwistPatch: patches already applied")
        return()
    endif()
endif()

file(GLOB _patches "${PATCH_DIR}/*.patch")
list(SORT _patches)
if(NOT _patches)
    message(FATAL_ERROR "TwistPatch: no patches in ${PATCH_DIR}")
endif()

# Back to the pinned commit (HEAD of the checkout), whatever was applied before.
foreach(_command "reset;--hard;--quiet;HEAD" "clean;-fd;--quiet")
    execute_process(COMMAND "${GIT_EXECUTABLE}" ${_command} RESULT_VARIABLE _result)
    if(NOT _result EQUAL 0)
        message(FATAL_ERROR "TwistPatch: git ${_command} failed (${_result})")
    endif()
endforeach()

execute_process(
    COMMAND "${GIT_EXECUTABLE}" apply --whitespace=nowarn ${_patches}
    RESULT_VARIABLE _result)
if(NOT _result EQUAL 0)
    message(FATAL_ERROR "TwistPatch: git apply failed (${_result}); is the Twist checkout "
                        "at the pinned commit?")
endif()
file(WRITE "${_stamp}" "${PATCHES_HASH}")
message(STATUS "TwistPatch: applied ${_patches}")
