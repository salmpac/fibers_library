# Twist: systematic concurrency testing (fault injection + deterministic simulation).
# https://gitlab.com/Lipovsky/twist
#
# Mode is selected with Twist's own cache options, e.g.
#   -DTWIST_FAULTY=ON                                  threads + fault injection
#   -DTWIST_FAULTY=ON -DTWIST_SIM=ON [-DTWIST_SIM_ISOLATION=ON]   simulation
#   ... -DTWIST_SIM_STORE_BUFFERING=ON   simulation with x86-TSO store buffers
#                                        (local patch, see below)
#
# Twist is upstream at a pinned commit plus our local patches
# (third_party/twist/patches, see the README there):
#
# * nix: sources of Twist and of everything it fetches are pinned by flake.lock;
#   the nix dev shell / build export FIBERS_DEP_<NAME>=/nix/store/... (Twist
#   already patched, by pkgs.applyPatches) and we turn them into
#   FETCHCONTENT_SOURCE_DIR_<NAME>, so no network access is needed.
# * without those variables FetchContent clones Twist at the same pinned commit
#   and applies the patches (cmake/TwistPatch.cmake, idempotent). Twist's own
#   dependencies (sure, sure-stack, futex_like, ...) are then fetched at
#   whatever revision Twist's CMake asks for: only flake.lock pins them.

include(FetchContent)

# env suffix -> FetchContent name (as declared by Twist and by us)
set(_fibers_twist_deps
    TWIST       twist
    FUTEX_LIKE  futex_like
    SPIN_WAIT   spin_wait
    SURE        sure
    SURE_STACK  sure-stack
    FUNCTION2   function2
    FMT         fmt
    JSON        json
    BACKWARD    backward)

while(_fibers_twist_deps)
    list(POP_FRONT _fibers_twist_deps _env _name)
    string(TOUPPER "${_name}" _upper)
    if(DEFINED ENV{FIBERS_DEP_${_env}} AND NOT FETCHCONTENT_SOURCE_DIR_${_upper})
        set(FETCHCONTENT_SOURCE_DIR_${_upper} "$ENV{FIBERS_DEP_${_env}}")
        message(STATUS "Twist dep ${_name}: $ENV{FIBERS_DEP_${_env}}")
    endif()
endwhile()

# fmt (fetched by Twist) builds a C++ module target when the toolchain allows;
# Twist only needs the regular library, and module scanning bypasses the nix
# cc-wrapper.
set(FMT_MODULE OFF CACHE BOOL "Build fmt as a C++20 module (not needed by Twist)")

# Parked ThreadPool workers sleep in atomic::wait (TwistSync::Atomic<uint32_t>);
# Twist only provides atomic wait / notify with this option. Cache variable, so
# presets and nix builds get it alike (Twist's option() keeps an existing value).
set(TWIST_ED_STD_ATOMIC_WAIT ON CACHE BOOL "Support std::atomic::wait in Twist")

# Keep in sync with the `twist` input of flake.nix.
set(FIBERS_TWIST_COMMIT 407b44e51a9d53c9fd5cbada08ada7a4a1cf029d)

if(FETCHCONTENT_SOURCE_DIR_TWIST)
    # Given sources are used as is (nix: already patched); no download, no patch.
    FetchContent_Declare(twist
        GIT_REPOSITORY https://gitlab.com/Lipovsky/twist.git
        GIT_TAG ${FIBERS_TWIST_COMMIT})
else()
    find_package(Git REQUIRED)
    set(_fibers_twist_patch_dir "${CMAKE_CURRENT_LIST_DIR}/../third_party/twist/patches")
    # Hash of the patch set: part of the patch command, so that changed patches
    # re-run the patch step (TwistPatch.cmake then re-applies from scratch).
    file(GLOB _fibers_twist_patches "${_fibers_twist_patch_dir}/*.patch")
    list(SORT _fibers_twist_patches)
    set(_fibers_twist_patches_hash "")
    foreach(_patch IN LISTS _fibers_twist_patches)
        file(SHA256 "${_patch}" _hash)
        string(SHA256 _fibers_twist_patches_hash "${_fibers_twist_patches_hash}${_hash}")
    endforeach()
    FetchContent_Declare(twist
        GIT_REPOSITORY https://gitlab.com/Lipovsky/twist.git
        GIT_TAG ${FIBERS_TWIST_COMMIT}
        PATCH_COMMAND ${CMAKE_COMMAND}
            -DGIT_EXECUTABLE=${GIT_EXECUTABLE}
            -DPATCH_DIR=${_fibers_twist_patch_dir}
            -DPATCHES_HASH=${_fibers_twist_patches_hash}
            -P ${CMAKE_CURRENT_LIST_DIR}/TwistPatch.cmake)
endif()
FetchContent_MakeAvailable(twist)
