# Twist: systematic concurrency testing (fault injection + deterministic simulation).
# https://gitlab.com/Lipovsky/twist
#
# Mode is selected with Twist's own cache options, e.g.
#   -DTWIST_FAULTY=ON                                  threads + fault injection
#   -DTWIST_FAULTY=ON -DTWIST_SIM=ON [-DTWIST_SIM_ISOLATION=ON]   simulation
#
# Sources of Twist and of everything it fetches are pinned by flake.lock: the nix
# dev shell / build export FIBERS_DEP_<NAME>=/nix/store/... and we turn them into
# FETCHCONTENT_SOURCE_DIR_<NAME>, so no network access is needed. Without those
# variables FetchContent downloads the default branches.

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

FetchContent_Declare(
    twist
    GIT_REPOSITORY https://gitlab.com/Lipovsky/twist.git
    GIT_TAG master)
FetchContent_MakeAvailable(twist)
