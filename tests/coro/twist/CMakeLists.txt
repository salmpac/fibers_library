# Coroutines over TwistSync (TwistContext, an adapter over twist::ed::sure):
# deterministic simulation (TWIST_SIM) or real threads with fault injection
# (TWIST_FAULTY). Twist links sure privately in fault mode: link it here.
add_executable(coro_twist_tests coro_twist_test.cpp)
target_link_libraries(coro_twist_tests PRIVATE fibers::coro fibers::stack twist sure GTest::gtest_main)
target_compile_features(coro_twist_tests PRIVATE cxx_std_23)  # Twist headers
target_compile_options(coro_twist_tests PRIVATE -Wall -Wextra)

gtest_discover_tests(coro_twist_tests DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 900)
# sure's context.S has no .note.GNU-stack section, which would make the whole
# binary's stack executable (ld warns); our own switch is inline asm in C++.
target_link_options(coro_twist_tests PRIVATE -Wl,-z,noexecstack)
