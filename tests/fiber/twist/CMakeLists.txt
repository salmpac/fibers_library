# Fibers over TwistSync (TwistContext over twist::ed::sure): deterministic
# simulation (TWIST_SIM) or real threads with fault injection (TWIST_FAULTY).
# Twist links sure privately in fault mode: link it here.
add_executable(fiber_twist_tests fiber_twist_test.cpp)
target_link_libraries(fiber_twist_tests PRIVATE fibers::fiber fibers::sched twist sure GTest::gtest_main)
target_compile_features(fiber_twist_tests PRIVATE cxx_std_23)  # Twist headers
target_compile_options(fiber_twist_tests PRIVATE -Wall -Wextra)

gtest_discover_tests(fiber_twist_tests DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 1800)
# sure's context.S has no .note.GNU-stack section (see tests/coro/twist).
target_link_options(fiber_twist_tests PRIVATE -Wl,-z,noexecstack)

# this_fiber::blocking: the handshake with a BlockingPool<TwistSync> thread.
add_executable(fiber_blocking_twist_tests blocking_twist_test.cpp)
target_link_libraries(fiber_blocking_twist_tests PRIVATE fibers::fiber twist sure GTest::gtest_main)
target_compile_features(fiber_blocking_twist_tests PRIVATE cxx_std_23)  # Twist headers
target_compile_options(fiber_blocking_twist_tests PRIVATE -Wall -Wextra)

gtest_discover_tests(fiber_blocking_twist_tests DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 1800)
target_link_options(fiber_blocking_twist_tests PRIVATE -Wl,-z,noexecstack)

# Request contexts: wait::ContextCore's claim / delist protocol and
# fiber::BasicContext's parent -> child propagation, on plain threads.
add_executable(context_twist_tests context_twist_test.cpp)
target_link_libraries(context_twist_tests PRIVATE fibers::fiber twist sure GTest::gtest_main)
target_compile_features(context_twist_tests PRIVATE cxx_std_23)  # Twist headers
target_compile_options(context_twist_tests PRIVATE -Wall -Wextra)

gtest_discover_tests(context_twist_tests DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 1800)
target_link_options(context_twist_tests PRIVATE -Wl,-z,noexecstack)
