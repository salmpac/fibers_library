# OneshotCore / Promise / the fiber Oneshot and Future over TwistSync:
# deterministic simulation (TWIST_SIM) or real threads with fault injection
# (TWIST_FAULTY). Twist links sure privately in fault mode: link it here.
add_executable(oneshot_twist_tests oneshot_twist_test.cpp)
target_link_libraries(oneshot_twist_tests PRIVATE fibers::fiber fibers::wait twist sure GTest::gtest_main)
target_compile_features(oneshot_twist_tests PRIVATE cxx_std_23)  # Twist headers
target_compile_options(oneshot_twist_tests PRIVATE -Wall -Wextra)

gtest_discover_tests(oneshot_twist_tests DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 1800)
# sure's context.S has no .note.GNU-stack section (see tests/coro/twist).
target_link_options(oneshot_twist_tests PRIVATE -Wl,-z,noexecstack)
