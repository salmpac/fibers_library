# fibers::metrics with sync::TwistSync under Twist: deterministic simulation
# (TWIST_SIM) or real threads with fault injection (TWIST_FAULTY).
add_executable(metrics_twist_tests metrics_twist_test.cpp)
target_link_libraries(metrics_twist_tests PRIVATE fibers::metrics twist GTest::gtest_main)
target_compile_features(metrics_twist_tests PRIVATE cxx_std_23)  # Twist headers
target_compile_options(metrics_twist_tests PRIVATE -Wall -Wextra)

gtest_discover_tests(metrics_twist_tests DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 900)
# sure's context.S has no .note.GNU-stack section (see tests/coro/twist).
target_link_options(metrics_twist_tests PRIVATE -Wl,-z,noexecstack)
