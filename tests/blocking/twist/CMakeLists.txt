# BlockingPool<TwistSync> under Twist: deterministic simulation (TWIST_SIM) or
# real threads with fault injection (TWIST_FAULTY).
add_executable(blocking_twist_tests blocking_twist_test.cpp)
target_link_libraries(blocking_twist_tests PRIVATE fibers::blocking twist GTest::gtest_main)
target_compile_features(blocking_twist_tests PRIVATE cxx_std_23)  # Twist headers
target_compile_options(blocking_twist_tests PRIVATE -Wall -Wextra)

gtest_discover_tests(blocking_twist_tests DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 900)
