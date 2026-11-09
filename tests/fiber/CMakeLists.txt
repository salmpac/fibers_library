add_executable(fiber_tests fiber_test.cpp)
target_link_libraries(fiber_tests PRIVATE fibers::fiber fibers::sched fibers::wait GTest::gtest_main)
target_compile_options(fiber_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(fiber_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 300)
