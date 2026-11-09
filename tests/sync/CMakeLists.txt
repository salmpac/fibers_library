add_executable(affinity_tests affinity_test.cpp)
target_link_libraries(affinity_tests PRIVATE fibers::sync fibers::thread_pool fibers::poller fibers::blocking
    GTest::gtest_main)
target_compile_options(affinity_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(affinity_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 120)
