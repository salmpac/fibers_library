add_executable(fs_tests fs_test.cpp)
target_link_libraries(fs_tests PRIVATE fibers::fs GTest::gtest_main)
target_compile_options(fs_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(fs_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 300)
