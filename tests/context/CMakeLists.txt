add_executable(context_tests context_test.cpp second_tu.cpp)
target_link_libraries(context_tests PRIVATE fibers::context GTest::gtest_main)
target_compile_options(context_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(context_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 120)
