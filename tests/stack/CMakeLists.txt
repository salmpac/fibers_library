add_executable(stack_tests stack_test.cpp)
target_link_libraries(stack_tests PRIVATE fibers::stack fibers::queue GTest::gtest_main)
target_compile_options(stack_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(stack_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 120)
