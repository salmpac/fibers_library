add_executable(fiber_tests fiber_test.cpp)
target_link_libraries(fiber_tests PRIVATE fibers::fiber fibers::sched fibers::wait GTest::gtest_main)
target_compile_options(fiber_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(fiber_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 300)

add_executable(fiber_blocking_tests blocking_test.cpp)
target_link_libraries(fiber_blocking_tests PRIVATE fibers::fiber GTest::gtest_main)
target_compile_options(fiber_blocking_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(fiber_blocking_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 300)

add_executable(fiber_context_tests context_test.cpp)
target_link_libraries(fiber_context_tests PRIVATE fibers::fiber fibers::sched fibers::wait GTest::gtest_main)
target_compile_options(fiber_context_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(fiber_context_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 300)

if(FIBERS_TWIST)
    add_subdirectory(twist)
endif()
