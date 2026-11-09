# EventCore / WaitGroupCore and the fiber Event / WaitGroup.
add_executable(wait_group_tests wait_group_test.cpp)
target_link_libraries(wait_group_tests PRIVATE fibers::fiber fibers::sched fibers::wait GTest::gtest_main)
target_compile_options(wait_group_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(wait_group_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 300)

if(FIBERS_TWIST)
    add_subdirectory(twist)
endif()
