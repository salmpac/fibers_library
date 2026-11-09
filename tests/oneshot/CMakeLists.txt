# OneshotCore / OneshotState / Promise and the fiber Oneshot / Future.
add_executable(oneshot_tests oneshot_test.cpp)
target_link_libraries(oneshot_tests PRIVATE fibers::fiber fibers::sched fibers::wait GTest::gtest_main)
target_compile_options(oneshot_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(oneshot_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 300)

if(FIBERS_TWIST)
    add_subdirectory(twist)
endif()
