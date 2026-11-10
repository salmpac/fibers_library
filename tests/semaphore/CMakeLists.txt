# SemaphoreCore and the fiber Semaphore / SemaphoreGuard.
add_executable(semaphore_tests semaphore_test.cpp)
target_link_libraries(semaphore_tests PRIVATE fibers::fiber fibers::sched fibers::wait GTest::gtest_main)
target_compile_options(semaphore_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(semaphore_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 300)

if(FIBERS_TWIST)
    add_subdirectory(twist)
endif()
