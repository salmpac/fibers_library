add_executable(thread_pool_tests
    task_test.cpp
    spsc_queue_test.cpp
    lock_free_spmc_queue_test.cpp
    sharded_queue_test.cpp
    thread_pool_test.cpp)
target_link_libraries(thread_pool_tests PRIVATE fibers::thread_pool GTest::gtest_main)
target_compile_options(thread_pool_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(thread_pool_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 120)

if(FIBERS_TWIST)
    add_subdirectory(twist)
endif()
