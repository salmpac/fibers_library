add_executable(blocking_tests blocking_test.cpp nowait_test.cpp)
target_link_libraries(blocking_tests PRIVATE fibers::blocking fibers::thread_pool GTest::gtest_main)
target_compile_options(blocking_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(blocking_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 120)

if(FIBERS_TWIST)
    add_subdirectory(twist)
endif()
