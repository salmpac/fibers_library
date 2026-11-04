add_executable(poller_tests poller_test.cpp)
target_link_libraries(poller_tests PRIVATE fibers::poller fibers::object_pool GTest::gtest_main)
target_compile_options(poller_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(poller_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 120)

if(FIBERS_TWIST)
    add_subdirectory(twist)
endif()
