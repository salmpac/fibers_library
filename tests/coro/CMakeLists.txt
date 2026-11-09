add_executable(coro_tests coro_test.cpp)
target_link_libraries(coro_tests PRIVATE fibers::coro fibers::stack GTest::gtest_main)
target_compile_options(coro_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(coro_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 120)

if(FIBERS_TWIST)
    add_subdirectory(twist)
endif()
