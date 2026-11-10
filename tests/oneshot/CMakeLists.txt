# OneshotCore / OneshotState / Promise and the fiber Oneshot / Future.
add_executable(oneshot_tests oneshot_test.cpp)
target_link_libraries(oneshot_tests PRIVATE fibers::fiber fibers::sched fibers::wait GTest::gtest_main)
target_compile_options(oneshot_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(oneshot_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 300)

# Promise::set() with a throwing, explicit-only construction must be a clear
# compile-time error: the test builds a target that must not compile and
# looks for the static_assert's message.
add_library(oneshot_set_rejects OBJECT EXCLUDE_FROM_ALL set_rejects.cpp)
target_link_libraries(oneshot_set_rejects PRIVATE fibers::fiber fibers::sched fibers::wait)
add_test(NAME oneshot.set_rejects_throwing_construction
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target oneshot_set_rejects)
set_tests_properties(oneshot.set_rejects_throwing_construction PROPERTIES
    PASS_REGULAR_EXPRESSION "set\\(\\) never throws: build the value yourself"
    TIMEOUT 300)

if(FIBERS_TWIST)
    add_subdirectory(twist)
endif()
