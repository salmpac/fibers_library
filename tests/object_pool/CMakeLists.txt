add_executable(object_pool_tests object_pool_test.cpp)
target_link_libraries(object_pool_tests PRIVATE fibers::object_pool GTest::gtest_main)
target_compile_options(object_pool_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(object_pool_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 120)

if(FIBERS_TWIST)
    add_subdirectory(twist)
endif()
