add_executable(net_tests net_test.cpp)
target_link_libraries(net_tests PRIVATE fibers::net GTest::gtest_main)
target_compile_options(net_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(net_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 300)

if(FIBERS_TWIST)
    add_subdirectory(twist)
endif()
