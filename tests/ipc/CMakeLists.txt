add_executable(ipc_tests ring_test.cpp)
target_link_libraries(ipc_tests PRIVATE fibers::ipc GTest::gtest_main)
target_compile_options(ipc_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(ipc_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 120)
