add_executable(net_tests net_test.cpp)
target_link_libraries(net_tests PRIVATE fibers::net GTest::gtest_main)
target_compile_options(net_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(net_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 300)

add_executable(buffered_reader_tests buffered_reader_test.cpp)
target_link_libraries(buffered_reader_tests PRIVATE fibers::net GTest::gtest_main)
target_compile_options(buffered_reader_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(buffered_reader_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 300)

# Resolver, Connection (plaintext; the TLS part is in tests/tls) and
# ConnectionPool.
foreach(_t resolver connection connection_pool)
    add_executable(${_t}_tests ${_t}_test.cpp)
    target_link_libraries(${_t}_tests PRIVATE fibers::net GTest::gtest_main)
    target_compile_options(${_t}_tests PRIVATE -Wall -Wextra -Wpedantic)
    gtest_discover_tests(${_t}_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 300)
endforeach()

# Network waits under a fiber::BasicContext (cancellation, deadlines).
add_executable(net_context_tests context_test.cpp)
target_link_libraries(net_context_tests PRIVATE fibers::net GTest::gtest_main)
target_compile_options(net_context_tests PRIVATE -Wall -Wextra -Wpedantic)
gtest_discover_tests(net_context_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 300)

if(FIBERS_TWIST)
    add_subdirectory(twist)
endif()
