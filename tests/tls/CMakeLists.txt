if(NOT FIBERS_OPENSSL_CLI)
    message(STATUS "fibers::tls tests skipped: no openssl CLI to generate certificates")
    return()
endif()

include(${PROJECT_SOURCE_DIR}/cmake/FibersTlsCerts.cmake)
set(_certs ${CMAKE_CURRENT_BINARY_DIR}/certs)
fibers_tls_certs(tls_test_certs ${_certs})

add_executable(tls_tests tls_test.cpp)
add_dependencies(tls_tests tls_test_certs)
target_link_libraries(tls_tests PRIVATE fibers::tls GTest::gtest_main)
target_compile_options(tls_tests PRIVATE -Wall -Wextra -Wpedantic)
target_compile_definitions(tls_tests PRIVATE
    FIBERS_TLS_CERTS="${_certs}"
    FIBERS_OPENSSL_CLI="${FIBERS_OPENSSL_CLI}")

gtest_discover_tests(tls_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 300)

# tls::Connection: plaintext -> TLS in place, per-operation timeouts.
add_executable(tls_connection_tests tls_connection_test.cpp)
add_dependencies(tls_connection_tests tls_test_certs)
target_link_libraries(tls_connection_tests PRIVATE fibers::tls GTest::gtest_main)
target_compile_options(tls_connection_tests PRIVATE -Wall -Wextra -Wpedantic)
target_compile_definitions(tls_connection_tests PRIVATE FIBERS_TLS_CERTS="${_certs}")

gtest_discover_tests(tls_connection_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 300)
