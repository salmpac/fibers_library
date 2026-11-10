# fibers::http2: the client against an in-test nghttp2 server (h2c / TLS,
# scripted GOAWAY, delays, kills, PING) and against nghttpd (FIBERS_NGHTTPD,
# h2c / TLS), JWT signing and the APNs client against a mock APNs.
if(NOT FIBERS_OPENSSL_CLI)
    message(STATUS "fibers::http2 tests skipped: no openssl CLI to generate certificates")
    return()
endif()

include(${PROJECT_SOURCE_DIR}/cmake/FibersTlsCerts.cmake)
set(_certs ${CMAKE_CURRENT_BINARY_DIR}/certs)
fibers_tls_certs(http2_test_certs ${_certs})

add_executable(http2_tests http2_client_test.cpp apns_test.cpp)
add_dependencies(http2_tests http2_test_certs)
target_link_libraries(http2_tests PRIVATE fibers::http2 GTest::gtest_main)
target_compile_options(http2_tests PRIVATE -Wall -Wextra -Wpedantic)
target_compile_definitions(http2_tests PRIVATE
    FIBERS_TLS_CERTS="${_certs}"
    FIBERS_HTTP2_TEST_DIR="${CMAKE_CURRENT_BINARY_DIR}")
if(FIBERS_NGHTTPD)
    target_compile_definitions(http2_tests PRIVATE FIBERS_NGHTTPD="${FIBERS_NGHTTPD}")
endif()

gtest_discover_tests(http2_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 300)
