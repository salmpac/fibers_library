# Examples (FIBERS_BUILD_EXAMPLES=ON): an echo server on fibers, a load
# generator on fibers, and a plain epoll echo server as the reference.
# See examples/README.md.
foreach(example echo_server echo_client echo_epoll_baseline)
    add_executable(${example} ${example}.cpp)
    target_link_libraries(${example} PRIVATE fibers::net fibers::sync)
    target_compile_options(${example} PRIVATE -Wall -Wextra)
endforeach()

# TLS echo server / client (fibers::tls). The test PKI is generated next to
# the binary (tls_certs/) when the openssl CLI is available.
if(FIBERS_TLS)
    add_executable(tls_echo tls_echo.cpp)
    target_link_libraries(tls_echo PRIVATE fibers::tls fibers::sync)
    target_compile_options(tls_echo PRIVATE -Wall -Wextra)
    if(FIBERS_OPENSSL_CLI)
        include(${PROJECT_SOURCE_DIR}/cmake/FibersTlsCerts.cmake)
        fibers_tls_certs(tls_echo_certs ${CMAKE_CURRENT_BINARY_DIR}/tls_certs)
        add_dependencies(tls_echo tls_echo_certs)
    endif()
endif()
