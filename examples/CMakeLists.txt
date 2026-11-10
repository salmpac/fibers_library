# Examples (FIBERS_BUILD_EXAMPLES=ON): an echo server on fibers, a load
# generator on fibers, a plain epoll echo server as the reference, TLS and
# HTTP servers, and small programs for the synchronization primitives.
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

# HTTP examples (need FIBERS_HTTP): a hello-world server (the wrk benchmark
# target), a small REST service, and one with Prometheus metrics.
if(FIBERS_HTTP)
    foreach(example http_hello http_rest)
        add_executable(${example} ${example}.cpp)
        target_link_libraries(${example} PRIVATE fibers::http fibers::sync)
        target_compile_options(${example} PRIVATE -Wall -Wextra)
    endforeach()
    # A REST service with Prometheus metrics (fibers::metrics) on /metrics.
    add_executable(metrics_server metrics_server.cpp)
    target_link_libraries(metrics_server PRIVATE fibers::http fibers::metrics fibers::sync)
    target_compile_options(metrics_server PRIVATE -Wall -Wextra)
endif()

# APNs sender (needs FIBERS_HTTP2): token auth, HTTP/2 over TLS.
if(FIBERS_HTTP2)
    add_executable(apns_send apns_send.cpp)
    target_link_libraries(apns_send PRIVATE fibers::http2)
    target_compile_options(apns_send PRIVATE -Wall -Wextra)
endif()

# Synchronization primitives (examples/sync/): short tutorial programs for
# fiber::Mutex, WaitGroup, Oneshot / Future, Semaphore and Event. Each runs
# in well under a second and exits 0 on success, so each is also a ctest
# smoke test when the tests are configured.
foreach(example mutex_counter wait_group_fanout promise_future semaphore_pool event_broadcast)
    add_executable(${example} sync/${example}.cpp)
    target_link_libraries(${example} PRIVATE fibers::fiber fibers::sync)
    target_compile_options(${example} PRIVATE -Wall -Wextra)
    if(FIBERS_BUILD_TESTS)
        add_test(NAME example.sync.${example} COMMAND ${example})
        set_tests_properties(example.sync.${example} PROPERTIES TIMEOUT 120 LABELS examples)
    endif()
endforeach()

# fibers::Context: a deadline / cancel for a request's tree of fibers, over
# loopback sockets (the cancellable waits are the network ones).
add_executable(context_deadline sync/context_deadline.cpp)
target_link_libraries(context_deadline PRIVATE fibers::net fibers::sync)
target_compile_options(context_deadline PRIVATE -Wall -Wextra)
if(FIBERS_BUILD_TESTS)
    add_test(NAME example.sync.context_deadline COMMAND context_deadline)
    set_tests_properties(example.sync.context_deadline PROPERTIES TIMEOUT 120 LABELS examples)
endif()
