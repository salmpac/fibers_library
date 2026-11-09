# fibers::http: the connection engine over a scripted in-memory stream
# (parser edge cases, limits, framing; no fibers), and the server / client
# over real sockets on a Scheduler (timeouts, concurrency, shutdown).
add_executable(http_tests http_parse_test.cpp http_server_test.cpp)
target_link_libraries(http_tests PRIVATE fibers::http GTest::gtest_main)
target_compile_options(http_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(http_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 300)
