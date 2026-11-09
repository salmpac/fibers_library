# net::BasicPollDesc over TwistSync: the readiness-word protocol and close(),
# with FakeBackend pollers (fake fds stand in for sockets) under Twist:
# deterministic simulation (TWIST_SIM) or fault injection (TWIST_FAULTY).
add_executable(net_twist_tests net_twist_test.cpp)
target_link_libraries(net_twist_tests PRIVATE fibers::net twist sure GTest::gtest_main)
target_compile_features(net_twist_tests PRIVATE cxx_std_23)  # Twist headers
target_compile_options(net_twist_tests PRIVATE -Wall -Wextra)

gtest_discover_tests(net_twist_tests DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 1800)
# sure's context.S has no .note.GNU-stack section (see tests/coro/twist).
target_link_options(net_twist_tests PRIVATE -Wl,-z,noexecstack)
