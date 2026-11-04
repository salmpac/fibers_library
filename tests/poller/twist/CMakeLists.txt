# Poller and in-process ipc ring with sync::TwistSync + FakeBackend under
# Twist: deterministic simulation (TWIST_SIM) or real threads with fault
# injection (TWIST_FAULTY).
add_executable(poller_twist_tests poller_twist_test.cpp)
target_link_libraries(poller_twist_tests PRIVATE fibers::poller fibers::ipc twist GTest::gtest_main)
target_compile_features(poller_twist_tests PRIVATE cxx_std_23)  # Twist headers
target_compile_options(poller_twist_tests PRIVATE -Wall -Wextra)

gtest_discover_tests(poller_twist_tests DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 900)
