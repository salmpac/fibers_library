# SemaphoreCore and the fiber Semaphore over TwistSync: deterministic
# simulation (TWIST_SIM) or real threads with fault injection (TWIST_FAULTY).
# Twist links sure privately in fault mode: link it here.
add_executable(semaphore_twist_tests semaphore_twist_test.cpp)
target_link_libraries(semaphore_twist_tests PRIVATE fibers::fiber fibers::wait twist sure GTest::gtest_main)
target_compile_features(semaphore_twist_tests PRIVATE cxx_std_23)  # Twist headers
target_compile_options(semaphore_twist_tests PRIVATE -Wall -Wextra)

gtest_discover_tests(semaphore_twist_tests DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 1800)
# sure's context.S has no .note.GNU-stack section (see tests/coro/twist).
target_link_options(semaphore_twist_tests PRIVATE -Wl,-z,noexecstack)
