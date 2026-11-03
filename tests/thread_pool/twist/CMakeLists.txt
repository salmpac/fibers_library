# ThreadPool<TwistSync> under Twist: deterministic simulation (TWIST_SIM) or
# real threads with fault injection (TWIST_FAULTY).
add_executable(thread_pool_twist_tests thread_pool_twist_test.cpp)
target_link_libraries(thread_pool_twist_tests PRIVATE fibers::thread_pool twist GTest::gtest_main)
target_compile_features(thread_pool_twist_tests PRIVATE cxx_std_23)  # Twist headers
target_compile_options(thread_pool_twist_tests PRIVATE -Wall -Wextra)

gtest_discover_tests(thread_pool_twist_tests DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 900)
