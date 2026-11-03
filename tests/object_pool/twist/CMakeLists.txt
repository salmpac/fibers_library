# Lock-free object pools with TwistSync under Twist: deterministic simulation
# (TWIST_SIM) or real threads with fault injection (TWIST_FAULTY).
add_executable(object_pool_twist_tests object_pool_twist_test.cpp)
target_link_libraries(object_pool_twist_tests PRIVATE fibers::object_pool twist GTest::gtest_main)
target_compile_features(object_pool_twist_tests PRIVATE cxx_std_23)  # Twist headers
target_compile_options(object_pool_twist_tests PRIVATE -Wall -Wextra)

gtest_discover_tests(object_pool_twist_tests DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 900)
