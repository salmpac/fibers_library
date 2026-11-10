add_executable(object_pool_tests object_pool_test.cpp)
target_link_libraries(object_pool_tests PRIVATE fibers::object_pool GTest::gtest_main)
target_compile_options(object_pool_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(object_pool_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 120)

# Custom pool allocators. Separate binary: it replaces the global operator
# new/delete to check that the pool allocates only through its allocator.
add_executable(object_pool_allocator_tests pool_allocator_test.cpp)
target_link_libraries(object_pool_allocator_tests PRIVATE fibers::object_pool GTest::gtest_main)
target_compile_options(object_pool_allocator_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(object_pool_allocator_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 120)

# Pooled (RAII leases) over the object pools and StackPool, also in fibers.
add_executable(pooled_tests pooled_test.cpp)
target_link_libraries(pooled_tests PRIVATE fibers::object_pool fibers::stack fibers::fiber fibers::sched
                                           GTest::gtest_main)
target_compile_options(pooled_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(pooled_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 600)

if(FIBERS_TWIST)
    add_subdirectory(twist)
endif()
