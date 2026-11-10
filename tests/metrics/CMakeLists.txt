# fibers::metrics: exposition format, instruments, registry, concurrency
# (threads and fibers on a Scheduler), built-in collectors.
add_executable(metrics_tests metrics_test.cpp)
target_link_libraries(metrics_tests PRIVATE fibers::metrics fibers::fiber GTest::gtest_main)
target_compile_options(metrics_tests PRIVATE -Wall -Wextra -Wpedantic)

gtest_discover_tests(metrics_tests DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 300)

# Prints a representative exposition (every instrument kind, escaping, the
# built-in collectors of a live Scheduler) for `promtool check metrics`.
add_executable(metrics_dump metrics_dump.cpp)
target_link_libraries(metrics_dump PRIVATE fibers::metrics fibers::fiber)
target_compile_options(metrics_dump PRIVATE -Wall -Wextra -Wpedantic)

find_program(FIBERS_PROMTOOL promtool)
if(FIBERS_PROMTOOL)
    add_test(NAME metrics_promtool_check
        COMMAND sh -c "set -e; '$<TARGET_FILE:metrics_dump>' > metrics_dump.prom; '${FIBERS_PROMTOOL}' check metrics < metrics_dump.prom"
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
    set_tests_properties(metrics_promtool_check PROPERTIES TIMEOUT 120)
else()
    message(STATUS "promtool not found: the metrics exposition is not validated with it")
endif()

if(FIBERS_TWIST)
    add_subdirectory(twist)
endif()
