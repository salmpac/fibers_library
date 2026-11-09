find_package(benchmark REQUIRED)

foreach(_bench queue_bench thread_pool_bench object_pool_bench)
    add_executable(${_bench} ${_bench}.cpp)
    target_link_libraries(${_bench} PRIVATE fibers::thread_pool fibers::object_pool benchmark::benchmark_main)
    target_compile_options(${_bench} PRIVATE -Wall -Wextra)
endforeach()

add_executable(poller_bench poller_bench.cpp)
target_link_libraries(poller_bench PRIVATE fibers::poller fibers::ipc benchmark::benchmark_main)
target_compile_options(poller_bench PRIVATE -Wall -Wextra)

add_executable(blocking_bench blocking_bench.cpp)
target_link_libraries(blocking_bench PRIVATE fibers::blocking fibers::thread_pool benchmark::benchmark_main)
target_compile_options(blocking_bench PRIVATE -Wall -Wextra)
