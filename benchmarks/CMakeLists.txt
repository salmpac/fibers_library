find_package(benchmark REQUIRED)

foreach(_bench queue_bench thread_pool_bench)
    add_executable(${_bench} ${_bench}.cpp)
    target_link_libraries(${_bench} PRIVATE fibers::thread_pool benchmark::benchmark_main)
    target_compile_options(${_bench} PRIVATE -Wall -Wextra)
endforeach()
