# Examples (FIBERS_BUILD_EXAMPLES=ON): an echo server on fibers, a load
# generator on fibers, and a plain epoll echo server as the reference.
# See examples/README.md.
foreach(example echo_server echo_client echo_epoll_baseline)
    add_executable(${example} ${example}.cpp)
    target_link_libraries(${example} PRIVATE fibers::net fibers::sync)
    target_compile_options(${example} PRIVATE -Wall -Wextra)
endforeach()
