include(CMakeFindDependencyMacro)
find_dependency(Threads)
# fibers::http (when built with FIBERS_HTTP) links llhttp::llhttp.
find_package(llhttp 9 CONFIG QUIET)
include("${CMAKE_CURRENT_LIST_DIR}/fibersTargets.cmake")
# fibers::tls, when the package was built with FIBERS_TLS.
if(EXISTS "${CMAKE_CURRENT_LIST_DIR}/fibersTlsTargets.cmake")
    find_dependency(OpenSSL 3.0 COMPONENTS SSL Crypto)
    include("${CMAKE_CURRENT_LIST_DIR}/fibersTlsTargets.cmake")
endif()
# fibers::http2, when the package was built with FIBERS_HTTP2 (links -lnghttp2).
if(EXISTS "${CMAKE_CURRENT_LIST_DIR}/fibersHttp2Targets.cmake")
    include("${CMAKE_CURRENT_LIST_DIR}/fibersHttp2Targets.cmake")
endif()
