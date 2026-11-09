include(CMakeFindDependencyMacro)
find_dependency(Threads)
include("${CMAKE_CURRENT_LIST_DIR}/fibersTargets.cmake")
# fibers::tls, when the package was built with FIBERS_TLS.
if(EXISTS "${CMAKE_CURRENT_LIST_DIR}/fibersTlsTargets.cmake")
    find_dependency(OpenSSL 3.0 COMPONENTS SSL Crypto)
    include("${CMAKE_CURRENT_LIST_DIR}/fibersTlsTargets.cmake")
endif()
