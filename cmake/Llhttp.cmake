# llhttp (https://github.com/nodejs/llhttp, MIT): the HTTP/1.1 parser of
# fibers::http. Defines FIBERS_LLHTTP_TARGET.
#
# * An installed package first (nix: pkgs.llhttp in the dev shell and the
#   flake builds; distro packages; anything on CMAKE_PREFIX_PATH).
# * Otherwise the release sources (generated C + CMakeLists) are fetched
#   with FetchContent and built as a static library, with this project's
#   flags (sanitizers included). FETCHCONTENT_SOURCE_DIR_LLHTTP=<dir> points
#   it at a local copy instead.

find_package(llhttp 9 CONFIG QUIET)

if(TARGET llhttp::llhttp)
    set(FIBERS_LLHTTP_TARGET llhttp::llhttp)
    set(FIBERS_LLHTTP_INSTALLABLE ON)
    message(STATUS "fibers::http: llhttp ${llhttp_VERSION} (installed package)")
else()
    include(FetchContent)
    enable_language(C)
    # Keep in sync with nixpkgs' llhttp (flake.lock) when bumping.
    FetchContent_Declare(llhttp
        URL https://github.com/nodejs/llhttp/archive/refs/tags/release/v9.4.3.tar.gz
        URL_HASH SHA256=1eb813c7437b31a87496a1cd3ed79f00746720f5e7e29c79b42c02cb69f36c39)
    set(LLHTTP_BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
    set(LLHTTP_BUILD_STATIC_LIBS ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(llhttp)
    set_target_properties(llhttp_static PROPERTIES POSITION_INDEPENDENT_CODE ON)
    set(FIBERS_LLHTTP_TARGET llhttp_static)
    set(FIBERS_LLHTTP_INSTALLABLE OFF)
    message(STATUS "fibers::http: llhttp 9.4.3 (fetched, static)")
endif()
