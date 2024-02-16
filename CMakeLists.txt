cmake_minimum_required(VERSION 3.22)
project(xo-server)
enable_language(C)

include(FetchContent)

include(${CMAKE_SOURCE_DIR}/cmake/modules/Findlibrados2.cmake)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules")

#option(BUILD_SHARED_LIBS "Build using shared libraries" OFF)
FetchContent_Declare(llhttp
  URL "https://github.com/nodejs/llhttp/archive/refs/tags/release/v8.1.0.tar.gz")

set(BUILD_SHARED_LIBS OFF CACHE INTERNAL "")
set(BUILD_STATIC_LIBS ON CACHE INTERNAL "")
FetchContent_MakeAvailable(llhttp)

## Link with the llhttp_static target
#target_link_libraries(${EXAMPLE_PROJECT_NAME} ${PROJECT_LIBRARIES} llhttp_static ${PROJECT_NAME})

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Debug")
endif()

if (POLICY CMP0074)
  cmake_policy(SET CMP0074 NEW)
endif()

set(C_SRC
    src/http_client.c
    src/object_store.c
    src/md5.c
    src/util.c
)

find_package(LibXml2 REQUIRED)
find_package(librados2 REQUIRED)
find_package(uriparser REQUIRED)
find_package(OpenSSL REQUIRED)

# build benchmark library
add_library(http_server STATIC
    ${C_SRC})
target_include_directories(http_server PRIVATE
    include
    ${URIPARSER_INCLUDE_DIR}
    ${llhttp_SOURCE_DIR}/include
    ${LIBXML2_INCLUDE_DIR}
    ${LIBRADOS2_INCLUDE_DIRS})
target_link_libraries(http_server PRIVATE
    llhttp_static
    uriparser::uriparser
    LibXml2::LibXml2
    OpenSSL::Crypto
    ${LIBRADOS2_LIBRARIES})
set_target_properties(http_server PROPERTIES
        POSITION_INDEPENDENT_CODE ON
        C_STANDARD 11)
target_compile_options(http_server PRIVATE
        -O3 -Wall -g)

# build driver application
add_executable(server.out src/server.c)
target_include_directories(server.out PUBLIC
    ${llhttp_SOURCE_DIR}/include
    ${LIBXML2_INCLUDE_DIR}
    include)
target_link_libraries(server.out PUBLIC
    http_server)
set_target_properties(server.out PROPERTIES
    C_STANDARD 11)
target_compile_options(server.out PRIVATE
    -O3 -Wall -g)

## install binary, library, and public header
#install(TARGETS s3_benchmark.out DESTINATION bin)
#install(TARGETS benchmark DESTINATION lib)
#install(FILES include/benchmark.hh DESTINATION include)

message(STATUS "Building xo server binary")

# uninstall target
#if(NOT TARGET uninstall)
#  configure_file(
#    "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
#    "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
#    IMMEDIATE @ONLY)
#
#  add_custom_target(uninstall
#    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)
#endif()

