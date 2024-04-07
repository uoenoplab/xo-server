cmake_minimum_required(VERSION 3.22)
project(xo-server)
enable_language(C)

include(FetchContent)

include(${CMAKE_SOURCE_DIR}/cmake/modules/Findlibrados2.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/modules/FindProtobufC.cmake)
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
    src/conn_migration.c
    src/md5.c
    src/util.c
)

#add_library(demo_protobuf ${PROTO_SRC} ${PROTO_HEAD})

find_package(LibXml2 REQUIRED)
find_package(librados2 REQUIRED)
find_package(uriparser REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(INIH REQUIRED inih)

find_program(Protobuf_PROTOC-C_EXECUTABLE
    NAMES protoc-c
    DOC "The Google Protocol Buffers Compiler C"
    PATHS
    ${Protobuf_SRC_ROOT_FOLDER}/vsprojects/${_PROTOBUF_ARCH_DIR}Release
    ${Protobuf_SRC_ROOT_FOLDER}/vsprojects/${_PROTOBUF_ARCH_DIR}Debug
)
mark_as_advanced(Protobuf_PROTOC-C_EXECUTABLE)
PROTOBUF_GENERATE_C(PROTO_SRC PROTO_HEAD proto/socket_serialize.proto)

# build benchmark library
add_library(http_server STATIC
    ${PROTO_SRC}
    ${C_SRC})
target_include_directories(http_server PRIVATE
    include
    ${CMAKE_CURRENT_BINARY_DIR} # for proto
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
	-fstack-protector-all -Wall -Wextra -ggdb -O3 -g)
#-fsanitize=undefined,address -fno-omit-frame-pointer -fstack-protector -Wall -Wextra -ggdb -O3 -g)
target_link_options(http_server PRIVATE
	-fstack-protector-all -Wall -Wextra -ggdb -O3 -g)
#-fsanitize=undefined,address -fno-omit-frame-pointer -fstack-protector -Wall -Wextra -ggdb -O3 -g)

# build driver application
add_executable(server.out src/server.c src/tls.c)
target_include_directories(server.out PUBLIC
    ${llhttp_SOURCE_DIR}/include
    ${INIH_INCLUDE_DIRS}
    ${LIBXML2_INCLUDE_DIR}
    include)
target_link_libraries(server.out PUBLIC
    http_server
    ${INIH_LIBRARIES}
    OpenSSL::Crypto
    OpenSSL::SSL)
set_target_properties(server.out PROPERTIES
    C_STANDARD 11)
target_compile_options(server.out PRIVATE
    ${INIH_CFLAGS_OTHER}
    -fstack-protector-all -O3 -Wall -Wextra -g)
#-fsanitize=address,undefined -fno-omit-frame-pointer -fstack-protector -O3 -Wall -Wextra -g)
target_link_options(server.out PRIVATE
    -fstack-protector-all -O3 -Wall -Wextra -g)
#-fsanitize=address,undefined -fno-omit-frame-pointer -fstack-protector -O3 -Wall -Wextra -g)

## install binary, library, and public header
#install(TARGETS s3_benchmark.out DESTINATION bin)
#install(TARGETS benchmark DESTINATION lib)
#install(FILES include/benchmark.hh DESTINATION include)

file(COPY ${CMAKE_SOURCE_DIR}/assets DESTINATION ${CMAKE_BINARY_DIR})

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
