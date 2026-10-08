cmake_minimum_required(VERSION 3.5)
project(raft-kv)

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)


add_definitions(-Wall)
add_definitions(-DDEBUG)

if(CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_GNUCXX OR CMAKE_COMPILER_IS_CLANG)
    add_compile_options(
        -Wno-format
        -Wno-reorder 
        -Wno-unused-function 
        -Wno-pessimizing-move 
        -Wno-narrowing
        -Wno-sign-compare
        -Wno-unused-variable
        -Wno-int-in-bool-context
        -Wno-switch
        -Wno-class-memaccess
        -Wno-unused-but-set-variable
        -Wno-return-type
    )
endif()


find_package(leveldb REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(Protobuf REQUIRED)
find_package(gflags REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Boost REQUIRED COMPONENTS system filesystem context)
find_library(ROCKSDB_LIB rocksdb)
pkg_check_modules(dependencies
                  msgpack REQUIRED)


# 头文件路径
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/kv-system/apply
    ${CMAKE_CURRENT_SOURCE_DIR}/kv-system/raft
    ${CMAKE_CURRENT_SOURCE_DIR}/kv-system/
    ${dependencies_INCLUDE_DIRS}
    ${Boost_INCLUDE_DIRS} 
    ${PROTOBUF_INCLUDE_DIR}
    ${gflags_INCLUDE_DIRS}
    ${OPENSSL_INCLUDE_DIR}
    ${leveldb_INCLUDE_DIRS}
    ${ZOOKEEPER_INCLUDE_DIRS}
)


# 库文件路径
set(LIBS
    gflags
    glog
    ${dependencies_LIBRARIES}
    pthread
    brpc
    zookeeper_mt
    leveldb
    jsoncpp
    ${Protobuf_LIBRARIES}
    ${Boost_LIBRARIES}
    ${ROCKSDB_LIB}
    ${OPENSSL_LIBRARIES}
    ${ZOOKEEPER_LIBRARIES}
)

# 生成protobuf代码
file(GLOB_RECURSE PROTO_FILES ${CMAKE_CURRENT_SOURCE_DIR}/kv-system/resource/*.proto)
set(PROTO_SRC "")
set(PROTO_HEADER "")
foreach(msg ${PROTO_FILES})
    get_filename_component(PREFIX ${msg} NAME_WE)

    list(APPEND PROTO_SRC "${CMAKE_CURRENT_SOURCE_DIR}/kv-system/resource/${PREFIX}.pb.cc")
    list(APPEND PROTO_HEADER "${CMAKE_CURRENT_SOURCE_DIR}/kv-system/resource/${PREFIX}.pb.h")

    add_custom_command(
            OUTPUT "${CMAKE_CURRENT_SOURCE_DIR}/kv-system/resource/${PREFIX}.pb.cc"
                    "${CMAKE_CURRENT_SOURCE_DIR}/kv-system/resource/${PREFIX}.pb.h"
            COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
            ARGS -I ${CMAKE_CURRENT_SOURCE_DIR}/kv-system/resource/ 
                 --cpp_out ${CMAKE_CURRENT_SOURCE_DIR}/kv-system/resource/
                 ${msg}
            DEPENDS ${msg}
            COMMENT "Running C++ protocol buffer compiler on ${msg}"
            VERBATIM
            )
endforeach()

# 设置文件属性为 GENERATED
set_source_files_properties(${PROTO_SRC} ${PROTO_HEADER} PROPERTIES GENERATED TRUE)
# 添加自定义target
add_custom_target(
    generate_message ALL
    DEPENDS ${PROTO_SRC} ${PROTO_HEADER}
    COMMENT "generate message target"
    VERBATIM
)


add_subdirectory(kv-system)
add_subdirectory(raft-app)
# add_subdirectory(demo)
pkg_check_modules(GoogleTest gtest_main)
if (GoogleTest_FOUND)
    include_directories(${GoogleTest_INCLUDE_DIRS})
    set(LIBS
        ${LIBS}
        ${GoogleTest_LIBRARIES})
    # add_subdirectory(tests)
    enable_testing()
endif (GoogleTest_FOUND)

configure_file(Procfile Procfile)
configure_file(kv-system-config.json kv-system-config.json)