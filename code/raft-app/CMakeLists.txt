set(LIBS
    ${LIBS}
    raft-kv++)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

enable_testing()
find_package(GTest REQUIRED)

# 运行集群的可执行文件
add_executable(raft-kv raft-kv.cpp)
target_link_libraries(raft-kv ${LIBS} raft-kv++)

# 客户端
add_executable(brpc_client brpc_client.cpp)
target_link_libraries(brpc_client ${LIBS} raft-kv++)
