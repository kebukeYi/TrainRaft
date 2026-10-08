
# ============================================================================
# kv-system 的 CMake 构建文件
#
# 大白话：这个文件告诉 cmake"这个库由哪些 .cpp 组成、依赖哪些库"。
# 它把整个 kv-system 编译成一个静态库 raft-kv++，
# 然后 raft-app/raft-kv.cpp（主程序）和 tests/（单元测试）链接它。
#
# 源码按功能分四组，从上到下正好是"系统分层"：
#   1. proto接口   —— 由外层 CMakeLists 生成的 protobuf 代码（resource/）
#   2. 公共接口    —— common/ 工具层（Status/Slice/ByteBuffer/日志/CRC32）
#   3. raft算法    —— raft/ 纯 Raft 算法层（选举/日志复制/提交/快照）
#   4. 应用层      —— apply/ 存储、网络、RPC、状态机、门面 RaftNode
# ============================================================================
set(SRC
    # proto接口
    ${PROTO_SRC}
    # 公共接口
    common/log.h
    common/slice.h
    common/status.cpp
    common/bytebuffer.cpp
    common/random_device.cpp
    common/utils.cpp
    # raft算法
    raft/config.cpp
    raft/raft.cpp
    raft/storage.cpp
    raft/node.cpp
    raft/raft_log.cpp
    raft/unstable.cpp
    raft/progress.cpp
    raft/readonly.cpp
    raft/ready.cpp
    raft/util.cpp
    raft/zk_client.cpp
    # 存储相关
    apply/store/engine/kv_engine.cpp
    apply/store/snap/snapshot_feature.cpp
    apply/store/wal/wal_feature.cpp
    apply/store/wal/wal_file.cpp
    # 网络相关
    apply/transport/transport.cpp
    # RPC服务
    apply/service/transport_service.cpp
    apply/service/client_service.cpp
    # 上层接口
    apply/raft-state-machine/state_machine.cpp
    apply/raft-node/raft_node.cpp
)

# 把上面所有源文件打成一个库：raft-kv++
# （头文件不用编译，但列出来方便 IDE 索引；实际参与编译的只有 .cpp）
add_library(raft-kv++ ${SRC})

# 链接外部依赖库：brpc、rocksdb、protobuf、glog、boost 等（由外层传入 ${LIBS}）
target_link_libraries(raft-kv++ ${LIBS})

