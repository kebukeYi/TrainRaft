#pragma once
#include <stdint.h>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <apply/raft-state-machine/state_machine.h>
#include <apply/transport/transport.h>
#include <raft/node.h>
#include <raft/util.h>
#include <store/snap/snapshot_feature.h>
#include <store/wal/wal_feature.h>

#include "resource/cli.pb.h"
#include "resource/raft.pb.h"

namespace kv {
    // ============================================================================
    // RaftNode： 应用层的"门面"（Facade）——把整个 Raft+KV 系统串起来
    //
    // 大白话： 如果说 raft/ 目录是"发动机"，那 RaftNode 就是"整车"。
    // 它把所有零件组装到一起：
    //   - Node（raft 状态机）
    //   - Transport（节点间通信）
    //   - StateMachine（状态机/客户端 RPC/RocksDB）
    //   - WAL（预写日志）、SnapshotFeature（快照）、MemoryStorage（内存日志）
    // 并且开一个独立线程（ioService_）跑整个系统的"事件循环":
    //   定时器 → node_->Tick() → PullReadyEvents（写 WAL/发消息/应用日志）
    // 客户端请求、网络消息都会投递到这个线程上串行执行（保证单线程安全）。
    // 具体实现在 raft_node.cpp 的 RaftNodeImpl。
    // ============================================================================
class RaftNode {
       public:
        RaftNode() = default;

        virtual ~RaftNode() = default;

        // 返回本节点 id
        virtual uint64_t NodeId() = 0;

        // 启动节点（启动通信、状态机、定时器，进入事件循环）
        virtual void Start() = 0;

        // 停止节点
        virtual void Stop() = 0;

        // 处理节点间通信 Message (收到其他节点发来的 raft 消息)
        virtual void ProcessMessage(std::shared_ptr<proto::Message> msg) = 0;

        // 处理写操作（客户端 Set/Del 请求 → Raft 提案）
        virtual void ProcessProposal(
            std::shared_ptr<std::vector<uint8_t>> data) = 0;

        // 处理线性一致读（ReadIndex 模式）
        virtual void ProcessReadIndex(
            std::shared_ptr<std::vector<uint8_t>> data) = 0;

        // 处理弱读（直接本地读，不经过 Raft）
        virtual void ProcessWeakRead(
            std::shared_ptr<std::vector<uint8_t>> data) = 0;

        // 应用一批已提交的日志条目（把请求真正执行到状态机）
        virtual bool PublishEntries(
            const std::vector<std::shared_ptr<proto::Entry>>& entries) = 0;

        // 过滤出"真正需要应用"的条目（跳过已经应用过的）
        virtual void EntriesToApply(
            const std::vector<std::shared_ptr<proto::Entry>>& entries,
            std::vector<std::shared_ptr<proto::Entry>>& ents) = 0;

        // 检查并触发快照（应用日志数达到阈值时做快照+压缩日志）
        virtual void MaybeTriggerSnapshot() = 0;

        // 不是实例方法: 节点进程的入口（被 raft-kv.cpp main 调用）
        static void Main(uint64_t id, const std::string originDataDir,
                         const std::vector<std::string>& cluster);

        // 信号处理（Ctrl+C 优雅退出）
        static void SignalHandler(int);
};

using RaftNodePtr = std::shared_ptr<RaftNode>;
}  // namespace kv
