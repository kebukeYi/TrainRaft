#pragma once
#include <common/status.h>
#include <raft/node.h>
#include <stdint.h>

#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "resource/raft.pb.h"
using namespace proto;

namespace kv {
class RaftNode;

// ============================================================================
// Transport：Raft 节点间通信模块, 屏蔽底层实际网络框架;
//
// 大白话：raft 状态机算出一堆要发给别的节点的消息（投票、追加日志、心跳…），
// 自己不会发网络包，全部交给 Transport 传输器;
// 它内部：
//   - 自己是一个 brpc server（监听节点间通信端口），接收其他节点发来的消息
//     （通过 TransportServiceImpl::MessageChannel）
//   - 维护一张"节点 id -> brpc channel"的表，把消息发到对应节点的 server 上
// 具体实现见 transport.cpp 的 TransportImpl
// ============================================================================
class Transport {
       public:
        virtual ~Transport() = default;

        // 启动本机 raft_server(监听自己的节点间通信端口)
        virtual void Start(const std::string& host) = 0;

        virtual void Stop() = 0;

        // 将给定的消息发送到 remote peer 节点。
        // 每条消息都有一个 "To" 字段（目的地），映射到传输中已有的对等节点。
        // 如果找不到该 ID，消息被忽略
        virtual void Send(std::vector<std::shared_ptr<proto::Message>> msgs) = 0;

        // 添加一个对等节点（建立到它的 brpc channel）
        virtual void AddPeer(uint64_t id, const std::string& peer) = 0;

        // 工厂：创建 Transport 实例
        static std::shared_ptr<Transport> Create(RaftNode* raft, uint64_t id);
};
using TransporterPtr = std::shared_ptr<Transport>;

}  // namespace kv
