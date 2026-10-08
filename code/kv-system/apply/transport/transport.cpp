#include <apply/raft-node/raft_node.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <common/log.h>
#include <resource/raft.pb.h>
#include <service/transport_service.h>
#include <transport/transport.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>
using namespace proto;

namespace kv {
// ============================================================================
// TransportImpl：Transport 接口的具体实现（基于 brpc）
//
// 大白话：每个节点启动时，Start() 会在自己"节点间通信端口"上起一个 brpc
// server，接收别的节点的 raft 消息（MessageChannel 回调 → 交给 raft 状态机）。
// 同时 AddPeer() 会为每个别的节点建一个 brpc channel（客户端连接），
// Send() 就是把 raft 消息通过 channel 发到对方的 server。
// 于是任意两个节点之间都能互相传 raft 消息。
// ============================================================================
class TransportImpl : public Transport {

   private:
    std::mutex mutex_;              // 保护 rpcClients_（可能有多个线程发消息）
    RaftNode* raft_;                // 传给 TransportServiceImpl (收到消息后交给 RaftNode 处理)
    uint64_t id_;                   // 本节点 id

    // 本节点自己的 brpc server 监听端口, (接收 其他 raft 节点发来的消息)
    brpc::Server rpcServer_;
    TransportServiceImpl service_;  // brpc 服务端 handler 实现(处理消息)
    // 其他节点的客户端连接表: <节点id, brpc channel>
    std::unordered_map<uint64_t, std::shared_ptr<brpc::Channel>> rpcClients_;


   public:
    // 构造
    explicit TransportImpl(RaftNode* raft, uint64_t id)
        : raft_(raft), id_(id), service_(raft_) {}

    ~TransportImpl() final {
        rpcServer_.Stop(10);  // 停止 server（参数是宽限毫秒）
        rpcServer_.Join();
    }

    // 启动本机 server，监听自己的节点间通信端口;
    // host 格式 "ip:传输端口:客户端端口"(取前两段做监听地址)
    void Start(const std::string& host) final {
        std::vector<std::string> strs;
        boost::split(strs, host, boost::is_any_of(":"));
        if (strs.size() != 3) {
            LOG_DEBUG("invalid host %s", host.c_str());
            exit(0);
        }
        std::string address(strs[0] + ":" + strs[1]);
        
        // 注册 brpc 服务端 handler(TransportServiceImpl::MessageChannel)
        rpcServer_.AddService(&service_, brpc::SERVER_DOESNT_OWN_SERVICE);
        rpcServer_.Start(address.c_str(), nullptr);
    }

    // 添加一个对等节点: 为它建一个 brpc channel（客户端连接）
    // peer 格式 "ip:传输端口:客户端端口"
    void AddPeer(uint64_t id, const std::string& peer) final {
        LOG_DEBUG("node:%lu, peer:%lu, addr:%s", id_, id, peer.c_str());
        std::lock_guard<std::mutex> guard(mutex_);
        std::vector<std::string> strs;
        boost::split(strs, peer, boost::is_any_of(":"));
        std::string address(strs[0] + ":" + strs[1]);  // ip + 传输端口

        // 初始化 channel，连接到对端服务器
        std::shared_ptr<brpc::Channel> rpcClient = std::make_shared<brpc::Channel>();
        brpc::ChannelOptions options;

        rpcClient->Init(address.c_str(), &options);
        auto it = rpcClients_.find(id);
        if (it != rpcClients_.end()) {
            LOG_DEBUG("peer already exists %lu", id);
            return;  // 已存在则跳过（不重复建连接）
        }
        rpcClients_[id] = rpcClient;
    }

    // 发送一批消息: 按每条消息的 to 字段找对应的 channel 发出去
    void Send(std::vector<std::shared_ptr<proto::Message>> msgs) final {
        for (auto& msg : msgs) {
            if (msg->to() == 0) {
                // to 为 0 表示"故意丢弃的消息"，跳过
                continue;
            }
            auto it = rpcClients_.find(msg->to());
            if (it == rpcClients_.end()) {
                // 目标不在连接表里（可能被移除了），忽略
                LOG_DEBUG(
                    "ignored message %d (sent to unknown peer "
                    "%lu)",
                    msg->type(), msg->to());
                continue;
            }
            // 通过 brpc 调对端节点的 MessageChannel 接口。
            // done 传 nullptr = 同步调用（阻塞到对端返回），所以
            // request/response/controller 都可以放在栈上，函数返回时自动析构。
            // （原来的写法是 new Controller/new Response 且从不 delete, 每发一条消息就泄漏一块内存）
            TransportService_Stub stub(it->second.get());
            TransportRequest request; // 原来: new TransportRequest()
            TransportResponse response; // 原来: new TransportResponse()
            brpc::Controller cntl; // 原来: new brpc::Controller()
            request.mutable_msg()->CopyFrom(*msg);

            stub.MessageChannel(&cntl, &request, &response, nullptr);

            // 发送失败（对端挂了/网络断）时打条日志，方便排查问题
            if (cntl.Failed()) {
                LOG_WARN("send message(type=%d) to peer %lu failed: %s",
                         msg->type(), msg->to(), cntl.ErrorText().c_str());
            }
        }
    }

    void Stop() final {}
};

// 工厂：创建 Transport 实例
std::shared_ptr<Transport> Transport::Create(RaftNode* raft, uint64_t id) {
    std::shared_ptr<TransportImpl> impl(new TransportImpl(raft, id));
    return impl;
}

}  // namespace kv
