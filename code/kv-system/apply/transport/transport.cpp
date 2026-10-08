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

// ----------------------------------------------------------------------------
// SendDone：异步发送完成后的回调（brpc 保证每条 RPC 的回调恰好被调用一次）
//
// 为什么改成异步：Send() 是在 raft 线程（ioService_ 事件循环）上被调用的。
// 若同步发送，每条消息都要阻塞等对端回包，raft 线程就被网络 IO 卡住，
// 心跳 / 选举 / 日志应用全被拖延。改成异步后 Send() 立即返回。
//
// 代价：controller / request / response 不能在栈上（函数返回即析构），
// 必须堆分配、在回调里释放；channel 也一并持有，保证调用期间连接不被销毁。
// ----------------------------------------------------------------------------
namespace {
struct SendDone : public google::protobuf::Closure {
    SendDone(brpc::Controller* c, TransportRequest* req,
             TransportResponse* resp, std::shared_ptr<brpc::Channel> ch,
             proto::MessageType t, uint64_t to_)
        : cntl(c), request(req), response(resp), channel(std::move(ch)),
          type(t), to(to_) {}

    void Run() override {
        if (cntl->Failed()) {
            LOG_WARN("send message(type=%d) to peer %lu failed: %s", type, to,
                     cntl->ErrorText().c_str());
        }
        delete cntl;
        delete request;
        delete response;
        delete this;  // 回调只执行一次，安全自毁
    }

    brpc::Controller* cntl;
    TransportRequest* request;
    TransportResponse* response;
    std::shared_ptr<brpc::Channel> channel;  // 让 channel 活到本次调用结束
    proto::MessageType type;
    uint64_t to;
};
}  // namespace

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

    // 发送一批消息: 按每条消息的 to 字段找对应的 channel 发出去。
    // 异步发送: done 非空 → brpc 立即返回，不阻塞 raft 线程；
    // 回包/失败时在 brpc 线程执行 SendDone::Run() 释放资源。
    void Send(std::vector<std::shared_ptr<proto::Message>> msgs) final {
        for (auto& msg : msgs) {
            if (msg->to() == 0) {
                // to 为 0 表示"故意丢弃的消息"，跳过
                continue;
            }
            // 只在查表时加锁，拿到 channel 的 shared_ptr 后立刻放锁
            std::shared_ptr<brpc::Channel> channel;
            {
                std::lock_guard<std::mutex> guard(mutex_);
                auto it = rpcClients_.find(msg->to());
                if (it == rpcClients_.end()) {
                    // 目标不在连接表里（可能被移除了），忽略
                    LOG_DEBUG("ignored message %d (sent to unknown peer %lu)",
                              msg->type(), msg->to());
                    continue;
                }
                channel = it->second;
            }

            // 异步调用: 参数必须堆分配（要活到回调执行），由 SendDone 负责释放
            TransportService_Stub stub(channel.get());
            auto* cntl = new brpc::Controller();
            auto* request = new TransportRequest();
            auto* response = new TransportResponse();
            request->mutable_msg()->CopyFrom(*msg);
            auto* done = new SendDone(cntl, request, response, channel, msg->type(), msg->to());
            stub.MessageChannel(cntl, request, response, done);
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
