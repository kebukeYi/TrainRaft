#ifndef CLIENT_SERVICE_H
#define CLIENT_SERVICE_H

#include <common/log.h>
#include <common/status.h>
#include <store/engine/kv_engine.h>
#include <unistd.h>

#include <atomic>
#include <boost/asio.hpp>
#include <future>
#include <msgpack.hpp>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "raft-node/raft_node.h"
#include "resource/cli.pb.h"

using namespace proto;
namespace kv {

class RaftNode;  // ⭐️ 加上前置声明！

// RaftReply：一条"待回复的客户端请求"的登记信息。
// 大白话：客户端调 Set/Get 后，RPC 不能立刻回复（要等 Raft 达成共识后
// 才能真正执行）。所以先把"怎么回复"（done 回调 + response 指针）存起来，
// 等这条日志被提交应用后，再拿着 requestid 找到它，执行 done->Run() 回复
struct RaftReply {
    google::protobuf::Closure* done;  // brpc 的完成回调（调用即回复）
    void* response;                   // 指向 RPC 响应对象（SetResponse 等）
};

// ============================================================================
// ClientServiceImpl：面向客户端的 brpc 服务器端（Set/Get/Del/Keys/Ping）
//
// 大白话：客户端连接节点调这些接口。每个接口的套路都差不多：
//   1. 把请求封装成 RaftEntryData（记上 requestid）
//   2. 序列化成字节
//   3. 把 {done, response} 登记到 pendingRequests_（等提交后回来回复）
//   4. 调 raft_->ProcessProposal(data) 让数据进入 Raft 共识
// 当这条日志在多数节点上提交并被状态机应用后（state_machine.cpp 里），
// 会拿着 requestid 找到这份登记，真正执行操作并 done->Run() 回复客户端。
// ============================================================================
class ClientServiceImpl : public ClientService {
   private:
    RaftNode* raft_;                // 门面（提交请求/读请求）
    std::shared_ptr<KVEngine> db_;  // 本地 KV 引擎（弱读/Keys 直接查）
    // 请求号发生器：会被多个 brpc 工作线程并发调用，必须用原子类型，
    // 否则并发自增会产生重复的 requestid，导致 pendingRequests_ 互相覆盖
    std::atomic<uint32_t> nextRequestId_;
    // 保护 pendingRequests_ 的互斥锁：
    // 这张表会被 brpc 工作线程（Set/Get/Del 里登记）和
    // raft 线程（ApplyStateMachine 里取出）并发访问，不加锁会破坏堆内存
    std::mutex pendingMutex_;

   public:
    // requestid -> 待回复的请求登记（上面 RaftReply 注释）。
    // 注意：不要直接操作它，统一走下面的 AddPendingRequest / TakePendingRequest
    std::unordered_map<uint32_t, RaftReply> pendingRequests_;

   public:
    ClientServiceImpl(RaftNode* raft, std::shared_ptr<KVEngine> db)
        : raft_(raft), db_(db), nextRequestId_(0) {}

    // 原子地生成一个请求号
    uint32_t NextRequestId() { return nextRequestId_++; }

    // 登记一条"待回复的客户端请求"（自带加锁）
    void AddPendingRequest(uint32_t id, const RaftReply& reply) {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        pendingRequests_[id] = reply;
    }

    // 取出并删除一条待回复请求（加锁，find + erase 一次完成，保证原子性）。
    // 返回 false 表示没有对应登记（如已被处理过 / 节点重启后丢失）
    bool TakePendingRequest(uint32_t id, RaftReply& out) {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        auto it = pendingRequests_.find(id);
        if (it == pendingRequests_.end()) {
            return false;
        }
        out = it->second;
        pendingRequests_.erase(it);
        return true;
    }

    void Get(::google::protobuf::RpcController* controller,
             const ::proto::GetRequest* request, ::proto::GetResponse* response,
             ::google::protobuf::Closure* done) override;

    void Set(::google::protobuf::RpcController* controller,
             const ::proto::SetRequest* request, ::proto::SetResponse* response,
             ::google::protobuf::Closure* done) override;

    void Del(::google::protobuf::RpcController* controller,
             const ::proto::DelRequest* request, ::proto::DelResponse* response,
             ::google::protobuf::Closure* done) override;

    void Keys(::google::protobuf::RpcController* controller,
              const ::proto::KeysRequest* request,
              ::proto::KeysResponse* response,
              ::google::protobuf::Closure* done) override;

    void Ping(::google::protobuf::RpcController* controller,
              const ::proto::PingRequest* request,
              ::proto::PingResponse* response,
              ::google::protobuf::Closure* done) override;
};
}  // namespace kv

#endif  // CLIENT_SERVICE_H
