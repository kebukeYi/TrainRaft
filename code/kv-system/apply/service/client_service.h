#ifndef CLIENT_SERVICE_H
#define CLIENT_SERVICE_H

#include <common/log.h>
#include <common/status.h>
#include <store/engine/kv_engine.h>
#include <unistd.h>

#include <atomic>
#include <boost/asio.hpp>
#include <chrono>
#include <cstdint>
#include <future>
#include <msgpack.hpp>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "raft-node/raft_node.h"
#include "resource/cli.pb.h"

using namespace proto;
namespace kv {

class RaftNode;  // 前置声明

// RaftReply：一条"待回复的客户端请求"的登记信息。
// 大白话：客户端调 Set/Get 后，RPC 不能立刻回复（要等 Raft 达成共识后
// 才能真正执行）。所以先把"怎么回复"（done 回调 + response 指针）存起来，
// 等这条日志被提交应用后，再拿着 requestid 找到它，执行 done->Run() 回复
struct RaftReply {
    google::protobuf::Closure* done = nullptr;  // brpc 的完成回调（调用即回复）
    void* response = nullptr;  // 指向 RPC 响应对象（SetResponse 等）
    // 请求类型: 超时清理时要按类型把错误写进对应的 Response（Set/Del/Get 都有
    // message 字段）
    proto::methodType type = proto::methodType::PingOperation;
    // 超时时刻 = 登记时间 + pendingTimeoutMs_；超过它还没被应用就由 sweeper
    // 清理
    std::chrono::steady_clock::time_point deadline;
};

// 三种读一致性模式（用环境变量 KV_READ_MODE 选择：weak / readindex / strong）
enum ReadConsistency {
    kStrongConsistency = 0,  // 读也当成"写"走一遍 Raft 共识：最强、最慢;
    kWeakConsistency = 1,    // 弱读：直接读本地 DB，最快但可能读到旧数据;
    kReadIndex = 2,  // ReadIndex：先取"安全水位"再读，强一致且不复制日志;
};

// ============================================================================
// ClientServiceImpl：面向客户端的 brpc 服务端（Set/Get/Del/Keys/Ping）
//
// 大白话：客户端连接节点调这些接口。每个接口的套路都差不多：
//   1. 把请求封装成 RaftEntryData（记上 requestid）
//   2. 序列化成字节
//   3. 把 {done, response} 登记到 pendingRequests_（等提交后回来回复）
//   4. 调 raft_->ProcessProposal(data) 让数据进入 Raft 共识
// 当这条日志在多数节点上提交并被状态机应用后（state_machine.cpp 里），
// 会拿着 requestid 找到这份登记，真正执行操作并 done->Run() 回复客户端。
//
// 注意（重要）：如果请求迟迟没被应用（服务端过载 / 客户端已断开），
// 这份登记就会一直挂着 —— brpc 会因为"RPC 没结束"而占着这条连接不放，
// 连接堆积成 CLOSE_WAIT，最终把文件描述符(FD)耗尽。
// 所以这里起了一个后台线程 sweeper_，定期把超时的登记取出来、
// 主动回一个错误并 done->Run()，让 brpc 能结束 RPC、释放连接。
// ============================================================================
class ClientServiceImpl : public ClientService {
   private:
    RaftNode* raft_;                // 门面（提交请求/读请求）
    std::shared_ptr<KVEngine> db_;  // 本地 KV 引擎（弱读/Keys 直接查）
    // 读一致性模式（见上面的 ReadConsistency），由环境变量 KV_READ_MODE 决定
    int readMode_;

    // 请求号发生器：会被多个 brpc 工作线程并发调用，必须用原子类型，
    // 否则并发自增会产生重复的 requestid，导致 pendingRequests_ 互相覆盖
    std::atomic<uint64_t> nextRequestId_;

    // 保护 pendingRequests_ 的互斥锁：
    // 这张表会被 brpc 工作线程（Set/Get/Del 里登记）、
    // raft 线程（ApplyStateMachine 里取出）和 sweeper 线程并发访问
    std::mutex pendingMutex_;

    // 待回复请求的超时时间（毫秒）。可用环境变量 KV_PENDING_TIMEOUT_MS 覆盖。
    uint64_t pendingTimeoutMs_;
    std::thread sweeper_;            // 超时清理线程
    std::atomic<bool> stopSweeper_;  // 通知 sweeper 退出

   public:
    // requestid -> 待回复的请求登记（上面 RaftReply 注释）。
    // 注意：不要直接操作它，统一走下面的 AddPendingRequest / TakePendingRequest
    std::unordered_map<uint64_t, RaftReply> pendingRequests_;

   public:
    ClientServiceImpl(RaftNode* raft, std::shared_ptr<KVEngine> db);
    ~ClientServiceImpl();

    // 停止超时清理线程（在 StateMachine::Stop 里调用）
    void Stop();

    // 原子地生成一个请求号
    uint64_t NextRequestId() { 
        return nextRequestId_.fetch_add(1); 
    }

    // 登记一条"待回复的客户端请求"（自带加锁，并打上超时时刻）
    void AddPendingRequest(uint64_t id, RaftReply reply) {
        reply.deadline = std::chrono::steady_clock::now() +
                         std::chrono::milliseconds(pendingTimeoutMs_);
        std::lock_guard<std::mutex> lock(pendingMutex_);
        pendingRequests_[id] = std::move(reply);
    }

    // 取出并删除一条待回复请求（加锁，find + erase 一次完成，保证原子性）。
    // 返回 false 表示没有对应登记（如已被处理过 / 已超时清理）
    bool TakePendingRequest(uint64_t id, RaftReply& out) {
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

   private:
    // 后台清理循环：定期检查并回收超时的待回复请求
    void SweepLoop();

    // 取出所有已超时的登记（加锁，取完即删）
    std::vector<std::pair<uint64_t, RaftReply>> TakeExpiredRequests();

    // 按请求类型，把错误信息写进对应的 Response，然后回复客户端
    static void ReplyError(RaftReply& reply, const std::string& msg);
};
}  // namespace kv

#endif  // CLIENT_SERVICE_H
