#include <apply/raft-node/raft_node.h>
#include <common/log.h>
#include <resource/raft.pb.h>
#include <unistd.h>

using namespace proto;

namespace kv {
// ============================================================================
// TransportServiceImpl：raft节点间通信的 brpc 服务端
//
// 大白话：每个节点的 Transport 会把自己的服务注册成
// "TransportService.MessageChannel" 接口。别的节点发 raft 消息时，
// 就是调用这个接口。本节点收到后，把消息直接交给 RaftNode（门面），
// 由它喂给 raft 状态机处理。
// ============================================================================
class TransportServiceImpl : public TransportService {
   
   private:
    RaftNode* raft_;  // 收到消息后交给的门面

   public:
    TransportServiceImpl(RaftNode* raft) : raft_(raft) {}

    // 收到一个 raft 消息：包成 shared_ptr 交给
    // raft_（RaftNode::ProcessMessage）
    void MessageChannel(::google::protobuf::RpcController* controller,
                        const ::proto::TransportRequest* request,
                        ::proto::TransportResponse* response,
                        ::google::protobuf::Closure* done) override;
};
}  // namespace kv
