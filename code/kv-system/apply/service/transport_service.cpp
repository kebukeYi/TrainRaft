#include <service/transport_service.h>

namespace kv {

// brpc 收到其他节点发来的 raft 消息：
// 把请求里的 Message 取出来（move 转移所有权，避免拷贝），
// 交给 RaftNode::ProcessMessage 处理（它会投递到 raft 线程）。
// 处理完立刻回包（done->Run()），发送方是同步等回包的
void TransportServiceImpl::MessageChannel(
    ::google::protobuf::RpcController* controller,
    const ::proto::TransportRequest* request,
    ::proto::TransportResponse* response, ::google::protobuf::Closure* done) {

    // 交给 RaftNode 处理 (可能投递到 raft 线程)
    raft_->ProcessMessage(std::make_shared<proto::Message>(std::move(request->msg())));
    if (done) {
        done->Run();
    }
}
}  // namespace kv
