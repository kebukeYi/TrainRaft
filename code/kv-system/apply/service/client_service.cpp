#include "service/client_service.h"

namespace kv {
// ============================================================================
// 客户端读接口 Get
//
// 大白话：这里演示了三种读一致性的实现方式（通过切换 switch 的 case）：
//   StrongConsistency：把读请求也当成"写"走一遍 Raft 共识（最原始最安全，
//                       但读也要复制日志，性能差）
//   WeakConsistency：FollowerRead，直接读本地 DB（快，但可能读到旧数据）
//   ReadIndex：先问 leader 要一个"安全水位"（commit 索引），
//               等本地 appliedIndex 超过它再读（强一致且不用复制日志，
//               本项目的推荐做法）
// 本项目编译时固定走 WeakConsistency（case 直接写死），
// 所以客户端读的是"弱一致"数据；想看其他模式改这里即可。
// ============================================================================
void ClientServiceImpl::Get(::google::protobuf::RpcController* controller,
                            const ::proto::GetRequest* request,
                            ::proto::GetResponse* response,
                            ::google::protobuf::Closure* done) {
    
    enum LinearConsistency {
        StrongConsistency,  // 最原始的强一致性
        WeakConsistency,    // 弱一致性的 FollowerRead
        ReadIndex           // 强一致性的 FollowerRead
    };

    // 生成请求号（回包时靠它找到这份登记）
    uint32_t requestId = NextRequestId();
    // 封装请求数据
    proto::RaftEntryData entryData;
    entryData.set_nodeid(raft_->NodeId());  // 记录是哪个节点接的请求
    entryData.set_requestid(requestId);
    entryData.set_type(proto::methodType::GetOperation);
    entryData.set_key(request->key());

    // 序列化成字节（将成为一条 Raft 日志的内容）
    std::string sbuf;
    entryData.SerializeToString(&sbuf);
    std::shared_ptr<std::vector<uint8_t>> data =
        std::make_shared<std::vector<uint8_t>>(sbuf.begin(), sbuf.end());

    // 登记"待回复请求"（等共识完成后来回复）
    RaftReply replyMeta;
    replyMeta.done = done;
    replyMeta.response = static_cast<void*>(response);

    // 记录"待回复请求"（等共识完成后来回复）
    AddPendingRequest(requestId, replyMeta);

    // 注意：本项目当前固定走 WeakConsistency（弱读）
    switch (LinearConsistency::WeakConsistency) {
        case StrongConsistency: {
            // 走 Raft 共识（像写操作一样复制日志）
            raft_->ProcessProposal(data);
        } break;
        case WeakConsistency: {
            // 弱读：直接在本地 DB 上读，不经过 Raft
            // （ReadIndex 里用 AppliedStateMachine 直接执行读，见 raft_node.cpp）
            raft_->ProcessWeakRead(data);
        } break;
        case ReadIndex: {
            // 线性一致读：先取安全水位再读
            raft_->ProcessReadIndex(data);
        } break;
        default:
            LOG_INFO("unkown type");
            break;
    }
}

// 写接口 Set：把写请求送进 Raft 共识
void ClientServiceImpl::Set(::google::protobuf::RpcController* controller,
                            const ::proto::SetRequest* request,
                            ::proto::SetResponse* response,
                            ::google::protobuf::Closure* done) {
    uint32_t requestId = NextRequestId();
    /*
    message RaftEntryData {
    uint64 nodeid = 1;    // 节点ID：哪个节点收到的客户端请求(用来过滤，只让该节点回复客户端)
    uint64 requestid = 2; // 请求ID：全局唯一，用于把"回复"和"当初的 RPC 请求"对上号
    methodType type = 3;  // 请求类型：Set/Get/Del
    string key = 4;       // 键
    string value = 5;     // 值
    string message = 6;   // 附带消息（如错误信息）
    }
    */
    proto::RaftEntryData entryData;
    // 需要附带其他消息,因此在这里把请求号、节点号、请求类型、key、value 都封装到 RaftEntryData 中;
    entryData.set_nodeid(raft_->NodeId());
    entryData.set_requestid(requestId);
    entryData.set_type(proto::methodType::SetOperation);
    entryData.set_key(request->key());
    entryData.set_value(request->value());

    // 序列化 + 登记待回复
    std::string sbuf;
    entryData.SerializeToString(&sbuf);

    // 把string改为 vector<uint8_t>, 因为 raft_->ProcessProposal() 需要 vector<uint8_t> 类型;
    std::shared_ptr<std::vector<uint8_t>> data =
        std::make_shared<std::vector<uint8_t>>(sbuf.begin(), sbuf.end());
    
    RaftReply replyMeta;
    replyMeta.done = done;
    replyMeta.response = static_cast<void*>(response);

    // ⭐ 必须先登记"待回复请求"，再把提案发出去！
    // 否则日志可能抢先提交并应用，ApplyStateMachine 找不到登记 → 客户端永远收不到回复
    AddPendingRequest(requestId, replyMeta);

    // controller 中含有raft请求器;
    // 走 Raft 共识（写操作必须复制到多数节点才算成功）
    raft_->ProcessProposal(std::move(data));
}

// 删接口 Del：同 Set，走 Raft 共识
void ClientServiceImpl::Del(::google::protobuf::RpcController* controller,
                            const ::proto::DelRequest* request,
                            ::proto::DelResponse* response,
                            ::google::protobuf::Closure* done) {
    uint32_t requestId = NextRequestId();
    proto::RaftEntryData entryData;
    entryData.set_nodeid(raft_->NodeId());
    entryData.set_requestid(requestId);
    entryData.set_type(proto::methodType::DelOperation);
    entryData.set_key(request->key());

    std::string sbuf;
    entryData.SerializeToString(&sbuf);
    std::shared_ptr<std::vector<uint8_t>> data =
        std::make_shared<std::vector<uint8_t>>(sbuf.begin(), sbuf.end());
    
    RaftReply replyMeta;
    replyMeta.done = done;
    replyMeta.response = static_cast<void*>(response);

    // ⭐ 必须先登记"待回复请求"，再把提案发出去（同 Set）
    AddPendingRequest(requestId, replyMeta);

    // 异步处理;
    raft_->ProcessProposal(std::move(data));
}

// 列出所有 key: 直接读本地 DB（弱一致），不需要走 Raft
void ClientServiceImpl::Keys(::google::protobuf::RpcController* controller,
                             const ::proto::KeysRequest* request,
                             ::proto::KeysResponse* response,
                             ::google::protobuf::Closure* done) {

    std::vector<std::string> keys;
    std::unordered_map<std::string, std::string> kvData;

    db_->Get(kvData);  // 遍历本地 KV 引擎
    for (auto it = kvData.begin(); it != kvData.end(); ++it) {
        keys.push_back(it->first);
    }
    response->set_message("ok");
    // 
    done->Run();  // 立即回复（不需要等共识）
}

// Ping：连通性测试，直接回 "pong"
void ClientServiceImpl::Ping(::google::protobuf::RpcController* controller,
                             const ::proto::PingRequest* request,
                             ::proto::PingResponse* response,
                             ::google::protobuf::Closure* done) {
    response->set_message("pong");
    done->Run();
}

}  // namespace kv
