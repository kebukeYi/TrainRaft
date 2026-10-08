#include "service/client_service.h"

#include <chrono>
#include <cstdlib>

namespace kv {

// ============================================================================
// 构造 / 析构 / 后台超时清理线程
//
// 为什么要它：客户端请求登记进 pendingRequests_ 后，本应由"日志被应用"时回复；
// 但如果服务端过载（请求一直排不上号）或客户端已经断开，这份登记就会永远挂着，
// brpc 会认为这条 RPC 还没结束、一直占着连接不放（CLOSE_WAIT），
// 连接越堆越多，最后把文件描述符(FD)耗尽。
// sweeper_ 负责定期回收这些"僵尸登记"：回一个错误、让 brpc 结束 RPC。
// ============================================================================
ClientServiceImpl::ClientServiceImpl(RaftNode* raft, std::shared_ptr<KVEngine> db)
    : raft_(raft), db_(db), nextRequestId_(0), stopSweeper_(false) {
    // 默认 10 秒；测试时可用环境变量覆盖，例如 KV_PENDING_TIMEOUT_MS=3000
    pendingTimeoutMs_ = 10000;
    if (const char* env = std::getenv("KV_PENDING_TIMEOUT_MS")) {
        unsigned long long v = std::strtoull(env, nullptr, 10);
        if (v > 0) {
            pendingTimeoutMs_ = v;
        }
    }
    sweeper_ = std::thread([this] { SweepLoop(); });
}

ClientServiceImpl::~ClientServiceImpl() { Stop(); }

void ClientServiceImpl::Stop() {
    stopSweeper_.store(true);
    if (sweeper_.joinable()) {
        sweeper_.join();
    }
}

// 后台循环：每 500ms 检查一次有没有"挂了太久"的待回复请求
void ClientServiceImpl::SweepLoop() {
    while (!stopSweeper_.load()) {
        // 分 5 小段睡，便于收到 Stop 后能很快退出
        for (int i = 0; i < 5 && !stopSweeper_.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (stopSweeper_.load()) {
            break;
        }

        std::vector<std::pair<uint32_t, RaftReply>> expired = TakeExpiredRequests();
        if (expired.empty()) {
            continue;
        }
        // 在锁外回复（避免持锁做网络 IO）
        for (auto& kv : expired) {
            ReplyError(kv.second, "request timeout (server overloaded)");
        }
        LOG_WARN("expired %zu pending client requests (server busy or client gone)",
                 expired.size());
    }
}

// 取出所有已超时的登记（加锁；取完即从表中删除，保证只有一个人能拿到）
std::vector<std::pair<uint32_t, RaftReply>> ClientServiceImpl::TakeExpiredRequests() {
    std::vector<std::pair<uint32_t, RaftReply>> out;
    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(pendingMutex_);
    for (auto it = pendingRequests_.begin(); it != pendingRequests_.end();) {
        if (now >= it->second.deadline) {
            out.emplace_back(it->first, it->second);
            it = pendingRequests_.erase(it);
        } else {
            ++it;
        }
    }
    return out;
}

// 按请求类型把错误写进对应的 Response，然后回复客户端
void ClientServiceImpl::ReplyError(RaftReply& reply, const std::string& msg) {
    if (reply.response != nullptr) {
        switch (reply.type) {
            case proto::methodType::SetOperation:
                static_cast<proto::SetResponse*>(reply.response)->set_message(msg);
                break;
            case proto::methodType::DelOperation:
                static_cast<proto::DelResponse*>(reply.response)->set_message(msg);
                break;
            case proto::methodType::GetOperation:
                static_cast<proto::GetResponse*>(reply.response)->set_message(msg);
                break;
            default:
                break;
        }
    }
    if (reply.done != nullptr) {
        // 线程安全：这条登记已被 TakeExpiredRequests 从表中摘除，
        // 不会和 ApplyStateMachine 那边重复回复（两边都是"取走才回复"）
        reply.done->Run();
    }
}

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
    replyMeta.type = proto::methodType::GetOperation;  // 超时清理时按类型回错误

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
    replyMeta.type = proto::methodType::SetOperation;  // 超时清理时按类型回错误

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
    replyMeta.type = proto::methodType::DelOperation;  // 超时清理时按类型回错误

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
