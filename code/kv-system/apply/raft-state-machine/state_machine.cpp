#include <common/log.h>
#include <common/random_device.h>
#include <common/utils.h>
#include <raft-node/raft_node.h>
#include <raft-state-machine/state_machine.h>
#include <store/engine/kv_engine.h>

#include <chrono>
#include <memory>
#include <msgpack.hpp>
#include <thread>
#include <unordered_map>

namespace kv {

// 构造:
// 1. 创建 KV 引擎（RocksDB）
// 2. 创建客户端服务（ClientServiceImpl）
// 3. 启动客户端 brpc server（监听 address："ip:客户端端口"）
// 4. 如果带了快照数据（重启恢复），解包后灌进 DB
StateMachine::StateMachine(RaftNode* raft, std::vector<uint8_t> snap, std::string address, std::string dbDir)
    : raft_(raft),
      db_(new KVEngine(dbDir)),
      service_(new ClientServiceImpl(raft_, db_)) {
    // 先创建 keep-alive 守卫，保证后面 Start() 里的 ioService_.run() 不会空转退出
    work_ = std::make_unique<boost::asio::io_service::work>(ioService_);

    // 注册并启动客户端 RPC 服务（客户端连这个端口调 Set/Get/...）
    rpcServer_.AddService(service_, brpc::SERVER_DOESNT_OWN_SERVICE);
    rpcServer_.Start(address.c_str(), nullptr);
    // 快照数据 → KV 表 → 写进 DB（重启恢复/快照恢复）
    std::unordered_map<std::string, std::string> kvData = SnapDataParseFromArray(snap);
    db_->Set(kvData);
}

StateMachine::~StateMachine() {
    // 先停掉客户端服务的超时清理线程，再停事件循环、等工作线程退出
    if (service_ != nullptr) {
        service_->Stop();
    }
    work_.reset();
    ioService_.stop();
    if (worker_.joinable()) {
        worker_.join();  // 等待工作线程退出
    }
}

// 启动工作线程: 线程里跑 ioService_ 事件循环,
// 之后 ApplyStateMachine/GetSnapshot 等操作都通过 ioService_.post() 投递到这
void StateMachine::Start(std::promise<pthread_t>& promise) {
    auto handler = [this, &promise]() {
        promise.set_value(pthread_self());  // 把线程 id 交给调用方
        this->ioService_.run();             // 事件循环(阻塞)
    };
    worker_ = std::thread(handler);
}

void StateMachine::Stop() {
    if (service_ != nullptr) {
        service_->Stop();  // 停掉待回复请求的超时清理线程
    }
    work_.reset();  // 松开 keep-alive，run() 才能返回
    ioService_.stop();
    if (worker_.joinable()) {
        worker_.join();
    }
}

// KV 表 -> 字节数组: 用 msgpack 打包整个 unordered_map
// （快照的本质就是把这张表序列化，让其他节点也能恢复出同样的数据）
std::shared_ptr<SnapshotData> StateMachine::SnapDataSerializeToArray(
    std::unordered_map<std::string, std::string> kvData) {
        
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, kvData);
    auto data = std::make_shared<SnapshotData>(sbuf.data(), sbuf.data() + sbuf.size());
    return data;
}

// 字节数组 -> KV 表：msgpack 解包
std::unordered_map<std::string, std::string>
StateMachine::SnapDataParseFromArray(const SnapshotData& snapData) {
    std::unordered_map<std::string, std::string> kvData;
    if (snapData.empty()) {
        LOG_WARN("empty snapshot");
        return kvData;  // 空快照（新节点），返回空表
    }
    msgpack::object_handle oh = msgpack::unpack((const char*)snapData.data(), snapData.size());
    try {
        oh.get().convert(kvData);  // 解包成 unordered_map
    } catch (std::exception& e) {
        LOG_WARN("invalid snapshot");  // 数据坏了，返回空表
    }
    return kvData;
}

// 上层（RaftNodeImpl::MaybeTriggerSnapshot）用它生成 Raft 快照
// 取当前整张 KV 表（异步生成）: 在工作线程里执行，通过回调返回;
void StateMachine::GetSnapshot(const OnGetSnapshot& callback) {
    auto cb = [this, callback] {
        std::unordered_map<std::string, std::string> kvData;
        db_->Get(kvData);  // 遍历 KV 引擎
        std::shared_ptr<SnapshotData> data = SnapDataSerializeToArray(kvData);
        callback(data);
    };
    ioService_.post(std::move(cb));  // 投递到工作线程执行
}


// 用快照恢复（异步）: 解包快照数据并写进 DB
void StateMachine::RecoverFromSnapshot(const proto::Snapshot& snap) {
    auto cb = [this, snap] {
        std::shared_ptr<SnapshotData> snapData = std::make_shared<SnapshotData>(
            snap.data().begin(), snap.data().end());
        std::unordered_map<std::string, std::string> kv = SnapDataParseFromArray(*snapData);
        db_->Set(kv);  // 覆盖写进 DB
        LOG_DEBUG("finished publishing snapshot at index %lu", snap.metadata().index());
    };
    ioService_.post(cb);
}

// ============================================================================
// ApplyStateMachine：应用一条已提交的日志条目（客户端请求最终被执行的地方）
//
// 大白话：Raft 提交一条日志后，每个节点都会执行它（保证所有节点数据一致）。
// 流程：
//   1. 解析日志内容（RaftEntryData）
//   2. 过滤：只有"当初接到这个客户端请求的节点"才需要回复客户端
//      （通过 nodeid 判断），其他节点只需默默执行
//   3. 按操作类型执行：Set/Del 写 DB，Get 读 DB
//   4. 找到当初登记的 RPC 请求（requestid），回包给客户端
// ============================================================================
void StateMachine::ApplyStateMachine(std::shared_ptr<proto::Entry> entry) {
    // uint64 nodeid = 1;    // 节点ID：哪个节点接到的客户端请求（用来过滤，只让该节点回复客户端）
    // uint64 requestid = 2; // 请求ID：全局唯一，用于把"回复"和"当初的 RPC 请求"对上号
    // methodType type = 3;  // 请求类型：Set/Get/Del
    // string key = 4;       // 键
    // string value = 5;     // 值
    // string message = 6;   // 附带消息（如错误信息）
    proto::RaftEntryData entryData;
    entryData.ParseFromString(entry->data());

    // ★ 关键：把"要不要回复客户端"和"要不要执行状态机"分开！
    //   - 只有"当初接到这个请求的节点"(is_origin) 才需要回复客户端；
    //   - 但写操作(Set/Del) 必须在**所有**节点上执行，否则各副本 RocksDB 内容不一致。
    //     Raft 的前提就是：每个节点执行相同的日志序列，得到相同的状态。
    //   （原来这里是 nodeid 不匹配就直接 return，导致写只落在发起节点上）
    const bool is_origin = (entryData.nodeid() == raft_->NodeId());

    // 只有 origin 才能取到待回复登记（其它节点根本没有这条登记）
    RaftReply replyMeta;
    const bool need_reply =
        is_origin && service_->TakePendingRequest(entryData.requestid(), replyMeta);
    
    // 特殊类型:读索引警告（appliedIndex 还没追上 readIndex，拒绝读）
    // 只有发起读的节点会遇到（它一定有登记）
    if (need_reply && entry->type() == proto::EntryType::EntryWarningReadIndex) {
        proto::GetResponse* response = static_cast<proto::GetResponse*>(replyMeta.response);
        // entryData.set_message(entry->message());
        response->set_message(entry->message());
        response->set_value("");  // 读索引警告，没数据
        response->set_readindex(entry->index());
        replyMeta.done->Run();  // 回复客户端（带警告）
        return;
    }

    // 开始应用状态机：按操作类型真正执行
    switch (entryData.type()) {
        case proto::methodType::SetOperation: {
            // 写：★所有节点都必须执行★，保证各副本一致
            db_->Set(entryData.key(), entryData.value());
            if (need_reply) {
                static_cast<proto::SetResponse*>(replyMeta.response)->set_message("ok");
            }
            break;
        }
        case proto::methodType::DelOperation: {
            // 删：同上，所有节点都执行
            db_->Delete(entryData.key());
            if (need_reply) {
                static_cast<proto::DelResponse*>(replyMeta.response)->set_message("ok");
            }
            break;
        }
        case proto::methodType::GetOperation: {
            // 读：读不改变状态，只有发起节点,需要真正读并回包;
            if (need_reply) {
                proto::GetResponse* response = static_cast<proto::GetResponse*>(replyMeta.response);
                std::string key = entryData.key();
                std::string value;
                db_->Get(key, value);
                response->set_value(value);
                response->set_message(entryData.message());
                response->set_readindex(entry->index());
            }
            break;
        }
        default: {
            LOG_ERROR("not supported type %d", entryData.type());
        }
    }

    // 回复客户端: 只有发起节点有登记，执行 brpc 的 done 回调;
    if (need_reply) {
        LOG_INFO("回复客户端, 请求ID: %d", entryData.requestid());
        replyMeta.done->Run();
    }
}
}  // namespace kv
