#include <common/log.h>
#include <common/utils.h>
#include <raft/node.h>
#include <raft/util.h>

namespace kv {

// ============================================================================
// NodeImpl：Node 接口的具体实现（一个线程不安全的 Node）
//
// 大白话：把 Raft 状态机（Raft 类）包一层，供上层调用。
// 它自己存两个"上一次的状态"（prevSoftState_/prevHardState_），
// 用来在生成 Ready 时对比"状态有没有变化"，有变化才放进 Ready 里。
// 注意: NodeImpl 要求单线程串行使用（本项目中所有调用都在 raft 线程上）。
// ============================================================================
class NodeImpl : public Node {
   public:
    RaftPtr raft_;                                     // 内部的 Raft 状态机
    std::shared_ptr<proto::SoftState> prevSoftState_;  // 上一次的软状态
    proto::HardState prevHardState_;                   // 上一次的硬状态
   public:
    // ------------------------------------------------------------------
    // StartNode 构造: 创建全新集群的节点
    // 关键动作：如果日志是空的（全新节点），构造一批"配置变更日志"
    // （把每个集群成员加进来），并立即标记为已提交。
    // 大白话：每个节点启动时，都把"集群里有谁"先写进自己的日志，
    // 这样所有节点的初始成员表一致
    // ------------------------------------------------------------------
    NodeImpl(const Config& conf, const std::vector<proto::PeerContext>& peers) {
        // 创建 Raft 状态机
        raft_ = std::make_shared<Raft>(conf);

        uint64_t lastIndex = 0;
        Status status = conf.storage->LastIndex(lastIndex);
        if (!status.IsOk()) {
            LOG_FATAL("%s", status.ToString().c_str());
        }

        // 日志为空 = 全新节点(StartNode);否则是恢复（RestartNode）
        if (lastIndex == 0) {
            // 以 follower 身份启动, 任期 1
            raft_->BecomeFollower(1, 0);
            std::vector<std::shared_ptr<proto::Entry>> entries;
            // 为每个集群成员构造一条"加节点"的配置变更日志; 也包括自己; 何意味?
            for (size_t i = 0; i < peers.size(); ++i) {
                auto& peer = peers[i];
                // 构造加节点指令（序列化成字节）
                proto::ConfChange cs;
                cs.set_id(0); // 变更编号
                // 变更类型
                cs.set_type(proto::ConfChangeType::ConfChangeAddNode);
                cs.set_nodeid(peer.id()); // 要操作的目标节点 id
                // 附加信息(新节点的地址 "ip:传输端口:客户端端口")
                cs.set_context(peer.context());

                std::string serializedData;
                cs.SerializeToString(&serializedData);
                // 再包一层日志条目
                std::shared_ptr<proto::Entry> entry(new proto::Entry());
                entry->set_type(proto::EntryConfChange);
                entry->set_term(1);
                entry->set_index(i + 1);  // 从第 1 条开始编号
                entry->set_data(serializedData);
                entries.push_back(entry);
            }

            // 先追加到未持久化的 unstable 存储中
            raft_->raftLog_->Append(entries);
            // 每个节点都这么干, 所以这些日志"天然已提交";
            raft_->raftLog_->committed_ = entries.size();

            // 把每个成员 构建进度表(AddNode 会更新 prs_)
            for (auto& peer : peers) {
                raft_->AddNode(peer.id());
            }
        }

        // 初始化"上一次"的软/硬状态(用于生成 Ready 时对比)
        prevSoftState_ = raft_->SoftState();
        if (lastIndex == 0) {
            prevHardState_ = proto::HardState();
        } else {
            prevHardState_ = raft_->HardState();
        }
    }

    // ------------------------------------------------------------------
    // RestartNode 构造: 重启恢复的节点（成员表从存储里恢复）
    // ------------------------------------------------------------------
    NodeImpl(const Config& conf) {
        uint64_t lastIndex = 0;
        Status status = conf.storage->LastIndex(lastIndex);
        
        if (!status.IsOk()) {
            LOG_FATAL("%s", status.ToString().c_str());
        }

        // 创建 Raft 状态机 (Raft 构造时已从 storage 恢复 硬状态和成员表)
        // BecomeFollower() 开始走拉票选举之路;
        raft_ = std::make_shared<Raft>(conf);

        // 初始化"上一次"的软/硬状态
        prevSoftState_ = raft_->SoftState();

        if (lastIndex == 0) {
            prevHardState_ = proto::HardState();
        } else {
            prevHardState_ = raft_->HardState();
        }
    }

    // 析构
    ~NodeImpl() = default;

    // 推时钟（交给 raft）
    void Tick() final { raft_->Tick(); }

    // 发起竞选（发一个 MsgHup 给自己）
    Status Campaign() final {
        std::shared_ptr<Message> msg(new proto::Message());
        msg->set_type(proto::MessageType::MsgHup);
        return raft_->Step(std::move(msg));
    }

    // 提议写日志：把数据包成 MsgProp 消息喂给 raft
    Status Propose(std::shared_ptr<std::vector<uint8_t>> data) final {
        std::shared_ptr<proto::Message> msg(new proto::Message());
        msg->set_type(proto::MessageType::MsgProp);
        msg->set_from(raft_->id_);
        auto* entry = msg->add_entries();
        entry->set_type(proto::EntryType::EntryNormal);
        entry->set_term(0);
        entry->set_index(0);
        entry->set_data(std::string(data->begin(), data->end()));
        // 核心算法: 把消息喂给 raft 状态机（交给当前角色对应的 step_ 函数处理）;
        return raft_->Step(std::move(msg));
    }

    // 提议配置变更：把 ConfChange 序列化后包成 EntryConfChange 条目
    Status ProposeConfChange(const proto::ConfChange& cc) final {
        std::shared_ptr<proto::Message> msg(new proto::Message());
        std::string serializedData;
        cc.SerializeToString(&serializedData);
        msg->set_type(proto::MessageType::MsgProp);
        auto* entry = msg->add_entries();
        entry->set_type(proto::EntryType::EntryConfChange);
        entry->set_term(0);
        entry->set_index(0);
        entry->set_data(serializedData);
        return raft_->Step(std::move(msg));
    }

    // 喂消息给 raft（网络消息到达时调用）。
    // 先做两道校验：
    //   1. 本地消息（MsgHup 等）不允许从网络进来
    //   2. 来自非成员节点的"回复类消息"（如旧成员的投票回复）丢弃
    Status Step(std::shared_ptr<proto::Message> msg) final {
        // 本地消息不允许从网络进来;
        if (IsLocalMsg(msg)) {
            // 本地消息只允许本地产生，从网络收到说明有人伪造;
            return Status::InvalidArgument(
                "raft: cannot step raft local message");
        }
        // Raft 里有个进度表 prs_，记录了集群成员的进度（每个成员的 match/index）
        ProgressPtr progress = raft_->GetProgress(msg->from());
        if (progress || !IsResponseMsg(msg)) {
            // 是集群成员 或 不是回复类消息：正常处理
            return raft_->Step(msg);
        }
        return Status::InvalidArgument("raft: cannot step as peer not found");
    }

    // 取 Ready：把 raft 里所有待办事项打包（见 ready.cpp 的构造逻辑）。
    // 取走消息队列后还要把 raft 里未提交大小的计数减掉（已提交的条目不再占"未提交配额"）
    ReadyPtr GetReady() final {
        // 封装 raft 里所有待办事项(见 ready.cpp 的构造逻辑)
        ReadyPtr rd = std::make_shared<Ready>(raft_, prevSoftState_, prevHardState_);
        // 待发送消息队列清空（取走了，交给上层发送）
        raft_->msgs_.clear();
        // 条目提交后，减少未提交大小的计数;
        raft_->ReduceUncommittedSize(rd->committedEntries);
        return rd;
    }

    // 是否有待处理的 Ready（逐项检查：软状态/硬状态/快照/消息/条目/读结果）
    bool HasReady() final {
        assert(prevSoftState_);
        
        // 软状态是否发生过变化;
        if (!IsEqualSoftState(*raft_->SoftState(), *prevSoftState_)) {
            return true;  // 软状态变了
        }

        // 硬状态是否发生过变化;
        proto::HardState hs = raft_->HardState();
        if (!IsEmptyHardState(hs) && !IsEqualHardState(hs, prevHardState_)) {
            return true;  // 硬状态变了
        }

        // 是否需要保存快照;
        std::shared_ptr<proto::Snapshot> snapshot = raft_->raftLog_->unstable_->snapshot_;
        if (snapshot && !IsEmptySnapshot(*snapshot)) {
            return true;  // 有快照要保存
        }

        // 是否有 待发送消息/新日志/待应用条目;
        if (!raft_->msgs_.empty() || !raft_->raftLog_->UnstableEntries().empty() || raft_->raftLog_->HasNextEntries()) {
            return true;  // 有消息/新日志/待应用条目
        }

        return !raft_->readStates_.empty();  // 有读结果
    }

    // Advance: 上层处理完 Ready 后调用, 推进 raft 的进度指针;
    void Advance(ReadyPtr rd) final {
        // 更新"上一次"软/硬状态（下次对比用）
        if (rd->softState) {
            prevSoftState_ = rd->softState;
        }
        if (!IsEmptyHardState(rd->hardState)) {
            prevHardState_ = rd->hardState;
        }

        // 推进 applied：把本次应用到的最新索引记下来。
        // 注意：即使硬状态里有新的 commit 索引，也不意味着所有新条目
        // 都被应用了（存在按大小分页的机制），所以用 Ready 里的
        // AppliedCursor（实际应用到的索引）
        uint64_t index = rd->AppliedCursor();
        if (index > 0) {
            raft_->raftLog_->AppliedTo(index);
        }

        // 已写 WAL 的条目: 从 unstable 挪走（标记稳定）
        if (!rd->entries.empty()) {
            auto& entry = rd->entries.back();
            raft_->raftLog_->StableTo(entry->index(), entry->term());
        }

        // 已保存的快照：清掉 unstable 里的快照
        if (!IsEmptySnapshot(rd->snapshot)) {
            raft_->raftLog_->StableSnapTo(rd->snapshot.metadata().index());
        }

        // 已处理的读结果：清空 raft 里的读状态
        if (!rd->readStates.empty()) {
            raft_->readStates_.clear();
        }
    }

    // 应用配置变更：真正修改集群成员表（上层应用完 EntryConfChange 后调用）
    std::shared_ptr<proto::ConfState> ApplyConfChange(
        const proto::ConfChange& cc) final {
        std::shared_ptr<proto::ConfState> state(new proto::ConfState());
        if (cc.nodeid() == 0) {
            // 特例：nodeid 为 0 表示"只查当前成员表"
            auto& nodes = *state->mutable_nodes();
            std::vector<uint64_t> node_ids(nodes.begin(), nodes.end());
            raft_->Nodes(node_ids);
            return state;
        }

        switch (cc.type()) {
            case proto::ConfChangeAddNode: {
                raft_->AddNode(cc.nodeid());
                break;
            }
            default: {
                LOG_FATAL("unexpected conf type");
            }
        }
        // 返回最新的成员表
        std::vector<uint64_t> peers;
        for (auto& node : state->nodes()) {
            peers.emplace_back(node);
        }

        raft_->Nodes(peers);
        return state;
    }

    // 转移领导权
    void TransferLeadership(uint64_t lead, ino64_t transferee) final {
        // 手动设置 from/to，让 leader 主动转移领导权
        std::shared_ptr<proto::Message> msg(new proto::Message());
        msg->set_type(proto::MessageType::MsgTransferLeader);
        msg->set_from(transferee);
        msg->set_to(lead);

        Status status = raft_->Step(std::move(msg));
        if (!status.IsOk()) {
            LOG_WARN("TransferLeadership %s", status.ToString().c_str());
        }
    }

    // 发起线性一致读（rctx 是读请求标识，会原样带回结果里）
    Status ReadIndex(std::shared_ptr<std::vector<uint8_t>> rctx) final {
        std::shared_ptr<proto::Message> msg(new proto::Message());
        msg->set_type(proto::MessageType::MsgReadIndex);
        proto::Entry* entry = msg->add_entries();
        entry->set_type(proto::EntryType::EntryNormal);
        entry->set_term(0);
        entry->set_index(0);
        entry->set_data(std::string(rctx->begin(), rctx->end()));
        // handler
        return raft_->Step(std::move(msg));
    }

    // 获取 Raft 状态（本项目未实现，返回空）
    std::shared_ptr<proto::RaftStatus> RaftStatus() final {
        LOG_DEBUG("no impl yet");
        return nullptr;
    }

    // 报告节点不可达：包成 MsgUnreachable 喂给 raft
    void ReportUnreachable(uint64_t id) final {
        std::shared_ptr<proto::Message> msg(new proto::Message());
        msg->set_type(proto::MessageType::MsgUnreachable);
        msg->set_from(id);

        Status status = raft_->Step(std::move(msg));
        if (!status.IsOk()) {
            LOG_WARN("report_unreachable %s", status.ToString().c_str());
        }
    }

    // 报告快照发送结果：包成 MsgSnapStatus 喂给 raft
    void ReportSnapshot(uint64_t id, proto::SnapshotStatus status) final {
        bool rej = (status == proto::SnapshotStatus::SnapshotFailure);
        std::shared_ptr<proto::Message> msg(new proto::Message());
        msg->set_type(proto::MessageType::MsgSnapStatus);
        msg->set_from(id);
        msg->set_reject(rej);

        Status s = raft_->Step(std::move(msg));
        if (!s.IsOk()) {
            LOG_WARN("report_snapshot %s", s.ToString().c_str());
        }
    }

    // 停止（本项目无特殊清理）
    void Stop() final {}
};

// 工厂方法：创建全新集群的节点
Node* Node::StartNode(const Config& conf,
                      const std::vector<PeerContext>& peers) {
    return new NodeImpl(conf, peers);
}

// 工厂方法：创建重启恢复的节点
Node* Node::RestartNode(const Config& conf) { return new NodeImpl(conf); }

}  // namespace kv
