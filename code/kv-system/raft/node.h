#pragma once
#include <raft/config.h>
#include <raft/raft.h>
#include <resource/raft.pb.h>
#include <resource/store.pb.h>
using namespace proto;

namespace kv {

// ============================================================================
// Node: Raft 状态机对上层应用的"接口层"
//
// 大白话：Raft 类（纯算法）不能直接被上层用——上层需要的是：
//   提议写日志、读取 Ready、推进进度（Advance）、配置变更……
// Node 就是这层"壳"，把 Raft 算法包装成上层好用的接口。
// 具体实现是 node.cpp 里的 NodeImpl（单线程、线程不安全，需串行调用）。
// 大白话总结 Node 的用法循环：
//   1. 上层不断调用 Tick()（驱动时钟）
//   2. 有请求就 Propose()（写）/ ReadIndex()（线性读）
//   3. 有网络消息就 Step(msg)（喂给状态机）
//   4. 每次调用后用 HasReady() 检查，有就用 GetReady() 取出任务去执行，
//      执行完调 Advance() 告诉 raft "干完了"
// ============================================================================
class Node {
   public:
    ~Node() = default;

    // tick：推进内部逻辑时钟（选举超时和心跳超时都以 tick 为单位）
    virtual void Tick() = 0;

    // campaign：让节点变成候选人并开始竞选 leader
    virtual Status Campaign() = 0;

    // propose：提议把数据追加到日志。注意：提议可能悄悄丢失
    // （如 leader 更换），用户需要自己保证重试
    virtual Status Propose(std::shared_ptr<std::vector<uint8_t>> data) = 0;

    // ProposeConfChange：提议配置变更（加节点）。
    // 一次只能有一个配置变更在共识中；应用 EntryConfChange 类型条目时，
    // 需要调用 ApplyConfChange
    virtual Status ProposeConfChange(const proto::ConfChange& cc) = 0;

    // step：把消息喂给状态机推进（网络消息到达时调用）
    virtual Status Step(std::shared_ptr<proto::Message> msg) = 0;

    // ready：返回当前 Raft 状态机的"待办事项"（Ready 包，见 ready.h）
    virtual ReadyPtr GetReady() = 0;

    // HasReady：检查是否有待处理的 Ready（有再取，避免空转）
    // 此方法中的检查逻辑应与 Ready.containsUpdates() 一致
    virtual bool HasReady() = 0;

    // advance：通知 raft "上层已经处理完上一个 Ready"。
    // 上层通常在应用完 Ready 中的条目后调用；
    // 作为优化，也可以在应用过程中提前调用（比如 Ready 里带了大快照，
    // 应用快照很耗时，可以先 advance 再慢慢应用，不阻塞 raft 前进）
    virtual void Advance(ReadyPtr ready) = 0;

    // ApplyConfChange：把配置变更应用到本地节点（真正改成员表），
    // 返回 ConfState（必须记录到快照里）
    virtual std::shared_ptr<ConfState> ApplyConfChange(
        const proto::ConfChange& cc) = 0;

    // TransferLeadership：尝试把领导权转移给指定节点
    virtual void TransferLeadership(uint64_t lead, ino64_t transferee) = 0;

    // ReadIndex：发起线性一致读。读结果会出现在 Ready.readStates 里，
    // 一旦应用推进超过读索引，读请求就可以安全处理
    virtual Status ReadIndex(std::shared_ptr<std::vector<uint8_t>> rctx) = 0;

    // RaftStatus：返回 Raft 的当前状态
    virtual std::shared_ptr<proto::RaftStatus> RaftStatus() = 0;

    // ReportUnreachable：报告某节点上一次发送失败（不可达）。
    // leader 收到后会把它的进度退回探测状态重新同步
    virtual void ReportUnreachable(uint64_t id) = 0;

    // ReportSnapshot：报告快照发送结果（成功 SnapshotFinish / 失败
    // SnapshotFailure）。快照发送失败必须上报，否则 follower 永远等不到
    // 日志恢复，会卡死
    virtual void ReportSnapshot(uint64_t id, proto::SnapshotStatus status) = 0;

    // stop：终止节点
    virtual void Stop() = 0;

    // StartNode：根据配置和节点列表创建全新 Node
    // （会为每个对等体在初始日志中追加一条 ConfChangeAddNode 条目）
    static Node* StartNode(const Config& conf,
                           const std::vector<PeerContext>& peers);

    // RestartNode：和 StartNode 类似，但不接收节点列表——
    // 集群成员从 Storage 中恢复（重启流程用）
    static Node* RestartNode(const Config& conf);
};
}  // namespace kv
