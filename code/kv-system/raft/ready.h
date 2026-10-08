#pragma once
#include <raft/readonly.h>
#include <resource/raft.pb.h>
namespace kv {

class Raft;

// ============================================================================
// Ready：Raft 状态机交给上层应用的一"袋"工作
//
// 大白话：Raft 状态机（Raft 类）不直接碰磁盘、不直接发网络消息，它只负责
// "算出该干什么"。上层应用（RaftNodeImpl）定期调用 node_->GetReady()，
// 拿到一个 Ready——里面装着"需要我做的事"：
//   - entries        → 请帮我写进 WAL（还没落盘的新日志）
//   - hardState      → 请帮我保存硬状态（term/vote/commit）
//   - snapshot       → 请帮我保存快照
//   - committedEntries → 这些日志已经提交了，请应用（执行）它们
//   - messages       → 请帮我把这些消息发给对应的节点
//   - readStates     → 请帮我把这些读结果返回给客户端
// 上层把上面的事干完，再调用 node_->Advance(rd) 告诉 raft "干完了，继续"。
// 这就是 etcd 的 Ready 机制，把"状态机计算"和"IO 操作"完全解耦。
// ============================================================================
struct Ready {
    Ready() : mustSync(false) {}

    // 构造：从 raft 里"打包"当前所有待办事项。
    // preSoftState/preHardState 是上一次的软/硬状态，用来对比"是否有变化"
    explicit Ready(std::shared_ptr<Raft> raft,
                   std::shared_ptr<SoftState> preSoftState,
                   const proto::HardState& preHardState);

    // 这个 Ready 里是否真的有活要干（空 Ready 就跳过，避免无谓 IO）
    bool ContainsUpdates() const;

    // 两个 Ready 是否内容完全相同（测试/去重用）
    bool Equal(const Ready& rd) const;

    // 从 Ready 里取出"本次应用到的最高索引"（上层应用完后，
    // 用这个索引更新 applied）。没有可应用的则返回 0
    uint64_t AppliedCursor() const;

    // 当前节点的易变状态（leader 是谁、我是啥角色）。
    // 如果没有变化，softState 为 null，上层可跳过
    std::shared_ptr<SoftState> softState;

    // 需要在发送消息之前保存到稳定存储的当前节点状态（term/vote/commit）。
    // 如果没有更新，等于空状态
    proto::HardState hardState;

    // readStates：可安全执行的线性一致读列表。
    // 大白话：上层检查自己 appliedIndex 是否超过 ReadState.index，
    // 超过了就可以放心执行读取并回包
    std::vector<ReadState> readStates;

    // entries：需要在发送消息之前保存到稳定存储的条目（写 WAL）
    std::vector<std::shared_ptr<proto::Entry>> entries;

    // Snapshot：需要保存到稳定存储的快照（写快照文件）
    proto::Snapshot snapshot;

    // committedEntries: 已经提交、可以交给上层应用到状态机的条目
    std::vector<std::shared_ptr<proto::Entry>> committedEntries;

    // messages：在条目提交到稳定存储后需要发送的出栈消息。
    // 注意：如果包含 MsgSnap，上层必须在快照接收/失败时调用
    // ReportSnapshot 告诉 raft 结果
    std::vector<std::shared_ptr<proto::Message>> messages;

    // mustSync：硬状态和条目是否必须同步写盘（fsync）后再继续。
    // 大白话：涉及新任期/新投票/新日志时必须同步刷盘（怕掉电丢），
    // 只是 commit 前进时可以异步写（性能优化）
    bool mustSync;
};

using ReadyPtr = std::shared_ptr<Ready>;

}  // namespace kv
