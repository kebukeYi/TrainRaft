#include <common/log.h>
#include <raft/raft.h>
#include <raft/ready.h>
#include <raft/util.h>
#include <resource/raft.pb.h>
namespace kv {

// Ready 构造：把 raft 状态机里"所有待办事项"打包进一个 Ready
Ready::Ready(std::shared_ptr<Raft> raft,
             std::shared_ptr<proto::SoftState> preSoftState,
             const proto::HardState& preHardState) {
    // ① 未落盘的新日志(要写 WAL)
    this->entries.reserve(raft->raftLog_->UnstableEntries().size());
    this->entries = raft->raftLog_->UnstableEntries();

    // ② 要发出去的消息(把 raft 内部的消息队列整个搬过来)
    std::swap(this->messages, raft->msgs_);

    // ③ 已提交、待应用的条目(applied+1 到 committed)
    raft->raftLog_->NextEntries(committedEntries);

    // ④ 软状态：只有变化了才带上（lead/角色变了）
    std::shared_ptr<proto::SoftState> st = raft->SoftState();
    if (!IsEqualSoftState(*st, *preSoftState)) {
        this->softState = st;
    }

    // ⑤ 硬状态：只有变化了才带上（term/vote/commit 变了）
    proto::HardState hs = raft->HardState();
    if (!IsEqualHardState(hs, preHardState)) {
        this->hardState = hs;
    }

    // ⑥ 待保存的快照(unstable 里还没落盘的那个)
    std::shared_ptr<proto::Snapshot> snapshot =
        raft->raftLog_->unstable_->snapshot_;
    if (snapshot) {
        // copy：深拷贝一份给上层（防止上层慢慢写盘时 raft 内部又改了）
        this->snapshot = *snapshot;
    }

    // ⑦ 线性一致读的结果
    if (!raft->readStates_.empty()) {
        this->readStates = raft->readStates_;
    }

    // ⑧ 是否必须同步刷盘（有日志/投票/任期变化就必须同步）
    this->mustSync = IsMustSync(hs, hardState, entries.size());
}

// 这个 Ready 里是否真的有活要干。
// 注意最后一个条件写的是 readStates.empty()（原项目如此），
// 语义上应是 !readStates.empty()，不过上层调用前会用 HasReady 再过滤一遍
bool Ready::ContainsUpdates() const {
    return softState != nullptr || !IsEmptyHardState(hardState) ||
           !IsEmptySnapshot(snapshot) || !entries.empty() ||
           !committedEntries.empty() || !messages.empty() || readStates.empty();
}

// 本次应用到的最高索引：
// 优先看已提交条目里最后一条的索引；没有条目就看快照索引
uint64_t Ready::AppliedCursor() const {
    if (!committedEntries.empty()) {
        return committedEntries.back()->index();
    }
    uint64_t index = snapshot.metadata().index();
    if (index > 0) {
        return index;
    }
    return 0;
}

// 两个 Ready 是否完全一样（逐字段比较，测试/校验用）
bool Ready::Equal(const Ready& rd) const {
    LOG_INFO("Ready::Equal");
    // 软状态：一个有另一个没有 → 不等
    if ((this->softState && !rd.softState) ||
        (!this->softState && rd.softState)) {
        return false;
    }
    // 都有则比较内容
    if (this->softState && rd.softState &&
        !IsEqualSoftState(*this->softState, *rd.softState)) {
        return false;
    }
    // 硬状态
    if (!IsEqualHardState(this->hardState, rd.hardState)) {
        return false;
    }

    // 读状态列表
    if (this->readStates.size() != rd.readStates.size()) {
        return false;
    }

    for (size_t i = 0; i < this->readStates.size(); ++i) {
        if (!this->readStates[i].Equal(rd.readStates[i])) {
            return false;
        }
    }

    // 待写 WAL 的条目
    if (entries.size() != rd.entries.size()) {
        return false;
    }

    for (size_t i = 0; i < entries.size(); ++i) {
        if (!IsEqualEntry(*entries[i], *rd.entries[i])) {
            return false;
        }
    }

    // 快照
    if (!IsEqualSnapshot(this->snapshot, rd.snapshot)) {
        return false;
    }

    // 已提交待应用的条目
    if (committedEntries.size() != rd.committedEntries.size()) {
        return false;
    }

    for (size_t i = 0; i < committedEntries.size(); ++i) {
        if (!IsEqualEntry(*committedEntries[i], *rd.committedEntries[i])) {
            return false;
        }
    }
    return mustSync == rd.mustSync;
}

}  // namespace kv
