#include <common/log.h>
#include <raft/progress.h>

namespace kv {

// 状态枚举转字符串（打日志用）
const char* ProgressStateToString(ProgressState state) {
    switch (state) {
        case ProgressStateProbe: {
            return "ProgressStateProbe";
        }
        case ProgressStateReplicate: {
            return "ProgressStateReplicate";
        }
        case ProgressStateSnapshot: {
            return "ProgressStateSnapshot";
        }
        default: {
            LOG_FATAL("unknown state %d", state);
        }
    }
}

// 往滑动窗口里加一条在途消息（环形缓冲）
void InFlight::Add(uint64_t inflight) {
    // 
    if (IsFull()) {
        LOG_FATAL("cannot add into a full inflights");
    }

    // 下一个写入位置 = 起点 + 已用数量（可能绕回开头）
    uint64_t next = start_ + count_;

    if (next >= size_) {
        next -= size_;  // 环形绕回
    }

    // 底层 vector 不够大就扩容（最多扩到 size_）
    if (next >= buffer_.size()) {
        uint32_t newSize = buffer_.size() * 2;
        if (newSize == 0) {
            newSize = 1;
        } else if (newSize > size_) {
            newSize = size_;
        }
        buffer_.resize(newSize);
    }
    buffer_[next] = inflight;
    count_++;
}

// 释放所有 <= to 的在途消息（follower 回复确认到第 to 条时调用）
void InFlight::FreeTo(uint64_t to) {
    if (count_ == 0 || to < buffer_[start_]) {
        // 窗口为空，或者 to 比最早的在途消息还小（过时回复），忽略
        return;
    }

    uint32_t idx = start_;
    size_t i;
    // 从头往后找：找到第一条比 to 大的在途消息，之前的全部释放
    for (i = 0; i < count_; i++) {
        if (to < buffer_[idx]) {
            // 找到首个大于 to 的在途消息，停下
            break;
        }

        // 索引前进（必要时绕回）
        idx++;

        if (idx >= size_) {
            idx -= size_;
        }
    }
    // 释放前 i 条：count 减少，起点后移
    count_ -= i;
    start_ = idx;
    if (count_ == 0) {
        // 全部释放完了，起点归零（避免 buffer 越用越大）
        start_ = 0;
    }
}

// 释放最早的那条（窗口满时腾位置用）
void InFlight::FreeFirstOne() { FreeTo(buffer_[start_]); }

// 进入复制状态：确认过进度了，从 match+1 开始连发
void Progress::BecomeReplicate() {
    // 重置这个节点的复制状态;
    ResetState(ProgressStateReplicate);
    next_ = match_ + 1;
}

// 进入探测状态：从 match+1 重新试探。
// 特殊情况：如果刚从快照状态切回来，要从 pendingSnapshot+1 开始探
void Progress::BecomeProbe() {
    // 如果原始状态是 ProgressStateSnapshot，则 progress 知道
    // 挂起的快照已成功发送到此对等节点，因此将从
    // pendingSnapshot + 1 开始探测
    if (state_ == ProgressStateSnapshot) {
        uint64_t pending = pendingSnapshot_;
        ResetState(ProgressStateProbe);
        next_ = std::max(match_ + 1, pending + 1);
    } else {
        ResetState(ProgressStateProbe);
        next_ = match_ + 1;
    }
}

// 进入快照状态：记录要发的快照索引，暂停普通复制
void Progress::BecomeSnapshot(uint64_t snapshot) {
    ResetState(ProgressStateSnapshot);
    pendingSnapshot_ = snapshot;
}

// 重置到指定状态：清暂停、清快照、清窗口
void Progress::ResetState(ProgressState st) {
    paused_ = false;
    pendingSnapshot_ = 0;
    this->state_ = st;
    this->inflights_->Reset();
}

// 进度描述（打日志用）
std::string Progress::String() const {
    char buffer[256];
    int n = snprintf(buffer, sizeof(buffer),
                     "next = %lu, match = %lu, state = %s, waiting = %d, "
                     "pendingSnapshot = %lu",
                     next_, match_, ProgressStateToString(state_), IsPaused(),
                     pendingSnapshot_);
    return std::string(buffer, n);
}

// 是否暂停发消息：
//  - probe：发了一条在等回复 → 暂停
//  - replicate：窗口满了 → 暂停
//  - snapshot：正在发快照 → 暂停
bool Progress::IsPaused() const {
    switch (state_) {
        case ProgressStateProbe: {
            return paused_;
        }
        case ProgressStateReplicate: {
            return inflights_->IsFull();
        }
        case ProgressStateSnapshot: {
            return true;
        }
        default: {
            LOG_FATAL("unexpected state");
        }
    }
}

// 更新进度: n 比 match_ 大才更新 match_（并恢复暂停）,
// next_ 总是推进到至少 n+1
bool Progress::MaybeUpdate(uint64_t n) {
    bool updated = false;
    if (match_ < n) {
        match_ = n;
        updated = true;
        Resume();
    }
    // n <= match_
    if (next_ < n + 1) {
        next_ = n + 1;
    }
    return updated;
}

// 进度回退: follower 拒绝了（它说"我日志只到 last"，rejected 是我发的起点）。
// 返回 false 表示"这个拒绝消息是过时的，忽略"
bool Progress::MaybeDecreasesTo(uint64_t rejected, uint64_t last) {
    if (state_ == ProgressStateReplicate) {
        // 正常复制状态下: 如果 rejected <= match_，说明这是条迟到的旧回复，忽略
        if (rejected <= match_) {
            return false;
        }
        // rejected > match_
        // 直接把 next 回退到 match+1，重新从确认过的位置发
        next_ = match_ + 1;
        return true;
    }

    // 探测状态下: rejected 必须正好是 next-1 才是有效的拒绝
    // （probe 一次只发一条，拒绝的必然是自己刚发的那条）
    if (next_ - 1 != rejected) {
        return false;
    }

    // 回退到 min(rejected, last+1)：last 是 follower 提示的"我最后到哪了"
    next_ = std::min(rejected, last + 1);
    if (next_ < 1) {
        next_ = 1;
    }
    // 恢复状态;
    Resume();
    return true;
}

// 快照是否可以中止：follower 的 match 已经追上 pendingSnapshot 了
bool Progress::NeedSnapshotAbort() const {
    return state_ == ProgressStateSnapshot && match_ >= pendingSnapshot_;
}

}  // namespace kv
