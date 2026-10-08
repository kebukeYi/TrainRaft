#pragma once
#include <memory>
#include <vector>

namespace kv {

// ============================================================================
// Progress：leader 眼里某个 follower 的"复制进度"
//
// 大白话：leader 要往每个 follower 上复制日志，就得记住"每个 follower 复制
// 到哪了"。这个类就是给每个 follower 记的一本小账本，包含两个核心数字：
//   - match_：follower 已经确认复制成功的最后一条日志索引
//   - next_ ：下一次要发给这个 follower 的日志索引（next_ = match_ + 1 起步）
// 另外还有复制状态机（Probe/Replicate/Snapshot）和流量控制（InFlight 窗口）。
// ============================================================================

// follower 复制进度所处的三种状态
enum ProgressState {
    // 探测状态：leader 不确定 follower 日志到哪了，先发一条试水。
    // 如果 follower 拒绝（说"我这里还没到那"），leader 就根据提示回退。
    // 每次只发一条，慢但稳（用于日志刚开始追赶时）
    ProgressStateProbe = 0,

    // 复制状态：确认了 follower 的进度后，可以一次性发多条日志，
    // 乐观地把 next 往前推（用 InFlight 滑动窗口做流量控制）
    ProgressStateReplicate = 1,

    // 快照状态：follower 落后太多（日志已被压缩），只能发快照。
    // 发快照期间暂停普通日志复制
    ProgressStateSnapshot = 2
};

// 将 ProgressState 枚举值转换为字符串形式（打日志用）
const char* ProgressStateToString(ProgressState state);

// ============================================================================
// InFlight：滑动窗口（在途消息跟踪器）
//
// 大白话：leader 给 follower 发了一条 MsgApp（可能带好几条日志），在收到
// 回复之前，这条消息的最后日志索引就记在"在途窗口"里。窗口满（IsFull）了
// 就暂停发新的，等收到回复（FreeTo 释放）再继续。这样既保证了速度，
// 又不会把网络和对端缓冲区打爆。
// 底层是个环形缓冲。
// ============================================================================
class InFlight {
   public:
    // 构造函数：maxInflightMsgs 是窗口大小（最多允许几条在途消息）
    explicit InFlight(uint64_t maxInflightMsgs)
        : start_(0), count_(0), size_(static_cast<uint32_t>(maxInflightMsgs)) {}

    // 重置（状态切换时调用，清空所有在途记录）
    void Reset() {
        start_ = 0;
        count_ = 0;
    }

    // 窗口是否已满（满了就不能再发了）
    bool IsFull() const { return size_ == count_; }

    // 添加一个在途消息的索引（发 MsgApp 时调用）
    void Add(uint64_t inflight);

    // 释放所有 <= to 的在途消息（收到 follower 回复时调用）
    void FreeTo(uint64_t to);

    // 释放最早的那条在途消息（窗口满了腾位置用）
    void FreeFirstOne();

    // 环形缓冲的起始位置
    uint32_t start_;
    // 在途消息数量
    uint32_t count_;
    // 缓冲容量
    uint32_t size_;
    // 环形缓冲：存每条在途消息里最后一条日志的索引
    std::vector<uint64_t> buffer_;
};

// ============================================================================
// Progress：leader 视角下某个 follower 的完整进度（见文件头注释）
// ============================================================================
class Progress {
   public:
    // 构造函数：maxInflight 是流量窗口大小
    explicit Progress(uint64_t maxInflight)
        : match_(0),
          next_(0),
          state_(ProgressState::ProgressStateProbe),
          paused_(false),
          pendingSnapshot_(0),
          recentActive_(false),
          inflights_(new InFlight(maxInflight)) {}

    // 进入复制状态：next 从 match+1 开始（确认过进度了，可以连发）
    void BecomeReplicate();
    // 进入探测状态：从 match+1（或快照后）重新试探
    void BecomeProbe();
    // 进入快照状态：记录 pendingSnapshot 为要发的快照索引
    void BecomeSnapshot(uint64_t snapshot);
    // 重置到指定状态（清暂停、清快照、清窗口）
    void ResetState(ProgressState state);
    // 进度描述字符串（打日志用）
    std::string String() const;

    // 是否暂停发消息（probe 等待回复 / replicate 窗口满 / snapshot
    // 期间都算暂停）
    bool IsPaused() const;

    // 设置暂停（probe 模式下发了一条就要暂停等回复）
    void SetPause() { this->paused_ = true; }

    // 恢复（收到任何回复就恢复）
    void Resume() { this->paused_ = false; }

    // 更新进度：如果 n 比 match_ 还小（过时消息），返回 false；
    // 否则更新 match_ 和 next_，返回 true
    bool MaybeUpdate(uint64_t n);

    // 乐观更新：直接把 next 推到 n+1（replicate 模式下先发先算）
    void OptimisticUpdate(uint64_t n) { next_ = n + 1; }

    // 进度回退：follower 拒绝了消息（rejected 位置），
    // 把 next 回退到 min(rejected, last+1)。如果消息已过时则返回 false
    bool MaybeDecreasesTo(uint64_t rejected, uint64_t last);

    // 快照是否可以中止：follower 的 match 已经追上 pendingSnapshot 了
    // （说明快照可能白发了，可以切回正常复制）
    bool NeedSnapshotAbort() const;

    // 快照发送失败：清掉 pendingSnapshot，下次重试
    void SnapshotFailure() { pendingSnapshot_ = 0; }

    // match_：follower 已经跟 leader 同步的最大 index。
    // next_：下一次要发送的日志索引。
    // 大白话：match_ 表示"follower 确认收到到第几条"，是判断能否提交的关键；
    // next_ 表示"leader 接下来从第几条开始发"
    uint64_t match_;

    // 下一个要发送的日志索引
    uint64_t next_;

    // 状态：决定 leader 如何与这个 follower 交互（见 ProgressState）
    ProgressState state_;

    // paused_：探测模式下是否暂停发送（发了一条，等回复）
    bool paused_;

    // pendingSnapshot_：快照状态下，正在发送的快照索引。
    // 设置了它，此 follower 的复制就暂停，直到快照成功/失败被上报
    uint64_t pendingSnapshot_;

    // recentActive_：这个 follower 最近是否活跃（收到过它的任何消息）。
    // leader 用它来判断"多数派是否还活着"（checkQuorum）
    bool recentActive_;

    // inflights_：在途消息滑动窗口（见 InFlight 注释）。
    // 发 MsgApp 时把最后一条日志的索引 Add 进去，收到回复时 FreeTo 释放。
    // 索引必须按顺序添加;
    std::shared_ptr<InFlight> inflights_;
};

using ProgressPtr = std::shared_ptr<Progress>;

}  // namespace kv
