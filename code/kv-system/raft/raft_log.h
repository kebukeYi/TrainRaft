#pragma once
#include <raft/storage.h>
#include <raft/unstable.h>

namespace kv {
// ============================================================================
// RaftLog：Raft 的"日志管理类"——把 storage（已落盘）和 unstable（未落盘）
// 两部分日志统一成一个视图，并提供所有日志操作
//
// 大白话：日志是 Raft 的命根子。本类管理三个指针：
//   - storage_  ：已持久化的日志（MemoryStorage）
//   - unstable_ ：新产生、还没写盘的日志（Unstable）
//   - committed_：已提交位置（大多数节点都有这条日志了）
//   - applied_  ：已应用位置（已经交给上层状态机执行了）
//
// 最重要的不变量：applied_ <= committed_（不能应用还没提交的日志）
// ============================================================================
class RaftLog {
   public:
   
    // 构造：用 storage 的"最后索引+1"作为 unstable 的起点，
    // applied/committed 初始化为 first-1（快照之后的位置）
    explicit RaftLog(StoragePtr storage, uint64_t max_next_ents_size);

    ~RaftLog();

    // 表示"不限大小"（maxSize 传这个值就不裁剪）
    static uint64_t Unlimited() { return std::numeric_limits<uint64_t>::max(); }

    // 日志当前状态的可读字符串（打日志用）
    std::string StatusString() const {
        char buffer[64];
        int n = snprintf(buffer, sizeof(buffer),
                         "committed=%lu, applied=%lu, "
                         "unstable.offset=%lu, unstable.entries=%lu",
                         committed_, applied_, unstable_->offset_,
                         unstable_->entries_.size());
        return std::string(buffer, n);
    }

    // 追加日志（follower 收到 leader 的 MsgApp 时调用）：
    // 先检查"index 处日志的任期 == logTerm"（一致性检查，Raft 的核心），
    // 通过后查找冲突点并处理，然后推进 committed，返回新日志的最后索引
    // 大白话：leader 说"我日志第 index 条任期是 logTerm，后面跟着这些新条目"，
    // follower 先对账（如果对不上就拒绝），对上了就收下
    void MaybeAppend(uint64_t index, uint64_t logTerm, uint64_t committed,
                     std::vector<std::shared_ptr<Entry>> entries,
                     uint64_t& last_new_index, bool& ok);

    // 追加条目（本地 leader 提案用），返回追加后的最后索引
    uint64_t Append(std::vector<std::shared_ptr<Entry>> entries);

    // 查找冲突位置：
    //  - 无冲突且已有日志包含全部新条目 → 返回 0
    //  - 无冲突但有新条目 → 返回第一个新条目的索引
    //  - 有冲突 → 返回第一个冲突条目的索引（同 index 不同 term 即冲突）
    uint64_t FindConflict(const std::vector<std::shared_ptr<Entry>>& entries);


    // 取出"下一批要交给上层应用"的条目（applied+1 到 committed）
    // 大白话：这是 Ready 里 committedEntries 的数据来源
    void NextEntries(std::vector<std::shared_ptr<Entry>>& entries) const;

    // 是否还有待应用的条目（快速判断，避免 NextEntries 做拷贝）
    bool HasNextEntries() const;

    // 取 [low, high) 的日志条目，最多 maxSize 字节（超出裁剪）
    Status Slice(uint64_t low, uint64_t high, uint64_t maxSize,
                 std::vector<std::shared_ptr<Entry>>& entries) const;

    // 判断给定日志 (lastIndex, lastTerm) 是否比我的日志"更新"。
    // 规则（Raft 选举投票的判断依据）：
    //   先比任期：任期大则更新；任期相同再比索引，索引大等于则更新
    bool IsUpToDate(uint64_t lasti, uint64_t term) const {
        uint64_t local_t = LastTerm();
        return term > local_t || (term == local_t && lasti >= LastIndex());
    }

    // 未落盘的日志条目（给 Ready 用，准备写 WAL）
    std::vector<std::shared_ptr<Entry>>& UnstableEntries() {
        return unstable_->entries_;
    }

    // 尝试推进 committed 到 maxIndex，但要求"第 maxIndex 条日志的任期
    // 必须是当前任期 term"（Raft 只允许提交当前任期的日志，
    // 防止旧任期日志被错误提交）
    bool MaybeCommit(uint64_t max_index, uint64_t term);

    // 用快照恢复：committed 跳到快照索引，unstable 重置
    void Restore(std::shared_ptr<proto::Snapshot> snapshot);

    // 获取最新快照（unstable 里有就返回 unstable 的，否则问 storage）
    Status Snapshot(std::shared_ptr<proto::Snapshot>& snap) const;

    // 标记已应用到 index（上层应用完日志后调用）
    void AppliedTo(uint64_t index);

    // 标记条目已稳定（已写入 WAL）, 转发给 unstable
    void StableTo(uint64_t index, uint64_t term) {
        unstable_->StableTo(index, term);
    }

    // 标记快照已稳定（快照文件已保存）
    void StableSnapTo(uint64_t index) { unstable_->StableSnapTo(index); }

    // 从 index 开始取条目（到末尾，最多 maxSize 字节）
    Status Entries(uint64_t index, uint64_t maxSize,
                   std::vector<std::shared_ptr<Entry>>& entries) const {
        if (index > LastIndex()) {
            return Status::Ok();
        }
        return Slice(index, LastIndex() + 1, maxSize, entries);
    }

    // 推进 committed 到 to_commit（只增不减，且不能超过日志末尾）
    void CommitTo(uint64_t to_commit);

    // 第 index 条日志的任期是否等于 t
    bool MatchTerm(uint64_t index, uint64_t t);

    // 最后一条日志的任期
    uint64_t LastTerm() const;

    // 第 index 条日志的任期
    Status Term(uint64_t index, uint64_t& t) const;

    // 第一条可用日志索引
    uint64_t FirstIndex() const;

    // 最后一条日志索引
    uint64_t LastIndex() const;

    // 检查 [low, high) 是否越界（被压缩/超出末尾）
    Status MustCheckOutOfBounds(uint64_t low, uint64_t high) const;

    // 取出所有日志条目（测试/调试用）
    void AllEntries(std::vector<std::shared_ptr<Entry>>& entries);

   public:
    // storage 包含自上次快照以来的所有稳定条目, 已持久化;
    StoragePtr storage_;

    // unstable 包含所有不稳定条目和快照（还没写盘的部分）
    UnstablePtr unstable_;

    // committed 是已知在多数节点稳定存储中的最高日志位置
    // （大多数节点都确认有这条日志了，可以安全应用）
    uint64_t committed_;

    // applied 是应用程序被指示应用到状态机的最高日志位置
    // 不变量：applied <= committed
    uint64_t applied_;

    // maxNextEntsSize_ 是每次 NextEntries 返回消息的最大聚合字节数
    uint64_t maxNextEntsSize_;
};

using RaftLogPtr = std::shared_ptr<RaftLog>;

}  // namespace kv
