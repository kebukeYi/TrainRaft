#include <common/log.h>
#include <raft/raft_log.h>
#include <raft/util.h>

namespace kv {

// 构造: RaftLog 的初始化;
RaftLog::RaftLog(StoragePtr storage, uint64_t max_next_ents_size)
    : storage_(std::move(storage)),
      committed_(0),
      applied_(0),
      maxNextEntsSize_(max_next_ents_size) {

    assert(storage_);
    uint64_t first;
    // 内存中的日志都是来自于 wal;
    auto status = storage_->FirstIndex(first);
    assert(status.IsOk());

    uint64_t last;
    status = storage_->LastIndex(last);
    assert(status.IsOk());

    // 这里的 unstable_ 是 RaftLog 的一个成员变量, 表示不稳定的日志部分;
    // 它是一个 Unstable 类的智能指针，负责管理那些还没有被持久化到存储中的日志条目;
    // unstable 的起点 = storage 最后索引 + 1;
    // 意思是"storage 里的都是稳定的, 新的日志从 last+1 开始算不稳定";
    unstable_ = std::make_shared<Unstable>(last + 1);

    // applied 和 committed 都先设成 first-1（快照/起点之前的位置）
    // 如果 storage 里已有日志（重启恢复），后续会通过 LoadState 纠正
    applied_ = committed_ = first - 1;
}

RaftLog::~RaftLog() {}

// follower 处理 leader 的追加日志请求的核心函数
void RaftLog::MaybeAppend(uint64_t index, uint64_t logTerm, uint64_t committed,
                          std::vector<std::shared_ptr<proto::Entry>> entries,
                          uint64_t& lastNewIndex, bool& ok) {
    // 第 index 条日志的任期必须和 leader 声称的 logTerm 一致。
    // 大白话：这是 Raft 日志匹配原则——"我这里的日志和你说的对得上，
    // 才相信你后面带来的新日志"。对不上就拒绝，leader 会回退重试
    if (MatchTerm(index, logTerm)) {
        // 如果追加成功, 最后新条目的索引
        uint64_t lastnewi = index + entries.size();
        // 找冲突: 新条目里有没有和本地日志冲突的（同 index 不同 term）
        uint64_t ci = FindConflict(entries);
        if (ci == 0) {
            // 无冲突: 新条目全部都是本地日志的副本,不用管, 仅仅更新进度即可;
        } else if (ci <= committed_) {
            // 冲突位置在已提交的日志里 —— 严重错误！
            // 已提交的日志不可能被覆盖（否则违背 Raft 的安全性）
            LOG_FATAL(
                "entry %lu conflict with committed entry "
                "[committed(%lu)]",
                ci, committed_);
        } else {
            // 冲突位置在未提交区: 把冲突点之后的新条目接上
            assert(ci > 0);
            uint64_t offset = index + 1;
            uint64_t n = ci - offset;
            // 如果是 纯追加的话, 那么 n = 0, entries.erase 就不会删掉任何条目;
            // 丢掉新条目里和本地冲突的部分，保留从 ci 开始的新条目
            entries.erase(entries.begin(), entries.begin() + n);
            Append(std::move(entries));
        }

        // 推进提交索引(不能超过本次携带的条目末尾)
        CommitTo(std::min(committed, lastnewi));

        lastNewIndex = lastnewi;
        ok = true;
        return;
    } else {
        // preLog 对账失败，拒绝这次追加
        lastNewIndex = 0;
        ok = false;
    }
}

// 追加条目到日志（走 unstable，还没落盘）
uint64_t RaftLog::Append(std::vector<std::shared_ptr<proto::Entry>> entries) {
    // 要添加的条目为空，直接返回当前最后索引
    if (entries.empty()) {
        return LastIndex();
    }

    // 新条目起点索引的前一条(用于检查是否和已提交日志冲突)
    uint64_t after = entries[0]->index() - 1;
    // 如果要覆盖的位置在已提交区，说明要覆盖已提交的日志，绝对不允许
    if (after < committed_) {
        LOG_FATAL(
            "after(%lu) is out of range [committed(%lu)]\", after, "
            "committed_",
            after, committed_);
    }

    // 先写入中间层 unstable(暂时认为数据"不太可靠"，还没写盘)
    // 那么什么时机 就认可写盘了? 等待被提取;
    unstable_->TruncateAndAppend(std::move(entries));

    return LastIndex();
}

// 查找冲突：遍历新条目，找到第一个"本地日志对不上"的条目
// （同 index 但 term 不同 = 冲突；本地没有这个 index = 也是冲突/新条目）
uint64_t RaftLog::FindConflict(
    const std::vector<std::shared_ptr<proto::Entry>>& entries) {
        // 第一个日志都匹配上了, 为什么之后的日志还会出现不匹配?
        // 可能leader处在探查阶段, 因此不能直接覆盖;
    for (const std::shared_ptr<proto::Entry>& entry : entries) {
        // 依次判断是否都合规;
        if (!MatchTerm(entry->index(), entry->term())) {
            if (entry->index() < LastIndex()) {
                // 本地确实有同 index 的日志但 term 不同 → 真冲突
                uint64_t t;
                Status status = this->Term(entry->index(), t);
                LOG_INFO(
                    "found conflict at index %lu [existing "
                    "term: %lu, conflicting term: %lu], %s",
                    entry->index(), t, entry->term(),
                    status.ToString().c_str());
            }
            return entry->index();
        }
    }
    return 0;  // 全部匹配，无冲突
}

// 提供下一批需要 应用 的日志条目（applied+1 到 committed，受 maxNextEntsSize限制）
void RaftLog::NextEntries(std::vector<std::shared_ptr<proto::Entry>>& entries) const {
    // 起点: applied 的下一条（但至少是 FirstIndex，因为更早的进快照了)
    uint64_t off = std::max(applied_ + 1, FirstIndex());
    if (committed_ + 1 > off) {
        // 有可应用的条目
        Status status = Slice(off, committed_ + 1, maxNextEntsSize_, entries);
        if (!status.IsOk()) {
            LOG_FATAL("unexpected error when getting unapplied entries");
        }
    }
}

// 是否还有待应用条目(快速判断)
bool RaftLog::HasNextEntries() const {
    uint64_t off = std::max(applied_ + 1, FirstIndex());
    return committed_ + 1 > off;
}

// 尝试提交：只有"第 maxIndex 条日志的任期 == 当前任期"时才允许提交。
// 大白话：Raft 有个规则——leader 只能提交自己任期内的日志，
// 因为旧任期的日志可能没有被正确复制到多数节点
bool RaftLog::MaybeCommit(uint64_t maxIndex, uint64_t term) {
    if (maxIndex > committed_) {
        uint64_t t;
        // 查看这个索引处的日志任期是否和当前任期一致（不一致就不能提交）
        this->Term(maxIndex, t);
        if (t == term) {
            CommitTo(maxIndex);
            return true;
        }
    }
    return false;
}

// 用快照恢复：committed 直接跳到快照索引，unstable 重置
// （快照代表这个时刻之前的所有日志都已经被覆盖，不需要了）
void RaftLog::Restore(std::shared_ptr<proto::Snapshot> snapshot) {
    LOG_INFO("log starts to restore snapshot [index: %lu, term: %lu]",
             snapshot->metadata().index(), snapshot->metadata().term());
    committed_ = snapshot->metadata().index();
    unstable_->Restore(snapshot);
}

// 获取最新快照：unstable 里有（还没保存的）就用它，否则问 storage
Status RaftLog::Snapshot(std::shared_ptr<proto::Snapshot>& snap) const {
    if (unstable_->snapshot_) {
        snap = unstable_->snapshot_;
        return Status::Ok();
    }

    std::shared_ptr<proto::Snapshot> s;
    Status status = storage_->Snapshot(s);
    if (s) {
        snap = s;
    }
    return status;
}

// 标记已应用到 index（上层应用完条目后调用）
void RaftLog::AppliedTo(uint64_t index) {
    if (index == 0) {
        return;
    }
    // 应用的位置必须在 [prevApplied, committed] 之间
    if (committed_ < index || index < applied_) {
        LOG_ERROR(
            "applied(%lu) is out of range [prevApplied(%lu), "
            "committed(%lu)]",
            index, applied_, committed_);
    }
    applied_ = index;
}

// 切片: 取 [low, high) 的日志, 可能跨 storage(持久化未提交) 和 unstable(未持久化,可提交) 两部分;
Status RaftLog::Slice(uint64_t low, uint64_t high, 
    uint64_t maxSize,std::vector<std::shared_ptr<Entry>>& entries) const {
    
    Status status = MustCheckOutOfBounds(low, high);
    if (!status.IsOk()) {
        return status;
    }
    if (low == high) {
        return Status::Ok();
    }

    // 先从 storage(已落盘部分)切片
    if (low < unstable_->offset_) {
        status = storage_->Entries(low, std::min(high, unstable_->offset_), maxSize, entries);
        if (!status.IsOk()) {
            return status;
        }

        // 如果已经达到大小限制，就不再切 unstable 了
        if (entries.size() < std::min(high, unstable_->offset_) - low) {
            return Status::Ok();
        }
    }

    // 再从 unstable(未落盘部分)切片, 两部分拼起来
    if (high > unstable_->offset_) {
        std::vector<std::shared_ptr<Entry>> unstable;
        unstable_->Slice(std::max(low, unstable_->offset_), high, entries);
        entries.insert(entries.end(), unstable.begin(), unstable.end());
    }
    EntryLimitSize(maxSize, entries);  // 总大小超限就裁剪

    return Status::Ok();
}

// 推进 committed(只增不减，且不能超过日志最后一条)
void RaftLog::CommitTo(uint64_t toCommit) {
    if (committed_ < toCommit) {
        if (LastIndex() < toCommit) {
            // 要提交的位置比日志末尾还大：日志损坏了
            LOG_FATAL(
                "toCommit(%lu) is out of range [LastIndex(%lu)]. "
                "Was the raft log corrupted, Truncated, or lost?",
                toCommit, LastIndex());
        }
        committed_ = toCommit;
    } else {
        // toCommit 比当前 committed 小，忽略（提交索引不能倒退）
    }
}

// 第 index 条日志的任期是否等于 t
bool RaftLog::MatchTerm(uint64_t index, uint64_t t) {
    uint64_t termOut;
    Status status = this->Term(index, termOut);
    if (!status.IsOk()) {
        return false;
    }
    return t == termOut;
}

// 最后一条日志的任期
uint64_t RaftLog::LastTerm() const {
    uint64_t t;
    Status status = Term(LastIndex(), t);
    assert(status.IsOk());
    return t;
}

// 查第 index 条日志的任期（优先 unstable，其次 storage）
Status RaftLog::Term(uint64_t index, uint64_t& t) const {
    uint64_t dummyIndex = FirstIndex() - 1;  // 哑条目位置
    if (index < dummyIndex || index > LastIndex()) {
        // 超出范围：返回 0（代码里多处用 0 表示"不存在"）
        t = 0;
        return Status::Ok();
    }

    uint64_t termIndex;
    bool ok;

    // 先在 unstable 里找
    unstable_->MaybeTerm(index, termIndex, ok);
    if (ok) {
        t = termIndex;
        return Status::Ok();
    }

    // unstable 里没有，去 storage 里找
    Status status = storage_->Term(index, termIndex);
    if (status.IsOk()) {
        t = termIndex;
    }
    return status;
}

// 第一条可用日志索引（优先 unstable 的快照，其次 storage）
uint64_t RaftLog::FirstIndex() const {
    uint64_t index;
    bool ok;
    unstable_->MaybeFirstIndex(index, ok);
    if (ok) {
        return index;
    }

    Status status = storage_->FirstIndex(index);
    assert(status.IsOk());

    return index;
}

// 最后一条日志索引（优先 unstable，其次 storage）
uint64_t RaftLog::LastIndex() const {
    uint64_t index;
    bool ok;
    unstable_->MaybeLastIndex(index, ok);
    if (ok) {
        return index;
    }
    Status status = storage_->LastIndex(index);
    assert(status.IsOk());

    return index;
}

// 取出全部日志（测试用）。若因压缩暂时取不到，重试（竞争场景）
void RaftLog::AllEntries(std::vector<std::shared_ptr<Entry>>& entries) {
    entries.clear();
    LOG_DEBUG("RaftLog::AllEntries");
    Status status = this->Entries(FirstIndex(), RaftLog::Unlimited(), entries);
    if (status.IsOk()) {
        return;
    }

    // 若存在竞争性压缩（边取边被压缩）则重试
    if (status.ToString() ==
        Status::InvalidArgument(
            "requested index is unavailable due to compaction")
            .ToString()) {
        this->AllEntries(entries);
    }
    LOG_FATAL("%s", status.ToString().c_str());
}

// 越界检查：[low, high) 必须在 [FirstIndex, LastIndex+1] 内
Status RaftLog::MustCheckOutOfBounds(uint64_t low, uint64_t high) const {
    assert(high >= low);

    uint64_t first = FirstIndex();

    // low 比第一条还小：这部分日志被压缩进快照了，取不到
    if (low < first) {
        return Status::InvalidArgument(
            "requested index is unavailable due to compaction");
    }

    uint64_t length = LastIndex() + 1 - first;
    // high 超出日志末尾：越界
    if (low < first || high > first + length) {
        LOG_FATAL("slice[%lu,%lu) out of bound [%lu,%lu]", low, high, first,
                  LastIndex());
    }
    return Status::Ok();
}

}  // namespace kv
