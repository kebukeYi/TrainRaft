#include <common/log.h>
#include <raft/unstable.h>

namespace kv {

// 有快照的话，第一条 可用索引 = 快照索引 + 1;
void Unstable::MaybeFirstIndex(uint64_t& index, bool& ok) {
    if (snapshot_) {
        ok = true;
        index = snapshot_->metadata().index() + 1;
    } else {
        ok = false;
        index = 0;
    }
}

// 最后一个索引：优先看条目（offset + 条数 - 1），没有条目就看快照
void Unstable::MaybeLastIndex(uint64_t& index, bool& ok) {
    if (!entries_.empty()) {
        ok = true;
        index = offset_ + entries_.size() - 1;
        return;
    }

    // entries_ 为空时看快照
    if (snapshot_) {
        ok = true;
        index = snapshot_->metadata().index();
        return;
    }
    // 又没有日志, 又没有快照, 那么就没有最后索引;
    index = 0;
    ok = false;
}

// 查索引 i 的任期（前提: i 在 unstable 范围内）
void Unstable::MaybeTerm(uint64_t index, uint64_t& term, bool& ok) {
    term = 0;
    ok = false;

    // i 比 offset 小：不在条目区，可能正好落在快照上
    if (index < offset_) {
        if (!snapshot_) {
            // false: 没有快照，i 不在 unstable 里;
            return;
        }
        if (snapshot_->metadata().index() == index) {
            // 快照正好是第 index 条，任期就是快照的任期
            term = snapshot_->metadata().term();
            ok = true;
            return;
        }
        return;
    }

    // i 在条目区，但要先确认没超出最后一条;
    uint64_t last = 0;
    bool last_ok = false;
    MaybeLastIndex(last, last_ok);
    if (!last_ok) {
        return;
    }
    if (index > last) {
        return;
    }
    ok = true;
    term = entries_[index - offset_]->term();
}

// 标记"index 及以前的条目已稳定"（已写入 WAL）;
// 大白话：上层每轮把 unstable 里的条目写进 WAL 后，就调用这个函数，
// 把这些条目从 unstable 里删除，offset 前移。
// 只有 index 的任期和 unstable 里存的任期一致才删（防止删错）
void Unstable::StableTo(uint64_t index, uint64_t term) {
    uint64_t gt = 0;
    bool ok = false;
    MaybeTerm(index, gt, ok);

    if (!ok) {
        return;
    }

    // 任期匹配且 index 在条目区内，才删除 [offset, index] 这一段
    if (gt == term && index >= offset_) {
        uint64_t n = index + 1 - offset_;
        entries_.erase(entries_.begin(), entries_.begin() + n);
        offset_ = index + 1;
    }
}

// 标记快照已稳定: 快照文件保存成功后，把内存里的快照清掉;
void Unstable::StableSnapTo(uint64_t index) {
    if (snapshot_ && snapshot_->metadata().index() == index) {
        snapshot_ = nullptr;
    }
}

// 用快照整体恢复: unstable 从快照处重新开始
void Unstable::Restore(std::shared_ptr<proto::Snapshot> snapshot) {
    offset_ = snapshot->metadata().index() + 1;
    entries_.clear();
    snapshot_ = snapshot;
}

// 截断并追加（unstable 的核心逻辑，处理日志冲突）：
// 三种情况：
//   1. after == offset+size()：新条目正好接在尾部 —— 直接追加
//   2. after <= offset_：新条目起点在 unstable 起点之前 —— 整体替换
//   3. offset_ < after < offset_+size()：新条目和旧条目部分重叠 ——
//      保留重叠前的，截掉重叠起的旧条目，再接上新条目
void Unstable::TruncateAndAppend(
    std::vector<std::shared_ptr<proto::Entry>> entries) {
    if (entries.empty()) {
        return;
    }

    // 新条目的起点索引
    uint64_t after = entries[0]->index();
    // 5 6 7
    // offset: 5
    // size: 3

    // after: 3
    // after: 6,10
    // after: 8

    if (after == after + entries_.size()) {
        // 情况1：完美衔接（最常见的正常追加路径）
        entries_.insert(entries_.end(), entries.begin(), entries.end());
    } else if (after <= offset_) {
        // 情况2：新日志覆盖了整个 unstable —— 直接替换
        // 这发生在 恢复快照/leader 冲突重写时
        LOG_INFO("replace the unstable entries from index %lu", after);
        offset_ = after;
        entries_ = std::move(entries);
    } else {
        // after >= offset_，说明新条目和旧条目有重叠
        // 情况3：部分重叠 —— 截掉 from `after` 开始的旧条目再追加
        // 典型场景：旧 leader 复活带来和现 leader 冲突的日志, 出现日志交叉;
        LOG_INFO("Truncate the unstable entries before index %lu", after);
        std::vector<std::shared_ptr<proto::Entry>> entries_slice;
        // 先把 [offset_, after) 的旧条目保留下来
        this->Slice(offset_, after, entries_slice);
        // 再接上新条目
        entries_slice.insert(entries_slice.end(), entries.begin(),
                             entries.end());
        entries_ = std::move(entries_slice);
    }
}

// 切片：取 [low, high) 索引区间的条目（下标换算：index - offset_）
void Unstable::Slice(uint64_t low, uint64_t high,
                     std::vector<std::shared_ptr<proto::Entry>>& entries) {
    assert(high > low);
    uint64_t upper = offset_ + entries_.size();
    if (low < offset_ || high > upper) {
        LOG_FATAL("unstable.slice[%lu,%lu) out of bound [%lu,%lu]", low, high,
                  offset_, upper);
    }

    entries.insert(entries.end(), entries_.begin() + low - offset_,
                   entries_.begin() + high - offset_);
}

}  // namespace kv
