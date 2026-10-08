#pragma once
#include <resource/raft.pb.h>

namespace kv {

// ============================================================================
// Unstable: Raft 日志的"不稳定区"(还没落盘的那部分日志)
//
// 大白话：Raft 的日志分两个地方存：
//   1. storage_（MemoryStorage）—— 已经落盘的"稳定区"
//   2. unstable_（本类）—— 刚产生、还没写进 WAL 的"不稳定区"
// 每轮 Ready 里, unstable 中的条目会被交给上层写 WAL; 写成功后调用
// StableTo() 把它们从 unstable 里挪走（标记为已稳定），后续查询就落回 storage;
//
// 索引关系：unstable.entries_[i] 对应的日志索引 = i + offset_
// 例如 offset_=5 时，entries_[0] 是日志第 5 条。
// ============================================================================
class Unstable {
       public:
        // 构造：offset 是 unstable 里第一条日志的索引。
        // 新建 RaftLog 时把 offset 设为 storage 最后索引 + 1，表示"不稳定的
        // 部分从那里开始"
        explicit Unstable(uint64_t offset):offset_(offset) {}

        // 如果有快照，返回快照后的第一条日志索引（快照索引 + 1）
        void MaybeFirstIndex(uint64_t& index, bool& ok);

        // 如果有条目或快照，返回最后一个索引
        void MaybeLastIndex(uint64_t& index, bool& ok);

        // 如果索引 i 在 unstable 里，返回它的任期
        void MaybeTerm(uint64_t index, uint64_t& term, bool& ok);

        // 把 index 及以前的条目标记为"已稳定"（已经写进 WAL 了），
        // 从 unstable 里删掉，offset 前进。
        // term 参数用来校验（防止 index 匹配但 term 不匹配的误删）
        void StableTo(uint64_t index, uint64_t term);

        // 把快照标记为"已稳定"（快照文件已保存），清掉内存里的快照
        void StableSnapTo(uint64_t index);

        // 用快照恢复：整个 unstable 重置（offset 变为快照索引+1，清空条目）
        void Restore(std::shared_ptr<proto::Snapshot> snapshot);

        // 截断并追加新条目（这是 unstable 的核心操作）：
        // 新条目来了，如果和已有条目冲突，先截掉冲突的后半段再追加。
        // 冲突的情况发生在"旧的 leader 复活"时，它的日志和现 leader 不一致
        void TruncateAndAppend(
            std::vector<std::shared_ptr<proto::Entry>> entries);

        // 取出 [low, high) 区间的条目（low/high 是日志索引）
        void Slice(uint64_t low, uint64_t high,
                   std::vector<std::shared_ptr<proto::Entry>>& entries);

       public:
        // 如果有，传入的不稳定快照(还没保存到磁盘的快照)
        std::shared_ptr<proto::Snapshot> snapshot_;

        // 所有尚未写入存储（WAL）的条目
        std::vector<std::shared_ptr<proto::Entry>> entries_;
        
        // unstable 里第一条日志的 index（起点）
        uint64_t offset_;
};

using UnstablePtr = std::shared_ptr<Unstable>;

}  // namespace kv
