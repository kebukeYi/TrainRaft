#pragma once
#include <common/log.h>
#include <common/status.h>
#include <common/utils.h>
#include <resource/raft.pb.h>
#include <resource/store.pb.h>
#include <stdio.h>
#include <store/wal/wal_file.h>

#include <memory>
#include <string>

namespace kv {

// ============================================================================
// WALFeature: WAL 的管理器（负责多文件管理、写入、读取恢复）
//
// 大白话：WAL 不是一个文件，而是一串文件（文件名带序号+索引）：
//   0000000000000001-0000000000000100.wal
//   序号(seq)──┘           └──索引(index)
// 文件按"日志索引"分段：每个文件记录从某个索引开始的日志。
//
// 本类职责：
//   1. Create：新建一个空 WAL（写一条"快照标记"当起点）
//   2. Open：按快照索引打开相关文件（只保留快照之后的文件）
//   3. Save/SaveEntry/SaveHardState/SaveSnapshot：写入（每个 Ready 调用）
//   4. ReadAll：重启时把所有记录读出来，恢复硬状态和日志条目
// ============================================================================

// 1. 先提前声明一下这个类（告诉编译器有这么一个类）
class WALFeature;

// 2. 接着定义别名（此时编译器已经知道 WALFeature 的存在了）
using WALptr = std::shared_ptr<WALFeature>;

// 3. 然后再写类定义
class WALFeature {
   private:
    explicit WALFeature(const std::string& dir) : dir_(dir), enti_(0) {}

    // 解析一条 WAL 记录，恢复到内存（按类型：条目/硬状态/快照标记）
    void HandleRecordWalRecord(
        WALtype type, const char* data, size_t dataLen, bool& matchsnap,
        proto::HardState& hs, std::vector<std::shared_ptr<proto::Entry>>& ents);

    std::string dir_;           // WAL 目录
    proto::HardState state_;    // 在 WAL 头部记录的 hardstate（最近一次保存的）
    proto::WALsnapshot start_;  // 开始读取所需的快照（恢复起点）
    uint64_t enti_;             // 最后一条保存到 wal 的日志索引
    std::vector<std::shared_ptr<WALfile>> files_;  // 所有 WAL 文件（按顺序）

   public:
    // 创建全新的 WAL（在空目录里写一个初始文件）
    static void Create(const std::string& dir);

    // 打开 WAL：只保留快照（snap）之后的文件，供恢复使用
    static WALptr Open(const std::string& dir, const proto::WALsnapshot& snap);

    ~WALFeature() = default;

    // ReadAll：读取全部记录，恢复出硬状态和日志条目。
    // 之后 WAL 就准备好继续追加新记录了
    Status ReadAll(proto::HardState& hs,
                   std::vector<std::shared_ptr<proto::Entry>>& ents);

    // Save：保存一批条目 + 硬状态（每个 Ready 调用，写 WAL 的核心入口）
    Status Save(proto::HardState hs,
                const std::vector<std::shared_ptr<proto::Entry>>& ents);

    // 保存快照标记（记录快照做到第几条）
    Status SaveSnapshot(const proto::WALsnapshot& snap);

    // 保存一条日志条目
    Status SaveEntry(const proto::Entry& entry);

    // 保存硬状态
    Status SaveHardState(const proto::HardState& hs);

    // 切换新文件（当前文件写满时调用）（本项目简化实现，空操作）
    Status Cut();

    // release_to 会释放那些索引小于给定索引的 wal 文件，
    // 但保留其中索引最大的那个（本项目简化实现，空操作）
    Status ReleaseTo(uint64_t index);

    // 列出目录下所有 ".wal" 文件（排序后）
    void GetWalNames(const std::string& dir, std::vector<std::string>& names);

    // 解析文件名：取出序号 seq 和索引 index
    static bool ParseWalName(const std::string& name, uint64_t* seq,
                             uint64_t* index);

    // names 应该已按序列号排序。
    // IsValidSeq 检查序列号是否连续递增
    static bool IsValidSeq(const std::vector<std::string>& names);

    // SearchIndex：在排序的文件名里，找到"索引 <= 给定索引"的最后一个
    // 文件的下标（恢复时从它开始读）
    static bool SearchIndex(const std::vector<std::string>& names,
                            uint64_t index, uint64_t* nameIndex);
};
}  // namespace kv
