#pragma once
#include <common/status.h>
#include <resource/raft.pb.h>

#include <memory>
#include <mutex>
using namespace proto;
namespace kv {

// ============================================================================
// Storage: Raft 日志的"持久化存储接口"
//
// 大白话：Raft 算法本身不关心日志存在哪，只关心"能不能读历史日志、有没有
// 快照"。所以定义了一个抽象接口 Storage，谁想当"存储"就实现它。
// 本项目用的是下面的 MemoryStorage（纯内存，真正的落盘交给上层 WAL 做）。
// Raft 通过 raftLog_ 来读写日志：已落盘的看 storage_，还没落盘的看 unstable_。
// ============================================================================
class Storage {
   public:
    ~Storage() = default;

    // 返回已保存的硬状态（term/vote/commit）和集群配置（ConfState）
    virtual Status InitialState(proto::HardState& hardState,
                                proto::ConfState& confState) = 0;

    // 返回区间 [low, high) 内的日志条目；maxSize 限制返回总大小，
    // 但若存在条目至少返回一条。
    virtual Status Entries(uint64_t low, uint64_t high, uint64_t maxSize,
                           std::vector<std::shared_ptr<Entry>>& entries) = 0;

    // 返回第 i 条日志的任期；i 必须在 [FirstIndex()-1, LastIndex()] 范围内。
    // 注意 FirstIndex-1 那条的任期也会保留（快照前一条的"哑条目"），
    // 因为 leader 发送 append 时需要 prevLogTerm 来匹配。
    virtual Status Term(uint64_t i, uint64_t& term) = 0;

    // 日志最后一条的索引
    virtual Status LastIndex(uint64_t& index) = 0;

    // 日志第一条可用索引；更早的条目已经被压缩进快照了。
    // 若只有哑条目（刚启动），第一条不可用
    virtual Status FirstIndex(uint64_t& index) = 0;

    // 返回最新快照。如果暂时没准备好，可以返回特定错误，
    // 让 raft 知道"稍后再来问"
    virtual Status Snapshot(std::shared_ptr<proto::Snapshot>& snapshot) = 0;
};

using StoragePtr = std::shared_ptr<Storage>;

// ============================================================================
// MemoryStorage：用内存数组实现的 Storage（仿 etcd 的 MemoryStorage）
//
// 大白话：所有日志条目放在一个 vector 里，重启就没了，所以它只是"运行期
// 缓存"。真正的持久化是上层 WAL 负责的：每次 Ready 的条目会同时写 WAL
// （磁盘）和这里（内存）。重启时从 WAL 恢复数据填进来。
//
// 数组的索引关系（关键）：
//   entries_[0] 永远是"哑条目"（哨兵），它的 index 记录着快照的索引，
//   真实日志从 entries_[1] 开始。
//   entries_[i] 对应的日志索引 = entries_[0].index + i
//   例：快照做到日志第 100 条，则 entries_[0].index=100，
//       entries_[1] 存的是日志第 101 条
// ============================================================================
class MemoryStorage : public Storage {
   public:
    // 创建一个空的 MemoryStorage
    explicit MemoryStorage() : snapshot_(new proto::Snapshot()) {
        // 初始时放入一条任期为零的哑条目（index=0），作为数组的"原点"
        std::shared_ptr<proto::Entry> entry(new proto::Entry());
        entries_.emplace_back(std::move(entry));
    }

    virtual Status InitialState(proto::HardState& hardState,
                                proto::ConfState& confState);

    void SetHardState(proto::HardState& hardState);

    virtual Status Entries(uint64_t low, uint64_t high, uint64_t maxSize,
                           std::vector<std::shared_ptr<proto::Entry>>& entries);

    virtual Status Term(uint64_t i, uint64_t& term);

    virtual Status LastIndex(uint64_t& index);

    virtual Status FirstIndex(uint64_t& index);

    virtual Status Snapshot(std::shared_ptr<proto::Snapshot>& snapshot);

    // 压缩：丢弃 compact_index 之前的所有日志条目。
    // 大白话：日志太长了占内存，做完快照后把快照覆盖的旧日志删掉。
    // 注意：不能压缩掉比 applied 还新的日志（快照还没覆盖到）
    Status Compact(uint64_t compact_index);

    // 追加日志条目（可能截断冲突的旧条目）
    Status Append(std::vector<std::shared_ptr<proto::Entry>> entries);

    // 生成一个快照（data 是上层给的 KV 数据），后续可通过 Snapshot() 获取
    Status CreateSnapshot(uint64_t index, std::shared_ptr<proto::ConfState> cs,
                          std::vector<uint8_t> data,
                          std::shared_ptr<proto::Snapshot>& snapshot);

    // 用给定快照的内容覆盖本存储：把 entries_ 重置成只含哑条目，
    // 哑条目的 index/term 改成快照的 index/term（表示"之前的日志都不要了"）
    Status ApplySnapshot(const proto::Snapshot& snapshot);

   public:
    Status LastIndexImpl(uint64_t& index);
    Status FirstIndexImpl(uint64_t& index);

    std::mutex
        mutex_;  // 内存存储会被多个线程访问（raft 线程 + 应用线程），加锁保护
    proto::HardState hardState_;                 // 持久化的硬状态
    std::shared_ptr<proto::Snapshot> snapshot_;  // 最新快照

    // 日志条目数组，索引关系见类注释;
    // entries_[i] 对应的日志位置为 i + snapshot.Metadata.Index
    // 例如日志是 1,2,3,4,5,6，快照到第 6 条后数组变成：
    // [6],1,2,3,4,5,6   （[6] 是哑条目）
    // 查日志索引 N 时：entries_[N - snapshot.Metadata.Index]
    std::vector<std::shared_ptr<proto::Entry>> entries_;
};

using MemoryStoragePtr = std::shared_ptr<MemoryStorage>;

}  // namespace kv
