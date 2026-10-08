#pragma once
#include <common/status.h>
#include <common/utils.h>
#include <resource/raft.pb.h>

#include <memory>
#include <string>

namespace kv {
// ============================================================================
// SnapshotFeature：Raft 快照文件的保存与加载
//
// 大白话：快照 = 某一时刻整张 KV 表 + 元信息（日志索引/任期/集群配置）。
// 保存到磁盘的格式：
//   [dataLen(4字节)][crc32(4字节)][data(变长)]
//   其中 data 是 protobuf 序列化后的 Snapshot 消息。
// 文件名：<term>-<index>.snap（16 位十六进制），例如：
//   0000000000000003-0000000000001234.snap
// 加载时优先加载最新的快照（按文件名降序），校验 CRC，坏文件改名 ".broken"
// ============================================================================

// 快照文件记录头：8 字节
struct SnapshotRecord {
    uint32_t dataLen;  // 数据长度，4 字节
    uint32_t crc32;    // 数据校验值，4 字节
    char data[0];      // 柔性数组：数据本体（不参与 sizeof）
};

class SnapshotFeature {
   private:
    std::string dir_;  // 快照目录（node_N/snap）

   public:
    explicit SnapshotFeature(const std::string& dir) : dir_(dir) {}

    ~SnapshotFeature() = default;

    // 加载最新快照（找不到返回 NotFound）
    Status Load(proto::Snapshot& snapshot);

    // 保存快照到磁盘
    Status SaveSnap(const proto::Snapshot& snapshot);

    // 生成快照文件名（term-index.snap）
    static std::string SnapName(uint64_t term, uint64_t index);

   private:
    // 收集目录下所有 ".snap" 文件（降序排序）
    void GetSnapNames(std::vector<std::string>& names);

    // 加载单个快照文件（校验长度/CRC）
    Status LoadSnap(const std::string& filename, proto::Snapshot& snapshot);
};

}  // namespace kv
