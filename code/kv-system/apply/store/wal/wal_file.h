#pragma once
#include <common/log.h>
#include <common/status.h>
#include <common/utils.h>
#include <resource/raft.pb.h>
#include <resource/store.pb.h>
#include <stdio.h>

#include <memory>
#include <string>

namespace kv {
// ============================================================================
// WAL（Write-Ahead Log 预写日志）的单文件格式
//
// 大白话：Raft 日志必须"先写盘再回 RPC"（否则掉电就丢），这个写盘动作
// 由 WAL 完成。WAL 由多个文件组成（一个文件写满了就开下一个），每个文件
// 里是一串"记录"（WALrecord）。
//
// 记录格式（每条记录 = 固定头 + 数据）：
//   [type(1字节)][crc32(4字节)][len(3字节)][data(变长)]
//   type：这条记录是什么（日志条目/硬状态/快照标记）
//   crc32：数据校验值（读的时候重算对比，发现写坏就截断丢弃）
//   len：数据长度（用 3 字节存，最大 16MB）
//   data：protobuf 序列化后的数据
// ============================================================================

// 单条记录允许的最大数据长度（3 字节能表示的最大值）
#define MAX_WAL_RECORD_LEN 0x00FFFFFF

// 记录类型
enum class WALtype : uint8_t {
    walInvalidType = 0,  // 无效/未定义
    walEntryType = 1,    // 日志条目（Entry）
    walStateType = 2,    // 硬状态（HardState：term/vote/commit）
    walCrcType = 3,      // 校验记录（本项目未使用）
    walSnapshotType = 4  // 快照标记（记录快照做到第几条了）
};

// 记录头：注意 #pragma pack(1) 表示按 1 字节对齐，
// 这样结构体在内存里不填充，sizeof 正好是 8 字节
#pragma pack(1)
struct WALrecord {
    WALtype type;   /*数据类型，1 字节*/
    uint32_t crc;   /*crc32校验码，4 字节*/
    uint8_t len[3]; /*数据长度，3 字节（小端存储）*/
    char data[0];   /*可变长度数据：柔性数组，不参与 sizeof()*/
};
#pragma pack()

// ============================================================================
// WALfile：一个 WAL 文件
//
// 大白话：一个文件 = 一个"记录缓冲区"。写入时先攒在内存 dataBuffer_ 里
// （Append），需要落盘时调用 Sync() 一次性写进文件（减少磁盘 IO 次数）。
// 读取时 ReadAll 把整个文件读进内存，逐条解析。
// ============================================================================
class WALfile {
   public:
    // 打开（或创建）指定路径的 WAL 文件
    WALfile(const char* path, int64_t seq);

    ~WALfile();

    // 截断：文件写到 offset 处为止（后面全不要）。
    // 大白话：读的时候发现记录不完整/校验失败，说明上次写了一半就断电了，
    // 把脏尾巴截掉，保证文件干净
    void Truncate(size_t offset);
    // 追加一条记录到内存缓冲区（还没写盘）
    void Append(WALtype type, const std::string& data);
    // 把缓冲区内容一次性同步到文件（fwrite）
    void Sync();
    // 把整个文件内容读进内存
    void ReadAll(std::vector<char>& out);

   public:
    // 内存缓冲区：攒着待写盘的记录
    std::vector<uint8_t> dataBuffer_;
    int64_t seq_;    // 文件的序列号（文件名里的第一部分）
    long fileSize_;  // 文件当前大小
    FILE* fp_;       // 文件句柄
};

// 从记录头里读出数据长度（3 字节小端 → uint32）
static inline uint32_t GetWalRecordLen(const WALrecord& record) {
    return uint32_t(record.len[2]) << 16 | uint32_t(record.len[1]) << 8 |
           uint32_t(record.len[0]) << 0;
}

// 把长度写进记录头的 3 个字节（超长会被截断到 MAX_WAL_RECORD_LEN）
static inline void SetWalRecordLen(WALrecord& record, uint32_t len) {
    len = std::min(len, (uint32_t)MAX_WAL_RECORD_LEN);
    // 只用 3 字节存储长度（小端：低位在前）
    record.len[2] = (len >> 16) & 0x000000FF;
    record.len[1] = (len >> 8) & 0x000000FF;
    record.len[0] = (len >> 0) & 0x000000FF;
}

}  // namespace kv
