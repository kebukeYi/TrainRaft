#ifndef ROCKSDB_ENGINE
#define ROCKSDB_ENGINE

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/write_batch.h>
#include <store/engine/kv_engine.h>

#include <cstdint>
#include <iostream>
#include <unordered_map>

namespace kv {
// ============================================================================
// KVEngine：对 RocksDB 的简单封装（KV 存储引擎）
//
// 大白话：状态机执行日志时真正落数据的地方。每个节点本地都有一份
// RocksDB（目录 node_N/rocksdb），存着全量的 key-value。
// Raft 保证所有节点的这份 DB 内容一致（通过复制并应用日志）。
// 本类只提供几个简单的操作：Set（写）、Get（读）、Delete（删）、
// 批量 Set/Get（快照恢复用）。
// ============================================================================
class KVEngine {
   public:
    KVEngine() = default;
    KVEngine(const std::string& dbPath) : dbPath_(dbPath), db_(nullptr) {
        // 初始化存储引擎选项
        options_.create_if_missing = true;  // 目录不存在就自动创建
        // 限制内存使用
        options_.write_buffer_size = 64 * 1024 * 1024;  // 写缓冲 64MB
        options_.max_write_buffer_number = 2;           // 最多 2 个写缓冲
        // 打开存储引擎
        Open();
    }

    ~KVEngine() { Close(); }

    // 写入或更新（单条）
    bool Set(const std::string& key, const std::string& value);
    // 批量写入或更新（快照恢复用）
    bool Set(const std::unordered_map<std::string, std::string>& kvData);
    // 读取（单条）
    bool Get(std::string& key, std::string& value);
    // 批量读取（遍历整张表，Keys/快照用）
    bool Get(std::unordered_map<std::string, std::string>& kvData);
    // 删除（单条）
    bool Delete(const std::string& key);
    // 批量删除
    bool Delete(const std::vector<std::string>& keys);

    // 打开存储引擎
    bool Open();
    // 关闭存储引擎（先刷 WAL 再关）
    bool Close();

   private:
    std::string dbPath_;               // 数据库目录
    std::unique_ptr<rocksdb::DB> db_;  // RocksDB 句柄
    rocksdb::Options options_;         // 打开选项
};
}  // namespace kv

#endif /* ROCKSDB_ENGINE */
