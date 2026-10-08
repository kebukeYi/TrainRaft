#include <common/utils.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/write_batch.h>
#include <store/engine/kv_engine.h>

#include <iostream>

namespace kv {
    
// 打开存储引擎：用配置好的 options 打开（或创建）数据库
bool KVEngine::Open() {
    rocksdb::Status s = rocksdb::DB::Open(options_, dbPath_, &db_);
    if (!s.ok()) {
        std::cerr << "Failed to open database: " << s.ToString() << std::endl;
        return false;
    }
    return true;
}

// 关闭存储引擎：先同步 RocksDB 自己的 WAL，确保数据落盘，再释放句柄
bool KVEngine::Close() {
    if (db_) {
        if (!db_->SyncWAL().ok()) {
            std::cerr << "Failed to sync WAL" << std::endl;
        }
        db_.reset();
        return true;
    }
    return false;
}

// 写入或更新：Put(key, value)
bool KVEngine::Set(const std::string& key, const std::string& value) {
    rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), key, value);
    if (!s.ok()) {
        std::cerr << "Failed to put data: " << s.ToString() << std::endl;
        return false;
    }
    return true;
}

// 批量写入或更新：用 WriteBatch 一次提交（原子性），快照恢复用
bool KVEngine::Set(const std::unordered_map<std::string, std::string>& kvData) {
    rocksdb::WriteBatch batch;
    for (auto kv : kvData) {
        batch.Put(kv.first, kv.second);
    }
    if (kvData.empty()) {
        return false;  // 空表没东西可写
    }
    rocksdb::Status s = db_->Write(rocksdb::WriteOptions(), &batch);
    if (!s.ok()) {
        std::cerr << "Failed to put data: " << s.ToString() << std::endl;
        return false;
    }
    return true;
}

// 读取：Get(key, &value)
bool KVEngine::Get(std::string& key, std::string& value) {
    rocksdb::ReadOptions options;
    rocksdb::Status s = db_->Get(options, key, &value);
    if (!s.ok()) {
        std::cerr << "Failed to get data: " << s.ToString() << std::endl;
        return false;
    }
    return true;
}

// 批量获取: 遍历整张表（Keys 接口/生成快照用）
bool KVEngine::Get(std::unordered_map<std::string, std::string>& kvData) {
    rocksdb::Iterator* it = db_->NewIterator(rocksdb::ReadOptions());
    // 遍历所有键值对
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        const rocksdb::Slice& key = it->key();
        const rocksdb::Slice& value = it->value();
        kvData[key.ToString()] = value.ToString();
    }
    // 检查迭代过程是否有错误
    if (!it->status().ok()) {
        std::cerr << "Iterator encountered an error: "
                  << it->status().ToString() << std::endl;
        delete it;
        return false;
    }
    delete it;
    return true;
}

// 删除：Delete(key)
bool KVEngine::Delete(const std::string& key) {
    rocksdb::Status s = db_->Delete(rocksdb::WriteOptions(), key);
    if (!s.ok()) {
        std::cerr << "Failed to delete data: " << s.ToString() << std::endl;
        return false;
    }
    return true;
}

// 批量删除：逐个删（有一个失败就停）
bool KVEngine::Delete(const std::vector<std::string>& keys) {
    bool res = true;
    for (auto key : keys) {
        res = Delete(key);
        if (!res) {
            return res;
        }
    }
    return res;
}

}  // namespace kv
