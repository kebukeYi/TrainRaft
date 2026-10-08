#include <store/wal/wal_file.h>

#include <unistd.h>  // fsync / fileno

namespace kv {

// 打开（或创建）WAL 文件：以追加模式打开（"a+"），并记录当前文件大小
WALfile::WALfile(const char* path, int64_t seq) : seq_(seq), fileSize_(0) {
    fp_ = fopen(path, "a+");
    if (!fp_) {
        LOG_FATAL("fopen error %s", strerror(errno));
    }

    // 记录当前文件大小（追加模式下指针在末尾，ftell 即大小）
    fileSize_ = ftell(fp_);
    if (fileSize_ == -1) {
        LOG_FATAL("ftell error %s", strerror(errno));
    }

    // 把指针拨回文件头，方便后面 ReadAll 从头读
    if (fseek(fp_, 0L, SEEK_SET) == -1) {
        LOG_FATAL("fseek error %s", strerror(errno));
    }
}

WALfile::~WALfile() {
    fclose(fp_);  // 关闭文件
}

// 截断：把文件砍到 offset 处。
// 大白话：读 WAL 时发现某条记录不完整或校验失败（上次断电写一半），
// 说明从这个位置开始的都是脏数据，全部丢掉。清空内存缓冲区
void WALfile::Truncate(size_t offset) {
    if (ftruncate(fileno(fp_), offset) != 0) {
        LOG_FATAL("fTruncate error %s", strerror(errno));
    }

    if (fseek(fp_, offset, SEEK_SET) == -1) {
        LOG_FATAL("fseek error %s", strerror(errno));
    }

    fileSize_ = offset;
    dataBuffer_.clear();
}

// 追加一条记录到内存缓冲区（不落盘，等 Sync 统一写）
void WALfile::Append(WALtype type, const std::string& data) {
    WALrecord record;
    record.type = type;
    // 计算数据的 crc32 校验值
    record.crc = ComputeCrc32((char*)data.data(), data.size());
    // 记录数据长度
    SetWalRecordLen(record, data.size());
    uint8_t* ptr = (uint8_t*)&record;
    // 先写入记录头（8 字节），再写入数据本体
    dataBuffer_.insert(dataBuffer_.end(), ptr, ptr + sizeof(record));
    dataBuffer_.insert(dataBuffer_.end(), data.begin(), data.end());
}

// 把缓冲区内容一次性写入文件并清空缓冲区（攒批写，减少磁盘 IO）
void WALfile::Sync() {
    if (dataBuffer_.empty()) {
        return;
    }

    size_t bytes = fwrite(dataBuffer_.data(), 1, dataBuffer_.size(), fp_);
    if (bytes != dataBuffer_.size()) {
        LOG_FATAL("fwrite error %s", strerror(errno));
    }

    // fwrite 只是写进 stdio 缓冲区，必须再 flush 到内核、fsync 到磁盘，
    // 否则进程被强杀/断电时这段 WAL 会丢（Raft 要求"日志落盘后才能回复 RPC"）
    if (fflush(fp_) != 0) {
        LOG_FATAL("fflush error %s", strerror(errno));
    }
    if (fsync(fileno(fp_)) != 0) {
        LOG_FATAL("fsync error %s", strerror(errno));
    }

    fileSize_ += dataBuffer_.size();
    // 写完后重置缓冲区
    dataBuffer_.clear();
}

// 把整个文件读进内存（供 ReadAll 逐条解析）
void WALfile::ReadAll(std::vector<char>& out) {
    char buffer[1024];
    while (true) {
        size_t bytes = fread(buffer, 1, sizeof(buffer), fp_);
        if (bytes <= 0) {
            break;
        }

        out.insert(out.end(), buffer, buffer + bytes);
    }

    // 读完后指针移到末尾（为后续追加做准备）
    fseek(fp_, 0L, SEEK_END);
}

}  // namespace kv
