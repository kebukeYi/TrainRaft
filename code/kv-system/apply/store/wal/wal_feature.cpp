#include <common/log.h>
#include <fcntl.h>
#include <inttypes.h>
#include <raft/util.h>
#include <store/wal/wal_feature.h>

#include <boost/filesystem.hpp>
#include <msgpack.hpp>
#include <sstream>
using namespace boost;

namespace kv {

// 单个 WAL 文件的最大字节数（64MB,写满就开新文件)
static const int segmentSizeBytes = 64 * 1000 * 1000;  // 64MB

// 生成 WAL 文件名：16 位十六进制序号 + 16 位十六进制索引 + ".wal"
// 例：0000000000000001-0000000000000100.wal
static std::string WalName(uint64_t seq, uint64_t index) {
    char buffer[64];
    snprintf(buffer, sizeof(buffer), "%016" PRIx64 "-%016" PRIx64 ".wal", seq,
             index);
    return buffer;
}

// ----------------------------------------------------------------------------
// Create：在空目录里创建全新的 WAL
// 做法：先写一个临时文件（含一条"快照标记"记录：任期 0 索引 0），
// 然后重命名成正式文件。快照标记是恢复的"原点"——
// 读 WAL 时要求必须能找到一个匹配的快照标记，否则视为无效
// ----------------------------------------------------------------------------
void WALFeature::Create(const std::string& dir) {
    // 目标文件路径
    filesystem::path walFile = filesystem::path(dir) / WalName(0, 0);

    // 临时文件路径
    std::string tmpPath = walFile.string() + ".tmp";

    if (filesystem::exists(tmpPath)) {
        filesystem::remove(tmpPath);  // 清掉上次残留的临时文件
    }

    {
        std::shared_ptr<WALfile> tmp_wal(new WALfile(tmpPath.c_str(), 0));
        proto::WALsnapshot snap;
        // 从第 0 任期、第 0 索引开始，作为日志的「原点」
        snap.set_term(0);
        snap.set_index(0);
        std::string sbuf;
        snap.SerializeToString(&sbuf);
        // 写一条"快照标记"记录
        tmp_wal->Append(WALtype::walSnapshotType, sbuf);
        // 同步落盘
        tmp_wal->Sync();
    }
    // 临时文件重命名成正式 WAL 文件
    filesystem::rename(tmpPath, walFile);
}

// ----------------------------------------------------------------------------
// Open：打开 WAL，只保留"快照索引之后"的文件（恢复只需要这些）
// ----------------------------------------------------------------------------
WALptr WALFeature::Open(const std::string& dir, const proto::WALsnapshot& snap) {
    // 创建 WALFeature 对象, 目的是管理 wal;
    WALptr w(new WALFeature(dir));

    std::vector<std::string> names;
    // 列出所有 ".wal" 文件并排序
    w->GetWalNames(dir, names);
    if (names.empty()) {
        LOG_FATAL("wal not found");
    }

    uint64_t nameIndex;
    // 找到"索引 <= 快照索引"的最后一个文件的位置（从它开始恢复）
    if (!WALFeature::SearchIndex(names, snap.index(), &nameIndex)) {
        LOG_FATAL("wal not found");
    }

    // 只保留从该位置开始的文件（快照之后的）
    std::vector<std::string> checkNames(names.begin() + nameIndex, names.end());
    // 校验这些文件的序号是否连续（中间缺文件 = 数据丢了）
    if (!WALFeature::IsValidSeq(checkNames)) {
        LOG_FATAL("invalid wal seq");
    }

    // 逐个打开文件，加入文件列表
    for (const std::string& name : checkNames) {
        uint64_t seq;
        uint64_t index;
        // 检查 wal 文件名是否合规
        if (!ParseWalName(name, &seq, &index)) {
            LOG_FATAL("invalid wal name %s", name.c_str());
        }
        boost::filesystem::path path = boost::filesystem::path(w->dir_) / name;
        std::shared_ptr<WALfile> file(new WALfile(path.string().c_str(), seq));
        w->files_.push_back(file);
    }

    // 保存恢复起点（读取记录时用它过滤）
    w->start_ = snap;
    return w;
}

// ----------------------------------------------------------------------------
// ReadAll：把 WAL 里所有记录读出来，恢复出硬状态和日志条目
// 对每个文件：读进内存 → 逐条解析 → 校验（长度/CRC）→ 恢复
// 发现脏数据（断电写一半）就截断文件丢弃
// ----------------------------------------------------------------------------
Status WALFeature::ReadAll(proto::HardState& hs,std::vector<std::shared_ptr<proto::Entry>>& ents) {
    std::vector<char> data;
    for (auto file : files_) {
        data.clear();
        // 把文件内容全读进内存
        file->ReadAll(data);
        size_t offset = 0;
        bool matchsnap = false;  // 是否找到了匹配的快照标记

        while (offset < data.size()) {
            // 文件剩余量
            size_t left = data.size() - offset;
            // 上条正确记录的末尾地址, 确保在接下来的日志解析中出现错误,能正确截断;
            // 当前记录的首地址（截断时从这里砍）
            size_t record_begin_offset = offset;

            // 剩余不足一条记录头（8 字节）：文件尾巴是脏数据，截断
            if (left < sizeof(WALrecord)) {
                file->Truncate(record_begin_offset);
                LOG_WARN("invalid record len %lu", left);
                break;
            }

            WALrecord record;
            // 读出一条记录的头部
            memcpy(&record, data.data() + offset, sizeof(record));

            left = left - sizeof(record);
            offset = offset + sizeof(record);

            if (record.type == WALtype::walInvalidType) {
                // 无效类型：停止解析
                LOG_INFO("record.type是Invalid的");
                break;
            }

            // 记录头里声明的数据长度
            uint32_t record_dataLen = GetWalRecordLen(record);

            // 声明的长度 > 剩余字节：记录不完整（断电写一半），截断
            if (record_dataLen > left) {
                file->Truncate(record_begin_offset);
                LOG_WARN("invalid record data len %lu, %u", left,record_dataLen);
                break;
            }

            char* data_ptr = data.data() + offset;
            // 重算这段数据的 crc32 校验值
            uint32_t crc = ComputeCrc32(data_ptr, record_dataLen);

            left = left - record_dataLen;
            offset = offset + record_dataLen;

            // 校验值和记录头里存的不一致：数据写坏了，截断;
            if (record.crc != 0 && crc != record.crc) {
                file->Truncate(record_begin_offset);
                LOG_WARN("invalid record crc %u, %u", record.crc, crc);
                break;
            }

            // 按记录类型恢复到内存（条目/硬状态/快照标记）
            HandleRecordWalRecord(record.type, data_ptr, record_dataLen,matchsnap, hs, ents);

            // 有点问题啊? 这里怎么能保证 matchsnap 一定会被设置为 true 呢? 
            // 也就是说, wal 文件里一定会有一条快照标记, 否则就会一直报错;
            if (record.type == WALtype::walSnapshotType) {
                matchsnap = true;
            }

        }  // while 单文件循环

        if (!matchsnap) {
            // 这个文件里没有匹配的快照标记：数据不完整，直接报错
            LOG_FATAL("wal: snapshot not found");
        }
    }  // for files

    return Status::Ok();
}

// 解析一条 WAL 记录，恢复到内存
void WALFeature::HandleRecordWalRecord(WALtype type, const char* data, size_t dataLen, bool& matchsnap,
    proto::HardState& hs, std::vector<std::shared_ptr<proto::Entry>>& ents) {
    
    //
    switch (type) {
        // 普通日志条目
        case WALtype::walEntryType: {
            std::shared_ptr<proto::Entry> entry(new proto::Entry());
            entry->ParseFromArray(data, dataLen);
            // 只保留"索引 > 快照索引"的条目（之前的被快照覆盖了）
            if (entry->index() > start_.index()) {
                // 按索引定位插入位置：如果已有同位置的旧条目，
                // 先截掉再插入（后面新写的日志覆盖旧的）
                ents.resize(entry->index() - start_.index() - 1);
                ents.push_back(entry);
            }
            enti_ = entry->index();
            break;
        }

        // 硬状态（term/vote/commit）
        case WALtype::walStateType: {
            hs.ParseFromArray(data, dataLen);
            break;
        }

        // 快照标记
        case WALtype::walSnapshotType: {
            WALsnapshot snap;
            snap.ParseFromArray(data, dataLen);
            // 快照索引和恢复起点一致才算匹配上
            if (snap.index() == start_.index()) {
                if (snap.term() != start_.term()) {
                    // 索引一致但任期不一致：数据不匹配
                    LOG_FATAL("wal: snapshot mismatch");
                }
                matchsnap = true;
            }
            break;
        }

        case WALtype::walCrcType: {
            LOG_FATAL("wal crc type");
            break;
        }
        default: {
            LOG_FATAL("invalid record type %d", type);
        }
    }
}

// ----------------------------------------------------------------------------
// Save：保存一批条目 + 硬状态（每个 Ready 的核心写盘调用）
// 流程：逐条写条目 → 写硬状态 → 决定是否 fsync（必须同步的情况）→
//       文件写满则切新文件
// ----------------------------------------------------------------------------
Status WALFeature::Save(
    proto::HardState hs,
    const std::vector<std::shared_ptr<proto::Entry>>& ents) {

    // 快捷路径: 没有硬状态也没有条目, 啥都不用写
    if (IsEmptyHardState(hs) && ents.empty()) {
        return Status::Ok();
    }

    // 是否必须同步刷盘(有日志/任期变化/投票变化时必须同步)
    bool mustSync = IsMustSync(hs, state_, ents.size());
    Status status;

    // 逐条写日志条目
    for (const std::shared_ptr<proto::Entry>& entry : ents) {
        status = SaveEntry(*entry);
        if (!status.IsOk()) {
            return status;
        }
    }

    // 写硬状态
    status = SaveHardState(hs);
    if (!status.IsOk()) {
        return status;
    }

    // 文件还没写满: 按需刷盘后返回
    if (files_.back()->fileSize_ < segmentSizeBytes) {
        if (mustSync) {
            files_.back()->Sync();
        }
        return Status::Ok();
    }

    // 文件写满了: 切新文件;
    return Cut();
}

// 切新文件(本项目简化: 只把当前文件刷盘, 不真正开新文件)
Status WALFeature::Cut() {
    files_.back()->Sync();
    return Status::Ok();
}

// 保存快照标记: 记录"快照做到第几条了"（下次重启从这里恢复）
Status WALFeature::SaveSnapshot(const proto::WALsnapshot& snap) {
    std::string sbuf;
    snap.SerializeToString(&sbuf);
    LOG_INFO("Append");
    files_.back()->Append(WALtype::walSnapshotType, sbuf);
    if (enti_ < snap.index()) {
        enti_ = snap.index();
    }
    files_.back()->Sync();
    return Status::Ok();
}

// 保存一条日志条目
Status WALFeature::SaveEntry(const proto::Entry& entry) {
    std::string sbuf;
    entry.SerializeToString(&sbuf);

    files_.back()->Append(WALtype::walEntryType, sbuf);
    enti_ = entry.index();
    return Status::Ok();
}

// 保存硬状态（空状态不写）
Status WALFeature::SaveHardState(const proto::HardState& hs) {
    if (IsEmptyHardState(hs)) {
        return Status::Ok();
    }
    state_ = hs;  // 记录最近保存的硬状态（下次 IsMustSync 对比用）

    std::string sbuf;
    hs.SerializeToString(&sbuf);

    files_.back()->Append(WALtype::walStateType, sbuf);
    return Status::Ok();
}

// 列出目录下所有 ".wal" 文件（升序排序）
void WALFeature::GetWalNames(const std::string& dir,std::vector<std::string>& names) {
    filesystem::directory_iterator end;
    for (boost::filesystem::directory_iterator it(dir); it != end; it++) {
        filesystem::path filename = (*it).path().filename();
        filesystem::path extension = filename.extension();
        if (extension != ".wal") {
            continue;
        }
        names.push_back(filename.string());
    }
    std::sort(names.begin(), names.end(), std::less<std::string>());
}

// 释放旧 WAL 文件（本项目简化：空实现）
Status WALFeature::ReleaseTo(uint64_t index) { return Status::Ok(); }

// 解析 WAL 文件名：16 位十六进制序号 + 16 位十六进制索引
bool WALFeature::ParseWalName(const std::string& name, uint64_t* seq,uint64_t* index) {
    *seq = 0;
    *index = 0;

    boost::filesystem::path path(name);
    if (path.extension() != ".wal") {
        return false;
    }

    std::string filename = name.substr(0, name.size() - 4);  // 去掉 ".wal"
    size_t pos = filename.find('-');
    if (pos == std::string::npos) {
        return false;  // 没有 '-' 分隔符，格式不对
    }

    try {
        // 解析序号（'-' 前）
        {
            std::string str = filename.substr(0, pos);
            std::stringstream ss;
            ss << std::hex << str;  // 按十六进制读
            ss >> *seq;
        }

        // 解析索引（'-' 后）
        {
            if (pos == filename.size() - 1) {
                return false;
            }
            std::string str = filename.substr(pos + 1, filename.size() - pos - 1);
            std::stringstream ss;
            ss << std::hex << str;
            ss >> *index;
        }
    } catch (...) {
        return false;
    }
    return true;
}

// 检查文件序号是否连续递增（中间缺文件 = WAL 丢过数据）
bool WALFeature::IsValidSeq(const std::vector<std::string>& names) {
    uint64_t lastSeq = 0;
    for (const std::string& name : names) {
        uint64_t curSeq;
        uint64_t i;
        if (!WALFeature::ParseWalName(name, &curSeq, &i)) {
            LOG_FATAL("parse correct name should never fail %s", name.c_str());
        }
        // 从第二个文件开始判断序号是否连续（前一个 + 1）
        if (lastSeq != 0 && lastSeq != curSeq - 1) {
            return false;
        }

        lastSeq = curSeq;
    }
    return true;
}

// SearchIndex: 在已排序的文件名里，返回"索引 <= 给定索引"的最后一个文件下标;
// 大白话：恢复时, 快照已经覆盖了快照索引之前的日志,
// 所以只需要从"索引 <= 快照索引"的那个文件开始读（它里面可能有部分旧日志，但都会被过滤掉）
// 例：names 索引 100, 200, 300, 400, 500；snap_index = 250 → 返回下标 1 (200)
bool WALFeature::SearchIndex(const std::vector<std::string>& names,
                             uint64_t snap_index, uint64_t* nameIndex) {
    // 逆序遍历, 从后往前找;
    for (size_t i = names.size() - 1; i >= 0; --i) {
        const std::string& name = names[i];
        uint64_t seq;
        uint64_t curIndex;
        if (!ParseWalName(name, &seq, &curIndex)) {
            LOG_FATAL("invalid wal name %s", name.c_str());
        }
        // 找到"快照索引 >= 文件索引"的位置，从这里开始要
        if (snap_index >= curIndex) {
            *nameIndex = i;
            return true;
        }
        if (i == 0) {
            break;  // 防止无符号下溢
        }
    }
    *nameIndex = -1;
    return false;
}
}  // namespace kv
