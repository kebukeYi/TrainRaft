#include <common/log.h>
#include <inttypes.h>
#include <store/snap/snapshot_feature.h>

#include <boost/filesystem.hpp>
#include <msgpack.hpp>
using namespace boost;

namespace kv {

// 加载最新快照：收集所有快照文件，按名字降序（最新的在前），
// 逐个尝试加载，第一个加载成功的就返回
Status SnapshotFeature::Load(proto::Snapshot& snapshot) {
    std::vector<std::string> names;
    // 收集所有快照文件，按名字降序排序（最新在前）
    GetSnapNames(names);

    // 只加载最新的那个快照即可
    for (std::string& filename : names) {
        Status status = LoadSnap(filename, snapshot);
        if (status.IsOk()) {
            return Status::Ok();
        }
    }
    return Status::NotFound("snap not found");
}

// 生成快照文件名：<term>-<index>.snap（16 位十六进制）
std::string SnapshotFeature::SnapName(uint64_t term, uint64_t index) {
    char buffer[64];
    snprintf(buffer, sizeof(buffer), "%016" PRIx64 "-%016" PRIx64 ".snap", term,
             index);
    return buffer;
}

// 保存快照到磁盘：
// 文件格式：[dataLen(4B)][crc32(4B)][data...]
// 注意：不要把 SnapshotRecord 当"变长数组"去 new —— 它 sizeof 只有 8 字节，
// 数据必须分两次写（先写 8 字节头，再写数据本体），否则就是堆越界写。
Status SnapshotFeature::SaveSnap(const proto::Snapshot& snapshot) {
    std::string sbuf;
    snapshot.SerializeToString(&sbuf);

    // 组装 8 字节记录头：数据长度 + 校验值
    SnapshotRecord header;
    header.dataLen = static_cast<uint32_t>(sbuf.size());
    header.crc32 = ComputeCrc32(sbuf.data(), sbuf.size());

    // 目标路径
    char save_path[128];
    snprintf(save_path, sizeof(save_path), "%s/%s", dir_.c_str(),
             SnapName(snapshot.metadata().term(), snapshot.metadata().index())
                 .c_str());

    FILE* fp = fopen(save_path, "w");
    if (!fp) {
        return Status::IoError(strerror(errno));
    }

    // 先写头、再写数据本体（两次 fwrite，避免给柔性数组分配空间的坑）
    Status status;
    if (fwrite(&header, 1, sizeof(SnapshotRecord), fp) != sizeof(SnapshotRecord) ||
        fwrite(sbuf.data(), 1, sbuf.size(), fp) != sbuf.size()) {
        status = Status::IoError(strerror(errno));
    }
    fclose(fp);

    return status;
}

// 收集目录下所有 ".snap" 文件（降序排序：最新的排前面）
void SnapshotFeature::GetSnapNames(std::vector<std::string>& names) {
    filesystem::directory_iterator end;
    for (boost::filesystem::directory_iterator it(dir_); it != end; it++) {
        filesystem::path filename = (*it).path().filename();
        filesystem::path extension = filename.extension();
        if (extension != ".snap") {
            continue;
        }
        names.push_back(filename.string());
    }
    // 降序排序（term 高的/index 大的排前面 = 最新的快照）
    std::sort(names.begin(), names.end(), std::greater<std::string>());
}

// 加载单个快照文件（带防御性校验）
Status SnapshotFeature::LoadSnap(const std::string& filename,
                                 proto::Snapshot& snapshot) {
    SnapshotRecord snapRecord;
    std::vector<char> data;
    filesystem::path path = filesystem::path(dir_) / filename;
    FILE* fp = fopen(path.c_str(), "r");

    if (!fp) {
        goto invalid_snap;  // 文件打不开 = 坏快照
    }

    // 精确读取一个 SnapshotRecord 头的大小：
    // 用"读了多少字节"来判定成功，能精确感知文件提前结束/结构被截断
    if (fread(&snapRecord, 1, sizeof(SnapshotRecord), fp) != sizeof(SnapshotRecord)) {
        goto invalid_snap;
    }

    // 长度或校验值为 0：不合法
    if (snapRecord.dataLen == 0 || snapRecord.crc32 == 0) {
        goto invalid_snap;
    }

    // 读取数据本体
    data.resize(snapRecord.dataLen);
    // 尝试读取全量数据;
    if (fread(data.data(), 1, snapRecord.dataLen, fp) != snapRecord.dataLen) {
        goto invalid_snap;
    }

    fclose(fp);
    fp = NULL;
    // 校验 CRC：数据可能写坏了
    if (ComputeCrc32(data.data(), data.size()) != snapRecord.crc32) {
        goto invalid_snap;
    }

    try {
        LOG_INFO("snapRecord=%d", snapRecord.dataLen);
        // 反序列化成 Snapshot 消息
        snapshot.ParseFromArray((const char*)data.data(), data.size());
        return Status::Ok();
    } catch (std::exception& e) {
        goto invalid_snap;
    }

invalid_snap:
    // 坏快照：关闭文件，把文件改名成 ".broken"（防止下次再加载它）
    if (fp) {
        fclose(fp);
    }
    LOG_INFO("broken snapshot %s", path.string().c_str());
    filesystem::rename(path, path.string() + ".broken");
    return Status::IoError("unexpected empty snapshot");
}

}  // namespace kv
