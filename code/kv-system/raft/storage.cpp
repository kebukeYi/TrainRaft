#include <common/log.h>
#include <raft/storage.h>
#include <raft/util.h>
using namespace proto;

namespace kv {

// 初始状态：把内部保存的硬状态和快照里的集群配置返回给 raft。
// 重启时 raft 靠这两个东西知道"我之前干到哪了、集群里有谁";
Status MemoryStorage::InitialState(proto::HardState& hardState,
                                   proto::ConfState& confState) {
    hardState = hardState_;
    confState = snapshot_->metadata().confstate();
    return Status::Ok();
}

// 设置硬状态（RaftNodeImpl 从 WAL 恢复硬状态后调用）
void MemoryStorage::SetHardState(proto::HardState& hardState) {
    std::lock_guard<std::mutex> guard(mutex_);
    hardState_ = hardState;
}

// 取 [low, high) 区间的日志条目，最多 maxSize 字节
Status MemoryStorage::Entries(uint64_t low, uint64_t high, uint64_t maxSize,
                              std::vector<std::shared_ptr<Entry>>& entries) {
    assert(low < high);
    std::lock_guard<std::mutex> guard(mutex_);

    uint64_t offset = entries_[0]->index();  // 哑条目的 index = 起点

    // 要取的日志比数组起点还老：早就被压缩进快照了，取不到
    if (low <= offset) {
        LOG_DEBUG("requested index is unavailable due to compaction");
        return Status::InvalidArgument(
            "requested index is unavailable due to compaction");
    }
    uint64_t last = 0;
    this->LastIndexImpl(last);

    // 上界超出最后一条，越界
    if (high > last + 1) {
        LOG_FATAL("entries' hi(%lu) is out of bound LastIndex(%lu)", high,
                  last);
    }
    // 只有哑条目（数组里没有真实日志），取不到
    if (entries_.size() == 1) {
        LOG_INFO("requested entry at index is unavailable");
        return Status::InvalidArgument(
            "requested entry at index is unavailable");
    }

    // 从数组里切片: 日志索引 i 对应数组下标 i-offset
    for (uint64_t i = low - offset; i < high - offset; ++i) {
        entries.push_back(entries_[i]);
    }

    EntryLimitSize(maxSize, entries);  // 超过 maxSize 就裁掉后面的
    return Status::Ok();
}

// 查第 i 条日志的任期
Status MemoryStorage::Term(uint64_t i, uint64_t& term) {
    std::lock_guard<std::mutex> guard(mutex_);

    uint64_t offset = entries_[0]->index();

    // 太老，被压缩了
    if (i < offset) {
        return Status::InvalidArgument(
            "requested index is unavailable due to compaction");
    }

    // 太新，超出数组范围
    if (i - offset >= entries_.size()) {
        return Status::InvalidArgument(
            "requested entry at index is unavailable");
    }
    term = entries_[i - offset]->term();
    return Status::Ok();
}

Status MemoryStorage::LastIndex(uint64_t& index) {
    std::lock_guard<std::mutex> guard(mutex_);
    return LastIndexImpl(index);
}

Status MemoryStorage::FirstIndex(uint64_t& index) {
    std::lock_guard<std::mutex> guard(mutex_);
    return FirstIndexImpl(index);
}

Status MemoryStorage::Snapshot(std::shared_ptr<proto::Snapshot>& snapshot) {
    std::lock_guard<std::mutex> guard(mutex_);
    snapshot = snapshot_;
    return Status::Ok();
}

// 压缩：把 compact_index 之前的日志丢掉。
// 做法：让哑条目"吸收"旧日志——把第 compact_index 条日志的 index/term
// 抄到哑条目上，然后删掉它之前的所有真实条目
Status MemoryStorage::Compact(uint64_t compact_index) {
    std::lock_guard<std::mutex> guard(mutex_);

    uint64_t offset = entries_[0]->index();

    // 要压缩的位置比起点还老，没意义
    if (compact_index <= offset) {
        return Status::InvalidArgument(
            "requested index is unavailable due to compaction");
    }

    uint64_t last_idx;
    this->LastIndexImpl(last_idx);

    // 不能压缩还没写进来的日志
    if (compact_index > last_idx) {
        LOG_FATAL("compact %lu is out of bound LastIndex(%lu)", compact_index,
                  last_idx);
    }

    uint64_t i = compact_index - offset;
    // 把 compact_index 那条的 index/term 复制到哑条目上
    entries_[0]->set_index(entries_[i]->index());
    entries_[0]->set_term(entries_[i]->term());

    // 删除哑条目和 compact_index 之间的所有条目（下标 1 ~ i）
    entries_.erase(entries_.begin() + 1, entries_.begin() + i + 1);
    return Status::Ok();
}

// 追加日志条目。可能会遇到三种情况，分别处理:
Status MemoryStorage::Append(
    std::vector<std::shared_ptr<proto::Entry>> entries) {
    if (entries.empty()) {
        return Status::Ok();
    }

    std::lock_guard<std::mutex> guard(mutex_);

    uint64_t first = 0;
    FirstIndexImpl(first);  // 第一条可用日志的索引
    uint64_t last = entries[0]->index() + entries.size() - 1;

    // 要追加的日志整体都比现有日志老（快照都覆盖了），直接忽略
    if (first > last) {
        return Status::Ok();
    }

    // last >= first
    // 把追加的日志从 "已被快照覆盖" 的前的位置扔掉;
    if (first > entries[0]->index()) {
        uint64_t n = first - entries[0]->index();
        entries.erase(entries.begin(), entries.begin() + n);
    }

    // 计算新日志相对数组原点的偏移量
    uint64_t offset = entries[0]->index() - entries_[0]->index();

    /*

    假设存储 entries_ = [1,2,3,4,5]，要追加的不同情况：

    要追加的 entries  offset      走到哪个分支	        结果
    [3,4,5,6,7]	      2	          先裁剪成 [5,6,7],size(5)>offset(4)...
    [1,2,3,4,5,6,7] [6,7,8]	          5	           size==offset
    [1,2,3,4,5,6,7,8] 正常追加 [3,4](都老于first) first>last	丢弃 [8,9]
    7	          size(5)<offset(7)	                   LOG_FATAL 空洞错误
    */

    if (entries_.size() > offset) {
        // 存储 [5,6,7,8]（下标0..3），要追加 [7,8,9,10], offset = 7 - 5 = 2
        // 删掉下标 2..end（原来的 7,8），得到 [5,6] 追加新日志 → [5,6,7,8,9,10]
        // 情况1：新日志和现有日志重叠 —— 发生冲突了（如旧的 leader 复活）。
        // 保留 [0, offset) 的旧日志，从 offset 起用新日志覆盖（截断重写）
        entries_.erase(entries_.begin() + offset, entries_.end());
        entries_.insert(entries_.end(), entries.begin(), entries.end());
    } else if (entries_.size() == offset) {
        // 存储 [5,6,7]，要追加 [8,9,10]，offset = 8-5 = 3，size = 3，等于
        // offset → 直接追加成 [5,6,7,8,9,10]。这是正常追加日志。
        // 情况2：新日志恰好接在现有日志后面（正常追加）
        entries_.insert(entries_.end(), entries.begin(), entries.end());
    } else {
        // entries_.size() < offset
        // 情况3：新日志的起点比现有日志尾部还靠后 —— 中间缺了一段日志，
        // 说明数据有问题（丢了日志），直接报错
        uint64_t last_idx;
        LastIndexImpl(last_idx);
        LOG_FATAL("missing log entry [last: %lu, append at: %lu", last_idx,
                  entries[0]->index());
    }
    return Status::Ok();
}

// 生成快照: 记录 index 时刻的 KV 数据和集群配置;
Status MemoryStorage::CreateSnapshot(
    uint64_t index, std::shared_ptr<proto::ConfState> cs,
    std::vector<uint8_t> data, std::shared_ptr<proto::Snapshot>& snapshot) {
    std::lock_guard<std::mutex> guard(mutex_);

    // 不能生成比已有快照更老的快照
    if (index <= snapshot_->metadata().index()) {
        snapshot = std::make_shared<proto::Snapshot>();
        return Status::InvalidArgument(
            "requested index is older than the existing snapshot");
    }

    uint64_t offset = entries_[0]->index();
    uint64_t last = 0;
    LastIndexImpl(last);
    // 不能生成比现有日志还新的快照
    if (index > last) {
        LOG_FATAL("snapshot %lu is out of bound LastIndex(%lu)", index, last);
    }

    // 填快照的元信息：索引、任期、集群配置、KV 数据
    snapshot_->mutable_metadata()->set_index(index);
    snapshot_->mutable_metadata()->set_term(entries_[index - offset]->term());
    if (cs) {
        // 注意：必须"拷贝"，不能用 set_allocated_confstate(cs.get())——
        // 那会让 protobuf 接管这个裸指针，而它同时还被调用方的 shared_ptr 持有，
        // 同一个对象有了两个主人，迟早 double free（二次做快照 / 改配置时）
        snapshot_->mutable_metadata()->mutable_confstate()->CopyFrom(*cs);
    }
    snapshot_->set_data(data.data(), data.size());
    snapshot = snapshot_;
    return Status::Ok();
}

// 用快照覆盖存储：重置数组为"只含哑条目"，哑条目指向快照的 index/term。
// 大白话：快照代表"这个时刻之前的所有状态都在快照里了，日志可以全扔掉"，
// 后续日志从快照 index 之后继续接
Status MemoryStorage::ApplySnapshot(const proto::Snapshot& snapshot) {
    std::lock_guard<std::mutex> guard(mutex_);
    // 本地快照
    uint64_t index_ = snapshot_->metadata().index();
    // 传递过来的快照
    uint64_t snap_index = snapshot.metadata().index();

    // 新快照比旧快照还老，拒绝
    if (index_ >= snap_index) {
        return Status::InvalidArgument(
            "requested index is older than the existing snapshot");
    }

    // 覆盖快照对象
    snapshot_ = std::make_shared<proto::Snapshot>(snapshot);

    // 重置数组:只留一个哑条目
    entries_.resize(1);
    std::shared_ptr<proto::Entry> entry(new proto::Entry());
    entry->set_term(snapshot_->metadata().term());
    entry->set_index(snapshot_->metadata().index());
    // std::move：把 entry 的所有权转交给 entries_[0]，避免复制开销
    entries_[0] = std::move(entry);
    return Status::Ok();
}

// 最后一条日志索引 = 哑条目索引 + 数组长度 - 1
Status MemoryStorage::LastIndexImpl(uint64_t& index) {
    index = entries_[0]->index() + entries_.size() - 1;
    return Status::Ok();
}

// 第一条可用日志索引 = 哑条目索引 + 1
Status MemoryStorage::FirstIndexImpl(uint64_t& index) {
    index = entries_[0]->index() + 1;
    return Status::Ok();
}

}  // namespace kv
