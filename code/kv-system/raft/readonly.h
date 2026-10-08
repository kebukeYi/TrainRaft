#pragma once
#include <raft/config.h>
#include <resource/raft.pb.h>

#include <vector>

namespace kv {

// ============================================================================
// ReadState / ReadOnly：线性一致读（ReadIndex）的支持结构
//
// 大白话：普通读可能读到旧数据（比如刚发给 leader 的写还没提交）。
// "线性一致读"要求读到的一定是最新已提交的数据。Raft 的 ReadIndex 方案是：
//   1. leader 收到读请求，先记下当前 commit 索引（这个索引就是"安全水位"）
//   2. 广播带标识（requestCtx）的心跳，等大多数节点确认
//   3. 确认后，凡是在这个索引之前发出的写都已经生效了，此时再读就安全
// 本文件就是管理这些"待确认读请求"的容器。
// ============================================================================

// ReadState：一个只读查询的"结果"。
// 大白话：当读请求的安全水位确定后，就把 {index=安全水位, requestCtx=请求标识}
// 放进 Ready.readStates，上层拿到后：如果 appliedIndex 已经超过 index，
// 就可以安全执行读取并把结果返回给客户端
struct ReadState {
    // 两个 ReadState 是否相同（index 和 requestCtx 都一样）
    bool Equal(const ReadState& rs) const {
        if (index != rs.index) {
            return false;
        }
        return requestCtx == rs.requestCtx;
    }

    uint64_t index;                   // 安全水位：提交索引
    std::vector<uint8_t> requestCtx;  // 请求标识（哪个读请求）
};

// ReadIndexStatus：一个"待确认"的读请求
// req  = 当初收到的 MsgReadIndex 请求（里面带着 requestCtx）
// index = 记下的安全水位（commit 索引）
// acks = 已经确认过这个水位的节点集合（用来数是否到多数派）
struct ReadIndexStatus {
    proto::Message req;
    uint64_t index;
    std::unordered_set<uint64_t> acks;
};

using ReadIndexStatusPtr = std::shared_ptr<ReadIndexStatus>;

// ============================================================================
// ReadOnly：批量管理"待确认的读请求"的容器
//
// 大白话：读请求可能很多，leader 把它们按 requestCtx 记在两张表里：
//   - pendingReadIndex：requestCtx -> 读请求状态（用来数确认数）
//   - readIndexQueue：请求标识的先进先出队列（保证确认时按顺序批量处理）
// 每个心跳会携带"最后一条待确认请求"的标识，收到多数派确认后，
// 该请求以及它之前的所有请求就一起"解锁"（Advance）。
// ============================================================================
struct ReadOnly {
    // 构造：option 是只读模式（安全模式/租约模式）
    explicit ReadOnly(ReadOnlyOption option) : option(option) {}

    // 取"最后一个待确认请求"的标识（rctx）。
    // 大白话：leader 发心跳时，会把最后一个待确认读请求的标识带上，
    // 这样收到多数派确认后，可以一次解锁它之前的所有读请求（批量处理）
    void LastPendingRequestCtx(std::vector<uint8_t>& ctx);

    /*
    RecvAck：收到一个 follower 的心跳回复后，登记它的确认。
    1. 消息的 context 就是请求标识，用它找到对应的 ReadIndexStatus
    2. 找不到说明该请求已经被处理过了，返回 0
    3. 找到就把这个节点 id 记进 acks（说明它确认了"我的 commit 没变"）
    4. 返回已确认的节点数（+1 是把 leader 自己也算上）
    */
    uint32_t RecvAck(const proto::Message& msg);

    // Advance：确认数达到多数派后，把指定请求及它之前的所有请求一起"解锁"，
    // 返回解锁出来的请求列表，并清理两张表
    std::vector<ReadIndexStatusPtr> Advance(const proto::Message& msg);

    /*
    AddRequest：leader 收到读请求时登记它。
    index 是当前 commit 索引（安全水位），msg 是读请求
    1. 取请求标识（存于 MsgReadIndex 的第一条 entry 的 data 里）
    2. 如果这个标识已经在 pendingReadIndex 里（重复请求），直接忽略
    3. 否则登记：index 记下当前提交位置，msg 保存请求本身
    4. 标识追加进 readIndexQueue 队列（保持顺序）
    */
    void AddRequest(uint64_t index, std::shared_ptr<Message>& msg);

    ReadOnlyOption option;  // 当前 只读请求的处理模式（安全/租约）

    /*
    pendingReadIndex：请求标识 -> 读请求信息 的映射表。
    注意"请求标识"用的是 MsgReadIndex 消息第一条 Entry 的 data 内容，
    一般是个唯一 ID（本项目中就是客户端请求序号）
    */
    std::unordered_map<std::string, ReadIndexStatusPtr> pendingReadIndex;

    // readIndexQueue：请求标识的 FIFO 队列，保证读请求按顺序批量确认
    std::vector<std::string> readIndexQueue;
};

using ReadOnlyPtr = std::shared_ptr<ReadOnly>;
}  // namespace kv
