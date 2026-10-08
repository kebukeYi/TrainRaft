#include <common/log.h>
#include <raft/readonly.h>

#include <chrono>
#include <thread>
namespace kv {

/*
LastPendingRequestCtx：获取"最后一个待确认读请求"的标识。
大白话：leader 每次发心跳时调用它，把最后一个待确认读请求的标识塞进心跳的
context 里。这样如果这个请求被多数派确认，它之前的所有请求也能一起确认
（因为心跳是顺序的，确认了后面的就说明前面的肯定也没问题）——这是为了
支持批量与流水线处理，减少网络往返。
*/
void ReadOnly::LastPendingRequestCtx(std::vector<uint8_t>& ctx) {
    if (readIndexQueue.empty()) {
        return;
    }
    ctx.insert(ctx.end(), readIndexQueue.back().begin(),
               readIndexQueue.back().end());
}

/*
RecvAck：登记一个 follower 对读请求的确认。
流程:
1. 消息的 context 就是请求标识，从 pendingReadIndex 里找对应的状态
2. 找不到（请求已被处理/从未登记）返回 0
3. 找到就把该 follower 的 id 记入 acks 集合
4. 返回"已确认节点数 + 1"（+1 是 leader 自己，它当然确认自己的 commit）
*/
uint32_t ReadOnly::RecvAck(const proto::Message& msg) {
    // 构建唯一索引键值;
    std::string str(msg.context().begin(), msg.context().end());
    // 查看是否有对应的请求状态;
    auto it = pendingReadIndex.find(str);
    // 没有找到;
    if (it == pendingReadIndex.end()) {
        return 0;
    }
    it->second->acks.insert(msg.from());
    // add one to include an ack from local node
    return it->second->acks.size() + 1;
}

// Advance: 确认达到多数派后, 解锁指定请求及之前的所有请求
/*
1. 遍历 readIndexQueue 队列，把每个请求都收进 rss，直到找到 context 对应的那个
2. 找到后，从队列和 pendingReadIndex 中删除这些已解锁的记录
3. 返回解锁出的请求列表（调用方据此回复客户端/发送 MsgReadIndexResp）
*/
std::vector<ReadIndexStatusPtr> ReadOnly::Advance(const proto::Message& msg) {
    // rss 是解锁出来的请求列表, 可以放行读取了;
    std::vector<ReadIndexStatusPtr> rss;

    std::string ctx(msg.context().begin(), msg.context().end());

    bool found = false;
    uint32_t i = 0;

    // FIFO
    for (std::string& okctx : readIndexQueue) {
        i++;
        auto it = pendingReadIndex.find(okctx);
        if (it == pendingReadIndex.end()) {
            // 队列里的标识在映射表里找不到：内部状态不一致，属于bug
            LOG_FATAL(
                "cannot find corresponding read state from pending "
                "map");
        }
        rss.push_back(it->second);
        if (okctx == ctx) {
            found = true;  // 找到目标请求，停止收集;
            break;
        }
    }

    if (found) {
        // 从队列里删掉前 i 个（已解锁的）
        readIndexQueue.erase(readIndexQueue.begin(),
                             readIndexQueue.begin() + i);
        // 从映射表里删掉对应的记录
        for (ReadIndexStatusPtr& rs : rss) {
            std::string str(rs->req.entries(0).data().begin(),
                            rs->req.entries(0).data().end());
            pendingReadIndex.erase(str);
        }
    }
    return rss;
}

// AddRequest：leader 登记一个新的读请求
void ReadOnly::AddRequest(uint64_t index,
                          std::shared_ptr<proto::Message>& msg) {
    // 请求标识存在 MsgReadIndex 第一条 entry 的 data 里
    std::string ctx = msg->entries(0).data();
    auto it = pendingReadIndex.find(ctx);
    if (it != pendingReadIndex.end()) {
        // 已经登记过了（重复请求），忽略
        return;
    }
    ReadIndexStatusPtr status(new ReadIndexStatus());
    status->index = index;  // 记下当前 commit 索引作为安全水位
    status->req = *msg;     // 保存请求本身（最后要拿它回包）
    pendingReadIndex[ctx] = status;
    readIndexQueue.push_back(ctx);  // 进队列，保持顺序
}

}  // namespace kv
