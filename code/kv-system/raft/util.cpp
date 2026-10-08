#include <common/log.h>
#include <raft/util.h>
#include <resource/raft.pb.h>

#include <boost/crc.hpp>
namespace kv {

// 按总字节数限制条目个数:
// 从第 0 条开始累加序列化后的大小，一旦超过 maxSize，就把后面的条目全部裁掉
// 这样一次 MsgApp 消息最多带 maxSize 字节（由配置 maxSizePerMsg 控制，默认1MB）
void EntryLimitSize(uint64_t maxSize,std::vector<std::shared_ptr<proto::Entry>>& entries) {
    if (entries.empty()) {
        return;
    }

    std::string str;
    uint64_t size = entries[0]->SerializeToString(&str);
    for (size_t limit = 1; limit < entries.size(); ++limit) {
        std::string unitStr;
        entries[limit]->SerializeToString(&unitStr);
        size += unitStr.size();
        if (size > maxSize) {
            // 超了，只保留 [0, limit) 这些条目
            entries.resize(limit);
            break;
        }
    }
}

// 空硬状态 = term/vote/commit 全是 0（相当于"啥都没发生过"的状态）
bool IsEmptyHardState(proto::HardState hardState) {
    return hardState.term() == 0 && hardState.vote() == 0 && hardState.commit() == 0;
}

// 空快照 = 索引是 0（还没做过任何快照）
bool IsEmptySnapshot(proto::Snapshot snapshot) {
    return snapshot.metadata().index() == 0;
}

// 软状态相等：leader 相同且角色相同
bool IsEqualSoftState(const proto::SoftState& cur,const proto::SoftState& pre) {
    return cur.lead() == pre.lead() && cur.state() == pre.state();
}

// 硬状态相等：term/vote/commit 全相同。
// 用在"这次 Ready 的硬状态和上次是否一样"，一样就不用重复刷盘
bool IsEqualHardState(const proto::HardState& cur,
                      const proto::HardState& pre) {
    return cur.term() == pre.term() && cur.vote() == pre.vote() &&
           cur.commit() == pre.commit();
}

// 配置状态相等：节点列表长度相同且每个节点 id 都相同
static bool IsEqualConfState(const proto::ConfState& cur,
                             const proto::ConfState& pre) {
    if (cur.nodes().size() != pre.nodes().size()) {
        return false;
    }
    for (int i = 0; i < cur.nodes().size(); ++i) {
        if (cur.nodes(i) != pre.nodes(i)) {
            return false;
        }
    }

    return true;
}

// 快照相等：数据、节点列表、索引、任期全部相同
bool IsEqualSnapshot(const proto::Snapshot& cur, const proto::Snapshot& pre) {
    return cur.data() == pre.data() &&
           IsEqualConfState(cur.metadata().confstate(),
                            pre.metadata().confstate()) &&
           cur.metadata().index() == pre.metadata().index() &&
           cur.metadata().term() == pre.metadata().term();
}

// 日志条目相等：type/term/index/data 全部相同
bool IsEqualEntry(const proto::Entry& cur, const proto::Entry& pre) {
    return cur.type() == pre.type() && cur.term() == pre.term() &&
           cur.index() == pre.index() && cur.data() == pre.data();
}

// 消息类型枚举 -> 字符串（打日志方便看）
const char* MsgTypeToString(proto::MessageType type) {
    static std::map<proto::MessageType, const char*> MsgTypeToStringMap = {
        {proto::MessageType::MsgHup, "MsgHup"},
        {proto::MessageType::MsgBeat, "MsgBeat"},
        {proto::MessageType::MsgProp, "MsgProp"},
        {proto::MessageType::MsgApp, "MsgApp"},
        {proto::MessageType::MsgAppResp, "MsgAppResp"},
        {proto::MessageType::MsgVote, "MsgVote"},
        {proto::MessageType::MsgVoteResp, "MsgVoteResp"},
        {proto::MessageType::MsgSnap, "MsgSnap"},
        {proto::MessageType::MsgHeartbeat, "MsgHeartbeat"},
        {proto::MessageType::MsgHeartbeatResp, "MsgHeartbeatResp"},
        {proto::MessageType::MsgUnreachable, "MsgUnreachable"},
        {proto::MessageType::MsgSnapStatus, "MsgSnapStatus"},
        {proto::MessageType::MsgCheckQuorum, "MsgCheckQuorum"},
        {proto::MessageType::MsgTransferLeader, "MsgTransferLeader"},
        {proto::MessageType::MsgTimeoutNow, "MsgTimeoutNow"},
        {proto::MessageType::MsgReadIndex, "MsgReadIndex"},
        {proto::MessageType::MsgReadIndexResp, "MsgReadIndexResp"},
        {proto::MessageType::MsgPreVote, "MsgPreVote"},
        {proto::MessageType::MsgPreVoteResp, "MsgPreVoteResp"}};
    if (MsgTypeToStringMap.find(type) == MsgTypeToStringMap.end()) {
        LOG_FATAL("invalid msg type %d", type);
    }
    return MsgTypeToStringMap[type];
}

// 日志条目类型枚举 -> 字符串
const char* EntryTypeToString(proto::EntryType type) {
    static std::map<proto::EntryType, const char*> EntryTypeToStringMap = {
        {proto::EntryType::EntryNormal, "EntryNormal"},
        {proto::EntryType::EntryConfChange, "EntryConfChange"}};
    if (EntryTypeToStringMap.find(type) == EntryTypeToStringMap.end()) {
        LOG_FATAL("invalid msg type %d", type);
    }
    return EntryTypeToStringMap[type];
}

// 投票消息 ->
// 对应的回复消息（MsgVote->MsgVoteResp，MsgPreVote->MsgPreVoteResp）
proto::MessageType VoteRespMsgType(proto::MessageType type) {
    {
        static std::map<proto::MessageType, proto::MessageType>
            // <key , val>
            VoteRespMsgMap = {
                {proto::MessageType::MsgVote, proto::MessageType::MsgVoteResp},
                {proto::MessageType::MsgPreVote,
                 proto::MessageType::MsgPreVoteResp}};

        if (VoteRespMsgMap.find(type) == VoteRespMsgMap.end()) {
            LOG_FATAL("not a vote message: %s", MsgTypeToString(type));
        }
        return VoteRespMsgMap[type];
    }
}

// 是不是"本地消息"：这类消息由本节点内部产生、发给本节点，
// 永远不会出现在网络上。Node::Step 看到这类消息会直接拒绝（防呆）
bool IsLocalMsg(std::shared_ptr<proto::Message> msg) {
    return msg->type() == proto::MessageType::MsgHup ||
           msg->type() == proto::MessageType::MsgBeat ||
           msg->type() == proto::MessageType::MsgUnreachable ||
           msg->type() == proto::MessageType::MsgSnapStatus ||
           msg->type() == proto::MessageType::MsgCheckQuorum;
}

// 是不是"回复类消息"：这类消息是响应别人的请求产生的。
// Node::Step 里用它做校验：如果消息来自一个不在集群里的节点，且是回复消息，
// 就丢弃（防止旧集群成员的幽灵回复干扰）
bool IsResponseMsg(std::shared_ptr<proto::Message> msg) {
    return msg->type() == proto::MessageType::MsgAppResp ||
           msg->type() == proto::MessageType::MsgVoteResp ||
           msg->type() == proto::MessageType::MsgHeartbeatResp ||
           msg->type() == proto::MessageType::MsgUnreachable ||
           msg->type() == proto::MessageType::MsgPreVoteResp;
}

// mustSync：是否需要"同步刷盘"（fsync）才能安全返回。
// Raft 论文要求：term、votedFor、日志条目在回复 RPC 前必须落盘。
// 所以只要这次有新的日志条目、或者投票/任期变了，就必须强制 fsync;
// 只有"纯提交索引变化"这类情况才允许异步写（性能优化）
bool IsMustSync(const proto::HardState& st, const proto::HardState& prevst,size_t entsnum) {
    return entsnum != 0 || st.vote() != prevst.vote() ||
           st.term() != prevst.term();
}

}  // namespace kv
