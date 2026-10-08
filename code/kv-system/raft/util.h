#pragma once
#include <resource/raft.pb.h>

namespace kv
{
    // ============================================================================
    // raft/util.h：Raft 算法层的小工具函数（声明）
    //
    // 大白话：Raft 状态机里有很多"判断消息新旧/是否为空/是否相等"的通用逻辑，
    // 抽到这里统一实现，避免 raft.cpp 里到处重复。所有函数都很直白，
    // 具体含义见 util.cpp 里的注释。
    // ============================================================================

    // 按 maxSize 限制条目的数量：如果所有条目序列化后总大小超过 maxSize，
    // 就把多余的条目从 entries 里裁掉（用于限制一次 append 消息的大小）
    void EntryLimitSize(uint64_t maxSize, std::vector<std::shared_ptr<proto::Entry>> &entries);

    // 投票消息类型 转 投票回复消息类型：MsgVote -> MsgVoteResp，MsgPreVote -> MsgPreVoteResp
    proto::MessageType VoteRespMsgType(proto::MessageType type);

    // 判断是不是"本地消息"（自己发给自己的，不需要走网络：
    // MsgHup 开始选举、MsgBeat 心跳、MsgUnreachable 不可达、MsgSnapStatus 快照状态、MsgCheckQuorum 检查多数派）
    bool IsLocalMsg(std::shared_ptr<proto::Message> msg);

    // 判断是不是"回复类消息"（MsgAppResp/MsgVoteResp/MsgHeartbeatResp/MsgUnreachable/MsgPreVoteResp）
    bool IsResponseMsg(std::shared_ptr<proto::Message> msg);

    // 判断"硬状态+日志条目数"是否需要强制同步刷盘（fsync）。
    // 只要出现了新任期/新投票/新日志条目，就必须同步写盘后才敢回 RPC，否则掉电就丢状态
    bool IsMustSync(const proto::HardState &st, const proto::HardState &prevst, size_t entsnum);

    // 把消息类型枚举转成字符串（打日志用）
    const char *MsgTypeToString(proto::MessageType type);

    // 把日志条目类型枚举转成字符串（打日志用）
    const char *EntryTypeToString(proto::EntryType type);

    // 硬状态是否"全零"（即空状态）：term=0 && vote=0 && commit=0
    bool IsEmptyHardState(proto::HardState hardState);

    // 快照是否为空：metadata.index == 0
    bool IsEmptySnapshot(proto::Snapshot snapshot);

    // 两个软状态是否相等（lead 相同 && 角色相同）
    bool IsEqualSoftState(const proto::SoftState &cur, const proto::SoftState &pre);

    // 两个硬状态是否相等（term/vote/commit 全部相同）
    bool IsEqualHardState(const proto::HardState &cur, const proto::HardState &pre);

    // 两个快照是否相等（data + confstate + index + term 全部相同）
    bool IsEqualSnapshot(const proto::Snapshot &cur, const proto::Snapshot &pre);

    // 两个日志条目是否相等（type/term/index/data 全部相同）
    bool IsEqualEntry(const proto::Entry &cur, const proto::Entry &pre);

}
