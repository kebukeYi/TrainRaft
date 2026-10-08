#pragma once
#include <common/random_device.h>
#include <raft/config.h>
#include <raft/progress.h>
#include <raft/raft_log.h>
#include <raft/readonly.h>
#include <raft/ready.h>
#include <raft/zk_client.h>
#include <resource/raft.pb.h>
#include <stdint.h>

#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>
using namespace proto;
namespace kv {

// ============================================================================
// Raft：Raft 算法状态机（整个项目的核心，仿 etcd 的 raft 实现）
//
// 大白话：这是一个"单线程"的有限状态机。它不碰磁盘、不发网络包，只做
// 两件事：
//   1. 被 tick 驱动（Tick 函数）：推动选举超时/心跳超时
//   2. 被消息驱动（Step 函数）：处理各种消息（投票、追加日志、心跳…）
// 处理完的结果（要写盘的日志、要发的消息、要应用的条目）打包成 Ready，
// 交给上层去执行。上层执行完调用 Advance 继续推进。
//
// 三个角色（state_）：Follower（追随者）/ Candidate（候选者）/ Leader（领导者）
// 三个重要指针（成员）：
//   - raftLog_   ：日志（committed/applied/unstable/storage 全在这）
//   - prs_       ：leader 视角下所有 follower 的复制进度表;
//   - readOnly_  ：线性一致读的登记表;
// ============================================================================
class Raft {
       public:
       
        uint64_t id_;  // 本节点 id（1,2,3...）
        uint64_t term_;  // 当前任期号（Raft 最重要的变量之一，用来判断消息新旧）
        uint64_t vote_;  // 本届任期投给了谁（0 = 还没投）
        std::vector<std::string> clusterInfo_;  // 集群节点地址列表
        ZkClient zkClient_;                     // zookeeper 客户端（leader 注册服务用）

        std::vector<ReadState> readStates_;     // 线性一致读的结果（等上层取走）

        // the log：日志管理（storage 稳定区 + unstable 不稳定区 + committed/applied）
        RaftLogPtr raftLog_;

        uint64_t maxMsgSize_;         // 单条追加消息的最大字节数
        uint64_t maxUncommittedSize_; // leader 未提交日志的总大小上限
        uint64_t maxInflight_;        // 单个 follower 的在途消息窗口大小

        std::unordered_map<uint64_t, ProgressPtr> prs_;  // 节点id -> 复制进度（leader 维护）管理器
        std::vector<uint64_t> matchBuf_;  // 计算提交位置时的临时缓冲区（避免重复分配）

        proto::RaftRole state_;  // 当前角色：Follower/Candidate/Leader/PreCandidate

        std::unordered_map<uint64_t, bool> votes_;  // 选举计票表：节点id -> 是否投赞成

        std::vector<std::shared_ptr<Message>> msgs_;  // 待发送的消息队列 (打包进 Ready)

        // 当前 leader 的 id（0 表示还不知道/没有）
        uint64_t lead_;

        // `leadTransferee_` 是领导权转移的目标节点 ID，当其值不为零时，
        // 按照 Raft 论文第 3.10 节定义的流程进行（把领导权主动让给另一个节点）
        uint64_t leadTransferee_;

        // 在日志中但尚未应用的配置变更（加节点/减节点），一次只能有一个待定。
        // 通过 pendingConfIndex_ 强制：它被设为 >= 最新待定配置变更的日志索引。
        // 只有 leader 的应用索引大于这个值时，才允许提出新的配置变更
        uint64_t pendingConfIndex_;

        // 未提交的 Raft 消息总大小（leader 日志尾部大小的估计值）。
        // 用于防止日志无限制增长；仅由 leader 维护，任期变化时重置;
        uint64_t uncommittedSize_;

        // 线性一致读（ReadIndex）的登记表;
        ReadOnlyPtr readOnly_;  
        

        // 选举计时器: 距离上次"重设点"过了多少 tick;
        // follower/candidate 用它判断是否该发起选举；leader 也用它配合 checkQuorum
        uint32_t electionElapsed_;

        // 心跳计时器：距离上次发心跳过了多少 tick（只有 leader 维护）
        uint32_t heartbeatElapsed_;


        bool checkQuorum_;  // 是否检查"多数派是否活跃"（配置项）
        bool preVote_;      // 是否启用预投票（配置项）

        uint32_t heartbeatTimeout_;  // 心跳间隔（tick 数）
        uint32_t electionTimeout_;   // 选举超时下限（tick 数）

        // `randomizedElectionTimeout_` 是随机化的选举超时：
        // 范围 [electionTimeout_, 2*electionTimeout_ - 1]。
        // 大白话：每个节点在 [T, 2T) 之间随机挑一个超时值，防止大家同时超时
        // 同时竞选导致票数分散。每当状态切为 follower/candidate 时重新随机
        uint32_t randomizedElectionTimeout_;

        bool disableProposalForwarding_;  // 是否禁止 follower 转发提案给 leader（配置项）

        // 状态机回调：tick_ 是当前角色下的"心跳/选举计时"函数;
        // step_ 是当前角色下的"消息处理"函数;
        // 切换角色时这两个函数会重新绑定（BecomeXxx 里做）
        std::function<void()> tick_;
        std::function<Status(std::shared_ptr<Message>)> step_; // 入参:消息; 返回值:状态;结果呢? 
        RandomDevice randomDevice_;  // 随机数生成器（随机化选举超时用）

       public:
        // 构造：根据 Config 初始化所有状态（连接 zk、创建日志、加载硬状态、初始化进度表）
        explicit Raft(const Config& c);

        virtual ~Raft();

        // 时钟推进：每 tick 一次（上层每 100ms 调用一次）。
        // 大白话：整个 Raft 的时间感都来自这里——follower 超时发起选举、
        // leader 定时发心跳，全靠 Tick 驱动
        void Tick();

        // ---- 角色切换（每次切换都会重绑 tick_/step_ 并重置相关状态） ----
        void BecomeFollower(uint64_t term, uint64_t lead);   // 变成追随者
        void BecomeCandidate();                              // 变成候选者（发起选举）
        void BecomePreCandidate();                           // 变成预候选者（预投票阶段）
        void BecomeLeader();                                 // 变成领导者


        // `campaign_type` 表示竞选的类型（普通选举/预选举/领导权转移）。
        // 用字符串而不是枚举，是为了方便直接塞进 Raft 日志条目里做标识
        void Campaign(const std::string& campaign_type);

        // 计票：记录节点 id 的投票结果（v 是否赞成），返回当前总赞成票数
        uint32_t Poll(uint64_t id, proto::MessageType type, bool v);


        // 消息处理的总入口：所有消息（本地/网络）都先进这里。
        // 先做"任期检查"（更高任期 => 让位/更新任期），再按角色分发到 step_
        virtual Status Step(std::shared_ptr<Message> msg);


        // 下面三个是"角色分发"处理函数（被 step_ 指向）
        Status StepLeader(std::shared_ptr<Message> msg);    // leader 的消息处理
        Status StepCandidate(std::shared_ptr<Message> msg); // candidate/pre-candidate 的消息处理
        Status StepFollower(std::shared_ptr<Message> msg);  // follower 的消息处理


        // ---- 三类核心 RPC 的处理（follower 侧） ----
        void HandleAppendEntries(std::shared_ptr<Message> msg);  // 处理追加日志
        void HandleHeartbeat(std::shared_ptr<Message> msg);      // 处理心跳
        void HandleSnapshot(std::shared_ptr<Message> msg);       // 处理快照



        // 用快照恢复本节点（比本地日志新才恢复），返回是否真的恢复了
        bool Restore(const proto::Snapshot& snapshot);

        // 把消息放进发送队列（打上自己的 id 和 term）
        void Send(std::shared_ptr<Message> msg);

        // 根据节点列表重建进度表（快照恢复/配置变更后调用）
        void RestoreNode(const std::vector<uint64_t>& nodes);

        // 本节点是否"够格"参与选举（自己的 id 在进度表里才算）
        bool Promotable() const;

        // 添加一个节点（配置变更：加节点）
        void AddNode(uint64_t id);

        // 多数派数量：节点数/2 + 1（3 节点集群 = 2，5 节点 = 3）
        uint32_t Quorum() const {
                return static_cast<uint32_t>(prs_.size() / 2 + 1);
        }

        // 软状态（lead + 角色，不持久化）
        std::shared_ptr<proto::SoftState> SoftState() const;

        // 硬状态（term + vote + commit，要持久化）
        proto::HardState HardState() const;

        // 从硬状态恢复（重启时调用）
        void LoadState(const proto::HardState& state);

        // 当前集群节点列表（排序后的 id）
        void Nodes(std::vector<uint64_t>& node) const;

        // 获取/设置/删除某个节点的进度
        ProgressPtr GetProgress(uint64_t id);
        void SetProgress(uint64_t id, uint64_t match, uint64_t next);
        void DelProgress(uint64_t id);


        // SendAppend：向指定 follower 发送追加日志 RPC(带新日志，可能为空);
        void SendAppend(uint64_t to);

        // MaybeSendAppend：仅在必要时发送追加 RPC，返回是否真发了;
        // sendIfEmpty 控制是否允许发"空消息" (空消息可用来更新 follower 的
        // commit 索引，但批量发送时通常不希望发空的)
        bool MaybeSendAppend(uint64_t to, bool SendIfEmpty);

        // 向指定 follower 发送心跳（ctx 是附带的只读请求标识）
        void SendHeartbeat(uint64_t to, std::vector<uint8_t> ctx);

        // 遍历所有节点的进度，逐个回调;
        void ForEachProgress(const std::function<void(uint64_t, ProgressPtr&)>& callback);

        // 向所有 follower 广播追加日志（有新日志/提交推进时调用）
        void BcastAppend();

        // 向所有 follower 广播心跳（普通心跳）
        void BcastHeartbeat();

        // 向所有 follower 广播带指定上下文的心跳（ReadIndex 确认用）
        void BcastHeartbeatWithCtx(const std::vector<uint8_t>& ctx);


        // 尝试推进提交索引：把所有 follower 的 match 排序，
        // 取第 Quorum 大的那个作为候选提交位置，提交成功返回 true
        bool MaybeCommit();

        // 重置状态到指定任期（选主、切换角色时调用，清掉进度/投票等）
        void Reset(uint64_t term);


        // 追加条目到本地日志（leader 提案的核心：编号、任期、记未提交大小）
        bool AppendEntry(const std::vector<Entry>& entries);

        // ---- 计时逻辑 ----
        // TickElection：follower/candidate 的选举计时（超时就发起选举）
        void TickElection();
        // TickHeartbeat：leader 的心跳计时（超时就广播心跳）
        void TickHeartbeat();

        // 选举超时是否已到（electionElapsed >= 随机化超时值）
        bool PastElectionTimeout();

        // 重新随机选举超时（每次切换角色时调用）
        void ResetRandomizedElectionTimeout();

        // 检查"多数派是否活跃"（checkQuorum 用：不活跃就下台）
        bool CheckQuorumActive();

        // 发送 MsgTimeoutNow 给目标节点（领导权转移：让它立刻发起选举）
        void SendTimeoutNow(uint64_t to);

        // 中止领导权转移（清 leadTransferee_）
        void AbortLeaderTransfer();

        // 计算新条目占用的未提交空间，超限则拒绝（返回 false）
        bool IncreaseUncommittedSize(const std::vector<std::shared_ptr<Entry>>& entries);

        // 条目提交后减少未提交空间计数
        void ReduceUncommittedSize(const std::vector<std::shared_ptr<Entry>>& entries);

        // 取走并清空待发送消息队列（打包进 Ready）
        virtual std::vector<std::shared_ptr<Message>> ReadMessages() {
                std::vector<std::shared_ptr<Message>> ret;
                ret.swap(msgs_);
                msgs_.clear();
                return ret;
        }
};
using RaftPtr = std::shared_ptr<Raft>;
}  // namespace kv
