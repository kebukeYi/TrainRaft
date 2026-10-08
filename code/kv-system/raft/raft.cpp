#include <common/log.h>
#include <common/slice.h>
#include <common/utils.h>
#include <raft/raft.h>
#include <raft/util.h>
#include <resource/raft.pb.h>

#include <algorithm>
#include <boost/algorithm/string.hpp>

using namespace proto;

namespace kv {

// 三种竞选类型的标识（塞进消息 context 里传给其他节点，判断投票时用）
static const std::string kCampaignPreElection = "CampaignPreElection";  // 预投票                                 
static const std::string kCampaignElection = "CampaignElection";  // 正式选举
static const std::string kCampaignTransfer = "CampaignTransfer";  // 领导权转移

// 统计一批日志条目里有多少条"配置变更"条目;
// 大白话：如果还有配置变更没应用完，就不允许 发起选举/再提配置变更
static uint32_t NumOfPendingConf(
    const std::vector<std::shared_ptr<proto::Entry>>& entries) {
    uint32_t n = 0;
    for (const std::shared_ptr<proto::Entry>& entry : entries) {
        if (entry->type() == proto::EntryConfChange) {
            n++;
        }
    }
    return n;
}

// ============================================================================
// Raft 构造函数：根据 Config 初始化一个全新的 Raft 状态机
// ============================================================================
Raft::Raft(const Config& conf)
    : id_(conf.id),
      term_(0),
      vote_(0),
      maxMsgSize_(conf.maxSizePerMsg),
      maxUncommittedSize_(conf.maxUncommittedEntriesSize),
      maxInflight_(conf.maxInflightMsgs),
      state_(proto::RaftRole::Follower),  // 初始是 follower 角色
      lead_(0),
      leadTransferee_(0),
      pendingConfIndex_(0),
      uncommittedSize_(0),
      readOnly_(new ReadOnly(conf.readOnlyOption)),
      electionElapsed_(0),
      heartbeatElapsed_(0),
      checkQuorum_(conf.checkQuorum),
      preVote_(conf.preVote),
      heartbeatTimeout_(conf.heartbeatTick),
      electionTimeout_(conf.electionTick),
      randomizedElectionTimeout_(0),
      disableProposalForwarding_(conf.disableProposalForwarding),
      randomDevice_(0, conf.electionTick),
      clusterInfo_(conf.clusterInfo) {
    // 连接 zookeeper（leader 当选后要向 zk 注册服务地址）
    zkClient_.Start();

    // 创建 RaftLog（内部会创建 unstable_，日志的"不稳定区"）
    raftLog_ = std::make_shared<RaftLog>(conf.storage, conf.maxCommittedSizePerReady);

    proto::HardState hs;  // 硬状态：term/vote/commit（重启恢复的关键）
    proto::ConfState cs;  // 配置信息：集群成员列表

    // 从存储里读上次保存的硬状态和集群配置（重启恢复的第一步）
    Status status = conf.storage->InitialState(hs, cs);

    if (!status.IsOk()) {
        LOG_FATAL("%s", status.ToString().c_str());
    }

    std::vector<uint64_t> peers = conf.peers;

    // 快照/存储 里存有集群配置(说明不是全新集群, 是重启恢复)
    if (cs.nodes_size() != 0) {
        if (!peers.empty()) {
            // 配置里既有 peers
            // 参数又有存储里的配置，二选一，不能同时给
            LOG_FATAL(
                "cannot specify both newRaft(peers) and "
                "ConfState.(Nodes)");
        }
        // 用存储里的配置作为集群成员
        for (auto& node : cs.nodes()) {
            peers.emplace_back(node);
        }
    }

    // 为每个集群成员创建一个"复制进度"(next 从 1 开始, 等日志加载后纠正)
    for (uint64_t peer : peers) {
        // 开始初始化 进度管理器;
        ProgressPtr p(new Progress(maxInflight_));
        p->next_ = 1;  // 下一个要发送的日志索引，先初始化成 1
        prs_[peer] = p;
    }

    // 存储里有硬状态(不是全新节点), 恢复 term/vote/commit
    if (!IsEmptyHardState(hs)) {
        // 加载状态;
        LoadState(hs);
    }

    // 上层告诉我们已经应用到哪里了(重启时设置,避免重复应用)
    if (conf.applied > 0) {
        raftLog_->AppliedTo(conf.applied);
    }

    // 先以 follower 身份启动(term_, lead=0 表示还不知道 leader 是谁)
    BecomeFollower(term_, 0);

    // 打印启动日志：成员列表、任期、提交/应用/日志位置;
    std::string nodeStr;
    {
        std::vector<std::string> nodeStrs;
        std::vector<uint64_t> node;
        this->Nodes(node);
        for (uint64_t n : node) {
            nodeStrs.push_back(std::to_string(n));
        }
        nodeStr = boost::join(nodeStrs, ",");
    }

    LOG_INFO(
        "raft %lu [peers: [%s], term: %lu, commit: %lu, applied: %lu, "
        "lastIndex: %lu, LastTerm: %lu]",
        id_, nodeStr.c_str(), term_, raftLog_->committed_, raftLog_->applied_,
        raftLog_->LastIndex(), raftLog_->LastTerm());
}

Raft::~Raft() {}

// ----------------------------------------------------------------------------
// 角色切换：BecomeFollower
// 大白话：变成"追随者"——听 leader 的。关键动作：重绑 step_（收到消息走
// StepFollower）、重绑 tick_（走选举计时）、重置 term 等状态、记住 leader 是谁
// ----------------------------------------------------------------------------
void Raft::BecomeFollower(uint64_t term, uint64_t lead) {
    // 角色对应的消息处理函数(后续 Step 会分发到这里)
    step_ = std::bind(&Raft::StepFollower, this, std::placeholders::_1);

    // 重置 term 和其他状态(进度表、投票、计时器等)
    Reset(term);

    // follower 的时钟走"选举计时", (超时没收到 leader 消息就发起选举)
    tick_ = std::bind(&Raft::TickElection, this);

    // 设置软状态;
    lead_ = lead;
    state_ = proto::RaftRole::Follower;

    LOG_INFO("%lu became follower at term %lu", id_, term_);
}

// 变成"预候选者"：预投票阶段（正式选举前先探探路，防止搅乱现任 leader）
void Raft::BecomePreCandidate() {
    if (state_ == proto::RaftRole::Leader) {
        LOG_FATAL("invalid transition [leader -> pre-candidate]");
    }
    step_ = std::bind(&Raft::StepCandidate, this, std::placeholders::_1);
    votes_.clear();  // 清掉上一轮的计票
    tick_ = std::bind(&Raft::TickElection, this);
    lead_ = 0;
    state_ = proto::RaftRole::PreCandidate;
    LOG_INFO("%lu became pre-candidate at term %lu", id_, term_);
}

// 变成"候选者"：发起正式选举
void Raft::BecomeCandidate() {
    if (state_ == proto::RaftRole::Leader) {
        LOG_FATAL("invalid transition [leader -> candidate]");
    }
    // 消息处理切到候选者逻辑
    step_ = std::bind(&Raft::StepCandidate, this, std::placeholders::_1);
    // 任期 +1（发起新选举必须进入新任期）
    Reset(term_ + 1);
    tick_ = std::bind(&Raft::TickElection, this);
    // 先投自己一票（Raft 规定候选人必须先投自己）
    vote_ = id_;
    state_ = proto::RaftRole::Candidate;
    LOG_INFO("%lu became candidate at term %lu", id_, term_);
}

// 变成"领导者"：选举获胜后的庆祝仪式+初始化, 上报 zk;
void Raft::BecomeLeader() {
    if (state_ == proto::RaftRole::Follower) {
        LOG_FATAL("invalid transition [follower -> leader]");
    }
    // 消息处理切到 leader 逻辑
    step_ = std::bind(&Raft::StepLeader, this, std::placeholders::_1);

    Reset(term_);
    // leader 的时钟走"心跳计时"（定时广播心跳维持领导地位）
    tick_ = std::bind(&Raft::TickHeartbeat, this);
    lead_ = id_;
    state_ = proto::RaftRole::Leader;
    // 自己的进度进入"复制状态"（leader 当然认为自己日志最新）
    auto it = prs_.find(id_);
    assert(it != prs_.end());
    // 开始下发复制
    it->second->BecomeReplicate();

    // 保守地把 pendingConfIndex 设为日志最后索引：
    // 可能存在或不存在 待处理的配置变更，但延迟后续提案直到
    // 已提交所有待处理日志是安全的
    pendingConfIndex_ = raftLog_->LastIndex();

    // Raft 规定：新 leader 当选后必须追加一条"空日志"（任期标记），
    // 这样当前任期的日志才能被提交（提交机制要求提交当前任期日志）
    auto empty_ent = std::make_shared<proto::Entry>();

    // 直接追加? 没看到 下发啊?
    if (!AppendEntry(std::vector<proto::Entry>{*empty_ent})) {
        // 由于我们刚调用了 Reset()，这种情况不会发生
        LOG_FATAL("empty entry was dropped");
    }

    // 特殊情况：初始空条目不计入未提交日志配额
    // （保留"当前用量为零时允许一个大于配额的条目"的行为）
    std::vector<std::shared_ptr<proto::Entry>> entries{empty_ent};
    // 重新计算 uncommittedSize_ 值;
    ReduceUncommittedSize(entries);
    LOG_INFO("%lu became leader at term %lu", id_, term_);

    // ---------------------------------------------------------------
    // 成为 leader 后进行 zk 服务注册：
    // 把集群里所有节点的"ip:客户端端口"拼成一个字符串，写进 zk 的 /raft
    // 节点。 客户端通过 zk 就能找到 leader（约定：clusterInfo[0] 是 leader
    // 的地址）
    // ---------------------------------------------------------------
    std::string ipList;
    if (clusterInfo_.size() > 1 && id_ - 1 != 0) {
        std::swap(clusterInfo_[id_ - 1], clusterInfo_[0]);
    }
    for (auto it = clusterInfo_.begin(); it != clusterInfo_.end(); ++it) {
        std::vector<std::string> ctx;
        boost::split(ctx, *it, boost::is_any_of(":"));
        ipList += (ctx[0] + ":" + ctx[2]);  // ip + 客户端端口
        if (it != std::prev(clusterInfo_.end())) {
            ipList += (" ");
        }
    }
    LOG_DEBUG("ipList=%s", ipList.c_str());
    zkClient_.CreateZkNode("/raft", ipList);
    std::string zkData = zkClient_.GetZkData("/raft");
    LOG_INFO("zkData: %s", zkData.c_str());
}

// ----------------------------------------------------------------------------
// Campaign：发起竞选（按类型区分：预投票/正式选举/领导权转移）
// ----------------------------------------------------------------------------
void Raft::Campaign(const std::string& campaignType) {
    uint64_t term = 0;
    MessageType voteMsg = MessageType::MsgHup;
    if (campaignType == kCampaignPreElection) {
        // 预投票：不改任期，用"未来任期
        // term+1"去问大家"如果我竞选，你们投吗"
        BecomePreCandidate();
        voteMsg = MessageType::MsgPreVote;
        term = term_ + 1;  // 预投票 RPC 在增加任期前为下一任期发送
    } else {
        // 正式选举（或领导权转移）: 进入新任期，发 MsgVote
        BecomeCandidate();
        voteMsg = MessageType::MsgVote;
        term = term_;
    }

    // 先给自己计一票（Poll 会把 id_ 的票记进去）
    // 如果自己一票就达到多数派：说明是单节点集群，直接当选
    if (Quorum() == Poll(id_, VoteRespMsgType(voteMsg), true)) {
        // 在为自己投票后赢得了选举（必须是单节点集群）
        if (campaignType == kCampaignPreElection) {
            // 预投票通过了，转入正式选举
            LOG_INFO("CAMPAIGN");
            Campaign(kCampaignElection);
        } else {
            // 正式选举通过，直接当 leader
            LOG_INFO("BecomeLeader");
            BecomeLeader();
        }
        return;
    }

    // 向集群里其他所有节点发送投票请求
    for (auto it = prs_.begin(); it != prs_.end(); ++it) {
        if (it->first == id_) {
            continue;  // 跳过自己
        }

        LOG_INFO(
            "%lu [logTerm: %lu, index: %lu] sent %s request to %lu at "
            "term %lu",
            id_, raftLog_->LastTerm(), raftLog_->LastIndex(),
            MsgTypeToString(voteMsg), it->first, term_);

        // 领导权转移的投票请求带特殊 context
        // 标识（接收方据此"强制投票"）
        std::vector<uint8_t> ctx;
        if (campaignType == kCampaignTransfer) {
            ctx = std::vector<uint8_t>(kCampaignTransfer.begin(),
                                       kCampaignTransfer.end());
        }

        std::shared_ptr<Message> msg(new Message());
        msg->set_term(term);
        msg->set_to(it->first);
        msg->set_type(voteMsg);
        // 带上自己日志的最后索引和任期：接收方用它判断"你的日志够不够新"
        // （Raft 选举原则：日志越新越有资格当选）
        msg->set_index(raftLog_->LastIndex());
        msg->set_logterm(raftLog_->LastTerm());
        msg->set_context(ctx.data(), ctx.size());
        Send(std::move(msg));
    }  // for over
}

// ----------------------------------------------------------------------------
// Poll：计票。记录节点 id 投的票（v 是否赞成），返回当前赞成票总数
// ----------------------------------------------------------------------------
uint32_t Raft::Poll(uint64_t id, proto::MessageType type, bool v) {
    uint32_t granted = 0;
    if (v) {
        LOG_INFO("%lu received %s from %lu at term %lu", id_,
                 MsgTypeToString(type), id, term_);
    } else {
        LOG_INFO("%lu received %s rejection from %lu at term %lu", id_,
                 MsgTypeToString(type), id, term_);
    }

    // 一张票只记一次（重复收到忽略）
    auto it = votes_.find(id);
    if (it == votes_.end()) {
        votes_[id] = v;
    }

    // 数一下赞成票总数
    for (it = votes_.begin(); it != votes_.end(); ++it) {
        if (it->second) {
            granted++;
        }
    }
    return granted;
}

// ============================================================================
// Step：所有消息的处理总入口（本节点内部消息、网络消息都走这里）
//
// 大白话：收到的每条消息先"对表"——比任期：
//   - 消息任期 > 我的任期：我过时了，让位（新 leader 出现/新选举开始）
//   - 消息任期 < 我的任期：消息是旧的，一般忽略（特殊场景会回个响应推任期）
//   - 任期相同：正常处理，按角色分发给
//   step_（StepLeader/StepCandidate/StepFollower）
// 另外 MsgHup（开始选举）和投票请求（MsgVote/MsgPreVote）是"通用"的，
// 任何角色都要响应，所以在总入口先处理
// ============================================================================
Status Raft::Step(std::shared_ptr<Message> msg) {
    // ---- 任期检查 ----
    if (msg->term() == 0) {
        // 本地消息(MsgHup/MsgBeat/等)或者 EntryNormal类型消息, 不带任期,
        // 跳过任期检查;
    } else if (msg->term() > term_) {
        // 消息来自更高的任期：说明集群里出现了新纪元（新选举或新leader）
        if (msg->type() == MessageType::MsgVote ||
            msg->type() == MessageType::MsgPreVote) {
            // 特殊场景: 领导权转移的投票请求，必须强制响应;
            bool force =
                (Slice((const char*)msg->context().data(),
                       msg->context().size()) == Slice(kCampaignTransfer));
            // 租约保护: 如果我在当前 leader 的"租约期"内
            // （checkQuorum 开启 && 有 leader && 还没到选举超时），
            // 就不理这个投票请求——因为现任 leader 明明还活着
            bool in_lease = (checkQuorum_ && lead_ != 0 &&
                             electionElapsed_ < electionTimeout_);
            // 不用强制回复 && 在租期内
            if (!force && in_lease) {
                // 在租约期内收到更高任期的投票请求，不更新任期、不投票
                LOG_INFO(
                    "%lu [logTerm: %lu, index: %lu, vote: %lu] "
                    "ignored %s from %lu [logTerm: %lu, index: "
                    "%lu] at term %lu: lease is not expired "
                    "(remaining ticks: %d)",
                    id_, raftLog_->LastTerm(), raftLog_->LastIndex(), vote_,
                    MsgTypeToString(msg->type()), msg->from(), msg->logterm(),
                    msg->index(), term_, electionTimeout_ - electionElapsed_);
                return Status::Ok();
            }
        }

        // 收到高任期的消息
        // 需要强制 || 不在租期内
        switch (msg->type()) {
            case MessageType::MsgPreVote:
                // 预投票消息不会改变我们的任期（预投票的特殊规则）
                break;
            case MessageType::MsgPreVoteResp:
                // 自己发出的 预投票没有被拒绝;
                if (!msg->reject()) {
                    // 预投票通过了：我们会发送带未来任期的预投票请求，
                    // 获得法定人数后才会增加任期。
                    // 如果没通过，任期将从拒绝我们的节点处获取，
                    // 所以应该在新任期下成为追随者
                    break;
                }
                // 预投票被拒：fall through 到
                // default，更新任期变 follower
            default:
                LOG_INFO(
                    "%lu [term: %lu] received a %s message "
                    "with higher term from %lu [term: %lu]",
                    id_, term_, MsgTypeToString(msg->type()), msg->from(),
                    msg->term());

                // 跟随高任期消息，降级为 follower;
                if (msg->type() == MessageType::MsgApp ||
                    msg->type() == MessageType::MsgHeartbeat ||
                    msg->type() == MessageType::MsgSnap) {
                    // 收到新 leader 的日志/心跳/快照：
                    // 新 leader 就是发消息的节点
                    LOG_INFO("BecomeFollower");
                    // 那么消息在哪里被读取消费;
                    BecomeFollower(msg->term(), msg->from());
                } else {
                    // 其他类型（如投票回复）：不知道 leader
                    // 是谁
                    LOG_INFO("BecomeFollower");
                    BecomeFollower(msg->term(), 0);
                }
        }
    } else if (msg->term() < term_) {
        // 消息来自更低的任期：通常是网络延迟的旧消息
        if ((checkQuorum_ || preVote_) &&
            (msg->type() == MessageType::MsgHeartbeat ||
             msg->type() == MessageType::MsgApp)) {
            // 特殊处理：收到"低任期 leader"的心跳/追加日志。
            // 大白话：这个低任期 leader 可能已被网络分区隔离，
            // 它不知道我已经升到更高任期了。给它回一个 MsgAppResp，
            // 它看到我的任期（响应里会带）就会自己让位。
            // 这是防止"孤立节点"无限捣乱的关键机制
            std::shared_ptr<Message> m(new Message());
            m->set_to(msg->from());
            m->set_type(MessageType::MsgAppResp);
            m->set_term(term_);  // 源码没加;
            Send(std::move(m));

        } else if (msg->type() == MessageType::MsgPreVote) {
            // 低任期的预投票：明确拒绝，并告诉对方我的任期，
            // 让它知道"集群里已经有更高任期了"
            LOG_INFO(
                "%lu [logTerm: %lu, index: %lu, vote: %lu] "
                "rejected %s from %lu [logTerm: %lu, index: %lu] "
                "at term %lu",
                id_, raftLog_->LastTerm(), raftLog_->LastIndex(), vote_,
                MsgTypeToString(msg->type()), msg->from(), msg->logterm(),
                msg->index(), term_);

            std::shared_ptr<Message> m(new Message());
            m->set_to(msg->from());
            m->set_type(MessageType::MsgPreVoteResp);
            m->set_reject(true);
            m->set_term(term_);
            Send(std::move(m));
        } else {
            // 其他低任期消息：忽略
            LOG_INFO(
                "%lu [term: %lu] ignored a %s message with lower "
                "term from %lu [term: %lu]",
                id_, term_, MsgTypeToString(msg->type()), msg->from(),
                msg->term());
        }
        return Status::Ok();
    }

    // 任期对, 角色对, 那就是正常消息:
    // ---- 按消息类型处理 ----
    switch (msg->type()) {
        // 是 Raft
        // 里的"开始选举"消息——而且它是本节点发给自己的本地消息，永远不出现在网络上;
        // 以前是超时到了就直接发起选举; 这里是到了超时时间后,
        // 再去判断一些是否还有配置变更的消息;
        // 否则不能进行选举,要先执行配置变更? 非leader 也能执行配置变更;
        case MessageType::MsgHup: {
            // "开始选举"（本地消息：选举超时后 tick 触发）
            if (state_ != proto::RaftRole::Leader) {
                // 如果还有"待应用的配置变更"没处理完，不能选举
                // （配置变更没应用完就换 leader 会乱）

                // 去找未应用的消息量;
                std::vector<std::shared_ptr<Entry>> entries;
                Status status = raftLog_->Slice(raftLog_->applied_ + 1,
                                                raftLog_->committed_ + 1,
                                                RaftLog::Unlimited(), entries);

                // 切片未成功;
                if (!status.IsOk()) {
                    LOG_FATAL(
                        "unexpected error getting "
                        "unapplied entries (%s)",
                        status.ToString().c_str());
                }

                // 去计算 已提交 但是未应用的消息[]中,
                // 还有配置变更的消息的数量;
                uint32_t pending = NumOfPendingConf(entries);
                // 含有没有应用的配置日志消息, 怎么办?
                if (pending > 0 && raftLog_->committed_ > raftLog_->applied_) {
                    LOG_WARN(
                        "%lu cannot campaign at term %lu "
                        "since there are still %u pending "
                        "configuration changes to apply",
                        id_, term_, pending);
                    // 不进入选举过程中;
                    return Status::Ok();
                }

                LOG_INFO(
                    "%lu is starting a new election at term "
                    "%lu",
                    id_, term_);

                // 按配置决定走预投票还是直接选举
                if (preVote_) {
                    Campaign(kCampaignPreElection);
                } else {
                    Campaign(kCampaignElection);
                }
            } else {
                // 已经是 leader 了，忽略"开始选举"
                LOG_DEBUG(
                    "%lu ignoring MsgHup because already "
                    "leader",
                    id_);
            }
            break;
        }

        // 拉票请求
        case MessageType::MsgVote:
        case MessageType::MsgPreVote: {
            // 收到投票请求：决定投还是不投
            // 可以投票的条件（满足其一）：
            //   1. 已经投过这个节点（重复请求）
            //   2. 还没投过票 && 当前没有已知 leader（vote_==0 &&
            //   lead_==0）
            //   3. 预投票且任期更高（预投票的特殊规则）
            bool can_vote =
                vote_ == msg->from() || (vote_ == 0 && lead_ == 0) ||
                (msg->type() == MessageType::MsgPreVote && msg->term() > term_);

            // 候选人日志是否足够新（Raft 选举限制：
            // 日志更旧的人当选会导致已提交日志丢失）
            if (can_vote &&
                this->raftLog_->IsUpToDate(msg->index(), msg->logterm())) {
                LOG_INFO(
                    "%lu [logTerm: %lu, index: %lu, vote: %lu] "
                    "cast %s for %lu [logTerm: %lu, index: "
                    "%lu] at term %lu",
                    id_, raftLog_->LastTerm(), raftLog_->LastIndex(), vote_,
                    MsgTypeToString(msg->type()), msg->from(), msg->logterm(),
                    msg->index(), term_);

                // 回复投票。注意回包用的任期是"消息里的任期"
                // 而不是本地任期：
                // 预投票时本地任期还没更新，如果带本地任期，
                // 候选人会认为消息过时而忽略;ok

                std::shared_ptr<Message> m(new Message());
                m->set_to(msg->from());
                m->set_term(msg->term());
                m->set_type(VoteRespMsgType(msg->type()));
                //
                Send(std::move(m));

                // 预先拉票的 不投;
                // 真实投票才记录：投给了谁、重置选举计时;
                if (msg->type() == MessageType::MsgVote) {
                    electionElapsed_ = 0;
                    vote_ = msg->from();
                }
            } else {
                // 拒绝投票：候选人日志不够新 / 已经投过别人
                LOG_INFO(
                    "%lu [logTerm: %lu, index: %lu, vote: %lu] "
                    "rejected %s from %lu [logTerm: %lu, "
                    "index: %lu] at term %lu",
                    id_, raftLog_->LastTerm(), raftLog_->LastIndex(), vote_,
                    MsgTypeToString(msg->type()), msg->from(), msg->logterm(),
                    msg->index(), term_);

                std::shared_ptr<Message> m(new Message());
                m->set_to(msg->from());
                m->set_term(term_);  // 本地 term
                m->set_type(VoteRespMsgType(msg->type()));
                m->set_reject(true);
                Send(std::move(m));
            }
            break;
        }
        default: {
            // 其他所有消息: 交给当前角色对应的处理函数;
            return step_(msg);
        }
    }
    return Status::Ok();
}

// ============================================================================
// StepLeader：leader 角色的消息处理器
// ============================================================================
Status Raft::StepLeader(std::shared_ptr<proto::Message> msg) {
    // 无需进度跟踪的消息类型（本地控制类消息）
    switch (msg->type()) {
        // 自己给自己发心跳提醒;
        case MessageType::MsgBeat: {
            // 心跳计时到了：广播心跳
            BcastHeartbeat();
            return Status::Ok();
        }

        // 检查多数派是否还活着（checkQuorum 定时器触发）
        case MessageType::MsgCheckQuorum:
            if (!CheckQuorumActive()) {
                // 多数派都失联了：主动下台，让集群重新选举;
                LOG_WARN(
                    "%lu stepped down to follower since quorum "
                    "is not active",
                    id_);
                // 不用再广播消息吗? 下台后的主要变化就是
                // 不再发送心跳了;
                BecomeFollower(term_, 0);
            }
            return Status::Ok();

        // 上层应用 → 发给本节点 raft	目的: "我想写这条日志"（请求）
        // 未编号的 entry（term/index 为 0）
        case MessageType::MsgProp:  // 提案消息：包含需要复制到日志中的提案数据
        {
            if (msg->entries_size() == 0) {
                LOG_FATAL("%lu stepped empty MsgProp", id_);
            }

            auto it = prs_.find(id_);
            if (it == prs_.end()) {
                // 什么情况下, 会被移除?
                // 自己都不在集群成员表里（被移除了）：拒绝提案
                return Status::InvalidArgument("raft proposal dropped");
            }

            // 正在领导者转移;
            if (leadTransferee_ != 0) {
                // 正在转移领导权期间，拒绝新提案（保证转移干净利落）
                LOG_DEBUG(
                    "%lu [term %lu] transfer leadership to %lu "
                    "is in progress; dropping proposal",
                    id_, term_, leadTransferee_);
                return Status::InvalidArgument("raft proposal dropped");
            }

            // for{} 找出 并 处理配置变更条目:
            // 一次只能有一个配置变更待定，如果已经有没应用完的,
            // 就把新提出的配置变更"降级"成普通条目（丢弃变更意图）;
            for (size_t i = 0; i < msg->entries_size(); ++i) {
                proto::Entry ent = msg->entries(i);
                // 如果是配置变更类型;
                if (ent.type() == EntryType::EntryConfChange) {
                    // 之前的配置变更还没处理完,
                    // 当前的配置变更消息可以抛弃掉;
                    if (pendingConfIndex_ > raftLog_->applied_) {
                        LOG_INFO(
                            "propose conf %s ignored "
                            "since pending unapplied "
                            "configuration [index %lu, "
                            "applied %lu]",
                            EntryTypeToString(ent.type()), pendingConfIndex_,
                            raftLog_->applied_);

                        // 清空这条配置变更（变成空普通条目）
                        ent.set_type(EntryType::EntryNormal);
                        ent.set_index(0);
                        ent.set_term(0);
                        ent.clear_data();
                    } else {
                        // 记录待定配置变更的位置;
                        pendingConfIndex_ = raftLog_->LastIndex() + i + 1;
                    }
                }
            }

            // 把条目追加到本地日志（leader 的第一份副本）
            std::vector<proto::Entry> entries;
            for (auto& entry : msg->entries()) {
                entries.emplace_back(entry);
            }

            // 追加到 raft unstable 中;
            if (!AppendEntry(entries)) {
                // 会尝试提交, 但是还没下发,会提交失败;
                return Status::InvalidArgument("raft proposal dropped");
            }

            LOG_INFO("broadcast append entries");

            // 广播给所有 follower 复制;
            BcastAppend();
            return Status::Ok();
        }

        // 用户的读请求类型: MsgReadIndex;消息类型: 线性一致读;
        case MessageType::MsgReadIndex: {
            if (Quorum() > 1) {
                // 取当前提交索引的任期
                uint64_t term = 0;
                raftLog_->Term(raftLog_->committed_, term);
                // 如果当前任期还没提交过任何日志（刚当选），
                // 拒绝读请求——因为可能还有旧 leader
                // 的日志没提交完;
                if (term != term_) {
                    return Status::Ok();
                }

                // 当前 只读请求的处理模式（安全/租约）
                switch (readOnly_->option) {
                    // 模式1
                    case ReadOnlySafe:
                        // 记下当前 commit
                        // 索引当"安全水位"，广播带身份证的心跳等确认
                        // 安全模式（ReadIndex）：
                        // 1. 把当前 commit
                        // 索引登记为读请求的"安全水位"
                        // 2. 广播带请求标识的心跳
                        // 3. 等大多数节点确认（commit
                        // 没变）
                        // 4. 确认后即可安全读取
                        // 先收下这个 只读请求,
                        // 然后再等待广播响应,然后再返回,
                        readOnly_->AddRequest(raftLog_->committed_, msg);
                        // 广播心跳;
                        BcastHeartbeatWithCtx(
                            std::vector<uint8_t>(msg->entries(0).data().begin(),
                                                 msg->entries(0).data().end()));
                        break;

                    // 模式2
                    case ReadOnlyLeaseBased:
                        // 租约模式：靠"租约没过期"直接读，
                        // 少一轮确认（有时钟漂移风险）
                        if (msg->from() == 0 || msg->from() == id_) {
                            // 本地发起的读:
                            // 直接把当前 commit
                            // 作为读结果
                            readStates_.push_back(
                                ReadState{.index = raftLog_->committed_,
                                          .requestCtx = std::vector<uint8_t>(
                                              msg->entries(0).data().begin(),
                                              msg->entries(0).data().end())});
                        } else {
                            // 远程节点转发的读;
                            // 直接把 commit
                            // 索引回给它
                            std::shared_ptr<proto::Message> m(new Message());
                            m->set_to(msg->from());
                            m->set_type(proto::MessageType::MsgReadIndexResp);
                            m->set_index(raftLog_->committed_);
                            m->mutable_entries()->CopyFrom(msg->entries());
                            Send(std::move(m));
                        }
                        break;
                }
            } else {
                // 单节点集群：没有确认的必要，直接返回当前 commit;
                readStates_.push_back(ReadState{
                    .index = raftLog_->committed_,
                    .requestCtx =
                        std::vector<uint8_t>(msg->entries(0).data().begin(),
                                             msg->entries(0).data().end())});
            }
            return Status::Ok();
        }
    }

    // 其他消息类型需要进度跟踪: 先找到发消息节点的进度管理器;
    auto pr = GetProgress(msg->from());
    if (pr == nullptr) {
        // 不在成员表里的节点（可能是被移除的旧成员）;忽略;
        LOG_DEBUG("%lu no progress available for %lu", id_, msg->from());
        return Status::Ok();
    }

    // 进度跟踪的消息类型;
    switch (msg->type()) {
        // 消息下发后的, follower的响应;
        case MessageType::MsgAppResp: {
            // follower 回复了"追加日志"的结果
            pr->recentActive_ = true;  // 它活着

            // follower 拒绝了: 说明它日志和我这里对不上;
            if (msg->reject()) {
                // follower 拒绝了: 说明它日志和我这里对不上;
                LOG_DEBUG(
                    "%lu received msgApp rejection(lastIndex: "
                    "%lu) from %lu for index %lu",
                    id_, msg->rejecthint(), msg->from(), msg->index());

                // 回退进度（next 往回退），然后重发;
                // rejecthint 是 follower
                // 提示的"我日志最后到哪了" 调整 next_;
                if (pr->MaybeDecreasesTo(msg->index(), msg->rejecthint())) {
                    LOG_DEBUG(
                        "%lu decreased progress of %lu to "
                        "[%s]",
                        id_, msg->from(), pr->String().c_str());

                    // 判断和对端节点的状态;
                    if (pr->state_ == ProgressStateReplicate) {
                        // 复制状态被打断，回到探测状态;
                        pr->BecomeProbe();
                    }
                    // 发送消息, 具体什么消息, next_index的消息;
                    // 不用判断任期因素 回退吗?
                    SendAppend(msg->from());  // 回退后重发
                }
            } else {
                // follower 接受: 更新它的进度;
                bool old_paused = pr->IsPaused();
                // 是否能更新一些变量?
                if (pr->MaybeUpdate(msg->index())) {
                    // 如果之前是探测状态;
                    if (pr->state_ == ProgressStateProbe) {
                        // 探测成功，进入快速复制状态
                        pr->BecomeReplicate();
                    } else if (pr->state_ == ProgressStateSnapshot &&
                               pr->NeedSnapshotAbort()) {
                        // 快照发送期间它竟然跟上了, 中止快照，切回正常复制
                        LOG_DEBUG(
                            "%lu snapshot aborted, "
                            "resumed sending "
                            "replication messages to "
                            "%lu [%s]",
                            id_, msg->from(), pr->String().c_str());
                        // 进入探测状态
                        pr->BecomeProbe();
                    } else if (pr->state_ == ProgressStateReplicate) {
                        // 正常复制状态: 释放滑动窗口里已确认的在途消息
                        pr->inflights_->FreeTo(msg->index());
                    }

                    // 尝试推进提交索引:
                    // 如果多数派 match(仅仅是多个节点的index进度,并没commit)
                    // 都过了某条日志， 它就被"提交"了
                    if (MaybeCommit()) {
                        // 把推进后的结果, 广播给所有人;
                        BcastAppend();  // 提交推进, 广播给所有人;
                    } else if (old_paused) {
                        // 之前暂停的节点可能不知道最新提交，
                        // 补发一条
                        SendAppend(msg->from());
                    }

                    // 为什么这里要再发日志???
                    // 如果还有更多日志要发，继续发
                    // （probe -> replicate
                    // 切换或窗口释放后，
                    // 可能可以连发多条）
                    while (MaybeSendAppend(msg->from(), false)) {
                    }

                    // 领导权转移:
                    // 如果目标节点已经追平日志， 发
                    // MsgTimeoutNow 让它立刻发起选举接班
                    if (msg->from() == leadTransferee_ &&
                        pr->match_ == raftLog_->LastIndex()) {
                        LOG_INFO(
                            "%lu sent MsgTimeoutNow to "
                            "%lu after received "
                            "MsgAppResp",
                            id_, msg->from());
                        // 发送过去, 让其进行选举;
                        SendTimeoutNow(msg->from());
                    }
                }
            }  // follower 是否接受: 更新它的进度
        } break;

        // follower 心跳回复;
        case MessageType::MsgHeartbeatResp: {
            pr->recentActive_ = true;
            pr->Resume();  // 恢复发送

            // 为满的传输窗口释放一个槽位;
            if (pr->state_ == ProgressStateReplicate &&
                pr->inflights_->IsFull()) {
                pr->inflights_->FreeFirstOne();
            }

            // 如果它日志落后, 趁机补发日志;
            if (pr->match_ < raftLog_->LastIndex()) {
                SendAppend(msg->from());
            }

            // ---- 线性一致读的确认环节（ReadIndex）----
            // 心跳回复里带了 context（读请求标识），说明它在确认
            // "我的 commit 还是那个 commit"
            if (readOnly_->option != ReadOnlySafe || msg->context().empty()) {
                return Status::Ok();
            }

            // 统计确认数: 加上 leader 自己, 够多数派才解锁读请求;
            uint32_t ack_count = readOnly_->RecvAck(*msg);
            LOG_INFO("ack_count = %d, from = %d", ack_count, msg->from());
            if (ack_count < Quorum()) {
                return Status::Ok();  // 还没到多数派，继续等;
            }

            // 到多数派了: 解锁该请求及之前的所有请求
            std::vector<ReadIndexStatusPtr> rss = readOnly_->Advance(*msg);

            // 判断每一个请求是否是发给自己的（本地读）还是发给别的节点(远程读)转发到这里的;
            for (ReadIndexStatusPtr& rs : rss) {
                // 消息体
                proto::Message& req = rs->req;
                if (req.from() == 0 ||
                    req.from() == id_)  // 请求是客户端发给自己的
                {
                    // 本地读: 把读的申请结果放进 readStates_，
                    // 上层检查 appliedIndex 后执行读取
                    ReadState read_state =
                        ReadState{.index = rs->index,
                                  .requestCtx = std::vector<uint8_t>(
                                      req.entries(0).data().begin(),
                                      req.entries(0).data().end())};
                    readStates_.push_back(std::move(read_state));
                } else  // 请求是客户端发给别的节点的, 其他节点再发给当前节点;
                {
                    // 远程读: 把安全水位回给那个节点
                    std::shared_ptr<proto::Message> m(new proto::Message());
                    m->set_to(req.from());
                    m->set_type(proto::MessageType::MsgReadIndexResp);
                    m->set_index(rs->index);
                    m->mutable_entries()->CopyFrom(req.entries());

                    Send(std::move(m));
                }
            }
        } break;

        // 快照发送结果的汇报（上层调 ReportSnapshot 触发）
        case MessageType::MsgSnapStatus: {
            if (pr->state_ != ProgressStateSnapshot) {
                return Status::Ok();
            }

            if (!msg->reject()) {
                // 快照发送成功：恢复复制
                pr->BecomeProbe();
                LOG_DEBUG(
                    "%lu snapshot succeeded, resumed sending "
                    "replication messages to %lu [%s]",
                    id_, msg->from(), pr->String().c_str());
            } else {
                // 快照失败：清掉待发送快照，等下一轮重试
                pr->SnapshotFailure();
                pr->BecomeProbe();
                LOG_DEBUG(
                    "%lu snapshot failed, resumed sending "
                    "replication messages to %lu [%s]",
                    id_, msg->from(), pr->String().c_str());
            }
            // 无论成败，先暂停一下;
            // 成功则等 follower 的 MsgAppResp 再继续.
            // 失败则等一个心跳间隔再重试
            pr->SetPause();
            break;
        }

        // 报告某节点不可达（网络发送失败）
        // 复制状态下消息可能丢了，退回探测状态重新同步
        case MessageType::MsgUnreachable: {
            if (pr->state_ == ProgressStateReplicate) {
                pr->BecomeProbe();
            }
            LOG_DEBUG(
                "%lu failed to send message to %lu because it is "
                "unreachable [%s]",
                id_, msg->from(), pr->String().c_str());
            break;
        }

        // 领导权转移请求;
        case MessageType::MsgTransferLeader: {
            uint64_t lead_transferee = msg->from();
            uint64_t last_lead_transferee = leadTransferee_;

            // 先本地检查目标节点;
            if (last_lead_transferee != 0) {
                // 已经在转移中, 并且相等;
                if (last_lead_transferee == lead_transferee) {
                    // 重复请求同一个目标：忽略掉;
                    LOG_INFO(
                        "%lu [term %lu] transfer "
                        "leadership to %lu is in progress, "
                        "ignores request to same node %lu",
                        id_, term_, lead_transferee, lead_transferee);
                    return Status::Ok();
                }

                // 换目标了：中止上一个转移;
                AbortLeaderTransfer();
                LOG_INFO(
                    "%lu [term %lu] abort previous "
                    "transferring leadership to %lu",
                    id_, term_, last_lead_transferee);
            }

            // 目标就是自己(已经是 leader),忽略;
            if (lead_transferee == id_) {
                LOG_DEBUG(
                    "%lu is already leader. Ignored "
                    "transferring leadership to self",
                    id_);
                return Status::Ok();
            }

            // 开始转移领导权给目标节点;
            LOG_INFO(
                "%lu [term %lu] starts to transfer leadership to "
                "%lu",
                id_, term_, lead_transferee);

            // 领导权转移应在一个选举超时内完成，重置选举计时
            electionElapsed_ = 0;
            leadTransferee_ = lead_transferee;
            if (pr->match_ == raftLog_->LastIndex()) {
                // 目标节点日志已经追平：直接让它发起选举
                SendTimeoutNow(lead_transferee);
                LOG_INFO(
                    "%lu sends MsgTimeoutNow to %lu "
                    "immediately as %lu already has up-to-date "
                    "log",
                    id_, lead_transferee, lead_transferee);
            } else {
                // 还没追平：先给它补日志，追平后再发
                // MsgTimeoutNow
                SendAppend(lead_transferee);
            }
            break;
        }
    }
    return Status::Ok();
}

// ============================================================================
// StepCandidate：candidate / pre-candidate 角色的消息处理
// ============================================================================
Status Raft::StepCandidate(std::shared_ptr<Message> msg) {
    // 只处理与候选人身份相关的消息
    // （candidate 状态下，可能会收到 pre-candidate 时期发出的、
    //  属于当前任期的过时 MsgPreVoteResp 消息）
    switch (msg->type()) {
        // 竞选期间收到提案：集群里可能没 leader，丢提案
        case MessageType::MsgProp:
            LOG_INFO("%lu no leader at term %lu; dropping proposal", id_, term_);
            return Status::InvalidArgument("raft proposal dropped");

        // 收到 leader 的追加日志：说明对方是合法 leader，让位
        case MessageType::MsgApp:
            BecomeFollower(msg->term(), msg->from());
            HandleAppendEntries(std::move(msg));
            break;

            // 收到 leader 心跳：让位
        case MessageType::MsgHeartbeat:
            BecomeFollower(msg->term(), msg->from());
            HandleHeartbeat(std::move(msg));
            break;
            
        // 收到 leader 快照：让位
        case MessageType::MsgSnap:
            BecomeFollower(msg->term(), msg->from());
            HandleSnapshot(std::move(msg));
            break;

        // 收到投票回复：计票
        case MessageType::MsgPreVoteResp:
        case MessageType::MsgVoteResp: {
            // 计算赞成票数（包括自己）和反对票数
            uint64_t gr = Poll(msg->from(), msg->type(), !msg->reject());
            LOG_INFO(
                "%lu [quorum:%u] has received %lu %s votes and %lu "
                "vote rejections",
                id_, Quorum(), gr, MsgTypeToString(msg->type()),
                votes_.size() - gr);
            if (Quorum() == gr) {
                // 赞成票到多数派：当选！
                if (state_ == proto::RaftRole::PreCandidate) {
                    // 预投票通过：转入正式选举
                    Campaign(kCampaignElection);
                } else {
                    // 正式选举通过：成为 leader
                    assert(state_ == proto::RaftRole::Candidate);
                    BecomeLeader();
                    BcastAppend();  // 广播日志（含空日志）
                }
            } else if (Quorum() == votes_.size() - gr) {
                // 反对票到多数派：竞选失败，变回 follower
                // （等更高任期的新 leader 来带）
                BecomeFollower(term_, 0);
            }
            break;
        }

        // candidate 收到 MsgTimeoutNow：忽略
        // （这种消息只有 leader才处理，本角色收到说明是乱序消息）
        case MessageType::MsgTimeoutNow: {
            LOG_DEBUG(
                "%lu [term %lu state %d] ignored MsgTimeoutNow "
                "from %lu",
                id_, term_, state_, msg->from());
        }
    }
    return Status::Ok();
}

// ============================================================================
// StepFollower：follower 角色的消息处理
// ============================================================================
Status Raft::StepFollower(std::shared_ptr<Message> msg) {
    switch (msg->type()) {
        // 客户端发来的提案消息（写请求）
        case MessageType::MsgProp:
            // 默认转发给 leader（disableProposalForwarding 可关闭）
            // 不知道 leader 是谁（刚启动/选举中）：丢提案
            if (lead_ == 0) {
                LOG_INFO(
                    "%lu no leader at term %lu; dropping "
                    "proposal",
                    id_, term_);
                return Status::InvalidArgument("raft proposal dropped");
            } else if (disableProposalForwarding_) {
                LOG_INFO(
                    "%lu not forwarding to leader %lu at term "
                    "%lu; dropping proposal",
                    id_, lead_, term_);
                return Status::InvalidArgument("raft proposal dropped");
            }

            // 转发给 leader
            msg->set_to(lead_);
            LOG_INFO("send msg to leader, MsgProp");

            // leader的回复消息给谁?
            Send(msg);
            break;

            // 收到 leader 的追加日志
        case MessageType::MsgApp: {
            electionElapsed_ = 0;  // 重置选举计时（leader 还活着）
            lead_ = msg->from();
            //
            HandleAppendEntries(msg);
            LOG_INFO("send msg to leader, MsgApp");
            break;
        }

        // 收到 leader 心跳
        case MessageType::MsgHeartbeat: {
            electionElapsed_ = 0;
            lead_ = msg->from();
            HandleHeartbeat(msg);
            break;
        }

        // 收到 leader 的快照
        case MessageType::MsgSnap: {
            electionElapsed_ = 0;
            lead_ = msg->from();
            HandleSnapshot(msg);
            break;
        }

        // 转移领导权请求：转发给 leader 处理
        case MessageType::MsgTransferLeader:
            if (lead_ == 0) {
                LOG_INFO(
                    "%lu no leader at term %lu; dropping "
                    "leader transfer msg",
                    id_, term_);
                return Status::Ok();
            }

            msg->set_to(lead_);
            LOG_INFO("send msg to leader, MsgTransferLeader");
            Send(msg);
            break;

        // 收到 MsgTimeoutNow: leader 让我立刻发起选举（交接班）
        case MessageType::MsgTimeoutNow:
            // 本节点是否够格参与选举：自己的 id 在进度表里
            if (Promotable()) {
                LOG_INFO(
                    "%lu [term %lu] received MsgTimeoutNow "
                    "from %lu and starts an election to get "
                    "leadership.",
                    id_, term_, msg->from());
                // 领导权转移从不使用预投票：
                // 我们知道当前并非从网络分区中恢复，
                // 因此不需要额外的往返确认
                Campaign(kCampaignTransfer);
            } else {
                LOG_INFO(
                    "%lu received MsgTimeoutNow from %lu but "
                    "is not Promotable",
                    id_, msg->from());
            }
            break;

        // 客户端读请求发到了 follower：转发给 leader 处理;
        case MessageType::MsgReadIndex:
            if (lead_ == 0) {
                LOG_INFO(
                    "%lu no leader at term %lu; dropping index "
                    "reading msg",
                    id_, term_);
                return Status::Ok();
            }
            msg->set_to(lead_);
            Send(msg);
            break;

        // 收到 leader 返回的安全水位: 可以执行读取了;
        case MessageType::MsgReadIndexResp:
            if (msg->entries().size() != 1) {
                LOG_ERROR(
                    "%lu invalid format of MsgReadIndexResp "
                    "from %lu, entries count: %lu",
                    id_, msg->from(), msg->entries_size());
                return Status::Ok();
            }

            ReadState rs;
            rs.index = msg->index();
            rs.requestCtx = std::vector<uint8_t>(msg->entries(0).data().begin(),
                                                 msg->entries(0).data().end());
            // 放进 readStates_，上层取走后判断能否安全读取;
            readStates_.push_back(std::move(rs));
            break;
    }
    return Status::Ok();
}

// ----------------------------------------------------------------------------
// Send: 把待发送的消息放进发送队列;
// 会统一打上"发送者 id"和"任期"。注意规则：
//   - 投票类消息必须带任期（候选人竞选时指定）
//   - 其他消息统一带当前任期 term_
//   - MsgProp/MsgReadIndex 不带任期（它们是转发给 leader 的本地逻辑消息）
// ----------------------------------------------------------------------------
void Raft::Send(std::shared_ptr<Message> msg) {
    msg->set_from(id_);
    // 拉票请求, 拉票响应;
    if (msg->type() == MessageType::MsgVote ||
        msg->type() == MessageType::MsgVoteResp ||
        msg->type() == MessageType::MsgPreVote ||
        msg->type() == MessageType::MsgPreVoteResp) {
        // 所有 {预}竞选消息在发送时都必须设置 term
        if (msg->term() == 0) {
            LOG_FATAL("term should be set when sending %s",
                      MsgTypeToString(msg->type()));
        }
    } else {
        // 其他消息不应该带任期(避免接收方误判任期)
        if (msg->term() != 0) {
            LOG_FATAL("term should not be set when sending %d (was %lu)",
                      msg->type(), msg->term());
        }

        // MsgProp: 本地消息. "提案"（客户端写请求进入 Raft 的第一步）
        // MsgReadIndex: 线性一致读请求（先取一个"安全读位置"）
        // MsgProp 和 MsgReadIndex 是转发给 leader 的本地逻辑消息，
        // 不附任期: 其他消息附当前任期（接收方用于任期对比）
        if (msg->type() != MessageType::MsgProp &&
            msg->type() != MessageType::MsgReadIndex) {
            msg->set_term(term_);
        }
    }

    // 放进发送队列, 统一由上层 raft_node 消费, 由网络模块发送;
    msgs_.push_back(std::move(msg));
}

// 根据节点列表重建进度表（快照恢复/配置变更后调用）。
// 每个节点的 match 初始为 0，next 从日志最后索引+1 开始；
// 自己除外（自己的 match 就是日志最后索引）
void Raft::RestoreNode(const std::vector<uint64_t>& nodes) {
    for (uint64_t node : nodes) {
        uint64_t match = 0;
        uint64_t next = raftLog_->LastIndex() + 1;
        if (node == id_) {
            match = next - 1;
        }
        SetProgress(node, match, next);
    }
}

// 本节点是否够格参与选举：自己的 id 在进度表里
bool Raft::Promotable() const {
    auto it = prs_.find(id_);
    return it != prs_.end();
}

// ----------------------------------------------------------------------------
// HandleAppendEntries：follower 处理 leader 的"追加日志"请求
// ----------------------------------------------------------------------------
void Raft::HandleAppendEntries(std::shared_ptr<Message> msg) {
    // 如果 leader 说"我的日志前一条 index"比我的 commit 还小：
    // 直接告诉 leader 我 commit 到哪了（快速对齐，不用走完整对账）
    if (msg->index() < raftLog_->committed_) {
        std::shared_ptr<Message> m(new Message());
        m->set_to(msg->from());
        m->set_type(MessageType::MsgAppResp);
        m->set_index(raftLog_->committed_);
        Send(std::move(m));
        return;
    }

    // 把消息里的条目转成共享指针列表
    std::vector<std::shared_ptr<Entry>> entries;
    for (Entry entry : msg->entries()) {
        entries.push_back(std::make_shared<Entry>(std::move(entry)));
    }

    // 核心对账：检查 index 处日志的任期是否和 leader 说的一致，
    // 一致则收下新条目并推进 commit；不一致则拒绝
    bool ok = false;
    uint64_t lastIndex = 0;
    raftLog_->MaybeAppend(msg->index(), msg->logterm(), msg->commit(),
                          std::move(entries), lastIndex, ok);

    if (ok) {
        // 接受: 回复 leader"我收到了, 日志到 lastIndex"
        std::shared_ptr<Message> m(new Message());
        m->set_to(msg->from());
        m->set_type(MessageType::MsgAppResp);
        m->set_index(lastIndex);
        Send(std::move(m));
    } else {
        // 拒绝：告诉 leader"从 index 开始对不上"，
        // 并用 rejecthint 提示自己日志最后到哪（帮 leader 快速回退）
        uint64_t term = 0;
        raftLog_->Term(msg->index(), term);
        LOG_DEBUG(
            "%lu [logTerm: %lu, index: %lu] rejected msgApp [logTerm: "
            "%lu, index: %lu] from %lu",
            id_, term, msg->index(), msg->logterm(), msg->index(), msg->from())

        std::shared_ptr<Message> m(new Message());
        m->set_to(msg->from());
        m->set_type(MessageType::MsgAppResp);
        m->set_index(msg->index());
        m->set_reject(true);
        m->set_rejecthint(raftLog_->LastIndex());
        Send(std::move(m));
    }
}

// follower 处理 leader 的心跳：
// 推进自己的 commit（leader 的 commit 一定 >= 我的），然后回个心跳确认
void Raft::HandleHeartbeat(std::shared_ptr<Message> msg) {
    raftLog_->CommitTo(msg->commit());
    std::shared_ptr<Message> m(new Message());
    m->set_to(msg->from());
    m->set_type(MessageType::MsgHeartbeatResp);
    m->set_context(msg->context());  // 原样带回 context（线性一致读确认用）
    Send(std::move(m));
}

// follower 处理 leader 的快照请求
void Raft::HandleSnapshot(std::shared_ptr<Message> msg) {
    uint64_t sindex = msg->snapshot().metadata().index();
    uint64_t sterm = msg->snapshot().metadata().term();

    if (Restore(msg->snapshot())) {
        // 快照比我的日志新，成功恢复
        LOG_INFO(
            "%lu [commit: %lu] restored snapshot [index: %lu, term: "
            "%lu]",
            id_, raftLog_->committed_, sindex, sterm);
        std::shared_ptr<Message> m(new proto::Message());
        m->set_to(msg->from());
        m->set_type(proto::MsgAppResp);
        msg->set_index(raftLog_->LastIndex());
        Send(std::move(m));
    } else {
        // 快照比我的日志旧，忽略（我的日志已经比快照新了）
        LOG_INFO(
            "%lu [commit: %lu] ignored snapshot [index: %lu, term: "
            "%lu]",
            id_, raftLog_->committed_, sindex, sterm);
        std::shared_ptr<Message> m(new proto::Message());
        m->set_to(msg->from());
        m->set_type(proto::MsgAppResp);
        msg->set_index(raftLog_->committed_);
        Send(std::move(m));
    }
}

// ----------------------------------------------------------------------------
// Restore：用快照恢复本节点
// 三种情况：
//   1. 快照索引 <= 我的 commit：快照太旧，不恢复
//   2. 快照索引处日志的任期和快照一致：日志还能接上，
//      直接快进 commit 到快照索引，不用真正恢复
//   3. 否则：真正恢复——清空日志，用快照重建，重建进度表
// ----------------------------------------------------------------------------
bool Raft::Restore(const proto::Snapshot& s) {
    if (s.metadata().index() <= raftLog_->committed_) {
        return false;  // 快照不比当前日志新
    }

    // 快照索引处的日志任期和快照匹配：说明我的日志和快照是连续的，
    // 只需把 commit 快进到快照索引即可，不用大动干戈
    if (raftLog_->MatchTerm(s.metadata().index(), s.metadata().term())) {
        LOG_INFO(
            "%lu [commit: %lu, lastIndex: %lu, LastTerm: %lu] "
            "fast-forwarded commit to snapshot [index: %lu, term: %lu]",
            id_, raftLog_->committed_, raftLog_->LastIndex(),
            raftLog_->LastTerm(), s.metadata().index(), s.metadata().term());
        raftLog_->CommitTo(s.metadata().index());
        return false;
    }

    LOG_INFO(
        "%lu [commit: %lu, lastIndex: %lu, LastTerm: %lu] starts to "
        "restore snapshot [index: %lu, term: %lu]",
        id_, raftLog_->committed_, raftLog_->LastIndex(), raftLog_->LastTerm(),
        s.metadata().index(), s.metadata().term());

    // 真正恢复：把快照交给日志（重置 unstable），清空进度表
    std::shared_ptr<proto::Snapshot> snap(new proto::Snapshot(s));
    raftLog_->Restore(snap);
    prs_.clear();

    // 用快照里的集群成员重建进度表
    std::vector<uint64_t> nodes;
    for (auto& node : s.metadata().confstate().nodes()) {
        nodes.emplace_back(node);
    }
    RestoreNode(nodes);

    return true;
}

// 时钟推进入口：每 tick 调用一次，交给当前角色绑定的 tick_ 函数
// （follower/candidate 走选举计时，leader 走心跳计时）
void Raft::Tick() {
    if (tick_) {
        tick_();
    } else {
        LOG_WARN("tick function is not set");
    }
}

// 软状态: leader 是谁 + 我是什么角色(不需要持久化)
std::shared_ptr<proto::SoftState> Raft::SoftState() const {
    std::shared_ptr<proto::SoftState> ss = std::make_shared<proto::SoftState>();
    ss->set_lead(this->lead_);
    ss->set_state(this->state_);
    return ss;
}

// 硬状态：term + vote + commit（需要持久化，掉电不能丢）
proto::HardState Raft::HardState() const {
    proto::HardState hs;
    hs.set_term(term_);
    hs.set_vote(vote_);
    hs.set_commit(raftLog_->committed_);
    return hs;
}

// 从硬状态恢复（重启时）：恢复 term/vote/commit
void Raft::LoadState(const proto::HardState& state) {
    // 硬状态里的 commit 正常应该在 [已提交位置, 日志末尾] 之间。
    // 但如果上次是被强杀/断电（WAL 尾部没落盘），读回来的 commit 可能比
    // 快照索引还小、也可能比日志末尾还大。这时不该让节点直接崩掉，而是
    // "就近钳制"到合法范围，让节点能起来——丢掉的那部分由 leader 重新复制回来。
    uint64_t commit = state.commit();
    uint64_t low = raftLog_->committed_;  // 通常等于快照索引
    uint64_t high = raftLog_->LastIndex();

    if (commit < low) {
        LOG_WARN("%lu state.commit %lu < committed %lu(snapshot), clamp up",
                 id_, commit, low);
        commit = low;
    } else if (commit > high) {
        LOG_WARN("%lu state.commit %lu > LastIndex %lu, clamp down", id_,
                 commit, high);
        commit = high;
    }

    // 获得 之前保存的 commit进度;
    raftLog_->committed_ = commit;
    term_ = state.term();
    vote_ = state.vote();
}

// 当前集群节点列表（按 id 排序后返回）
void Raft::Nodes(std::vector<uint64_t>& node) const {
    for (auto it = prs_.begin(); it != prs_.end(); ++it) {
        node.push_back(it->first);
    }
    std::sort(node.begin(), node.end());
}

// 获取某个节点的进度（不在成员表返回 nullptr）
ProgressPtr Raft::GetProgress(uint64_t id) {
    auto it = prs_.find(id);
    if (it != prs_.end()) {
        return it->second;
    }
    return nullptr;
}

// 设置某个节点的进度（重建进度表用）
void Raft::SetProgress(uint64_t id, uint64_t match, uint64_t next) {
    ProgressPtr progress(new Progress(maxInflight_));
    progress->next_ = next;
    progress->match_ = match;
    prs_[id] = progress;
    return;
}

// 删除某个节点的进度（配置变更：移除节点）
void Raft::DelProgress(uint64_t id) { prs_.erase(id); }

// 发送追加日志（允许空消息，用于更新 commit）
void Raft::SendAppend(uint64_t to) {
    // 给 follower 发送追加消息;
    MaybeSendAppend(to, true);
}

// ----------------------------------------------------------------------------
// MaybeSendAppend：构造并发送一条追加日志（或快照）消息
// 这是 leader 复制日志的核心函数
// ----------------------------------------------------------------------------
bool Raft::MaybeSendAppend(uint64_t to, bool sendIfEmpty) {
    ProgressPtr pr = GetProgress(to);
    if (pr->IsPaused()) {
        return false;  // 暂停中（等回复/窗口满/发快照），不发
    }

    std::shared_ptr<Message> msg(new Message());
    msg->set_to(to);
    uint64_t term = 0;

    // 取"next-1" 位置的任期（前一条日志的任期, append 对账要用）
    Status status_term = raftLog_->Term(pr->next_ - 1, term);
    std::vector<std::shared_ptr<proto::Entry>> entries;

    // 从 next 开始取日志(最多 maxMsgSize_ 字节)
    Status status_entries = raftLog_->Entries(pr->next_, maxMsgSize_, entries);
    if (entries.empty() && !sendIfEmpty) {
        return false;  // 没有新日志且不允许空消息，不发
    }

    // 发送快照
    if (!status_term.IsOk() || !status_entries.IsOk()) {
        // 日志取不出来（next 已经被压缩掉了）:必须发快照
        if (!pr->recentActive_) {
            // follower 最近不活跃，先不发快照（省带宽，等它活过来）
            LOG_DEBUG(
                "ignore sending snapshot to %lu since it is not "
                "recently active",
                to);
            return false;
        }

        msg->set_type(proto::MessageType::MsgSnap);

        std::shared_ptr<proto::Snapshot> snap;
        Status status = raftLog_->Snapshot(snap);
        if (!status.IsOk()) {
            LOG_FATAL("snapshot error %s", status.ToString().c_str());
        }

        if (IsEmptySnapshot(*snap)) {
            LOG_FATAL("need non-empty snapshot");
        }

        uint64_t sindex = snap->metadata().index();
        uint64_t sterm = snap->metadata().term();
        LOG_DEBUG(
            "%lu [first_index: %lu, commit: %lu] sent snapshot[index: "
            "%lu, term: %lu] to %lu [%s]",
            id_, raftLog_->FirstIndex(), raftLog_->committed_, sindex, sterm,
            to, pr->String().c_str());
        // 进入快照状态，暂停普通复制;
        pr->BecomeSnapshot(sindex);
        msg->set_allocated_snapshot(snap.get());
        LOG_DEBUG("%lu paused sending replication messages to %lu [%s]", id_,
                  to, pr->String().c_str());
    } else {
        // 正常发送追加日志[];
        msg->set_type(proto::MessageType::MsgApp);
        msg->set_index(pr->next_ - 1);  // 前一条日志的索引
        msg->set_logterm(term);         // 前一条日志的任期

        for (std::shared_ptr<proto::Entry>& entry : entries) {
            msg->add_entries()->CopyFrom(*entry);
        }

        msg->set_commit(raftLog_->committed_);  // 带上 leader 的提交索引;

        if (!msg->entries_size() == 0) {
            switch (pr->state_) {
                // 复制状态: 乐观地把 next 前推，同时把
                // 最后一条日志索引记入在途窗口
                case ProgressStateReplicate: {
                    uint64_t last =
                        msg->entries(msg->entries_size() - 1).index();
                    pr->OptimisticUpdate(last);
                    pr->inflights_->Add(last);
                    break;
                }
                // 探测状态: 发一条就暂停，等回复确认
                case ProgressStateProbe: {
                    pr->SetPause();
                    break;
                }
                default: {
                    LOG_FATAL(
                        "%lu is sending append in "
                        "unhandled state %s",
                        id_, ProgressStateToString(pr->state_));
                }
            }
        }
    }
    // 下发到任务队列中, 等待被提取走;
    Send(std::move(msg));
    return true;
}

// 发送心跳：commit 字段取 min(对方 match, 我的 commit)。
// 大白话：不能把对方的 commit 推到它日志没跟上的位置——
// 如果 follower 的 match 比我的 commit 小，说明它还没收到那些已提交的日志，
// 只能告诉它"你确认到哪, commit 就推进到哪"
void Raft::SendHeartbeat(uint64_t to, std::vector<uint8_t> ctx) {
    uint64_t commit = std::min(GetProgress(to)->match_, raftLog_->committed_);
    std::shared_ptr<proto::Message> msg(new proto::Message());
    msg->set_to(to);
    msg->set_type(proto::MessageType::MsgHeartbeat);
    msg->set_commit(commit);
    // ctx 是"最后一个待确认读请求"的标识（线性一致读确认用）
    msg->set_context(std::string(std::begin(ctx), std::end(ctx)));
    Send(std::move(msg));
}

// 遍历所有节点的进度
void Raft::ForEachProgress(
    const std::function<void(uint64_t, ProgressPtr&)>& callback) {
    for (auto it = prs_.begin(); it != prs_.end(); ++it) {
        callback(it->first, it->second);
    }
}

// 向所有 follower 广播追加日志（跳过自己）
void Raft::BcastAppend() {
    auto handler = [this](uint64_t id, ProgressPtr& progress) {
        if (id == id_) {
            return;
        }
        // 开始复制日志（可能是空日志，带上 commit 进度）
        this->SendAppend(id);
    };
    ForEachProgress(handler);
}

// 广播普通心跳（带上最后一个待确认读请求的标识）
void Raft::BcastHeartbeat() {
    std::vector<uint8_t> ctx;
    readOnly_->LastPendingRequestCtx(ctx);
    BcastHeartbeatWithCtx(std::move(ctx));
}

// 广播带指定上下文的（读确认）心跳
void Raft::BcastHeartbeatWithCtx(const std::vector<uint8_t>& ctx) {
    auto handler = [this, ctx](uint64_t id, ProgressPtr& progress) {
        if (id == id_) {
            return;
        }
        // 发送心跳;
        this->SendHeartbeat(id, std::move(ctx));
    };
    ForEachProgress(handler);
}

// ----------------------------------------------------------------------------
// MaybeCommit: 尝试推进提交索引（leader 每收到一次 MsgAppResp 都调用）
//
// 大白话: 把所有人的 match 排个序，取"第 Quorum 大"的那个值——
// 如果有超过半数的节点 match >= X，那么 X 就有资格被提交。
// 真正提交前还要检查：第 X 条日志的任期必须是当前任期
// （Raft 规则：只能提交当前任期的日志）
// ----------------------------------------------------------------------------
bool Raft::MaybeCommit() {
    // matchBuf 是复用缓冲区，避免每次调用都重新分配;
    matchBuf_.clear();

    for (auto it = prs_.begin(); it != prs_.end(); ++it) {
        matchBuf_.push_back(it->second->match_);
    }
    std::sort(matchBuf_.begin(), matchBuf_.end());
    // 取第 Quorum 大的值（升序排序后倒数第 Quorum 个）
    auto mci = matchBuf_[matchBuf_.size() - Quorum()];
    // 找到 mci 对应的日志任期, 并判断是否能提交,因为任期必须一致;
    return raftLog_->MaybeCommit(mci, term_);
}

// ----------------------------------------------------------------------------
// Reset：重置状态（角色切换时调用）
// 清掉：lead、计时器、投票、进度表、待定配置、未提交大小、读请求登记
// ----------------------------------------------------------------------------
void Raft::Reset(uint64_t term) {
    if (term_ != term) {
        // 任期变化：清投票（新任期重新投）
        term_ = term;
        vote_ = 0;
    }
    lead_ = 0;

    electionElapsed_ = 0;
    heartbeatElapsed_ = 0;

    // 重新随机选举超时（防止大家同时超时）
    ResetRandomizedElectionTimeout();

    // 中止领导权转移
    AbortLeaderTransfer();

    // 清投票记录
    votes_.clear();

    // 重建所有节点的进度: next 从日志最后索引+1 开始；
    // 自己例外：match 就是日志最后索引
    auto handler = [this](uint64_t id, ProgressPtr& progress) {
        progress = std::make_shared<Progress>(maxInflight_);
        progress->next_ = raftLog_->LastIndex() + 1;

        if (id == id_) {
            progress->match_ = raftLog_->LastIndex();
        }
    };

    ForEachProgress(handler);

    pendingConfIndex_ = 0;
    uncommittedSize_ = 0;

    // 清掉所有待确认的读请求
    readOnly_->pendingReadIndex.clear();
    readOnly_->readIndexQueue.clear();
}

// 添加节点（配置变更：加节点）。
// 新节点 match=0，next 从最后索引+1 开始（它会先收快照或日志追齐）
void Raft::AddNode(uint64_t id) {
    ProgressPtr pr = GetProgress(id);
    if (pr == nullptr) {
        SetProgress(id, 0, raftLog_->LastIndex() + 1);
    } else {
        prs_[id] = pr;
    }

    // 新节点刚加入时标记为"最近活跃"：
    // 否则若 checkQuorum 在新节点有机会通信前被调用，
    // 可能误判多数派失联而主动下台
    GetProgress(id)->recentActive_ = true;
}

// ----------------------------------------------------------------------------
// AppendEntry：把新条目追加到 leader 本地日志（提案的核心）
// 流程：编号（term/index）→ 检查未提交大小 → 写 unstable → 更新自己进度
// ----------------------------------------------------------------------------
bool Raft::AppendEntry(const std::vector<Entry>& entries) {
    uint64_t li = raftLog_->LastIndex();
    std::vector<std::shared_ptr<Entry>> ents(entries.size(), nullptr);

    // 给每个条目编上 index 和 term（日志的身份证）
    for (size_t i = 0; i < entries.size(); ++i) {
        std::shared_ptr<Entry> ent(new Entry());
        ent->set_term(term_);
        ent->set_index(li + 1 + i);
        ent->set_data(entries[i].data());
        ent->set_type(entries[i].type());
        ents[i] = ent;
    }

    // 记录这条尚未提交提案的大小（防止日志无限膨胀）
    if (!IncreaseUncommittedSize(ents)) {
        LOG_DEBUG(
            "%lu appending new entries to log would exceed uncommitted "
            "entry size limit; dropping proposal",
            id_);
        // 超限，丢弃该提案
        return false;
    }

    // 写入日志(unstable 区)
    li = raftLog_->Append(ents);
    // 更新自己的进度（match = 最新日志索引）
    GetProgress(id_)->MaybeUpdate(li);
    // 尝试提交（即使返回 false，调用者也会广播 append）
    MaybeCommit();
    return true;
}

// follower/candidate 的选举计时：
// 每 tick 一次，如果超时（且自己有资格），就发起选举
void Raft::TickElection() {
    electionElapsed_++;
    // 有资格参与选举 && 已到随机化超时时间
    if (Promotable() && PastElectionTimeout()) {
        electionElapsed_ = 0;
        // 发"开始选举"的本地消息（走正常 Step 流程）
        std::shared_ptr<Message> msg(new Message());
        msg->set_from(id_);
        msg->set_type(MessageType::MsgHup);
        // 发送拉票请求;
        Step(std::move(msg));
    }
}

// leader 的心跳计时:
// 每 tick 一次: 到心跳间隔就广播心跳;到选举超时就检查多数派/中止转移;
void Raft::TickHeartbeat() {
    heartbeatElapsed_++;
    electionElapsed_++;

    // 选举超时到期（leader 的"任期保护"检查点）
    if (electionElapsed_ >= electionTimeout_) {
        electionElapsed_ = 0;
        if (checkQuorum_) {
            // 检查多数派是否活跃（不活跃就下台）
            std::shared_ptr<Message> msg(new Message());
            msg->set_from(id_);
            msg->set_type(MessageType::MsgCheckQuorum);
            Step(std::move(msg));
        }
        // 如果领导权转移在选举超时内没完成，中止（重新当回正常 leader）
        if (state_ == proto::RaftRole::Leader && leadTransferee_ != 0) {
            AbortLeaderTransfer();
        }
    }

    // 非 leader 不走心跳逻辑
    if (state_ != proto::RaftRole::Leader) {
        return;
    }

    // 心跳间隔到期：广播心跳
    if (heartbeatElapsed_ >= heartbeatTimeout_) {
        heartbeatElapsed_ = 0;
        std::shared_ptr<Message> msg(new Message());
        msg->set_from(id_);
        msg->set_type(MessageType::MsgBeat);
        // 处理所有消息;
        Step(std::move(msg));
    }
}

// 选举超时是否已到
bool Raft::PastElectionTimeout() {
    return electionElapsed_ >= randomizedElectionTimeout_;
}

// 重新随机选举超时：范围 [electionTimeout_, 2*electionTimeout_]
void Raft::ResetRandomizedElectionTimeout() {
    randomizedElectionTimeout_ = electionTimeout_ + randomDevice_.Gen();
    assert(randomizedElectionTimeout_ <= 2 * electionTimeout_);
}

// 检查"多数派是否活跃"：数一下自己和最近活跃的 follower 数量是否 >= 多数派
bool Raft::CheckQuorumActive() {
    size_t act = 0;
    auto handler = [&act, this](uint64_t id, ProgressPtr& pr) {
        if (id == this->id_) {
            act++;  // 自己当然活跃
            return;
        }
        if (pr->recentActive_) {
            act++;
        }
    };
    //
    ForEachProgress(handler);
    //
    return act >= Quorum();
}

// 发送 MsgTimeoutNow：命令目标节点立刻发起选举（领导权转移的最后一步）
void Raft::SendTimeoutNow(uint64_t to) {
    std::shared_ptr<Message> msg(new Message());
    msg->set_to(to);
    msg->set_type(MessageType::MsgTimeoutNow);
    Send(std::move(msg));
}

// 中止领导权转移
void Raft::AbortLeaderTransfer() { leadTransferee_ = 0; }

// 计算新条目的未提交大小，超限返回 false（拒绝提案）
// 特殊规则：如果当前未提交区为空，允许任何大小的提案（哪怕超过配额）
bool Raft::IncreaseUncommittedSize(
    const std::vector<std::shared_ptr<Entry>>& entries) {
    uint32_t s = 0;
    for (auto& entry : entries) {
        s += entry->data().size();  // 按数据字节数估算
    }
    if (uncommittedSize_ > 0 && uncommittedSize_ + s > maxUncommittedSize_) {
        // 超限：丢弃提案
        return false;
    }
    uncommittedSize_ += s;
    return true;
}

// 条目提交后，减少未提交大小的计数;
void Raft::ReduceUncommittedSize(
    const std::vector<std::shared_ptr<Entry>>& entries) {
    // 未设置限制，直接返回;
    if (uncommittedSize_ == 0) {
        // 快速路径：follower 不追踪该限制，直接返回;
        return;
    }

    uint32_t size = 0;

    for (const std::shared_ptr<Entry>& e : entries) {
        size += e->data().size();
    }

    if (size > uncommittedSize_) {
        // uncommittedSize 可能低估了实际大小(但绝不会高估),
        // 防止下溢，饱和为 0
        uncommittedSize_ = 0;
    } else {
        uncommittedSize_ -= size;
    }
}
}  // namespace kv
