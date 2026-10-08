#include <apply/raft-node/raft_node.h>
#include <common/log.h>
#include <common/utils.h>

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <chrono>
#include <future>
#include <thread>

namespace kv {

// 全局唯一的节点实例(信号处理用)
static RaftNodePtr gNode = nullptr;

// 快照阈值: 应用日志条数超过这个值就触发快照
static uint64_t defaultSnapCount = 100000;

// 快照后保留的日志条数(压缩日志时保留最近 N 条, 不全部删光)
static uint64_t snapshotCatchUpEntriesN = 100000;

// ============================================================================
// RaftNodeImpl：RaftNode 的具体实现（整个应用层的"总装车间"）
//
// 大白话：这个类把前面所有模块拼成一台完整的机器。核心是一个事件循环：
//
//   ┌─ 定时器（每 100ms）─────────────────────────────┐
//   │   node_->Tick()  →  raft 推进时钟               │
//   │   PullReadyEvents() → 处理 raft 产生的任务      │
//   └─────────────────────────────────────────────────┘
//
//   PullReadyEvents 是核心循环（对应 Ready 机制）：
//     1. 有 Ready 吗？（HasReady/GetReady）
//     2. 写 WAL（硬状态 + 新日志）——持久化
//     3. 存快照（如果有）
//     4. 日志追加进 MemoryStorage
//     5. 通过 Transport 发送消息给其他节点
//     6. 应用已提交的日志（PublishEntries → StateMachine → RocksDB）
//     7. 处理读结果（PublishReadStates）
//     8. 检查是否触发快照
//     9. node_->Advance(rd) 通知 raft "处理完了"
// ============================================================================
class RaftNodeImpl : public RaftNode {
   private:
    pthread_t pthreadId_;  // 主事件循环线程 id(用于判断是否在自己线程)
    boost::asio::io_service ioService_;            // 主线程事件循环, 用来消费 raft 的任务;
    boost::asio::deadline_timer timer_;            // 定时器(驱动 Tick, 以及控制任务消费的频率)

    uint64_t id_;                                  // 本节点 id
    std::vector<std::string> peers_;               // 集群地址列表
    // repeated uint64 nodes = 1;
    std::shared_ptr<proto::ConfState> confState_;  // 集群配置（成员id表）

    uint64_t lastIndex_;      // 恢复时 WAL 的最后日志索引
    uint64_t snapshotIndex_;  // 最近一次快照的日志索引
    uint64_t appliedIndex_;   // 已应用到状态机的日志索引

    MemoryStoragePtr storage_;    // 内存日志存储, 为的是方便raft能读取日志做日志检测和匹配;
    std::unique_ptr<Node> node_;  // raft 状态机(计算日志/选举/心跳/投票等)
    TransporterPtr transport_;    // 节点间通信器;
    std::shared_ptr<StateMachine>stateMachine_;  // 应用状态机(RocksDB + 客户端RPC_server)

    std::string rootDataDir_;                           // 数据根目录
    SnapshotData snapData_;                             // 内存中的快照数据容器
    std::string snapDir_;                               // 快照目录
    uint64_t snapCount_;                                // 快照触发阈值
    std::unique_ptr<SnapshotFeature> snapshotFeature_;  // 快照文件管理

    std::string dbDir_;   // RocksDB 目录
    std::string walDir_;  // WAL 目录
    WALptr wal_;          // WAL 管理器

    std::string dbPath_;  // DB 路径（实际未使用，可忽略）

   public:
    // ------------------------------------------------------------------
    // 构造函数：初始化一切
    // 大白话：干四件事
    //   1. 建目录（node_<id>/snap、wal、rocksdb）
    //   2. 恢复数据（先加载快照，再按快照索引选择性重放 WAL）
    //   3. 填配置（选举/心跳超时、日志大小限制、是否 pre-vote…）
    //   4. 创建 raft 节点（有 WAL 走重启流程 RestartNode，否则全新 StartNode）
    // ------------------------------------------------------------------
    RaftNodeImpl(uint64_t id, std::string originDataDir,
                 const std::vector<std::string>& cluster)
        : pthreadId_(0),
          timer_(ioService_),
          id_(id),
          lastIndex_(0),
          confState_(new proto::ConfState()),
          snapshotIndex_(0),
          appliedIndex_(0),
          storage_(new MemoryStorage()),
          snapCount_(defaultSnapCount) {

        peers_ = cluster;
        if (peers_.empty()) {
            LOG_FATAL("invalid args cluster");
        }

        // 数据目录: <rootDir>/node_<id>/{snap,wal,rocksdb}
        using boost::filesystem::path;
        path rootDir = originDataDir;
        path workDir = rootDir / ("node_" + std::to_string(id));
        snapDir_ = (workDir / "snap").string();
        walDir_ = (workDir / "wal").string();
        dbDir_ = (workDir / "rocksdb").string();

        // 快照目录不存在就创建
        if (!boost::filesystem::exists(snapDir_)) {
            boost::filesystem::create_directories(snapDir_);
        }

        // 快照文件管理
        if (!snapshotFeature_) {
            snapshotFeature_ = std::make_unique<SnapshotFeature>(snapDir_);
        }

        // WAL 目录是否已存在 (存在 = 重启; 不存在 = 全新启动)
        bool wal_exists = boost::filesystem::exists(walDir_);

        // 重启时: 1.加载快照,获取快照的 index, 硬状态: term/vote/commit;
        //        2.根据快照 index 选择性重放 WAL 日志;
        //           没有 WAL 可恢复时，用快照信息为起点;
        //        3.把快照 index 写入内存日志存储 MemoryStorage;
        ReplayWAL();

        // ---- 填配置 ----
        Config& conf = Config::GetInstance();
        conf.id = id;
        // 选举超时 = 10 个时钟（每个时钟 100ms = 1 秒）
        conf.electionTick = 10;
        // 心跳间隔 = 1 个时钟（100ms）
        conf.heartbeatTick = 1;
        // 内存存储
        conf.storage = storage_;
        conf.applied = 0;
        // 单条追加消息最大 1MB
        conf.maxSizePerMsg = 1024 * 1024;
        conf.maxCommittedSizePerReady = 0;
        // leader 未提交日志上限 1GB
        conf.maxUncommittedEntriesSize = 1 << 30;
        // 单个 follower 在途消息窗口 256 条
        conf.maxInflightMsgs = 256;
        // leader 检查多数派是否活跃
        conf.checkQuorum = true;
        // 开启预投票（防止分区节点捣乱）
        conf.preVote = true;

        // 只读请求用安全模式（ReadIndex）or ReadOnlyLeaseBased
        conf.readOnlyOption = ReadOnlySafe;
        conf.disableProposalForwarding = false;
        conf.clusterInfo = peers_;

        // 校验配置合法性
        Status status = conf.Validate();

        if (!status.IsOk()) {
            LOG_FATAL("invalid configure %s", status.ToString().c_str());
        }

        // ---- 构造对端节点上下文(每个节点的地址信息)----
        std::vector<proto::PeerContext> peersCtx;
        for (size_t i = 0; i < peers_.size(); ++i) {
            // 地址格式 "ip:传输端口:客户端访问端口"
            std::vector<std::string> ctx;
            boost::split(ctx, peers_[i], boost::is_any_of(":"));
            proto::PeerContext peerCtx;
            peerCtx.set_id(i + 1);        // 节点编号从 1 开始
            peerCtx.set_address(ctx[0]);  // ip地址
            peerCtx.set_portfortransport(std::stoi(ctx[1]));  // 节点间通信端口
            peerCtx.set_portforclient(std::stoi(ctx[2]));     // 客户端访问端口
            peersCtx.push_back(peerCtx); // 节点上下文数据
        }

        // ---- 创建 raft 节点 ----
        // 有 WAL = 重启恢复(成员表从存储里来)
        if (wal_exists) {
            node_.reset(Node::RestartNode(conf));
        } else {
            // 全新启动(把集群成员写进初始日志)
            node_.reset(Node::StartNode(conf, peersCtx));
        }
    }  // RaftNodeImpl{} 构造函数;

    // 析构：停止通信
    ~RaftNodeImpl() {
        LOG_DEBUG("stopped");
        if (transport_) {
            transport_->Stop();
            transport_ = nullptr;
        }
    }

    // ------------------------------------------------------------------
    // Start：启动节点（阻塞运行）
    // 流程：
    //   1. 创建并启动 Transport（节点间通信 server）
    //   2. 为每个其他节点建立连接
    //   3. 从存储恢复快照信息（confState/snapshotIndex/appliedIndex）
    //   4. 启动 StateMachine（客户端 RPC server + 工作线程）
    //   5. 启动定时器（驱动 raft 时钟）
    //   6. 进入事件循环 ioService_.run()（阻塞）
    // ------------------------------------------------------------------
    void Start() {
        // 开启节点间通信;
        transport_ = Transport::Create(this, id_);

        std::string& host = peers_[id_ - 1];  // 自己的地址

        // 启动本机 server（监听节点间通信端口）
        transport_->Start(host);

        // 为其他每个节点建立连接
        for (uint64_t i = 0; i < peers_.size(); ++i) {
            uint64_t peer = i + 1;
            if (peer == id_) {
                continue;  // 跳过自己
            }
            transport_->AddPeer(peer, peers_[i]);
        }

        // 记录主线程 id（用于判断"是否在 raft 线程上"）
        pthreadId_ = pthread_self();

        // 从存储里取快照，恢复各种索引
        std::shared_ptr<proto::Snapshot> snap;
        Status status = storage_->Snapshot(snap);
        if (!status.IsOk()) {
            LOG_FATAL("get snapshot failed %s", status.ToString().c_str());
        }

        *(this->confState_) = snap->metadata().confstate();  // 集群配置
        snapshotIndex_ = snap->metadata().index();           // 快照索引
        appliedIndex_ = snap->metadata().index();            // 已应用索引

        // 启动应用状态机: 开客户端 RPC server + 工作线程
        std::vector<std::string> ctx;
        boost::split(ctx, peers_[id_ - 1], boost::is_any_of(":"));
        std::string address = ctx[0] + ":" + ctx[2];  // "ip:客户端端口"

        stateMachine_ = std::make_shared<StateMachine>(this, snapData_, address, dbDir_);

        // 等待状态机工作线程启动完成（拿到线程 id）
        std::promise<pthread_t> promise;
        std::future<pthread_t> future = promise.get_future();

        // 子线程 执行 this->ioService_.run();异步任务池;
        stateMachine_->Start(promise);
        // 阻塞，直到子线程 set_value
        future.wait();

        pthread_t id = future.get();
        LOG_DEBUG("server start [%lu]", id);

        // 启动定时器(驱动 raft 时钟: 心跳/选举超时)
        StartTimer();

        // 进入事件循环(阻塞在这里直到 ioService_.Stop())
        ioService_.run();
    }

    // Stop: 停止节点
    void Stop() {
        LOG_DEBUG("stopping");
        stateMachine_->Stop();
        if (transport_) {
            transport_->Stop();
            transport_ = nullptr;
        }
        ioService_.stop();
    }

    // ------------------------------------------------------------------
    // PublishEntries：应用一批已提交的日志条目（真正的"执行"）
    // 大白话：Raft 提交日志后，这里逐条执行：
    //   - 普通条目：交给状态机执行（Set/Get/Del → RocksDB + 回包客户端）
    //   - 配置变更：改集群成员表 + 通知 Transport 建立新连接
    // 每条应用完更新 appliedIndex_
    // ------------------------------------------------------------------
    bool PublishEntries(
        const std::vector<std::shared_ptr<proto::Entry>>& entries) {
        for (const std::shared_ptr<proto::Entry>& entry : entries) {
            switch (entry->type()) {
                case proto::EntryType::EntryNormal: {
                    if (entry->data().empty()) {
                        // 空日志（新 leader 的任期标记），忽略
                        break;
                    }
                    // 真正执行：状态机解析请求 → 操作 DB → 回包
                    stateMachine_->ApplyStateMachine(entry);
                    break;
                }

                case proto::EntryType::EntryConfChange: {
                    // 配置变更：解析出加节点指令
                    proto::ConfChange cc;
                    try {
                        cc.ParseFromArray(entry->data().data(),
                                          entry->data().size());
                    } catch (std::exception& e) {
                        LOG_ERROR(
                            "invalid EntryConfChange "
                            "msg %s",
                            e.what());
                        continue;
                    }
                    // 应用到 raft（更新成员表）
                    confState_ = node_->ApplyConfChange(cc);

                    switch (cc.type()) {
                        case proto::ConfChangeType::ConfChangeAddNode:
                            if (!cc.context().empty()) {
                                // 新节点的地址
                                // （context 里存着 "ip:端口:端口"）
                                transport_->AddPeer(cc.nodeid(), cc.context());
                            }
                            break;
                        default: {
                            LOG_INFO(
                                "configure change "
                                "%d",
                                cc.type());
                        }
                    }
                    break;
                }
                default: {
                    LOG_FATAL("unknown type %d", entry->type());
                    return false;
                }
            }

            // 提交后更新 appliedIndex
            appliedIndex_ = entry->index();

            // 恢复完成标记（WAL 重放时用）
            if (entry->index() == this->lastIndex_) {
                LOG_DEBUG("replay has finished");
            }
        }
        return true;
    }

    // 过滤出"真正需要应用"的条目：跳过已经应用过的（appliedIndex 之前的）
    void EntriesToApply(const std::vector<std::shared_ptr<Entry>>& entries,
                        std::vector<std::shared_ptr<Entry>>& ents) {
        if (entries.empty()) {
            return;
        }

        uint64_t first = entries[0]->index();
        if (first > appliedIndex_ + 1) {
            // 第一条待应用条目跳跃了（中间缺日志）：数据异常
            LOG_FATAL(
                "first index of committed entry[%lu] should <= "
                "progress.appliedIndex[%lu]+1",
                first, appliedIndex_);
        }
        // 只取 appliedIndex 之后的部分
        if (appliedIndex_ - first + 1 < entries.size()) {
            ents.insert(ents.end(), entries.begin() + appliedIndex_ - first + 1,
                        entries.end());
        }
    }

    // ------------------------------------------------------------------
    // MaybeTriggerSnapshot：检查是否该做快照了
    // 大白话: 已应用条数 - 上次快照条数 > 阈值时：
    //   1. 让状态机把整张 KV 表打包出来(异步)
    //   2. 生成 raft 快照（存储层 CreateSnapshot）
    //   3. 保存快照文件（SaveSnap）
    //   4. 压缩日志（Compact：只保留最近 N 条）
    // ------------------------------------------------------------------
    void MaybeTriggerSnapshot() {
        if (appliedIndex_ - snapshotIndex_ <= snapCount_) {
            return;  // 还没到阈值
        }

        LOG_DEBUG(
            "start snapshot [applied index: %lu | last snapshot index: "
            "%lu], "
            "snapshot count[%lu]",
            appliedIndex_, snapshotIndex_, snapCount_);

        // 异步取整张 KV 表（等状态机工作线程返回）
        std::promise<std::shared_ptr<SnapshotData>> promise;
        std::future<std::shared_ptr<SnapshotData>> future = promise.get_future();
        auto handler = [&promise](const std::shared_ptr<SnapshotData>& data) {
            promise.set_value(data);
        };
        // 
        stateMachine_->GetSnapshot(std::move(handler));
        future.wait();
        std::shared_ptr<SnapshotData> snapshotData = future.get();

        // 生成 raft 快照（记录索引/任期/集群配置/数据）
        std::shared_ptr<proto::Snapshot> snap;
        Status status = storage_->CreateSnapshot(appliedIndex_, confState_,
                                                 *snapshotData, snap);
        if (!status.IsOk()) {
            LOG_FATAL("create snapshot error %s", status.ToString().c_str());
        }

        // 保存快照文件
        status = SaveSnap(*snap);
        if (!status.IsOk()) {
            LOG_FATAL("save snapshot error %s", status.ToString().c_str());
        }

        // 压缩日志: 只保留最近 snapshotCatchUpEntries N 条
        uint64_t compactIndex = 1;
        if (appliedIndex_ > snapshotCatchUpEntriesN) {
            compactIndex = appliedIndex_ - snapshotCatchUpEntriesN;
        }
        // 删除掉 compactIndex 之前的日志（包括 compactIndex）
        status = storage_->Compact(compactIndex);
        if (!status.IsOk()) {
            LOG_FATAL("compact error %s", status.ToString().c_str());
        }
        LOG_INFO("compacted log at index %lu", compactIndex);
        snapshotIndex_ = appliedIndex_;  // 更新快照索引
    }

    // ------------------------------------------------------------------
    // ProcessMessage：收到其他节点发来的 raft 消息
    // 如果不在 raft 线程上，投递到 raft 线程（ioService_）执行；
    // 否则直接执行。执行后处理 Ready
    // ------------------------------------------------------------------
    void ProcessMessage(std::shared_ptr<proto::Message> msg) {
        if (pthreadId_ != pthread_self()) {
            auto cb = [this, msg]() {
                // 交给 node_ 计算处理;
                Status status = this->node_->Step(msg);
                // 拉取 Ready 事件（写盘/发消息/应用日志…）
                PullReadyEvents();
            };
            ioService_.post(cb);  // 投递到 raft 线程
        } else {
            Status status = this->node_->Step(msg);
            // 拉取 Ready 事件(写盘/发消息/应用日志…）
            PullReadyEvents();
        }
    }

    // 处理写提案（客户端 Set/Del）：投递到 raft 线程 → Propose → 处理 Ready
    void ProcessProposal(std::shared_ptr<std::vector<uint8_t>> data) {
        // 默认是 brpc 线程进来时，这个条件成立;
        // 如果当前线程不是 raft 线程，就投递到 raft 线程执行；否则直接执行;
        if (pthreadId_ != pthread_self()) {
            auto cb = [this, data]() {
                Status status = node_->Propose(data);
                PullReadyEvents();
            };
            ioService_.post(cb);
        } else {
            // todo 处理client put()
            Status status = node_->Propose(data);
            PullReadyEvents();
        }
    }

    // 处理线性一致读（ReadIndex）：投递到 raft 线程 → ReadIndex → 处理 Ready
    void ProcessReadIndex(std::shared_ptr<std::vector<uint8_t>> data) {
        if (pthreadId_ != pthread_self()) {
            auto cb = [this, data]() {
                Status status = node_->ReadIndex(std::move(data));
                PullReadyEvents();
            };
            ioService_.post(cb);
        } else {
            Status status = node_->ReadIndex(std::move(data));
            PullReadyEvents();
        }
    }

    // 处理弱读: 不经过 Raft，直接在本地状态机上执行读取
    // （包成一个普通 Entry 交给状态机，由状态机解析并回包）
    void ProcessWeakRead(std::shared_ptr<std::vector<uint8_t>> data) {
        if (pthreadId_ != pthread_self()) {
            auto cb = [this, data]() {
                std::shared_ptr<proto::Entry> entry =
                    std::make_shared<proto::Entry>();
                entry->set_type(proto::EntryType::EntryNormal);
                entry->set_index(appliedIndex_);
                entry->set_data(std::string(data->begin(), data->end()));
                stateMachine_->ApplyStateMachine(entry);
            };
            ioService_.post(cb);
        } else {
            std::shared_ptr<proto::Entry> entry = std::make_shared<proto::Entry>();
            entry->set_type(proto::EntryType::EntryNormal);
            entry->set_index(appliedIndex_);
            entry->set_data(std::string(data->begin(), data->end()));
            stateMachine_->ApplyStateMachine(entry);
        }
    }

    uint64_t NodeId() final { return id_; }

   private:
    // ------------------------------------------------------------------
    // StartTimer：启动定时器（每 100ms 触发一次）
    // 大白话: 这是整个系统的"心跳"——每个 tick 驱动 raft 时钟
    // （选举超时/心跳超时），然后处理 raft 产生的所有 Ready 任务
    // ------------------------------------------------------------------
    void StartTimer() {
        auto handler = [this](const boost::system::error_code& err) {
            if (err) {
                LOG_ERROR("timer waiter error %s", err.message().c_str());
                return;
            }
            // 先重设定时器（保证持续触发）
            this->StartTimer();
            // 推进 raft 时钟(100ms = 1 tick)
            this->node_->Tick();
            // 处理 raft 产生的任务 (写盘/发消息/应用日志…)
            this->PullReadyEvents();
        };
        
        // 100毫秒执行一次;
        timer_.expires_from_now(boost::posix_time::millisec(100));
        // 异步执行;
        timer_.async_wait(handler);
    }

    // ------------------------------------------------------------------
    // PullReadyEvents：核心循环——处理 raft 状态机产生的所有"待办事项"
    // （对应 Ready 机制，见 ready.h）
    // ------------------------------------------------------------------
    void PullReadyEvents() {
        assert(pthreadId_ == pthread_self());  // 必须在 raft 线程

        // 循环处理: raft 可能连续产生多个 Ready
        while (node_->HasReady()) {
            // 获取 Ready（包含硬状态/新日志/快照/消息/已提交日志/读结果）
            auto rd = node_->GetReady();

            // 判断是否 发生了 Ready 事件(有新日志/快照/消息/已提交日志/读结果)
            if (!rd->ContainsUpdates()) {
                LOG_WARN("ready not contains updates");
                return;
            }

            // ① 写 WAL: 硬状态, 新日志(持久化，掉电不丢)
            wal_->Save(rd->hardState, rd->entries);

            // ② 保存快照(如果有):
            //    写快照文件 → 应用到内存存储 → 应用快照数据;
            if (!rd->snapshot.metadata().index() == 0)  // 快照为空则跳过
            {
                Status status = SaveSnap(rd->snapshot);
                if (!status.IsOk()) {
                    LOG_FATAL("save snapshot error %s",status.ToString().c_str());
                }
                // 覆盖快照对象
                // entries[0] = snap, 这里的 snap 是 raft 快照对象;
                storage_->ApplySnapshot(rd->snapshot);
                // 让状态机用快照数据恢复 KV 表;
                PublishSnapshot(rd->snapshot);
            }

            // ③ 新日志追加进内存存储(raft 读日志时要用)
            if (!rd->entries.empty()) {
                storage_->Append(rd->entries);
            }

            // ④ 发送消息给其他节点（投票/追加日志/心跳…）
            if (!rd->messages.empty()) {
                // 异步发送：Send 内部走 brpc 异步回调，不会阻塞本（raft）线程
                transport_->Send(rd->messages);
            }

            // ⑤ 应用已提交的日志（真正执行客户端请求）
            if (!rd->committedEntries.empty()) {
                std::vector<std::shared_ptr<proto::Entry>> ents;
                // 过滤掉已经应用过的
                EntriesToApply(rd->committedEntries, ents);
                if (!ents.empty()) {
                    // 应用一批已提交的日志条目（真正的"执行"）, 并回答客户端的请求;
                    PublishEntries(ents);
                }
            }

            // ⑥ 处理线性一致读结果（安全水位到了就执行读取）
            if (!rd->readStates.empty()) {
                // 
                PublishReadStates(rd->readStates);
            }

            // ⑦ 检查是否触发快照
            MaybeTriggerSnapshot();

            // ⑧ 通知 raft "处理完了，继续";
            node_->Advance(rd);
        }
    }

    // 保存快照：先写"快照标记"进 WAL（记录快照索引），再写快照文件。
    // 顺序很重要：保证重启时能以"已保存的快照"为起点打开 WAL;
    Status SaveSnap(const proto::Snapshot& snap) {
        Status status;

        // 把快照的索引/任期写进 WAL（作为恢复原点标记）
        proto::WALsnapshot walSnapshot;
        walSnapshot.set_index(snap.metadata().index());
        walSnapshot.set_term(snap.metadata().term());

        status = wal_->SaveSnapshot(walSnapshot);
        if (!status.IsOk()) {
            return status;
        }
        // 保存快照文件
        status = snapshotFeature_->SaveSnap(snap);
        if (!status.IsOk()) {
            LOG_FATAL("save snapshot error %s", status.ToString().c_str());
        }
        return status;

        // 释放旧 WAL（本项目的简化实现为空操作，这行实际执行不到）
        return wal_->ReleaseTo(snap.metadata().index());
    }

    // 应用一个快照(follower 从 leader 收到快照时);
    // 更新 confState/snapshotIndex/appliedIndex，让状态机恢复数据
    void PublishSnapshot(const proto::Snapshot& snap) {
        if (IsEmptySnapshot(snap)) {
            // 空快照, 忽略;
            return;
        }

        LOG_DEBUG("publishing snapshot at index %lu", snapshotIndex_);

        if (snap.metadata().index() <= appliedIndex_) {
            // 快照比已应用的还旧：数据异常
            LOG_FATAL(
                "snapshot index [%lu] should > "
                "progress.appliedIndex [%lu] + 1",
                snap.metadata().index(), appliedIndex_);
        }

        // 更新索引
        *(this->confState_) = snap.metadata().confstate();
        snapshotIndex_ = snap.metadata().index();
        appliedIndex_ = snap.metadata().index();

        // 让状态机用快照数据恢复 KV 表;
        stateMachine_->RecoverFromSnapshot(snap);
    }

    // 处理线性一致读结果：
    // 把 readStates 转成 Entry 交给状态机；如果本地 appliedIndex 还没追上
    // readIndex（安全水位），就标记为"警告读"（拒绝执行，避免读到旧数据）
    void PublishReadStates(const std::vector<ReadState>& readStates) {

        for (int i = 0; i < readStates.size(); i++) {
            std::shared_ptr<proto::Entry> entry = std::make_shared<proto::Entry>();
            uint64_t readIndex = readStates[i].index;

            entry->set_index(readIndex);
            entry->set_data(std::string(std::begin(readStates[i].requestCtx),
                                        std::end(readStates[i].requestCtx)));
            // 判断每个entry的readIndex是否小于appliedIndex;
            if (appliedIndex_ >= readIndex) {
                // 已应用超过安全水位：可以安全读取
                entry->set_type(proto::EntryType::EntryNormal);
            } else {
                // 还没追上: 拒绝本次读（否则会读到旧数据）,
                entry->set_type(proto::EntryType::EntryWarningReadIndex);
                entry->set_message(
                    "appliedIndex is less than readIndex, "
                    "reject "
                    "readOnlyRequest!!!");
            }
            // 
            stateMachine_->ApplyStateMachine(entry);
        }
    }

    // ------------------------------------------------------------------
    // ReplayWAL：重启恢复的核心
    // 流程：
    //   1. 加载最新快照（有的话）→ 应用到内存存储
    //   2. 打开 WAL（只保留快照之后的文件）
    //   3. 读取全部记录：恢复硬状态（term/vote/commit）+ 日志条目
    //   4. 把硬状态/日志填回内存存储，让 raft 从正确位置继续
    // ------------------------------------------------------------------
    void ReplayWAL() {
        LOG_DEBUG("replaying WAL of member %lu", id_);
        proto::Snapshot snapshot;
        // 加载最新快照（找不到也没关系，说明是全新节点）
        Status status = snapshotFeature_->Load(snapshot);
        if (!status.IsOk()) {
            if (status.IsNotFound()) {
                LOG_INFO("snapshot not found for node %lu", id_);
            } else {
                LOG_FATAL("error loading snapshot %s",
                          status.ToString().c_str());
            }
        } else {
            // 存储层: 把快照信息装到 entry_[0] 的位置上
            // (哑条目的 index 变成快照索引, 表示之前的日志都不需要了)
            storage_->ApplySnapshot(snapshot);
        }

        // 根据快照来筛选哪些 WAL 可恢复(只开快照之后的文件)
        OpenWAL(snapshot);
        assert(wal_ != nullptr);

        // 硬状态: term/vote/commit
        proto::HardState hs;
        std::vector<std::shared_ptr<proto::Entry>> ents;
        // 进行磁盘 -> 内存恢复（1.日志条目 2.硬状态）
        status = wal_->ReadAll(hs, ents);
        if (!status.IsOk()) {
            LOG_FATAL("failed to read WAL %s", status.ToString().c_str());
        }

        // 恢复之前保存在 WAL 中的硬状态
        storage_->SetHardState(hs);

        // 追加日志到存储, 让 Raft 从日志中的正确位置启动;
        storage_->Append(ents);

        if (!ents.empty()) {
            // 记录 WAL 重放的最后索引（恢复完成标记用）
            lastIndex_ = ents.back()->index();
        } else {
            // WAL 里没有日志可恢复时，直接把快照数据填进内存
            // （稍后状态机启动时用它初始化 DB）
            snapData_.assign(snapshot.data().begin(), snapshot.data().end());
        }
    }

    // OpenWAL：打开 WAL（目录不存在则创建全新 WAL）
    void OpenWAL(const proto::Snapshot& snap) {
        // 不存在快照文件;
        if (!boost::filesystem::exists(walDir_)) {
            boost::filesystem::create_directories(walDir_);
            // 全新 WAL: 写一条"第 0 任期、第 0 索引"的快照标记当原点
            WALFeature::Create(walDir_);
        }

        // 把快照信息复制给 walsnap（恢复起点）
        proto::WALsnapshot walsnap;
        walsnap.set_index(snap.metadata().index());
        walsnap.set_term(snap.metadata().term());

        LOG_INFO("loading WAL at term %lu and index %lu", walsnap.term(),
                 walsnap.index());
        // 打开 WAL, 并只保留快照之后的 WAL 文件，以便恢复
        wal_ = WALFeature::Open(walDir_, walsnap);
    }
};

/*********************************分割线*********************************/
// 信号处理：收到 Ctrl+C 等信号时优雅停止节点
void RaftNode::SignalHandler(int) {
    LOG_INFO("catch signal");
    if (gNode) {
        gNode->Stop();
    }
}

// 节点进程入口（被 raft-kv.cpp 的 main 调用）：
// 注册信号 → 创建节点 → 启动（阻塞）
void RaftNode::Main(uint64_t id, std::string originDataDir,
                    const std::vector<std::string>& cluster) {
    ::signal(SIGINT, SignalHandler);
    ::signal(SIGHUP, SignalHandler);
    // 也要接管 SIGTERM（docker stop / kill / pkill 默认发的就是它）。
    // 不接管的话进程会被内核直接杀死，WAL 的内存缓冲区来不及刷盘，
    // 重启时就会读到损坏的 WAL 尾部（invalid record）
    ::signal(SIGTERM, SignalHandler);

    // 创建节点实例（构造函数里完成全部初始化）
    gNode = std::make_shared<RaftNodeImpl>(id, originDataDir, cluster);

    // 启动（进入事件循环，阻塞）
    gNode->Start();
}

}  // namespace kv
