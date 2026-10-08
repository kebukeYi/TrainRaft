# 分布式 KV 系统 · 源码导读（大白话版）

> 这份文档写给"第一次看这个项目"的人。源码里已经逐文件加了注释，
> 这份文档负责把**整体运作流程**串起来。建议顺序：先读本导读，
> 再按目录顺序看代码，看不懂的函数回来看导读里的流程图。

---

## 一、这个项目是什么？

一句话：**用 Raft 算法实现的、多副本强一致的 KV 存储**（仿 etcd 的架构）。

- 一个集群有 N 个节点（默认 3 个），每个节点都存着**完整的一份数据**。
- 客户端向任意节点发起 `Set / Get / Del` 请求。
- 所有节点通过 Raft 算法**保持一致**：写请求必须经过"多数节点确认"才算成功。
- 就算有节点挂了，只要还有多数派活着，集群就能继续服务。

关键技术栈：C++11、brpc（RPC 通信）、RocksDB（KV 存储）、
protobuf（消息序列化）、Zookeeper（leader 服务发现）、boost（定时器/文件操作）。

---

## 二、目录结构（先记住这几层）

```
kv-system/
├── CMakeLists.txt              # 编译配置（源码按层分组）
├── resource/                   # protobuf 定义（三种"协议"）
│   ├── cli.proto               #   ① 客户端 ↔ 节点 的 RPC 接口
│   ├── raft.proto              #   ② 节点 ↔ 节点 的 Raft 消息
│   └── store.proto             #   ③ 快照/存储的格式
├── common/                     # 工具层：Status 错误码、Slice、ByteBuffer、日志、CRC32
├── raft/                       # ★ Raft 算法层（纯算法，不碰磁盘不碰网络）
│   ├── raft.h/cpp              #   Raft 状态机核心（选举/复制/提交）
│   ├── raft_log.h/cpp          #   日志管理（稳定区+不稳定区）
│   ├── unstable.h/cpp          #   未落盘的日志
│   ├── storage.h/cpp           #   已落盘日志（MemoryStorage，内存版）
│   ├── progress.h/cpp          #   leader 眼里每个 follower 的复制进度
│   ├── ready.h/cpp             #   Ready：raft 交给上层的"待办任务包"
│   ├── readonly.h/cpp          #   线性一致读（ReadIndex）的登记表
│   ├── node.h/cpp              #   Node：raft 对外的接口壳
│   ├── config.h/cpp            #   配置项（心跳/选举超时/大小限制…）
│   ├── util.h/cpp              #   小工具函数
│   └── zk_client.h/cpp         #   Zookeeper 客户端（leader 注册服务）
└── apply/                      # ★ 应用层（把 Raft 变成能用的 KV 服务）
    ├── raft-node/raft_node.*   #   门面：把下面所有模块组装起来，开事件循环
    ├── raft-state-machine/*    #   状态机：真正执行日志（Set/Get/Del → RocksDB）
    ├── service/*               #   brpc 服务：客户端接口 + 节点间消息接口
    ├── transport/*             #   节点间通信（brpc channel）
    └── store/                  #   存储：RocksDB 引擎 + WAL 日志 + 快照
```

**最重要的心智模型**：`raft/` 是"大脑"（纯计算），`apply/` 是"手脚"
（磁盘、网络、数据库）。大脑算一步，手脚干一步，Ready 就是传话的信封。

---

## 三、核心机制：Ready 循环（整个系统的发动机）

Raft (node)状态机**不主动干活**，它只"算"，算完把活打包进 Ready，交给上层：

```
           每 100ms 定时器
                 │
                 ▼
        ┌─────────────────┐
        │ node_->Tick()   │  ← 推进时钟（选举超时/心跳超时）
        └────────┬────────┘
                 ▼
        ┌──────────────────────────┐
        │ PullReadyEvents()        │  ← apply/raft-node/raft_node.cpp 的核心循环
        │  while (node_->HasReady())│
        │    rd = node_->GetReady()│  ← 拿到一袋任务
        │    ① wal_->Save(硬状态, 新日志)   → 写盘（掉电不丢）
        │    ② 保存快照（如果有）
        │    ③ storage_->Append(新日志)    → 进内存存储
        │    ④ transport_->Send(消息)      → 发给其他节点
        │    ⑤ PublishEntries(已提交日志)  → 真正执行客户端请求
        │    ⑥ PublishReadStates(读结果)   → 处理读
        │    ⑦ MaybeTriggerSnapshot()      → 日志多了做快照
        │    ⑧ node_->Advance(rd)          → 告诉 raft"干完了，继续"
        └──────────────────────────┘
```

**对应代码**：`raft_node.cpp` 的 `PullReadyEvents()`。
看懂这个循环，就看懂了整个系统一半。

---

## 四、一条写请求（Set）的完整旅程 ★★★★★

假设客户端连上节点 1（无论连哪个节点，流程都相同）：

```
客户端
  │  brpc: Set("name", "张三")
  ▼
ClientServiceImpl::Set()                    [apply/service/client_service.cpp]
  │  ① 生成 requestid，把请求包成 RaftEntryData 并序列化
  │  ② 把 {done回调, response指针} 登记进 pendingRequests_
  │  ③ 调 raft_->ProcessProposal(字节数据)
  ▼
RaftNodeImpl::ProcessProposal()             [apply/raft-node/raft_node.cpp]
  │  投递到 raft 线程 → node_->Propose(data) → PullReadyEvents()
  ▼
NodeImpl::Propose() → Raft::Step(MsgProp)   [raft/node.cpp → raft/raft.cpp]
  │
  ├─ 如果本节点是 FOLLOWER：把提案转发给 leader（StepFollower）
  ├─ 如果本节点是 CANDIDATE：丢弃提案（没有 leader 可转发）
  └─ 如果本节点是 LEADER：★走下面的核心路径
        │
        ▼
Raft::StepLeader(MsgProp)                   [raft/raft.cpp]
  │  AppendEntry(条目)：编上 index/term → 写进 unstable（还没落盘）
  │  BcastAppend()：给所有 follower 发 MsgApp（追加日志）
  ▼
Follower 收到 MsgApp                        [raft/raft.cpp HandleAppendEntries]
  │  MaybeAppend()：先对账（prevLogIndex/prevLogTerm 是否一致）
  │  一致 → 收下日志（unstable）→ 回 MsgAppResp(ok)
  │  不一致 → 回 MsgAppResp(reject + 自己日志最后索引)
  ▼
Leader 收到 MsgAppResp                      [raft/raft.cpp StepLeader]
  │  更新这个 follower 的进度（Progress）
  │  MaybeCommit()：把所有人 match 排序，取第 Quorum 大的 → 提交！
  │  提交成功 → BcastAppend() 广播最新 commit
  ▼
各节点 PullReadyEvents()                    [apply/raft-node/raft_node.cpp]
  │  ① wal_->Save(...)          → 新日志写 WAL（此时才真正落盘！）
  │  ⑤ PublishEntries(已提交)   → stateMachine_->ApplyStateMachine(entry)
  ▼
StateMachine::ApplyStateMachine()           [apply/raft-state-machine/state_machine.cpp]
  │  ① 解析出 RaftEntryData（原来是 Set 请求）
  │  ② 只有"当初接请求的节点"（nodeid 匹配）需要回复客户端
  │  ③ db_->Set(key, value)     → 写 RocksDB
  │  ④ pendingRequests_[requestid].done->Run() → 回包给客户端
  ▼
客户端收到 SetResponse("ok")   ★写请求完成
```

**关键点**：
- 客户端请求先变成**日志**，日志被**多数节点确认**后算"提交"，提交后才**执行**（写 DB）。
- 执行顺序在所有节点上**完全一致**（日志顺序 = 执行顺序），这就是一致性。
- "写盘"发生在执行之前：WAL 保证掉电后日志还在，能恢复。

---

## 五、一条读请求（Get）的完整旅程

本项目 `ClientServiceImpl::Get` 里编译时固定走 **WeakConsistency（弱读）**，
所以实际跑起来是"直接读本地 DB"。三种模式都写在代码里，改 switch 就能切换：

| 模式 | 流程 | 一致性 | 性能 |
|---|---|---|---|
| WeakConsistency（当前默认） | 直接读本地 RocksDB | 弱（可能读到旧数据） | 最快 |
| ReadIndex | 问 leader 要安全水位 → 等 applied 追上 → 读 | 强（线性一致） | 较快 |
| StrongConsistency | 读请求也走一遍 Raft 日志 | 强 | 最慢 |

**ReadIndex 模式的详细流程**（raft/readonly.cpp + raft.cpp 的 MsgReadIndex 处理）：

```
客户端 Get 请求
  → ProcessReadIndex → node_->ReadIndex(ctx)
  → Raft::StepLeader(MsgReadIndex)
     ① readOnly_->AddRequest(commit索引, 请求)   ← 记下"安全水位"
     ② BcastHeartbeatWithCtx(请求标识)            ← 心跳里带上请求 ID
  → follower 回 MsgHeartbeatResp(带同一个请求标识) ← 相当于确认"commit 没变"
  → leader 数确认数，够 Quorum 后：
     readOnly_->Advance() → 解锁该请求及之前的所有请求
  → 本地请求：放进 readStates_；远程请求：回 MsgReadIndexResp
  → PullReadyEvents 里 PublishReadStates()
     appliedIndex >= 安全水位 → 执行读取并回包
     appliedIndex <  安全水位 → 拒绝（EntryWarningReadIndex，"还没追上"）
```

---

## 六、选举流程（leader 挂了怎么办）

```
Follower 每 tick 检查：electionElapsed >= 随机化选举超时？
  │  是 → TickElection → 发 MsgHup（本地消息"开始选举"）
  ▼
Raft::Step(MsgHup)
  │  检查没有待应用的配置变更 → Campaign()
  ▼
Campaign()  [raft/raft.cpp]
  │  preVote 开启 → 先 BecomePreCandidate，发 MsgPreVote（预投票）
  │  预投票通过（多数派同意）→ 再 BecomeCandidate，发 MsgVote
  ▼
其他节点收到 MsgVote  [Raft::Step 的 MsgVote 分支]
  │  判断条件：
  │    ① 我还没投过票 且 不知道有 leader
  │    ② 候选人的日志比我新（term 大 或 term 相同 index 大）→ 才投
  │  同意 → 回 MsgVoteResp(赞成)；否则回(拒绝)
  ▼
Candidate 收票  [StepCandidate]
  │  Poll() 计票
  │  赞成 >= Quorum → BecomeLeader()！
  │  拒绝 >= Quorum → 变回 Follower（竞选失败）
  ▼
BecomeLeader()  [raft/raft.cpp]
  │  ① 追加一条"空日志"（本任期的日志，为了能提交）
  │  ② BcastAppend() 广播空日志
  │  ③ 用 zk 注册服务（把集群地址写进 /raft 节点，客户端可发现 leader）
  │  ④ 开始定期广播心跳维持地位
```

**关键点**：
- 选举超时是**随机化**的（`[electionTick, 2*electionTick)`），防止所有节点同时
  超时、票数互相瓜分导致选不出来。
- **日志新的人才能当选**：防止日志旧的节点当选后"覆盖"已提交日志。
- pre-vote：分区出去的节点重新联网时，先探路不捣乱。

---

## 七、日志复制、冲突与提交

日志是 Raft 的全部。本项目日志结构（raft/raft_log.h）：

```
storage_（已落盘，MemoryStorage）        unstable_（未落盘，刚产生的）
┌──────────────┬──────────────┬──────────┬──────────────┐
│  ... 已落盘日志 │ 正在落盘的日志 │ 边界      │  新日志(未落盘) │
└──────────────┴──────────────┴──────────┴──────────────┘
committed_（已提交：多数派确认）   applied_（已应用：上层执行过）
```

- **提交规则**（raft.cpp MaybeCommit / raft_log.cpp MaybeCommit）：
  把所有人 match 排序取第 Quorum 大，且那条日志的**任期必须等于当前任期**。
  新 leader 的空日志就是用来"打通"提交的。
- **冲突处理**（follower 拒绝 append）：leader 根据 rejecthint 回退 next_，
  重新发；双方日志最终在"第一个冲突点"之前对齐（Raft 日志匹配原则）。
- **只提交当前任期日志**：旧任期日志即使复制了多数也不能直接提交，
  必须等一条当前任期日志提交后"捎带"提交（安全性保证）。

---

## 八、快照机制（日志无限增长怎么办）

```
已应用条数 - 上次快照条数 > 100000（snapCount_）？
  │  是 → RaftNodeImpl::MaybeTriggerSnapshot()
  ▼
① stateMachine_->GetSnapshot()：把整张 RocksDB 表打包（msgpack）
② storage_->CreateSnapshot()：生成 raft 快照（记录索引/任期/成员/数据）
③ SaveSnap()：先写 WAL 快照标记，再写快照文件 <term>-<index>.snap
④ storage_->Compact()：压缩日志，只留最近 100000 条
```

**follower 日志落后太多**（要的日志已经被压缩了）：
leader 的 MaybeSendAppend 取日志失败 → 改发 **MsgSnap 快照** →
follower 收到后 Restore() 恢复，再用快照数据重建 DB（RecoverFromSnapshot）。

---

## 九、重启恢复（WAL + 快照）

```
进程启动 → RaftNodeImpl 构造函数 → ReplayWAL()  [apply/raft-node/raft_node.cpp]
  │  ① 加载最新快照文件（如果有）→ storage_->ApplySnapshot()
  │  ② OpenWAL()：只打开"快照索引之后"的 WAL 文件
  │  ③ wal_->ReadAll()：读出硬状态（term/vote/commit）+ 日志条目
  │  ④ storage_->SetHardState(hs) + storage_->Append(entries)
  ▼
创建 Node（RestartNode）：从 storage 恢复成员表/任期/日志位置
  ▼
Start()：恢复快照信息 → 启动状态机（客户端 RPC + RocksDB）
         → 启动定时器 → 进入事件循环
```

**为什么要有 WAL**：Raft 要求"先写盘再回包"。WAL 是"日志的日志"——
内存日志（MemoryStorage）重启就没了，WAL 负责把每条新日志落盘。
断电后重启：快照（已执行的状态）+ WAL（未执行/未提交的日志）就能完整恢复。

---

## 十、一条消息从节点 A 到节点 B（传输层）

```
raft 状态机算出消息 → 放进 msgs_ → 打包进 Ready
  ▼
PullReadyEvents → transport_->Send(msgs)      [apply/transport/transport.cpp]
  │  按消息的 to 字段找 brpc channel → 调对端的 MessageChannel 接口
  ▼
节点 B 的 TransportServiceImpl::MessageChannel  [apply/service/transport_service.cpp]
  │  收到消息 → raft_->ProcessMessage(msg) → 投递到 B 的 raft 线程
  ▼
节点 B: node_->Step(msg) → raft 状态机处理（投票/追加日志/心跳…）
```

每个节点同时是"server"（收消息）和"client"（发消息），
端口约定：`地址 = "ip:传输端口:客户端端口"`。

---

## 十一、Raft 术语 ↔ 代码对照表

| Raft 术语 | 代码位置 |
|---|---|
| 任期 term | `Raft::term_`，消息的 `term` 字段 |
| 日志条目 Entry | `proto::Entry`（term + index + data） |
| 提交索引 commit | `RaftLog::committed_` |
| 应用索引 applied | `RaftLog::applied_` |
| 选举超时/心跳超时 | `Config::electionTick/heartbeatTick` |
| 角色 Follower/Candidate/Leader | `Raft::state_` |
| RequestVote RPC | `MsgVote / MsgVoteResp` |
| AppendEntries RPC | `MsgApp / MsgAppResp` |
| Heartbeat RPC | `MsgHeartbeat / MsgHeartbeatResp` |
| 快照 InstallSnapshot | `MsgSnap` + `HandleSnapshot` |
| 领导者变更 leader election | `Campaign()` / `BecomeLeader()` |
| 日志匹配 Log Matching | `raft_log.cpp MaybeAppend/MatchTerm` |
| 领导者选举限制 | `IsUpToDate()`（日志新才能当选） |
| 多数派 Quorum | `Raft::Quorum()` = n/2+1 |
| 预投票 Pre-Vote | `MsgPreVote/MsgPreVoteResp` |
| 领导权转移 | `MsgTransferLeader/MsgTimeoutNow` |
| 线性一致读 ReadIndex | `MsgReadIndex` + `ReadOnly` 类 |
| Ready（交给上层的任务包） | `struct Ready` + `PullReadyEvents` |

---

## 十二、建议阅读顺序

1. `resource/*.proto`（三个协议，先知道"聊什么"）
2. `apply/raft-node/raft_node.cpp` 的 `PullReadyEvents`（整个系统的发动机）
3. `raft/raft.cpp` 的 `Step` + `StepLeader`（算法核心入口）
4. `raft/raft_log.cpp` + `unstable.cpp` + `storage.cpp`（日志结构）
5. `raft/ready.cpp` + `node.cpp`（状态机与上层的接口）
6. `apply/raft-state-machine/state_machine.cpp`（请求如何真正执行）
7. 剩下的按需看（progress、readonly、WAL、快照…）

祝你读代码愉快！🚀
