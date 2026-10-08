#pragma once
#include <raft/storage.h>

#include <cstdint>
#include <mutex>
#include <vector>

namespace kv {

// 只读请求（线性一致读）的两种处理模式
enum ReadOnlyOption {
    // ReadOnlySafe：安全模式（默认、推荐）。
    // 大白话：leader 收到读请求后不马上读，先把自己的 commit 索引记下来，
    // 然后广播带标识的心跳，等大多数节点都确认后，才认为"我读到的一定是
    // 最新数据"，再执行读取。多一轮通信，但绝对正确。
    ReadOnlySafe = 0,

    // ReadOnlyLeaseBased：租约模式（省一轮通信，但有风险）。
    // 大白话：leader 只要还在选举超时内收到过大多数节点的消息，就认为自己
    // 的"租约"没过期，直接读本地数据。缺点：如果系统时钟漂移/机器暂停，
    // 租约可能被错误延长，读到旧数据。
    ReadOnlyLeaseBased = 1
};

// ============================================================================
// Config：Raft 节点的配置项（创建 Raft 时一次性传入，之后基本不改）
//
// 大白话：相当于 Raft 的"出生参数"：我是谁、心跳多久、选举超时多久、
// 日志多大、要不要 pre-vote 等等。RaftNodeImpl 构造时会把所有参数填好，
// 校验通过（Validate）后才创建 Raft 状态机。
// ============================================================================
class Config {
   public:
    explicit Config()
        : id(0),
          electionTick(0),
          heartbeatTick(0),
          applied(0),
          maxSizePerMsg(0),
          maxCommittedSizePerReady(0),
          maxUncommittedEntriesSize(0),
          maxInflightMsgs(0),
          checkQuorum(false),
          preVote(false),
          readOnlyOption(ReadOnlySafe),
          disableProposalForwarding(false),
          isCacheOpen(true) {}
    ~Config();  // 如果需要清理资源，可以在这里定义析构函数

    // id 是本地 raft 的身份标识（节点编号，从 1 开始，不能为 0）
    uint64_t id;
    // 集群信息：每个节点的地址字符串列表
    // 格式："ip:传输端口:客户端端口"，例如 "127.0.0.1:8100:8200"
    std::vector<std::string> clusterInfo;

    // peers 包含 raft 集群中所有节点（包括自身）的 ID。
    // 它应该只在启动一个新的 raft 集群时设置（StartNode 时用）。
    std::vector<uint64_t> peers;

    // electionTick：选举超时的"时钟数"。
    // 大白话：节点内部有个时钟在走（每 100ms tick 一下），follower 如果
    // 连续 electionTick 次都没收到 leader 的消息，就认为自己被抛弃了，
    // 变成 candidate 发起选举。必须 > heartbeatTick，一般建议 10 倍。
    uint32_t electionTick;

    // heartbeatTick：心跳间隔的"时钟数"。
    // 大白话：leader 每 heartbeatTick 次 tick 就向所有 follower 发一次
    // 心跳，告诉它们"我还活着"。
    uint32_t heartbeatTick;

    // storage 是 raft 的持久化存储接口（本项目中是 MemoryStorage，
    // 真正落盘由上层 WAL 负责）。raft 生成要保存的日志条目和状态，
    // 需要读历史日志时也从这里拿。
    StoragePtr storage;

    // applied 是"已应用到状态机"的日志索引，只在重启时设置：
    // raft 不会返回小于等于它的条目给上层（避免重复应用）。
    uint64_t applied;

    // maxSizePerMsg：单条追加消息（MsgApp）最多携带多少字节的日志。
    // 越小恢复越稳但吞吐越低；本项目配 1MB。
    uint64_t maxSizePerMsg;

    // maxCommittedSizePerReady：一次 Ready 最多返回多少字节的已提交条目。
    // 大白话：防止一次应用太多日志导致上层卡顿，多了分批给。
    uint64_t maxCommittedSizePerReady;

    // maxUncommittedEntriesSize：leader 日志里"未提交条目"的累计大小上限。
    // 超过后新的写提案直接拒绝（防止日志无限膨胀）。0 表示不限制。
    uint64_t maxUncommittedEntriesSize;

    // maxInflightMsgs：乐观复制阶段允许"在途未确认"的追加消息最大条数。
    // 大白话：leader 一次性可以"不等待确认就连续发"多少条日志给 follower，
    // 限制它，防止把网络/对端缓冲区打爆。本项目 256。
    uint64_t maxInflightMsgs;

    // checkQuorum：leader 是否定期检查"多数派是否还活着"。
    // 大白话：如果开了，leader 在选举超时内没收到多数节点的消息，
    // 就主动下台（让位），避免"伪 leader 占着茅坑"。
    bool checkQuorum;

    // preVote：是否启用 Raft 论文 9.6 节的"预投票"机制。
    // 大白话：正式选举前先发一轮"预投票"，如果发现集群里已经有活着的
    // leader（任期比我高），就老老实实回去当 follower，不捣乱。
    bool preVote;

    // readOnlyOption：只读请求怎么处理（见上面 ReadOnlyOption 说明）
    ReadOnlyOption readOnlyOption;

    // disableProposalForwarding：follower 收到客户端写请求时，默认会把
    // 请求转发给 leader；开了这个开关就丢弃。本项目不开（允许转发）
    bool disableProposalForwarding;

    // 是否打开缓存
    bool isCacheOpen;

   public:
    // 静态工厂：拿到全局唯一的 Config 实例（单例，全进程共用一份配置）
    static Config& GetInstance();

    // 获取只读请求的选项配置
    ReadOnlyOption getReadOnlyOption() const { return readOnlyOption; }

    // 设置只读选项
    void setReadOnlyOption(ReadOnlyOption option) { readOnlyOption = option; }

    // 其他getter和setter方法...

    // 校验配置是否合法（id 不能为 0、选举超时必须大于心跳超时等）。
    // RaftNodeImpl 构造时会调用，不合法直接 FATAL 退出
    Status Validate();

   private:
    static Config* instance_;  // 单例指针
    static std::mutex mutex_;  // 互斥锁，保证单例线程安全
};
}  // namespace kv
