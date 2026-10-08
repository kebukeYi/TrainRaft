#include <raft/config.h>

namespace kv {

// 校验配置合法性的具体规则：
// 不合法的配置会导致 Raft 跑不起来或行为异常，所以启动前必须先过这一关
Status Config::Validate() {
    // id 不能是 0（0 在 Raft 里表示"未知/无"）
    if (this->id == 0) {
        return Status::InvalidArgument("cannot use none as id");
    }

    // 心跳间隔必须 > 0
    if (this->heartbeatTick <= 0) {
        return Status::InvalidArgument("heartbeat tick must be greater than 0");
    }

    // 选举超时 必须 大于 心跳间隔。
    // 大白话：如果选举超时 <= 心跳间隔，leader 还没发心跳，
    // follower 就超时了，集群会陷入"永远在选举"的混乱
    if (this->electionTick <= this->heartbeatTick) {
        return Status::InvalidArgument(
            "election tick must be greater than heartbeat tick");
    }

    // 存储不能为空（Raft 必须有地方读历史日志）
    if (!this->storage) {
        return Status::InvalidArgument("storage cannot be nil");
    }

    // maxUncommittedEntriesSize == 0 时自动改成无限大：
    // 大白话：0 本来是"不限制"的意思，但实现里判断用的是"是否超过上限"，
    // 所以把 0 替换成 uint64 最大值，等于放开限制
    if (this->maxUncommittedEntriesSize == 0) {
        this->maxUncommittedEntriesSize = std::numeric_limits<uint64_t>::max();
    }

    // maxCommittedSizePerReady == 0 时默认取 maxSizePerMsg 的值
    // （历史版本这俩是同一个参数，为了兼容保持默认一致）
    if (this->maxCommittedSizePerReady == 0) {
        maxCommittedSizePerReady = this->maxSizePerMsg;
    }

    // 在途消息上限必须 > 0（滑动窗口不能是 0）
    if (this->maxInflightMsgs <= 0) {
        return Status::InvalidArgument(
            "max inflight messages must be greater than 0");
    }

    // 租约模式读（ReadOnlyLeaseBased）必须配合 checkQuorum 使用：
    // 因为租约的前提是"leader 知道多数派还活着"，不开 checkQuorum 就无从判断
    if (this->readOnlyOption == ReadOnlyLeaseBased && !this->checkQuorum) {
        return Status::InvalidArgument(
            "checkQuorum must be enabled when readOnlyOption is "
            "ReadOnlyLeaseBased");
    }

    return Status::Ok();
}

// 单例：第一次调用时 new 一个 Config，之后都返回同一个（带锁保证线程安全）
Config& Config::GetInstance() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!instance_) {
        // 三段式构造：先 new 出对象，再调用构造函数初始化，保证线程安全;
        instance_ = new Config();
    }
    return *instance_;
}

// 静态成员的定义（声明在头文件里，定义在这里，否则链接报错）
std::mutex Config::mutex_;
Config* Config::instance_ = nullptr;

}  // namespace kv
