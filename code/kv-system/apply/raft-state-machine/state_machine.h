#ifndef STATE_MACHINE_H
#define STATE_MACHINE_H

#include <brpc/channel.h>
#include <brpc/server.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>

#include <memory>

#include "common/log.h"
#include "resource/cli.pb.h"
#include "service/client_service.h"
#include "transport/transport.h"

using namespace proto;
namespace kv {

class RaftNode;  // 加上前置声明！
using SnapshotData = std::vector<uint8_t>;  // 快照数据的字节容器
using OnGetSnapshot = std::function<void(std::shared_ptr<SnapshotData>)>;  // 取快照的回调
class ClientServiceImpl;

// ============================================================================
// StateMachine：应用层的"状态机"——真正执行 Raft 提交的日志
//
// 大白话：Raft 提交日志后，日志内容（一个客户端请求）最终要落到 KV 库。
// 这个类干三件事：
//   1. 跑一个 brpc server，提供面向客户端的服务（ClientServiceImpl）
//   2. 执行已提交的日志：把 RaftEntryData 解析出来，Set/Get/Del 到 RocksDB，
//      然后找到当初的 RPC 请求回包给客户端（ApplyStateMachine）
//   3. 提供快照能力：把整张 KV 表打包成字节（msgpack 序列化），
//      供上层生成 Raft 快照；也支持从快照恢复整张表
// 注意：StateMachine 内部有一个独立的工作线程（worker_），
// 应用日志的操作会投递到这个线程上执行（ioService_.post）
// ============================================================================
class StateMachine {
       private:
        RaftNode* raft_;                        // 门面（回查请求信息用）
        std::shared_ptr<KVEngine> db_;          // RocksDB KV 引擎
        std::thread worker_;                    // 工作线程（跑 ioService_）
        brpc::Server rpcServer_;                // 客户端 RPC 服务
        boost::asio::io_service ioService_;     // 工作线程的事件循环
        // 事件循环的 keep-alive 守卫：ioService_.run() 在"没有任务"时会立即返回，
        // 有了它 run() 才会一直阻塞、等待后面投递进来的任务。
        // （没有它的话，工作线程会在启动瞬间就退出，导致 GetSnapshot /
        //   RecoverFromSnapshot 投递的回调永远没人执行 → future.wait() 死锁）
        std::unique_ptr<boost::asio::io_service::work> work_;
        ClientServiceImpl* service_;            // 客户端服务实现

       public:
        // 构造：启动客户端 brpc server + 用快照数据初始化 DB
        StateMachine(RaftNode* raft, SnapshotData snap, std::string address, std::string dbDir);

        ~StateMachine();

        // 启动工作线程（把 promise 传出去，让调用方拿到线程 id）
        void Start(std::promise<pthread_t>& promise);

        void Stop();

        // KV 表 -> 字节数组（msgpack 打包，做快照用）
        std::shared_ptr<SnapshotData> SnapDataSerializeToArray(
            std::unordered_map<std::string, std::string> kvData);

        // 字节数组 -> KV 表（msgpack 解包，恢复快照用）
        std::unordered_map<std::string, std::string> SnapDataParseFromArray(
            const SnapshotData& snapData);

        // 把当前整张 KV 表取出来（异步回调，在工作线程执行）
        void GetSnapshot(const OnGetSnapshot& callback);

        // 用快照恢复整张 KV 表（异步，在工作线程执行）
        void RecoverFromSnapshot(const proto::Snapshot& snap);

        // 应用一条已提交的日志条目（异步，在工作线程执行）。
        // 这是"客户端请求最终被执行"的地方
        void ApplyStateMachine(std::shared_ptr<proto::Entry> entry);
};
}  // namespace kv

#endif /* STATE_MACHINE_H */
