#ifndef REGISTRATION_CENTER
#define REGISTRATION_CENTER

#include <common/log.h>
#include <semaphore.h>
#include <zookeeper/zookeeper.h>

#include <iostream>
#include <string>

namespace kv {

// ============================================================================
// ZkClient：Zookeeper 客户端封装（服务注册 + 服务发现）
//
// 大白话：客户端怎么知道"哪个节点是 leader、怎么连它"？靠 Zookeeper。
// 约定：
//   - 每个节点启动时连接本机的 zk（127.0.0.1:2181）
//   - 当选 leader 的节点把集群所有节点的 "ip:客户端端口" 写进 zk 的 /raft 节点
//   - 客户端读 /raft 节点，拿到 leader 的地址去连它（项目中客户端固定用
//     brpc 直连，zk 更多是展示服务发现机制）
// ============================================================================

// 全局的 watcher 观察器：zkserver 通知 ZkClient 的回调函数
void GlobalWatcher(zhandle_t* zh, int type, int state, const char* path,
                   void* watcherCtx);

class ZkClient {
       public:
        ZkClient();

        ~ZkClient();

        // 连接 zkserver（阻塞等待连接成功）
        void Start();

        // 服务注册：创建 znode 节点（写数据）
        void CreateZkNode(const std::string& path, const std::string& data);

        // 服务发现：读取 znode 节点的数据
        std::string GetZkData(const std::string& path);

       private:
        // zk 的客户端句柄（连接后才有值）
        zhandle_t* zooHandle_;
};
}  // namespace kv

#endif  // REGISTRATION_CENTER
