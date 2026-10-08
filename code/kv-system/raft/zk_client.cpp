#include <raft/zk_client.h>

namespace kv {

// 全局的 watcher 观察器：zkserver 通知 ZkClient 的回调。
// 这里只关心"会话连接成功"这一种事件：连接成功就释放信号量，
// 让阻塞在 Start() 里的主流程继续往下走
void GlobalWatcher(zhandle_t* zh, int type, int state, const char* path,
                   void* watcherCtx) {
    if (type == ZOO_SESSION_EVENT)  // 回调的消息类型是会话相关事件
    {
        if (state == ZOO_CONNECTED_STATE)  // 已与 zkserver 连接成功
        {
            // 取出之前放进 context 的信号量，post 一下唤醒等待方
            sem_t* sem = (sem_t*)zoo_get_context(zh);
            sem_post(sem);
        }
    }
}

ZkClient::ZkClient() : zooHandle_(nullptr) {}

ZkClient::~ZkClient() {
    if (zooHandle_ != nullptr) {
        // 关闭句柄，释放资源
        zookeeper_close(zooHandle_);
    }
}

// 连接 zkserver（阻塞，直到连接成功才返回）
void ZkClient::Start() {
    /*
    使用 zookeeper_mt（多线程版本）：
    zookeeper 的 API 客户端程序内部会启动三个线程：
    - API 调用线程（我们发起请求的线程）
    - 网络 I/O 线程（负责收发）
    - watcher 回调线程（负责执行回调函数）
    */
    zooHandle_ = zookeeper_init("127.0.0.1:2181", GlobalWatcher, 30000, nullptr,
                                nullptr, 0);
    if (nullptr == zooHandle_) {
        LOG_ERROR("zookeeper_init error!");
        exit(EXIT_FAILURE);
    }

    // 用信号量阻塞等待"连接成功"的回调：
    // zookeeper_init 是异步的，连接成功时才会触发 watcher
    sem_t sem;
    sem_init(&sem, 0, 0);
    zoo_set_context(zooHandle_, &sem);

    // 阻塞直到 GlobalWatcher 里 sem_post
    sem_wait(&sem);
    LOG_INFO("zookeeper_init success!");
}

// 服务注册：创建（或覆盖）指定 path 的 znode 节点
void ZkClient::CreateZkNode(const std::string& path, const std::string& data) {
    int flag;
    // 先判断节点是否已存在，存在就先删掉（保证写入的是最新数据）
    flag = zoo_exists(zooHandle_, path.c_str(), 0, nullptr);
    if (ZOK == flag) {
        zoo_delete(zooHandle_, path.c_str(), -1);
    }

    // 创建节点：ZOO_OPEN_ACL_UNSAFE 表示任何客户端都可以读写这个节点
    flag = zoo_create(zooHandle_, path.c_str(), data.c_str(), data.size(),
                      &ZOO_OPEN_ACL_UNSAFE, 0, nullptr, 0);
    if (flag == ZOK) {
        LOG_INFO("znode create success... path: %s", path.c_str());
    } else {
        LOG_ERROR("flag: %d", flag);
        LOG_ERROR("znode create error... path: %s", path.c_str());
        exit(EXIT_FAILURE);
    }
}

// 服务发现：读取指定 path 的节点数据
std::string ZkClient::GetZkData(const std::string& path) {
    char buffer[1024];
    int bufferlen = sizeof(buffer);
    int flag =
        zoo_get(zooHandle_, path.c_str(), 0, buffer, &bufferlen, nullptr);
    if (flag != ZOK) {
        LOG_ERROR("get znode error... path: %s", path);
        return "";
    } else {
        return buffer;
    }
}

}  // namespace kv
