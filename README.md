# 分布式KV系统

## 依赖库

```bash
主要的依赖项已在Dockerfile中写明: 主要有 brpc、Rocksdb、Goreman、gperftools、protobuf、gtest、zookeeper、Java、python、go、Cmake

pip3 install kazoo
pip3 install numpy
```

## 环境搭建

### 容器创建

```bash
# 1、到Dockerfile路径下 构建 镜像
docker build -t kv-system:1.0 .

# 2、基于镜像创建容器, 在宿主机的源码根目录执行挂载操作
docker run --privileged -v $(pwd):/cxx_project/dkv -w /cxx_project/dkv --name=distributed -it kv-system:1.0 bash

# 3、启动容器
docker start distributed

# 4、进入容器
docker exec -it distributed bash
```

### zookeeper配置

- 在zookeeper/conf目录下创建zoo.cfg文件

```bash
# The number of milliseconds of each tick
tickTime=2000
# The number of ticks that the initial
# synchronization phase can take
initLimit=10
# The number of ticks that can pass between
# sending a request and getting an acknowledgement
syncLimit=5
# the directory where the snapshot is stored.
# do not use /tmp for storage, /tmp here is just
# example sakes.
dataDir=/opt/zookeeperData  # 主要改这里
# the port at which the clients will connect
clientPort=2181
# the maximum number of client connections.
# increase this if you need to handle more clients
#maxClientCnxns=60
#
# Be sure to read the maintenance section of the
# administrator guide before turning on autopurge.
#
# https://zookeeper.apache.org/doc/current/zookeeperAdmin.html#sc_maintenance
#
# The number of snapshots to retain in dataDir
#autopurge.snapRetainCount=3
# Purge task interval in hours
# Set to "0" to disable auto purge feature
#autopurge.purgeInterval=1

## Metrics Providers
#
# https://prometheus.io Metrics Exporter
#metricsProvider.className=org.apache.zookeeper.metrics.prometheus.PrometheusMetricsProvider
#metricsProvider.httpHost=0.0.0.0
#metricsProvider.httpPort=7000
#metricsProvider.exportJvmInfo=true
```

## 项目结构

```bash
.
├── README.md
├── build_x86.sh：进入docker容器后,执行编译脚本
├── code
│   ├── CMakeLists.txt
│   ├── Procfile：Goreman的配置文件
│   ├── demo
│   ├── kv-system：KV系统的主目录
│   │   ├── CMakeLists.txt
│   │   ├── apply：应用层
│   │   │   ├── raft-node：最上层的类，RaftNode
│   │   │   ├── raft-state-machine：raft状态机
│   │   │   ├── service：rpc服务实现
│   │   │   ├── store：存储模块，里面有持久化存储引擎、缓存、快照、预写式日志的相关功能
│   │   │   └── transport：节点间通信模块
│   │   ├── common：通用模块
│   │   ├── raft：算法层
│   │   └── resource：存放proto文件及其C++代码
│   ├── raft-app：raft应用软件
│   │   ├── CMakeLists.txt
│   │   ├── brpc-press.py：基于brpc压测工具rpc_press编写的压测脚本
│   │   ├── brpc_client.cpp：基于brpc的客户端
│   │   └── raft-kv.cpp：基于raft的KV系统入口
│   └── tests：gtest单元测试
├── docker：docker脚本
│   ├── Dockerfile：编译环境安装
│   └── install-opensource.sh：git安装三方库
└── script：辅助测试、性能分析等的脚本
```

## 编译

```bash
cd 源码根目录后,执行编译构建命令
./build_x86.sh   # 编译脚本中会把 .json procfile 拷贝到build目录中
```

## 运行

```bash
# 集群启动
cd /cxx_project/dkv/build
goreman start        # 这一步将会自动 读取 Procfile 文件内容, 并行多进程启动;


# 客户端 简单访问(写,读,删除 3次请求)
./brpc_client


# 客户端 模拟压力测试 (需要修改内部 cli.pro 路径)
python3 brpc-press.py

```
