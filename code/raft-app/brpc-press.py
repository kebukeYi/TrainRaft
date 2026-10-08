from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from statistics import mean, stdev
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
import numpy as np
import random
import time
import signal
import threading
import sys
import subprocess

"""
rpc_press 常用命令行参数及其解析: 
必需参数: 
    -proto : 指定相关的 proto 文件名。
    -method : 指定方法名, 形式必须是  package.service.method 。
    -server : 服务器的 IP 和端口, 或者集群地址。
可选参数: 
    -input : 指定 JSON 请求或包含 JSON 请求的文件。
    -inc : 包含被 import 的 proto 文件的路径。
    -lb_policy : 指定负载均衡算法(如  rr  轮询,  random  随机等)。
    -timeout_ms : 设定超时时间(毫秒)。
    -max_retry : 最大重试次数。
    -protocol : 连接服务器使用的协议(如  baidu_std )。
    -connection_type : 连接方式(如  single  短连接,  pooled  连接池等)。
    -output : 如果指定, 响应会转为 JSON 并写入文件。
    -duration : 发送这么多秒的压力后退出(默认一直发送直到手动停止)。
    -qps : 以指定的压力发送请求(默认为最大速度发送)。
    -dummy_port : 修改 dummy server 的端口(默认为 8888)。

使用示例: 
1. 向服务器  0.0.0.0:8002  发送请求, 使用  baidu_std  协议, 重复发送  input.json  中的所有请求, 直到按下  ctrl-c , QPS 设置为 100: 

./rpc_press -proto=echo.proto -method=example.EchoService.Echo -server=0.0.0.0:8002 -input=./input.json -qps=100
2. 使用 round-robin 负载均衡算法向  bns://node-name  代表的所有下游机器发送请求, QPS 为 100: 

./rpc_press -proto=echo.proto -method=example.EchoService.Echo -server=bns://node-name -lb_policy=rr -input='{"message":"hello"} {"message":"world"}' -qps=100
3. 向服务器  0.0.0.0:8002  发送请求, 使用  hulu_pbrpc  协议, QPS 为 100: 

./rpc_press -proto=echo.proto -method=example.EchoService.Echo -server=0.0.0.0:8002 -protocol=hulu_pbrpc -input='{"message":"hello"} {"message":"world"}' -qps=100
4. 以最大速度向服务器  0.0.0.0:8002  发送请求, 直到按下  ctrl-c : 

./rpc_press -proto=echo.proto -method=example.EchoService.Echo -server=0.0.0.0:8002 -input='{"message":"hello"} {"message":"world"}' -qps=0
5. 向服务器  0.0.0.0:8002  发送请求, 持续最大压力 10 秒钟: 

./rpc_press -proto=echo.proto -method=example.EchoService.Echo -server=0.0.0.0:8002 -input='{"message":"hello"} {"message":"world"}' -qps=0 -duration=10
 rpc_press  启动后, 会默认在 8888 端口启动一个 dummy server, 用于观察  rpc_press  本身的运行情况。你可以通过  -dummy_port  参数修改 dummy server 的端口。
这些参数和选项使得  rpc_press  非常灵活, 能够适应各种不同的测试需求和场景。
"""


class ZkClient:
    def __init__(self, host, raftBasePath):
        self.zk = KazooClient(hosts=host)
        self.zk.start()
        self.raftBasePath = raftBasePath
        self.leader = None
        self.leaderIp = None
        self.leaderPort = None

    def ServicesDiscover(self):
        # 获取所有子节点: nodes = self.zk.get_children(self.raftBasePath)
        try:
            data, _ = self.zk.get(self.raftBasePath)
            nodeList = data.decode('utf-8').split(" ")
            return nodeList
        except NoNodeError:
            print("No registered nodes found.")
            return []


if __name__ == "__main__":
    # ZooKeeper服务器地址
    zkHosts = '127.0.0.1:2181'
    # 指定Raft节点的ZooKeeper路径
    raftBasePath = '/raft'
    # 客户端数量
    clientCount = 1
    # 请求数量
    reqCnt = 10
    # 客户端列表
    clientList = []

    client = ZkClient(host=zkHosts, raftBasePath=raftBasePath)
    nodeList = client.ServicesDiscover()
    # nodeList = ['127.0.0.1:63791', '127.0.0.1:63792', '127.0.0.1:63793']
    leader = nodeList[0]
    followerList = nodeList[1:]

# rpc_press -proto=/root/code/cpp/KV_Storage_Engine/distributedKV/code/kv-system/resource/cli.proto -method=proto.ClientService.Set -server=127.0.0.1:63791 -input='{"key":"ft"}'  -qps=0

    # 定义要测试的 RPC 方法
    method = "proto.ClientService.Set"

    # 定义测试参数，例如请求的并发数和总请求数
    concurrency = 10
    total_requests = 1000

    # 构建 rpc_press 命令
    command = [
        "rpc_press",
        "-server=" + nodeList[0],
        # "-proto=/root/code/cpp/KV_Storage_Engine/distributedKV/code/kv-system/resource/cli.proto",  # 指定你的 .proto 文件
        "-proto=/cxx_project/dkv/code/kv-system/resource/cli.proto",  # 本机测试的
        "-method=" + method,
        "-duration=10",  # 可选，指定测试持续时间（秒）
        '-input={"key":"name","value":"ft"}',  # 可选，指定请求的输入数据
    ]

    # 执行命令
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # 打印输出结果
    print("stdout:", result.stdout)
    print("stderr:", result.stderr)

