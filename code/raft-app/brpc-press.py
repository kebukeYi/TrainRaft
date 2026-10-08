#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
brpc-press.py —— Raft KV 集群压测启动脚本

它做三件事：
  1. 连接 ZooKeeper，读取 /raft 节点，拿到集群所有节点地址（形如 "ip:客户端端口"）
  2. 约定列表里第 1 个是 leader，其余是 follower
  3. 调用 brpc 自带的压测工具 rpc_press，按指定方法/QPS/时长打流量并打印结果

【必须在本项目的 docker 容器内运行】
（容器里才有 kazoo、numpy、rpc_press 以及 proto 路径 /cxx_project/dkv/...）
    docker exec -it distributed bash
    cd /cxx_project/dkv/code/raft-app
    python3 brpc-press.py                                  # 用默认参数
    python3 brpc-press.py -m set -q 2000 -d 20             # 写, 2000 QPS, 20 秒
    python3 brpc-press.py -m get -q 500  -d 10 -t follower # 读, 压 follower
    python3 brpc-press.py -m set -q 0    -d 15             # 不限速, 打满 15 秒

跑之前请确保集群已启动（容器内: cd /cxx_project/dkv/build && goreman start）。

------------------------------------------------------------
rpc_press 常用参数（脚本会自动拼好，这里留作参考）：
  -proto       proto 文件路径
  -method      package.Service.Method，如 proto.ClientService.Set
  -server      ip:port（或集群地址）
  -input       请求 JSON（也可以是一个包含 JSON 的文件路径）
  -qps         每秒请求数；0 = 不限速（最大压力）
  -duration    持续秒数；不设则一直压到 Ctrl+C
  -thread_num  发送线程数；0 = 自动
  -connection_type  single / pooled / short
  -timeout_ms  RPC 超时（毫秒）
  -max_retry   失败重试次数
  -dummy_port  压测器自带 dummy server 的端口（默认 8888），用于观察压测器自身状态
------------------------------------------------------------
"""
import argparse
import json
import os
import shutil
import subprocess
import sys

# kazoo 是第三方包，不是标准库。这里不直接崩，而是记下错误，
# 等真正要连 ZooKeeper 时再给人话提示（这样 --help 在没装 kazoo 时也能用）。
_KAZOO_IMPORT_ERROR = None
try:
    from kazoo.client import KazooClient
    from kazoo.exceptions import NoNodeError
except ImportError as e:
    # 用 ImportError 而非 ModuleNotFoundError：兼容"模块不存在"和"模块里没这个名字"
    _KAZOO_IMPORT_ERROR = e


def _check_kazoo() -> bool:
    """检查 kazoo 是否可用；不可用则打印友好提示并返回 False。"""
    if _KAZOO_IMPORT_ERROR is None:
        return True
    print("[错误] 缺少 Python 包 kazoo（它是第三方库，不是标准库）。")
    print("       本脚本请在本项目的 docker 容器内运行（容器已装好依赖）：")
    print("           docker exec -it distributed bash")
    print("           cd /cxx_project/dkv/code/raft-app && python3 brpc-press.py")
    print("       若确实想在当前机器跑，先执行: pip3 install kazoo")
    return False


# ------------------------------ 默认配置 ------------------------------
ZK_HOSTS = "127.0.0.1:2181"   # ZooKeeper 地址（容器里 ZK 就跑在本机）
RAFT_PATH = "/raft"           # leader 注册的 znode 路径（C++ 端写死的就是这个）
DEFAULT_PROTO = "/cxx_project/dkv/code/kv-system/resource/cli.proto"

# 支持的测试方法： 名字 -> rpc_press 需要的完整方法名
METHODS = {
    "set": "proto.ClientService.Set",   # 写
    "get": "proto.ClientService.Get",   # 读
    "del": "proto.ClientService.Del",   # 删
}


def build_input(method: str, key: str, value: str) -> str:
    """按方法构造合法的请求 JSON。
    注意：cli.proto 里 SetRequest 的字段是 key/value，不能乱写字段名。"""
    if method == "set":
        payload = {"key": key, "value": value}
    else:  # get / del 只需要 key
        payload = {"key": key}
    return json.dumps(payload, ensure_ascii=False)


class ZkClient:
    """读取 /raft 节点，做服务发现（拿到集群节点地址列表）。"""

    def __init__(self, host: str, raft_path: str):
        self.raft_path = raft_path
        try:
            self.zk = KazooClient(hosts=host)
            self.zk.start(timeout=10)
        except Exception as e:  # 连接超时/被拒等，统一给提示
            print(f"[错误] 连接 ZooKeeper({host}) 失败: {e}")
            print("       - ZooKeeper 起了吗？（容器内 goreman start 会自动拉起）")
            print("       - 本脚本要在容器内运行，宿主机的 127.0.0.1:2181 连不到容器里的 ZK")
            sys.exit(1)

    def discover(self) -> list:
        """返回 [leader, follower1, follower2, ...]；失败返回 []"""
        try:
            data, _ = self.zk.get(self.raft_path)
        except NoNodeError:
            print(f"[错误] ZooKeeper 里没有 {self.raft_path} 节点：")
            print("       集群可能还没起来，或还没有节点当选 leader（zk 里没注册）")
            return []
        # C++ 端写的是 "ip:port ip:port ..."，用 split() 按任意空白切分更稳
        nodes = [n for n in data.decode("utf-8").split() if n]
        return nodes

    def close(self):
        self.zk.stop()
        self.zk.close()


def parse_args():
    p = argparse.ArgumentParser(
        description="Raft KV 压测脚本（内部调用 brpc 的 rpc_press）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("-m", "--method", choices=sorted(METHODS), default="set",
                   help="压测的方法：set(写)/get(读)/del(删)")
    p.add_argument("-q", "--qps", type=int, default=1000,
                   help="每秒请求数；0 表示不限速（最大压力）")
    p.add_argument("-d", "--duration", type=int, default=10,
                   help="持续压测的秒数")
    p.add_argument("-t", "--target", choices=["leader", "follower"], default="leader",
                   help="压 leader 还是某个 follower（follower 场景可测转发路径）")
    p.add_argument("--threads", type=int, default=8,
                   help="rpc_press 发送线程数（0=自动）")
    p.add_argument("--connection-type", default="pooled",
                   choices=["single", "pooled", "short"],
                   help="连接方式：single 单连接 / pooled 连接池 / short 短连接")
    p.add_argument("--timeout-ms", type=int, default=3000,
                   help="单次 RPC 超时（毫秒）")
    p.add_argument("--max-retry", type=int, default=3,
                   help="失败重试次数")
    p.add_argument("--key", default="name", help="请求用的 key")
    p.add_argument("--value", default="ft", help="请求用的 value（仅 set 用）")
    p.add_argument("--proto", default=DEFAULT_PROTO,
                   help="cli.proto 的路径")
    p.add_argument("--zk", default=ZK_HOSTS, help="ZooKeeper 地址")
    p.add_argument("--dummy-port", type=int, default=8888,
                   help="rpc_press dummy server 端口")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    # --- 前置检查：把"环境不对"这类问题挡在最前面，给出人话提示 ---
    if not _check_kazoo():
        return 1
    if not os.path.isfile(args.proto):
        print(f"[错误] 找不到 proto 文件: {args.proto}")
        print("       请在 docker 容器内运行，或用 --proto 指定正确路径")
        return 1
    if shutil.which("rpc_press") is None:
        print("[错误] PATH 里找不到 rpc_press")
        print("       它是 brpc 自带工具，只装在 docker 容器里，请在容器内运行本脚本")
        return 1

    # --- 服务发现：从 ZK 拿节点列表 ---
    client = ZkClient(args.zk, RAFT_PATH)
    nodes = client.discover()
    client.close()
    if not nodes:
        return 1

    leader, followers = nodes[0], nodes[1:]
    print(f"[信息] 发现 {len(nodes)} 个节点: {nodes}")
    print(f"[信息] leader = {leader}")

    # --- 选压测目标 ---
    if args.target == "leader":
        server = leader
    else:
        if not followers:
            print("[错误] 没有 follower 可用（单节点集群？）")
            return 1
        server = followers[0]
    print(f"[信息] 压测目标({args.target}) = {server}")

    # --- 拼 rpc_press 命令 ---
    # 注意：subprocess 用列表传参、不走 shell，所以 JSON 后面不要加单引号！
    input_json = build_input(args.method, args.key, args.value)
    cmd = [
        "rpc_press",
        f"-proto={args.proto}",
        f"-method={METHODS[args.method]}",
        f"-server={server}",
        f"-input={input_json}",
        f"-qps={args.qps}",
        f"-duration={args.duration}",
        f"-thread_num={args.threads}",
        f"-connection_type={args.connection_type}",
        f"-timeout_ms={args.timeout_ms}",
        f"-max_retry={args.max_retry}",
        f"-dummy_port={args.dummy_port}",
    ]
    print("[信息] 执行命令:")
    print("       " + " ".join(cmd))
    print("-" * 60)
    sys.stdout.flush()  # 先把上面的信息刷出去，免得被 rpc_press 的输出抢先

    # --- 执行（不捕获输出，让 rpc_press 的实时统计直接打印出来）---
    try:
        result = subprocess.run(cmd, text=True)
    except FileNotFoundError:
        print("[错误] 无法执行 rpc_press")
        return 1
    except KeyboardInterrupt:
        print("\n[信息] 已手动中断压测")
        return 130

    print("-" * 60)
    print(f"[信息] rpc_press 退出码 = {result.returncode}")
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
