#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
leader-bench.py —— 只压 leader 的单场景性能采集

思路：把"读模式"当成服务端的开关（环境变量 KV_READ_MODE），
      客户端始终调同一个 Get/Set，且目标永远是 leader。
      这样四种场景（写 / 强一致读 / 线性一致读 / 弱读）打的是同一个节点，
      唯一变量就是代码路径本身，数据才可比。

做什么：
  1. 从 ZooKeeper 的 /raft 取节点列表，第一个即 leader
  2. 用 brpc 自带 rpc_press 打 leader
  3. 从 rpc_press 输出里解析 QPS(sent/ok) 与 avg/p50/p90/p99/max
  4. 打印一行 RESULT {json}；若给 --tsv 则再追加一行到表格

用法（容器内）：
  python3 leader-bench.py --label weak-read --mode weak --method get \
          --qps 100 --duration 10
  python3 leader-bench.py --label write --mode n/a --method set \
          --qps 100 --duration 10 --tsv /cxx_project/dkv/build/leader_out/summary.tsv
"""
import argparse
import json
import re
import shutil
import subprocess
import sys

DEFAULT_PROTO = "/cxx_project/dkv/code/kv-system/resource/cli.proto"

# rpc_press 的 [Latency] 块形如: "  50%   54928 us"
_LAT_RE = re.compile(r"^\s*(avg|50%|90%|99%|max)\s+(\d+)\s+us", re.M)
# 形如: "total_error:0         total_sent:152"
_TOT_RE = re.compile(r"total_error:(\d+)\s+total_sent:(\d+)")


def get_leader(zk="127.0.0.1:2181", path="/raft"):
    """从 ZK 读节点列表，返回 leader 地址（列表第 1 个）。"""
    from kazoo.client import KazooClient
    z = KazooClient(hosts=zk)
    z.start(timeout=10)
    try:
        data, _ = z.get(path)
    finally:
        z.stop()
        z.close()
    return data.decode("utf-8").split()[0]


def main():
    ap = argparse.ArgumentParser(description="只压 leader 的性能采集")
    ap.add_argument("--label", required=True, help="场景名，如 weak-read")
    ap.add_argument("--mode", default="n/a",
                    help="服务端读模式(weak/readindex/strong)，仅用于记录")
    ap.add_argument("--method", choices=["get", "set", "del", "ping", "keys"],
                    default="get",
                    help="get/set/del 走业务路径；ping 是底座探针(直接回 pong)")
    ap.add_argument("--key", default="bench")
    ap.add_argument("--value", default="v")
    ap.add_argument("--qps", type=int, default=100, help="目标 QPS；0=不限速")
    ap.add_argument("--duration", type=int, default=10, help="持续秒数")
    ap.add_argument("--threads", type=int, default=8, help="rpc_press 发送线程数")
    ap.add_argument("--connection-type", default="single",
                    choices=["single", "pooled", "short"])
    ap.add_argument("--timeout-ms", type=int, default=30000)
    ap.add_argument("--proto", default=DEFAULT_PROTO)
    ap.add_argument("--zk", default="127.0.0.1:2181")
    ap.add_argument("--tsv", default="", help="把结果追加到这个 TSV 文件")
    a = ap.parse_args()

    if shutil.which("rpc_press") is None:
        print("[错误] PATH 里找不到 rpc_press（本脚本要在 docker 容器内运行）")
        return 1

    addr = get_leader(a.zk)
    rpc_method = {"get": "Get", "set": "Set", "del": "Del",
                  "ping": "Ping", "keys": "Keys"}[a.method]
    if a.method == "set":
        payload = json.dumps({"key": a.key, "value": a.value})
    elif a.method == "ping":
        payload = json.dumps({"message": "hi"})
    elif a.method == "keys":
        payload = json.dumps({"key": a.key, "value": ""})
    else:
        payload = json.dumps({"key": a.key})

    cmd = [
        "rpc_press",
        f"-proto={a.proto}",
        f"-method=proto.ClientService.{rpc_method}",
        f"-server={addr}",
        f"-input={payload}",
        f"-qps={a.qps}",
        f"-duration={a.duration}",
        f"-thread_num={a.threads}",
        f"-connection_type={a.connection_type}",
        f"-timeout_ms={a.timeout_ms}",
        "-dummy_port=8888",
    ]
    print(f"[{a.label}] leader={addr} method={a.method} "
          f"qps={a.qps} dur={a.duration}s")
    print("   命令: " + " ".join(cmd))
    sys.stdout.flush()

    # 给 rpc_press 留出收尾余量；超时则强杀，避免脚本挂死
    try:
        out = subprocess.run(cmd, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True,
                             timeout=a.duration + 30).stdout
    except subprocess.TimeoutExpired as e:
        out = e.stdout or ""
        if isinstance(out, bytes):
            out = out.decode("utf-8", "replace")
        print(f"[{a.label}] 警告: rpc_press 超过 {a.duration + 30}s 未退出，已强杀")

    sent = err = 0
    for e, s in _TOT_RE.findall(out):
        err = max(err, int(e))
        sent = max(sent, int(s))
    lat = {k: int(v) for k, v in _LAT_RE.findall(out)}

    ok = sent - err
    res = {
        "label": a.label, "mode": a.mode, "method": a.method, "leader": addr,
        "qps_target": a.qps, "sent": sent, "err": err, "ok": ok,
        "ok_qps": round(ok / a.duration, 1) if a.duration else 0,
        "err_pct": round(100.0 * err / sent, 2) if sent else 0.0,
        "avg_us": lat.get("avg", 0), "p50_us": lat.get("50%", 0),
        "p90_us": lat.get("90%", 0), "p99_us": lat.get("99%", 0),
        "max_us": lat.get("max", 0),
    }
    print("RESULT " + json.dumps(res))

    if a.tsv:
        row = "{label}\t{mode}\t{method}\t{ok_qps}\t{sent}\t{err}\t{err_pct}\t" \
              "{avg_us}\t{p50_us}\t{p90_us}\t{p99_us}\t{max_us}\n".format(**res)
        with open(a.tsv, "a") as f:
            f.write(row)
        print(f"[{a.label}] 已追加一行到 {a.tsv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
