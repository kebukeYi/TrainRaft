#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
read-bench.py —— 读场景混合压测（leader / follower 按权重分发）

作用：对当前集群发起一批读请求，按权重把请求分发给 leader 和各个 follower：
    - leader  占 --leader-ratio（默认 0.4）
    - 其余权重平均分给每个 follower
实现方式：每个目标起一个 rpc_press 进程，QPS 按权重分配，并发跑；
         结束后汇总每个目标的统计，输出一行 RESULT（JSON）。

注意：读一致性模式(weak/readindex/strong)是【服务端】启动时决定的，
      由环境变量 KV_READ_MODE / KV_READ_ONLY_OPTION 控制，本脚本不负责切换。

用法（容器内）：
  python3 read-bench.py --label weak            --total-qps 200 --duration 10
  python3 read-bench.py --label readindex-safe  --total-qps 200 --duration 10
  python3 read-bench.py --label readindex-lease --total-qps 200 --duration 10
  python3 read-bench.py --label strong          --total-qps 200 --duration 10
"""
import argparse
import json
import os
import re
import shutil
import subprocess
import sys

try:
    from kazoo.client import KazooClient
except ImportError:
    print("[错误] 缺少 Python 包 kazoo，请在本项目的 docker 容器内运行本脚本")
    sys.exit(1)

ZK_HOSTS = "127.0.0.1:2181"
RAFT_PATH = "/raft"
DEFAULT_PROTO = "/cxx_project/dkv/code/kv-system/resource/cli.proto"

# rpc_press 输出里的延迟块，例如:  "  avg           1066 us"  /  "  99%          4872 us"
_LAT_RE = re.compile(r"^\s*(avg|50%|90%|99%|max)\s+(\d+)\s+us", re.M)
# rpc_press 每秒统计行:  "total_error:0         total_sent:40"
_TOT_RE = re.compile(r"total_error:(\d+)\s+total_sent:(\d+)")


def discover(hosts, path):
    zk = KazooClient(hosts=hosts)
    zk.start(timeout=10)
    try:
        data, _ = zk.get(path)
    finally:
        zk.stop()
        zk.close()
    return [n for n in data.decode("utf-8").split() if n]


def parse_press_output(out):
    """从 rpc_press 输出里取 (总发送, 总错误, 延迟 dict)"""
    sent = err = 0
    for e, s in _TOT_RE.findall(out):
        err = max(err, int(e))
        sent = max(sent, int(s))
    lat = {k: int(v) for k, v in _LAT_RE.findall(out)}
    return sent, err, lat


def main():
    ap = argparse.ArgumentParser(description="读场景混合压测(leader/follower 按权重)")
    ap.add_argument("--label", default="", help="本次结果的名字(如 weak/readindex-safe)")
    ap.add_argument("--total-qps", type=int, default=200, help="所有目标加起来的总 QPS")
    ap.add_argument("--duration", type=int, default=10, help="持续秒数")
    ap.add_argument("--leader-ratio", type=float, default=0.4, help="leader 承担的请求占比")
    ap.add_argument("--key", default="bench", help="读的 key")
    ap.add_argument("--threads", type=int, default=4, help="每个 rpc_press 的线程数")
    ap.add_argument("--connection-type", default="single", choices=["single", "pooled", "short"])
    ap.add_argument("--timeout-ms", type=int, default=20000, help="单次 RPC 超时(毫秒)")
    ap.add_argument("--proto", default=DEFAULT_PROTO)
    ap.add_argument("--zk", default=ZK_HOSTS)
    a = ap.parse_args()

    if not os.path.isfile(a.proto):
        print(f"[错误] 找不到 proto 文件: {a.proto}")
        return 1
    if shutil.which("rpc_press") is None:
        print("[错误] PATH 里找不到 rpc_press（请在容器内运行）")
        return 1

    nodes = discover(a.zk, RAFT_PATH)
    if not nodes:
        print("[错误] ZooKeeper 里没有 /raft 节点（集群没起来 / 没选出 leader）")
        return 1
    leader, followers = nodes[0], nodes[1:]

    # 计算每个目标的权重与 QPS
    targets = [(leader, a.leader_ratio)]
    if followers:
        per = (1.0 - a.leader_ratio) / len(followers)
        targets += [(f, per) for f in followers]

    # 先写一次 key，保证读能读到数据
    subprocess.run(
        ["rpc_press", f"-proto={a.proto}", "-method=proto.ClientService.Set",
         f"-server={leader}", f"-input={json.dumps({'key': a.key, 'value': 'v'})}",
         "-qps=1", "-duration=2", "-dummy_port=8888"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    print(f"[{a.label}] 目标分布: 总QPS={a.total_qps} 时长={a.duration}s "
          f"leader占比={a.leader_ratio:.0%}")
    sys.stdout.flush()

    input_json = json.dumps({"key": a.key})
    procs = []
    for i, (addr, w) in enumerate(targets):
        q = max(1, int(round(a.total_qps * w)))
        cmd = ["rpc_press",
               f"-proto={a.proto}", "-method=proto.ClientService.Get",
               f"-server={addr}", f"-input={input_json}",
               f"-qps={q}", f"-duration={a.duration}",
               f"-thread_num={a.threads}", f"-connection_type={a.connection_type}",
               f"-timeout_ms={a.timeout_ms}", f"-dummy_port={8890 + i}"]
        print(f"   -> {addr}  权重={w:.0%}  qps={q}")
        procs.append((addr, w, q, subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)))
    sys.stdout.flush()

    results = []
    for addr, w, q, p in procs:
        out, _ = p.communicate()
        sent, err, lat = parse_press_output(out)
        results.append(dict(addr=addr, weight=w, qps=q, sent=sent, err=err, lat=lat))

    # 汇总
    total_sent = sum(r["sent"] for r in results)
    total_err = sum(r["err"] for r in results)
    ok = total_sent - total_err
    rps = ok / a.duration if a.duration else 0
    # 加权平均延迟（按发送量加权）
    avg = (sum(r["lat"].get("avg", 0) * r["sent"] for r in results) / total_sent
           if total_sent else 0)
    # 分位数取"各目标中最差的那个"（保守）
    p50 = max((r["lat"].get("50%", 0) for r in results), default=0)
    p90 = max((r["lat"].get("90%", 0) for r in results), default=0)
    p99 = max((r["lat"].get("99%", 0) for r in results), default=0)

    print("")
    for r in results:
        print(f"   {r['addr']}: sent={r['sent']} err={r['err']} "
              f"avg={r['lat'].get('avg', 0)}us p50={r['lat'].get('50%', 0)} "
              f"p90={r['lat'].get('90%', 0)} p99={r['lat'].get('99%', 0)}")
    summary = {"mode": a.label, "total_sent": total_sent, "total_err": total_err,
               "ok": ok, "rps": round(rps, 1), "avg_us": int(avg),
               "p50_us": p50, "p90_us": p90, "p99_us": p99}
    print("RESULT " + json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
