#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
read-bench.py —— 读/读写混合场景压测（leader / follower 按权重分发）

- 读请求：按权重分发，leader 占 --leader-ratio（默认 0.4），其余平均分给每个 follower
- 写请求：(1-读权重) 部分由 --write-ratio 指定，全部打 leader（写最终由 leader 落账）
- 每个目标起一个 rpc_press 进程并发跑，结束后汇总输出一行 RESULT (JSON)

注意：读一致性模式(weak/readindex/strong)是【服务端】启动时决定的，
      由环境变量 KV_READ_MODE / KV_READ_ONLY_OPTION 控制，本脚本不负责切换。

用法（容器内）：
  # 纯读
  python3 read-bench.py --label weak-pure    --total-qps 300 --duration 20
  # 混合读写(20% 写)
  python3 read-bench.py --label weak-mixed   --total-qps 300 --duration 20 --write-ratio 0.2
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

_LAT_RE = re.compile(r"^\s*(avg|50%|90%|99%|max)\s+(\d+)\s+us", re.M)
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
    sent = err = 0
    for e, s in _TOT_RE.findall(out):
        err = max(err, int(e))
        sent = max(sent, int(s))
    lat = {k: int(v) for k, v in _LAT_RE.findall(out)}
    return sent, err, lat


def main():
    ap = argparse.ArgumentParser(description="读/读写混合场景压测")
    ap.add_argument("--label", default="", help="本次结果的名字")
    ap.add_argument("--total-qps", type=int, default=300, help="所有目标加起来的总 QPS")
    ap.add_argument("--duration", type=int, default=20, help="持续秒数")
    ap.add_argument("--leader-ratio", type=float, default=0.4,
                    help="【读】请求里打 leader 的占比")
    ap.add_argument("--write-ratio", type=float, default=0.0,
                    help="总 QPS 里写的占比（写全部打 leader）")
    ap.add_argument("--key", default="bench", help="读写的 key")
    ap.add_argument("--threads", type=int, default=4, help="每个 rpc_press 的线程数")
    ap.add_argument("--connection-type", default="single", choices=["single", "pooled", "short"])
    ap.add_argument("--timeout-ms", type=int, default=30000, help="单次 RPC 超时(毫秒)")
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

    # 先写一次 key，保证读能读到数据
    subprocess.run(
        ["rpc_press", f"-proto={a.proto}", "-method=proto.ClientService.Set",
         f"-server={leader}", f"-input={json.dumps({'key': a.key, 'value': 'v'})}",
         "-qps=1", "-duration=2", "-dummy_port=8888"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    read_qps = a.total_qps * (1.0 - a.write_ratio)
    write_qps = a.total_qps * a.write_ratio

    # 组装任务: (类型, 目标, 方法, 输入, qps)
    jobs = []
    if read_qps > 0:
        jobs.append(("read", leader, "Get", json.dumps({"key": a.key}),
                     max(1, int(round(read_qps * a.leader_ratio)))))
        if followers:
            per = read_qps * (1.0 - a.leader_ratio) / len(followers)
            for f in followers:
                jobs.append(("read", f, "Get", json.dumps({"key": a.key}),
                             max(1, int(round(per)))))
    if write_qps > 0:
        jobs.append(("write", leader, "Set",
                     json.dumps({"key": a.key, "value": "v"}),
                     max(1, int(round(write_qps)))))

    print(f"[{a.label}] total_qps={a.total_qps} 时长={a.duration}s "
          f"读:{int(read_qps)}(leader占{a.leader_ratio:.0%}) 写:{int(write_qps)}")
    sys.stdout.flush()

    procs = []
    for i, (kind, addr, method, payload, q) in enumerate(jobs):
        cmd = ["rpc_press",
               f"-proto={a.proto}", f"-method=proto.ClientService.{method}",
               f"-server={addr}", f"-input={payload}",
               f"-qps={q}", f"-duration={a.duration}",
               f"-thread_num={a.threads}", f"-connection_type={a.connection_type}",
               f"-timeout_ms={a.timeout_ms}", f"-dummy_port={8890 + i}"]
        print(f"   -> [{kind}] {addr}  qps={q}")
        procs.append((kind, addr, q, subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)))
    sys.stdout.flush()

    results = []
    for kind, addr, q, p in procs:
        out, _ = p.communicate()
        sent, err, lat = parse_press_output(out)
        results.append(dict(kind=kind, addr=addr, qps=q, sent=sent, err=err, lat=lat))

    total_sent = sum(r["sent"] for r in results)
    total_err = sum(r["err"] for r in results)
    ok = total_sent - total_err
    rps = ok / a.duration if a.duration else 0
    avg = (sum(r["lat"].get("avg", 0) * r["sent"] for r in results) / total_sent
           if total_sent else 0)
    p50 = max((r["lat"].get("50%", 0) for r in results), default=0)
    p90 = max((r["lat"].get("90%", 0) for r in results), default=0)
    p99 = max((r["lat"].get("99%", 0) for r in results), default=0)

    print("")
    for r in results:
        print(f"   [{r['kind']}] {r['addr']}: qps={r['qps']} sent={r['sent']} "
              f"err={r['err']} avg={r['lat'].get('avg', 0)}us "
              f"p50={r['lat'].get('50%', 0)} p99={r['lat'].get('99%', 0)}")
    summary = {"mode": a.label, "write_ratio": a.write_ratio,
               "total_sent": total_sent, "total_err": total_err, "ok": ok,
               "rps": round(rps, 1), "avg_us": int(avg),
               "p50_us": p50, "p90_us": p90, "p99_us": p99}
    print("RESULT " + json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
