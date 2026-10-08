#!/bin/bash
# bench-suite.sh —— 读模式对比压测套件（可复现）
#
# 3 种读模式 × 2 种负载 = 6 个场景：
#   weak / readindex / strong  ×  pure(纯读) / mixed(80%读 + 20%写)
#
# 每个场景：
#   1) 用对应 KV_READ_MODE 重启集群（gperftools CPU 采样开着，见 Procfile）
#   2) read-bench 按 leader 40% / follower 60% 分发；混合场景另有 20% 写打 leader
#   3) 优雅停止节点 → gperftools 落盘 .prof，并在日志里打印
#      "PROFILE: interrupts/evictions/bytes = N/M/B"（N = 1kHz 下的 CPU 采样数 ≈ CPU 毫秒数）
#   4) 保存每个场景的 .prof / 日志，供离线用 pprof 分析
#
# 产物在 $OUT（默认 build/bench_out；build/ 已被 .gitignore 忽略）
# 结果表: $OUT/summary.tsv
#
# 用法（容器内）：bash /cxx_project/dkv/code/raft-app/bench-suite.sh
#   TOTAL_QPS=200 DURATION=15 bash bench-suite.sh
#   DO_PPROF=1 bash bench-suite.sh     # 额外跑 pprof --text（本环境很慢，~6min/个，慎用）
set -u
PROTO=/cxx_project/dkv/code/kv-system/resource/cli.proto
BENCH=/cxx_project/dkv/code/raft-app/read-bench.py
BUILD=/cxx_project/dkv/build
PPROF=/opt/brpc/tools/pprof
BIN=$BUILD/raft-app/raft-kv
OUT=$BUILD/bench_out
TOTAL_QPS=${TOTAL_QPS:-200}
DURATION=${DURATION:-15}
DO_PPROF=${DO_PPROF:-0}

rm -rf "$OUT"; mkdir -p "$OUT"
printf "scenario\ttotal_sent\terr\trps\tavg_us\tp50_us\tp90_us\tp99_us\tcpu_samples(节点和)\n" > "$OUT/summary.tsv"

leader_addr() { python3 -c "
from kazoo.client import KazooClient as K
z=K(hosts='127.0.0.1:2181'); z.start(); d,_=z.get('/raft'); print(d.decode().split()[0]); z.stop(); z.close()"; }

start_cluster() {  # $1 = KV_READ_MODE
  local attempt n
  for attempt in 1 2 3; do
    pkill -x raft-kv 2>/dev/null; pkill -x goreman 2>/dev/null
    for i in $(seq 1 20); do pgrep -x raft-kv >/dev/null 2>&1 || break; sleep 0.5; done
    sleep 3; rm -rf /diskvdata; rm -f "$BUILD"/*.prof
    cd "$BUILD"
    nohup bash -c "ulimit -n 65535; export KV_READ_MODE=$1; export CPUPROFILE_FREQUENCY=1000; exec goreman start" > /tmp/gm.log 2>&1 &
    sleep 12
    n=$(grep -c 'ClientServiceImpl] is serving on port=' /tmp/gm.log)
    if [ "$n" = "3" ]; then return 0; fi
    echo "  [启动不完整 $n/3，重试]"
  done
  return 1
}

run_case() {  # $1=mode $2=workload(pure|mixed)
  local mode=$1 wl=$2 wr=0
  [ "$wl" = "mixed" ] && wr=0.2
  local label="${mode}_${wl}"
  echo "########## $label ##########"
  start_cluster "$mode" || { echo "  [跳过: 集群启动失败]"; return 1; }

  cd /cxx_project/dkv/code/raft-app
  local res
  res=$(python3 "$BENCH" --label "$label" --total-qps "$TOTAL_QPS" \
        --duration "$DURATION" --write-ratio "$wr" 2>&1 | grep "^RESULT")
  echo "  $res"

  # 优雅停止 → profile 落盘 + 日志打印 CPU 采样数
  pkill -INT -x raft-kv
  for i in $(seq 1 20); do pgrep -x raft-kv >/dev/null 2>&1 || break; sleep 0.5; done
  sleep 1
  cp /tmp/gm.log "$OUT/$label.log"
  local cpu
  cpu=$(grep -o "interrupts/evictions/bytes = [0-9]*" "$OUT/$label.log" \
        | grep -o "[0-9]*" | paste -sd+ | bc 2>/dev/null)
  [ -n "${cpu:-}" ] || cpu=0

  local f
  for f in "$BUILD"/raft-kv-*.prof; do
    [ -s "$f" ] && mv "$f" "$OUT/$label-$(basename "$f")"
  done

  # 把 RESULT 里的字段拆出来写进 summary.tsv
  local v
  v=$(echo "$res" | sed 's/^RESULT //')
  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$label" \
    "$(echo "$v" | python3 -c 'import sys,json;print(json.load(sys.stdin)["total_sent"])')" \
    "$(echo "$v" | python3 -c 'import sys,json;print(json.load(sys.stdin)["total_err"])')" \
    "$(echo "$v" | python3 -c 'import sys,json;print(json.load(sys.stdin)["rps"])')" \
    "$(echo "$v" | python3 -c 'import sys,json;print(json.load(sys.stdin)["avg_us"])')" \
    "$(echo "$v" | python3 -c 'import sys,json;print(json.load(sys.stdin)["p50_us"])')" \
    "$(echo "$v" | python3 -c 'import sys,json;print(json.load(sys.stdin)["p90_us"])')" \
    "$(echo "$v" | python3 -c 'import sys,json;print(json.load(sys.stdin)["p99_us"])')" \
    "$cpu" >> "$OUT/summary.tsv"
  echo "  [cpu] samples=$cpu   [prof] $(ls "$OUT/$label"-*.prof 2>/dev/null | wc -l) 个"

  # 可选：离线热点分析（慢）
  if [ "$DO_PPROF" = "1" ]; then
    local prof
    prof=$(ls "$OUT/$label"-raft-kv-*.prof 2>/dev/null | head -1)
    [ -n "$prof" ] && "$PPROF" --text "$BIN" "$prof" > "$OUT/$label.pprof.txt" 2>/dev/null
    echo "  [pprof] -> $OUT/$label.pprof.txt"
  fi
}

for mode in weak readindex strong; do
  for wl in pure mixed; do
    run_case "$mode" "$wl"
  done
done
echo "########## done ##########"
cat "$OUT/summary.tsv"
