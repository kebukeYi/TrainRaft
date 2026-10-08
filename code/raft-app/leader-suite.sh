#!/bin/bash
# leader-suite.sh —— leader 视角性能对比套件（可复现）
#
# 四个场景，全部只打 leader：
#   write         写(Set)          —— 与读模式无关，在默认模式下测一次
#   weak-read     弱读(Get)        —— KV_READ_MODE=weak
#   readindex-read 线性一致读(Get) —— KV_READ_MODE=readindex
#   strong-read   强一致读(Get)    —— KV_READ_MODE=strong
#
# 每种读模式都要用对应环境变量重启一次集群（读模式是服务端启动配置）。
# 结果写入 build/leader_out/summary.tsv。
#
# 用法（容器内）：
#   bash /cxx_project/dkv/code/raft-app/leader-suite.sh
#   QPS=100 DURATION=10 bash leader-suite.sh
set -u
BENCH=/cxx_project/dkv/code/raft-app/leader-bench.py
BUILD=/cxx_project/dkv/build
OUT=$BUILD/leader_out
QPS=${QPS:-100}
DURATION=${DURATION:-10}
THREADS=${THREADS:-8}

rm -rf "$OUT"; mkdir -p "$OUT"
printf "label\tmode\tmethod\tok_qps\tsent\terr\terr_pct\tavg_us\tp50_us\tp90_us\tp99_us\tmax_us\n" \
  > "$OUT/summary.tsv"

# 用指定读模式重启集群；成功后返回 0
restart() {  # $1 = KV_READ_MODE
  local attempt n
  for attempt in 1 2 3; do
    pkill -x raft-kv 2>/dev/null; pkill -x goreman 2>/dev/null
    for i in $(seq 1 20); do pgrep -x raft-kv >/dev/null 2>&1 || break; sleep 0.5; done
    sleep 3; rm -rf /diskvdata
    ( cd "$BUILD" && nohup bash -c \
        "ulimit -n 65535; export KV_READ_MODE=$1; exec goreman start" \
        > /tmp/gm.log 2>&1 & )
    sleep 12
    # 只数客户端服务(ClientServiceImpl)的监听行，3 个节点都起来才算好
    n=$(grep -c 'ClientServiceImpl] is serving on port=' /tmp/gm.log)
    [ "$n" -ge 3 ] && return 0
    echo "  [启动不完整 $n/3，重试]"
  done
  return 1
}

# 跑一个场景：$1=label $2=mode $3=method
run() {
  echo "########## $1 (mode=$2) ##########"
  python3 "$BENCH" --label "$1" --mode "$2" --method "$3" \
      --qps "$QPS" --duration "$DURATION" --threads "$THREADS" \
      --tsv "$OUT/summary.tsv" 2>&1 | tee "$OUT/$1.log" | grep -E "RESULT"
}

seed() {  # 写一次 key，保证读能读到数据
  python3 "$BENCH" --label seed --mode n/a --method set \
      --qps 1 --duration 2 >/dev/null 2>&1
}

# 1) 写场景（与读模式无关）
restart weak || { echo "[失败] 集群起不来"; exit 1; }
seed; run write n/a set

# 2) 弱读
run weak-read weak get

# 3) 线性一致读（ReadIndex）
restart readindex || { echo "[失败] 集群起不来"; exit 1; }
seed; run readindex-read readindex get

# 4) 强一致读（Get 也走提案）
restart strong || { echo "[失败] 集群起不来"; exit 1; }
seed; run strong-read strong get

echo "########## 结果表 ##########"
cat "$OUT/summary.tsv"
