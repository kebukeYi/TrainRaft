#!/bin/bash
# tick-experiment.sh —— 验证"~100ms 延迟台阶"是否来自定时器周期
#
# 定时器同时驱动 raft 时钟 和 Ready/readStates 的消费(回包)。
# 直接调小 tick 会让墙钟心跳/选举超时同步变小 → 集群乱选主，实验不干净。
# 所以这里做【等比例】对照：
#   base100 : tick=100ms, election=10, heartbeat=1  → 墙钟 心跳100ms / 选举1000ms
#   tick10  : tick= 10ms, election=100, heartbeat=10 → 墙钟 心跳100ms / 选举1000ms
# 两者 raft 时间参数完全一致，唯一差别是回包消费频率(10Hz → 100Hz)。
#
# 若 tick10 的 p90/p99 从 ~100ms 掉到 ~10ms 量级，即坐实台阶来自 tick 周期。
#
# 用法（容器内）：bash /cxx_project/dkv/code/raft-app/tick-experiment.sh
set -u
BENCH=/cxx_project/dkv/code/raft-app/leader-bench.py
OUT=/cxx_project/dkv/build/leader_out
mkdir -p "$OUT"
TSV=$OUT/tick.tsv
printf "label\tmode\tmethod\tok_qps\tsent\terr\terr_pct\tavg_us\tp50_us\tp90_us\tp99_us\tmax_us\n" > "$TSV"

restart() {  # $1=tick_ms $2=election_tick $3=heartbeat_tick
  local attempt n
  for attempt in 1 2 3; do
    pkill -x raft-kv 2>/dev/null; pkill -x goreman 2>/dev/null
    for i in $(seq 1 20); do pgrep -x raft-kv >/dev/null 2>&1 || break; sleep 0.5; done
    sleep 2; rm -rf /diskvdata
    ( cd /cxx_project/dkv/build && nohup bash -c \
        "ulimit -n 65535; export KV_READ_MODE=weak \
         KV_TICK_MS=$1 KV_ELECTION_TICK=$2 KV_HEARTBEAT_TICK=$3; \
         exec goreman start" > /tmp/gm_tick.log 2>&1 & )
    for i in $(seq 1 40); do
      n=$(grep -c "ClientServiceImpl] is serving on port=" /tmp/gm_tick.log 2>/dev/null)
      [ "${n:-0}" -ge 3 ] && break
      sleep 1
    done
    [ "${n:-0}" -ge 3 ] && { sleep 3; return 0; }
    echo "  [启动不完整，重试]"
  done
  return 1
}

run_cfg() {  # $1=label $2=tick $3=election $4=heartbeat
  echo "########## $1 (tick=$2ms election=$3 heartbeat=$4) ##########"
  restart "$2" "$3" "$4" || { echo "  [集群起不来]"; return 1; }

  # 稳定性检查：这段时间内发生过几次选主/leader 变更
  python3 "$BENCH" --label "$1-seed" --mode n/a --method set \
      --qps 1 --duration 2 >/dev/null 2>&1
  local leaders
  leaders=$(grep -c "became leader" /tmp/gm_tick.log)
  echo "  [稳定性] 日志中 'became leader' 出现 $leaders 次（越接近节点数越稳）"

  python3 "$BENCH" --label "$1-q100" --mode weak --method get \
      --qps 100 --duration 10 --tsv "$TSV" 2>&1 | grep RESULT
  python3 "$BENCH" --label "$1-sat" --mode weak --method get \
      --qps 0 --duration 10 --timeout-ms 10000 --tsv "$TSV" 2>&1 | grep RESULT
}

run_cfg base100 100 10 1
run_cfg tick10  10  100 10

echo "########## tick.tsv ##########"
cat "$TSV"
