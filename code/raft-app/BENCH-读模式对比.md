# 读一致性模式性能对比 · 可复现证据链

> 目的：对比 **弱读 / 线性一致读(ReadIndex) / 强一致读** 在 **纯读** 与 **混合读写** 两种负载下的
> QPS、延迟分位（p50/p90/p99）与 **CPU 开销**，并给出可复现的脚本与产物位置。
>
> 结论先行：**三种读模式的差异，在这台机器上被一条"本机固定开销"淹没了**；
> 真正值得关注的是——**"回包在 raft 线程产生"这条路径给每个请求固定加了 ~75ms**。
> 这一点比"选哪种读模式"重要得多。

---

## 1. 环境与方法

| 项 | 值 |
|---|---|
| 集群 | 3 节点（goreman 启动），ZooKeeper 做 leader 发现 |
| 机器 | WSL2 + Docker（overlay 文件系统，4 CPU） |
| 负载工具 | brpc 自带 `rpc_press`，由 `read-bench.py` 按权重编排 |
| 请求分布 | **读：leader 40%，两个 follower 各 30%**；**写：全部打 leader** |
| 压测参数 | 总 QPS=200，时长 15s，单连接(single)，超时 30s |
| 场景 | 3 读模式 × 2 负载 = 6 个（`pure` 纯读 / `mixed` 80%读+20%写） |
| 读模式开关 | `KV_READ_MODE=weak|readindex|strong`（服务端启动时决定） |
| CPU 采样 | gperftools（`LD_PRELOAD=libprofiler.so, CPUPROFILE_FREQUENCY=1000`），退出时打印 `PROFILE: interrupts=N`，N 即 1kHz 下的 **CPU 毫秒数** |

### 复现命令

```bash
# 容器内
bash /cxx_project/dkv/code/raft-app/bench-suite.sh          # 跑全部 6 个场景
# 可选参数
TOTAL_QPS=200 DURATION=15 bash bench-suite.sh
DO_PPROF=1 bash bench-suite.sh                              # 额外生成 pprof 热点文本（本机很慢）
```

产物落在 `build/bench_out/`（`build/` 已被 `.gitignore` 忽略）：
`summary.tsv`（结果表）、`<场景>.log`（含 CPU 采样数）、`<场景>-raft-kv-N.prof`（CPU profile）。

---

## 2. 结果表（单次完整运行）

| 场景 | 请求数 | 错误 | QPS | avg | p50 | p90 | p99 | CPU采样(3节点合计) |
|---|---|---|---|---|---|---|---|---|
| **weak_pure**（弱读·纯读） | 3010 | 0 | 200.7 | 17.9 ms | 18.3 ms | 49.2 ms | 68.2 ms | 1564 |
| **weak_mixed**（弱读·混合） | 3009 | 0 | 200.6 | 78.0 ms | 82.2 ms | 172.7 ms | 343.8 ms | 1968 |
| **readindex_pure**（线性一致·纯读） | 3005 | 0 | 200.3 | 14.4 ms | 8.2 ms | 42.1 ms | 69.1 ms | 1443 |
| **readindex_mixed**（线性一致·混合） | 3006 | 0 | 200.4 | 72.2 ms | 80.0 ms | 196.8 ms | 302.5 ms | 1828 |
| **strong_pure**（强一致·纯读） | 3004 | 846 | 143.9 | 5316 ms | 8016 ms | 9398 ms | 9742 ms | —（未落盘） |
| **strong_mixed**（强一致·混合） | 3009 | 0 | 200.6 | 9027 ms | 10185 ms | 10433 ms | 10515 ms | —（未落盘） |

> strong 场景第二次观测（10s）：`strong_pure` QPS 200.8、avg 2829ms、0 错误；
> `strong_mixed` QPS 138.9、617 超时、avg 2654ms。**运行间波动大**，量级稳定在"秒级"。
>
> `strong` 场景 CPU profile 未落盘：节点被压满后 `SIGINT` 无法在 60s 内走完优雅退出流程
> （这本身说明**强一致读会把节点打满**）。

---

## 3. 关键对照实验（比模式对比更重要）

同一集群、同一条连接、**低负载 2 QPS 单线程**下测不同 RPC：

| RPC | 走什么路径 | 延迟 |
|---|---|---|
| `Ping` | 只回 pong，**在 brpc 工作线程回包** | **0.55 ms** |
| `Keys` | **在 brpc 工作线程**直接查 RocksDB 后回包 | **0.91 ms** |
| `Get` | post 到 raft 线程处理，**在 raft 线程回包** | **86.7 ms**（p50 85.5 / p99 88.2） |

服务端埋点（日志微秒时间戳）显示：**handler 进入 → 回复，只用了 ~12ms**。

再叠加两项独立测量：`fsync` 单次 **0.34ms**（strace 统计 20 次写共 22 次 fsync）、
brpc 往返 **~0.5ms**。

**结论**：慢的地方**不是网络、不是磁盘、也不是读协议**，而是
**"请求投递到 raft 单线程 + 在 raft 线程上 `done->Run()` 回包"这条跨线程路径**
（本机固定加了 ~75ms；分布很紧，是确定性延迟而非抖动）。

---

## 4. 可写进简历/面试的结论

1. **读模式对延迟的影响远小于"回包路径"**：weak / linearizable 的差异在十几毫秒噪声内；
   而"回包从 raft 线程产生"给每个请求固定加 ~75ms（Ping 0.55ms ↔ Get 86.7ms 对照）。
   → 优化优先级应该是**回包路径**，而不是换读模式。
2. **强一致读（读走共识）代价高 2 个数量级**：每次读都变成一条 Raft 日志，
   延迟从 ~18ms 涨到 **秒级**，并在高负载下超时（846 次）。**只适合极低频读**。
3. **弱读 vs 线性一致读**：纯读下两者延迟相当（都在十毫秒级）；
   线性一致读多一次"取安全水位 + 多数派确认"，换来**可线性化**语义。
   → 生产上应选 `ReadIndex`（强且不复制日志），而不是"读也走共识"。
4. **混合读写显著变慢**：纯读 ~18ms → 混合(20%写) ~78ms，且 p99 从 ~68ms 涨到 ~344ms。
   → **写请求（共识 + 落盘）才是延迟预算的大头**。
5. **服务端 CPU 很低**（weak/readindex：3 节点 15s 合计约 1.4~2.0s ≈ 9~13% CPU）：
   → 瓶颈是**等待**（跨线程/调度），不是算力；这解释了为什么"加机器/加线程"收益有限。

---

## 5. 关于火焰图（务必如实说明）

本环境**无法生成 SVG 火焰图**，原因如下（面试可诚实说明，反而是加分项）：

- brpc 自带的 `pprof` 版本**不支持 `--collapsed`**（FlameGraph 需要折叠栈）；
  `--svg/--dot` 需要 `addr2line` 符号化巨大的 `librocksdb/brpc`，单个 profile **超过 6 分钟甚至超时**；
- 容器内**没有 `perf`**（WSL2 一般也不支持硬件 PMU 事件）。

因此本仓库提供的是 **CPU 采样数（`interrupts=N`，1kHz）+ profile 文件 + 热点函数文本** 作为
可量化、可复现的替代证据。要在正常 Linux 机器上出火焰图，用下面任一命令：

```bash
# 方式一：perf（推荐，标准火焰图）
perf record -F 999 -g -p <leader_pid> -- sleep 30
perf script | /opt/FlameGraph/stackcollapse-perf.pl | /opt/FlameGraph/flamegraph.pl > read.svg

# 方式二：gperftools（pprof 需 >= 2.8 才支持 --collapsed）
pprof --collapsed ./raft-kv raft-kv-1.prof | /opt/FlameGraph/flamegraph.pl > read.svg
```

本机已用 `pprof --text` 得到的弱读纯读热点（Top6，说明**CPU 几乎不花在业务逻辑上**）：

```
syscall 13.9% | epoll_wait 7.3% | __libc_write 4.7% | _init 3.3% | readv 3.0% | tcache_get 1.8%
```

---

## 6. 面试防翻车清单

- ❌ 别说"我优化了读性能 X 倍"——**读模式之间本机差异在噪声内**。
- ✅ 可以说"我用对照实验定位到瓶颈是**回包跨线程**（Ping 0.55ms vs Get 86.7ms），
  并给出了量化证据"。
- ✅ 可以说"我做了 **3 模式 × 2 负载** 的对比压测，含 QPS/p50/p90/p99 与 CPU 采样，
  脚本与产物可复现"。
- ⚠️ 若被问"数字为什么这么大（几十 ms/秒级）"：如实说**这是 WSL2+docker 环境**，
  进程间/跨线程唤醒延迟被放大；同时说明瓶颈定位方法（Ping/Keys 对照 + 日志时间戳埋点）。
- ⚠️ 若被问"火焰图呢"：说明本环境的限制（详见第 5 节），并给出在正常机器上的复现命令。
