# Stage 2：主动策略容量响应与 Reactive 时段消融

本入口只包含 24 个主动策略 case + 4 个时段消融 case。每个新 case 一次 replay；没有 `--repeat`、formal pilot 或验证性重跑选项。Stage 1 的 24 个基线按 artifact SHA256 直接复用。

## 固定矩阵和时间语义

- N=16；585 / 1170 / 2340 / infinite。
- 主动策略：AFF + Future-Demand W=300s、Persistence h=300s/K=10、Recency q=0.9/decay=60s；仅 COPY。
- 消融 C：arrival <1500000ms 使用 REQ，之后持续 AFF；D 相反。capacity=2340。
- 从 trace 起点至与 Stage 1 相同的 visibility_end=3537000ms；之后排空已启动 transfer。评价窗口 [1500000,2700000)。没有状态 reset。
- 已在前段启动的 reactive COPY 保留原 arrival cohort；ready 单独按事件时间归属。
- 利用率沿用冻结的 **(ready, ready+300s]**、actual final-target hit、完整 Prefix 与 residency generation 匹配。完整观察且未用为 UNUSED；不足 300s 为 CENSORED，即使已有部分观察使用。固定窗口 UNUSED 与 still-resident / lifecycle unresolved 分开。

## 实现和性能边界

Stage 0、Stage 1 与 canonical 文件均不改写。独立 `src/simulator/task_main/stage2/` 使用冻结的请求完成、Saved/Load 修正、target preflight 和 COPY 状态机。

候选 holder/inflight coverage 在实际 cache/transfer mutation 上增量维护；时间窗正分数集合与结构可行集合取交集，免去每个请求遍历所有 seen Prefix。Persistence 和 Oracle 的分数仍用冻结 count，Recency 仍逐项 `math.fsum(exp(...))`，避免改变浮点排序。Oracle 必须输出的正候选 shortlist 仍有与输出量相关的开销，finite capacity 的 leaf-LRU preflight 也仍有成本。

cache 摘要复用精确 canonical JSON 片段，SHA256 内容不变；cache mutation 做增量不变量检查，最终做完整检查。候选诊断分段 gzip 保存且不丢行；最终记录使用 `.jsonl.gz`，不是第二遍 simulation。每个 workload 只解析一次 trace，immutable Prefix metadata 在 fork worker 间共享；批次完成后释放 worker，避免大 case 的 Python 内存长期保留。

合成性能测试可以验证局部行为和开销，不能作为真实 workload runtime 或内存上限。两台机器的容器配额是 10 CPU /100 GiB；启动示例使用 **8 workers**，runner 默认 4，可按实际 RSS 调整并通过 checkpoint resume 继续。

## 预检（无正式 replay）

在 Server A 仓库根目录：

```bash
mkdir -p results/task_main/stage2_active_capacity/preflight
PYTHONDONTWRITEBYTECODE=1 PYTEST_ADDOPTS='-p no:cacheprovider' /root/miniconda3/bin/python -m pytest -q tests/task_main tests/test_trace.py tests/test_cache.py --junitxml=results/task_main/stage2_active_capacity/preflight/pytest.xml
PYTHONDONTWRITEBYTECODE=1 /root/miniconda3/bin/python -m scripts.task_main_stage2.preflight benchmark --output results/task_main/stage2_active_capacity/preflight/synthetic_benchmark.json
PYTHONDONTWRITEBYTECODE=1 /root/miniconda3/bin/python -m scripts.task_main_stage2.preflight receipt --junit results/task_main/stage2_active_capacity/preflight/pytest.xml --benchmark results/task_main/stage2_active_capacity/preflight/synthetic_benchmark.json --output results/task_main/stage2_active_capacity/preflight/preflight.json
```

receipt 对代码、配置、测试内容做 hash，commit 本身不改变 receipt。正式 manifest 在 commit 后生成并绑定 Git HEAD。任一源码/配置变化会拒绝沿用旧 manifest。不要在运行期间切换 HEAD 或修改源码。

## Commit、同步与 HEAD 检查

只提交 Stage 2 范围；保留原有未跟踪的 Stage 1 结果报告。

```bash
cd /root/data2/llm-kv-active-balancing
git add -- src/simulator/task_main/stage2 scripts/task_main_stage2 configs/task_main/stage2_active_capacity tests/task_main/test_stage2_active_capacity.py docs/analysis/task_main_stage2_preflight.md
git commit -m "Add single-pass Stage 2 active capacity and reactive ablation study"
git push origin main
```

Server A 上同步 Server B；Ansible 只做状态检查，Git 同步用 SSH：

```bash
ssh -p 30513 root@ssh-cn-xibei2.ebcloud.com 'cd /root/llm-kv-active-balancing && git pull --ff-only'
ansible all -i /root/kv-ansible/inventory.ini -m shell -a 'cd {{ project_dir }} && git rev-parse HEAD && git status --short'
```

两服务器使用**同一份 portable preflight receipt**，不要在 B 生成不同 receipt 后混用 manifest：

```bash
ssh -p 30513 root@ssh-cn-xibei2.ebcloud.com 'mkdir -p /root/llm-kv-active-balancing/results/task_main/stage2_active_capacity/preflight'
scp -P 30513 results/task_main/stage2_active_capacity/preflight/preflight.json results/task_main/stage2_active_capacity/preflight/pytest.xml results/task_main/stage2_active_capacity/preflight/synthetic_benchmark.json root@ssh-cn-xibei2.ebcloud.com:/root/llm-kv-active-balancing/results/task_main/stage2_active_capacity/preflight/
```

先在各服务器生成配置快照和 manifest（不会开始 replay）：

```bash
# Server A
cd /root/data2/llm-kv-active-balancing
PYTHONDONTWRITEBYTECODE=1 /root/miniconda3/bin/python -m scripts.task_main_stage2.run prepare

# 从 A 准备 B
ssh -p 30513 root@ssh-cn-xibei2.ebcloud.com 'cd /root/llm-kv-active-balancing && PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m scripts.task_main_stage2.run prepare'
```

## 手动启动正式任务

本次实施不会代为执行以下 tmux 命令。每台只有自己的 14 个 case，不跑另一 workload。

Server A / Conversation：

```bash
tmux new-session -d -s stage2-conversation 'cd /root/data2/llm-kv-active-balancing && PYTHONDONTWRITEBYTECODE=1 /root/miniconda3/bin/python -u -m scripts.task_main_stage2.run run --workload conversation --workers 8 --checkpoint-seconds 600 --resume > results/task_main/stage2_active_capacity/run_20260923_01/conversation.console.log 2>&1'
```

在 Server A 窗口启动 Server B / ToolAgent：

```bash
ssh -p 30513 root@ssh-cn-xibei2.ebcloud.com "tmux new-session -d -s stage2-toolagent 'cd /root/llm-kv-active-balancing && PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -u -m scripts.task_main_stage2.run run --workload toolagent --workers 8 --checkpoint-seconds 600 --resume > results/task_main/stage2_active_capacity/run_20260923_01/toolagent.console.log 2>&1'"
```

## Status、日志和 resume

```bash
cd /root/data2/llm-kv-active-balancing
PYTHONDONTWRITEBYTECODE=1 /root/miniconda3/bin/python -m scripts.task_main_stage2.run status
ssh -p 30513 root@ssh-cn-xibei2.ebcloud.com 'cd /root/llm-kv-active-balancing && PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m scripts.task_main_stage2.run status'
ansible all -i /root/kv-ansible/inventory.ini -m shell -a 'tmux ls 2>/dev/null || true'

tail -f results/task_main/stage2_active_capacity/run_20260923_01/conversation.console.log
tail -f results/task_main/stage2_active_capacity/run_20260923_01/logs/conversation/FUTURE_DEMAND__585.log
ssh -p 30513 root@ssh-cn-xibei2.ebcloud.com 'tail -n 30 /root/llm-kv-active-balancing/results/task_main/stage2_active_capacity/run_20260923_01/logs/toolagent/FUTURE_DEMAND__585.log'
```

若 tmux 已退出，直接重复对应启动命令（包含 `--resume`）。已完成 case 每次都先验证 COMPLETE/config/head/artifact hash，**即使没传 --resume 也不会重复执行**。运行中的同一 case 有排他锁，重复启动不会重复 replay。

未完成 case 从最后一个原子 checkpoint 的完整时间戳批次恢复；最多重新执行上次 checkpoint 后未持久化的工作，不进行整 case 第二遍。ready queue、pending assignment、pins、cache、LRU、load、history、候选、diagnostics 全部恢复。checkpoint 和无损候选 segment 均校验 hash；缺失或身份不符会拒绝恢复。pickle checkpoint 仅用于本 runner 生成的本地可信文件。COMPLETE 后删除本 case 冗余 engine/segment 文件，正式输出保留全部记录。

## 完成后汇总（不 replay）

在 Server A 获取 B 的 **已完成 ToolAgent 结果**和主机性能记录：

```bash
cd /root/data2/llm-kv-active-balancing
scp -P 30513 -r root@ssh-cn-xibei2.ebcloud.com:/root/llm-kv-active-balancing/results/task_main/stage2_active_capacity/run_20260923_01/toolagent results/task_main/stage2_active_capacity/run_20260923_01/
scp -P 30513 root@ssh-cn-xibei2.ebcloud.com:/root/llm-kv-active-balancing/results/task_main/stage2_active_capacity/run_20260923_01/hosts/toolagent.json results/task_main/stage2_active_capacity/run_20260923_01/hosts/
PYTHONDONTWRITEBYTECODE=1 /root/miniconda3/bin/python -m scripts.task_main_stage2.run collect
```

不足 28 COMPLETE 时只写 INCOMPLETE 清单，不生成完整结果报告。全部验证通过后输出：

- `stage2_capacity_6strategy.csv`：48 行 evaluation 六策略容量比较及 ΔvsREQ。
- `stage2_active_only.csv`：24 行新增 proactive evaluation。
- `stage2_phase_metrics.csv`：208 行，包括各容量各策略及消融的四个时间阶段，含 total/reactive/proactive Wire。
- `stage2_per_pod.csv`、`stage2_timeseries.csv`：request/workload share、occupancy、eviction 与轨迹汇总。
- `stage2_proactive_diagnostics.csv`：opportunity/candidate/shortlist/attempt/start/ready/admission failure。
- `stage2_copy_utilization.csv`、`stage2_copy_utilization_summary.csv`：每 action 300s 归因、分母、censoring、new-page use、redundancy、lifecycle。
- `stage2_reactive_ablation.csv`、`stage2_reactive_diagnostics.csv`：A/B/C/D 与 C−A/B−D/B−C/D−A，arrival/start/ready 分别归属。
- `stage2_replica_layout.csv`：共同 external 全页集合上的最终 replica count，零副本也计入；hotspot coverage 明确未实现。
- `stage2_performance.csv`、`stage2_validation.json`、`stage2_results.md`：性能、完整性和最终报告。

每 case 的压缩 raw records 与 diagnostics 保留全部 occupancy mutation 轨迹和 eviction 原因。baseline 的轨迹仍位于原 Stage 1 diagnostics/raw summary 文件；不改写、不重放。

请求均衡与 miss-prefill 工作量均衡分开解释。Oracle 不是全局最优；收益和 total Wire 差值不直接等同净收益。Reactive 消融比较的是 routing/direct target/COPY 及整个历史状态的联合效应。完成 Stage 2 后停止，不进入 Stage 3/4。
