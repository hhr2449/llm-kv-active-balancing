# Stage 1 N=16 Capacity Scan — Preflight

**STAGE1_PREFLIGHT_PASS / READY_FOR_FORMAL**。正式矩阵 **0/24，未启动**；这不是 Stage 1 科学结果 PASS。

完成时间：2026-09-22T13:18:52.033050+00:00。HEAD：`17f4cccd8d93ef670d2167e7def5b5208aef99fc`。

## 范围与语义

冻结 2 workloads × 4 capacities（585/1170/2340/infinite）× 3 baselines（R_AFF/R_LEAST/R_REQ_KV_TASK），共 24 YAML 配置。N=16，proactive=NONE。沿用 Stage 0 actual-ready Saved、arrival-time Load reconciliation、独立完整 wire buffer、feasible-target-first 和 512-token logical page。旧 TaskMain/Stage 0 源码、协议和 canonical results 未修改。

优化仅位于 Stage 1：无容量缺口时不构建无用的 eviction scratch heap；在真实 unpin mutation 时更新缓存摘要，查询只读复用。未改路由、准入结果、淘汰次序、Saved、Load、transfer timing 或 random-Gini 定义。

## Benchmark 与长任务判断

两个代表 case 均在 Server A 上完成全 replay（Conversation 12,031 / ToolAgent 23,608 requests），没有将其当成正式矩阵结果。

| Workload | wall time | CPU | peak RSS | 实际写出块 I/O |
|---|---:|---:|---:|---:|
| conversation | 249.29s (4.15min) | 99.74% | 1160.50 MiB | 187.96 MiB |
| toolagent | 486.39s (8.11min) | 99.91% | 1553.67 MiB | 260.41 MiB |

计时包括 replay、诊断、结果序列化；trace 预解析在计时前。每个 benchmark 使用独立进程，RSS 是进程高水位。计时区间 read_bytes=0；这不表示输入无需读取，而是输入已预载/命中页缓存。原始 /proc I/O counters 见 benchmark.json。CPU 约 100% 表示单核瓶颈，不是整台 128 核服务器满载。

未优化 Conversation 全程 1133.70s；优化后 249.29s，**4.55×**。完整 RunResult 与 Stage 1 diagnostics SHA256 完全一致。两 workload 另有真实 trace 前 120s 的逐项等价验证。未优化 ToolAgent 的冗余全程参考测量在优化版完成后主动中止，未记为成功 benchmark；其 partial/日志及原因保留。

按各自 1170/REQ case ×12 粗估：Conversation 0.83h，ToolAgent 1.62h；24 case 单遍串行合计 **2.45h**。这只是代表 case 外推，不能保证 infinite/其他路由耗时相同。正式命令每 case 双遍，工作量约再翻倍；两服务器各 workers=2 可缩短墙钟时间，但不能据此承诺完成时间。达到用户长任务阈值，因此没有启动正式矩阵。

优化采用 case 级 fork 并行，每 worker 内保持单 case 的严格时间顺序；父进程 trace 只解析一次，immutable 输入通过 COW 共享。相同 N/count/window/seed 的 random-Gini 复用数学上相同的缓存结果。

## 验证

- **366 passed / 0 failed / 0 skipped**（Stage 0/TaskMain 345 项 + Stage 1 21 项）；涵盖 Saved/Load regression、N=16 全 Pod、finite/infinite、完整链、非变更查询、优化前后完整结果相等。
- 6 个正式前小 Pilot：每 workload AFF@585、REQ@585、REQ@infinite，均为原始 trace [0,120s)，每 case 双遍确定性 PASS。原时间戳保留，不声称短 Pilot 实际覆盖主评价窗口；[25,45) 边界和 ready/start 分离另由 synthetic tests 验证。
- R_LEAST@585 两 workload 另有真实短 trace 双遍检查，三种 routing 都已执行。
- skipped/truncated=0；finite occupancy 合法；infinite eviction/rejection=0；proactive action=0；16 Pod 字段齐全。无限容量比率、空 cohort 比率/quantile 的 null 是明确定义，不是 NaN 或缺失计算。
- 恢复测试实际注入 KeyboardInterrupt：partial 不被认作成功，重启完成；再次 --resume 验证哈希并跳过；损坏输出或不同配置身份拒绝；运行中 status 可读取而不争用 runner 锁。
- Pilot 的真实 --resume 已验证跳过 3 个 Conversation completed case，不重复运行。
- canonical 最终 artifact **105/105 SHA256 匹配**。已有 tracked 文件无 diff。

实现过程中发现并修正 runner staging 目录与原 write_outputs 的 exist_ok=False 冲突；现在标准 replay 输出写在 case/replay/，case 目录仍原子发布。初次失败 benchmark 的 partial 与 FAILED checkpoint 保留，不进入正式结果。

## 输出和诊断口径

每个 case 有 config、identity（HEAD/source hashes/config hash/run id/workload/N/capacity/eval/semantic version）、performance、stage1_validation、COMPLETE；标准完整 records/summary 位于 replay/。

- 全程连续演化，输出 pre [0,25)、evaluation [25,45)、post、full 和 5 分钟窗口；25min 不 reset。
- 请求分布与 miss-prefill 工作量分布分别输出；含 per-Pod share、Top1、active Pods、Gini/random-Gini/Skew、max Pod workload share。
- occupancy trajectory 在 replay/summary.json 的 stage0_diagnostics；time-weighted/peak/ratio、三类 ordinary eviction event/pages、capacity probe/fallback 原因均输出。
- Wire 按 start；ready events 按 ready；completed-start-cohort 另列。边界 in-flight 使用左极限。
- 有效副本分布统计共同 external history 已观察的完整页在 full-run 结束时的 published replicas，含零副本；没有按各策略 hit 选热点。Stage 0 未实现 hotspot coverage，本轮不伪造该指标。
- 容量失败计数区分 target preflight 次数和 request cache admission；不能当作请求失败率。

## 双服务器运行

Ansible 只读检查：A/B 起始 HEAD 一致且均干净；两台可见 CPU 均为 128、内存约 1 TiB。Server B 默认 shell 没有 python，系统 python3 缺 PyYAML；项目 .venv/bin/python 为 Python 3.10.16，PyYAML 6.0.3、pytest 8.4.2，Stage 0 import PASS。Server B 的 ToolAgent trace SHA256 也与冻结配置一致。未安装包或修改 Ansible/SSH 配置。

新 Stage 1 文件尚在 A 的 untracked 工作区；未 commit/push/pull。须先提交推送再同步 B，并再次用 check_repo.yml 核对 HEAD。预检凭据按源码/配置/测试内容哈希验证，commit 后 HEAD 变化不使等内容凭据失效；每个新正式 run 的 manifest 记录当时实际 HEAD。

完整可复制的 **commit/push、B pull、A/B tmux、status、resume、log 和最终汇总命令**见 [Stage 1 README](../../scripts/task_main_stage1/README.md)。建议每台 workers=2，每 case --repeat 双遍；运行目录 run_20260922_01。不要把长实验绑定到 Ansible 前台会话。

当前只提供 case 级 checkpoint。两个代表 benchmark <30min；未测量全部 infinite case，不保证其单 case 时间。已完成 case 永不因 resume 重跑，未完成 case 从头重启。

## 交付与 Git

- [预检 JSON](../../results/task_main/stage1_n16_capacity/stage1_validation.json)
- [Benchmark JSON](../../results/task_main/stage1_n16_capacity/benchmark.json)
- [可随代码同步的 preflight receipt](../../configs/task_main/stage1_n16_capacity/preflight.json)
- [366 项 JUnit](../../results/task_main/stage1_n16_capacity/pytest_release.xml)
- 有效 Pilot/短等价证据：results/task_main/stage1_n16_capacity/release/；早期开发/benchmark 目录保留，不能混入正式矩阵。

新增文件列表（没有修改既有 tracked 文件）：

- `configs/task_main/stage1_n16_capacity/conversation/R_AFF__1170.yaml`
- `configs/task_main/stage1_n16_capacity/conversation/R_AFF__2340.yaml`
- `configs/task_main/stage1_n16_capacity/conversation/R_AFF__585.yaml`
- `configs/task_main/stage1_n16_capacity/conversation/R_AFF__infinite.yaml`
- `configs/task_main/stage1_n16_capacity/conversation/R_LEAST__1170.yaml`
- `configs/task_main/stage1_n16_capacity/conversation/R_LEAST__2340.yaml`
- `configs/task_main/stage1_n16_capacity/conversation/R_LEAST__585.yaml`
- `configs/task_main/stage1_n16_capacity/conversation/R_LEAST__infinite.yaml`
- `configs/task_main/stage1_n16_capacity/conversation/R_REQ_KV_TASK__1170.yaml`
- `configs/task_main/stage1_n16_capacity/conversation/R_REQ_KV_TASK__2340.yaml`
- `configs/task_main/stage1_n16_capacity/conversation/R_REQ_KV_TASK__585.yaml`
- `configs/task_main/stage1_n16_capacity/conversation/R_REQ_KV_TASK__infinite.yaml`
- `configs/task_main/stage1_n16_capacity/preflight.json`
- `configs/task_main/stage1_n16_capacity/stage0_freeze.json`
- `configs/task_main/stage1_n16_capacity/toolagent/R_AFF__1170.yaml`
- `configs/task_main/stage1_n16_capacity/toolagent/R_AFF__2340.yaml`
- `configs/task_main/stage1_n16_capacity/toolagent/R_AFF__585.yaml`
- `configs/task_main/stage1_n16_capacity/toolagent/R_AFF__infinite.yaml`
- `configs/task_main/stage1_n16_capacity/toolagent/R_LEAST__1170.yaml`
- `configs/task_main/stage1_n16_capacity/toolagent/R_LEAST__2340.yaml`
- `configs/task_main/stage1_n16_capacity/toolagent/R_LEAST__585.yaml`
- `configs/task_main/stage1_n16_capacity/toolagent/R_LEAST__infinite.yaml`
- `configs/task_main/stage1_n16_capacity/toolagent/R_REQ_KV_TASK__1170.yaml`
- `configs/task_main/stage1_n16_capacity/toolagent/R_REQ_KV_TASK__2340.yaml`
- `configs/task_main/stage1_n16_capacity/toolagent/R_REQ_KV_TASK__585.yaml`
- `configs/task_main/stage1_n16_capacity/toolagent/R_REQ_KV_TASK__infinite.yaml`
- `scripts/task_main_stage1/README.md`
- `scripts/task_main_stage1/__init__.py`
- `scripts/task_main_stage1/diagnostics.py`
- `scripts/task_main_stage1/optimized.py`
- `scripts/task_main_stage1/preflight.py`
- `scripts/task_main_stage1/run.py`
- `tests/task_main/test_stage1_capacity.py`
- `docs/analysis/task_main_stage1_preflight.md`

建议 commit message：`add N16 Stage 1 baseline capacity runner and validated diagnostics`。

**停止于正式运行前。** 未选择 C_low/C_ref，未执行 Stage 2，未将 benchmark/Pilot 当作 24-case 正式结论。
