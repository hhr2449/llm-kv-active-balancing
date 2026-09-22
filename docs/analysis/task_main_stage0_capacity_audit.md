# Stage 0：实施核查与 workload 容量画像

**STAGE0_PASS**。345 tests passed，0 failed/skipped；14 个 synthetic case 各双遍确定性验证通过；105 个既有最终 artifact SHA256 一致。没有运行正式策略实验或 Stage 1。

HEAD：`4ab6f9ee2ab49679bbc5f3a9b51c866505fea3c7`。新增版本 `TASK_MAIN_CAPACITY_STAGE0_V1`，原源码、协议、正式结果保持本轮开始时内容。六个此前已暂存的历史源码文件保持原状。

## 真实问题与版本边界

reactive COPY ready 时，旧实现可能继续使用启动时的 committed hit，少计最终目标上的实际最长命中。人工例中 Saved 从 1024 修正到 1112 tokens；Load 校正同一 arrival-time entry，pending 时仍使用已保证命中的 provisional miss，避免未来信息。旧 Load 查询会 prune/推进时钟，新版改为只读。ordinary eviction 新增事件数、页数及三类原因；reactive overload gate 与 Cost-aware Score gate 分开。

可能受 Saved 修复影响的是 R_REQ_KV_TASK（主实验 Line 2、D1 theta/N reactive case，包括窗口前状态传播）。未重跑正式 case，影响幅度未知。后续须使用新入口并标明语义版本，不能混合旧 canonical 结果。active runtime KV 未建模。

## Workload 画像

FULL_REPLAY=[0,3537000) ms；EVALUATION=[1500000,2700000) ms。页面为当前 512-token logical page。三个容量 585/1170/2340 的 full-path-over-capacity 请求数、请求占比、输入 tokens 占比在四组中均为 0。

| workload | scope | requests | prompt tokens P50/P90/P99/max | prompt pages P50/P90/P99/max |
|---|---|---:|---|---|
| conversation | FULL_REPLAY | 12031 | 6909 / 27367 / 85400.4 / 126195 | 14 / 54 / 167 / 247 |
| conversation | EVALUATION | 4275 | 6418 / 24800.2 / 81242.52 / 124847 | 13 / 49 / 158.78 / 244 |
| toolagent | FULL_REPLAY | 23608 | 6346 / 16805.8 / 61660.78 / 126195 | 13 / 33 / 121 / 247 |
| toolagent | EVALUATION | 8375 | 6311 / 15589.2 / 56011.36 / 124847 | 13 / 31 / 109.78 / 244 |

| workload | scope | window | unique pages P50/P90/P99/max |
|---|---|---:|---|
| conversation | FULL_REPLAY | 60s | 4378 / 4939.8 / 5474.68 / 5980 |
| conversation | EVALUATION | 60s | 4236.5 / 4751.8 / 5131.64 / 5630 |
| conversation | FULL_REPLAY | 300s | 18651 / 19603 / 19957.56 / 20133 |
| conversation | EVALUATION | 300s | 18315.5 / 19105.5 / 19698.04 / 19817 |
| toolagent | FULL_REPLAY | 60s | 4118 / 4621.4 / 5006.64 / 5187 |
| toolagent | EVALUATION | 60s | 3993 / 4518.5 / 4806.18 / 5006 |
| toolagent | FULL_REPLAY | 300s | 17218 / 17866.8 / 18287.64 / 18690 |
| toolagent | EVALUATION | 300s | 17066 / 17737.6 / 18118.59 / 18309 |

| workload | scope | sharing | mean references/page | reuse P50/P90/P99 (s) | reuse ≤60s / ≤300s |
|---|---|---:|---:|---|---|
| conversation | FULL_REPLAY | 38.19% | 1.62 | 114 / 519 / 1578 | 28.19% / 78.52% |
| conversation | EVALUATION | 32.58% | 1.48 | 126 / 531 / 1509 | 27.96% / 76.03% |
| toolagent | FULL_REPLAY | 58.56% | 2.41 | 0 / 243 / 609 | 69.23% / 92.65% |
| toolagent | EVALUATION | 56.29% | 2.29 | 0 / 267 / 627 | 69.60% / 91.64% |

| workload | scope | bucket | requests | input tokens | request share | token share |
|---|---|---|---:|---:|---:|---:|
| conversation | FULL_REPLAY | [20k,60k) | 1712 | 52688741 | 14.230% | 36.389% |
| conversation | FULL_REPLAY | [60k,120k) | 278 | 22944324 | 2.311% | 15.846% |
| conversation | FULL_REPLAY | [120k,300k] | 17 | 2083424 | 0.141% | 1.439% |
| conversation | FULL_REPLAY | >300k | 0 | 0 | 0.000% | 0.000% |
| conversation | EVALUATION | [20k,60k) | 530 | 15999218 | 12.398% | 33.583% |
| conversation | EVALUATION | [60k,120k) | 84 | 7038994 | 1.965% | 14.775% |
| conversation | EVALUATION | [120k,300k] | 3 | 371170 | 0.070% | 0.779% |
| conversation | EVALUATION | >300k | 0 | 0 | 0.000% | 0.000% |
| toolagent | FULL_REPLAY | [20k,60k) | 1543 | 47458819 | 6.536% | 23.386% |
| toolagent | FULL_REPLAY | [60k,120k) | 240 | 19727870 | 1.017% | 9.721% |
| toolagent | FULL_REPLAY | [120k,300k] | 12 | 1473475 | 0.051% | 0.726% |
| toolagent | FULL_REPLAY | >300k | 0 | 0 | 0.000% | 0.000% |
| toolagent | EVALUATION | [20k,60k) | 475 | 14192907 | 5.672% | 20.624% |
| toolagent | EVALUATION | [60k,120k) | 75 | 6342090 | 0.896% | 9.216% |
| toolagent | EVALUATION | [120k,300k] | 2 | 249243 | 0.024% | 0.362% |
| toolagent | EVALUATION | >300k | 0 | 0 | 0.000% | 0.000% |

585 大于观察到的最大单请求 247 pages，但 585/1170/2340 都小于两 workload 的全局 60s 工作集 P50；300s 工作集更大。这是全局需求画像，不能直接当作 N=16 的每 Pod 驻留需求，也不能据此选 C_low/C_ref。>300k 无样本，120k–300k 样本很少。

## 性能与交付

先完成各 120s 输入窗口 benchmark，再运行全量。Conversation 1.151s / peak RSS 86.27 MiB；ToolAgent 1.682s / 95.19 MiB。使用递归 prefix identity intern、增量 Counter/deque 与 1s 时间网格，避免请求乘以全部 prefix 的扫描。无需 tmux/checkpoint；此耗时不代表后续 closed-loop 策略实验性能。

完整交付：

- [实施核查（10 项及代码证据）](../../results/task_main/stage0_capacity_audit/implementation_audit.md)
- [字段精确定义](../../results/task_main/stage0_capacity_audit/diagnostics_schema.md)
- [核心画像 CSV](../../results/task_main/stage0_capacity_audit/workload_capacity_profile.csv)
- [工作集 CSV](../../results/task_main/stage0_capacity_audit/working_set_summary.csv)
- [桶支持 CSV](../../results/task_main/stage0_capacity_audit/prompt_bucket_support.csv)
- [验证记录](../../results/task_main/stage0_capacity_audit/stage0_validation.json)
- [独立运行与合并命令](../../scripts/task_main_stage0/README.md)

`profiles/` 保存时间序列、reference-count/reuse histograms；`smoke/` 保存 14 cases。画像窗口另以完整祖先 tuple 暴力算法独立复核 32 个采样点。合并脚本输出与现有三份汇总 CSV 逐字节一致。

具备开始 Stage 1 的实施和输入画像条件；需使用新版本入口并由用户决定实验 case。未启动 Stage 1。

## 本轮文件清单

新增文件；未改写已有文件：

- `docs/analysis/task_main_stage0_capacity_audit.md`
- `scripts/task_main_stage0/README.md`
- `scripts/task_main_stage0/__init__.py`
- `scripts/task_main_stage0/combine_profiles.py`
- `scripts/task_main_stage0/profile_workload.py`
- `scripts/task_main_stage0/run_smoke.py`
- `src/simulator/task_main/stage0/__init__.py`
- `src/simulator/task_main/stage0/cache.py`
- `src/simulator/task_main/stage0/config.py`
- `src/simulator/task_main/stage0/diagnostics.py`
- `src/simulator/task_main/stage0/engine.py`
- `src/simulator/task_main/stage0/isolation.py`
- `src/simulator/task_main/stage0/load.py`
- `tests/task_main/test_stage0_capacity.py`

Stage 0 数据与报告在 results 下，沿用仓库 ignore 规则，不自动暂存。没有执行 git add/commit/push。建议 commit message：`add Stage 0 capacity audit and versioned workload diagnostics`。
