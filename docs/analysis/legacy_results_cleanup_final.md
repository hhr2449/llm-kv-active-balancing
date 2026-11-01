# OLD RESULTS CLEANUP

最终状态：**OLD_RESULTS_CLEANUP_PASS**。完成时间（UTC）：2026-09-21T15:16:09.117100+00:00。

## 删除范围

按用户最新授权，仅保留 results/task_main 与 results/trace_audit；删除以下24个顶层旧结果目录：

- `results/development_validation`
- `results/final_p0`
- `results/formal_v1`
- `results/full_o2a`
- `results/full_o2a_analysis_replay`
- `results/kernel_v0`
- `results/kernel_v1`
- `results/kernel_v2`
- `results/kernel_v3a`
- `results/kernel_v3b`
- `results/o1_development`
- `results/o1_diagnostics`
- `results/o2_oracle`
- `results/p1_history`
- `results/p25_strong_reactive_history`
- `results/p2_strong_reactive`
- `results/p31_exact_candidate_coverage`
- `results/p32_action_ranking`
- `results/p32_action_ranking_corrected`
- `results/p3_copy_predictability`
- `results/p4_validation`
- `results/summary`
- `results/taskmain_evaluation`
- `results/taskmain_sweeps`

另删除 `checkpoints/full_o2a`、已为空且无当前TaskMain依赖的 `logs/`，以及全仓16个 `__pycache__` 目录与生成字节码。缓存清理按本轮明确授权包含TaskMain目录中的缓存，源码、配置、输入和正式结果保留。

共删除 **1279 个文件**：旧结果 822，checkpoint 294，生成缓存 163；文件计数无重复。清理后全仓 `__pycache__` / `*.pyc` / `*.pyo` 均为0。

**NO BACKUP**：本轮未创建备份、归档或复制旧结果；未恢复旧结果、重跑旧实验、修改源码/协议或提交Git commit。

## 空间

前后均实际执行 `du -sh results`、`du -sh .`、`du -h --max-depth=1 results | sort -h`；另用 `du -s -B1` 获取allocated字节。after包含本文。

| 项目 | before bytes | after bytes | 净减少 bytes |
|---|---:|---:|---:|
| repo | 95930109952 | 89963933696 | 5966176256 |
| results | 95899574272 | 89946914816 | 5952659456 |

仓库占用：**89.341877 → 83.785442 GiB**；净减少 **5.556435 GiB**。这是du占用变化，不等同于精确df可用空间变化。

### before

```text
90G	results
90G	.

16K	results/trace_audit
64K	results/summary
92K	results/p4_validation
676K	results/p32_action_ranking_corrected
764K	results/full_o2a
2.0M	results/o2_oracle
2.4M	results/p3_copy_predictability
5.2M	results/p32_action_ranking
6.7M	results/final_p0
15M	results/kernel_v0
37M	results/full_o2a_analysis_replay
59M	results/kernel_v1
62M	results/o1_diagnostics
74M	results/kernel_v2
93M	results/p2_strong_reactive
133M	results/o1_development
138M	results/development_validation
188M	results/formal_v1
191M	results/p1_history
251M	results/kernel_v3b
272M	results/p25_strong_reactive_history
304M	results/kernel_v3a
384M	results/taskmain_evaluation
623M	results/taskmain_sweeps
2.8G	results/p31_exact_candidate_coverage
84G	results/task_main
90G	results
```

### after

```text
84G	results
84G	.

16K	results/trace_audit
84G	results
84G	results/task_main
```

## 三项验证

1. TaskMain pytest：**303 passed / 0 failed / 0 skipped / 0 errors，13.28秒**，进程退出码0。运行指定的tests/task_main、tests/test_trace.py、tests/test_cache.py，设置PYTHONDONTWRITEBYTECODE=1和PYTEST_ADDOPTS='-p no:cacheprovider'；JUnit记录在仓库外临时目录。
2. Final artifacts：**105/105 存在，SHA256全部一致**。依据final_results/consolidation_manifest.json与final_figures/figure_manifest.json逐项核对，未重新生成产物。
3. TaskMain dependency smoke：**PASS**。实际导入69个当前模块；64条config输入路径、15条静态config/data/script源码路径均存在；TaskMain、D1、D2 dependency audit全部PASS，无missing import/config/data/script。

## results最终目录

实际执行 `find results -maxdepth 1 -mindepth 1 -type d | sort`：

```text
results/task_main
results/trace_audit
```

无其他顶层目录或文件残留。删除和指定验证已完成，本轮停止。
