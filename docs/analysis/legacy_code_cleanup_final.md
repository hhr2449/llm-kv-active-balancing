# Legacy Code Cleanup Final

最终状态：**LEGACY_CODE_CLEANUP_PASS**

完成时间（UTC）：2026-09-21T15:30:27.302212+00:00。仓库：`/root/data2/llm-kv-active-balancing`。

## 1. 执行范围与删除数量

按本轮明确授权完成最后一次旧代码、配置、脚本、测试、文档清理。涉及旧 kernel/service simulator、TaskMain-v1.1、O1/O2/Full O2-A、P1/P2/P2.5 Strong Reactive、P3/P31/P32/P4 及中间清理文档。

实际删除 **145 个文件**：106 个 tracked、39 个 untracked；清除 22 个目录。计数仅包括本轮开始时存在并实际删除的文件，不重复计入此前旧结果清理。没有创建旧实验备份。

| 类别 | 删除文件数 |
|---|---:|
| configs | 99 |
| scripts | 6 |
| src | 15 |
| tests | 4 |
| docs | 16 |
| 生成缓存 .pytest_cache | 5 |
| 合计 | 145 |

旧配置目录 `configs/formal`、`o2_oracle`、`p1_history`、`p2_strong_reactive`、`p25_strong_reactive_history`、`taskmain_v1.1` 及 19 个顶层 kernel YAML 已删除。旧脚本 `scripts/p2`、`scripts/p25`、`scripts/p4` 已删除。

7 份旧研究分析文档、6 份中间清理文档、旧协议 `taskmain_v1.1.md` 已删除。`docs/README.md` 仅为旧 v1.1 待运行实验注册页，`protocol_changelog.md` 仅描述旧 v1.1/byte-budget 语义，也一并删除。保留 `legacy_results_cleanup_final.md` 和全部当前 TaskMain 分析/协议文档。

4 个根目录清理文件 `legacy_review_deep_audit.csv`、`legacy_review_cleanup_plan.sh.DRY_RUN`、`legacy_cleanup_candidates.csv`、`legacy_cleanup_delete.sh.DRY_RUN` 在本轮开始时已不存在，未计入删除数。

tracked 文件使用精确路径 `git rm --`。明确获准删除的两个已修改旧测试 `tests/test_pressure.py`、`tests/test_proactive.py`，先精确删除工作文件，再使用不带 `-f` 的 `git rm --` 暂存删除。已有暂存删除保留，未执行 commit/push。

## 2. 保留的共享模块和未提交文件

- `src/__init__.py`、`src/simulator/__init__.py`：当前 Python 包加载链。
- `src/simulator/config.py`：父包初始化实际导入，且已有未提交修改。
- `src/simulator/trace.py`：当前 TaskMain Trace 与 trace 回归依赖。
- `src/simulator/cache.py`：指定共享 cache 回归依赖。
- `src/simulator/engine.py`、`metrics.py`、`oracle.py`、`pressure.py`、`strategies.py`：虽不在当前依赖闭包中，但含未提交修改；按照本轮安全优先规则原样保留。
- `scripts/audit_mooncake_trace.py`、`results/trace_audit`、原始输入及全部 `note/`：原始输入能力与项目证据。

保留的旧 dirty 核心文件仅保全未提交内容；它们仍可能引用本轮删除的旧模块，不代表旧 simulator 仍可运行。当前 TaskMain 不依赖这些旧核心文件。包括共享 config 在内的 6 个保留 dirty 源码 SHA256 均与删除前一致。

## 3. 安全与回归结果

保护集合与删除集合交集为空。机械 AST 检查包含父包初始化，当前代码、指定测试及输入审计脚本闭包共 100 个 Python 文件；删除集合不含闭包文件。当前代码/配置/测试未出现删除路径的直接字面量引用。未修改当前实现来绕过依赖。

指定回归命令（额外 JUnit 输出仅写入仓库外临时目录）：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTEST_ADDOPTS='-p no:cacheprovider' \
python -m pytest -q tests/task_main tests/test_trace.py tests/test_cache.py
```

结果：**303 passed / 0 failed / 0 skipped / 0 errors，12.89 秒**，exit code=0。JUnit 的 303 个 testcase 已核对，测试数量不变。

- **最终 artifact：105/105 存在，105/105 SHA256 匹配。** 读取 `consolidation_manifest.json` 与 `figure_manifest.json` 验证，没有重新生成结果或图像。
- **受保护小文件：175/175 SHA256 匹配。** 删除前清单保存在 `/tmp/taskmain_protected_pre_cleanup_sha256.txt`，覆盖当前 source/scripts/configs/tests/protocol/analysis docs。
- **Dependency smoke：PASS。** 实际导入 69 个当前模块，检查 64 个配置输入路径、15 个静态文件路径；没有发现 missing import/config/data/script。TaskMain、D1、D2 三项 dependency audit 全部 PASS。静态路径检查不宣称穷尽任意动态构造。
- **保护区：2,875 个文件全部存在。** 最终复核中，既有非删除文件除 `.git/index` 的预期更新及 `.git/FETCH_HEAD` 的 mtime 变化外，size/mtime_ns/mode/inode 均不变。本轮未调用 git fetch；FETCH_HEAD 的 size/mode/inode 不变，时间戳变化来源未确认，因此不宣称 Git 元数据全部不变。未对全部大体积结果重新计算 SHA256。
- 本轮已有文件缺失集合恰好等于 145 个删除文件；仓库内只新增本报告。没有残留 `__pycache__`、`.pyc`、`.pyo` 或 `.pytest_cache`。

## 4. 删除文件清单

### configs

```text
configs/formal/aff_10000_v1.yaml
configs/formal/aff_5000_v1.yaml
configs/formal/aff_infinite_diagnostic_v1.yaml
configs/formal/b0_5000_v1.yaml
configs/formal/b0_development_10000_v1.yaml
configs/formal/b0_development_5000_v1.yaml
configs/formal/b0_infinite_diagnostic_v1.yaml
configs/formal/b0_nominal_v1.yaml
configs/formal/o1_budget_protocol_v1.yaml
configs/formal/o1_development_nominal.yaml
configs/formal/o1_mechanism_diagnostics_v1.yaml
configs/formal/sensitivity_protocol_v1.yaml
configs/formal/taskmain_v1.1.yaml
configs/kernel_v0.yaml
configs/kernel_v1_1000.yaml
configs/kernel_v1_10000.yaml
configs/kernel_v1_5000.yaml
configs/kernel_v2_aff_10000.yaml
configs/kernel_v2_aff_5000.yaml
configs/kernel_v2_aff_infinite.yaml
configs/kernel_v2_least_10000.yaml
configs/kernel_v2_least_infinite.yaml
configs/kernel_v3a_req_10000.yaml
configs/kernel_v3a_req_10000_trace_page.yaml
configs/kernel_v3a_req_5000.yaml
configs/kernel_v3a_req_infinite.yaml
configs/kernel_v3b_b0_10000.yaml
configs/kernel_v3b_b0_5000.yaml
configs/kernel_v3b_sens_absolute_1000.yaml
configs/kernel_v3b_sens_cache_01.yaml
configs/kernel_v3b_sens_relative_20.yaml
configs/kernel_v3b_sens_timeout_1000.yaml
configs/o2_oracle/full_o2a_conversation.yaml
configs/o2_oracle/full_o2a_toolagent.yaml
configs/o2_oracle/o2a_closed_loop_smoke.yaml
configs/p1_history/conversation_v1_persistence.yaml
configs/p1_history/conversation_v1_persistence_costaware.yaml
configs/p1_history/toolagent_v1_persistence.yaml
configs/p1_history/toolagent_v1_persistence_costaware.yaml
configs/p25_strong_reactive_history/conversation_reactive_ect_persistence.yaml
configs/p25_strong_reactive_history/conversation_reactive_ect_persistence_costaware.yaml
configs/p25_strong_reactive_history/toolagent_reactive_ect_persistence.yaml
configs/p25_strong_reactive_history/toolagent_reactive_ect_persistence_costaware.yaml
configs/p2_strong_reactive/conversation_reactive_ect_v1.yaml
configs/p2_strong_reactive/toolagent_reactive_ect_v1.yaml
configs/taskmain_v1.1/conversation/E01_r_aff.yaml
configs/taskmain_v1.1/conversation/E02_r_req_kv_task.yaml
configs/taskmain_v1.1/conversation/E03_oracle.yaml
configs/taskmain_v1.1/conversation/E04_persist_h1_k10.yaml
configs/taskmain_v1.1/conversation/E05_persist_h5_k10.yaml
configs/taskmain_v1.1/conversation/E06_recency_q09.yaml
configs/taskmain_v1.1/conversation/E07_cost_aware.yaml
configs/taskmain_v1.1/matched/M01_conversation_r_req_kv_v1.yaml
configs/taskmain_v1.1/matched/M02_conversation_matched_o1.yaml
configs/taskmain_v1.1/matched/M03_toolagent_r_req_kv_v1.yaml
configs/taskmain_v1.1/matched/M04_toolagent_matched_o1.yaml
configs/taskmain_v1.1/sweeps/copy_vs_move/conversation_move.yaml
configs/taskmain_v1.1/sweeps/copy_vs_move/toolagent_move.yaml
configs/taskmain_v1.1/sweeps/num_pods/conversation_n2_cost_aware.yaml
configs/taskmain_v1.1/sweeps/num_pods/conversation_n2_oracle.yaml
configs/taskmain_v1.1/sweeps/num_pods/conversation_n2_persist_h1_k10.yaml
configs/taskmain_v1.1/sweeps/num_pods/conversation_n2_persist_h5_k10.yaml
configs/taskmain_v1.1/sweeps/num_pods/conversation_n2_r_aff.yaml
configs/taskmain_v1.1/sweeps/num_pods/conversation_n2_r_req_kv_task.yaml
configs/taskmain_v1.1/sweeps/num_pods/conversation_n2_recency_q09.yaml
configs/taskmain_v1.1/sweeps/num_pods/toolagent_n2_cost_aware.yaml
configs/taskmain_v1.1/sweeps/num_pods/toolagent_n2_oracle.yaml
configs/taskmain_v1.1/sweeps/num_pods/toolagent_n2_persist_h1_k10.yaml
configs/taskmain_v1.1/sweeps/num_pods/toolagent_n2_persist_h5_k10.yaml
configs/taskmain_v1.1/sweeps/num_pods/toolagent_n2_r_aff.yaml
configs/taskmain_v1.1/sweeps/num_pods/toolagent_n2_r_req_kv_task.yaml
configs/taskmain_v1.1/sweeps/num_pods/toolagent_n2_recency_q09.yaml
configs/taskmain_v1.1/sweeps/oracle_window/conversation_w1.yaml
configs/taskmain_v1.1/sweeps/oracle_window/conversation_w30.yaml
configs/taskmain_v1.1/sweeps/oracle_window/conversation_w5.yaml
configs/taskmain_v1.1/sweeps/oracle_window/toolagent_w1.yaml
configs/taskmain_v1.1/sweeps/oracle_window/toolagent_w30.yaml
configs/taskmain_v1.1/sweeps/oracle_window/toolagent_w5.yaml
configs/taskmain_v1.1/sweeps/persistence_h_k/conversation_h1_k20.yaml
configs/taskmain_v1.1/sweeps/persistence_h_k/conversation_h1_k5.yaml
configs/taskmain_v1.1/sweeps/persistence_h_k/conversation_h5_k20.yaml
configs/taskmain_v1.1/sweeps/persistence_h_k/conversation_h5_k5.yaml
configs/taskmain_v1.1/sweeps/persistence_h_k/toolagent_h1_k20.yaml
configs/taskmain_v1.1/sweeps/persistence_h_k/toolagent_h1_k5.yaml
configs/taskmain_v1.1/sweeps/persistence_h_k/toolagent_h5_k20.yaml
configs/taskmain_v1.1/sweeps/persistence_h_k/toolagent_h5_k5.yaml
configs/taskmain_v1.1/sweeps/recency/conversation_q08.yaml
configs/taskmain_v1.1/sweeps/recency/toolagent_q08.yaml
configs/taskmain_v1.1/sweeps/theta/conversation_theta15.yaml
configs/taskmain_v1.1/sweeps/theta/conversation_theta30.yaml
configs/taskmain_v1.1/sweeps/theta/toolagent_theta15.yaml
configs/taskmain_v1.1/sweeps/theta/toolagent_theta30.yaml
configs/taskmain_v1.1/toolagent/E08_r_aff.yaml
configs/taskmain_v1.1/toolagent/E09_r_req_kv_task.yaml
configs/taskmain_v1.1/toolagent/E10_oracle.yaml
configs/taskmain_v1.1/toolagent/E11_persist_h1_k10.yaml
configs/taskmain_v1.1/toolagent/E12_persist_h5_k10.yaml
configs/taskmain_v1.1/toolagent/E13_recency_q09.yaml
configs/taskmain_v1.1/toolagent/E14_cost_aware.yaml
```

### scripts

```text
scripts/p2/audit_b0_gates.py
scripts/p2/run_p2_strong_reactive.sh
scripts/p2/summarize_p2_strong_reactive.py
scripts/p25/run_p25_strong_reactive_history.sh
scripts/p25/summarize_p25.py
scripts/p4/audit_validation_data.py
```

### src

```text
src/simulator/events.py
src/simulator/full_o2a.py
src/simulator/full_o2a_analysis.py
src/simulator/o2a.py
src/simulator/o2a_smoke.py
src/simulator/o2p.py
src/simulator/p31_candidate_coverage.py
src/simulator/p32_action_ranking.py
src/simulator/p3_predictability.py
src/simulator/pod.py
src/simulator/reactive_ect.py
src/simulator/routing.py
src/simulator/service.py
src/simulator/tickets.py
src/simulator/transfer.py
```

### tests

```text
tests/test_diagnostics.py
tests/test_metrics.py
tests/test_pressure.py
tests/test_proactive.py
```

### docs

```text
docs/README.md
docs/analysis/p25_strong_reactive_history.md
docs/analysis/p2_strong_reactive.md
docs/analysis/p31_exact_candidate_coverage.md
docs/analysis/p32_action_value_ranking.md
docs/analysis/p32_correction_and_scope.md
docs/analysis/p3_copy_predictability.md
docs/analysis/p4_sr_srh2_validation.md
docs/analysis/pre_taskmain_cleanup_snapshot.md
docs/analysis/pre_taskmain_deleted_files_manifest.csv
docs/analysis/pre_taskmain_deleted_units.csv
docs/analysis/pre_taskmain_legacy_cleanup_plan.md
docs/analysis/pre_taskmain_legacy_cleanup_result.md
docs/analysis/pre_taskmain_review_deep_audit.md
docs/protocol/protocol_changelog.md
docs/protocol/taskmain_v1.1.md
```

### 生成缓存

```text
.pytest_cache/.gitignore
.pytest_cache/CACHEDIR.TAG
.pytest_cache/README.md
.pytest_cache/v/cache/lastfailed
.pytest_cache/v/cache/nodeids
```

## 5. 最终目录结构

`configs/` 只剩 `task_main/`；`scripts/` 只剩当前 TaskMain 四个目录与输入审计脚本；`tests/` 只剩 `task_main/`、`test_trace.py`、`test_cache.py`。旧专用模块已删除，共享模块及不确定的 dirty 核心按上述原因保留。

`results/` 仍只有：

```text
results/task_main
results/trace_audit
```

实际 `tree -L 3`：

```text
.
├── README.md
├── checkpoints
├── configs
│   └── task_main
│       ├── formal
│       ├── pilot
│       ├── stage_a_r_aff.yaml
│       ├── stage_a_r_req_kv_task.yaml
│       ├── stage_d1
│       └── stage_d2
├── data
│   └── mooncake
│       ├── conversation_trace.jsonl
│       └── toolagent_trace.jsonl
├── docs
│   ├── analysis
│   │   ├── legacy_code_cleanup_final.md
│   │   ├── legacy_results_cleanup_final.md
│   │   ├── task_main_final_figures.md
│   │   ├── task_main_final_results_consolidation.md
│   │   ├── task_main_formal_diagnostics_v1.md
│   │   ├── task_main_preformal_compliance_check.md
│   │   ├── task_main_stage_a_implementation.md
│   │   ├── task_main_stage_b_implementation.md
│   │   ├── task_main_stage_c_implementation.md
│   │   ├── task_main_stage_d1_sensitivity.md
│   │   ├── task_main_stage_d2_move_ablation.md
│   │   └── task_main_v1_gap_analysis.md
│   └── protocol
│       ├── task_main_v1.md
│       └── task_main_v1_method.md
├── note
│   ├── 我的笔记
│   │   ├── T1_community_survey.md
│   │   ├── T2_mooncake_trace_capability_audit.md
│   │   ├── T2_simulator_design_final_v1.1.md
│   │   ├── 我的任务.md
│   │   ├── 整体理解.md
│   │   └── 第二阶段笔记.md
│   └── 项目文档
│       ├── P2P集群对照分析_svc-rnwjq8pb.md
│       ├── p2pkvcache.md
│       ├── 主动KV干预的前期证据工作.md
│       └── 完整技术报告_KVCache亲和与负载倾斜.md
├── requirements.txt
├── results
│   ├── task_main
│   │   ├── final_figures
│   │   ├── final_results
│   │   ├── formal
│   │   ├── formal.console.log
│   │   ├── formal_diagnostics_v1
│   │   ├── preformal_audit
│   │   ├── stage_a_smoke
│   │   ├── stage_b_smoke
│   │   ├── stage_c_pilot
│   │   ├── stage_d1_sensitivity
│   │   ├── stage_d2_move_ablation
│   │   ├── task_main_final_export_20260918.tar.gz
│   │   └── task_main_final_export_20260918.tar.gz.sha256
│   └── trace_audit
│       ├── trace_audit.json
│       └── trace_audit.md
├── scripts
│   ├── audit_mooncake_trace.py
│   ├── task_main
│   │   ├── README.md
│   │   ├── final_figures
│   │   ├── run_formal.py
│   │   ├── run_matrix.py
│   │   ├── run_pilot.py
│   │   ├── run_stage_a.py
│   │   ├── run_stage_a_smoke.py
│   │   ├── run_stage_b_smoke.py
│   │   ├── summarize.py
│   │   └── validate.py
│   ├── task_main_d1
│   │   ├── README.md
│   │   ├── __init__.py
│   │   ├── preflight.py
│   │   └── run_stage_d1.py
│   ├── task_main_d2
│   │   ├── README.md
│   │   ├── __init__.py
│   │   ├── preflight.py
│   │   ├── run_smoke.py
│   │   └── run_stage_d2.py
│   └── task_main_final
│       ├── __init__.py
│       └── consolidate.py
├── src
│   ├── __init__.py
│   └── simulator
│       ├── __init__.py
│       ├── cache.py
│       ├── config.py
│       ├── engine.py
│       ├── metrics.py
│       ├── oracle.py
│       ├── pressure.py
│       ├── strategies.py
│       ├── task_main
│       └── trace.py
└── tests
    ├── task_main
    │   ├── conftest.py
    │   ├── test_c1_input_roles.py
    │   ├── test_candidates.py
    │   ├── test_engine_affinity.py
    │   ├── test_engine_reactive.py
    │   ├── test_isolation_cli.py
    │   ├── test_optimization_checkpoint.py
    │   ├── test_policies_cost_aware.py
    │   ├── test_policies_oracle.py
    │   ├── test_policies_persistence.py
    │   ├── test_policies_recency.py
    │   ├── test_primitives.py
    │   ├── test_proactive_execution.py
    │   ├── test_routing.py
    │   ├── test_stage_b_isolation.py
    │   ├── test_stage_c_gates_reporting.py
    │   ├── test_stage_c_metrics.py
    │   ├── test_stage_d1_sensitivity.py
    │   ├── test_stage_d2_engine.py
    │   ├── test_stage_d2_gates_runner.py
    │   ├── test_stage_d2_move_release.py
    │   ├── test_target_feasibility.py
    │   └── test_transfer_events.py
    ├── test_cache.py
    └── test_trace.py

39 directories, 91 files
```

## 6. 当前 Git 状态与停止

HEAD：`ec204c892bb494e54db8a0be81b5d4a775361daf`，未创建提交。以下包含此前已有删除和未跟踪 TaskMain 文件，不把这些全部归为本轮变更。

```text
D  configs/formal/aff_10000_v1.yaml
D  configs/formal/aff_5000_v1.yaml
D  configs/formal/aff_infinite_diagnostic_v1.yaml
D  configs/formal/b0_5000_v1.yaml
D  configs/formal/b0_development_10000_v1.yaml
D  configs/formal/b0_development_5000_v1.yaml
D  configs/formal/b0_infinite_diagnostic_v1.yaml
D  configs/formal/b0_nominal_v1.yaml
D  configs/formal/o1_budget_protocol_v1.yaml
D  configs/formal/o1_development_nominal.yaml
D  configs/formal/o1_mechanism_diagnostics_v1.yaml
D  configs/formal/sensitivity_protocol_v1.yaml
D  configs/formal/taskmain_v1.1.yaml
D  configs/kernel_v0.yaml
D  configs/kernel_v1_1000.yaml
D  configs/kernel_v1_10000.yaml
D  configs/kernel_v1_5000.yaml
D  configs/kernel_v2_aff_10000.yaml
D  configs/kernel_v2_aff_5000.yaml
D  configs/kernel_v2_aff_infinite.yaml
D  configs/kernel_v2_least_10000.yaml
D  configs/kernel_v2_least_infinite.yaml
D  configs/kernel_v3a_req_10000.yaml
D  configs/kernel_v3a_req_10000_trace_page.yaml
D  configs/kernel_v3a_req_5000.yaml
D  configs/kernel_v3a_req_infinite.yaml
D  configs/kernel_v3b_b0_10000.yaml
D  configs/kernel_v3b_b0_5000.yaml
D  configs/kernel_v3b_sens_absolute_1000.yaml
D  configs/kernel_v3b_sens_cache_01.yaml
D  configs/kernel_v3b_sens_relative_20.yaml
D  configs/kernel_v3b_sens_timeout_1000.yaml
D  configs/o2_oracle/full_o2a_conversation.yaml
D  configs/o2_oracle/full_o2a_toolagent.yaml
D  configs/o2_oracle/o2a_closed_loop_smoke.yaml
D  configs/taskmain_v1.1/conversation/E01_r_aff.yaml
D  configs/taskmain_v1.1/conversation/E02_r_req_kv_task.yaml
D  configs/taskmain_v1.1/conversation/E03_oracle.yaml
D  configs/taskmain_v1.1/conversation/E04_persist_h1_k10.yaml
D  configs/taskmain_v1.1/conversation/E05_persist_h5_k10.yaml
D  configs/taskmain_v1.1/conversation/E06_recency_q09.yaml
D  configs/taskmain_v1.1/conversation/E07_cost_aware.yaml
D  configs/taskmain_v1.1/matched/M01_conversation_r_req_kv_v1.yaml
D  configs/taskmain_v1.1/matched/M02_conversation_matched_o1.yaml
D  configs/taskmain_v1.1/matched/M03_toolagent_r_req_kv_v1.yaml
D  configs/taskmain_v1.1/matched/M04_toolagent_matched_o1.yaml
D  configs/taskmain_v1.1/sweeps/copy_vs_move/conversation_move.yaml
D  configs/taskmain_v1.1/sweeps/copy_vs_move/toolagent_move.yaml
D  configs/taskmain_v1.1/sweeps/num_pods/conversation_n2_cost_aware.yaml
D  configs/taskmain_v1.1/sweeps/num_pods/conversation_n2_oracle.yaml
D  configs/taskmain_v1.1/sweeps/num_pods/conversation_n2_persist_h1_k10.yaml
D  configs/taskmain_v1.1/sweeps/num_pods/conversation_n2_persist_h5_k10.yaml
D  configs/taskmain_v1.1/sweeps/num_pods/conversation_n2_r_aff.yaml
D  configs/taskmain_v1.1/sweeps/num_pods/conversation_n2_r_req_kv_task.yaml
D  configs/taskmain_v1.1/sweeps/num_pods/conversation_n2_recency_q09.yaml
D  configs/taskmain_v1.1/sweeps/num_pods/toolagent_n2_cost_aware.yaml
D  configs/taskmain_v1.1/sweeps/num_pods/toolagent_n2_oracle.yaml
D  configs/taskmain_v1.1/sweeps/num_pods/toolagent_n2_persist_h1_k10.yaml
D  configs/taskmain_v1.1/sweeps/num_pods/toolagent_n2_persist_h5_k10.yaml
D  configs/taskmain_v1.1/sweeps/num_pods/toolagent_n2_r_aff.yaml
D  configs/taskmain_v1.1/sweeps/num_pods/toolagent_n2_r_req_kv_task.yaml
D  configs/taskmain_v1.1/sweeps/num_pods/toolagent_n2_recency_q09.yaml
D  configs/taskmain_v1.1/sweeps/oracle_window/conversation_w1.yaml
D  configs/taskmain_v1.1/sweeps/oracle_window/conversation_w30.yaml
D  configs/taskmain_v1.1/sweeps/oracle_window/conversation_w5.yaml
D  configs/taskmain_v1.1/sweeps/oracle_window/toolagent_w1.yaml
D  configs/taskmain_v1.1/sweeps/oracle_window/toolagent_w30.yaml
D  configs/taskmain_v1.1/sweeps/oracle_window/toolagent_w5.yaml
D  configs/taskmain_v1.1/sweeps/persistence_h_k/conversation_h1_k20.yaml
D  configs/taskmain_v1.1/sweeps/persistence_h_k/conversation_h1_k5.yaml
D  configs/taskmain_v1.1/sweeps/persistence_h_k/conversation_h5_k20.yaml
D  configs/taskmain_v1.1/sweeps/persistence_h_k/conversation_h5_k5.yaml
D  configs/taskmain_v1.1/sweeps/persistence_h_k/toolagent_h1_k20.yaml
D  configs/taskmain_v1.1/sweeps/persistence_h_k/toolagent_h1_k5.yaml
D  configs/taskmain_v1.1/sweeps/persistence_h_k/toolagent_h5_k20.yaml
D  configs/taskmain_v1.1/sweeps/persistence_h_k/toolagent_h5_k5.yaml
D  configs/taskmain_v1.1/sweeps/recency/conversation_q08.yaml
D  configs/taskmain_v1.1/sweeps/recency/toolagent_q08.yaml
D  configs/taskmain_v1.1/sweeps/theta/conversation_theta15.yaml
D  configs/taskmain_v1.1/sweeps/theta/conversation_theta30.yaml
D  configs/taskmain_v1.1/sweeps/theta/toolagent_theta15.yaml
D  configs/taskmain_v1.1/sweeps/theta/toolagent_theta30.yaml
D  configs/taskmain_v1.1/toolagent/E08_r_aff.yaml
D  configs/taskmain_v1.1/toolagent/E09_r_req_kv_task.yaml
D  configs/taskmain_v1.1/toolagent/E10_oracle.yaml
D  configs/taskmain_v1.1/toolagent/E11_persist_h1_k10.yaml
D  configs/taskmain_v1.1/toolagent/E12_persist_h5_k10.yaml
D  configs/taskmain_v1.1/toolagent/E13_recency_q09.yaml
D  configs/taskmain_v1.1/toolagent/E14_cost_aware.yaml
D  docs/README.md
D  docs/analysis/astra_experiment_summary.md
D  docs/analysis/final_analysis.md
D  docs/analysis/main_table.md
D  docs/analysis/sweep_analysis.md
D  docs/diagnostics/D01_relaxed_capacity.md
D  docs/diagnostics/D02_no_evict_admission.md
D  docs/diagnostics/D03_fanout2.md
D  docs/diagnostics/D04_common_support_horizon.md
D  docs/experiments/E01_conversation_r_aff.md
D  docs/experiments/E02_conversation_r_req_kv_task.md
D  docs/experiments/E03_conversation_oracle.md
D  docs/experiments/E04_conversation_persist_h1_k10.md
D  docs/experiments/E05_conversation_persist_h5_k10.md
D  docs/experiments/E06_conversation_recency_q09.md
D  docs/experiments/E07_conversation_cost_aware.md
D  docs/experiments/E08_toolagent_r_aff.md
D  docs/experiments/E09_toolagent_r_req_kv_task.md
D  docs/experiments/E10_toolagent_oracle.md
D  docs/experiments/E11_toolagent_persist_h1_k10.md
D  docs/experiments/E12_toolagent_persist_h5_k10.md
D  docs/experiments/E13_toolagent_recency_q09.md
D  docs/experiments/E14_toolagent_cost_aware.md
D  docs/matched/M01_conversation_r_req_kv_v1.md
D  docs/matched/M02_conversation_matched_o1.md
D  docs/matched/M03_toolagent_r_req_kv_v1.md
D  docs/matched/M04_toolagent_matched_o1.md
D  docs/protocol/protocol_changelog.md
D  docs/protocol/taskmain_v1.1.md
D  docs/sweeps/S01_oracle_window.md
D  docs/sweeps/S02_persistence_h_k.md
D  docs/sweeps/S03_recency.md
D  docs/sweeps/S04_theta.md
D  docs/sweeps/S05_num_pods.md
D  docs/sweeps/S06_copy_vs_move.md
D  docs/sweeps/S07_cost_aware.md
D  scripts/calibrate_taskmain_development.py
D  scripts/init_taskmain_docs.py
D  scripts/migrate_full_o2a_checkpoint.py
D  scripts/o2_oracle/run_o2a_pilot.sh
D  scripts/o2_oracle/run_o2a_smoke_validation.sh
D  scripts/o2_oracle/run_o2p_pilot.sh
D  scripts/refresh_task_activity_counters.py
D  scripts/run_full_o2a_conversation.sh
D  scripts/run_full_o2a_toolagent.sh
D  scripts/run_o1_budget_sensitivity.py
D  scripts/run_o1_development.py
D  scripts/run_o1_diagnostics.py
D  scripts/run_taskmain_development_validation.py
D  scripts/summarize_o1_development.py
D  scripts/summarize_taskmain_validation.py
D  scripts/taskmain_v1.1/consolidate_results.py
D  scripts/taskmain_v1.1/run_conversation_main.sh
D  scripts/taskmain_v1.1/run_matched_main.sh
D  scripts/taskmain_v1.1/run_s01_oracle_window.sh
D  scripts/taskmain_v1.1/run_s02_persistence_h_k.sh
D  scripts/taskmain_v1.1/run_s03_recency.sh
D  scripts/taskmain_v1.1/run_s04_theta.sh
D  scripts/taskmain_v1.1/run_s05_num_pods.sh
D  scripts/taskmain_v1.1/run_s06_copy_vs_move.sh
D  scripts/taskmain_v1.1/run_toolagent_main.sh
D  scripts/taskmain_v1.1/summarize_conversation_main.py
D  scripts/taskmain_v1.1/summarize_matched_main.py
D  scripts/taskmain_v1.1/summarize_s01_oracle_window.py
D  scripts/taskmain_v1.1/summarize_s02_persistence_h_k.py
D  scripts/taskmain_v1.1/summarize_s03_recency.py
D  scripts/taskmain_v1.1/summarize_s04_theta.py
D  scripts/taskmain_v1.1/summarize_s05_num_pods.py
D  scripts/taskmain_v1.1/summarize_s06_copy_vs_move.py
D  scripts/taskmain_v1.1/summarize_s07_cost_aware.py
D  scripts/taskmain_v1.1/summarize_toolagent_main.py
 M src/simulator/config.py
 M src/simulator/engine.py
D  src/simulator/events.py
D  src/simulator/full_o2a.py
 M src/simulator/metrics.py
D  src/simulator/o2a.py
D  src/simulator/o2a_smoke.py
D  src/simulator/o2p.py
 M src/simulator/oracle.py
D  src/simulator/pod.py
 M src/simulator/pressure.py
D  src/simulator/routing.py
D  src/simulator/service.py
 M src/simulator/strategies.py
D  src/simulator/tickets.py
D  src/simulator/transfer.py
D  tests/test_diagnostics.py
D  tests/test_engine.py
D  tests/test_full_o2a.py
D  tests/test_metrics.py
D  tests/test_move_safe.py
D  tests/test_o2p.py
D  tests/test_oracle.py
D  tests/test_pod.py
D  tests/test_pressure.py
D  tests/test_proactive.py
D  tests/test_routing.py
D  tests/test_service.py
D  tests/test_splits.py
D  tests/test_taskmain.py
D  tests/test_tickets.py
D  tests/test_transfer.py
?? configs/task_main/formal/conversation/line_1.yaml
?? configs/task_main/formal/conversation/line_2.yaml
?? configs/task_main/formal/conversation/line_3.yaml
?? configs/task_main/formal/conversation/line_4.yaml
?? configs/task_main/formal/conversation/line_5.yaml
?? configs/task_main/formal/conversation/line_6.yaml
?? configs/task_main/formal/conversation/line_7.yaml
?? configs/task_main/formal/toolagent/line_1.yaml
?? configs/task_main/formal/toolagent/line_2.yaml
?? configs/task_main/formal/toolagent/line_3.yaml
?? configs/task_main/formal/toolagent/line_4.yaml
?? configs/task_main/formal/toolagent/line_5.yaml
?? configs/task_main/formal/toolagent/line_6.yaml
?? configs/task_main/formal/toolagent/line_7.yaml
?? configs/task_main/pilot/conversation/line_1.yaml
?? configs/task_main/pilot/conversation/line_2.yaml
?? configs/task_main/pilot/conversation/line_3.yaml
?? configs/task_main/pilot/conversation/line_4.yaml
?? configs/task_main/pilot/conversation/line_5.yaml
?? configs/task_main/pilot/conversation/line_6.yaml
?? configs/task_main/pilot/conversation/line_7.yaml
?? configs/task_main/pilot/toolagent/line_1.yaml
?? configs/task_main/pilot/toolagent/line_2.yaml
?? configs/task_main/pilot/toolagent/line_3.yaml
?? configs/task_main/pilot/toolagent/line_4.yaml
?? configs/task_main/pilot/toolagent/line_5.yaml
?? configs/task_main/pilot/toolagent/line_6.yaml
?? configs/task_main/pilot/toolagent/line_7.yaml
?? configs/task_main/stage_a_r_aff.yaml
?? configs/task_main/stage_a_r_req_kv_task.yaml
?? configs/task_main/stage_d1/conversation/n2_aff.yaml
?? configs/task_main/stage_d1/conversation/n2_oracle.yaml
?? configs/task_main/stage_d1/conversation/n2_persist300.yaml
?? configs/task_main/stage_d1/conversation/n2_recency.yaml
?? configs/task_main/stage_d1/conversation/n2_reqkv.yaml
?? configs/task_main/stage_d1/conversation/oracle_w1m.yaml
?? configs/task_main/stage_d1/conversation/oracle_w30m.yaml
?? configs/task_main/stage_d1/conversation/oracle_w5m.yaml
?? configs/task_main/stage_d1/conversation/persist_h300_k20.yaml
?? configs/task_main/stage_d1/conversation/persist_h300_k5.yaml
?? configs/task_main/stage_d1/conversation/persist_h60_k20.yaml
?? configs/task_main/stage_d1/conversation/persist_h60_k5.yaml
?? configs/task_main/stage_d1/conversation/recency_q0p8.yaml
?? configs/task_main/stage_d1/conversation/rleast.yaml
?? configs/task_main/stage_d1/conversation/theta_1p5.yaml
?? configs/task_main/stage_d1/conversation/theta_3.yaml
?? configs/task_main/stage_d1/toolagent/n2_aff.yaml
?? configs/task_main/stage_d1/toolagent/n2_oracle.yaml
?? configs/task_main/stage_d1/toolagent/n2_persist300.yaml
?? configs/task_main/stage_d1/toolagent/n2_recency.yaml
?? configs/task_main/stage_d1/toolagent/n2_reqkv.yaml
?? configs/task_main/stage_d1/toolagent/oracle_w1m.yaml
?? configs/task_main/stage_d1/toolagent/oracle_w30m.yaml
?? configs/task_main/stage_d1/toolagent/oracle_w5m.yaml
?? configs/task_main/stage_d1/toolagent/persist_h300_k20.yaml
?? configs/task_main/stage_d1/toolagent/persist_h300_k5.yaml
?? configs/task_main/stage_d1/toolagent/persist_h60_k20.yaml
?? configs/task_main/stage_d1/toolagent/persist_h60_k5.yaml
?? configs/task_main/stage_d1/toolagent/recency_q0p8.yaml
?? configs/task_main/stage_d1/toolagent/rleast.yaml
?? configs/task_main/stage_d1/toolagent/theta_1p5.yaml
?? configs/task_main/stage_d1/toolagent/theta_3.yaml
?? configs/task_main/stage_d2/conversation/oracle_move.yaml
?? configs/task_main/stage_d2/conversation/persist300_move.yaml
?? configs/task_main/stage_d2/toolagent/oracle_move.yaml
?? configs/task_main/stage_d2/toolagent/persist300_move.yaml
?? docs/analysis/legacy_code_cleanup_final.md
?? docs/analysis/legacy_results_cleanup_final.md
?? docs/analysis/task_main_final_figures.md
?? docs/analysis/task_main_final_results_consolidation.md
?? docs/analysis/task_main_formal_diagnostics_v1.md
?? docs/analysis/task_main_preformal_compliance_check.md
?? docs/analysis/task_main_stage_a_implementation.md
?? docs/analysis/task_main_stage_b_implementation.md
?? docs/analysis/task_main_stage_c_implementation.md
?? docs/analysis/task_main_stage_d1_sensitivity.md
?? docs/analysis/task_main_stage_d2_move_ablation.md
?? docs/analysis/task_main_v1_gap_analysis.md
?? docs/protocol/task_main_v1.md
?? docs/protocol/task_main_v1_method.md
?? scripts/task_main/README.md
?? scripts/task_main/final_figures/build_all.py
?? scripts/task_main/final_figures/common.py
?? scripts/task_main/final_figures/fig01_baselines.py
?? scripts/task_main/final_figures/fig02_main.py
?? scripts/task_main/final_figures/fig03_oracle.py
?? scripts/task_main/final_figures/fig04_buckets.py
?? scripts/task_main/final_figures/fig05_persistence.py
?? scripts/task_main/final_figures/fig06_oracle_w.py
?? scripts/task_main/final_figures/fig07_robustness.py
?? scripts/task_main/final_figures/fig08_move.py
?? scripts/task_main/final_figures/fig09_efficiency.py
?? scripts/task_main/final_figures/figures.py
?? scripts/task_main/final_figures/validate.py
?? scripts/task_main/run_formal.py
?? scripts/task_main/run_matrix.py
?? scripts/task_main/run_pilot.py
?? scripts/task_main/run_stage_a.py
?? scripts/task_main/run_stage_a_smoke.py
?? scripts/task_main/run_stage_b_smoke.py
?? scripts/task_main/summarize.py
?? scripts/task_main/validate.py
?? scripts/task_main_d1/README.md
?? scripts/task_main_d1/__init__.py
?? scripts/task_main_d1/preflight.py
?? scripts/task_main_d1/run_stage_d1.py
?? scripts/task_main_d2/README.md
?? scripts/task_main_d2/__init__.py
?? scripts/task_main_d2/preflight.py
?? scripts/task_main_d2/run_smoke.py
?? scripts/task_main_d2/run_stage_d2.py
?? scripts/task_main_final/__init__.py
?? scripts/task_main_final/consolidate.py
?? src/simulator/task_main/__init__.py
?? src/simulator/task_main/cache.py
?? src/simulator/task_main/candidates.py
?? src/simulator/task_main/config.py
?? src/simulator/task_main/engine.py
?? src/simulator/task_main/equivalence.py
?? src/simulator/task_main/final_metrics.py
?? src/simulator/task_main/future_access.py
?? src/simulator/task_main/history.py
?? src/simulator/task_main/isolation.py
?? src/simulator/task_main/load.py
?? src/simulator/task_main/metrics.py
?? src/simulator/task_main/policies.py
?? src/simulator/task_main/proactive.py
?? src/simulator/task_main/records.py
?? src/simulator/task_main/reporting.py
?? src/simulator/task_main/routing.py
?? src/simulator/task_main/stage_d1/__init__.py
?? src/simulator/task_main/stage_d1/candidates.py
?? src/simulator/task_main/stage_d1/config.py
?? src/simulator/task_main/stage_d1/diagnostics.py
?? src/simulator/task_main/stage_d1/engine.py
?? src/simulator/task_main/stage_d1/isolation.py
?? src/simulator/task_main/stage_d1/reporting.py
?? src/simulator/task_main/stage_d2/__init__.py
?? src/simulator/task_main/stage_d2/cache.py
?? src/simulator/task_main/stage_d2/candidates.py
?? src/simulator/task_main/stage_d2/config.py
?? src/simulator/task_main/stage_d2/engine.py
?? src/simulator/task_main/stage_d2/history.py
?? src/simulator/task_main/stage_d2/isolation.py
?? src/simulator/task_main/stage_d2/metrics.py
?? src/simulator/task_main/stage_d2/records.py
?? src/simulator/task_main/stage_d2/reference.py
?? src/simulator/task_main/stage_d2/reporting.py
?? src/simulator/task_main/stage_d2/transfer.py
?? src/simulator/task_main/trace.py
?? src/simulator/task_main/transfer.py
?? src/simulator/task_main/validation.py
?? tests/task_main/conftest.py
?? tests/task_main/test_c1_input_roles.py
?? tests/task_main/test_candidates.py
?? tests/task_main/test_engine_affinity.py
?? tests/task_main/test_engine_reactive.py
?? tests/task_main/test_isolation_cli.py
?? tests/task_main/test_optimization_checkpoint.py
?? tests/task_main/test_policies_cost_aware.py
?? tests/task_main/test_policies_oracle.py
?? tests/task_main/test_policies_persistence.py
?? tests/task_main/test_policies_recency.py
?? tests/task_main/test_primitives.py
?? tests/task_main/test_proactive_execution.py
?? tests/task_main/test_routing.py
?? tests/task_main/test_stage_b_isolation.py
?? tests/task_main/test_stage_c_gates_reporting.py
?? tests/task_main/test_stage_c_metrics.py
?? tests/task_main/test_stage_d1_sensitivity.py
?? tests/task_main/test_stage_d2_engine.py
?? tests/task_main/test_stage_d2_gates_runner.py
?? tests/task_main/test_stage_d2_move_release.py
?? tests/task_main/test_target_feasibility.py
?? tests/task_main/test_transfer_events.py
```

**LEGACY_CODE_CLEANUP_PASS**。删除与验证完成，到此停止。未运行新实验、重构源码、修改当前协议或正式结果。
