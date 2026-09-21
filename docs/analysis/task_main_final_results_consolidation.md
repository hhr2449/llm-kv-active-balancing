# TaskMain Final Results Consolidation

**状态：FINAL_RESULTS_CONSOLIDATION_PASS。** 本阶段只读取既有 PASS artifacts；未调用 simulator，未修改既有结果、协议或配置。

## 1. 数据来源

- Source A：Formal Main 14-line，run `formal:5ddeca8e0c768e04`，manifest `7499b1e08fe0f4b7a8bbd72c83b120c16ebfc6a647d797095ca469c056a4debf`。
- Source B：Formal Result Diagnostics V1，仅用于诊断字段。
- Source C：Stage D1 Sensitivity `run_20260917_01`；FORMAL_REFERENCE 只登记引用，不新增 canonical row。
- Source D：Stage D2 MOVE Ablation `run_20260918_02`；COPY 只读引用 Formal，新增四个 MOVE row。

所有 source manifest、checkpoint、progress、determinism、protocol、trace identity 与已冻结 provenance 均通过 gate。pilot、smoke、INVALID、SUPERSEDED_INCOMPLETE、O2/SR/P3 均未读取为数据源。

## 2. Canonicalization 与去重

Canonical key 使用 workload、routing、policy、action、N、theta、W、h、K、q 和 Evaluation 起止时间。family 不参与行为去重；D1/D2 的 Formal reference 通过 key 和原始指标交叉验证后写入 `referenced_by_families`。

最终母表共 **50** 行：Formal Main 14、D1 新 case 32、D2 MOVE 4。canonical ID 与行为 key 均无重复。

## 3. Cohort 隔离与单位

Main、theta、Persistence K、Recency q、N、MOVE 使用 `MAIN_25_45=[1500000,2700000)`；Oracle W sensitivity 使用 `W_SENS_10_25=[600000,1500000)`。只有 workload 和 cohort 完全相同才计算 delta。Saved/WeightedSaved 同时保留原值与 /1e6；wire 同时保留 bytes 与 decimal TB=/1e12。

## 4. 输出表

- `baseline_comparison`：6 rows。
- `copy_vs_move`：8 rows。
- `copy_vs_move_delta`：4 rows。
- `diagnostics_summary`：14 rows。
- `final_results_long`：50 rows。
- `history_strategy_summary`：6 rows。
- `main_7line_combined`：14 rows。
- `main_7line_conversation`：7 rows。
- `main_7line_toolagent`：7 rows。
- `n_sensitivity_final`：20 rows。
- `oracle_headroom`：2 rows。
- `oracle_prompt_bucket_delta`：12 rows。
- `oracle_w_sensitivity_final`：6 rows。
- `persistence_sensitivity_final`：12 rows。
- `prompt_bucket_final`：84 rows。
- `recency_sensitivity_final`：4 rows。
- `sensitivity_master`：48 rows。
- `strategy_parameter_master`：72 rows。
- `theta_sensitivity_final`：6 rows。

Excel 提供 README、主表、baseline、Oracle headroom、各 sensitivity、COPY/MOVE、Prompt buckets、Diagnostics、Strategy Master 与 All Long；CSV/JSON 是 canonical machine-readable source。

## 5. 一致性检查

| Check | Result |
|---|---|
| `formal_main_14_rows` | PASS |
| `formal_7_per_workload` | PASS |
| `line5_line7_main_metrics_equal` | PASS |
| `rleast_both_workloads` | PASS |
| `theta_grid` | PASS |
| `persistence_grid` | PASS |
| `recency_grid` | PASS |
| `oracle_w_grid_common_cohort` | PASS |
| `n_grid` | PASS |
| `copy_move_four_pairs` | PASS |
| `prompt_bucket_six_and_conserved` | PASS |
| `wire_eval_le_full` | PASS |
| `rates_in_unit_interval` | PASS |
| `waste_in_unit_interval_or_na` | PASS |
| `move_release_partition` | PASS |
| `move_release_pages_nonnegative` | PASS |
| `canonical_id_unique` | PASS |
| `behavior_key_unique` | PASS |
| `canonical_row_count_50` | PASS |
| `all_validation_pass` | PASS |
| `all_protocol_frozen` | PASS |
| `trace_identity_frozen` | PASS |
| `move_deltas_recomputed` | PASS |
| `no_cross_cohort_delta` | PASS |

## 6. Provenance

| Source | SHA-256 |
|---|---|
| `formal_validation_manifest` | `7499b1e08fe0f4b7a8bbd72c83b120c16ebfc6a647d797095ca469c056a4debf` |
| `formal_source_provenance` | `25ddfae1be89148d8f30a39b03fd0b4764b0703041d4a7be4e00d5b69f4eb318` |
| `diagnostics_manifest` | `c6559850d95f3703b05fe6764efe7ecff77a004ad439b1aca2ff501100011198` |
| `d1_validation_manifest` | `4a4d1717f7a821e5b0125bd3f739ae76149b77b3bac6f476d6f09a403e7d5ba1` |
| `d2_validation_manifest` | `97013fb94b65dd829b98f60c8c068f0bec31fd6ebbbe57b5c269c6b8af0da6c6` |

最终输出 artifact SHA-256 记录在 `consolidation_manifest.json`。

## 7. Missing / NA

CSV 中 NA、JSON 中 null。MOVE-only release 字段对 COPY 为 NA；无 proactive transfer 的 unused ratio 为 NA；未由可靠 source 发出的 ordinary-LRU/churn 字段保持 NA，并用 scope/reason 字段说明。没有把未知值写成 0。

## 8. 解释边界

本阶段没有画图、没有补实验、没有参数选择，也没有写最终科研结论。WeightedSaved 不是延迟；unused transfer wire 不等于完全无 partial reuse；MOVE source release 与 target-side unused ratio 是不同指标。

## 9. 最终状态

**FINAL_RESULTS_CONSOLIDATION_PASS**
