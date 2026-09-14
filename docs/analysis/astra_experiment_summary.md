# Active KV Balancing Experiment Summary

This note consolidates the experimental context, frozen design, and recorded results for the active KV balancing stage. Latencies are in milliseconds, rates are computed over the recorded Evaluation split, and byte counts are wire bytes unless stated otherwise. `N/A` denotes a field that is not present in the referenced result artifact.

## 1. Research Background

Cache-affinity routing can concentrate reusable prefix KV on a subset of Pods while request and service work evolve across the cluster. The active KV replication experiments evaluate controlled placement of published prompt KV replicas under finite cache capacity, network transfer constraints, and a byte token budget. The recorded experiments cover three questions: whether a legal replication action exists at a sampled state, whether prefix and target enumeration changes the selected action, and how sequential action-value decisions behave in a closed-loop replay.

## 2. Experimental Setup

| Item | Frozen setting | Source |
|---|---|---|
| Workloads | Mooncake `conversation_trace.jsonl`; Mooncake `toolagent_trace.jsonl` | TaskMain protocol and result provenance |
| Replay and split | Replay starts at 0 ms; Development `[0, 900000)`; Warmup `[900000, 1500000)`; Evaluation `[1500000, 2700000)`; ObservationTail starts at 2700000 ms | TaskMain configs |
| Pods | 4 | TaskMain and Full O2-A configs |
| Cache capacity | 585 pages per Pod; 2340 pages cluster total | TaskMain protocol/configs |
| Trace page | 512 tokens; one page is 14 MiB = 14,680,064 bytes | TaskMain protocol/configs |
| KV scope | P-only; FULL_PREFIX; FULL_PAGE_ONLY | TaskMain protocol/configs |
| Service model | Base latency `d0 = 2 ms`; prefill rate `mu = 10,000 tokens/s` | TaskMain protocol/configs |
| Routing | `R_AFF` for affinity experiments; `R_REQ_KV_V1` for matched O1 and Full O2-A trajectories | Matched and Full O2-A configs |
| Reactive transfer | 25,000,000,000 bytes/s effective bandwidth; 1 ms control latency; 5000 ms admission timeout | TaskMain configs |
| Proactive trigger | 1000 ms interval; at most one proactive action per tick | TaskMain protocol/configs |
| Future-demand horizon | 300,000 ms | Matched-O1, O2-P, O2-A configs/results |
| Proactive budget | `q = 0.05` aggregate cache-capacity turnover/minute; refill 28,626,124.8 bytes/s; one-minute budget 1,717,567,488 bytes | Development budget audit |
| Token bucket | Initial tokens 0; no reset at Warmup/Evaluation; cap 8,587,837,440 bytes = 585 pages | TaskMain protocol and budget audit |
| Random seed / arrival mode | 20260911 / `RAW_BUCKETED` | TaskMain configs |
| Protocol and metric version | `TASKMAIN_V1_1`; metric version `v1` in TaskMain result provenance | TaskMain summaries |
| Simulator revisions recorded | TaskMain main/matched: `54f3c5e6d1105f96e57c2ccc2dc8b26f3835089f`; Full O2-A ToolAgent checkpoint: `c02643c5d29f8a1ab61a856ba491490414590eef`; Full O2-A Conversation summary: N/A | Result provenance and checkpoint manifest |

The S01 Oracle-window sweep uses a separate common-support Evaluation interval, `[900000, 1500000)` ms, recorded in its configs and summary. It is not mixed into the main tables below.

## 3. Baseline and Oracle Definitions

### R_AFF

`R_AFF` is the cache-affinity baseline. It routes by the longest published prefix hit, with load and Pod ID used as deterministic tie-breaks. It does not perform reactive or proactive KV transfer.

### R_REQ_KV

`R_REQ_KV_V1` is the request-driven KV transfer baseline used by the matched comparison and by the Full O2-A main trajectory. It uses the frozen cache-hit, relative-load, and absolute-load-gap gates, finite FULL_PREFIX transfer, NodeGate admission, and fallback behavior.

TaskMain E02/E09 use the distinct `R_REQ_KV_TASK` definition. `R_REQ_KV_TASK` disables the cache-hit and absolute-gap gates and applies `Ls > theta * Lt` over EffectiveLoad. The main tables below use `R_REQ_KV_V1` because it is the paired baseline for Matched-O1 and Full O2-A.

### Matched-O1

Matched-O1 combines the `R_REQ_KV_V1` trajectory with the Future-Demand reference trigger. It observes future external prefix demand over `W = 300 s`, then uses the existing O1 candidate/action selection with COPY and trigger-only scheduling. It is not recorded as a global optimum.

### O2-P

O2-P is a placement/action diagnostic Oracle over sampled common states. For each state it fixes the O1-selected prefix, enumerates legal target Pods plus NO_COPY, and replays each counterfactual branch over the 300-second cohort horizon. Its per-state values use overlapping cohorts and are not accumulated as a closed-loop replay result.

### Full O2-A

Full O2-A is an action-value Oracle. At each one-second decision it uses the Future-Demand candidate generator, takes the Top-5 prefixes, enumerates legal `COPY(prefix, target)` actions and NO_COPY, and counterfactually replays each branch. The selected action maximizes the reduction in future cohort completion-latency loss. The selected action is committed to the main trajectory before the next decision; counterfactual branches are discarded.

## 4. Main Evaluation Results

In the following tables, `Transfers (R/P)` reports reactive and proactive completed transfer counts. `Wire Bytes` is reactive plus proactive wire bytes. The Evaluation interval is `[1500000, 2700000)` ms.

### 4.1 Conversation

| Strategy | Mean Completion (ms) | P95 (ms) | Token Hit Rate | Saved Tokens | Transfers (R/P) | Wire Bytes |
|---|---:|---:|---:|---:|---:|---:|
| R_AFF | 5366.478 | 11850.030 | 6.5250% | 3,108,577 | 0 / 0 | 0 |
| R_REQ_KV_V1 | 5368.270 | 11908.270 | 6.5476% | 3,119,327 | 10 / 0 | 8,029,995,008 |
| Matched-O1 | 5319.032 | 11798.720 | 6.6953% | 3,189,685 | 16 / 3006 | 101,894,324,224 |
| Full O2-A | 5138.881 | 11602.120 | 7.2721% | 3,464,506 | 11 / 56 | 30,578,573,312 |

### 4.2 ToolAgent

| Strategy | Mean Completion (ms) | P95 (ms) | Token Hit Rate | Saved Tokens | Transfers (R/P) | Wire Bytes |
|---|---:|---:|---:|---:|---:|---:|
| R_AFF | 3663.111 | 9457.290 | 37.6199% | 25,888,830 | 0 / 0 | 0 |
| R_REQ_KV_V1 | 3540.328 | 9367.220 | 37.5849% | 25,864,766 | 19 / 0 | 3,024,093,184 |
| Matched-O1 | 3521.521 | 9307.520 | 37.6325% | 25,897,534 | 15 / 3020 | 97,226,063,872 |
| Full O2-A | 3408.814 | 9129.220 | 37.9671% | 26,127,796 | 14 / 68 | 26,247,954,432 |

### 4.3 Supplemental parameter sweeps

| Sweep | Recorded variation | Result rows | Summary artifact |
|---|---|---:|---|
| S01 Oracle window | `W = 60, 300, 1800 s`; two workloads | 6 | `results/taskmain_sweeps/S01_oracle_window/oracle_window_summary.csv` |
| S02 Persistence | `h = 60, 300 s`; `K = 5, 10, 20`; two workloads | 12 | `results/taskmain_sweeps/S02_persistence_h_k/persistence_h_k_summary.csv` |
| S03 Recency | selection quantile `0.8, 0.9`; decay `60 s`; two workloads | 4 | `results/taskmain_sweeps/S03_recency/recency_summary.csv` |
| S04 Request-transfer gate | `theta = 1.5, 2.0, 3.0`; two workloads | 6 | `results/taskmain_sweeps/S04_theta/theta_summary.csv` |
| S05 Number of Pods | `N = 2, 4`; seven TaskMain policies; two workloads | 28 | `results/taskmain_sweeps/S05_num_pods/num_pods_summary.csv` |
| S06 Action semantics | `COPY`, `MOVE_SAFE`; two workloads | 4 | `results/taskmain_sweeps/S06_copy_vs_move/copy_vs_move_summary.csv` |
| S07 Scheduling | trigger-only, cost-aware; two workloads | 4 | `results/taskmain_sweeps/S07_cost_aware/cost_aware_summary.csv` |

## 5. O1 Results

Differences below are `Matched-O1 - R_REQ_KV_V1`. Completion columns report both the absolute difference and the relative difference. Token-hit differences are percentage points.

| Workload | Mean Completion Difference | P95 Difference | Token Hit Difference |
|---|---:|---:|---:|
| Conversation | -49.238 ms (-0.9172%) | -109.550 ms (-0.9199%) | +0.1477 pp |
| ToolAgent | -18.807 ms (-0.5312%) | -59.700 ms (-0.6373%) | +0.0476 pp |

## 6. O2-P Pilot Results

The pilot sampled 100 states per workload from the recorded multi-target-eligible states. Placement regret is the mean per-state latency difference between the best legal target and the O1 target.

| Workload | Sampled States | Positive Action Ratio | NO_COPY Ratio | O1 Target Positive Ratio | Mean Placement Regret (ms) |
|---|---:|---:|---:|---:|---:|
| Conversation | 100 | 35.0% | 65.0% | 16.0% | 6.628711 |
| ToolAgent | 100 | 36.0% | 64.0% | 28.0% | 6.319014 |

The candidate inventory contains 1200 states for each workload; 1190 Conversation states and 1077 ToolAgent states have at least two legal targets.

## 7. O2-A Pilot Results

`Rescued States` counts states where O2-P selected NO_COPY and O2-A selected a positive COPY. `Additional Headroom` is the recorded mean O2-A additional headroom in milliseconds per request. `Rank-1 Fraction` is calculated among O2-A-positive states, matching the pilot result field.

| Workload | O2-P Positive | O2-A Positive | Rescued States | Mean Additional Headroom (ms/request) | Rank-1 Fraction | Spearman Correlation |
|---|---:|---:|---:|---:|---:|---:|
| Conversation | 35.0% | 49.0% | 14 (21.5% of O2-P NO_COPY) | 5.699510 | 38.8% | 0.241994 |
| ToolAgent | 36.0% | 58.0% | 22 (34.4% of O2-P NO_COPY) | 4.650310 | 34.5% | 0.155761 |

The Spearman statistic uses 500 prefix-state observations per workload and compares Future-Demand score with the best per-prefix action value. The pilot states overlap and their action values are not additive closed-loop gains.

## 8. Full O2-A Closed-loop Results

Both replays contain one decision per second over the 1,200,000 ms Evaluation interval. Proactive wire bytes exclude reactive wire bytes.

| Workload | Decisions | COPY | NO_COPY | Mean Completion (ms) | Mean Queue (ms) | Mean Service (ms) | P95 (ms) | Token Hit Rate | Saved Tokens | Proactive Transfers | Proactive Wire Bytes | Unused Replicas | Wasted Copies |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Conversation | 1200 | 56 | 1144 | 5138.881 | 4103.471 | 1035.366 | 11602.120 | 7.2721% | 3,464,506 | 56 | 26,145,193,984 | 30 | 30 |
| ToolAgent | 1200 | 68 | 1132 | 3408.814 | 2897.062 | 511.721 | 9129.220 | 37.9671% | 26,127,796 | 68 | 20,199,768,064 | 30 | 30 |

## 9. Correctness Checks

| Result family | Future Leakage | Capacity Violation | Budget Violation | COPY Failure | Deterministic Replay | Request Loss |
|---|---:|---:|---:|---:|---|---:|
| TaskMain-v1.1 Development validation | 0 | 0 | 0 | N/A | `true` | N/A |
| TaskMain main, 14 runs | 0 for proactive runs | 0 | 0 for proactive runs | N/A | N/A | N/A |
| Matched M01-M04, 4 runs | 0 for proactive runs | 0 | 0 for proactive runs | N/A | N/A | N/A |
| O2-P pilot COPY branches | N/A | N/A | N/A | 0 / 597 | N/A | N/A |
| O2-A pilot COPY branches | N/A | N/A | N/A | 0 / 2992 | N/A | N/A |
| O2-A closed-loop smoke | N/A | 0 | 0 | 0 | `true` | 0 |
| Full O2-A Conversation | 0 | 0 | 0 | 0 | `SMOKE_VALIDATED_AND_CHECKPOINT_HASH_GUARDED` | 0 |
| Full O2-A ToolAgent | 0 | 0 | 0 | 0 | `SMOKE_VALIDATED_AND_CHECKPOINT_HASH_GUARDED` | 0 |

For the 14 TaskMain main runs and four matched runs, every summary reports peak memory at or below its configured capacity and zero capacity-admission failures. Across summaries containing proactive activity, `actions_started == actions_completed`; the main runs record 21,226 started/completed actions and the matched runs record 6,026 started/completed actions. These counts are lifecycle checks and are not substituted for a missing explicit `copy_failure` field.

The closed-loop smoke additionally records branch isolation `true` and deadlock count 0. Full O2-A records 56/56 admitted/completed COPY actions for Conversation and 68/68 for ToolAgent.

## 10. Raw Result References

### Protocol, configuration, and audits

- `docs/protocol/taskmain_v1.1.md`
- `docs/protocol/protocol_changelog.md`
- `configs/formal/taskmain_v1.1.yaml`
- `configs/taskmain_v1.1/conversation/`
- `configs/taskmain_v1.1/toolagent/`
- `configs/taskmain_v1.1/matched/`
- `configs/o2_oracle/full_o2a_conversation.yaml`
- `configs/o2_oracle/full_o2a_toolagent.yaml`
- `configs/o2_oracle/o2a_closed_loop_smoke.yaml`
- `results/development_validation/development_validation_summary.md`
- `results/development_validation/development_budget_audit.json`
- `results/summary/result_manifest.csv`

### Main and matched results

- `results/taskmain_evaluation/conversation/conversation_main_summary.csv`
- `results/taskmain_evaluation/toolagent/toolagent_main_summary.csv`
- `results/taskmain_evaluation/matched/matched_main_summary.csv`
- `results/taskmain_evaluation/conversation/E01_r_aff/summary.json`
- `results/taskmain_evaluation/toolagent/E08_r_aff/summary.json`
- `results/taskmain_evaluation/matched/M01_conversation_r_req_kv_v1/summary.json`
- `results/taskmain_evaluation/matched/M02_conversation_matched_o1/summary.json`
- `results/taskmain_evaluation/matched/M03_toolagent_r_req_kv_v1/summary.json`
- `results/taskmain_evaluation/matched/M04_toolagent_matched_o1/summary.json`

### O2-P, O2-A pilot, smoke, and full replay

- `results/o2_oracle/O2P_pilot/o2p_workload_summary.csv`
- `results/o2_oracle/O2P_pilot/o2p_state_summary.csv`
- `results/o2_oracle/O2P_pilot/o2p_branch_results.csv`
- `results/o2_oracle/O2P_pilot/o2p_pilot_report.md`
- `results/o2_oracle/O2A_pilot/o2a_workload_summary.csv`
- `results/o2_oracle/O2A_pilot/o2a_state_summary.csv`
- `results/o2_oracle/O2A_pilot/o2a_branch_results.csv`
- `results/o2_oracle/O2A_pilot/o2a_pilot_report.md`
- `results/o2a_smoke_validation/o2a_smoke_summary.json`
- `results/o2a_smoke_validation/o2a_action_sequence.json`
- `results/full_o2a/conversation/summary.json`
- `results/full_o2a/conversation/action_sequent.json`
- `results/full_o2a/toolagent/summary.json`
- `results/full_o2a/toolagent/action_sequence.json`

### Supplemental sweep summaries

- `results/taskmain_sweeps/S01_oracle_window/oracle_window_summary.csv`
- `results/taskmain_sweeps/S02_persistence_h_k/persistence_h_k_summary.csv`
- `results/taskmain_sweeps/S03_recency/recency_summary.csv`
- `results/taskmain_sweeps/S04_theta/theta_summary.csv`
- `results/taskmain_sweeps/S05_num_pods/num_pods_summary.csv`
- `results/taskmain_sweeps/S06_copy_vs_move/copy_vs_move_summary.csv`
- `results/taskmain_sweeps/S07_cost_aware/cost_aware_summary.csv`
