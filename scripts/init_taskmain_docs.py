#!/usr/bin/env python3
"""Create pending registry pages; never writes experiment results."""
from pathlib import Path

EXPERIMENTS = [
    ("E01", "Conversation", "R_AFF"), ("E02", "Conversation", "R_REQ_KV_TASK"),
    ("E03", "Conversation", "TaskMain-Oracle"), ("E04", "Conversation", "Persist h=60 K=10"),
    ("E05", "Conversation", "Persist h=300 K=10"), ("E06", "Conversation", "Recency q=0.9"),
    ("E07", "Conversation", "Cost-aware"), ("E08", "ToolAgent", "R_AFF"),
    ("E09", "ToolAgent", "R_REQ_KV_TASK"), ("E10", "ToolAgent", "TaskMain-Oracle"),
    ("E11", "ToolAgent", "Persist h=60 K=10"), ("E12", "ToolAgent", "Persist h=300 K=10"),
    ("E13", "ToolAgent", "Recency q=0.9"), ("E14", "ToolAgent", "Cost-aware"),
]
NAMES = {
    "E01": "conversation_r_aff", "E02": "conversation_r_req_kv_task", "E03": "conversation_oracle",
    "E04": "conversation_persist_h1_k10", "E05": "conversation_persist_h5_k10",
    "E06": "conversation_recency_q09", "E07": "conversation_cost_aware",
    "E08": "toolagent_r_aff", "E09": "toolagent_r_req_kv_task", "E10": "toolagent_oracle",
    "E11": "toolagent_persist_h1_k10", "E12": "toolagent_persist_h5_k10",
    "E13": "toolagent_recency_q09", "E14": "toolagent_cost_aware",
}
FIELDS = """- mean completion: pending
- P50 completion: pending
- P95 completion: pending
- mean queue: pending
- mean service: pending
- request hit rate: pending
- token hit rate: pending
- Saved Prefill Tokens: pending
- Gini actual: pending
- Gini random mean: pending
- Gini ratio: pending
- reactive transfer count: pending
- proactive transfer count: pending
- reactive wire bytes: pending
- proactive wire bytes: pending
- total wire bytes: pending
- evictions: pending
- wasted copies: pending
- fallbacks: pending"""
TEMPLATE = """# {id} {workload} - {policy}

> Status: pending
> Protocol: TaskMain-v1.1
> Experiment ID: {id}
> Run ID: pending
> Git Commit: pending
> Config: pending
> Result Directory: pending

## 1. 实验目的

Measure this experiment's preregistered policy row only.

## 2. 配置

workload/policy/N/capacity/page_bytes/W/h/K/recency quantile/theta/bandwidth/control
latency/proactive budget/action cap/service model/simulator version/protocol version/
trace path/trace SHA256/split/Git commit/config path: pending.

## 3. 正确性检查

pytest/deterministic replay/invariant failures/future leakage/capacity violations/
invalid transfers/budget violations/censored future reads/failure count: pending.

## 4. 核心结果

{fields}

## 5. 相对基线

- vs R_AFF: pending
- vs R_REQ_KV_TASK: pending

## 6. 本实验观察

pending
"""

def main():
    for ident, workload, policy in EXPERIMENTS:
        path = Path("docs/experiments") / f"{ident}_{NAMES[ident]}.md"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(TEMPLATE.format(id=ident, workload=workload, policy=policy, fields=FIELDS))
    groups = {
        "matched": ["M01_conversation_r_req_kv_v1", "M02_conversation_matched_o1",
                    "M03_toolagent_r_req_kv_v1", "M04_toolagent_matched_o1"],
        "sweeps": ["S01_oracle_window", "S02_persistence_h_k", "S03_recency", "S04_theta",
                   "S05_num_pods", "S06_copy_vs_move", "S07_cost_aware"],
        "diagnostics": ["D01_relaxed_capacity", "D02_no_evict_admission",
                        "D03_fanout2", "D04_common_support_horizon"],
        "analysis": ["main_table", "sweep_analysis", "final_analysis"],
    }
    for folder, names in groups.items():
        for name in names:
            path = Path("docs") / folder / f"{name}.md"
            path.parent.mkdir(parents=True, exist_ok=True)
            status = "validated" if folder == "diagnostics" else "pending"
            path.write_text(f"# {name}\n\n> Status: {status}\n\nNo TaskMain formal results recorded.\n")

if __name__ == "__main__":
    main()
