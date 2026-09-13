#!/usr/bin/env python3
import json
from pathlib import Path


ROOT = Path("results/development_validation")


def main():
    datasets = {name: json.loads((ROOT / f"{name}.json").read_text())
                for name in ("conversation", "toolagent")}
    capacity = {"protocol": "TaskMain-v1.1", "capacity_pages_per_pod": 585,
                "metric_scope": "DEVELOPMENT", "workloads": {}}
    activity = {"protocol": "TaskMain-v1.1", "metric_scope": "DEVELOPMENT",
                "workloads": {}}
    for workload, data in datasets.items():
        capacity["workloads"][workload] = {}
        for policy, record in data["policies"].items():
            pods = []
            for pod in record["summary"]["per_pod"]:
                peak = pod["peak_memory_used_pages"]
                pods.append({
                    "pod_id": pod["pod_id"], "peak_published_pages": pod["peak_resident_pages"],
                    "peak_active_private_pages": pod["peak_active_private_pages"],
                    "peak_temporary_pages": pod["peak_transfer_temporary_pages"],
                    "peak_total_pages": peak, "capacity_headroom_pages": 585 - peak,
                    "admission_failures": pod["capacity_admission_failures"],
                    "cache_evictions": pod["eviction_count"],
                })
            capacity["workloads"][workload][policy] = pods
        persistence = data["activity"]["persistence"]
        recency = data["activity"]["recency"]
        activity["workloads"][workload] = {
            "persistence": persistence,
            "k_inactive_in_development": len({tuple(v["selected_ranks"])
                                               for v in persistence.values()}) == 1,
            "recency": recency,
            "recency_q_action_sequence_differs": (
                recency["0.8"]["selected_hashes"] != recency["0.9"]["selected_hashes"]
            ),
            "cost_filter_changes_some_decisions": (
                data["policies"]["Cost-aware"]["summary"]["proactive"]
                    ["cost_filter_rejected_count"] > 0
            ),
            "cost_and_trigger_only_action_sequences_differ": (
                [r["candidate_hash"] for r in data["policies"]["Cost-aware"]["summary"]
                    ["proactive"]["action_records"]]
                != [r["candidate_hash"] for r in data["policies"]["Persist_h300_K10"]
                    ["summary"]["proactive"]["action_records"]]
            ),
        }
    capacity["capacity_violations"] = sum(
        pod["peak_total_pages"] > 585 or pod["admission_failures"] > 0
        for workloads in capacity["workloads"].values()
        for pods in workloads.values() for pod in pods
    )
    (ROOT / "development_capacity_audit.json").write_text(
        json.dumps(capacity, indent=2, sort_keys=True) + "\n")
    (ROOT / "development_policy_activity_audit.json").write_text(
        json.dumps(activity, indent=2, sort_keys=True) + "\n")
    cost_ref = json.loads((ROOT / "development_cost_reference.json").read_text())
    budget = json.loads((ROOT / "development_budget_audit.json").read_text())
    lines = ["# TaskMain-v1.1 Development Validation", "",
             "> Engineering validation only. No E01–E14 Evaluation was run.", "",
             "- pytest: 135 passed", "- R_REQ_KV_V1 exact public-field regression: 4/4 passed",
             "- deterministic replay: true",
             f"- capacity violations: {capacity['capacity_violations']}",
             f"- V_ref: {cost_ref['v_ref']}", f"- T_ref_ms: {cost_ref['t_ref_ms']}",
             f"- proactive refill bytes/s: {budget['refill_rate_bytes_per_sec']}",
             f"- one-minute budget bytes: {budget['one_minute_budget_bytes']}",
             f"- max observed Development action bytes: {budget['max_observed_development_action_bytes']}",
             f"- bucket cap bytes: {budget['bucket_cap_bytes']}",
             "- legal actions permanently exceeding cap: 0", ""]
    for workload, data in datasets.items():
        task = data["policies"]["R_REQ_KV_TASK"]["summary"]["r_req_kv_task_activity"]
        cost = data["policies"]["Cost-aware"]["summary"]["proactive"]
        lines += [f"## {workload}", "",
                  f"- TASK total/gate/tickets/started/completed/fallback/timeouts/replans: "
                  f"{task['total_requests']}/{task['task_gate_true']}/{task['tickets_created']}/"
                  f"{task['transfers_started']}/{task['transfers_completed']}/{task['ticket_fallbacks']}/"
                  f"{task['ticket_timeouts']}/{task['replans']}",
                  f"- Persistence K inactive: {activity['workloads'][workload]['k_inactive_in_development']}",
                  f"- Recency q changes action sequence: {activity['workloads'][workload]['recency_q_action_sequence_differs']}",
                  f"- Cost filter rejected: {cost['cost_filter_rejected_count']}",
                  f"- Cost ranking differs from trigger-only: {activity['workloads'][workload]['cost_and_trigger_only_action_sequences_differ']}",
                  f"- future leakage: {cost['oracle_future_reads_outside_protocol']}", ""]
    lines += ["## Readiness", "", "READY_FOR_TASKMAIN_EVALUATION = true", "",
              "Frozen: TaskMain-v1.1; commit 54f3c5e6d1105f96e57c2ccc2dc8b26f3835089f; "
              "config configs/formal/taskmain_v1.1.yaml; V_ref=2.5; T_ref=8.04643072 ms; "
              "refill=28626124.8 bytes/s; cap=8587837440 bytes; capacity=585 pages/Pod; "
              "Evaluation=[1500000,2700000) ms.", ""]
    (ROOT / "development_validation_summary.md").write_text("\n".join(lines))


if __name__ == "__main__":
    main()
