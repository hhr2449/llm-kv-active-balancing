from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

EVALUATION_START_MS, EVALUATION_END_MS = 1_500_000, 2_700_000
POLICIES = (
    ("R_AFF", "R_AFF", "r_aff"),
    ("R_REQ_KV_TASK", "R_REQ_KV_TASK", "r_req_kv_task"),
    ("TaskMain-Oracle", "ORACLE", "oracle"),
    ("Persist h=60s K=10", "PERSIST_H1_K10", "persist_h1_k10"),
    ("Persist h=300s K=10", "PERSIST_H5_K10", "persist_h5_k10"),
    ("Recency q=0.9", "RECENCY_Q09", "recency_q09"),
    ("Cost-aware", "COST_AWARE", "cost_aware"),
)
N4_RESULTS = {
    "Conversation": ("conversation", ("E01_r_aff", "E02_r_req_kv_task", "E03_oracle",
        "E04_persist_h1_k10", "E05_persist_h5_k10", "E06_recency_q09", "E07_cost_aware")),
    "ToolAgent": ("toolagent", ("E08_r_aff", "E09_r_req_kv_task", "E10_oracle",
        "E11_persist_h1_k10", "E12_persist_h5_k10", "E13_recency_q09", "E14_cost_aware")),
}


def nested(data: dict[str, Any], *keys: str, default: Any = "") -> Any:
    value: Any = data
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return default
        value = value[key]
    return value


def eval_requests(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        return [row for row in map(json.loads, handle) if row.get("split") == "EVALUATION"]


def build_row(directory: Path, workload: str, policy: str, num_pods: int,
              source: str) -> dict[str, Any]:
    summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
    if summary.get("metric_scope") != "EVALUATION" or summary.get("num_pods") != num_pods:
        raise ValueError(f"{directory}: unexpected metric scope or Pod count")
    requests = eval_requests(directory / "requests.jsonl")
    proactive = summary.get("proactive")
    if isinstance(proactive, dict):
        actions = [a for a in proactive.get("action_records", [])
                   if EVALUATION_START_MS <= a.get("trigger_time", -1) < EVALUATION_END_MS]
        page_bytes = nested(summary, "provenance", "p2p", "page_bytes", default=0)
        proactive_wire = sum(a.get("transferable_pages", 0) * page_bytes for a in actions)
        created = [a for a in actions if a.get("newly_resident_pages", 0) > 0]
        unused = sum(not a.get("actually_used", False) for a in created)
        wasted = sum(a.get("classification") not in {"USEFUL", "CENSORED"} for a in actions)
    else:
        actions, proactive_wire, unused, wasted = [], 0, "", ""
    transfer = summary.get("transfer", {})
    reactive_count = transfer.get("transfer_completed", 0)
    reactive_wire = transfer.get("wire_bytes", 0)
    tickets = transfer.get("ticket_created", 0)
    saved, inputs = summary.get("h_used_tokens", 0), summary.get("input_tokens", 0)
    return {
        "workload": workload, "policy": policy, "N": num_pods, "result_source": source,
        "mean_completion_ms": nested(summary, "completion_latency_ms", "mean"),
        "p50_completion_ms": nested(summary, "completion_latency_ms", "p50"),
        "p95_completion_ms": nested(summary, "completion_latency_ms", "p95"),
        "mean_queue_ms": nested(summary, "queue_time_ms", "mean"),
        "mean_service_ms": nested(summary, "service_time_ms", "mean"),
        "request_hit_rate": (sum(r["h_used_tokens"] > 0 for r in requests) / len(requests)
                             if requests else 0.0),
        "token_hit_rate": saved / inputs if inputs else 0.0,
        "saved_prefill_tokens": saved,
        "reactive_transfer_count": reactive_count,
        "proactive_transfer_count": len(actions),
        "reactive_wire_bytes": reactive_wire,
        "proactive_wire_bytes": proactive_wire,
        "total_wire_bytes": reactive_wire + proactive_wire,
        "evictions": summary.get("eviction_count", ""),
        "fallbacks": tickets - reactive_count,
        "unused_replica_count": unused,
        "wasted_copy_count": wasted,
        "cluster_total_capacity_pages": num_pods * summary.get("cache_capacity_pages", 0),
        "proactive_refill_bytes_per_sec": nested(
            summary, "provenance", "proactive", "byte_rate"),
        "gini_actual": nested(summary, "cluster", "executed_miss_token_gini"),
        "gini_random_mean": "",
        "gini_ratio": "",
    }


def add_deltas(rows: list[dict[str, Any]]) -> None:
    index = {(r["workload"], r["policy"], r["N"]): r for r in rows}
    for row in rows:
        baseline = index[(row["workload"], row["policy"], 4)]
        row["delta_mean_completion_n2_vs_n4_pct"] = (
            (row["mean_completion_ms"] / baseline["mean_completion_ms"] - 1) * 100)
        row["delta_p95_completion_n2_vs_n4_pct"] = (
            (row["p95_completion_ms"] / baseline["p95_completion_ms"] - 1) * 100)
        row["delta_token_hit_n2_vs_n4_pp"] = (
            (row["token_hit_rate"] - baseline["token_hit_rate"]) * 100)
        row["delta_saved_prefill_tokens_n2_vs_n4"] = (
            row["saved_prefill_tokens"] - baseline["saved_prefill_tokens"])
        row["delta_transfer_count_n2_vs_n4"] = (
            row["reactive_transfer_count"] + row["proactive_transfer_count"]
            - baseline["reactive_transfer_count"] - baseline["proactive_transfer_count"])
        row["delta_total_wire_bytes_n2_vs_n4"] = (
            row["total_wire_bytes"] - baseline["total_wire_bytes"])
        row["delta_evictions_n2_vs_n4"] = row["evictions"] - baseline["evictions"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize S05 Pod-count sweep")
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--result-root", type=Path, required=True)
    args = parser.parse_args()
    rows = []
    for workload, prefix in (("Conversation", "C"), ("ToolAgent", "T")):
        n4_group, n4_names = N4_RESULTS[workload]
        for index, (policy, run_slug, _config_slug) in enumerate(POLICIES):
            rows.append(build_row(args.result_root / f"S05-{prefix}-N2-{run_slug}",
                                  workload, policy, 2, "s05_new_run"))
            rows.append(build_row(args.repo_root / "results/taskmain_evaluation" / n4_group
                                  / n4_names[index], workload, policy, 4,
                                  "taskmain_reused"))
    add_deltas(rows)
    output = args.result_root / "num_pods_summary.csv"
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    (args.result_root / "num_pods_summary.json").write_text(
        json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {output}")


if __name__ == "__main__":
    main()
