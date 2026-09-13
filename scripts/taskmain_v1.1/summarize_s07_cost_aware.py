from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "results/taskmain_sweeps/S07_cost_aware/cost_aware_summary.csv"
EVALUATION_START_MS, EVALUATION_END_MS = 1_500_000, 2_700_000
ROWS = (
    ("Conversation", "Trigger-only", "results/taskmain_evaluation/conversation/E05_persist_h5_k10"),
    ("Conversation", "Cost-aware", "results/taskmain_evaluation/conversation/E07_cost_aware"),
    ("ToolAgent", "Trigger-only", "results/taskmain_evaluation/toolagent/E12_persist_h5_k10"),
    ("ToolAgent", "Cost-aware", "results/taskmain_evaluation/toolagent/E14_cost_aware"),
)


def nested(data: dict[str, Any], *keys: str, default: Any = "") -> Any:
    value: Any = data
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return default
        value = value[key]
    return value


def build_row(workload: str, scheduling: str, relative_path: str) -> dict[str, Any]:
    directory = ROOT / relative_path
    summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
    if summary.get("metric_scope") != "EVALUATION":
        raise ValueError(f"{directory}: metric scope is not EVALUATION")
    with (directory / "requests.jsonl").open(encoding="utf-8") as handle:
        requests = [row for row in map(json.loads, handle) if row.get("split") == "EVALUATION"]
    proactive = summary["proactive"]
    actions = [a for a in proactive["action_records"]
               if EVALUATION_START_MS <= a["trigger_time"] < EVALUATION_END_MS]
    page_bytes = nested(summary, "provenance", "p2p", "page_bytes", default=0)
    proactive_wire = sum(a["transferable_pages"] * page_bytes for a in actions)
    created = [a for a in actions if a.get("newly_resident_pages", 0) > 0]
    saved, inputs = summary["h_used_tokens"], summary["input_tokens"]
    return {
        "workload": workload,
        "scheduling": scheduling,
        "mean_completion_ms": nested(summary, "completion_latency_ms", "mean"),
        "p50_completion_ms": nested(summary, "completion_latency_ms", "p50"),
        "p95_completion_ms": nested(summary, "completion_latency_ms", "p95"),
        "request_hit_rate": (sum(r["h_used_tokens"] > 0 for r in requests) / len(requests)
                             if requests else 0.0),
        "token_hit_rate": saved / inputs if inputs else 0.0,
        "saved_prefill_tokens": saved,
        "proactive_transfer_count": len(actions),
        "proactive_wire_bytes": proactive_wire,
        "total_wire_bytes": proactive_wire + nested(summary, "transfer", "wire_bytes", default=0),
        "evictions": summary["eviction_count"],
        "unused_replica_count": sum(not a.get("actually_used", False) for a in created),
        "wasted_copy_count": sum(a.get("classification") not in {"USEFUL", "CENSORED"}
                                 for a in actions),
        "candidate_count": proactive.get("candidate_count", ""),
        "score_gt_zero_count": proactive.get("score_gt_zero_count", ""),
        "cost_filter_rejected_count": proactive.get("cost_filter_rejected_count", ""),
    }


def add_deltas(rows: list[dict[str, Any]]) -> None:
    for trigger, cost in ((rows[0], rows[1]), (rows[2], rows[3])):
        for row in (trigger, cost):
            row["delta_mean_completion_pct"] = (
                (row["mean_completion_ms"] / trigger["mean_completion_ms"] - 1) * 100)
            row["delta_p95_completion_pct"] = (
                (row["p95_completion_ms"] / trigger["p95_completion_ms"] - 1) * 100)
            row["delta_token_hit_pp"] = (row["token_hit_rate"] - trigger["token_hit_rate"]) * 100
            row["delta_saved_prefill_tokens"] = (
                row["saved_prefill_tokens"] - trigger["saved_prefill_tokens"])
            row["delta_proactive_transfer_count"] = (
                row["proactive_transfer_count"] - trigger["proactive_transfer_count"])
            row["delta_proactive_wire_bytes"] = (
                row["proactive_wire_bytes"] - trigger["proactive_wire_bytes"])
            row["delta_evictions"] = row["evictions"] - trigger["evictions"]
            row["delta_unused_replica_count"] = (
                row["unused_replica_count"] - trigger["unused_replica_count"])


def main() -> None:
    rows = [build_row(*spec) for spec in ROWS]
    add_deltas(rows)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    (OUTPUT.with_suffix(".json")).write_text(json.dumps(rows, indent=2) + "\n",
                                              encoding="utf-8")
    print(OUTPUT)


if __name__ == "__main__":
    main()
