from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any

EVALUATION_START_MS, EVALUATION_END_MS = 1_500_000, 2_700_000
ROWS = (
    ("Conversation", 0.8, "s03_new_run", "S03-C-Q08"),
    ("Conversation", 0.9, "taskmain_reused", "results/taskmain_evaluation/conversation/E06_recency_q09"),
    ("ToolAgent", 0.8, "s03_new_run", "S03-T-Q08"),
    ("ToolAgent", 0.9, "taskmain_reused", "results/taskmain_evaluation/toolagent/E13_recency_q09"),
)


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


def percentile(values: list[float], q: float) -> float | str:
    if not values:
        return ""
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    low, high = math.floor(position), math.ceil(position)
    if low == high:
        return ordered[low]
    return ordered[low] * (high - position) + ordered[high] * (position - low)


def build_row(directory: Path, workload: str, selection_quantile: float,
              source: str) -> dict[str, Any]:
    summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
    if summary.get("metric_scope") != "EVALUATION":
        raise ValueError(f"{directory}: metric scope is not EVALUATION")
    requests = eval_requests(directory / "requests.jsonl")
    proactive = summary.get("proactive", {})
    actions = [a for a in proactive.get("action_records", [])
               if EVALUATION_START_MS <= a.get("trigger_time", -1) < EVALUATION_END_MS]
    page_bytes = nested(summary, "provenance", "p2p", "page_bytes", default=0)
    proactive_wire = sum(a.get("transferable_pages", 0) * page_bytes for a in actions)
    created = [a for a in actions if a.get("newly_resident_pages", 0) > 0]
    thresholds = proactive.get("recency_thresholds", [])
    saved, inputs = summary.get("h_used_tokens", 0), summary.get("input_tokens", 0)
    return {
        "workload": workload,
        "selection_quantile": selection_quantile,
        "decay_seconds": 60,
        "result_source": source,
        "mean_completion_ms": nested(summary, "completion_latency_ms", "mean"),
        "p50_completion_ms": nested(summary, "completion_latency_ms", "p50"),
        "p95_completion_ms": nested(summary, "completion_latency_ms", "p95"),
        "mean_queue_ms": nested(summary, "queue_time_ms", "mean"),
        "mean_service_ms": nested(summary, "service_time_ms", "mean"),
        "request_hit_rate": (sum(r["h_used_tokens"] > 0 for r in requests) / len(requests)
                             if requests else 0.0),
        "token_hit_rate": saved / inputs if inputs else 0.0,
        "saved_prefill_tokens": saved,
        "proactive_transfer_count": len(actions),
        "proactive_wire_bytes": proactive_wire,
        "total_wire_bytes": proactive_wire + nested(summary, "transfer", "wire_bytes", default=0),
        "evictions": summary.get("eviction_count", ""),
        "unused_replica_count": sum(not a.get("actually_used", False) for a in created),
        "wasted_copy_count": sum(a.get("classification") not in {"USEFUL", "CENSORED"}
                                 for a in actions),
        "eligible_candidate_count": proactive.get("candidate_count", ""),
        "positive_candidate_count": proactive.get("positive_candidate_count", ""),
        "actions_started": proactive.get("actions_started", ""),
        "quantile_threshold_p50": percentile(thresholds, 0.5),
        "quantile_threshold_p90": percentile(thresholds, 0.9),
    }


def add_deltas(rows: list[dict[str, Any]]) -> None:
    for q08, q09 in ((rows[0], rows[1]), (rows[2], rows[3])):
        for row in (q08, q09):
            row["delta_mean_completion_vs_q09_pct"] = (
                (row["mean_completion_ms"] / q09["mean_completion_ms"] - 1) * 100)
            row["delta_p95_completion_vs_q09_pct"] = (
                (row["p95_completion_ms"] / q09["p95_completion_ms"] - 1) * 100)
            row["delta_token_hit_vs_q09_pp"] = (
                (row["token_hit_rate"] - q09["token_hit_rate"]) * 100)
            row["delta_saved_prefill_tokens_vs_q09"] = (
                row["saved_prefill_tokens"] - q09["saved_prefill_tokens"])
            row["delta_proactive_transfers_vs_q09"] = (
                row["proactive_transfer_count"] - q09["proactive_transfer_count"])
            row["delta_proactive_wire_bytes_vs_q09"] = (
                row["proactive_wire_bytes"] - q09["proactive_wire_bytes"])
            row["delta_unused_replicas_vs_q09"] = (
                row["unused_replica_count"] - q09["unused_replica_count"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize S03 Recency sweep")
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--result-root", type=Path, required=True)
    args = parser.parse_args()
    rows = []
    for workload, quantile, source, location in ROWS:
        directory = (args.repo_root / location if source == "taskmain_reused"
                     else args.result_root / location)
        rows.append(build_row(directory, workload, quantile, source))
    add_deltas(rows)
    output = args.result_root / "recency_summary.csv"
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    (args.result_root / "recency_summary.json").write_text(
        json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {output}")


if __name__ == "__main__":
    main()
