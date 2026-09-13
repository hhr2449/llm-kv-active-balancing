from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

EXPERIMENTS = (
    ("S01-C-W1", "Conversation", 60),
    ("S01-C-W5", "Conversation", 300),
    ("S01-C-W30", "Conversation", 1800),
    ("S01-T-W1", "ToolAgent", 60),
    ("S01-T-W5", "ToolAgent", 300),
    ("S01-T-W30", "ToolAgent", 1800),
)
EVALUATION_START_MS, EVALUATION_END_MS = 900_000, 1_500_000


def nested(data: dict[str, Any], *keys: str, default: Any = "") -> Any:
    value: Any = data
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return default
        value = value[key]
    return value


def evaluation_requests(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        return [row for row in map(json.loads, handle) if row.get("split") == "EVALUATION"]


def scoped_actions(summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [a for a in nested(summary, "proactive", "action_records", default=[])
            if EVALUATION_START_MS <= a.get("trigger_time", -1) < EVALUATION_END_MS]


def build_row(root: Path, experiment_id: str, workload: str, horizon_s: int) -> dict[str, Any]:
    directory = root / experiment_id
    summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
    if summary.get("metric_scope") != "EVALUATION":
        raise ValueError(f"{experiment_id}: metric scope is not EVALUATION")
    requests = evaluation_requests(directory / "requests.jsonl")
    actions = scoped_actions(summary)
    page_bytes = nested(summary, "provenance", "p2p", "page_bytes", default=0)
    proactive_wire = sum(a.get("transferable_pages", 0) * page_bytes for a in actions)
    created = [a for a in actions if a.get("newly_resident_pages", 0) > 0]
    saved, inputs = summary.get("h_used_tokens", 0), summary.get("input_tokens", 0)
    proactive = summary.get("proactive", {})
    return {
        "experiment_id": experiment_id,
        "workload": workload,
        "W_seconds": horizon_s,
        "evaluation_request_count": len(requests),
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
        "unused_replica_count": sum(not a.get("actually_used", False) for a in created),
        "wasted_copy_count": sum(a.get("classification") not in {"USEFUL", "CENSORED"}
                                 for a in actions),
        "evictions": summary.get("eviction_count", ""),
        "candidate_count": proactive.get("candidate_count", ""),
        "actions_started": proactive.get("actions_started", ""),
        "budget_insufficient_count": proactive.get("budget_insufficient_current_tokens", ""),
    }


def add_deltas(rows: list[dict[str, Any]]) -> None:
    for group in (rows[:3], rows[3:]):
        baseline = group[1]
        counts = {row["evaluation_request_count"] for row in group}
        if len(counts) != 1:
            raise ValueError(f"{baseline['workload']}: Evaluation request sets differ")
        for row in group:
            row["delta_mean_completion_vs_w5_pct"] = (
                (row["mean_completion_ms"] / baseline["mean_completion_ms"] - 1) * 100)
            row["delta_token_hit_vs_w5_pp"] = (
                (row["token_hit_rate"] - baseline["token_hit_rate"]) * 100)
            row["delta_saved_prefill_tokens_vs_w5"] = (
                row["saved_prefill_tokens"] - baseline["saved_prefill_tokens"])
            row["delta_proactive_wire_bytes_vs_w5"] = (
                row["proactive_wire_bytes"] - baseline["proactive_wire_bytes"])
            row["delta_unused_replica_count_vs_w5"] = (
                row["unused_replica_count"] - baseline["unused_replica_count"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize S01 Oracle window sweep")
    parser.add_argument("--result-root", type=Path, required=True)
    args = parser.parse_args()
    rows = [build_row(args.result_root, *experiment) for experiment in EXPERIMENTS]
    add_deltas(rows)
    output = args.result_root / "oracle_window_summary.csv"
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    (args.result_root / "oracle_window_summary.json").write_text(
        json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {output}")


if __name__ == "__main__":
    main()
