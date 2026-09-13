from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

ROWS = (
    ("Conversation", 1.5, "s04_new_run", "S04-C-T15"),
    ("Conversation", 2.0, "taskmain_reused", "results/taskmain_evaluation/conversation/E02_r_req_kv_task"),
    ("Conversation", 3.0, "s04_new_run", "S04-C-T30"),
    ("ToolAgent", 1.5, "s04_new_run", "S04-T-T15"),
    ("ToolAgent", 2.0, "taskmain_reused", "results/taskmain_evaluation/toolagent/E09_r_req_kv_task"),
    ("ToolAgent", 3.0, "s04_new_run", "S04-T-T30"),
)


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


def build_row(directory: Path, workload: str, theta: float, source: str) -> dict[str, Any]:
    summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
    if summary.get("metric_scope") != "EVALUATION":
        raise ValueError(f"{directory}: metric scope is not EVALUATION")
    requests = evaluation_requests(directory / "requests.jsonl")
    transfer = summary.get("transfer", {})
    activity = summary.get("r_req_kv_task_activity", {})
    saved, inputs = summary.get("h_used_tokens", 0), summary.get("input_tokens", 0)
    completed = transfer.get("transfer_completed", 0)
    tickets_eval = transfer.get("ticket_created", 0)
    return {
        "workload": workload, "theta": theta, "result_source": source,
        "mean_completion_ms": nested(summary, "completion_latency_ms", "mean"),
        "p50_completion_ms": nested(summary, "completion_latency_ms", "p50"),
        "p95_completion_ms": nested(summary, "completion_latency_ms", "p95"),
        "mean_queue_ms": nested(summary, "queue_time_ms", "mean"),
        "mean_service_ms": nested(summary, "service_time_ms", "mean"),
        "request_hit_rate": (sum(r["h_used_tokens"] > 0 for r in requests) / len(requests)
                             if requests else 0.0),
        "token_hit_rate": saved / inputs if inputs else 0.0,
        "saved_prefill_tokens": saved,
        "reactive_transfer_count": completed,
        "reactive_wire_bytes": transfer.get("wire_bytes", 0),
        "total_wire_bytes": transfer.get("wire_bytes", 0),
        "evictions": summary.get("eviction_count", ""),
        "fallbacks": tickets_eval - completed,
        "timeouts": transfer.get("transfer_admission_timeouts", 0),
        "activity_counter_scope": "FULL_REPLAY",
        "replans": activity.get("replans", ""),
        "task_gate_true_count": activity.get("task_gate_true", ""),
        "tickets_created": activity.get("tickets_created", ""),
        "transfers_started": activity.get("transfers_started", ""),
        "transfers_completed": activity.get("transfers_completed", ""),
        "stayed_local_gate_false": activity.get("stayed_local_gate_false", ""),
        "stayed_local_no_transferable_prefix": activity.get(
            "stayed_local_no_transferable_prefix", ""),
    }


def add_deltas(rows: list[dict[str, Any]]) -> None:
    for group in (rows[:3], rows[3:]):
        baseline = group[1]
        for row in group:
            row["delta_mean_completion_vs_theta2_pct"] = (
                (row["mean_completion_ms"] / baseline["mean_completion_ms"] - 1) * 100)
            row["delta_p95_completion_vs_theta2_pct"] = (
                (row["p95_completion_ms"] / baseline["p95_completion_ms"] - 1) * 100)
            row["delta_token_hit_vs_theta2_pp"] = (
                (row["token_hit_rate"] - baseline["token_hit_rate"]) * 100)
            row["delta_saved_prefill_tokens_vs_theta2"] = (
                row["saved_prefill_tokens"] - baseline["saved_prefill_tokens"])
            row["delta_reactive_transfers_vs_theta2"] = (
                row["reactive_transfer_count"] - baseline["reactive_transfer_count"])
            row["delta_reactive_wire_bytes_vs_theta2"] = (
                row["reactive_wire_bytes"] - baseline["reactive_wire_bytes"])
            row["delta_fallbacks_vs_theta2"] = row["fallbacks"] - baseline["fallbacks"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize S04 R_REQ_KV_TASK theta sweep")
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--result-root", type=Path, required=True)
    args = parser.parse_args()
    rows = []
    for workload, theta, source, location in ROWS:
        directory = (args.repo_root / location if source == "taskmain_reused"
                     else args.result_root / location)
        rows.append(build_row(directory, workload, theta, source))
    add_deltas(rows)
    output = args.result_root / "theta_summary.csv"
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    (args.result_root / "theta_summary.json").write_text(
        json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {output}")


if __name__ == "__main__":
    main()
