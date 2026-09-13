from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

EVALUATION_START_MS, EVALUATION_END_MS = 1_500_000, 2_700_000
ROWS = (
    ("Conversation", "COPY", "taskmain_reused", "results/taskmain_evaluation/conversation/E05_persist_h5_k10"),
    ("Conversation", "MOVE_SAFE", "s06_new_run", "S06-C-MOVE"),
    ("ToolAgent", "COPY", "taskmain_reused", "results/taskmain_evaluation/toolagent/E12_persist_h5_k10"),
    ("ToolAgent", "MOVE_SAFE", "s06_new_run", "S06-T-MOVE"),
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


def build_row(directory: Path, workload: str, action: str, source: str) -> dict[str, Any]:
    summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
    if summary.get("metric_scope") != "EVALUATION":
        raise ValueError(f"{directory}: metric scope is not EVALUATION")
    requests = eval_requests(directory / "requests.jsonl")
    records = [a for a in nested(summary, "proactive", "action_records", default=[])
               if EVALUATION_START_MS <= a.get("trigger_time", -1) < EVALUATION_END_MS]
    page_bytes = nested(summary, "provenance", "p2p", "page_bytes", default=0)
    proactive_wire = sum(a.get("transferable_pages", 0) * page_bytes for a in records)
    created = [a for a in records if a.get("newly_resident_pages", 0) > 0]
    saved, inputs = summary.get("h_used_tokens", 0), summary.get("input_tokens", 0)
    move_records = records if action == "MOVE_SAFE" else []
    freed = sum(a.get("move_source_pages_freed", 0) for a in move_records)
    return {
        "workload": workload, "action": action, "result_source": source,
        "mean_completion_ms": nested(summary, "completion_latency_ms", "mean"),
        "p50_completion_ms": nested(summary, "completion_latency_ms", "p50"),
        "p95_completion_ms": nested(summary, "completion_latency_ms", "p95"),
        "mean_queue_ms": nested(summary, "queue_time_ms", "mean"),
        "mean_service_ms": nested(summary, "service_time_ms", "mean"),
        "request_hit_rate": (sum(r["h_used_tokens"] > 0 for r in requests) / len(requests)
                             if requests else 0.0),
        "token_hit_rate": saved / inputs if inputs else 0.0,
        "saved_prefill_tokens": saved,
        "proactive_transfer_count": len(records),
        "proactive_wire_bytes": proactive_wire,
        "total_wire_bytes": proactive_wire + nested(summary, "transfer", "wire_bytes", default=0),
        "evictions": summary.get("eviction_count", ""),
        "unused_replica_count": sum(not a.get("actually_used", False) for a in created),
        "wasted_copy_count": sum(a.get("classification") not in {"USEFUL", "CENSORED"}
                                 for a in records),
        "move_source_pages_freed": freed if move_records else "",
        "move_source_tokens_freed": freed * 512 if move_records else "",
        "move_zero_release_count": sum(a.get("move_release_class") == "ZERO" for a in move_records) if move_records else "",
        "move_partial_release_count": sum(a.get("move_release_class") == "PARTIAL" for a in move_records) if move_records else "",
        "move_full_release_count": sum(a.get("move_release_class") == "FULL" for a in move_records) if move_records else "",
    }


def add_deltas(rows: list[dict[str, Any]]) -> None:
    for copy, move in ((rows[0], rows[1]), (rows[2], rows[3])):
        for row in (copy, move):
            row["delta_mean_completion_move_vs_copy_pct"] = (
                (row["mean_completion_ms"] / copy["mean_completion_ms"] - 1) * 100)
            row["delta_p95_completion_move_vs_copy_pct"] = (
                (row["p95_completion_ms"] / copy["p95_completion_ms"] - 1) * 100)
            row["delta_token_hit_move_vs_copy_pp"] = (
                (row["token_hit_rate"] - copy["token_hit_rate"]) * 100)
            row["delta_saved_prefill_tokens_move_vs_copy"] = (
                row["saved_prefill_tokens"] - copy["saved_prefill_tokens"])
            row["delta_proactive_wire_bytes_move_vs_copy"] = (
                row["proactive_wire_bytes"] - copy["proactive_wire_bytes"])
            row["delta_evictions_move_vs_copy"] = row["evictions"] - copy["evictions"]
        completed = move["proactive_transfer_count"]
        move["source_pages_freed_per_move_completed"] = (
            move["move_source_pages_freed"] / completed if completed else 0.0)
        move["zero_release_fraction"] = move["move_zero_release_count"] / completed if completed else 0.0
        move["partial_release_fraction"] = move["move_partial_release_count"] / completed if completed else 0.0
        move["full_release_fraction"] = move["move_full_release_count"] / completed if completed else 0.0
        for field in ("source_pages_freed_per_move_completed", "zero_release_fraction",
                      "partial_release_fraction", "full_release_fraction"):
            copy[field] = ""


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize S06 COPY vs MOVE_SAFE")
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--result-root", type=Path, required=True)
    args = parser.parse_args()
    rows = []
    for workload, action, source, location in ROWS:
        directory = (args.repo_root / location if source == "taskmain_reused"
                     else args.result_root / location)
        rows.append(build_row(directory, workload, action, source))
    add_deltas(rows)
    output = args.result_root / "copy_vs_move_summary.csv"
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    (args.result_root / "copy_vs_move_summary.json").write_text(
        json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {output}")


if __name__ == "__main__":
    main()
