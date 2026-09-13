from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from statistics import fmean
from typing import Any

EVALUATION_START_MS, EVALUATION_END_MS = 1_500_000, 2_700_000
ROWS = (
    ("Conversation", 60, 5, "s02_new_run", "S02-C-H1-K5"),
    ("Conversation", 60, 10, "taskmain_reused", "results/taskmain_evaluation/conversation/E04_persist_h1_k10"),
    ("Conversation", 60, 20, "s02_new_run", "S02-C-H1-K20"),
    ("Conversation", 300, 5, "s02_new_run", "S02-C-H5-K5"),
    ("Conversation", 300, 10, "taskmain_reused", "results/taskmain_evaluation/conversation/E05_persist_h5_k10"),
    ("Conversation", 300, 20, "s02_new_run", "S02-C-H5-K20"),
    ("ToolAgent", 60, 5, "s02_new_run", "S02-T-H1-K5"),
    ("ToolAgent", 60, 10, "taskmain_reused", "results/taskmain_evaluation/toolagent/E11_persist_h1_k10"),
    ("ToolAgent", 60, 20, "s02_new_run", "S02-T-H1-K20"),
    ("ToolAgent", 300, 5, "s02_new_run", "S02-T-H5-K5"),
    ("ToolAgent", 300, 10, "taskmain_reused", "results/taskmain_evaluation/toolagent/E12_persist_h5_k10"),
    ("ToolAgent", 300, 20, "s02_new_run", "S02-T-H5-K20"),
)


def nested(data: dict[str, Any], *keys: str, default: Any = "") -> Any:
    value: Any = data
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return default
        value = value[key]
    return value


def load_eval_requests(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        return [row for row in map(json.loads, handle) if row.get("split") == "EVALUATION"]


def build_row(directory: Path, workload: str, history_s: int, top_k: int,
              source: str) -> dict[str, Any]:
    summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
    if summary.get("metric_scope") != "EVALUATION":
        raise ValueError(f"{directory}: metric scope is not EVALUATION")
    requests = load_eval_requests(directory / "requests.jsonl")
    actions = [a for a in nested(summary, "proactive", "action_records", default=[])
               if EVALUATION_START_MS <= a.get("trigger_time", -1) < EVALUATION_END_MS]
    page_bytes = nested(summary, "provenance", "p2p", "page_bytes", default=0)
    proactive_wire = sum(a.get("transferable_pages", 0) * page_bytes for a in actions)
    created = [a for a in actions if a.get("newly_resident_pages", 0) > 0]
    proactive = summary.get("proactive", {})
    ranks = proactive.get("selected_candidate_ranks", [])
    saved, inputs = summary.get("h_used_tokens", 0), summary.get("input_tokens", 0)
    return {
        "workload": workload, "h_seconds": history_s, "K": top_k,
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
        "selected_candidate_rank_mean": fmean(ranks) if ranks else "",
        "rank_gt_1_count": proactive.get("rank_gt_1_count", ""),
        "top_k_exhausted_count": proactive.get("top_k_exhausted_count", ""),
        "budget_insufficient_count": proactive.get("budget_insufficient_current_tokens", ""),
    }


def metric_delta(row: dict[str, Any], baseline: dict[str, Any], suffix: str) -> None:
    row[f"delta_mean_completion_{suffix}_pct"] = (
        (row["mean_completion_ms"] / baseline["mean_completion_ms"] - 1) * 100)
    row[f"delta_token_hit_{suffix}_pp"] = (
        (row["token_hit_rate"] - baseline["token_hit_rate"]) * 100)
    row[f"delta_saved_prefill_tokens_{suffix}"] = (
        row["saved_prefill_tokens"] - baseline["saved_prefill_tokens"])
    row[f"delta_proactive_wire_bytes_{suffix}"] = (
        row["proactive_wire_bytes"] - baseline["proactive_wire_bytes"])


def add_deltas(rows: list[dict[str, Any]]) -> None:
    index = {(r["workload"], r["h_seconds"], r["K"]): r for r in rows}
    for row in rows:
        metric_delta(row, index[(row["workload"], row["h_seconds"], 10)], "vs_k10")
        metric_delta(row, index[(row["workload"], 60, row["K"])], "vs_h60")


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize S02 Persistence h/K sweep")
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--result-root", type=Path, required=True)
    args = parser.parse_args()
    rows = []
    for workload, history_s, top_k, source, location in ROWS:
        directory = ((args.repo_root / location) if source == "taskmain_reused"
                     else (args.result_root / location))
        rows.append(build_row(directory, workload, history_s, top_k, source))
    add_deltas(rows)
    output = args.result_root / "persistence_h_k_summary.csv"
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    (args.result_root / "persistence_h_k_summary.json").write_text(
        json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {output}")


if __name__ == "__main__":
    main()
