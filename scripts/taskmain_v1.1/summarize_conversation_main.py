from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


EXPERIMENTS = (
    ("E01_r_aff", "R_AFF"),
    ("E02_r_req_kv_task", "R_REQ_KV_TASK"),
    ("E03_oracle", "TaskMain-Oracle"),
    ("E04_persist_h1_k10", "Persist h=60s K=10"),
    ("E05_persist_h5_k10", "Persist h=300s K=10"),
    ("E06_recency_q09", "Recency q=0.9 decay=60s"),
    ("E07_cost_aware", "Cost-aware"),
)
EVALUATION_START_MS = 1_500_000
EVALUATION_END_MS = 2_700_000


def nested(data: dict[str, Any], *keys: str, default: Any = "") -> Any:
    value: Any = data
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return default
        value = value[key]
    return value


def evaluation_requests(path: Path) -> list[dict[str, Any]]:
    rows = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if row.get("split") == "EVALUATION":
                rows.append(row)
    return rows


def proactive_scope(summary: dict[str, Any]) -> tuple[int | str, int | str, int | str, int | str]:
    proactive = summary.get("proactive")
    if not isinstance(proactive, dict):
        return 0, 0, "", ""
    actions = [
        action for action in proactive.get("action_records", [])
        if EVALUATION_START_MS <= action.get("trigger_time", -1) < EVALUATION_END_MS
    ]
    wire_bytes = sum(
        action.get("transferable_pages", 0)
        * nested(summary, "provenance", "p2p", "page_bytes", default=0)
        for action in actions
    )
    created = [action for action in actions if action.get("newly_resident_pages", 0) > 0]
    unused = sum(not action.get("actually_used", False) for action in created)
    wasted = sum(
        action.get("classification") not in {"USEFUL", "CENSORED"}
        for action in actions
    )
    return len(actions), wire_bytes, unused, wasted


def build_row(result_root: Path, experiment_id: str, policy: str) -> dict[str, Any]:
    directory = result_root / experiment_id
    summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
    if summary.get("metric_scope") != "EVALUATION":
        raise ValueError(f"{experiment_id}: summary metric_scope is not EVALUATION")
    requests = evaluation_requests(directory / "requests.jsonl")
    request_hit_rate = (
        sum(row["h_used_tokens"] > 0 for row in requests) / len(requests)
        if requests else 0.0
    )
    input_tokens = summary.get("input_tokens", 0)
    saved_tokens = summary.get("h_used_tokens", 0)
    proactive_count, proactive_bytes, unused, wasted = proactive_scope(summary)
    reactive_count = nested(summary, "transfer", "transfer_completed", default=0)
    reactive_bytes = nested(summary, "transfer", "wire_bytes", default=0)
    tickets = nested(summary, "transfer", "ticket_created", default=0)
    row = {
        "experiment_id": experiment_id,
        "policy": policy,
        "mean_completion_ms": nested(summary, "completion_latency_ms", "mean"),
        "p50_completion_ms": nested(summary, "completion_latency_ms", "p50"),
        "p95_completion_ms": nested(summary, "completion_latency_ms", "p95"),
        "mean_queue_ms": nested(summary, "queue_time_ms", "mean"),
        "mean_service_ms": nested(summary, "service_time_ms", "mean"),
        "request_hit_rate": request_hit_rate,
        "token_hit_rate": saved_tokens / input_tokens if input_tokens else 0.0,
        "saved_prefill_tokens": saved_tokens,
        "gini_actual": nested(summary, "cluster", "executed_miss_token_gini"),
        "gini_random_mean": "",
        "gini_ratio": "",
        "reactive_transfer_count": reactive_count,
        "proactive_transfer_count": proactive_count,
        "reactive_wire_bytes": reactive_bytes,
        "proactive_wire_bytes": proactive_bytes,
        "total_wire_bytes": reactive_bytes + proactive_bytes,
        "evictions": summary.get("eviction_count", ""),
        "fallbacks": tickets - reactive_count,
        "unused_replica_count": unused,
        "wasted_copy_count": wasted,
    }
    return row


def add_deltas(rows: list[dict[str, Any]]) -> None:
    aff, task = rows[0], rows[1]
    for row in rows:
        completion = row["mean_completion_ms"]
        row["delta_completion_vs_r_aff_pct"] = (
            (completion / aff["mean_completion_ms"] - 1) * 100
            if aff["mean_completion_ms"] else ""
        )
        row["delta_completion_vs_r_req_kv_task_pct"] = (
            (completion / task["mean_completion_ms"] - 1) * 100
            if task["mean_completion_ms"] else ""
        )
        row["delta_token_hit_vs_r_aff_pp"] = (
            (row["token_hit_rate"] - aff["token_hit_rate"]) * 100
        )
        row["delta_token_hit_vs_r_req_kv_task_pp"] = (
            (row["token_hit_rate"] - task["token_hit_rate"]) * 100
        )
        row["delta_gini_vs_r_aff"] = row["gini_actual"] - aff["gini_actual"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize TaskMain-v1.1 Conversation rows")
    parser.add_argument("--result-root", type=Path, required=True)
    args = parser.parse_args()
    rows = [build_row(args.result_root, experiment_id, policy)
            for experiment_id, policy in EXPERIMENTS]
    add_deltas(rows)
    csv_path = args.result_root / "conversation_main_summary.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    (args.result_root / "conversation_main_summary.json").write_text(
        json.dumps(rows, indent=2) + "\n", encoding="utf-8"
    )
    print(f"wrote {csv_path}")


if __name__ == "__main__":
    main()
