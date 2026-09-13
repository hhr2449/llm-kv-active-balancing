from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

EXPERIMENTS = (
    ("M01_conversation_r_req_kv_v1", "Conversation", "R_REQ_KV_V1"),
    ("M02_conversation_matched_o1", "Conversation", "Matched-O1"),
    ("M03_toolagent_r_req_kv_v1", "ToolAgent", "R_REQ_KV_V1"),
    ("M04_toolagent_matched_o1", "ToolAgent", "Matched-O1"),
)
EVALUATION_START_MS, EVALUATION_END_MS = 1_500_000, 2_700_000


def nested(data: dict[str, Any], *keys: str, default: Any = "") -> Any:
    value: Any = data
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return default
        value = value[key]
    return value


def evaluation_requests(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle]
    return [row for row in rows if row.get("split") == "EVALUATION"]


def proactive_scope(summary: dict[str, Any]) -> tuple[int | str, int | str, int | str, int | str]:
    proactive = summary.get("proactive")
    if not isinstance(proactive, dict):
        return 0, 0, "", ""
    actions = [a for a in proactive.get("action_records", [])
               if EVALUATION_START_MS <= a.get("trigger_time", -1) < EVALUATION_END_MS]
    page_bytes = nested(summary, "provenance", "p2p", "page_bytes", default=0)
    wire_bytes = sum(a.get("transferable_pages", 0) * page_bytes for a in actions)
    created = [a for a in actions if a.get("newly_resident_pages", 0) > 0]
    unused = sum(not a.get("actually_used", False) for a in created)
    wasted = sum(a.get("classification") not in {"USEFUL", "CENSORED"} for a in actions)
    return len(actions), wire_bytes, unused, wasted


def build_row(root: Path, experiment_id: str, workload: str, policy: str) -> dict[str, Any]:
    directory = root / experiment_id
    summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
    if summary.get("metric_scope") != "EVALUATION":
        raise ValueError(f"{experiment_id}: summary metric_scope is not EVALUATION")
    requests = evaluation_requests(directory / "requests.jsonl")
    saved, inputs = summary.get("h_used_tokens", 0), summary.get("input_tokens", 0)
    proactive_count, proactive_bytes, unused, wasted = proactive_scope(summary)
    reactive_count = nested(summary, "transfer", "transfer_completed", default=0)
    reactive_bytes = nested(summary, "transfer", "wire_bytes", default=0)
    tickets = nested(summary, "transfer", "ticket_created", default=0)
    return {
        "experiment_id": experiment_id.split("_", 1)[0], "workload": workload, "policy": policy,
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
        "proactive_transfer_count": proactive_count,
        "reactive_wire_bytes": reactive_bytes,
        "proactive_wire_bytes": proactive_bytes,
        "total_wire_bytes": reactive_bytes + proactive_bytes,
        "evictions": summary.get("eviction_count", ""),
        "fallbacks": tickets - reactive_count,
        "unused_replica_count": unused,
        "wasted_copy_count": wasted,
    }


def add_matched_deltas(rows: list[dict[str, Any]]) -> None:
    for baseline, proactive in ((rows[0], rows[1]), (rows[2], rows[3])):
        for row in (baseline, proactive):
            row["delta_mean_completion_pct"] = (
                (row["mean_completion_ms"] / baseline["mean_completion_ms"] - 1) * 100)
            row["delta_p95_completion_pct"] = (
                (row["p95_completion_ms"] / baseline["p95_completion_ms"] - 1) * 100)
            row["delta_token_hit_pp"] = (row["token_hit_rate"] - baseline["token_hit_rate"]) * 100
            row["delta_saved_prefill_tokens"] = row["saved_prefill_tokens"] - baseline["saved_prefill_tokens"]
            row["delta_total_wire_bytes"] = row["total_wire_bytes"] - baseline["total_wire_bytes"]
            row["delta_evictions"] = row["evictions"] - baseline["evictions"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize TaskMain-v1.1 matched comparison")
    parser.add_argument("--result-root", type=Path, required=True)
    args = parser.parse_args()
    rows = [build_row(args.result_root, *experiment) for experiment in EXPERIMENTS]
    add_matched_deltas(rows)
    output = args.result_root / "matched_main_summary.csv"
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    (args.result_root / "matched_main_summary.json").write_text(
        json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {output}")


if __name__ == "__main__":
    main()
