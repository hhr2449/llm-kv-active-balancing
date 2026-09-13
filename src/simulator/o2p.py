from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import asdict, replace
from pathlib import Path
from statistics import fmean, median
from typing import Any

from .config import SimulatorConfig
from .engine import SimulatorEngine
from .metrics import RequestResult
from .trace import load_trace

EVAL_START_MS, EVAL_END_MS = 1_500_000.0, 2_700_000.0
HORIZON_MS = 300_000.0


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    pos = (len(ordered) - 1) * q
    lo, hi = math.floor(pos), math.ceil(pos)
    return ordered[lo] if lo == hi else ordered[lo] * (hi - pos) + ordered[hi] * (pos - lo)


def evenly_spaced(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    rows = sorted(rows, key=lambda r: (r["time_ms"], r["prefix_id"], r["source_pod"]))
    if len(rows) <= limit:
        return rows
    indices = [round(i * (len(rows) - 1) / (limit - 1)) for i in range(limit)]
    return [rows[index] for index in indices]


def sample_states(rows: list[dict[str, Any]], max_total: int | None = None) -> list[dict[str, Any]]:
    selected = []
    for bin_id in range(10):
        lo = EVAL_START_MS + bin_id * 120_000
        hi = lo + 120_000
        members = [r for r in rows if lo <= r["time_ms"] < hi
                   and len(r["legal_targets"]) >= 2]
        for row in evenly_spaced(members, 10):
            item = dict(row)
            item["bin_id"] = bin_id
            selected.append(item)
    return selected if max_total is None else selected[:max_total]


def cohort(results: list[RequestResult], time_ms: float) -> list[RequestResult]:
    cutoff = time_ms + HORIZON_MS
    return [r for r in results if
            (r.arrival_ms <= time_ms < r.service_done_ms)
            or (time_ms < r.arrival_ms <= cutoff)]


def branch_metrics(results: list[RequestResult], summary: dict, time_ms: float,
                   target: int | None, page_bytes: int) -> dict[str, Any]:
    members = cohort(results, time_ms)
    losses = [(r.service_done_ms - time_ms if r.arrival_ms <= time_ms
               else r.completion_latency_ms) for r in members]
    inputs = sum(r.input_tokens for r in members)
    saved = sum(r.h_used_tokens for r in members)
    proactive = summary.get("proactive", {})
    action_records = proactive.get("action_records", [])
    per_pod_work = [sum(r.miss_tokens for r in members if r.pod_id == pod)
                    for pod in range(summary["num_pods"])]
    per_pod_requests = [sum(r.pod_id == pod for r in members)
                        for pod in range(summary["num_pods"])]
    return {
        "target_pod": "NO_COPY" if target is None else target,
        "cohort_request_count": len(members),
        "loss_total": sum(losses),
        "mean_residual_latency": fmean(losses) if losses else 0.0,
        "mean_queue": fmean(r.queue_time_ms for r in members) if members else 0.0,
        "mean_service": fmean(r.service_time_ms for r in members) if members else 0.0,
        "token_hit_rate": saved / inputs if inputs else 0.0,
        "saved_prefill_tokens": saved,
        "reactive_transfer_count": sum(r.p2p_assisted for r in members),
        "reactive_wire_bytes": sum(r.wire_bytes for r in members),
        "proactive_transfer_count": len(action_records),
        "proactive_wire_bytes": sum(a["transferable_pages"] * page_bytes
                                    for a in action_records),
        "evictions": summary["eviction_count"],
        "per_pod_service_work_tokens": json.dumps(per_pod_work),
        "per_pod_request_count": json.dumps(per_pod_requests),
        "load_gap_tokens": max(per_pod_work, default=0) - min(per_pod_work, default=0),
        "state_fingerprint": proactive.get("o2p_state_fingerprint"),
        "action_success": (target is None or proactive.get("o2p_forced_action_success", False)),
        "action_failure_reason": proactive.get("o2p_forced_action_failure_reason"),
    }


def select_oracle(branches: list[dict[str, Any]], o1_target: int) -> tuple[dict, dict, str | int]:
    copies = [branch for branch in branches if branch["target_pod"] != "NO_COPY"
              and branch["action_success"]]
    if not copies:
        raise ValueError("sampled state has no successful legal COPY branch")
    best_copy = max(copies, key=lambda branch: (
        branch["delta_loss_total"], -int(branch["target_pod"])))
    try:
        o1 = next(branch for branch in copies if int(branch["target_pod"]) == o1_target)
    except StopIteration as exc:
        raise ValueError("O1 target is absent from successful legal branches") from exc
    oracle_action = best_copy["target_pod"] if best_copy["delta_loss_total"] > 0 else "NO_COPY"
    return best_copy, o1, oracle_action


def run_branch(base: SimulatorConfig, requests, state: dict[str, Any],
               target: int | None) -> tuple[list[RequestResult], dict, dict[str, Any]]:
    action = "O2P_NO_COPY" if target is None else "O2P_FORCED_COPY"
    config = replace(
        base, proactive_enabled=True, proactive_strategy="FUTURE_DEMAND",
        proactive_action=action, o2p_forced_time_ms=state["time_ms"],
        o2p_forced_prefix_id=(None if target is None else state["prefix_id"]),
        o2p_forced_source_pod=(None if target is None else state["source_pod"]),
        o2p_forced_target_pod=target,
        o2p_arrival_cutoff_ms=state["time_ms"] + HORIZON_MS,
    )
    results, summary = SimulatorEngine(config).run(requests)
    action_count = len(summary.get("proactive", {}).get("action_records", []))
    if target is None and action_count != 0:
        raise AssertionError("NO_COPY branch injected a proactive action")
    if target is not None and action_count > 1:
        raise AssertionError("COPY branch injected more than one proactive action")
    metrics = branch_metrics(results, summary, state["time_ms"], target, base.page_bytes)
    return results, summary, metrics


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("\n", encoding="utf-8")
        return
    fieldnames = list(rows[0])
    seen = set(fieldnames)
    for row in rows[1:]:
        for key in row:
            if key not in seen:
                seen.add(key); fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_workload(workload: str, trace_path: Path, config_path: Path,
                 max_states: int | None = None):
    base = SimulatorConfig.from_yaml(config_path)
    requests = load_trace(trace_path, base.page_tokens, base.split_config)
    probe_config = replace(
        base, proactive_enabled=True, proactive_strategy="FUTURE_DEMAND",
        proactive_action="O2P_PROBE", trigger_phase_ms=EVAL_START_MS,
        proactive_trigger_latest_ms=EVAL_END_MS - base.trigger_period_ms,
    )
    _, probe_summary = SimulatorEngine(probe_config).run(requests)
    candidates = [r for r in probe_summary["proactive"]["o2p_candidate_states"]
                  if EVAL_START_MS <= r["time_ms"] < EVAL_END_MS]
    sampled = sample_states(candidates, max_states)
    selected_keys = {(r["time_ms"], r["prefix_id"], r["source_pod"]) for r in sampled}
    candidate_rows = []
    for index, row in enumerate(candidates):
        candidate_rows.append({
            "workload": workload, "candidate_id": index, **row,
            "legal_targets": json.dumps(row["legal_targets"]),
            "legal_target_count": len(row["legal_targets"]),
            "eligible_multi_target": len(row["legal_targets"]) >= 2,
            "sampled": (row["time_ms"], row["prefix_id"], row["source_pod"]) in selected_keys,
        })
    branch_rows, state_rows = [], []
    for state_number, state in enumerate(sampled):
        state_id = f"{workload[0]}-{state_number:03d}"
        branches = []
        cohort_ids: list[list[int]] = []
        for target in [None, *state["legal_targets"]]:
            branch_results, _, metrics = run_branch(base, requests, state, target)
            if metrics["state_fingerprint"] != state["state_fingerprint"]:
                raise AssertionError(f"{state_id}: reconstructed branch state mismatch")
            cohort_ids.append([r.request_id for r in cohort(branch_results, state["time_ms"])])
            branches.append(metrics)
        if any(ids != cohort_ids[0] for ids in cohort_ids[1:]):
            raise AssertionError(f"{state_id}: branch future/cohort request sequences differ")
        no_copy = branches[0]
        for branch in branches:
            branch.update({
                "state_id": state_id, "workload": workload,
                "time_ms": state["time_ms"], "prefix_id": state["prefix_id"],
                "source_pod": state["source_pod"],
                "copy_bytes": (0 if branch["target_pod"] == "NO_COPY"
                               else state["transferable_pages"] * base.page_bytes),
            })
            branch["delta_loss_total"] = no_copy["loss_total"] - branch["loss_total"]
            branch["delta_mean_latency_ms"] = (
                branch["delta_loss_total"] / branch["cohort_request_count"])
            branch["delta_mean_latency_pct"] = (
                branch["delta_mean_latency_ms"] / no_copy["mean_residual_latency"] * 100
                if no_copy["mean_residual_latency"] else 0.0)
            branch_rows.append(branch)
        best_copy, o1, oracle_action = select_oracle(branches, state["o1_target"])
        best_positive = best_copy["delta_loss_total"] > 0
        state_rows.append({
            "state_id": state_id, "workload": workload, "time_ms": state["time_ms"],
            "prefix_id": state["prefix_id"], "prefix_depth_pages": state["prefix_depth_pages"],
            "transferable_pages": state["transferable_pages"],
            "prefix_valid_tokens": state["prefix_valid_tokens"], "source_pod": state["source_pod"],
            "o1_target": state["o1_target"], "legal_target_count": len(state["legal_targets"]),
            "cohort_request_count": no_copy["cohort_request_count"],
            "no_copy_loss_total": no_copy["loss_total"],
            "no_copy_mean_residual_latency": no_copy["mean_residual_latency"],
            "no_copy_mean_queue": no_copy["mean_queue"], "no_copy_mean_service": no_copy["mean_service"],
            "oracle_action": oracle_action, "best_target": best_copy["target_pod"],
            "best_copy_is_positive": best_positive,
            "best_delta_loss_total": best_copy["delta_loss_total"],
            "best_delta_mean_latency_ms": best_copy["delta_mean_latency_ms"],
            "best_delta_mean_latency_pct": best_copy["delta_mean_latency_pct"],
            "o1_target_delta_mean_latency_ms": o1["delta_mean_latency_ms"],
            "o1_target_delta_mean_latency_pct": o1["delta_mean_latency_pct"],
            "o1_target_is_best": abs(o1["delta_loss_total"] - best_copy["delta_loss_total"]) < 1e-9,
            "o1_target_is_positive": o1["delta_loss_total"] > 0,
            "placement_regret_ms": best_copy["delta_mean_latency_ms"] - o1["delta_mean_latency_ms"],
        })
    workload_summary = aggregate_workload(workload, candidates, sampled, state_rows)
    return candidate_rows, branch_rows, state_rows, workload_summary


def aggregate_workload(workload, candidates, sampled, states):
    best = [r["best_delta_mean_latency_ms"] for r in states]
    best_pct = [r["best_delta_mean_latency_pct"] for r in states]
    o1 = [r["o1_target_delta_mean_latency_ms"] for r in states]
    regret = [r["placement_regret_ms"] for r in states]
    count = len(states)
    return {
        "workload": workload, "total_candidate_states": len(candidates),
        "eligible_multi_target_states": sum(len(r["legal_targets"]) >= 2 for r in candidates),
        "sampled_states": count,
        "states_with_any_positive_copy": sum(r["best_copy_is_positive"] for r in states),
        "positive_copy_fraction": sum(r["best_copy_is_positive"] for r in states) / count if count else 0,
        "states_where_o1_target_positive": sum(r["o1_target_is_positive"] for r in states),
        "o1_positive_fraction": sum(r["o1_target_is_positive"] for r in states) / count if count else 0,
        "states_where_o1_target_is_best": sum(r["o1_target_is_best"] for r in states),
        "o1_target_best_fraction": sum(r["o1_target_is_best"] for r in states) / count if count else 0,
        "states_where_best_action_is_no_copy": sum(not r["best_copy_is_positive"] for r in states),
        "no_copy_best_fraction": sum(not r["best_copy_is_positive"] for r in states) / count if count else 0,
        "mean_best_delta_mean_latency_ms": fmean(best) if best else None,
        "median_best_delta_mean_latency_ms": median(best) if best else None,
        "p90_best_delta_mean_latency_ms": percentile(best, .9),
        "mean_best_delta_mean_latency_pct": fmean(best_pct) if best_pct else None,
        "median_best_delta_mean_latency_pct": median(best_pct) if best_pct else None,
        "mean_o1_delta_mean_latency_ms": fmean(o1) if o1 else None,
        "median_o1_delta_mean_latency_ms": median(o1) if o1 else None,
        "mean_placement_regret_ms": fmean(regret) if regret else None,
        "median_placement_regret_ms": median(regret) if regret else None,
        "p90_placement_regret_ms": percentile(regret, .9),
        "mean_legal_target_count": fmean(r["legal_target_count"] for r in states) if states else 0,
    }


def report(rows):
    lines = ["# O2-P Counterfactual Placement / Action-Value Pilot", "",
             "O2-P uses overlapping common states. Per-state DeltaL values must not be summed as a full 20-minute replay benefit. This is a counterfactual action-value diagnostic, not closed-loop Oracle policy performance.", "",
             "| Workload | Candidate states | Multi-target eligible | Sampled | Positive copy | NO_COPY best | O1 positive | O1 best | Mean regret (ms) |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for r in rows:
        lines.append(f"| {r['workload']} | {r['total_candidate_states']} | {r['eligible_multi_target_states']} | {r['sampled_states']} | {r['positive_copy_fraction']:.3%} | {r['no_copy_best_fraction']:.3%} | {r['o1_positive_fraction']:.3%} | {r['o1_target_best_fraction']:.3%} | {r['mean_placement_regret_ms']:.6f} |")
    lines += ["", "Conversation/ToolAgent differences and placement headroom are represented mechanically by the positive-copy, O1-best, and placement-regret columns; no policy conclusion is inferred by this pilot."]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="O2-P counterfactual placement pilot")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-states-per-bin", type=int, default=10)
    parser.add_argument("--smoke-max-states-per-workload", type=int)
    args = parser.parse_args()
    if args.max_states_per_bin != 10:
        raise ValueError("formal pilot fixes max_states_per_bin=10")
    specs = (
        ("Conversation", Path("data/mooncake/conversation_trace.jsonl"), Path("configs/taskmain_v1.1/matched/M01_conversation_r_req_kv_v1.yaml")),
        ("ToolAgent", Path("data/mooncake/toolagent_trace.jsonl"), Path("configs/taskmain_v1.1/matched/M03_toolagent_r_req_kv_v1.yaml")),
    )
    all_candidates, all_branches, all_states, summaries = [], [], [], []
    for spec in specs:
        candidates, branches, states, summary = run_workload(
            *spec, max_states=args.smoke_max_states_per_workload)
        all_candidates += candidates; all_branches += branches; all_states += states
        summaries.append(summary)
    args.output.mkdir(parents=True, exist_ok=True)
    write_csv(args.output / "o2p_state_candidates.csv", all_candidates)
    write_csv(args.output / "o2p_branch_results.csv", all_branches)
    write_csv(args.output / "o2p_state_summary.csv", all_states)
    write_csv(args.output / "o2p_workload_summary.csv", summaries)
    (args.output / "o2p_pilot_report.md").write_text(report(summaries), encoding="utf-8")


if __name__ == "__main__":
    main()
