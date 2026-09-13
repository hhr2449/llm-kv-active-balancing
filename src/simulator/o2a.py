from __future__ import annotations

import argparse
import csv
import json
import math
import time
from dataclasses import replace
from pathlib import Path
from statistics import fmean, median
from typing import Any

from .config import SimulatorConfig
from .engine import SimulatorEngine
from .o2p import (EVAL_END_MS, EVAL_START_MS, cohort, percentile, run_branch,
                  write_csv)
from .trace import load_trace


TOP_M = 5
EPSILON = 1e-9


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def average_ranks(values: list[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda index: values[index])
    ranks = [0.0] * len(values)
    start = 0
    while start < len(order):
        end = start + 1
        while end < len(order) and values[order[end]] == values[order[start]]:
            end += 1
        rank = (start + 1 + end) / 2
        for position in range(start, end):
            ranks[order[position]] = rank
        start = end
    return ranks


def spearman(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 2 or len(xs) != len(ys):
        return None
    rx, ry = average_ranks(xs), average_ranks(ys)
    mx, my = fmean(rx), fmean(ry)
    numerator = sum((x - mx) * (y - my) for x, y in zip(rx, ry))
    denominator = math.sqrt(sum((x - mx) ** 2 for x in rx)
                            * sum((y - my) ** 2 for y in ry))
    return numerator / denominator if denominator else None


def choose_action(copy_branches: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, str]:
    successful = [row for row in copy_branches if row["action_success"]]
    if not successful:
        return None, "NO_COPY"
    best = min(successful, key=lambda row: (
        -row["delta_loss_total"], row["copy_bytes"],
        -row["prefix_depth_pages"], row["prefix_id"], row["target_pod"],
    ))
    return (best, "COPY") if best["delta_loss_total"] > 0 else (best, "NO_COPY")


def classify(no_copy: bool, exact_o1: bool, strictly_better: bool,
             best_prefix_is_o1: bool) -> str:
    if no_copy:
        return "TYPE_0_NO_OPPORTUNITY"
    if exact_o1:
        return "TYPE_3_O1_ALREADY_GOOD"
    if strictly_better and not best_prefix_is_o1:
        return "TYPE_2_PREFIX_SELECTION"
    return "TYPE_1_PLACEMENT_ONLY"


def probe_candidates(base: SimulatorConfig, requests) -> dict[float, dict[str, Any]]:
    config = replace(
        base, proactive_enabled=True, proactive_strategy="FUTURE_DEMAND",
        proactive_action="O2A_PROBE", trigger_phase_ms=EVAL_START_MS,
        proactive_trigger_latest_ms=EVAL_END_MS - base.trigger_period_ms,
    )
    _, summary = SimulatorEngine(config).run(requests)
    return {float(row["time_ms"]): row
            for row in summary["proactive"]["o2a_candidate_states"]}


def run_workload(workload: str, trace_path: Path, config_path: Path,
                 o2p_states: list[dict[str, str]],
                 o2p_branches: list[dict[str, str]]):
    base = SimulatorConfig.from_yaml(config_path)
    requests = load_trace(trace_path, base.page_tokens, base.split_config)
    print(f"[O2-A] {workload}: probing baseline Top-{TOP_M} candidates...", flush=True)
    probes = probe_candidates(base, requests)
    prior_branches = {}
    for row in o2p_branches:
        prior_branches.setdefault(row["state_id"], []).append(row)

    candidate_rows: list[dict[str, Any]] = []
    branch_rows: list[dict[str, Any]] = []
    state_rows: list[dict[str, Any]] = []
    prefix_values_for_correlation: list[tuple[float, float]] = []
    workload_started = time.monotonic()
    completed_branches = 0
    total_states = len(o2p_states)

    for state_index, prior in enumerate(
            sorted(o2p_states, key=lambda row: row["state_id"]), start=1):
        state_id = prior["state_id"]
        time_ms = float(prior["time_ms"])
        probe = probes.get(time_ms)
        if probe is None:
            raise AssertionError(f"{state_id}: O2-A baseline probe state missing")
        old_group = prior_branches[state_id]
        old_no_copy = next(row for row in old_group if row["target_pod"] == "NO_COPY")
        old_fingerprint = old_no_copy["state_fingerprint"]
        if probe["state_fingerprint"] != old_fingerprint:
            raise AssertionError(f"{state_id}: O2-A/O2-P state fingerprint mismatch")
        candidates = probe["candidates"]
        o1_prefix = int(prior["prefix_id"])
        if not candidates or int(candidates[0]["prefix_id"]) != o1_prefix:
            raise AssertionError(f"{state_id}: Top-M candidate set does not preserve O1 prefix")

        no_state = {"time_ms": time_ms}
        no_results, _, no_metrics = run_branch(base, requests, no_state, None)
        state_branch_count = 1
        if abs(no_metrics["loss_total"] - float(old_no_copy["loss_total"])) > EPSILON:
            raise AssertionError(f"{state_id}: NO_COPY does not reproduce O2-P")
        common_ids = [r.request_id for r in cohort(no_results, time_ms)]
        branch_rows.append({
            "state_id": state_id, "workload": workload, "time_ms": time_ms,
            "branch_type": "NO_COPY", "prefix_id": "", "prefix_rank": "",
            "prefix_depth_pages": "",
            "source_pod": "", "target_pod": "NO_COPY", "copy_bytes": 0,
            "delta_loss_total": 0.0, "delta_mean_latency_ms": 0.0,
            "delta_mean_latency_pct": 0.0,
            **no_metrics,
        })

        copies: list[dict[str, Any]] = []
        for candidate in candidates:
            prefix_id = int(candidate["prefix_id"])
            targets = [int(value) for value in candidate["legal_targets"]]
            candidate_rows.append({
                "state_id": state_id, "workload": workload, "time_ms": time_ms,
                "prefix_id": prefix_id, "o1_rank": candidate["o1_rank"],
                "prefix_depth_pages": candidate["prefix_depth_pages"],
                "transferable_pages": candidate["transferable_pages"],
                "prefix_valid_tokens": candidate["prefix_valid_tokens"],
                "source_pod": candidate["source_pod"],
                "future_demand_score": candidate["future_demand_score"],
                "total_eligible_prefix_count": probe["total_eligible_prefix_count"],
                "candidate_prefix_count_used": len(candidates),
                "legal_target_count": len(targets),
                "legal_targets": json.dumps(targets),
                "no_legal_target_reason": "" if targets else "no_feasible_target",
                "state_fingerprint": probe["state_fingerprint"],
            })
            prefix_branches = []
            forced_state = {
                "time_ms": time_ms, "prefix_id": prefix_id,
                "source_pod": int(candidate["source_pod"]),
            }
            for target in targets:
                results, _, metrics = run_branch(base, requests, forced_state, target)
                if metrics["state_fingerprint"] != old_fingerprint:
                    raise AssertionError(f"{state_id}: COPY branch state mismatch")
                if [r.request_id for r in cohort(results, time_ms)] != common_ids:
                    raise AssertionError(f"{state_id}: future cohort mismatch")
                if not metrics["action_success"] or metrics["proactive_transfer_count"] != 1:
                    raise AssertionError(f"{state_id}: legal COPY branch failed")
                row = {
                    "state_id": state_id, "workload": workload, "time_ms": time_ms,
                    "branch_type": "COPY", "prefix_id": prefix_id,
                    "prefix_rank": int(candidate["o1_rank"]),
                    "prefix_depth_pages": int(candidate["prefix_depth_pages"]),
                    "source_pod": int(candidate["source_pod"]), "target_pod": target,
                    "copy_bytes": int(candidate["transferable_pages"]) * base.page_bytes,
                    **metrics,
                }
                row["delta_loss_total"] = no_metrics["loss_total"] - row["loss_total"]
                row["delta_mean_latency_ms"] = row["delta_loss_total"] / row["cohort_request_count"]
                row["delta_mean_latency_pct"] = (
                    row["delta_mean_latency_ms"] / no_metrics["mean_residual_latency"] * 100
                    if no_metrics["mean_residual_latency"] else 0.0)
                copies.append(row); prefix_branches.append(row); branch_rows.append(row)
                state_branch_count += 1
            if prefix_branches:
                best_prefix_branch = min(prefix_branches, key=lambda row: (
                    -row["delta_loss_total"], row["copy_bytes"], row["target_pod"]))
                prefix_values_for_correlation.append((
                    float(candidate["future_demand_score"]),
                    best_prefix_branch["delta_mean_latency_ms"],
                ))

        o1_copies = [row for row in copies if row["prefix_id"] == o1_prefix]
        old_best_copy = float(prior["best_delta_mean_latency_ms"])
        reproduced_o1 = max(row["delta_mean_latency_ms"] for row in o1_copies)
        if abs(reproduced_o1 - old_best_copy) > EPSILON:
            raise AssertionError(f"{state_id}: O1 Prefix branches do not reproduce O2-P")

        best_copy, oracle_action = choose_action(copies)
        best_value = max(0.0, best_copy["delta_mean_latency_ms"] if best_copy else 0.0)
        o2p_action = prior["oracle_action"]
        o2p_value = max(0.0, old_best_copy)
        additional = best_value - o2p_value
        best_prefix = None if oracle_action == "NO_COPY" else best_copy["prefix_id"]
        best_target = None if oracle_action == "NO_COPY" else best_copy["target_pod"]
        best_rank = None if oracle_action == "NO_COPY" else best_copy["prefix_rank"]
        exact_o1 = bool(
            oracle_action == "COPY" and best_prefix == o1_prefix
            and best_target == int(prior["o1_target"])
        )
        best_prefix_is_o1 = oracle_action == "COPY" and best_prefix == o1_prefix
        strict = additional > EPSILON
        practical = additional >= 1.0
        state_rows.append({
            "state_id": state_id, "workload": workload, "time_ms": time_ms,
            "state_fingerprint": old_fingerprint,
            "o1_prefix_id": o1_prefix,
            "o1_prefix_depth_pages": int(prior["prefix_depth_pages"]),
            "o1_target": int(prior["o1_target"]),
            "total_eligible_prefix_count": probe["total_eligible_prefix_count"],
            "candidate_prefix_count": len(candidates),
            "total_action_branch_count": 1 + len(copies),
            "o2p_best_action": o2p_action,
            "o2p_best_target": prior["best_target"],
            "o2p_best_copy_delta_mean_latency_ms": old_best_copy,
            "o2p_best_delta_mean_latency_ms": o2p_value,
            "o2p_o1_target_delta_mean_latency_ms": float(prior["o1_target_delta_mean_latency_ms"]),
            "o2a_best_action": oracle_action,
            "o2a_best_prefix": "" if best_prefix is None else best_prefix,
            "o2a_best_target": "" if best_target is None else best_target,
            "o2a_best_prefix_rank_in_o1": "" if best_rank is None else best_rank,
            "o2a_best_delta_loss_total": 0.0 if oracle_action == "NO_COPY" else best_copy["delta_loss_total"],
            "o2a_best_delta_mean_latency_ms": best_value,
            "o2a_best_delta_mean_latency_pct": 0.0 if oracle_action == "NO_COPY" else best_copy["delta_mean_latency_pct"],
            "o2a_additional_headroom_ms": additional,
            "prefix_selection_regret_ms": additional,
            "o2a_strictly_better_than_o2p": strict,
            "o2a_ge_1ms_better_than_o2p": practical,
            "o2a_best_prefix_is_o1_prefix": best_prefix_is_o1,
            "o2a_best_action_is_exact_o1_action": exact_o1,
            "rescued_from_o2p_no_copy": o2p_action == "NO_COPY" and oracle_action == "COPY",
            "classification_strict": classify(oracle_action == "NO_COPY", exact_o1,
                                               strict, best_prefix_is_o1),
            "classification_practical": classify(oracle_action == "NO_COPY", exact_o1,
                                                  practical, best_prefix_is_o1),
            "best_wire_bytes": 0 if oracle_action == "NO_COPY" else best_copy["copy_bytes"],
            "best_saved_prefill_tokens": no_metrics["saved_prefill_tokens"] if oracle_action == "NO_COPY" else best_copy["saved_prefill_tokens"],
            "best_token_hit_rate": no_metrics["token_hit_rate"] if oracle_action == "NO_COPY" else best_copy["token_hit_rate"],
            "best_reactive_transfer_count": no_metrics["reactive_transfer_count"] if oracle_action == "NO_COPY" else best_copy["reactive_transfer_count"],
            "best_reactive_wire_bytes": no_metrics["reactive_wire_bytes"] if oracle_action == "NO_COPY" else best_copy["reactive_wire_bytes"],
            "best_evictions": no_metrics["evictions"] if oracle_action == "NO_COPY" else best_copy["evictions"],
        })
        completed_branches += state_branch_count
        elapsed = time.monotonic() - workload_started
        mean_state_seconds = elapsed / state_index
        eta_seconds = mean_state_seconds * (total_states - state_index)
        print(
            f"[O2-A] {workload}: state {state_index}/{total_states} "
            f"({state_id}), state_branches={state_branch_count}, "
            f"completed_branches={completed_branches}, elapsed={elapsed / 60:.1f}m, "
            f"workload_eta={eta_seconds / 60:.1f}m",
            flush=True,
        )
    summary = aggregate(workload, state_rows, prefix_values_for_correlation)
    return candidate_rows, branch_rows, state_rows, summary


def aggregate(workload: str, states: list[dict[str, Any]], correlations):
    count = len(states)
    positive = [row for row in states if row["o2a_best_action"] == "COPY"]
    o2p_positive = [row for row in states if row["o2p_best_action"] != "NO_COPY"]
    o2p_no_copy = [row for row in states if row["o2p_best_action"] == "NO_COPY"]
    ranks = [int(row["o2a_best_prefix_rank_in_o1"]) for row in positive]
    values = [row["o2a_best_delta_mean_latency_ms"] for row in states]
    prior = [row["o2p_best_delta_mean_latency_ms"] for row in states]
    extra = [row["o2a_additional_headroom_ms"] for row in states]
    xs = [x for x, _ in correlations]; ys = [y for _, y in correlations]
    result = {
        "workload": workload, "sampled_states": count,
        "o2a_positive_action_fraction": len(positive) / count if count else 0,
        "o2a_no_copy_best_fraction": 1 - len(positive) / count if count else 0,
        "o2p_positive_action_fraction": len(o2p_positive) / count if count else 0,
        "o2p_no_copy_best_fraction": len(o2p_no_copy) / count if count else 0,
        "o2a_best_prefix_is_o1_prefix_fraction": sum(row["o2a_best_prefix_is_o1_prefix"] for row in states) / count if count else 0,
        "o2a_best_action_is_exact_o1_action_fraction": sum(row["o2a_best_action_is_exact_o1_action"] for row in states) / count if count else 0,
        "o2a_best_prefix_rank_p50": percentile(ranks, .5),
        "o2a_best_prefix_rank_p90": percentile(ranks, .9),
        "mean_o2a_best_delta_mean_latency_ms": fmean(values) if values else None,
        "median_o2a_best_delta_mean_latency_ms": median(values) if values else None,
        "p90_o2a_best_delta_mean_latency_ms": percentile(values, .9),
        "mean_o2p_best_delta_mean_latency_ms": fmean(prior) if prior else None,
        "median_o2p_best_delta_mean_latency_ms": median(prior) if prior else None,
        "mean_o2a_additional_headroom_ms": fmean(extra) if extra else None,
        "median_o2a_additional_headroom_ms": median(extra) if extra else None,
        "p90_o2a_additional_headroom_ms": percentile(extra, .9),
        "states_o2a_strictly_better_than_o2p": sum(row["o2a_strictly_better_than_o2p"] for row in states),
        "fraction_o2a_strictly_better_than_o2p": sum(row["o2a_strictly_better_than_o2p"] for row in states) / count if count else 0,
        "states_o2a_ge_1ms_better_than_o2p": sum(row["o2a_ge_1ms_better_than_o2p"] for row in states),
        "fraction_o2a_ge_1ms_better_than_o2p": sum(row["o2a_ge_1ms_better_than_o2p"] for row in states) / count if count else 0,
        "rescued_state_count": sum(row["rescued_from_o2p_no_copy"] for row in states),
        "rescued_state_fraction": sum(row["rescued_from_o2p_no_copy"] for row in states) / len(o2p_no_copy) if o2p_no_copy else 0,
        "rank1_count": ranks.count(1), "rank2_count": ranks.count(2),
        "rank3_count": ranks.count(3), "rank4_count": ranks.count(4),
        "rank5_count": ranks.count(5),
        "rank1_fraction": ranks.count(1) / len(ranks) if ranks else 0,
        "rank_gt_1_fraction": sum(rank > 1 for rank in ranks) / len(ranks) if ranks else 0,
        "future_demand_action_value_spearman": spearman(xs, ys),
        "prefixes_in_correlation": len(xs),
    }
    for kind in ("TYPE_0_NO_OPPORTUNITY", "TYPE_1_PLACEMENT_ONLY",
                 "TYPE_2_PREFIX_SELECTION", "TYPE_3_O1_ALREADY_GOOD"):
        result[f"strict_{kind.lower()}_count"] = sum(
            row["classification_strict"] == kind for row in states)
        result[f"practical_{kind.lower()}_count"] = sum(
            row["classification_practical"] == kind for row in states)
    return result


def make_report(rows: list[dict[str, Any]]) -> str:
    lines = [
        "# O2-A Counterfactual Action-Value Pilot", "",
        "O2-A reuses the exact O2-P common states and evaluates one action per branch. Overlapping state values are not closed-loop replay gains.", "",
        "| Workload | States | O2-P positive | O2-A positive | O2-P NO_COPY | O2-A NO_COPY | Rescued | Mean extra (ms/request) | Median extra | Rank-1 | Spearman | Gate |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        if row["fraction_o2a_strictly_better_than_o2p"] < .01:
            gate = "LOW"
        elif (row["median_o2a_additional_headroom_ms"] == 0
              and row["o2a_no_copy_best_fraction"] > .5):
            gate = "SPARSE"
        else:
            gate = "HIGH"
        corr = row["future_demand_action_value_spearman"]
        lines.append(
            f"| {row['workload']} | {row['sampled_states']} | "
            f"{row['o2p_positive_action_fraction']:.1%} | {row['o2a_positive_action_fraction']:.1%} | "
            f"{row['o2p_no_copy_best_fraction']:.1%} | {row['o2a_no_copy_best_fraction']:.1%} | "
            f"{row['rescued_state_count']} ({row['rescued_state_fraction']:.1%}) | "
            f"{row['mean_o2a_additional_headroom_ms']:.6f} | "
            f"{row['median_o2a_additional_headroom_ms']:.6f} | {row['rank1_fraction']:.1%} | "
            f"{('N/A' if corr is None else f'{corr:.6f}')} | {gate} |"
        )
        rank_total = sum(row[f"rank{rank}_count"] for rank in range(1, 6))
        rank_text = ", ".join(
            f"rank {rank}: {row[f'rank{rank}_count']} "
            f"({row[f'rank{rank}_count'] / rank_total:.1%})"
            for rank in range(1, 6)
        ) if rank_total else "no positive COPY actions"
        if row["o2a_no_copy_best_fraction"] > .5:
            diagnostic = "opportunity scarcity remains dominant, with sparse prefix-selection/placement effects"
        elif row["fraction_o2a_ge_1ms_better_than_o2p"] >= .25:
            diagnostic = "prefix selection contributes material Pilot headroom"
        else:
            diagnostic = "mixed prefix-selection and placement effects"
        lines += [
            "", f"## {row['workload']}", "",
            f"- Positive-action states: {row['o2a_positive_action_fraction']:.1%}; "
            f"NO_COPY changed from {row['o2p_no_copy_best_fraction']:.1%} in O2-P "
            f"to {row['o2a_no_copy_best_fraction']:.1%} in O2-A.",
            f"- Rescued O2-P NO_COPY states: {row['rescued_state_count']} "
            f"({row['rescued_state_fraction']:.1%} of O2-P NO_COPY states).",
            f"- Additional headroom (mean / median / P90): "
            f"{row['mean_o2a_additional_headroom_ms']:.6f} / "
            f"{row['median_o2a_additional_headroom_ms']:.6f} / "
            f"{row['p90_o2a_additional_headroom_ms']:.6f} ms/request.",
            f"- O2-A best Prefix is O1 Top-1 in "
            f"{row['o2a_best_prefix_is_o1_prefix_fraction']:.1%} of all states; "
            f"the exact O1 Prefix-target action is best in "
            f"{row['o2a_best_action_is_exact_o1_action_fraction']:.1%}.",
            f"- Positive-action Prefix ranks: {rank_text}; rank > 1 fraction "
            f"{row['rank_gt_1_fraction']:.1%}.",
            f"- Future-Demand score vs best per-Prefix action-value Spearman: "
            f"{('N/A' if corr is None else f'{corr:.6f}')} "
            f"over {row['prefixes_in_correlation']} Prefix-state observations.",
            f"- Strictly better than O2-P: {row['states_o2a_strictly_better_than_o2p']} "
            f"({row['fraction_o2a_strictly_better_than_o2p']:.1%}); at least 1 ms/request better: "
            f"{row['states_o2a_ge_1ms_better_than_o2p']} "
            f"({row['fraction_o2a_ge_1ms_better_than_o2p']:.1%}).",
            f"- Pilot factor description: {diagnostic}.",
            f"- PREFIX_SELECTION_HEADROOM = {gate}",
        ]
    lines += ["", "Conversation/ToolAgent differences are the mechanical differences shown above. These overlapping counterfactual states are not additive closed-loop gains.", "", "This is a Pilot diagnostic and does not imply a production-system conclusion.", ""]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="O2-A counterfactual action-value pilot")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--o2p-root", type=Path,
                        default=Path("results/o2_oracle/O2P_pilot"))
    args = parser.parse_args()
    o2p_states = read_csv(args.o2p_root / "o2p_state_summary.csv")
    o2p_branches = read_csv(args.o2p_root / "o2p_branch_results.csv")
    specs = (
        ("Conversation", Path("data/mooncake/conversation_trace.jsonl"), Path("configs/taskmain_v1.1/matched/M01_conversation_r_req_kv_v1.yaml")),
        ("ToolAgent", Path("data/mooncake/toolagent_trace.jsonl"), Path("configs/taskmain_v1.1/matched/M03_toolagent_r_req_kv_v1.yaml")),
    )
    all_candidates, all_branches, all_states, summaries = [], [], [], []
    for workload, trace, config in specs:
        print(f"[O2-A] starting {workload}", flush=True)
        states = [row for row in o2p_states if row["workload"] == workload]
        branches = [row for row in o2p_branches if row["workload"] == workload]
        candidate_rows, branch_rows, state_rows, summary = run_workload(
            workload, trace, config, states, branches)
        all_candidates += candidate_rows; all_branches += branch_rows
        all_states += state_rows; summaries.append(summary)
        print(f"[O2-A] completed {workload}: {len(state_rows)} states, "
              f"{len(branch_rows)} branches", flush=True)
    if {(row["state_id"], row["workload"]) for row in all_states} != {
            (row["state_id"], row["workload"]) for row in o2p_states}:
        raise AssertionError("O2-A/O2-P state_id bijection violated")
    args.output.mkdir(parents=True, exist_ok=True)
    write_csv(args.output / "o2a_candidate_prefixes.csv", all_candidates)
    write_csv(args.output / "o2a_branch_results.csv", all_branches)
    write_csv(args.output / "o2a_state_summary.csv", all_states)
    write_csv(args.output / "o2a_workload_summary.csv", summaries)
    write_csv(args.output / "o2a_vs_o2p_summary.csv", summaries)
    (args.output / "o2a_pilot_report.md").write_text(make_report(summaries), encoding="utf-8")
    print(f"[O2-A] all workloads complete; wrote {args.output}", flush=True)


if __name__ == "__main__":
    main()
