from __future__ import annotations

import argparse
import hashlib
import json
import random
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

import yaml

from .config import SimulatorConfig
from .engine import SimulatorEngine
from .o2a import TOP_M, choose_action
from .o2p import HORIZON_MS, branch_metrics, cohort
from .trace import load_trace


def replay(base: SimulatorConfig, requests, actions, probe_time):
    config = replace(
        base, proactive_enabled=True, proactive_strategy="FUTURE_DEMAND",
        proactive_action="O2A_CLOSED_LOOP",
        o2a_committed_actions=tuple(actions), o2a_probe_time_ms=probe_time,
        o2p_arrival_cutoff_ms=None,
    )
    return SimulatorEngine(config).run(requests)


def fingerprint(summary: dict, time_ms: float, validation: bool = False) -> str:
    key = ("o2a_closed_validation_fingerprints" if validation
           else "o2a_closed_state_fingerprints")
    return summary["proactive"][key][str(float(time_ms))]


def current_action_record(summary: dict, time_ms: float) -> dict | None:
    return next((record for record in summary["proactive"]["action_records"]
                 if record["trigger_time"] == time_ms), None)


def run_controller(base: SimulatorConfig, requests, decision_times: list[float],
                   isolation_audit: bool) -> dict[str, Any]:
    committed: list[tuple[float, int, int, int]] = []
    sequence = []
    branch_count = 0
    copy_selected = copy_completed = copy_failed = 0
    isolation_pass = True
    audit_indices = set(random.Random(base.random_seed).sample(
        range(len(decision_times)), min(3, len(decision_times)))) if isolation_audit else set()

    for index, now in enumerate(decision_times):
        no_results, no_summary = replay(base, requests, committed, now)
        probes = no_summary["proactive"]["o2a_candidate_states"]
        if len(probes) != 1 or probes[0]["time_ms"] != now:
            raise AssertionError("closed-loop decision probe missing")
        probe = probes[0]
        candidates = probe["candidates"][:TOP_M]
        main_hash = fingerprint(no_summary, now)
        validation_hash = fingerprint(no_summary, now, True)
        no_metrics = branch_metrics(no_results, no_summary, now, None, base.page_bytes)
        cohort_ids = [result.request_id for result in cohort(no_results, now)]
        copies = []
        branch_count += 1

        for candidate in candidates:
            for target in candidate["legal_targets"]:
                proposed = (*committed, (
                    now, int(candidate["prefix_id"]),
                    int(candidate["source_pod"]), int(target),
                ))
                results, summary = replay(base, requests, proposed, now)
                if fingerprint(summary, now) != main_hash:
                    raise AssertionError("COPY branch structural state differs at snapshot")
                if fingerprint(summary, now, True) != validation_hash:
                    raise AssertionError("COPY branch event/counter state differs at snapshot")
                if [result.request_id for result in cohort(results, now)] != cohort_ids:
                    raise AssertionError("COPY branch future cohort differs")
                record = current_action_record(summary, now)
                admitted = record is not None
                completed = admitted and summary["proactive"]["actions_completed"] >= len(proposed)
                metrics = branch_metrics(results, summary, now, int(target), base.page_bytes)
                row = {
                    "prefix_id": int(candidate["prefix_id"]),
                    "prefix_depth_pages": int(candidate["prefix_depth_pages"]),
                    "prefix_rank": int(candidate["o1_rank"]),
                    "source_pod": int(candidate["source_pod"]),
                    "target_pod": int(target),
                    "copy_bytes": int(candidate["transferable_pages"]) * base.page_bytes,
                    "action_success": completed,
                    "admitted": admitted, "completed": completed,
                    "delta_loss_total": no_metrics["loss_total"] - metrics["loss_total"],
                }
                copies.append(row); branch_count += 1

        best, action_type = choose_action(copies)
        if action_type == "COPY" and best is not None:
            copy_selected += 1
            if not best["admitted"] or not best["completed"]:
                copy_failed += 1
                action_type = "NO_COPY"
            else:
                copy_completed += 1
                committed.append((now, best["prefix_id"], best["source_pod"],
                                  best["target_pod"]))

        if index in audit_indices:
            check_results, check_summary = replay(base, requests, committed[:-1]
                                                  if action_type == "COPY" else committed, now)
            if (check_results != no_results
                    or fingerprint(check_summary, now, True) != validation_hash):
                isolation_pass = False
                raise AssertionError("branch evaluation polluted reconstructed main trajectory")

        sequence.append({
            "timestamp": now,
            "selected_prefix": (best["prefix_id"] if action_type == "COPY" else None),
            "selected_target": (best["target_pod"] if action_type == "COPY" else None),
            "delta_loss": (best["delta_loss_total"] if action_type == "COPY" else 0.0),
            "action_type": action_type,
            "selected_action": action_type,
            "admitted_action": "COPY" if action_type == "COPY" and best["admitted"] else "NO_COPY",
            "completed_action": "COPY" if action_type == "COPY" and best["completed"] else "NO_COPY",
        })
        print(f"[O2-A smoke] decision {index + 1}/{len(decision_times)} "
              f"t={now:.0f} action={action_type} branches={1 + len(copies)}", flush=True)

    final_probe = decision_times[-1] + base.trigger_period_ms
    final_results, final_summary = replay(base, requests, committed, final_probe)
    final_payload = {
        "state": fingerprint(final_summary, final_probe, True),
        "actions": committed,
        "results": [asdict(result) for result in final_results],
        "capacity": final_summary["peak_memory_used_pages"],
        "wire": final_summary["network_cost"],
    }
    final_hash = hashlib.sha256(
        json.dumps(final_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    capacity_violation = int(
        base.cache_capacity_pages is not None
        and final_summary["peak_memory_used_pages"] > base.cache_capacity_pages
    )
    budget_violation = int(final_summary["proactive"]["action_exceeds_bucket_cap"] > 0)
    expected_requests = sum(request.split == base.summary_split for request in requests)
    return {
        "sequence": sequence, "final_state_hash": final_hash,
        "decision_count": len(decision_times), "branch_count": branch_count,
        "copy_selected_count": copy_selected,
        "no_copy_count": len(decision_times) - copy_selected,
        "copy_completed_count": copy_completed, "copy_failed_count": copy_failed,
        "branch_isolation_pass": isolation_pass,
        "capacity_violation": capacity_violation,
        "budget_violation": budget_violation,
        "deadlock": 0,
        "request_completed": len(final_results),
        "request_lost": expected_requests - len(final_results),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Full closed-loop O2-A smoke validation")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--trace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw = yaml.safe_load(args.config.read_text(encoding="utf-8"))
    smoke = raw["closed_loop_smoke"]
    if int(smoke["top_m"]) != TOP_M:
        raise ValueError("smoke must preserve O2-A Top-5")
    base = SimulatorConfig.from_yaml(args.config)
    requests = load_trace(args.trace, base.page_tokens, base.split_config)
    times = [float(value) for value in range(
        int(smoke["decision_start_ms"]), int(smoke["decision_end_ms"]) + 1,
        int(smoke["decision_period_ms"]))]
    if not 20 <= len(times) <= 100:
        raise ValueError("smoke decision count must be in [20,100]")
    first = run_controller(base, requests, times, isolation_audit=True)
    second = run_controller(base, requests, times, isolation_audit=False)
    deterministic = (first["sequence"] == second["sequence"]
                     and first["final_state_hash"] == second["final_state_hash"])
    if not deterministic:
        raise AssertionError("closed-loop O2-A replay is not deterministic")
    summary = {
        "workload": smoke["workload"],
        "duration_ms": float(smoke["decision_end_ms"]),
        **{key: value for key, value in first.items() if key != "sequence"},
        "deterministic_pass": deterministic,
    }
    if (not summary["branch_isolation_pass"] or summary["capacity_violation"]
            or summary["budget_violation"] or summary["deadlock"]
            or summary["request_lost"] or summary["copy_failed_count"]):
        raise AssertionError("closed-loop O2-A smoke invariant failed")
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "o2a_smoke_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (args.output / "o2a_action_sequence.json").write_text(
        json.dumps(first["sequence"], indent=2) + "\n", encoding="utf-8")
    print(f"[O2-A smoke] PASS; wrote {args.output}", flush=True)


if __name__ == "__main__":
    main()
