from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import pickle
import random
import subprocess
import time
import multiprocessing
from dataclasses import asdict
from pathlib import Path
from statistics import fmean
from typing import Any

import yaml

from .config import SimulatorConfig
from .o2a import TOP_M, choose_action
from .o2a_smoke import current_action_record, fingerprint, replay
from .o2p import branch_metrics, cohort
from .trace import load_trace


_WORKER_BASE = None
_WORKER_REQUESTS = None


def _init_worker(base, requests) -> None:
    global _WORKER_BASE, _WORKER_REQUESTS
    _WORKER_BASE, _WORKER_REQUESTS = base, requests


def _cohort_digest(results, now: float) -> str:
    ids = [result.request_id for result in cohort(results, now)]
    return hashlib.sha256(repr(ids).encode()).hexdigest()


def _evaluate_copy_worker(spec):
    candidate, target, committed, now = spec
    proposed = (*committed, (now, int(candidate["prefix_id"]),
                             int(candidate["source_pod"]), int(target)))
    results, summary = replay(_WORKER_BASE, _WORKER_REQUESTS, proposed, now)
    record = current_action_record(summary, now)
    metrics = branch_metrics(results, summary, now, int(target), _WORKER_BASE.page_bytes)
    return {
        "prefix_id": int(candidate["prefix_id"]),
        "prefix_rank": int(candidate["o1_rank"]),
        "prefix_depth_pages": int(candidate["prefix_depth_pages"]),
        "source_pod": int(candidate["source_pod"]), "target_pod": int(target),
        "copy_bytes": int(candidate["transferable_pages"]) * _WORKER_BASE.page_bytes,
        "action_success": record is not None,
        "loss_total": metrics["loss_total"],
        "state_fingerprint": fingerprint(summary, now),
        "validation_fingerprint": fingerprint(summary, now, True),
        "cohort_digest": _cohort_digest(results, now),
    }


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_commit(repo: Path) -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, check=True,
        capture_output=True, text=True,
    ).stdout.strip()


def checkpoint(root: Path, workload: str, commit: str, config_hash: str,
               trace_hash: str, next_index: int, timestamp: float,
               committed, controller, latest_results, latest_summary) -> Path:
    destination = root / f"decision_{next_index:06d}"
    temporary = root / f".decision_{next_index:06d}.tmp-{os.getpid()}"
    temporary.mkdir(parents=True, exist_ok=False)
    with (temporary / "simulator_state.pkl").open("wb") as handle:
        pickle.dump({"committed_actions": committed,
                     "latest_state_fingerprint": controller.get("latest_state_fingerprint")}, handle)
    (temporary / "oracle_controller_state.json").write_text(
        json.dumps({"next_decision_index": next_index, **controller}, indent=2,
                   sort_keys=True) + "\n", encoding="utf-8")
    (temporary / "metrics.json").write_text(json.dumps({
        "decision_count": next_index,
        "candidate_count": controller["candidate_count"],
        "evaluated_action_count": controller["evaluated_action_count"],
        "latest_request_count": latest_summary["request_count"],
        "latest_peak_memory_used_pages": latest_summary["peak_memory_used_pages"],
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with (temporary / "rng_state.pkl").open("wb") as handle:
        pickle.dump(random.getstate(), handle)
    (temporary / "processed_request_state.json").write_text(json.dumps({
        "simulation_timestamp": timestamp,
        "arrived_request_count": sum(result.arrival_ms <= timestamp for result in latest_results),
        "completed_request_count": sum(result.service_done_ms <= timestamp for result in latest_results),
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (temporary / "checkpoint_manifest.json").write_text(json.dumps({
        "workload": workload, "git_commit": commit, "config_hash": config_hash,
        "trace_hash": trace_hash, "decision_index": next_index,
        "simulation_timestamp": timestamp,
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.rename(destination)
    return destination


def load_checkpoint(path: Path, workload: str, commit: str,
                    config_hash: str, trace_hash: str):
    manifest = json.loads((path / "checkpoint_manifest.json").read_text())
    expected = {"workload": workload, "git_commit": commit,
                "config_hash": config_hash, "trace_hash": trace_hash}
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise ValueError(f"resume checkpoint {key} mismatch")
    with (path / "simulator_state.pkl").open("rb") as handle:
        simulator = pickle.load(handle)
    controller = json.loads((path / "oracle_controller_state.json").read_text())
    with (path / "rng_state.pkl").open("rb") as handle:
        random.setstate(pickle.load(handle))
    return int(controller.pop("next_decision_index")), simulator["committed_actions"], controller


def evaluate_decision(base, requests, committed, now, executor=None):
    no_results, no_summary = replay(base, requests, committed, now)
    probes = no_summary["proactive"]["o2a_candidate_states"]
    if len(probes) != 1:
        raise AssertionError("formal O2-A decision probe missing")
    candidates = probes[0]["candidates"][:TOP_M]
    state_hash = fingerprint(no_summary, now)
    validation_hash = fingerprint(no_summary, now, True)
    no_metrics = branch_metrics(no_results, no_summary, now, None, base.page_bytes)
    cohort_hash = _cohort_digest(no_results, now)
    specs = []
    for candidate in candidates:
        for target in candidate["legal_targets"]:
            specs.append((candidate, target, tuple(committed), now))
    if executor is None:
        _init_worker(base, requests)
        copies = list(map(_evaluate_copy_worker, specs))
    else:
        copies = list(executor.map(_evaluate_copy_worker, specs))
    for row in copies:
            if (row.pop("state_fingerprint") != state_hash
                    or row.pop("validation_fingerprint") != validation_hash):
                raise AssertionError("formal O2-A branch isolation violated")
            if row.pop("cohort_digest") != cohort_hash:
                raise AssertionError("formal O2-A branch cohort mismatch")
            row["delta_loss_total"] = no_metrics["loss_total"] - row.pop("loss_total")
    best, action_type = choose_action(copies)
    return candidates, copies, best, action_type, no_results, no_summary, state_hash


def final_summary(base, requests, committed, controller, final_results, engine_summary,
                  workload, duration_ms):
    scoped_ids = {request.request_id for request in requests
                  if request.split == base.summary_split}
    scoped = [result for result in final_results if result.request_id in scoped_ids]
    proactive = engine_summary["proactive"]
    request_hit = sum(result.h_used_tokens > 0 for result in scoped) / len(scoped) if scoped else 0
    input_tokens = sum(result.input_tokens for result in scoped)
    saved = sum(result.h_used_tokens for result in scoped)
    ranks = controller["selected_prefix_ranks"]
    targets = controller["selected_targets"]
    rank_dist = {str(rank): ranks.count(rank) for rank in range(1, TOP_M + 1)}
    target_dist = {str(pod): targets.count(pod) for pod in range(base.num_pods)}
    wasted = proactive.get("wasted_copy_breakdown", {})
    return {
        "workload": workload, "duration_ms": duration_ms,
        "mean_completion": engine_summary["completion_latency_ms"]["mean"],
        "p50_completion": engine_summary["completion_latency_ms"]["p50"],
        "p95_completion": engine_summary["completion_latency_ms"]["p95"],
        "mean_queue": engine_summary["queue_time_ms"]["mean"],
        "mean_service": engine_summary["service_time_ms"]["mean"],
        "request_hit_rate": request_hit,
        "token_hit_rate": saved / input_tokens if input_tokens else 0,
        "saved_prefill_tokens": saved,
        "proactive_transfer_count": proactive["actions_completed"],
        "proactive_wire_bytes": proactive["wire_bytes"],
        "reactive_transfer_count": engine_summary["transfer"]["transfer_completed"],
        "reactive_wire_bytes": engine_summary["transfer"]["wire_bytes"],
        "evictions": engine_summary["eviction_count"],
        "unused_replica_count": proactive.get("replicas_created", 0) - proactive.get("replicas_used", 0),
        "wasted_copy_count": sum(
            record.get("classification") not in {"USEFUL", "CENSORED"}
            for record in proactive.get("action_records", [])
        ),
        "decision_count": controller["decision_count"],
        "copy_selected_count": controller["copy_selected_count"],
        "no_copy_count": controller["no_copy_count"],
        "copy_admitted_count": proactive["actions_started"],
        "copy_completed_count": proactive["actions_completed"],
        "copy_failed_count": controller["copy_failed_count"],
        "candidate_count": controller["candidate_count"],
        "evaluated_action_count": controller["evaluated_action_count"],
        "mean_candidates_per_decision": controller["candidate_count"] / controller["decision_count"],
        "mean_branch_count": controller["evaluated_action_count"] / controller["decision_count"],
        "selected_prefix_rank_distribution": rank_dist,
        "selected_target_distribution": target_dist,
        "capacity_violations": int(base.cache_capacity_pages is not None
                                   and engine_summary["peak_memory_used_pages"] > base.cache_capacity_pages),
        "budget_violations": proactive["action_exceeds_bucket_cap"],
        "future_leakage": proactive["oracle_future_reads_outside_protocol"],
        "request_loss": len(requests) - len(final_results),
        "deterministic_replay_status": "SMOKE_VALIDATED_AND_CHECKPOINT_HASH_GUARDED",
        "final_state_hash": hashlib.sha256(json.dumps({
            "actions": committed, "results": [asdict(result) for result in final_results],
        }, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Full closed-loop O2-A formal replay")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--trace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--checkpoint-root", type=Path, required=True)
    parser.add_argument("--resume", type=Path)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--max-decisions-this-run", type=int)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError(f"refusing to overwrite output: {args.output}")
    if args.workers <= 0:
        raise ValueError("workers must be positive")
    raw = yaml.safe_load(args.config.read_text(encoding="utf-8"))
    formal = raw["full_o2a"]
    if int(formal["top_m"]) != TOP_M or float(formal["decision_period_ms"]) != 1000:
        raise ValueError("formal O2-A freezes Top-5 and 1-second decisions")
    base = SimulatorConfig.from_yaml(args.config)
    requests = load_trace(args.trace, base.page_tokens, base.split_config)
    times = [float(value) for value in range(
        int(formal["decision_start_ms"]), int(formal["decision_end_ms"]),
        int(formal["decision_period_ms"]))]
    repo = Path(__file__).resolve().parents[2]
    commit, config_hash, trace_hash = git_commit(repo), sha256(args.config), sha256(args.trace)
    controller = {
        "action_sequence": [], "decision_count": 0, "copy_selected_count": 0,
        "no_copy_count": 0, "copy_failed_count": 0, "branch_count": 0,
        "candidate_count": 0, "evaluated_action_count": 0,
        "selected_prefix_ranks": [], "selected_targets": [],
        "latest_state_fingerprint": None,
    }
    start_index, committed = 0, []
    if args.resume:
        start_index, committed, controller = load_checkpoint(
            args.resume, formal["workload"], commit, config_hash, trace_hash)
        print(f"[Full O2-A] resumed at decision {start_index}/{len(times)}", flush=True)
    started = time.monotonic()
    latest_results = latest_summary = None
    stop_index = len(times) if args.max_decisions_this_run is None else min(
        len(times), start_index + args.max_decisions_this_run)
    executor_context = (
        concurrent.futures.ProcessPoolExecutor(
            max_workers=args.workers, mp_context=multiprocessing.get_context("fork"),
            initializer=_init_worker, initargs=(base, requests),
        ) if args.workers > 1 else None
    )
    try:
      for index in range(start_index, stop_index):
        now = times[index]
        candidates, copies, best, action_type, latest_results, latest_summary, state_hash = \
            evaluate_decision(base, requests, committed, now, executor_context)
        controller["candidate_count"] += len(candidates)
        controller["evaluated_action_count"] += 1 + len(copies)
        controller["branch_count"] += 1 + len(copies)
        controller["decision_count"] += 1
        admitted = completed = False
        if action_type == "COPY" and best is not None:
            controller["copy_selected_count"] += 1
            admitted = completed = best["action_success"]
            if completed:
                committed.append((now, best["prefix_id"], best["source_pod"], best["target_pod"]))
                controller["selected_prefix_ranks"].append(best["prefix_rank"])
                controller["selected_targets"].append(best["target_pod"])
            else:
                controller["copy_failed_count"] += 1
        else:
            controller["no_copy_count"] += 1
        controller["latest_state_fingerprint"] = state_hash
        controller["action_sequence"].append({
            "decision_index": index, "timestamp": now,
            "selected_prefix": best["prefix_id"] if action_type == "COPY" else None,
            "selected_prefix_rank": best["prefix_rank"] if action_type == "COPY" else None,
            "selected_target": best["target_pod"] if action_type == "COPY" else None,
            "delta_loss": best["delta_loss_total"] if action_type == "COPY" else 0.0,
            "action_type": action_type, "admitted": admitted, "completed": completed,
            "candidate_count": len(candidates), "evaluated_action_count": 1 + len(copies),
        })
        elapsed = time.monotonic() - started
        done = index - start_index + 1
        eta = elapsed / done * (len(times) - index - 1)
        print(f"[Full O2-A] {formal['workload']} decision {index + 1}/{len(times)} "
              f"action={action_type} branches={1 + len(copies)} elapsed={elapsed / 3600:.2f}h "
              f"eta={eta / 3600:.2f}h", flush=True)
        if (index + 1) % int(formal["checkpoint_period_decisions"]) == 0:
            location = checkpoint(
                args.checkpoint_root, formal["workload"], commit, config_hash,
                trace_hash, index + 1, now, committed, controller,
                latest_results, latest_summary,
            )
            print(f"[Full O2-A] checkpoint {location}", flush=True)
    finally:
        if executor_context is not None:
            executor_context.shutdown(wait=True, cancel_futures=True)

    if stop_index < len(times):
        print(f"[Full O2-A] validation stop after decision {stop_index}/{len(times)}", flush=True)
        return

    final_probe = times[-1] + float(formal["decision_period_ms"])
    final_results, engine_summary = replay(base, requests, committed, final_probe)
    if engine_summary["proactive"]["actions_completed"] != len(committed):
        raise AssertionError("committed COPY did not complete on final trajectory")
    summary = final_summary(base, requests, committed, controller, final_results,
                            engine_summary, formal["workload"],
                            float(formal["decision_end_ms"] - formal["decision_start_ms"]))
    if any(summary[key] for key in (
            "capacity_violations", "budget_violations", "future_leakage",
            "request_loss", "copy_failed_count")):
        raise AssertionError("formal O2-A final correctness invariant failed")
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (args.output / "action_sequence.json").write_text(
        json.dumps(controller["action_sequence"], indent=2) + "\n", encoding="utf-8")
    print(f"[Full O2-A] complete: {args.output}", flush=True)


if __name__ == "__main__":
    main()
