"""Run the frozen 32-case Stage D1 sensitivity matrix with double replay."""
from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from dataclasses import asdict
from datetime import datetime, timezone
import gc
import hashlib
import json
import multiprocessing
from pathlib import Path
import subprocess
import sys
import time

OUTPUT_FILES = {
    "request_records.jsonl", "transfer_records.jsonl", "opportunity_records.jsonl",
    "event_records.jsonl", "proactive_action_records.jsonl", "candidate_decision_records.jsonl",
    "summary.json", "validation.json", "copy_observation_records.jsonl",
    "reuse_observation_records.jsonl", "final_state.json", "execution_evidence.json",
}


def write_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name+".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False)+"\n")
    temporary.replace(path)


def canonical_sha256(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                         allow_nan=False).encode()).hexdigest()


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024*1024), b""):
            digest.update(block)
    return digest.hexdigest()


def output_hashes(folder):
    return {path.name: file_sha256(path) for path in sorted(Path(folder).iterdir()) if path.is_file()}


def source_provenance(root):
    paths = []
    for folder in ("src/simulator/task_main", "scripts/task_main_d1", "configs/task_main/stage_d1"):
        paths.extend(path for path in (root/folder).rglob("*")
                     if path.is_file() and "__pycache__" not in path.parts)
    paths.append(root/"src/simulator/trace.py")
    mapping = {str(path.relative_to(root)): file_sha256(path) for path in sorted(set(paths))}
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    return {
        "head": head,
        "status_porcelain": subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=root, text=True),
        "working_tree_sha256": mapping,
        "source_identity": "working_tree_module_sha256_not_head_commit",
        "source_identity_sha256": canonical_sha256({"head": head, "working_tree_sha256": mapping}),
    }


def validate_formal_reference(root):
    formal = root/"results/task_main/formal"
    manifest = json.loads((formal/"validation_manifest.json").read_text())
    checkpoint = json.loads((formal/"checkpoint.json").read_text())
    progress = json.loads((formal/"progress.json").read_text())
    source = json.loads((formal/"source_provenance.json").read_text())
    checks = {
        "manifest_pass": manifest.get("status") == "PASS",
        "checkpoint_pass_28": checkpoint.get("status") == "PASS" and len(checkpoint.get("completed", {})) == 28,
        "progress_pass_14": progress.get("status") == "PASS" and progress.get("completed_double_replay_cases") == 14,
        "case_count": len(manifest.get("cases", [])) == 14,
        "all_deterministic": all(case.get("status") == "PASS" and case.get("deterministic")
                                 for case in manifest.get("cases", [])),
        "evaluation_scope": all(
            case["config"]["evaluation_start_ms"] == 1500000 and
            case["config"]["evaluation_end_ms"] == 2700000
            for case in manifest.get("cases", [])),
    }
    current_matches = {}
    for relative, expected in source["working_tree_sha256"].items():
        path = root/relative
        current_matches[relative] = path.is_file() and file_sha256(path) == expected
    checks["frozen_formal_source_files_unchanged"] = all(current_matches.values())
    config_match = True
    for case in manifest["cases"]:
        persisted = json.loads((formal/case["case"]/"config.json").read_text())
        config_match &= persisted == case["config"]
    checks["formal_configs_match_manifest"] = config_match
    if not all(checks.values()):
        raise ValueError(f"formal reference identity mismatch: {checks}")
    diagnostics_identity = root/"results/task_main/formal_diagnostics_v1/run_identity.json"
    formal_run_id = (json.loads(diagnostics_identity.read_text())["formal_run_id"]
                     if diagnostics_identity.is_file() else
                     "formal:"+canonical_sha256(manifest)[:16])
    return {
        "status": "PASS", "checks": checks, "formal_run_id": formal_run_id,
        "protocol_version": "TASK_MAIN_V1_STAGE_C",
        "formal_path": str(formal.relative_to(root)),
        "validation_manifest_sha256": file_sha256(formal/"validation_manifest.json"),
        "source_provenance_sha256": file_sha256(formal/"source_provenance.json"),
        "frozen_source_file_count": len(current_matches),
    }


def replay_job(root, output, config, repeat, launch_source, request_progress_every):
    from src.simulator.task_main.isolation import install_legacy_import_guard
    install_legacy_import_guard()
    from src.simulator.task_main.equivalence import projection_digest
    from src.simulator.task_main.metrics import write_outputs
    from src.simulator.task_main.stage_d1.diagnostics import build_diagnostics, validate_d1
    from src.simulator.task_main.stage_d1.engine import StageD1Engine
    from src.simulator.task_main.stage_d1.isolation import stage_d1_dependency_audit
    from src.simulator.task_main.trace import (full_page_prefix, input_role_identity,
                                                read_trace, separate_input_roles)
    from src.simulator.task_main.validation import gate_a_evidence

    root, output = Path(root), Path(output)
    trace_path = root/config.trace_path
    trace_sha = file_sha256(trace_path)
    if trace_sha != config.trace_sha256:
        raise ValueError("trace SHA256 mismatch")
    replay, observation = separate_input_roles(read_trace(trace_path), config.visibility_end_ms)
    input_identity = dict(
        trace_sha256=trace_sha, selected_request_count=len(replay),
        selected_requests_sha256=projection_digest([asdict(row) for row in replay]),
        replay_arrival_range=[0, config.visibility_end_ms],
        **input_role_identity(replay, observation),
    )
    case = f"{config.workload}__{config.case_id}"
    print(f"START case={case} repeat={repeat} requests={len(replay)}", flush=True)

    def progress(processed, total, simulated_time):
        print(f"REPLAY_PROGRESS case={case} repeat={repeat} requests={processed}/{total} "
              f"simulated_time_ms={simulated_time}", flush=True)

    engine = StageD1Engine(config)
    result = engine.run(
        replay,
        oracle_observation_requests=(observation if config.proactive_policy == "FUTURE_DEMAND" else None),
        progress_callback=progress if request_progress_every else None,
        progress_every_requests=request_progress_every,
    )
    replay_ids = {row.request_id for row in replay}
    boundary_ids = {row.request_id for row in observation}-replay_ids
    checks = result.validation["checks"]
    checks["input_roles_processed_only_replay"] = (
        {row.request_id for row in result.request_records} == replay_ids and
        engine.demand_history.request_ids == engine.reuse_history.request_ids == replay_ids and
        set(engine.load.entries) == replay_ids and
        {row.request_id for row in result.opportunity_records} == replay_ids)
    seen = {path[:depth] for row in replay
            for path in [full_page_prefix(row, len(row.block_ids))]
            for depth in range(1, len(path)+1)}
    checks["input_roles_no_future_only_candidates"] = engine.universe.seen == seen
    if config.proactive_policy == "FUTURE_DEMAND":
        query_time = config.visibility_end_ms-config.future_window_ms
        expected = sum(query_time < row.arrival_ms <= config.visibility_end_ms and
                       len(full_page_prefix(row, len(row.block_ids))) > 0 and row.block_ids[0] == 0
                       for row in observation)
        actual = engine.future_index.count((0,), query_time, config.future_window_ms)
        checks["oracle_right_endpoint_observation"] = actual == expected
        result.summary["policy_diagnostics"]["endpoint_regression"] = {
            "chain_id": [0], "query_time": query_time, "expected_count": expected,
            "actual_count": actual, "boundary_only_count": len(boundary_ids),
        }
    result.summary["stage_d1_diagnostics"] = build_diagnostics(result, config)
    checks.update(validate_d1(result, config))
    audit = stage_d1_dependency_audit()
    checks["stage_d1_dependency_isolation"] = audit["status"] == "PASS"
    result.summary["provenance"].update(
        stage_d1_study_version=config.study_version,
        stage_d1_dependency_audit=audit,
        input_identity=input_identity,
        source_identity_sha256=launch_source["source_identity_sha256"],
        runtime_legacy_import_guard="ACTIVE",
        loaded_simulator_modules=sorted(name for name in sys.modules if name.startswith("src.simulator")),
    )
    result.validation["status"] = "PASS" if all(checks.values()) else "INVALID"
    if result.validation["status"] != "PASS":
        raise AssertionError(result.validation)
    evidence = gate_a_evidence(result)
    target = output/case/f"run_{repeat}"
    write_outputs(target, result)
    write_json(target/"execution_evidence.json", evidence)
    hashes = output_hashes(target)
    if set(hashes) != OUTPUT_FILES:
        raise AssertionError("incomplete replay artifact set")
    entry = {
        "case": case, "case_id": config.case_id, "family": config.family,
        "workload": config.workload, "repeat": repeat, "config": config.as_dict(),
        "input_identity": input_identity, "status": result.validation["status"],
        "validation": result.validation, "output_sha256": hashes,
        "execution_projection_sha256": evidence["execution_projection_sha256"],
        "final_logical_state_digest": evidence["final_logical_state_digest"],
        "policy_diagnostics": result.summary["policy_diagnostics"],
        "final_metrics": result.summary["final_metrics"],
    }
    summary = result.summary
    del result, engine
    gc.collect()
    print(f"DONE case={case} repeat={repeat} validation=PASS", flush=True)
    return repeat, entry, summary


def load_completed(output, config, repeat, expected_hashes):
    target = Path(output)/f"{config.workload}__{config.case_id}"/f"run_{repeat}"
    observed = output_hashes(target)
    if observed != expected_hashes or set(observed) != OUTPUT_FILES:
        raise ValueError(f"checkpoint artifact mismatch: {target}")
    summary = json.loads((target/"summary.json").read_text())
    validation = json.loads((target/"validation.json").read_text())
    evidence = json.loads((target/"execution_evidence.json").read_text())
    entry = {
        "case": f"{config.workload}__{config.case_id}", "case_id": config.case_id,
        "family": config.family, "workload": config.workload, "repeat": repeat,
        "config": config.as_dict(), "input_identity": summary["provenance"]["input_identity"],
        "status": validation["status"], "validation": validation, "output_sha256": observed,
        "execution_projection_sha256": evidence["execution_projection_sha256"],
        "final_logical_state_digest": evidence["final_logical_state_digest"],
        "policy_diagnostics": summary["policy_diagnostics"],
        "final_metrics": summary["final_metrics"],
    }
    return repeat, entry, summary


def _job_priority(config):
    """Longest expected jobs first; affects wall scheduling, never simulation order."""
    workload = 0 if config.workload == "toolagent" else 1
    if config.family == "PERSISTENCE_K":
        weight = 0 if config.shortlist_k == 20 else 2
    elif config.proactive_policy == "FUTURE_DEMAND":
        weight = 1
    elif config.proactive_policy == "RECENCY":
        weight = 2
    elif config.proactive_policy == "PERSISTENCE":
        weight = 3
    else:
        weight = 4
    return weight, workload, config.case_id


def run_stage_d1(output, workers=20, resume=False, progress_interval=60,
                 request_progress_every=250):
    from src.simulator.task_main.isolation import install_legacy_import_guard
    install_legacy_import_guard()
    from src.simulator.task_main.stage_d1.config import StageD1Config
    from src.simulator.task_main.stage_d1.reporting import publish_results

    root = Path(__file__).resolve().parents[2]
    output = Path(output).resolve()
    required_parent = (root/"results/task_main/stage_d1_sensitivity").resolve()
    if output.parent != required_parent or not output.name.startswith("run_"):
        raise ValueError(f"output must be a new run_<id> under {required_parent}")
    if type(workers) is not int or not 1 <= workers <= 24:
        raise ValueError("workers must be in 1..24")
    if type(progress_interval) is not int or progress_interval <= 0:
        raise ValueError("progress_interval must be positive")
    if type(request_progress_every) is not int or request_progress_every < 0:
        raise ValueError("request_progress_every must be nonnegative")
    formal_reference = validate_formal_reference(root)
    configs = []
    for workload in ("conversation", "toolagent"):
        folder = root/f"configs/task_main/stage_d1/{workload}"
        configs.extend(StageD1Config.from_yaml(path) for path in sorted(folder.glob("*.yaml")))
    if len(configs) != 32 or len({(row.workload, row.case_id) for row in configs}) != 32:
        raise ValueError("Stage D1 requires exactly 32 unique cases")
    current_source = source_provenance(root)
    source_digest = current_source["source_identity_sha256"]
    checkpoint_path = output/"checkpoint.json"
    if resume:
        if not checkpoint_path.is_file():
            raise ValueError("--resume requires checkpoint.json")
        frozen_source = json.loads((output/"source_provenance.json").read_text())
        if frozen_source["source_identity_sha256"] != source_digest:
            raise ValueError("current Stage D1 source differs from checkpoint")
        checkpoint = json.loads(checkpoint_path.read_text())
        checkpoint["resume_count"] += 1
        launch_source = frozen_source
    else:
        output.mkdir(parents=True, exist_ok=False)
        write_json(output/"source_provenance.json", current_source)
        write_json(output/"formal_reference_identity.json", formal_reference)
        checkpoint = {"version": 1, "status": "RUNNING", "source_identity_sha256": source_digest,
                      "completed": {}, "resume_count": 0}
        launch_source = current_source
    by_key = {(row.workload, row.case_id): row for row in configs}
    report = {
        "status": "RUNNING", "study_version": "TASK_MAIN_STAGE_D1_SENSITIVITY_V1",
        "case_count": 32, "replay_count": 64, "cases": [],
        "formal_reference": formal_reference, "no_parameter_search": True,
        "move_not_implemented": True, "runtime_legacy_import_guard": "ACTIVE",
    }
    jobs = [(row, repeat) for row in configs for repeat in (1, 2)]
    jobs.sort(key=lambda item: (_job_priority(item[0]), item[1]))
    for config, _ in jobs:
        folder = output/f"{config.workload}__{config.case_id}"
        folder.mkdir(exist_ok=True)
        config_file = folder/"config.json"
        if config_file.exists() and json.loads(config_file.read_text()) != config.as_dict():
            raise ValueError(f"config mismatch: {folder}")
        if not config_file.exists():
            write_json(config_file, config.as_dict())
    completed, summaries = {}, {}

    def register(repeat, entry, summary):
        key = (entry["workload"], entry["case_id"])
        completed.setdefault(key, {})[repeat] = (entry, summary)
        if len(completed[key]) != 2 or key in summaries:
            return
        first, second = completed[key][1], completed[key][2]
        if first[0]["output_sha256"] != second[0]["output_sha256"]:
            raise ValueError(f"determinism failure: {key}")
        case_entry = first[0]
        case_entry["deterministic"] = True
        report["cases"].append(case_entry)
        report["cases"].sort(key=lambda row: (row["workload"], row["family"], row["case_id"]))
        summaries[key] = first[1]
        print(f"DOUBLE_REPLAY_PASS case={case_entry['case']}", flush=True)

    if resume:
        # Recover the narrow interval in which a worker has finished all twelve
        # files but the parent has not yet atomically registered the checkpoint.
        for config, repeat in jobs:
            checkpoint_key = f"{config.workload}__{config.case_id}:run_{repeat}"
            if checkpoint_key in checkpoint["completed"]:
                continue
            target = output/f"{config.workload}__{config.case_id}"/f"run_{repeat}"
            if not target.exists():
                continue
            hashes = output_hashes(target)
            complete = set(hashes) == OUTPUT_FILES
            if complete:
                try:
                    summary = json.loads((target/"summary.json").read_text())
                    validation = json.loads((target/"validation.json").read_text())
                    complete = (validation["status"] == "PASS" and
                                summary["provenance"]["source_identity_sha256"] == source_digest)
                except (KeyError, ValueError, json.JSONDecodeError):
                    complete = False
            if complete:
                checkpoint["completed"][checkpoint_key] = {
                    "workload": config.workload, "case_id": config.case_id,
                    "repeat": repeat, "output_sha256": hashes,
                }
                print(f"RECOVER_ORPHAN_CHECKPOINT case={config.workload}__{config.case_id} "
                      f"repeat={repeat}", flush=True)
            else:
                quarantine = target.with_name(target.name+f".partial_{int(time.time())}")
                target.replace(quarantine)
                print(f"QUARANTINE_PARTIAL_REPLAY source={target} target={quarantine}", flush=True)

    for item in checkpoint["completed"].values():
        config = by_key[(item["workload"], item["case_id"])]
        register(*load_completed(output, config, item["repeat"], item["output_sha256"]))
    missing = [(config, repeat) for config, repeat in jobs
               if f"{config.workload}__{config.case_id}:run_{repeat}" not in checkpoint["completed"]]
    write_json(checkpoint_path, checkpoint)
    write_json(output/"validation_manifest.json", report)
    started = time.monotonic()
    pool = None

    def emit(status, futures=()):
        pending = list(futures)
        progress = {
            "status": status, "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "elapsed_seconds": int(time.monotonic()-started),
            "completed_replays": len(checkpoint["completed"]), "total_replays": 64,
            "completed_double_replay_cases": len(report["cases"]), "total_cases": 32,
            "running_replays": sum(item.running() and not item.done() for item in pending),
            "queued_replays": sum(not item.running() and not item.done() for item in pending),
            "checkpoint_file": "checkpoint.json",
        }
        write_json(output/"progress.json", progress)
        print("PROGRESS "+" ".join(f"{key}={value}" for key, value in progress.items()
                                    if key not in {"timestamp_utc", "checkpoint_file"}), flush=True)

    try:
        if missing:
            pool = ProcessPoolExecutor(max_workers=workers,
                mp_context=multiprocessing.get_context("spawn"))
            futures = {pool.submit(replay_job, root, output, config, repeat,
                                   launch_source, request_progress_every): (config, repeat)
                       for config, repeat in missing}
            pending = set(futures)
            emit("RUNNING", pending)
            while pending:
                done, pending = wait(pending, timeout=progress_interval,
                                     return_when=FIRST_COMPLETED)
                if not done:
                    emit("RUNNING", pending)
                    continue
                for future in done:
                    repeat, entry, summary = future.result()
                    config, _ = futures[future]
                    checkpoint_key = f"{config.workload}__{config.case_id}:run_{repeat}"
                    checkpoint["completed"][checkpoint_key] = {
                        "workload": config.workload, "case_id": config.case_id,
                        "repeat": repeat, "output_sha256": entry["output_sha256"],
                    }
                    write_json(checkpoint_path, checkpoint)
                    register(repeat, entry, summary)
                    write_json(output/"validation_manifest.json", report)
                emit("RUNNING", pending)
            pool.shutdown(wait=True)
            pool = None
        if len(summaries) != 32:
            raise AssertionError("incomplete Stage D1 matrix")
        if source_provenance(root)["source_identity_sha256"] != source_digest:
            raise ValueError("Stage D1 source changed during the run")
        tables_dir = output/"tables"
        report_path = root/"docs/analysis/task_main_stage_d1_sensitivity.md"
        publish_results(root, output, summaries, formal_reference, tables_dir, report_path)
        report["status"] = checkpoint["status"] = "PASS"
        report["tables"] = str(tables_dir.relative_to(root))
        report["analysis_report"] = str(report_path.relative_to(root))
        write_json(checkpoint_path, checkpoint)
        write_json(output/"validation_manifest.json", report)
        emit("PASS")
    except BaseException as error:
        status = "INTERRUPTED_CHECKPOINTED" if isinstance(error, KeyboardInterrupt) else "INVALID"
        checkpoint["status"] = report["status"] = status
        report["blocker"] = None if status == "INTERRUPTED_CHECKPOINTED" else f"{type(error).__name__}: {error}"
        write_json(checkpoint_path, checkpoint)
        write_json(output/"validation_manifest.json", report)
        emit(status)
        if pool is not None:
            processes = getattr(pool, "_processes", None) or {}
            for process in processes.values():
                process.terminate()
            pool.shutdown(wait=True, cancel_futures=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--progress-interval", type=int, default=60)
    parser.add_argument("--request-progress-every", type=int, default=250)
    args = parser.parse_args()
    run_stage_d1(args.output, args.workers, args.resume, args.progress_interval,
                 args.request_progress_every)
