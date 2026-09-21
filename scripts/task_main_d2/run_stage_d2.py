"""Run the frozen four-case TaskMain D2 MOVE ablation with double replay."""
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
    "move_action_records.jsonl", "move_source_release_records.jsonl",
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
    for folder in ("src/simulator/task_main", "scripts/task_main_d2", "configs/task_main/stage_d2"):
        paths.extend(path for path in (root/folder).rglob("*")
                     if path.is_file() and "__pycache__" not in path.parts)
    paths.append(root/"src/simulator/trace.py")
    mapping = {str(path.relative_to(root)): file_sha256(path) for path in sorted(set(paths))}
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    value = {"head": head, "status_porcelain": subprocess.check_output(
        ["git", "status", "--porcelain"], cwd=root, text=True),
        "working_tree_sha256": mapping,
        "source_identity": "working_tree_module_sha256_not_head_commit"}
    value["source_identity_sha256"] = canonical_sha256(
        {"head": head, "working_tree_sha256": mapping})
    return value


def write_d2_outputs(target, result):
    from src.simulator.task_main.metrics import write_outputs
    write_outputs(target, result)
    for name, rows in (
        ("move_action_records.jsonl", result.move_action_records),
        ("move_source_release_records.jsonl", result.move_source_release_records),
    ):
        with (Path(target)/name).open("w") as handle:
            for row in rows:
                handle.write(json.dumps(asdict(row), sort_keys=True, allow_nan=False)+"\n")


def replay_job(root, output, config, repeat, launch_source, progress_every):
    from src.simulator.task_main.isolation import install_legacy_import_guard
    install_legacy_import_guard()
    from src.simulator.task_main.equivalence import projection_digest
    from src.simulator.task_main.stage_d2.engine import StageD2Engine
    from src.simulator.task_main.stage_d2.isolation import stage_d2_dependency_audit
    from src.simulator.task_main.trace import (full_page_prefix, input_role_identity,
                                                read_trace, separate_input_roles)
    from src.simulator.task_main.validation import gate_a_evidence

    root, output = Path(root), Path(output)
    trace_path = root/config.trace_path
    if file_sha256(trace_path) != config.trace_sha256:
        raise ValueError("trace SHA256 mismatch")
    replay, observation = separate_input_roles(read_trace(trace_path), config.visibility_end_ms)
    input_identity = dict(
        trace_sha256=config.trace_sha256, selected_request_count=len(replay),
        selected_requests_sha256=projection_digest([asdict(row) for row in replay]),
        **input_role_identity(replay, observation),
    )
    case = f"{config.workload}__{config.case_id}"
    print(f"START case={case} repeat={repeat} requests={len(replay)}", flush=True)

    def progress(processed, total, simulated_time):
        print(f"REPLAY_PROGRESS case={case} repeat={repeat} requests={processed}/{total} "
              f"simulated_time_ms={simulated_time}", flush=True)

    engine = StageD2Engine(config)
    result = engine.run(
        replay,
        oracle_observation_requests=(observation if config.proactive_policy == "FUTURE_DEMAND" else None),
        progress_callback=progress if progress_every else None,
        progress_every_requests=progress_every,
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
                       full_page_prefix(row, len(row.block_ids)) and row.block_ids[0] == 0
                       for row in observation)
        actual = engine.future_index.count((0,), query_time, config.future_window_ms)
        checks["oracle_right_endpoint_observation"] = actual == expected
        result.summary["policy_diagnostics"]["endpoint_regression"] = {
            "chain_id": [0], "query_time": query_time, "expected_count": expected,
            "actual_count": actual, "boundary_only_count": len(boundary_ids),
        }
    audit = stage_d2_dependency_audit()
    checks["stage_d2_dependency_isolation"] = audit["status"] == "PASS"
    result.summary["provenance"].update(
        input_identity=input_identity, stage_d2_dependency_audit=audit,
        source_identity_sha256=launch_source["source_identity_sha256"],
        runtime_legacy_import_guard="ACTIVE",
        loaded_simulator_modules=sorted(
            name for name in sys.modules if name.startswith("src.simulator")),
    )
    result.validation["status"] = "PASS" if all(checks.values()) else "INVALID"
    if result.validation["status"] != "PASS":
        raise AssertionError(result.validation)
    evidence = gate_a_evidence(result)
    target = output/case/f"run_{repeat}"
    write_d2_outputs(target, result)
    write_json(target/"execution_evidence.json", evidence)
    hashes = output_hashes(target)
    if set(hashes) != OUTPUT_FILES:
        raise AssertionError(f"incomplete D2 artifact set: {set(hashes)^OUTPUT_FILES}")
    entry = {
        "case": case, "case_id": config.case_id, "workload": config.workload,
        "repeat": repeat, "config": config.as_dict(), "input_identity": input_identity,
        "status": result.validation["status"], "validation": result.validation,
        "output_sha256": hashes,
        "execution_projection_sha256": evidence["execution_projection_sha256"],
        "final_logical_state_digest": evidence["final_logical_state_digest"],
        "final_metrics": result.summary["final_metrics"],
        "stage_d2_metrics": result.summary["stage_d2_metrics"],
    }
    summary = result.summary
    del result, engine
    gc.collect()
    print(f"DONE case={case} repeat={repeat} validation=PASS", flush=True)
    return repeat, entry, summary


def load_completed(output, config, repeat, hashes):
    target = Path(output)/f"{config.workload}__{config.case_id}"/f"run_{repeat}"
    observed = output_hashes(target)
    if observed != hashes or set(observed) != OUTPUT_FILES:
        raise ValueError(f"checkpoint artifact mismatch: {target}")
    summary = json.loads((target/"summary.json").read_text())
    validation = json.loads((target/"validation.json").read_text())
    evidence = json.loads((target/"execution_evidence.json").read_text())
    entry = {"case": f"{config.workload}__{config.case_id}", "case_id": config.case_id,
             "workload": config.workload, "repeat": repeat, "config": config.as_dict(),
             "input_identity": summary["provenance"]["input_identity"],
             "status": validation["status"], "validation": validation,
             "output_sha256": observed,
             "execution_projection_sha256": evidence["execution_projection_sha256"],
             "final_logical_state_digest": evidence["final_logical_state_digest"],
             "final_metrics": summary["final_metrics"],
             "stage_d2_metrics": summary["stage_d2_metrics"]}
    return repeat, entry, summary


def run_stage_d2(output, workers=8, resume=False, progress_interval=60, progress_every=250):
    from src.simulator.task_main.isolation import install_legacy_import_guard
    install_legacy_import_guard()
    from src.simulator.task_main.stage_d2.config import StageD2Config
    from src.simulator.task_main.stage_d2.reference import validate_copy_references
    from src.simulator.task_main.stage_d2.reporting import publish_results

    root = Path(__file__).resolve().parents[2]
    output = Path(output).resolve()
    parent = (root/"results/task_main/stage_d2_move_ablation").resolve()
    if output.parent != parent or not output.name.startswith("run_"):
        raise ValueError(f"output must be a run_<id> under {parent}")
    if type(workers) is not int or not 1 <= workers <= 8:
        raise ValueError("workers must be in 1..8")
    configs = [StageD2Config.from_yaml(path) for path in
               sorted((root/"configs/task_main/stage_d2").rglob("*.yaml"))]
    if len(configs) != 4 or len({(row.workload, row.case_id) for row in configs}) != 4:
        raise ValueError("Stage D2 requires exactly four MOVE cases")
    reference = validate_copy_references(root, configs)
    current_source = source_provenance(root)
    source_digest = current_source["source_identity_sha256"]
    checkpoint_path = output/"checkpoint.json"
    if resume:
        checkpoint = json.loads(checkpoint_path.read_text())
        launch_source = json.loads((output/"source_provenance.json").read_text())
        if launch_source["source_identity_sha256"] != source_digest:
            raise ValueError("current D2 source differs from checkpoint")
        checkpoint["resume_count"] += 1
    else:
        output.mkdir(parents=True, exist_ok=False)
        write_json(output/"source_provenance.json", current_source)
        write_json(output/"copy_reference_identity.json", reference)
        checkpoint = {"version": 1, "status": "RUNNING", "completed": {},
                      "resume_count": 0, "source_identity_sha256": source_digest}
        launch_source = current_source
    report = {"status": "RUNNING", "case_count": 4, "replay_count": 8,
              "cases": [], "copy_reference": reference}
    jobs = [(config, repeat) for config in configs for repeat in (1, 2)]
    jobs.sort(key=lambda item: (item[0].workload != "toolagent",
                                item[0].proactive_policy != "FUTURE_DEMAND", item[1]))
    by_key = {(row.workload, row.case_id): row for row in configs}
    for config, _ in jobs:
        folder = output/f"{config.workload}__{config.case_id}"
        folder.mkdir(exist_ok=True)
        config_path = folder/"config.json"
        if config_path.exists() and json.loads(config_path.read_text()) != config.as_dict():
            raise ValueError(f"config mismatch: {folder}")
        if not config_path.exists():
            write_json(config_path, config.as_dict())
    completed, summaries = {}, {}

    def register(repeat, entry, summary):
        key = (entry["workload"], entry["case_id"])
        completed.setdefault(key, {})[repeat] = (entry, summary)
        if len(completed[key]) != 2 or key in summaries:
            return
        first, second = completed[key][1], completed[key][2]
        if first[0]["output_sha256"] != second[0]["output_sha256"]:
            raise ValueError(f"D2 determinism failure: {key}")
        value = first[0]
        value["deterministic"] = True
        report["cases"].append(value)
        report["cases"].sort(key=lambda row: (row["workload"], row["case_id"]))
        summaries[key] = first[1]
        print(f"DOUBLE_REPLAY_PASS case={value['case']}", flush=True)

    # Recover only complete, hashable orphan runs. Partial directories are quarantined.
    if resume:
        for config, repeat in jobs:
            checkpoint_key = f"{config.workload}__{config.case_id}:run_{repeat}"
            if checkpoint_key in checkpoint["completed"]:
                continue
            target = output/f"{config.workload}__{config.case_id}"/f"run_{repeat}"
            if not target.exists():
                continue
            hashes = output_hashes(target)
            if set(hashes) == OUTPUT_FILES:
                checkpoint["completed"][checkpoint_key] = {
                    "workload": config.workload, "case_id": config.case_id,
                    "repeat": repeat, "output_sha256": hashes}
            else:
                target.replace(target.with_name(target.name+f".partial_{int(time.time())}"))
    for value in checkpoint["completed"].values():
        config = by_key[(value["workload"], value["case_id"])]
        register(*load_completed(output, config, value["repeat"], value["output_sha256"]))
    missing = [(config, repeat) for config, repeat in jobs if
               f"{config.workload}__{config.case_id}:run_{repeat}" not in checkpoint["completed"]]
    write_json(checkpoint_path, checkpoint)
    write_json(output/"validation_manifest.json", report)
    started = time.monotonic()
    pool = None

    def emit(status, futures=()):
        pending = list(futures)
        value = {"status": status,
                 "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                 "elapsed_seconds": int(time.monotonic()-started),
                 "completed_replays": len(checkpoint["completed"]), "total_replays": 8,
                 "completed_double_replay_cases": len(report["cases"]), "total_cases": 4,
                 "running_replays": sum(item.running() and not item.done() for item in pending),
                 "queued_replays": sum(not item.running() and not item.done() for item in pending)}
        write_json(output/"progress.json", value)
        print("PROGRESS "+" ".join(f"{key}={item}" for key, item in value.items()
                                    if key != "timestamp_utc"), flush=True)

    try:
        if missing:
            pool = ProcessPoolExecutor(max_workers=workers,
                mp_context=multiprocessing.get_context("spawn"))
            futures = {pool.submit(replay_job, root, output, config, repeat,
                                   launch_source, progress_every): (config, repeat)
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
                    key = f"{config.workload}__{config.case_id}:run_{repeat}"
                    checkpoint["completed"][key] = {
                        "workload": config.workload, "case_id": config.case_id,
                        "repeat": repeat, "output_sha256": entry["output_sha256"]}
                    write_json(checkpoint_path, checkpoint)
                    register(repeat, entry, summary)
                    write_json(output/"validation_manifest.json", report)
                emit("RUNNING", pending)
            pool.shutdown(wait=True)
            pool = None
        if len(summaries) != 4:
            raise AssertionError("incomplete D2 matrix")
        if source_provenance(root)["source_identity_sha256"] != source_digest:
            raise ValueError("D2 source changed during run")
        source_run_id = "stage_d2:"+source_digest[:16]
        publish_results(root, output, summaries, reference, source_run_id)
        checkpoint["status"] = report["status"] = "PASS"
        report["tables"] = str((output/"tables").relative_to(root))
        report["analysis_report"] = "docs/analysis/task_main_stage_d2_move_ablation.md"
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
            for process in (getattr(pool, "_processes", None) or {}).values():
                process.terminate()
            pool.shutdown(wait=True, cancel_futures=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--progress-interval", type=int, default=60)
    parser.add_argument("--request-progress-every", type=int, default=250)
    args = parser.parse_args()
    run_stage_d2(args.output, args.workers, args.resume, args.progress_interval,
                 args.request_progress_every)
