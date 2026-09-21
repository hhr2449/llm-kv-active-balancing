"""Shared pilot/formal driver with isolated replays and resumable checkpoints."""
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

STAGE_C_OUTPUT_FILES = {
    "request_records.jsonl", "transfer_records.jsonl", "opportunity_records.jsonl",
    "event_records.jsonl", "proactive_action_records.jsonl", "candidate_decision_records.jsonl",
    "summary.json", "validation.json", "copy_observation_records.jsonl",
    "reuse_observation_records.jsonl", "final_state.json", "execution_evidence.json",
}


def write_json(path, value):
    """Atomically replace a JSON control file so interruption cannot truncate it."""
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False)+"\n")
    temporary.replace(path)


def canonical_sha256(value):
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode()).hexdigest()


def output_hashes(folder):
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(Path(folder).iterdir()) if p.is_file()}


def replay_job(root, output, config, repeat, git, request_progress_every):
    from src.simulator.task_main.isolation import install_legacy_import_guard
    install_legacy_import_guard()
    from src.simulator.task_main.engine import TaskMainEngine
    from src.simulator.task_main.equivalence import projection_digest
    from src.simulator.task_main.metrics import write_outputs
    from src.simulator.task_main.trace import read_trace, separate_input_roles, input_role_identity, full_page_prefix
    from src.simulator.task_main.validation import gate_a_evidence
    # Import the same module set before every replay, irrespective of worker reuse.
    from src.simulator.task_main import reporting

    root, output = Path(root), Path(output)
    path = root/config.trace_path
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    if sha != config.trace_sha256:
        raise ValueError("trace SHA256 mismatch")
    requests, observation = separate_input_roles(read_trace(path), config.visibility_end_ms)
    if config.experiment_kind == "pilot" and not any(r.arrival_ms >= 600000 for r in requests):
        raise ValueError("pilot observation tail missing")
    input_identity = dict(trace_sha256=sha, selected_request_count=len(requests),
                          selected_requests_sha256=projection_digest([asdict(r) for r in requests]),
                          replay_arrival_range=[0, config.visibility_end_ms],
                          **input_role_identity(requests, observation))
    case = f"{config.workload}_line_{config.line_id}"
    print(f"START case={case} repeat={repeat} requests={len(requests)}", flush=True)

    def request_progress(processed, total, simulated_time):
        print(f"REPLAY_PROGRESS case={case} repeat={repeat} requests={processed}/{total} "
              f"simulated_time_ms={simulated_time}", flush=True)

    engine = TaskMainEngine(config)
    result = engine.run(
        requests,
        oracle_observation_requests=(observation if config.proactive_policy == "FUTURE_DEMAND" else None),
        progress_callback=request_progress if request_progress_every else None,
        progress_every_requests=request_progress_every,
    )
    replay_ids = {r.request_id for r in requests}
    boundary_ids = {r.request_id for r in observation}-replay_ids
    checks = result.validation["checks"]
    checks["input_roles_processed_only_replay"] = (
        {r.request_id for r in result.request_records} == replay_ids and
        engine.demand_history.request_ids == engine.reuse_history.request_ids == replay_ids and
        set(engine.load.entries) == replay_ids and
        {r.request_id for r in result.opportunity_records} == replay_ids)
    seen = {path[:depth] for r in requests
            for path in [full_page_prefix(r, len(r.block_ids))]
            for depth in range(1, len(path)+1)}
    checks["input_roles_no_future_only_candidates"] = engine.universe.seen == seen
    if config.proactive_policy == "FUTURE_DEMAND":
        query_time = config.visibility_end_ms-config.future_window_ms
        expected = sum(query_time < r.arrival_ms <= config.visibility_end_ms and
                       len(full_page_prefix(r,len(r.block_ids))) > 0 and r.block_ids[0] == 0
                       for r in observation)
        actual = engine.future_index.count((0,),query_time,config.future_window_ms)
        checks["oracle_right_endpoint_observation"] = actual == expected
        result.summary["policy_diagnostics"]["endpoint_regression"] = dict(
            chain_id=[0], query_time=query_time, expected_count=expected,
            actual_count=actual, boundary_only_count=len(boundary_ids))
    if not all(checks.values()):
        result.validation["status"] = "INVALID"
        raise AssertionError(result.validation)
    del engine
    result.summary["provenance"].update(input_identity=input_identity, git_state=git,
        runtime_legacy_import_guard="ACTIVE",
        loaded_simulator_modules=sorted(k for k in sys.modules if k.startswith("src.simulator")))
    evidence = gate_a_evidence(result)
    target = output/case/f"run_{repeat}"
    write_outputs(target, result)
    write_json(target/"execution_evidence.json", evidence)
    hashes = output_hashes(target)
    entry = dict(case=case, config=config.as_dict(), input_identity=input_identity,
        status=result.validation["status"], validation=result.validation,
        output_sha256=hashes, execution_projection_sha256=evidence["execution_projection_sha256"],
        final_logical_state_digest=evidence["final_logical_state_digest"],
        policy_diagnostics=result.summary["policy_diagnostics"], final_metrics=result.summary["final_metrics"])
    summary = result.summary
    del result
    gc.collect()
    print(f"DONE case={case} repeat={repeat} validation=PASS", flush=True)
    return repeat, entry, summary, evidence


def load_completed_replay(output, config, repeat, expected_hashes):
    """Reconstruct a completed worker result after validating every persisted byte."""
    target = Path(output)/f"{config.workload}_line_{config.line_id}"/f"run_{repeat}"
    observed_hashes = output_hashes(target)
    if observed_hashes != expected_hashes:
        raise ValueError(f"checkpoint artifact hash mismatch: {target}")
    summary = json.loads((target/"summary.json").read_text())
    validation = json.loads((target/"validation.json").read_text())
    evidence = json.loads((target/"execution_evidence.json").read_text())
    entry = dict(
        case=f"{config.workload}_line_{config.line_id}", config=config.as_dict(),
        input_identity=summary["provenance"]["input_identity"], status=validation["status"],
        validation=validation, output_sha256=observed_hashes,
        execution_projection_sha256=evidence["execution_projection_sha256"],
        final_logical_state_digest=evidence["final_logical_state_digest"],
        policy_diagnostics=summary["policy_diagnostics"], final_metrics=summary["final_metrics"],
    )
    return repeat, entry, summary, evidence


def run_matrix(kind, output, workers=10, *, resume=False, progress_interval=60,
               request_progress_every=250):
    from src.simulator.task_main.isolation import install_legacy_import_guard
    install_legacy_import_guard()
    from src.simulator.task_main.config import TaskMainConfig
    from src.simulator.task_main.reporting import build_tables, write_tables
    from src.simulator.task_main.validation import static_gate_a, compare_gate_a

    root = Path(__file__).resolve().parents[2]
    output = Path(output).resolve()
    required = root/"results/task_main"/("stage_c_pilot" if kind == "pilot" else "formal")
    if (kind == "pilot" and (output.parent != required or not output.name.startswith("run_"))) or (kind == "formal" and output != required):
        raise ValueError(f"{kind} output must be a new run_<id> under {required}")
    if type(workers) is not int or not 1 <= workers <= 10:
        raise ValueError("workers must be an integer in 1..10")
    if type(progress_interval) is not int or progress_interval <= 0:
        raise ValueError("progress_interval must be a positive integer")
    if type(request_progress_every) is not int or request_progress_every < 0:
        raise ValueError("request_progress_every must be a nonnegative integer")
    configs = {}
    for workload in ("conversation", "toolagent"):
        configs[workload] = [TaskMainConfig.from_yaml(root/f"configs/task_main/{kind}/{workload}/line_{line}.yaml") for line in range(1,8)]
        if not static_gate_a(configs[workload][4], configs[workload][6]):
            raise ValueError("INVALID static Gate A")
    by_key = {(config.workload, config.line_id): config
              for matrix in configs.values() for config in matrix}
    git = dict(head=subprocess.check_output(["git","rev-parse","HEAD"],cwd=root,text=True).strip(),
        status_porcelain=subprocess.check_output(["git","status","--porcelain"],cwd=root,text=True),
        source_identity="working_tree_module_sha256_not_head_commit")
    sources = [p for folder in ("src/simulator/task_main","scripts/task_main",f"configs/task_main/{kind}")
               for p in (root/folder).rglob("*") if p.is_file() and "__pycache__" not in p.parts]
    git["working_tree_sha256"] = {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(sources)}
    source_digest = canonical_sha256({"head": git["head"], "working_tree_sha256": git["working_tree_sha256"]})
    checkpoint_path = output/"checkpoint.json"
    if resume:
        if not output.is_dir() or not checkpoint_path.is_file():
            raise ValueError("--resume requires an existing run directory with checkpoint.json")
        saved_source = json.loads((output/"source_provenance.json").read_text())
        saved_digest = canonical_sha256({"head": saved_source["head"],
                                         "working_tree_sha256": saved_source["working_tree_sha256"]})
        if source_digest != saved_digest:
            raise ValueError("checkpoint source identity differs from the current working tree")
        checkpoint = json.loads(checkpoint_path.read_text())
        if checkpoint["source_identity_sha256"] != source_digest:
            raise ValueError("checkpoint source identity is inconsistent")
        # Every replay in one matrix must carry byte-identical provenance.  The
        # live git status may gain unrelated files while a run is interrupted;
        # retain the frozen launch-time provenance after source identity passes.
        git = saved_source
        checkpoint["resume_count"] = checkpoint.get("resume_count", 0) + 1
        print(f"RESUME output={output} completed_replays={len(checkpoint['completed'])}/28", flush=True)
    else:
        output.mkdir(parents=True,exist_ok=False)
        write_json(output/"source_provenance.json",git)
        checkpoint = dict(version=1, status="RUNNING", kind=kind,
                          source_identity_sha256=source_digest, completed={}, resume_count=0)
    report = dict(status="RUNNING",purpose="pipeline_validation_only" if kind=="pilot" else "formal",
        kind=kind,cases=[],gate_a={},runtime_legacy_import_guard="ACTIVE",
        resumed_from_checkpoint=resume,
        replay_scheduling=dict(worker_processes=workers, independent_replays=28, shared_simulator_state=False))
    jobs = [(config,repeat) for matrix in configs.values() for config in matrix for repeat in (1,2)]
    jobs.sort(key=lambda item:(item[0].line_id!=3,item[0].workload!="toolagent",item[0].line_id,item[1]))
    for config,_ in jobs:
        folder=output/f"{config.workload}_line_{config.line_id}"
        if not folder.exists():
            folder.mkdir()
            write_json(folder/"config.json",config.as_dict())
        elif json.loads((folder/"config.json").read_text()) != config.as_dict():
            raise ValueError(f"checkpoint config mismatch: {folder}")

    if resume:
        # Recover the narrow crash window after a worker atomically completed its
        # output directory but before the parent recorded that replay checkpoint.
        for config,repeat in jobs:
            checkpoint_key=f"{config.workload}_line_{config.line_id}:run_{repeat}"
            if checkpoint_key in checkpoint["completed"]:
                continue
            target=output/f"{config.workload}_line_{config.line_id}"/f"run_{repeat}"
            if not target.exists():
                continue
            hashes=output_hashes(target)
            complete=(set(hashes)==STAGE_C_OUTPUT_FILES)
            if complete:
                try:
                    summary=json.loads((target/"summary.json").read_text())
                    validation=json.loads((target/"validation.json").read_text())
                    worker_source=summary["provenance"]["git_state"]
                    worker_digest=canonical_sha256({"head":worker_source["head"],
                        "working_tree_sha256":worker_source["working_tree_sha256"]})
                    complete=(validation["status"]=="PASS" and worker_digest==source_digest)
                except (KeyError,ValueError,json.JSONDecodeError):
                    complete=False
            if complete:
                checkpoint["completed"][checkpoint_key]=dict(
                    workload=config.workload,line_id=config.line_id,repeat=repeat,
                    output_sha256=hashes)
                print(f"RECOVER_ORPHAN_CHECKPOINT case={config.workload}_line_{config.line_id} "
                      f"repeat={repeat}",flush=True)
            else:
                quarantine=target.with_name(target.name+f".partial_{int(time.time())}")
                target.replace(quarantine)
                print(f"QUARANTINE_PARTIAL_REPLAY source={target} target={quarantine}",flush=True)

    completed, summaries, gates = {}, {}, {}

    def register(repeat, entry, summary, evidence):
        key=(entry["config"]["workload"],entry["config"]["line_id"])
        completed.setdefault(key,{})[repeat]=(entry,summary,evidence)
        if len(completed[key]) != 2 or key in summaries:
            return
        first,second=completed[key][1],completed[key][2]
        if first[0]["output_sha256"] != second[0]["output_sha256"]:
            raise ValueError(f"INVALID deterministic replay: {entry['case']}")
        case_entry,case_summary,case_evidence=first
        case_entry["deterministic"]=True
        report["cases"].append(case_entry)
        report["cases"].sort(key=lambda c:(c["config"]["workload"],c["config"]["line_id"]))
        summaries[key],gates[key]=case_summary,case_evidence
        workload=key[0]
        if (workload,5) in gates and (workload,7) in gates:
            gate=compare_gate_a(configs[workload][4],configs[workload][6],
                                gates[(workload,5)],gates[(workload,7)])
            report["gate_a"][workload]=gate
            if gate["status"]!="PASS":
                raise ValueError(f"INVALID Gate A: {workload}: {gate}")
        print(f"DOUBLE_REPLAY_PASS case={case_entry['case']}",flush=True)

    for item in checkpoint["completed"].values():
        config = by_key[(item["workload"], item["line_id"])]
        register(*load_completed_replay(output, config, item["repeat"], item["output_sha256"]))
    report["completed_replays"] = len(checkpoint["completed"])
    write_json(checkpoint_path, checkpoint)
    write_json(output/"validation_manifest.json",report)
    missing = [(config,repeat) for config,repeat in jobs
               if f"{config.workload}_line_{config.line_id}:run_{repeat}" not in checkpoint["completed"]]
    started_at = time.monotonic()
    pool = None

    def emit_progress(status, futures=()):
        elapsed = int(time.monotonic()-started_at)
        values = list(futures)
        progress = dict(
            status=status, timestamp_utc=datetime.now(timezone.utc).isoformat(),
            elapsed_seconds=elapsed, completed_replays=len(checkpoint["completed"]),
            total_replays=28, completed_double_replay_cases=len(report["cases"]), total_cases=14,
            running_replays=sum(f.running() and not f.done() for f in values),
            queued_replays=sum(not f.running() and not f.done() for f in values),
            checkpoint_file="checkpoint.json",
        )
        write_json(output/"progress.json",progress)
        print("PROGRESS " + " ".join(f"{key}={value}" for key,value in progress.items()
                                     if key not in {"timestamp_utc","checkpoint_file"}), flush=True)

    try:
        if missing:
            pool = ProcessPoolExecutor(max_workers=workers,mp_context=multiprocessing.get_context("spawn"))
            future_jobs = {pool.submit(replay_job,root,output,c,r,git,request_progress_every):(c,r)
                           for c,r in missing}
            pending = set(future_jobs)
            emit_progress("RUNNING", pending)
            while pending:
                done,pending = wait(pending,timeout=progress_interval,return_when=FIRST_COMPLETED)
                if not done:
                    emit_progress("RUNNING", pending)
                    continue
                for future in done:
                    repeat,entry,summary,evidence=future.result()
                    config,_=future_jobs[future]
                    checkpoint_key=f"{config.workload}_line_{config.line_id}:run_{repeat}"
                    checkpoint["completed"][checkpoint_key]=dict(
                        workload=config.workload,line_id=config.line_id,repeat=repeat,
                        output_sha256=entry["output_sha256"])
                    checkpoint["status"]="RUNNING"
                    write_json(checkpoint_path,checkpoint)
                    register(repeat,entry,summary,evidence)
                    report["completed_replays"]=len(checkpoint["completed"])
                    write_json(output/"validation_manifest.json",report)
                emit_progress("RUNNING", pending)
            pool.shutdown(wait=True)
            pool = None
        ordered_summaries=[summaries[key] for key in sorted(summaries)]
        tables=output/"tables"
        expected_tables=build_tables(ordered_summaries)
        expected_persisted=json.loads(json.dumps(expected_tables,sort_keys=True,allow_nan=False))
        if tables.exists():
            persisted={path.stem:json.loads(path.read_text()) for path in tables.glob("*.json")}
            if persisted != expected_persisted:
                raise ValueError("existing final tables differ from checkpoint reconstruction")
        else:
            partial=output/"tables.partial"
            if partial.exists():
                quarantine=output/f"tables.partial_{int(time.time())}"
                partial.replace(quarantine)
            write_tables(partial,ordered_summaries)
            partial.replace(tables)
        report["status"]="PASS"
        checkpoint["status"]="PASS"
        write_json(checkpoint_path,checkpoint)
        write_json(output/"validation_manifest.json",report)
        emit_progress("PASS")
    except BaseException as error:
        interrupted=isinstance(error,KeyboardInterrupt)
        status="INTERRUPTED_CHECKPOINTED" if interrupted else "INVALID"
        report.update(status=status,blocker=None if interrupted else f"{type(error).__name__}: {error}")
        checkpoint["status"]=status
        checkpoint["last_error"]=None if interrupted else f"{type(error).__name__}: {error}"
        write_json(checkpoint_path,checkpoint)
        write_json(output/"validation_manifest.json",report)
        emit_progress(status)
        if pool is not None:
            processes=getattr(pool,"_processes",None) or {}
            for process in list(processes.values()):
                process.terminate()
            pool.shutdown(wait=True,cancel_futures=True)
        raise
