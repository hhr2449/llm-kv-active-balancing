"""Run a small deterministic MOVE smoke for both frozen D2 triggers."""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
from pathlib import Path

from scripts.task_main_d2.run_stage_d2 import write_json


def digest(value):
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def run_once(config, requests):
    from src.simulator.task_main.stage_d2.engine import StageD2Engine
    engine = StageD2Engine(config)
    result = engine.run(
        requests,
        oracle_observation_requests=(
            requests if config.proactive_policy == "FUTURE_DEMAND" else None),
    )
    projection = {
        "requests": [asdict(row) for row in result.request_records],
        "transfers": [asdict(row) for row in result.transfer_records],
        "opportunities": [asdict(row) for row in result.opportunity_records],
        "actions": [asdict(row) for row in result.proactive_action_records],
        "move_actions": [asdict(row) for row in result.move_action_records],
        "move_releases": [asdict(row) for row in result.move_source_release_records],
        "final_state": result.final_state,
        "stage_d2_metrics": result.summary["stage_d2_metrics"],
    }
    checks = {
        "validation_pass": result.validation["status"] == "PASS" and
            all(result.validation["checks"].values()),
        "move_started": len(result.move_action_records) > 0,
        "one_release_per_move": len(result.move_action_records) ==
            len(result.move_source_release_records),
        "target_before_release_path_completed": all(
            row.release_status in {"ZERO_RELEASE", "PARTIAL_RELEASE", "FULL_RELEASE"}
            for row in result.move_action_records),
        "temporary_and_pins_drained": all(
            cache["temporary_pages"] == cache["pinned_pages"] == 0
            for cache in result.summary["per_pod_cache"]),
        "capacity_respected": all(
            cache["peak_memory_pages"] <= cache["capacity_pages"]
            for cache in result.summary["per_pod_cache"]),
        "no_extra_load_or_opportunity": len(result.request_records) ==
            len(result.opportunity_records) == len(engine.load.entries),
    }
    return {"digest": digest(projection), "checks": checks,
            "move_action_count": len(result.move_action_records),
            "release_statuses": [row.release_status for row in result.move_action_records],
            "released_pages": sum(row.actual_source_released_pages
                                  for row in result.move_action_records)}


if __name__ == "__main__":
    from src.simulator.task_main.isolation import install_legacy_import_guard
    install_legacy_import_guard()
    from src.simulator.task_main.stage_d2.config import StageD2Config
    from src.simulator.task_main.trace import TraceRequest

    root = Path(__file__).resolve().parents[2]
    trace_sha = "b8cbb061a85206d729d91cdc2981f43c9e0d99209dce588d3af5f7934408b9df"
    requests = [TraceRequest.from_record(index, {
        "timestamp": time, "input_length": 512, "output_length": 0,
        "hash_ids": [1],
    }) for index, time in enumerate((0, 1, 2))]
    cases = []
    for case_id, policy in (("oracle_move", "FUTURE_DEMAND"),
                            ("persist300_move", "PERSISTENCE")):
        config = StageD2Config(
            workload="conversation", case_id=case_id, proactive_policy=policy,
            trace_path="data/mooncake/conversation_trace.jsonl", trace_sha256=trace_sha)
        first, second = run_once(config, requests), run_once(config, requests)
        checks = dict(first["checks"])
        checks["deterministic_replay"] = first["digest"] == second["digest"]
        cases.append({"case_id": case_id, "policy": policy,
                      "status": "PASS" if all(checks.values()) else "INVALID",
                      "checks": checks, "run_1": first, "run_2": second})
    payload = {"status": "PASS" if all(row["status"] == "PASS" for row in cases)
               else "INVALID", "case_count": len(cases), "cases": cases,
               "scope": "synthetic_correctness_only_not_performance"}
    output = root/"results/task_main/stage_d2_move_ablation/smoke_validation.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    write_json(output, payload)
    print(json.dumps({"status": payload["status"], "cases": [
        {"case_id": row["case_id"], "actions": row["run_1"]["move_action_count"],
         "released_pages": row["run_1"]["released_pages"]} for row in cases]}, indent=2))
    if payload["status"] != "PASS":
        raise SystemExit(1)
