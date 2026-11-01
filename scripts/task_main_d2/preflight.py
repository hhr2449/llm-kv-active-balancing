"""Validate the frozen Stage D2 matrix and COPY references without replay."""
from __future__ import annotations

import json
from pathlib import Path

from scripts.task_main_d2.run_stage_d2 import source_provenance, write_json
from src.simulator.task_main.stage_d2.config import StageD2Config
from src.simulator.task_main.stage_d2.isolation import stage_d2_dependency_audit
from src.simulator.task_main.stage_d2.reference import validate_copy_references


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[2]
    configs = [StageD2Config.from_yaml(path) for path in
               sorted((root/"configs/task_main/stage_d2").rglob("*.yaml"))]
    reference = validate_copy_references(root, configs)
    audit = stage_d2_dependency_audit()
    pairs = {(row.workload, row.case_id) for row in configs}
    checks = {
        "exact_four_move_cases": len(configs) == 4,
        "unique_workload_case_pairs": len(pairs) == 4,
        "exact_eight_double_replays": 2*len(configs) == 8,
        "both_workloads": {row.workload for row in configs} == {"conversation", "toolagent"},
        "fixed_triggers": {row.case_id for row in configs} == {
            "oracle_move", "persist300_move"},
        "all_move": all(row.action == "MOVE" for row in configs),
        "copy_reference_identity": reference["status"] == "PASS",
        "dependency_isolation": audit["status"] == "PASS",
        "formal_scope": all(
            (row.num_pods, row.capacity_pages, row.future_window_ms,
             row.history_window_ms, row.shortlist_k, row.theta,
             row.evaluation_start_ms, row.evaluation_end_ms) ==
            (4, 585, 300000, 300000, 10, 2.0, 1500000, 2700000)
            for row in configs),
    }
    payload = {
        "status": "PASS" if all(checks.values()) else "INVALID",
        "checks": checks,
        "case_count": 4,
        "replay_count": 8,
        "worker_limit": 8,
        "recommended_workers": 8,
        "estimated_wall_time_hours": {"lower": 3, "upper": 6},
        "estimate_basis": (
            "All eight independent MOVE replays run concurrently within the 10-core cgroup. "
            "D2 now reuses the exact leaf-LRU Temporary eviction sequence per Cache version "
            "and maintains exact monotonic Persistence window counts. Microbenchmarks measured "
            "about 82x and 3x on those operations, while Oracle remains sequential and is the "
            "expected wall-time limiter. The range remains conservative because MOVE can alter "
            "the closed-loop candidate/action sequence."
        ),
        "estimated_additional_disk_gib": {"lower": 14, "upper": 20},
        "copy_reference": reference,
        "dependency_audit": audit,
        "stage_d2_source": source_provenance(root),
        "matrix": [row.as_dict() for row in sorted(
            configs, key=lambda row: (row.workload, row.case_id))],
    }
    output = root/"results/task_main/stage_d2_move_ablation/preflight.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    write_json(output, payload)
    print(json.dumps({
        "status": payload["status"], "cases": 4, "replays": 8,
        "estimated_wall_time_hours": payload["estimated_wall_time_hours"],
        "estimated_additional_disk_gib": payload["estimated_additional_disk_gib"],
    }, indent=2))
    if payload["status"] != "PASS":
        raise SystemExit(1)
