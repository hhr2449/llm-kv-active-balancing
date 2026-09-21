"""Validate the fixed Stage D1 matrix and frozen formal references without replay."""
from __future__ import annotations

import json
from pathlib import Path

from scripts.task_main_d1.run_stage_d1 import source_provenance, validate_formal_reference, write_json
from src.simulator.task_main.stage_d1.config import StageD1Config
from src.simulator.task_main.stage_d1.isolation import stage_d1_dependency_audit


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[2]
    configs = [StageD1Config.from_yaml(path)
               for path in (root/"configs/task_main/stage_d1").rglob("*.yaml")]
    formal = validate_formal_reference(root)
    audit = stage_d1_dependency_audit()
    checks = {
        "exact_32_cases": len(configs) == 32,
        "unique_cases": len({(row.workload, row.case_id) for row in configs}) == 32,
        "exact_64_double_replays": 2*len(configs) == 64,
        "formal_reference_identity": formal["status"] == "PASS",
        "dependency_isolation": audit["status"] == "PASS",
        "all_copy": all(row.action == "COPY" for row in configs),
        "no_cost_aware_replay": all(row.proactive_policy != "PERSISTENCE_COST_AWARE" for row in configs),
        "w_common_support": all(
            row.family != "ORACLE_W" or
            (row.evaluation_start_ms, row.evaluation_end_ms) == (600000, 1500000)
            for row in configs),
        "n2_per_pod_capacity_585": all(
            row.family != "N" or (row.num_pods == 2 and row.capacity_pages == 585)
            for row in configs),
    }
    payload = {
        "status": "PASS" if all(checks.values()) else "INVALID",
        "checks": checks, "formal_reference": formal,
        "stage_d1_source": source_provenance(root),
        "case_count": 32, "replay_count": 64, "worker_limit": 24,
        "recommended_workers": 20,
        "estimated_wall_time_hours": {"lower": 7, "upper": 12},
        "estimate_basis": (
            "formal 28-replay run took 18418s with 10 workers; D1 now uses 20 independent "
            "workers, longest-expected-job-first scheduling, cached candidate ordering, and "
            "an equivalent O(1) full-Prefix residency test. The range remains conservative "
            "because ToolAgent K=20 and W sensitivity have not yet completed at full scale"
        ),
        "estimated_additional_disk_gib": {"lower": 40, "upper": 55},
        "matrix": [row.as_dict() for row in sorted(configs, key=lambda row: (
            row.workload, row.family, row.case_id))],
    }
    output = root/"results/task_main/stage_d1_sensitivity/preflight.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    write_json(output, payload)
    print(json.dumps({"status": payload["status"], "cases": 32, "replays": 64,
                      "estimated_wall_time_hours": payload["estimated_wall_time_hours"]}, indent=2))
    if payload["status"] != "PASS":
        raise SystemExit(1)
