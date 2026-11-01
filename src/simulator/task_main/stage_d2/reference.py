from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .config import behavior_projection


REFERENCE_LINE = {"oracle_move": 3, "persist300_move": 5}


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024*1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_copy_references(root, configs):
    root = Path(root)
    formal = root/"results/task_main/formal"
    manifest = json.loads((formal/"validation_manifest.json").read_text())
    checkpoint = json.loads((formal/"checkpoint.json").read_text())
    progress = json.loads((formal/"progress.json").read_text())
    provenance = json.loads((formal/"source_provenance.json").read_text())
    by_case = {row["case"]: row for row in manifest["cases"]}
    file_identity = {}
    for relative, expected in provenance["working_tree_sha256"].items():
        path = root/relative
        file_identity[relative] = path.is_file() and file_sha256(path) == expected
    checks = {
        "formal_manifest_pass": manifest.get("status") == "PASS",
        "formal_checkpoint_pass": checkpoint.get("status") == "PASS" and
            len(checkpoint.get("completed", {})) == 28,
        "formal_progress_pass": progress.get("status") == "PASS" and
            progress.get("completed_double_replay_cases") == 14,
        "formal_source_files_unchanged": all(file_identity.values()),
        "exact_four_d2_configs": len(configs) == 4,
    }
    references = {}
    for config in configs:
        line = REFERENCE_LINE[config.case_id]
        key = f"{config.workload}_line_{line}"
        formal_case = by_case[key]
        persisted = json.loads((formal/key/"config.json").read_text())
        parameter_match = behavior_projection(config) == behavior_projection(persisted)
        case_checks = {
            "formal_case_pass": formal_case.get("status") == "PASS" and
                formal_case.get("deterministic") is True,
            "manifest_config_matches_persisted": formal_case["config"] == persisted,
            "all_behavior_parameters_match": parameter_match,
            "copy_reference_action": persisted["action"] == "COPY",
            "move_case_action": config.action == "MOVE",
            "target_selection_frozen": json.loads(
                (formal/key/"run_1/summary.json").read_text())["provenance"][
                    "target_selection"] == "capacity_feasible_targets_then_load_pod_id",
        }
        checks[f"{config.workload}_{config.case_id}"] = all(case_checks.values())
        references[(config.workload, config.case_id)] = {
            "formal_case": key, "line_id": line, "checks": case_checks,
            "summary_path": str((formal/key/"run_1/summary.json").relative_to(root)),
            "formal_output_sha256": formal_case["output_sha256"],
        }
    status = "PASS" if all(checks.values()) else "INVALID"
    if status != "PASS":
        raise ValueError(f"D2 COPY reference provenance blocker: {checks}")
    return {
        "status": status, "checks": checks, "references": {
            f"{key[0]}__{key[1]}": value for key, value in references.items()},
        "formal_run_id": "formal:5ddeca8e0c768e04",
        "formal_manifest_sha256": file_sha256(formal/"validation_manifest.json"),
        "formal_source_provenance_sha256": file_sha256(formal/"source_provenance.json"),
        "frozen_source_file_count": len(file_identity),
    }
