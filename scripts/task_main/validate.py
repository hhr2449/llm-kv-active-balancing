"""Read-only artifact identity and hard-gate verification for a completed matrix."""
import argparse
import hashlib
import json
from pathlib import Path


def validate_artifacts(folder):
    manifest = json.loads((folder/"validation_manifest.json").read_text())
    checks = dict(matrix_complete=len(manifest["cases"]) == 14,
                  status=manifest["status"] == "PASS",
                  gate_a=all(g["status"] == "PASS" for g in manifest["gate_a"].values()) and len(manifest["gate_a"]) == 2)
    from src.simulator.task_main.config import TaskMainConfig
    from src.simulator.task_main.metrics import read_outputs
    from src.simulator.task_main.final_metrics import finalize_metrics
    from src.simulator.task_main.equivalence import projection_digest
    from src.simulator.task_main.validation import gate_a_evidence, compare_gate_a, validate_metrics
    evidence = {}
    for case in manifest["cases"]:
        checks[case["case"]] = (case["status"] == "PASS" and case["deterministic"] and
            all(case["validation"]["checks"].values()) and all(
                hashlib.sha256((folder/case["case"]/f"run_{repeat}"/filename).read_bytes()).hexdigest() == expected
                for repeat in (1, 2) for filename, expected in case["output_sha256"].items()))
        config = TaskMainConfig(**case["config"])
        result = read_outputs(folder/case["case"]/"run_1")
        observed = gate_a_evidence(result)
        evidence[(config.workload, config.line_id)] = (config, observed)
        checks[case["case"]+"_recomputed"] = (
            all(validate_metrics(result, config).values()) and
            projection_digest(finalize_metrics(result, config)) == projection_digest(result.summary["final_metrics"]) and
            observed["execution_projection_sha256"] == case["execution_projection_sha256"] and
            observed["final_logical_state_digest"] == case["final_logical_state_digest"])
    for workload in ("conversation", "toolagent"):
        if (workload, 5) in evidence and (workload, 7) in evidence:
            a, left = evidence[(workload, 5)]
            b, right = evidence[(workload, 7)]
            checks[workload+"_gate_a_recomputed"] = compare_gate_a(a,b,left,right)["status"] == "PASS"
        else:
            checks[workload+"_gate_a_recomputed"] = False
    return dict(status="PASS" if all(checks.values()) else "INVALID", checks=checks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    args = parser.parse_args()
    result = validate_artifacts(args.input)
    print(json.dumps(result, indent=2))
    if result["status"] != "PASS":
        raise SystemExit(1)
