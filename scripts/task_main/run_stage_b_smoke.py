"""Fixed synthetic correctness smoke, never a workload performance experiment."""
import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path

from src.simulator.task_main.isolation import install_legacy_import_guard


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    install_legacy_import_guard()
    from src.simulator.task_main.config import STAGE_B_VERSION, TaskMainConfig
    from src.simulator.task_main.engine import TaskMainEngine
    from src.simulator.task_main.equivalence import execution_projection
    from src.simulator.task_main.metrics import write_outputs
    from src.simulator.task_main.trace import TraceRequest

    raw = [(0, [1, 2, 3]), (2, [1, 2, 3]), (3, [1, 2, 4]), (10, [1, 2, 3]),
           (20, [5, 6]), (60001, [5, 6]), (300001, [1, 2, 3])]
    requests = [TraceRequest.from_record(i, dict(timestamp=time, input_length=len(path)*512,
                                                output_length=0, hash_ids=path))
                for i, (time, path) in enumerate(raw)]
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output/"synthetic_input.json").write_text(json.dumps([asdict(r) for r in requests], indent=2)+"\n")
    cases = [("oracle", "FUTURE_DEMAND", 300000), ("persist60", "PERSISTENCE", 60000),
             ("persist300", "PERSISTENCE", 300000), ("recency", "RECENCY", 300000),
             ("cost", "PERSISTENCE_COST_AWARE", 300000)]
    report = {"purpose": "synthetic_correctness_only", "cases": []}
    projections = {}
    for name, policy, history in cases:
        config = TaskMainConfig(protocol_version=STAGE_B_VERSION, workload="stage_b_synthetic",
                                proactive_policy=policy, history_window_ms=history)
        folder = args.output/name
        folder.mkdir()
        (folder/"config.json").write_text(json.dumps(config.as_dict(), indent=2)+"\n")
        hashes = []
        for repeat in (1, 2):
            result = TaskMainEngine(config).run(requests)
            assert result.validation["status"] == "PASS"
            assert result.proactive_action_records
            if policy == "PERSISTENCE_COST_AWARE":
                assert all(row.cost_score > 0 for row in result.candidate_decision_records)
                assert result.summary["policy_diagnostics"]["cost_gate_rejected_count"] == 0
            output = folder/f"run_{repeat}"
            write_outputs(output, result)
            hashes.append({p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(output.iterdir())})
        assert hashes[0] == hashes[1]
        projections[name] = execution_projection(result)
        report["cases"].append({"case": name, "policy": policy, "request_count": len(requests),
            "opportunities": len(result.opportunity_records), "actions": len(result.proactive_action_records),
            "validation": result.validation, "file_sha256": hashes[0],
            "execution_projection_sha256": digest(projections[name]),
            "final_state_digest": result.summary["final_state_digest"],
            "policy_diagnostics": result.summary["policy_diagnostics"], "deterministic": True})
        print(f"PASS {name}: requests={len(requests)}, actions={len(result.proactive_action_records)}", flush=True)
    assert projections["persist300"] == projections["cost"]
    report.update(status="PASS", line5_line7_execution_equivalent=True,
                  runtime_legacy_import_guard="ACTIVE")
    (args.output/"smoke_validation.json").write_text(json.dumps(report, indent=2, sort_keys=True)+"\n")


if __name__ == "__main__":
    main()
