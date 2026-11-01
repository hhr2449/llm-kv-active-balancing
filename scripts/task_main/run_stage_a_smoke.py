"""Four fixed five-minute correctness smokes; no performance comparisons or tuning."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sys

from src.simulator.task_main.isolation import install_legacy_import_guard


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_smokes(output: Path) -> dict:
    install_legacy_import_guard()
    # Import execution only after the fail-fast guard is active.
    from src.simulator.task_main.config import TaskMainConfig
    from src.simulator.task_main.engine import TaskMainEngine
    from src.simulator.task_main.metrics import write_outputs
    from src.simulator.task_main.trace import read_trace

    repo = Path(__file__).resolve().parents[2]
    output.mkdir(parents=True, exist_ok=False)
    manifest = {"purpose": "correctness_only_not_performance", "window_ms": [0, 300000],
                "window_interval": "[0,300000)", "cases": []}
    for workload in ("conversation", "toolagent"):
        trace_path = repo / f"data/mooncake/{workload}_trace.jsonl"
        requests = [request for request in read_trace(trace_path) if request.arrival_ms < 300000]
        subset_hash = hashlib.sha256(json.dumps(
            [asdict(request) for request in requests], sort_keys=True,
            separators=(",", ":"), allow_nan=False,
        ).encode()).hexdigest()
        for policy in ("R_AFF", "R_REQ_KV_TASK"):
            case_dir = output / f"{workload}_{policy.lower()}"
            case_dir.mkdir()
            config = TaskMainConfig(workload=workload, routing_policy=policy)
            (case_dir / "config.json").write_text(json.dumps(config.as_dict(), indent=2) + "\n")
            hashes = []
            first_summary = None
            first_validation = None
            for repetition in (1, 2):
                engine = TaskMainEngine(config)
                result = engine.run(requests)
                result.summary["provenance"]["smoke"] = {
                    "purpose": "correctness_only_not_performance", "window_ms": [0, 300000],
                    "window_interval": "[0,300000)", "input_request_count": len(requests),
                    "source_trace_sha256": sha(trace_path), "subset_sha256": subset_hash,
                    "runtime_legacy_import_guard": "ACTIVE",
                    "loaded_simulator_modules": sorted(
                        name for name in sys.modules if name.startswith("src.simulator")
                    ),
                }
                result.validation["checks"]["processed_equals_smoke_input"] = (
                    result.summary["request_count"] == len(requests)
                )
                if not all(result.validation["checks"].values()):
                    raise AssertionError(result.validation)
                run_dir = case_dir / f"run_{repetition}"
                write_outputs(run_dir, result)
                hashes.append({path.name: sha(path) for path in sorted(run_dir.iterdir())})
                if repetition == 1:
                    first_summary, first_validation = result.summary, result.validation
            if hashes[0] != hashes[1]:
                raise AssertionError(f"deterministic replay mismatch: {case_dir}")
            case = {
                "workload": workload, "routing_policy": policy, "config": config.as_dict(),
                "input_requests": len(requests), "correctness": first_validation,
                "sanity": first_summary["sanity"],
                "cache_admission_skip_count": first_summary["cache_admission_skips"],
                "per_pod_cache": first_summary["per_pod_cache"],
                "capacity_violations": 0,
                "started_reactive_transfers": first_summary["transfer_count"],
                "completed_reactive_transfers": first_summary["transfer_count"],
                "final_temporary_pages": sum(c["temporary_pages"] for c in first_summary["per_pod_cache"]),
                "final_pin_pages": sum(c["pinned_pages"] for c in first_summary["per_pod_cache"]),
                "deterministic_replay": "PASS", "file_sha256": hashes[0],
                "source_trace_sha256": sha(trace_path), "subset_sha256": subset_hash,
            }
            manifest["cases"].append(case)
            (output / "smoke_validation.json").write_text(
                json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False) + "\n")
            print(f"PASS {workload} {policy}: requests={len(requests)}, "
                  f"copies={first_summary['transfer_count']}, deterministic=PASS", flush=True)
    manifest["status"] = "PASS"
    (output / "smoke_validation.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False) + "\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    run_smokes(args.output)


if __name__ == "__main__":
    main()
