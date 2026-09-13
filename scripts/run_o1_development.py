#!/usr/bin/env python3
"""Finite, preregistered Development-only O1 headroom mapping."""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from dataclasses import asdict, replace
from pathlib import Path

from src.simulator.config import B0_IMPL_VERSION, METRIC_VERSION, SimulatorConfig
from src.simulator.engine import SimulatorEngine
from src.simulator.trace import load_trace


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trace", required=True, type=Path)
    parser.add_argument("--workload", required=True)
    parser.add_argument("--capacity", required=True, type=int, choices=(5000, 10000))
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    template = SimulatorConfig.from_yaml("configs/formal/o1_development_nominal.yaml")
    requests = load_trace(args.trace, template.page_tokens, template.split_config)
    digest = hashlib.sha256(args.trace.read_bytes()).hexdigest()
    commit = subprocess.run(["git", "rev-parse", "HEAD"], check=True,
                            capture_output=True, text=True).stdout.strip()
    args.output.mkdir(parents=True, exist_ok=True)
    records = []
    for horizon in (30000, 60000, 300000):
        for period in (3000, 6000, 12000):
            for phase in (0, period / 3, 2 * period / 3):
                config = replace(
                    template, cache_capacity_pages=args.capacity,
                    oracle_horizon_ms=horizon, trigger_period_ms=period,
                    trigger_phase_ms=phase,
                )
                _, summary = SimulatorEngine(config).run(requests)
                if summary["metric_scope"] != "DEVELOPMENT":
                    raise AssertionError("O1 emitted a non-Development summary")
                record = {
                    "workload": args.workload, "capacity_pages": args.capacity,
                    "horizon_ms": horizon, "period_ms": period, "phase_ms": phase,
                    "request_count": summary["request_count"],
                    "token_hit_rate": summary["cluster"]["token_hit_rate"],
                    "completion_latency_ms": summary["completion_latency_ms"],
                    "queue_time_ms": summary["queue_time_ms"],
                    "service_time_ms": summary["service_time_ms"],
                    "h_route_tokens": summary["h_route_tokens"],
                    "h_used_tokens": summary["h_used_tokens"],
                    "load_gap_ms": summary["pressure"]["load_gap_ms"],
                    "busy_while_other_idle_ms": summary["pressure"]["busy_while_other_idle_ms"],
                    "eviction_count": summary["eviction_count"],
                    "reactive": summary["transfer"],
                    "proactive": summary["proactive"],
                    "network_cost": summary["network_cost"],
                }
                records.append(record)
                name = f"h{horizon}_p{period}_phase{int(phase)}.json"
                (args.output / name).write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    provenance = {
        "git_commit": commit, "b0_impl_version": B0_IMPL_VERSION,
        "metric_version": METRIC_VERSION,
        "trace_path": str(args.trace.resolve()), "trace_sha256": digest,
        "split": template.split_config.as_dict(), "summary_split": "DEVELOPMENT",
        "oracle_policy": "Future-Demand Reference",
        "oracle_visibility_end_ms": template.oracle_visibility_end_ms,
        "nominal_budget": {"max_actions_per_tick": template.max_proactive_actions_per_tick,
                           "byte_rate": template.proactive_byte_rate,
                           "burst_bytes": template.proactive_burst_bytes},
        "baseline_config": asdict(template),
    }
    (args.output / "manifest.json").write_text(json.dumps({
        "provenance": provenance, "runs": len(records)
    }, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
