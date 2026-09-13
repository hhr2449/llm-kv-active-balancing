#!/usr/bin/env python3
"""Run the frozen Development-only O1 mechanism decomposition protocol."""
import argparse
import hashlib
import json
import subprocess
from dataclasses import asdict, replace
from pathlib import Path

from src.simulator.config import B0_IMPL_VERSION, METRIC_VERSION, SimulatorConfig
from src.simulator.engine import SimulatorEngine
from src.simulator.trace import load_trace


def compact(summary):
    result = {
        "metric_scope": summary["metric_scope"],
        "request_count": summary["request_count"],
        "completion_latency_ms": summary["completion_latency_ms"],
        "queue_time_ms": summary["queue_time_ms"],
        "service_time_ms": summary["service_time_ms"],
        "token_hit_rate": summary["cluster"]["token_hit_rate"],
        "h_route_tokens": summary["h_route_tokens"],
        "h_used_tokens": summary["h_used_tokens"],
        "eviction_count": summary["eviction_count"],
        "busy_while_other_idle_ms": summary["pressure"]["busy_while_other_idle_ms"],
        "load_gap_ms": summary["pressure"]["load_gap_ms"],
        "reactive": summary["transfer"],
    }
    if "proactive" in summary:
        result["proactive"] = summary["proactive"]
        result["network_cost"] = summary["network_cost"]
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trace", required=True, type=Path)
    parser.add_argument("--workload", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    base = SimulatorConfig.from_yaml("configs/formal/o1_development_nominal.yaml")
    base = replace(base, oracle_horizon_ms=60000, trigger_period_ms=6000,
                   trigger_phase_ms=0, max_proactive_actions_per_tick=1,
                   proactive_fanout_targets=1, proactive_no_evict_admission=False,
                   proactive_trigger_latest_ms=None)
    requests = load_trace(args.trace, base.page_tokens, base.split_config)
    specs = {}
    def add(name, **changes):
        config = replace(base, **changes)
        _, summary = SimulatorEngine(config).run(requests)
        if summary["metric_scope"] != "DEVELOPMENT":
            raise AssertionError("non-Development O1 output forbidden")
        specs[name] = {"config": asdict(config), "summary": compact(summary)}

    add("D1_B0_INF", proactive_enabled=False, cache_capacity_pages=None)
    add("D1_O1_INF", cache_capacity_pages=None)
    add("D2_B0_5K", proactive_enabled=False, cache_capacity_pages=5000)
    add("D2_O1_NOMINAL_5K", cache_capacity_pages=5000)
    add("D2_O1_NO_EVICT_ADMISSION_5K", cache_capacity_pages=5000,
        proactive_no_evict_admission=True)
    add("D3_B0_10K", proactive_enabled=False, cache_capacity_pages=10000,
        max_proactive_actions_per_tick=2)
    add("D3_O1_SINGLE_TARGET_10K", cache_capacity_pages=10000,
        max_proactive_actions_per_tick=2, proactive_fanout_targets=1)
    add("D3_O1_FANOUT2_10K", cache_capacity_pages=10000,
        max_proactive_actions_per_tick=2, proactive_fanout_targets=2)
    for horizon in (30000, 60000, 300000):
        add(f"D4_COMMON_SUPPORT_H{horizon}", cache_capacity_pages=10000,
            oracle_horizon_ms=horizon, proactive_trigger_latest_ms=600000)
    payload = {
        "provenance": {
            "git_commit": subprocess.run(["git", "rev-parse", "HEAD"], check=True,
                                         capture_output=True, text=True).stdout.strip(),
            "b0_impl_version": B0_IMPL_VERSION, "metric_version": METRIC_VERSION,
            "workload": args.workload, "trace_path": str(args.trace.resolve()),
            "trace_sha256": hashlib.sha256(args.trace.read_bytes()).hexdigest(),
            "scope": "DEVELOPMENT", "evaluation_executed": False,
            "protocol": "o1_mechanism_diagnostics_v1",
        },
        "runs": specs,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
