#!/usr/bin/env python3
"""Small preregistered LOW/MEDIUM and action-cap Development comparison."""
import argparse
import json
from dataclasses import replace
from pathlib import Path

from src.simulator.config import SimulatorConfig
from src.simulator.engine import SimulatorEngine
from src.simulator.trace import load_trace


TIERS = {
    "LOW": (25_000_000, 67_108_864),
    "MEDIUM": (100_000_000, 268_435_456),
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trace", type=Path, required=True)
    parser.add_argument("--workload", required=True)
    parser.add_argument("--capacity", type=int, required=True, choices=(5000, 10000))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    base = SimulatorConfig.from_yaml("configs/formal/o1_development_nominal.yaml")
    requests = load_trace(args.trace, base.page_tokens, base.split_config)
    records = []
    for tier, (rate, burst) in TIERS.items():
        for action_cap in (1, 2):
            config = replace(base, cache_capacity_pages=args.capacity,
                             oracle_horizon_ms=60000, trigger_period_ms=6000,
                             trigger_phase_ms=0, max_proactive_actions_per_tick=action_cap,
                             proactive_byte_rate=rate, proactive_burst_bytes=burst)
            _, summary = SimulatorEngine(config).run(requests)
            records.append({
                "workload": args.workload, "capacity_pages": args.capacity,
                "tier": tier, "action_cap": action_cap,
                "completion_latency_ms": summary["completion_latency_ms"],
                "token_hit_rate": summary["cluster"]["token_hit_rate"],
                "proactive": summary["proactive"], "network_cost": summary["network_cost"],
            })
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(records, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
