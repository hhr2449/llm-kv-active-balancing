#!/usr/bin/env python3
"""Refresh only the clarified TASK counters; does not rerun the strategy matrix."""
import json
from dataclasses import replace
from pathlib import Path

from src.simulator.config import SimulatorConfig
from src.simulator.engine import SimulatorEngine
from src.simulator.trace import load_trace


def main():
    base = SimulatorConfig.from_yaml("configs/formal/taskmain_v1.1.yaml")
    config = replace(base, routing_policy="R_REQ_KV_TASK", transfer_enabled=True,
                     proactive_enabled=False)
    for workload in ("conversation", "toolagent"):
        trace = Path(f"data/mooncake/{workload}_trace.jsonl")
        requests = [r for r in load_trace(trace, base.page_tokens, base.split_config)
                    if r.split == "DEVELOPMENT"]
        _, summary = SimulatorEngine(config).run(requests)
        path = Path(f"results/development_validation/{workload}.json")
        payload = json.loads(path.read_text())
        payload["policies"]["R_REQ_KV_TASK"]["summary"]["r_req_kv_task_activity"] = (
            summary["r_req_kv_task_activity"]
        )
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
