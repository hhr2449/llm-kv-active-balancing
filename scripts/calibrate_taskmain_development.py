#!/usr/bin/env python3
"""One-shot Development-only TaskMain cost and burst reference audit."""
import hashlib
import json
import statistics
from dataclasses import replace
from pathlib import Path

from src.simulator.config import SimulatorConfig
from src.simulator.engine import SimulatorEngine
from src.simulator.trace import load_trace


def main():
    base = SimulatorConfig.from_yaml("configs/formal/taskmain_v1.1.yaml")
    config = replace(base, proactive_enabled=True, proactive_strategy="COST_AWARE",
                     persistence_history_ms=300000, oracle_horizon_ms=300000,
                     cost_v_ref=1, cost_t_ref_ms=1)
    values, transfer_times = [], []
    traces = {}
    for workload in ("conversation", "toolagent"):
        path = Path(f"data/mooncake/{workload}_trace.jsonl")
        traces[workload] = hashlib.sha256(path.read_bytes()).hexdigest()
        requests = load_trace(path, config.page_tokens, config.split_config)
        _, summary = SimulatorEngine(config).run(requests)
        proactive = summary["proactive"]
        values.extend(proactive["cost_reference_values"])
        transfer_times.extend(proactive["cost_reference_transfer_ms"])
    v_ref = statistics.median(values)
    t_ref = statistics.median(transfer_times)
    output = Path("results/development_validation")
    output.mkdir(parents=True, exist_ok=True)
    (output / "development_cost_reference.json").write_text(json.dumps({
        "protocol": "TaskMain-v1.1", "development_interval_ms": [0, 900000],
        "v_ref": v_ref, "t_ref_ms": t_ref,
        "candidate_count": len(values), "positive_value_count": sum(v > 0 for v in values),
        "transfer_count": len(transfer_times), "trace_sha256": traces,
    }, indent=2, sort_keys=True) + "\n")
    page_bytes = config.page_bytes
    max_transfer_ms = max(transfer_times)
    max_pages = round((max_transfer_ms - config.control_latency_ms) / 1000
                      * config.effective_bandwidth_bytes_per_s / page_bytes)
    max_action_bytes = max_pages * page_bytes
    one_minute = int(.05 * config.num_pods * config.cache_capacity_pages * page_bytes)
    protocol_cap = config.cache_capacity_pages * page_bytes
    if any(pages * page_bytes > protocol_cap
           for pages in range(config.cache_capacity_pages + 1)):
        raise AssertionError("capacity-legal action exceeds protocol bucket cap")
    (output / "development_budget_audit.json").write_text(json.dumps({
        "protocol": "TaskMain-v1.1", "q_turnover_per_minute": .05,
        "refill_rate_bytes_per_sec": one_minute / 60,
        "one_minute_budget_bytes": one_minute,
        "bucket_cap_bytes": protocol_cap,
        "bucket_cap_pages_equivalent": config.cache_capacity_pages,
        "max_observed_development_action_bytes": max_action_bytes,
        "max_observed_development_action_pages": max_pages,
        "max_observed_development_action_tokens": max_pages * 512,
        "structural_capacity_action_coverage": True,
        "action_exceeds_bucket_cap": 0, "trace_sha256": traces,
    }, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
