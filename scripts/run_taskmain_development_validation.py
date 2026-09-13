#!/usr/bin/env python3
"""Engineering-only TaskMain-v1.1 Development validation; never runs Evaluation."""
import argparse
import json
from dataclasses import asdict, replace
from pathlib import Path

from src.simulator.config import SimulatorConfig
from src.simulator.engine import SimulatorEngine
from src.simulator.trace import load_trace


def compact(results, summary):
    item = {
        "metric_scope": summary["metric_scope"], "request_count": summary["request_count"],
        "completion_latency_ms": summary["completion_latency_ms"],
        "queue_time_ms": summary["queue_time_ms"], "service_time_ms": summary["service_time_ms"],
        "request_hit_rate": sum(r.h_used_tokens > 0 for r in results) / len(results),
        "token_hit_rate": summary["cluster"]["token_hit_rate"],
        "saved_prefill_tokens": summary["h_used_tokens"],
        "gini_actual": summary["cluster"]["executed_miss_token_gini"],
        "reactive_transfer_count": summary["transfer"]["transfer_completed"],
        "reactive_wire_bytes": summary["transfer"]["wire_bytes"],
        "evictions": summary["eviction_count"], "capacity_failures": summary["capacity_admission_failures"],
        "per_pod": summary["per_pod"], "reactive": summary["transfer"],
    }
    if "proactive" in summary:
        item.update({"proactive": summary["proactive"], "network_cost": summary["network_cost"]})
    if "r_req_kv_task_activity" in summary:
        item["r_req_kv_task_activity"] = summary["r_req_kv_task_activity"]
    return item


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trace", required=True, type=Path)
    parser.add_argument("--workload", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    base = SimulatorConfig.from_yaml("configs/formal/taskmain_v1.1.yaml")
    all_requests = load_trace(args.trace, base.page_tokens, base.split_config)
    development = [r for r in all_requests if r.split == "DEVELOPMENT"]
    specs = {
        "R_AFF": replace(base, routing_policy="R_AFF", transfer_enabled=False),
        "R_REQ_KV_TASK": replace(base, routing_policy="R_REQ_KV_TASK", transfer_enabled=True),
        "TaskMain-Oracle": replace(base, routing_policy="R_AFF", transfer_enabled=False,
                                   proactive_enabled=True, proactive_strategy="FUTURE_DEMAND"),
        "Persist_h60_K10": replace(base, proactive_enabled=True, proactive_strategy="PERSIST",
                                    persistence_history_ms=60000, persistence_top_k=10),
        "Persist_h300_K10": replace(base, proactive_enabled=True, proactive_strategy="PERSIST",
                                     persistence_history_ms=300000, persistence_top_k=10),
        "Recency_q09": replace(base, proactive_enabled=True, proactive_strategy="RECENCY",
                               recency_selection_quantile=.9, recency_decay_seconds=60),
        "Cost-aware": replace(base, proactive_enabled=True, proactive_strategy="COST_AWARE",
                              persistence_history_ms=300000, persistence_top_k=10),
    }
    output = {"workload": args.workload, "metric_scope": "DEVELOPMENT",
              "evaluation_executed": False, "policies": {}}
    for name, config in specs.items():
        source = all_requests if config.proactive_enabled else development
        results, summary = SimulatorEngine(config).run(source)
        assert summary["metric_scope"] == "DEVELOPMENT"
        output["policies"][name] = {"config": asdict(config),
                                      "summary": compact(results, summary)}
    activity = {"persistence": {}, "recency": {}}
    for k in (5, 10, 20):
        config = replace(base, proactive_enabled=True, proactive_strategy="PERSIST",
                         persistence_history_ms=300000, persistence_top_k=k)
        _, summary = SimulatorEngine(config).run(all_requests)
        p = summary["proactive"]
        activity["persistence"][str(k)] = {
            "actions": p["actions_completed"], "selected_ranks": p["selected_candidate_ranks"],
            "rank_gt_1_count": p["rank_gt_1_count"],
            "top_k_exhausted_count": p["top_k_exhausted_count"],
        }
    for q in (.8, .9):
        config = replace(base, proactive_enabled=True, proactive_strategy="RECENCY",
                         recency_selection_quantile=q)
        _, summary = SimulatorEngine(config).run(all_requests)
        p = summary["proactive"]
        activity["recency"][str(q)] = {
            "actions": p["actions_completed"], "selected_hashes": [
                record["candidate_hash"] for record in p["action_records"]
            ], "positive_candidates": p["positive_candidate_count"],
            "thresholds": p["recency_thresholds"],
        }
    output["activity"] = activity
    # Determinism check on the new request-driven policy.
    first = SimulatorEngine(specs["R_REQ_KV_TASK"]).run(development)
    second = SimulatorEngine(specs["R_REQ_KV_TASK"]).run(development)
    output["deterministic_replay"] = first == second
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
