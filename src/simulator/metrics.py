from __future__ import annotations

import json
import math
import csv
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import fmean
from typing import Sequence

from .pod import Pod
from .pressure import pressure_diagnostics


@dataclass
class RequestResult:
    request_id: int
    pod_id: int
    arrival_ms: float
    service_start_ms: float
    service_done_ms: float
    input_tokens: int
    output_tokens: int
    h_route_pages: int
    h_route_tokens: int
    h_used_pages: int
    h_used_tokens: int
    miss_tokens: int
    queue_time_ms: float
    service_time_ms: float
    completion_latency_ms: float
    cache_hit_pages: int
    cache_inserted_pages: int
    route_estimated_service_ms: float
    estimate_error_ms: float
    estimate_actual_ratio: float
    p2p_assisted: bool
    source_pod_id: int | None
    target_pod_id: int | None
    request_transfer_wait_ms: float
    ticket_target_estimated_service_ms: float | None
    split: str | None = None
    ticket_created: bool = False
    ticket_state: str | None = None
    wire_pages: int = 0
    wire_bytes: int = 0
    ticket_admission_wait_ms: float | None = None
    reactive_ect_selected_plan_type: str | None = None
    reactive_ect_selected_source: int | None = None
    reactive_ect_selected_target: int | None = None
    reactive_ect_selected_ect_ms: float | None = None
    reactive_ect_best_direct_target: int | None = None
    reactive_ect_best_direct_ect_ms: float | None = None
    reactive_ect_best_copy_source: int | None = None
    reactive_ect_best_copy_target: int | None = None
    reactive_ect_best_copy_ect_ms: float | None = None
    reactive_ect_estimated_copy_advantage_ms: float | None = None
    reactive_ect_final_plan_type: str | None = None
    reactive_ect_final_source: int | None = None
    reactive_ect_final_target: int | None = None
    reactive_ect_final_ect_ms: float | None = None
    reactive_ect_copy_selected: bool = False
    reactive_ect_copy_completed: bool = False
    reactive_ect_wire_bytes: int = 0
    reactive_ect_current_hit_tokens: int | None = None
    reactive_ect_post_copy_hit_tokens: int | None = None
    reactive_ect_direct_candidate_count: int = 0
    reactive_ect_copy_candidate_count: int = 0
    reactive_ect_replan_count: int = 0
    reactive_ect_fallback: bool = False
    reactive_ect_endpoint_blocked: bool = False
    reactive_ect_capacity_blocked: bool = False
    b0_transfer_considered: bool = False
    b0_gate_outcome: str | None = None
    b0_replan_count: int = 0
    b0_endpoint_blocked: bool = False
    b0_capacity_blocked: bool = False


def _percentile(values: Sequence[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    low, high = math.floor(position), math.ceil(position)
    if low == high:
        return float(ordered[low])
    return ordered[low] * (high - position) + ordered[high] * (position - low)


def distribution(values: Sequence[float]) -> dict[str, float | None]:
    return {
        "mean": fmean(values) if values else None,
        "p50": _percentile(values, 0.50),
        "p95": _percentile(values, 0.95),
        "p99": _percentile(values, 0.99),
    }


def diagnostic_distribution(values: Sequence[float]) -> dict[str, float | None]:
    result = distribution(values)
    result["p90"] = _percentile(values, 0.90)
    return result


def gini(values: Sequence[float]) -> float:
    """Population Gini; an all-zero population is defined as zero."""
    if not values:
        return 0.0
    if any(value < 0 for value in values):
        raise ValueError("Gini inputs must be non-negative")
    total = sum(values)
    if total == 0:
        return 0.0
    ordered = sorted(values)
    n = len(ordered)
    weighted = sum((index + 1) * value for index, value in enumerate(ordered))
    return 2.0 * weighted / (n * total) - (n + 1.0) / n


def _pod_summary(pod: Pod, results: Sequence[RequestResult], duration_ms: float,
                 evaluation_range: tuple[float, float] | None = None) -> dict:
    own = [result for result in results if result.pod_id == pod.pod_id]
    if evaluation_range is None:
        busy_time = pod.busy_time_ms
    else:
        lo, hi = evaluation_range
        busy_time = sum(max(0.0, min(r.service_done_ms, hi) - max(r.service_start_ms, lo))
                        for r in results if r.pod_id == pod.pod_id)
    return {
        "pod_id": pod.pod_id,
        "routed_requests": len(own),
        "completed_requests": len(own),
        "input_tokens": sum(result.input_tokens for result in own),
        "output_tokens": sum(result.output_tokens for result in own),
        "h_route_pages": sum(result.h_route_pages for result in own),
        "h_route_tokens": sum(result.h_route_tokens for result in own),
        "h_used_pages": sum(result.h_used_pages for result in own),
        "h_used_tokens": sum(result.h_used_tokens for result in own),
        "miss_tokens": sum(result.miss_tokens for result in own),
        "queue_time_ms": distribution([result.queue_time_ms for result in own]),
        "service_time_ms": distribution([result.service_time_ms for result in own]),
        "completion_latency_ms": distribution(
            [result.completion_latency_ms for result in own]
        ),
        "eviction_count": pod.cache.eviction_count,
        "evicted_pages": pod.cache.evicted_pages,
        "resident_pages": pod.cache.resident_pages,
        "peak_resident_pages": pod.cache.peak_resident_pages,
        "peak_active_private_pages": pod.cache.peak_active_private_pages,
        "transfer_temporary_pages": pod.cache.transfer_temporary_pages,
        "peak_transfer_temporary_pages": pod.cache.peak_transfer_temporary_pages,
        "peak_memory_used_pages": pod.cache.peak_memory_used_pages,
        "capacity_admission_failures": pod.cache.capacity_admission_failures,
        "busy_time_ms": busy_time,
        "busy_fraction": busy_time / duration_ms if duration_ms > 0 else 0.0,
        "max_queue_length": pod.max_queue_length,
        "time_weighted_load_ms": pod.load_area_ms2 / duration_ms if duration_ms > 0 else 0.0,
        "max_load_ms": pod.max_load_ms,
        "final_load_ms": pod.load_ms(float("inf")),
        "endpoint_busy_time_ms": pod.endpoint_busy_time_ms,
        "endpoint_utilization": pod.endpoint_busy_time_ms / duration_ms if duration_ms > 0 else 0.0,
        "mean_remote_pressure_ms": pod.remote_pressure_area_ms2 / duration_ms if duration_ms > 0 else 0.0,
        "peak_remote_pressure_ms": pod.peak_remote_pressure_ms,
        "final_remote_pressure_ms": pod.committed_remote_pressure_ms,
    }


def summarize(results: Sequence[RequestResult], pods: Sequence[Pod],
              duration_ms: float, routing_policy: str, transfer_stats: dict,
              pressure_intervals: Sequence[dict], config) -> dict:
    all_results = list(results)
    split_counts = {name: sum(r.split == name for r in all_results) for name in
                    ("DEVELOPMENT", "WARMUP", "EVALUATION", "OBSERVATION_TAIL")}
    evaluation_range = None
    if config.split_config is not None:
        scope = config.summary_split
        results = [r for r in all_results if r.split == scope]
        ranges = {
            "DEVELOPMENT": (config.split_config.development_start_ms,
                            config.split_config.warmup_start_ms),
            "WARMUP": (config.split_config.warmup_start_ms,
                       config.split_config.evaluation_start_ms),
            "EVALUATION": (config.split_config.evaluation_start_ms,
                           config.split_config.observation_tail_start_ms),
        }
        if scope == "OBSERVATION_TAIL":
            end = max((r.service_done_ms for r in all_results),
                      default=config.split_config.observation_tail_start_ms)
            evaluation_range = (config.split_config.observation_tail_start_ms, end)
        else:
            evaluation_range = ranges[scope]
        duration_ms = evaluation_range[1] - evaluation_range[0]
    pod_metrics = [_pod_summary(pod, results, duration_ms, evaluation_range) for pod in pods]
    request_counts = [item["routed_requests"] for item in pod_metrics]
    input_tokens_per_pod = [item["input_tokens"] for item in pod_metrics]
    miss_tokens_per_pod = [item["miss_tokens"] for item in pod_metrics]
    busy_times = [item["busy_time_ms"] for item in pod_metrics]
    time_weighted_loads = [item["time_weighted_load_ms"] for item in pod_metrics]
    total_input = sum(result.input_tokens for result in results)
    total_hit = sum(result.h_used_tokens for result in results)
    cluster = {
        "request_count_gini": gini(request_counts),
        "input_token_gini": gini(input_tokens_per_pod),
        "executed_miss_token_gini": gini(miss_tokens_per_pod),
        "busy_time_gini": gini(busy_times),
        "time_weighted_load_ms_per_pod": time_weighted_loads,
        "mean_time_weighted_load_ms": fmean(time_weighted_loads),
        "max_time_weighted_load_ms": max(time_weighted_loads, default=0.0),
        "max_observed_load_ms": max(
            (item["max_load_ms"] for item in pod_metrics), default=0.0
        ),
        "max_queue_length": max(
            (item["max_queue_length"] for item in pod_metrics), default=0
        ),
        "h_route_pages": sum(result.h_route_pages for result in results),
        "h_route_tokens": sum(result.h_route_tokens for result in results),
        "h_used_pages": sum(result.h_used_pages for result in results),
        "h_used_tokens": total_hit,
        "token_hit_rate": total_hit / total_input if total_input else 0.0,
        "latency_ms": distribution(
            [result.completion_latency_ms for result in results]
        ),
    }
    estimate_groups = {}
    groups = {
        "all": list(results),
        "p2p": [r for r in results if r.p2p_assisted],
        "non_p2p": [r for r in results if not r.p2p_assisted],
    }
    for name, group in groups.items():
        estimate_groups[name] = {
            "estimate_error_ms": diagnostic_distribution([r.estimate_error_ms for r in group]),
            "estimate_actual_ratio": {
                key: value for key, value in diagnostic_distribution(
                    [r.estimate_actual_ratio for r in group]
                ).items() if key in {"p50", "p90", "p95"}
            },
        }
    source_target = {}
    for result in results:
        if result.p2p_assisted:
            key = f"{result.source_pod_id}->{result.target_pod_id}"
            source_target.setdefault(key, []).append(result)
    estimate_groups["source_target"] = {
        key: diagnostic_distribution([r.estimate_error_ms for r in group])
        for key, group in sorted(source_target.items())
    }
    p2p_target_errors = [
        r.ticket_target_estimated_service_ms - r.service_time_ms
        for r in results if r.p2p_assisted and r.ticket_target_estimated_service_ms is not None
    ]
    estimate_groups["p2p_target_estimate_error_ms"] = diagnostic_distribution(
        p2p_target_errors
    )
    transfer_summary = {k: v for k, v in transfer_stats.items()
                        if k not in {"transfer_latencies_ms", "ticket_admission_waits_ms",
                                     "ticket_target_estimate_errors_ms"}}
    transfer_summary["transfer_latency_ms"] = diagnostic_distribution(
        transfer_stats["transfer_latencies_ms"]
    )
    transfer_summary["duplicate_wire_fraction"] = (
        transfer_stats["duplicate_wire_pages"] / transfer_stats["wire_pages"]
        if transfer_stats["wire_pages"] else 0.0
    )
    transfer_summary["request_transfer_wait_ms"] = diagnostic_distribution(
        [r.request_transfer_wait_ms for r in results if r.p2p_assisted]
    )
    transfer_summary["ticket_admission_wait_ms"] = diagnostic_distribution(
        transfer_stats["ticket_admission_waits_ms"]
    )
    if evaluation_range is not None:
        full_replay_diagnostics = transfer_summary
        transfer_summary = {
            "metric_scope": f"{config.summary_split.lower()}_requests",
            "ticket_created": sum(r.ticket_created for r in results),
            "transfer_completed": sum(r.p2p_assisted for r in results),
            "transfer_admitted": sum(r.p2p_assisted for r in results),
            "p2p_assisted_requests": sum(r.p2p_assisted for r in results),
            "wire_pages": sum(r.wire_pages for r in results),
            "wire_bytes": sum(r.wire_bytes for r in results),
            "request_transfer_wait_ms": diagnostic_distribution(
                [r.request_transfer_wait_ms for r in results if r.p2p_assisted]
            ),
            "ticket_admission_wait_ms": diagnostic_distribution(
                [r.ticket_admission_wait_ms for r in results
                 if r.ticket_admission_wait_ms is not None]
            ),
            "transfer_admission_timeouts": sum(
                r.ticket_state == "TIMED_OUT" for r in results
            ),
            "ticket_terminal_states": {
                state: sum(r.ticket_state == state for r in results)
                for state in sorted({r.ticket_state for r in results if r.ticket_state})
            },
            "full_replay_diagnostics": full_replay_diagnostics,
        }
    pressure = pressure_diagnostics(results, pressure_intervals, len(pods),
                                    range_start_ms=evaluation_range[0] if evaluation_range else None,
                                    range_end_ms=evaluation_range[1] if evaluation_range else None)
    resource = None
    if evaluation_range is not None:
        lo, hi = evaluation_range
        scoped_intervals = [i for i in pressure_intervals
                            if i["end_ms"] > lo and i["start_ms"] < hi]
        def peak(field):
            return max((max(i[field], default=0) for i in scoped_intervals), default=0)
        last = scoped_intervals[-1] if scoped_intervals else None
        resource = {
            "resident_pages": sum(last["resident_pages"]) if last else 0,
            "active_private_pages": sum(last["active_private_pages"]) if last else 0,
            "transfer_temporary_pages": sum(last["transfer_temporary_pages"]) if last else 0,
            "memory_used_pages": sum(last["memory_used_pages"]) if last else 0,
            "peak_resident_pages_per_pod": peak("resident_pages"),
            "peak_active_private_pages_per_pod": peak("active_private_pages"),
            "peak_transfer_temporary_pages_per_pod": peak("transfer_temporary_pages"),
            "peak_memory_used_pages_per_pod": peak("memory_used_pages"),
            "eviction_count": sum(sum(lo <= t < hi for t in p.cache.eviction_timestamps_ms)
                                  for p in pods),
        }
    return {
        "routing_policy": routing_policy,
        "metric_scope": config.summary_split if evaluation_range else "FULL_REPLAY",
        "split_counts": split_counts,
        "num_pods": len(pods),
        "request_count": len(results),
        "input_tokens": total_input,
        "output_tokens": sum(result.output_tokens for result in results),
        "h_route_pages": cluster["h_route_pages"],
        "h_route_tokens": cluster["h_route_tokens"],
        "h_used_pages": cluster["h_used_pages"],
        "h_used_tokens": total_hit,
        "miss_tokens": sum(result.miss_tokens for result in results),
        "cache_hit_pages": sum(result.cache_hit_pages for result in results),
        "cache_inserted_pages": sum(result.cache_inserted_pages for result in results),
        "cache_capacity_pages": pods[0].cache.capacity_pages if pods else None,
        "cache_resident_pages": (resource["resident_pages"] if resource else
                                 sum(pod.cache.resident_pages for pod in pods)),
        "cache_peak_resident_pages": (resource["peak_resident_pages_per_pod"] if resource else
                                      sum(pod.cache.peak_resident_pages for pod in pods)),
        "active_private_pages": (resource["active_private_pages"] if resource else
                                 sum(pod.cache.active_private_pages for pod in pods)),
        "peak_active_private_pages": resource["peak_active_private_pages_per_pod"] if resource else max(
            (pod.cache.peak_active_private_pages for pod in pods), default=0
        ),
        "transfer_temporary_pages": (resource["transfer_temporary_pages"] if resource else
                                     sum(p.cache.transfer_temporary_pages for p in pods)),
        "peak_transfer_temporary_pages": resource["peak_transfer_temporary_pages_per_pod"] if resource else max(
            (p.cache.peak_transfer_temporary_pages for p in pods), default=0
        ),
        "memory_used_pages": (resource["memory_used_pages"] if resource else
                              sum(pod.cache.memory_used_pages for pod in pods)),
        "peak_memory_used_pages": resource["peak_memory_used_pages_per_pod"] if resource else max(
            (pod.cache.peak_memory_used_pages for pod in pods), default=0
        ),
        "eviction_count": resource["eviction_count"] if resource else sum(pod.cache.eviction_count for pod in pods),
        "evicted_pages": resource["eviction_count"] if resource else sum(pod.cache.evicted_pages for pod in pods),
        "capacity_admission_failures": sum(
            pod.cache.capacity_admission_failures for pod in pods
        ),
        "queue_time_ms": distribution([result.queue_time_ms for result in results]),
        "service_time_ms": distribution([result.service_time_ms for result in results]),
        "completion_latency_ms": cluster["latency_ms"],
        "per_pod": pod_metrics,
        "cluster": cluster,
        "transfer": transfer_summary,
        "load_estimation": estimate_groups,
        "pressure": pressure,
        "transfer_config": {
            "enabled": config.transfer_enabled,
            "theta_simple": config.theta_simple,
            "page_bytes": config.page_bytes,
            "effective_bandwidth_bytes_per_s": config.effective_bandwidth_bytes_per_s,
            "control_latency_ms": config.control_latency_ms,
            "partial_page_mode": config.partial_page_mode,
            "transfer_mode": config.transfer_mode,
            "parameter_status": "development_assumption_only",
            "cache_hit_threshold": config.cache_hit_threshold,
            "relative_load_threshold": config.relative_load_threshold,
            "absolute_load_gap_ms": config.absolute_load_gap_ms,
            "admission_timeout_ms": config.admission_timeout_ms,
        },
    }


def write_outputs(output_dir: str | Path, results: Sequence[RequestResult],
                  summary: dict) -> None:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    with (output / "requests.jsonl").open("w", encoding="utf-8") as handle:
        for result in sorted(results, key=lambda item: item.request_id):
            handle.write(json.dumps(asdict(result), sort_keys=True) + "\n")
    (output / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    lines = ["# Kernel v2 Summary", ""]
    for key, value in summary.items():
        if key in {"per_pod", "cluster", "pressure", "load_estimation"}:
            continue
        if isinstance(value, dict):
            lines.extend([f"## {key}", "", "| statistic | value |", "|---|---:|"])
            lines.extend(f"| {name} | {number} |" for name, number in value.items())
            lines.append("")
        else:
            lines.append(f"- {key}: {value}")
    lines.extend(["", "## Cluster", "", "```json",
                  json.dumps(summary["cluster"], indent=2, sort_keys=True), "```",
                  "", "## Per Pod", "", "```json",
                  json.dumps(summary["per_pod"], indent=2, sort_keys=True), "```"])
    (output / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    plan_results = [
        result for result in results
        if result.reactive_ect_selected_plan_type is not None
        and (summary.get("metric_scope") == "FULL_REPLAY"
             or result.split == summary.get("metric_scope"))
    ]
    if plan_results:
        fields = [
            "request_id", "arrival_ms", "selected_plan_type",
            "selected_source", "selected_target", "selected_ect_ms",
            "best_direct_target", "best_direct_ect_ms", "best_copy_source",
            "best_copy_target", "best_copy_ect_ms",
            "estimated_copy_advantage_ms", "actual_queue_ms",
            "actual_service_ms", "actual_completion_ms", "copy_selected",
            "copy_completed", "wire_bytes", "current_hit_tokens",
            "post_copy_hit_tokens", "final_plan_type", "final_source",
            "final_target", "final_ect_ms", "replan_count", "fallback",
            "endpoint_blocked", "capacity_blocked", "direct_candidate_count",
            "copy_candidate_count",
        ]
        with (output / "reactive_plan_records.csv").open(
            "w", newline="", encoding="utf-8"
        ) as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for result in sorted(plan_results, key=lambda item: item.request_id):
                writer.writerow({
                    "request_id": result.request_id,
                    "arrival_ms": result.arrival_ms,
                    "selected_plan_type": result.reactive_ect_selected_plan_type,
                    "selected_source": result.reactive_ect_selected_source,
                    "selected_target": result.reactive_ect_selected_target,
                    "selected_ect_ms": result.reactive_ect_selected_ect_ms,
                    "best_direct_target": result.reactive_ect_best_direct_target,
                    "best_direct_ect_ms": result.reactive_ect_best_direct_ect_ms,
                    "best_copy_source": result.reactive_ect_best_copy_source,
                    "best_copy_target": result.reactive_ect_best_copy_target,
                    "best_copy_ect_ms": result.reactive_ect_best_copy_ect_ms,
                    "estimated_copy_advantage_ms": (
                        result.reactive_ect_estimated_copy_advantage_ms
                    ),
                    "actual_queue_ms": result.queue_time_ms,
                    "actual_service_ms": result.service_time_ms,
                    "actual_completion_ms": result.completion_latency_ms,
                    "copy_selected": result.reactive_ect_copy_selected,
                    "copy_completed": result.reactive_ect_copy_completed,
                    "wire_bytes": result.reactive_ect_wire_bytes,
                    "current_hit_tokens": result.reactive_ect_current_hit_tokens,
                    "post_copy_hit_tokens": result.reactive_ect_post_copy_hit_tokens,
                    "final_plan_type": result.reactive_ect_final_plan_type,
                    "final_source": result.reactive_ect_final_source,
                    "final_target": result.reactive_ect_final_target,
                    "final_ect_ms": result.reactive_ect_final_ect_ms,
                    "replan_count": result.reactive_ect_replan_count,
                    "fallback": result.reactive_ect_fallback,
                    "endpoint_blocked": result.reactive_ect_endpoint_blocked,
                    "capacity_blocked": result.reactive_ect_capacity_blocked,
                    "direct_candidate_count": result.reactive_ect_direct_candidate_count,
                    "copy_candidate_count": result.reactive_ect_copy_candidate_count,
                })
