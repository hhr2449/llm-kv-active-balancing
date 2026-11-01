from __future__ import annotations

import math
from collections import defaultdict
from typing import Sequence


def _gini(values: Sequence[float]) -> float:
    total = sum(values)
    if not values or total == 0:
        return 0.0
    ordered = sorted(values)
    n = len(ordered)
    return 2 * sum((i + 1) * x for i, x in enumerate(ordered)) / (n * total) - (n + 1) / n


def _weighted_percentile(samples: Sequence[tuple[float, float]], q: float) -> float:
    positive = sorted((value, weight) for value, weight in samples if weight > 0)
    if not positive:
        return 0.0
    threshold = sum(weight for _, weight in positive) * q
    cumulative = 0.0
    for value, weight in positive:
        cumulative += weight
        if cumulative >= threshold:
            return value
    return positive[-1][0]


def pressure_diagnostics(results, intervals: Sequence[dict], num_pods: int,
                         windows_ms: Sequence[int] = (1000, 3000, 15000, 30000, 60000),
                         range_start_ms: float | None = None,
                         range_end_ms: float | None = None) -> dict:
    if range_start_ms is not None or range_end_ms is not None:
        lo = -math.inf if range_start_ms is None else range_start_ms
        hi = math.inf if range_end_ms is None else range_end_ms
        clipped = []
        for original in intervals:
            start, end = max(original["start_ms"], lo), min(original["end_ms"], hi)
            if end <= start:
                continue
            total = original["end_ms"] - original["start_ms"]
            left = (start - original["start_ms"]) / total
            right = (end - original["start_ms"]) / total
            item = dict(original)
            item["start_ms"], item["end_ms"] = start, end
            item["start_loads"] = [a + (b - a) * left for a, b in
                                   zip(original["start_loads"], original["end_loads"])]
            item["end_loads"] = [a + (b - a) * right for a, b in
                                 zip(original["start_loads"], original["end_loads"])]
            clipped.append(item)
        intervals = clipped
    gap_samples: list[tuple[float, float]] = []
    busy_idle_ms = 0.0
    queued_wait_while_idle = 0.0
    queued_ids: set[int] = set()
    for interval in intervals:
        duration = interval["end_ms"] - interval["start_ms"]
        midpoint_loads = [
            (a + b) / 2.0
            for a, b in zip(interval["start_loads"], interval["end_loads"])
        ]
        gap_samples.append((max(midpoint_loads, default=0.0)
                            - min(midpoint_loads, default=0.0), duration))
        has_queue = any(interval["queue_lengths"])
        has_idle = any(
            not running and length == 0
            for running, length in zip(interval["running"], interval["queue_lengths"])
        )
        if has_queue and has_idle:
            busy_idle_ms += duration
            queued_wait_while_idle += sum(interval["queue_lengths"]) * duration
            for ids in interval["queue_ids"]:
                queued_ids.update(ids)

    total_duration = sum(i["end_ms"] - i["start_ms"] for i in intervals)
    load_gap = {
        "p50": _weighted_percentile(gap_samples, 0.50),
        "p90": _weighted_percentile(gap_samples, 0.90),
        "p95": _weighted_percentile(gap_samples, 0.95),
        "p99": _weighted_percentile(gap_samples, 0.99),
        "max": max((value for value, _ in gap_samples), default=0.0),
    }

    all_windows = {}
    for width in windows_ms:
        bins: dict[int, dict] = {}

        def get_bin(index: int) -> dict:
            if index not in bins:
                bins[index] = {
                    "window_start_ms": index * width,
                    "window_end_ms": (index + 1) * width,
                    "pods": [{
                        "request_count": 0, "completed_request_count": 0,
                        "arrived_input_tokens": 0, "arrived_output_tokens": 0,
                        "executed_miss_tokens": 0, "h_used_tokens": 0,
                        "busy_time_ms": 0.0,
                        "queue_wait_ms": 0.0, "queue_length_area": 0.0,
                        "max_queue_length": 0, "load_area": 0.0,
                        "max_load_ms": 0.0,
                        "running_request_count_area": 0.0,
                        "running_remaining_service_ms_area": 0.0,
                        "queued_estimated_service_ms_area": 0.0,
                        "gpu_service_load_ms_area": 0.0,
                        "remote_pressure_ms_area": 0.0,
                        "active_transfer_source_area": 0.0,
                        "active_transfer_target_area": 0.0,
                        "available_capacity_pages_area": 0.0,
                        "available_capacity_pages_observed_ms": 0.0,
                        "resident_pages_area": 0.0,
                        "active_private_pages_area": 0.0,
                        "transfer_temporary_pages_area": 0.0,
                        "memory_used_pages_area": 0.0,
                        "peak_resident_pages": 0,
                        "peak_active_private_pages": 0,
                        "peak_transfer_temporary_pages": 0,
                        "peak_memory_used_pages": 0,
                        "end_resident_pages": 0,
                        "end_active_private_pages": 0,
                        "end_transfer_temporary_pages": 0,
                        "end_memory_used_pages": 0,
                    } for _ in range(num_pods)],
                    "stranded": {
                        "queue_while_other_idle_ms": 0.0,
                        "stranded_capacity_ms": 0.0,
                        "queued_requests_area_when_other_idle": 0.0,
                        "queued_work_ms_area_when_other_idle": 0.0,
                        "max_effective_load_gap_when_other_idle_ms": 0.0,
                    },
                }
            return bins[index]

        for result in results:
            arrival_bin = math.floor(result.arrival_ms / width)
            pod = get_bin(arrival_bin)["pods"][result.pod_id]
            pod["request_count"] += 1
            pod["arrived_input_tokens"] += result.input_tokens
            pod["arrived_output_tokens"] += getattr(result, "output_tokens", 0)
            start_bin = math.floor(result.service_start_ms / width)
            start_pod = get_bin(start_bin)["pods"][result.pod_id]
            start_pod["executed_miss_tokens"] += result.miss_tokens
            start_pod["h_used_tokens"] += getattr(result, "h_used_tokens", 0)
            if hasattr(result, "service_done_ms"):
                done_bin = math.floor(result.service_done_ms / width)
                get_bin(done_bin)["pods"][result.pod_id]["completed_request_count"] += 1
            ready_ms = result.service_start_ms - result.queue_time_ms
            cursor = ready_ms
            while cursor < result.service_start_ms:
                index = math.floor(cursor / width)
                end = min(result.service_start_ms, (index + 1) * width)
                get_bin(index)["pods"][result.pod_id]["queue_wait_ms"] += end - cursor
                cursor = end

        for interval in intervals:
            cursor = interval["start_ms"]
            total = interval["end_ms"] - interval["start_ms"]
            while cursor < interval["end_ms"]:
                index = math.floor(cursor / width)
                end = min(interval["end_ms"], (index + 1) * width)
                overlap = end - cursor
                left_fraction = (cursor - interval["start_ms"]) / total
                right_fraction = (end - interval["start_ms"]) / total
                bin_record = get_bin(index)
                has_queue = any(interval["queue_lengths"])
                has_idle = any(
                    not running and length == 0
                    for running, length in zip(
                        interval["running"], interval["queue_lengths"]
                    )
                )
                remote_values = interval.get("remote_pressure_ms")
                has_zero_effective_work = any(
                    not interval["running"][pod_id]
                    and interval["queue_lengths"][pod_id] == 0
                    and (remote_values is None or remote_values[pod_id] == 0)
                    for pod_id in range(num_pods)
                )
                if has_queue and has_idle:
                    stranded = bin_record["stranded"]
                    stranded["queue_while_other_idle_ms"] += overlap
                    stranded["queued_requests_area_when_other_idle"] += (
                        sum(interval["queue_lengths"]) * overlap
                    )
                    queued_values = interval.get("queued_estimated_service_ms")
                    if queued_values is not None:
                        stranded["queued_work_ms_area_when_other_idle"] += (
                            sum(queued_values) * overlap
                        )
                    left_loads = [
                        start + (finish - start) * left_fraction
                        for start, finish in zip(
                            interval["start_loads"], interval["end_loads"]
                        )
                    ]
                    right_loads = [
                        start + (finish - start) * right_fraction
                        for start, finish in zip(
                            interval["start_loads"], interval["end_loads"]
                        )
                    ]
                    stranded["max_effective_load_gap_when_other_idle_ms"] = max(
                        stranded["max_effective_load_gap_when_other_idle_ms"],
                        max(left_loads, default=0.0) - min(left_loads, default=0.0),
                        max(right_loads, default=0.0) - min(right_loads, default=0.0),
                    )
                if has_queue and has_zero_effective_work:
                    bin_record["stranded"]["stranded_capacity_ms"] += overlap
                for pod_id in range(num_pods):
                    p = bin_record["pods"][pod_id]
                    qlen = interval["queue_lengths"][pod_id]
                    p["queue_length_area"] += qlen * overlap
                    p["max_queue_length"] = max(p["max_queue_length"], qlen)
                    if interval["running"][pod_id]:
                        p["busy_time_ms"] += overlap
                    start_load = interval["start_loads"][pod_id]
                    end_load = interval["end_loads"][pod_id]
                    left = start_load + (end_load - start_load) * left_fraction
                    right = start_load + (end_load - start_load) * right_fraction
                    p["load_area"] += (left + right) * overlap / 2.0
                    p["max_load_ms"] = max(p["max_load_ms"], left, right)
                    p["running_request_count_area"] += int(
                        interval["running"][pod_id]
                    ) * overlap
                    running_start_values = interval.get(
                        "start_running_remaining_service_ms"
                    )
                    running_end_values = interval.get(
                        "end_running_remaining_service_ms"
                    )
                    if running_start_values is not None and running_end_values is not None:
                        running_start = running_start_values[pod_id]
                        running_end = running_end_values[pod_id]
                        running_left = running_start + (
                            running_end - running_start
                        ) * left_fraction
                        running_right = running_start + (
                            running_end - running_start
                        ) * right_fraction
                        p["running_remaining_service_ms_area"] += (
                            running_left + running_right
                        ) * overlap / 2.0
                    queued_values = interval.get("queued_estimated_service_ms")
                    if queued_values is not None:
                        p["queued_estimated_service_ms_area"] += (
                            queued_values[pod_id] * overlap
                        )
                    gpu_start_values = interval.get("start_gpu_service_load_ms")
                    gpu_end_values = interval.get("end_gpu_service_load_ms")
                    if gpu_start_values is not None and gpu_end_values is not None:
                        gpu_start = gpu_start_values[pod_id]
                        gpu_end = gpu_end_values[pod_id]
                        gpu_left = gpu_start + (gpu_end - gpu_start) * left_fraction
                        gpu_right = gpu_start + (gpu_end - gpu_start) * right_fraction
                        p["gpu_service_load_ms_area"] += (
                            gpu_left + gpu_right
                        ) * overlap / 2.0
                    remote_values = interval.get("remote_pressure_ms")
                    if remote_values is not None:
                        p["remote_pressure_ms_area"] += remote_values[pod_id] * overlap
                    source_values = interval.get("active_transfer_source")
                    target_values = interval.get("active_transfer_target")
                    if source_values is not None:
                        p["active_transfer_source_area"] += int(
                            source_values[pod_id]
                        ) * overlap
                    if target_values is not None:
                        p["active_transfer_target_area"] += int(
                            target_values[pod_id]
                        ) * overlap
                    available_values = interval.get("available_capacity_pages")
                    if available_values is not None and available_values[pod_id] is not None:
                        p["available_capacity_pages_area"] += (
                            available_values[pod_id] * overlap
                        )
                        p["available_capacity_pages_observed_ms"] += overlap
                    for field in (
                        "resident_pages", "active_private_pages",
                        "transfer_temporary_pages", "memory_used_pages",
                    ):
                        values = interval.get(field)
                        value = values[pod_id] if values is not None else 0
                        p[f"{field}_area"] += value * overlap
                        p[f"peak_{field}"] = max(p[f"peak_{field}"], value)
                        p[f"end_{field}"] = value
                cursor = end

        records = []
        for index in sorted(bins):
            record = bins[index]
            duration = width
            for pod in record["pods"]:
                pod["mean_queue_length"] = pod.pop("queue_length_area") / duration
                pod["mean_load_ms"] = pod.pop("load_area") / duration
                pod["mean_running_request_count"] = pod.pop(
                    "running_request_count_area"
                ) / duration
                pod["mean_running_remaining_service_ms"] = pod.pop(
                    "running_remaining_service_ms_area"
                ) / duration
                pod["mean_queued_estimated_service_ms"] = pod.pop(
                    "queued_estimated_service_ms_area"
                ) / duration
                pod["mean_gpu_service_load_ms"] = pod.pop(
                    "gpu_service_load_ms_area"
                ) / duration
                pod["mean_remote_pressure_ms"] = pod.pop(
                    "remote_pressure_ms_area"
                ) / duration
                pod["active_transfer_source_fraction"] = pod.pop(
                    "active_transfer_source_area"
                ) / duration
                pod["active_transfer_target_fraction"] = pod.pop(
                    "active_transfer_target_area"
                ) / duration
                available_area = pod.pop("available_capacity_pages_area")
                available_observed = pod.pop("available_capacity_pages_observed_ms")
                pod["mean_available_capacity_pages"] = (
                    available_area / available_observed if available_observed else None
                )
                for field in (
                    "resident_pages", "active_private_pages",
                    "transfer_temporary_pages", "memory_used_pages",
                ):
                    pod[f"mean_{field}"] = pod.pop(f"{field}_area") / duration
            record["cluster"] = {
                "request_count_gini": _gini([p["request_count"] for p in record["pods"]]),
                "executed_miss_token_gini": _gini([p["executed_miss_tokens"] for p in record["pods"]]),
                "busy_time_gini": _gini([p["busy_time_ms"] for p in record["pods"]]),
                "mean_load_gap_ms": max((p["mean_load_ms"] for p in record["pods"]), default=0.0)
                    - min((p["mean_load_ms"] for p in record["pods"]), default=0.0),
                "max_load_gap_ms": max((p["max_load_ms"] for p in record["pods"]), default=0.0)
                    - min((p["max_load_ms"] for p in record["pods"]), default=0.0),
            }
            stranded = record["stranded"]
            stranded["queue_while_other_idle_fraction"] = (
                stranded["queue_while_other_idle_ms"] / width
            )
            stranded["stranded_capacity_fraction"] = (
                stranded["stranded_capacity_ms"] / width
            )
            active_ms = stranded["queue_while_other_idle_ms"]
            stranded["mean_total_queued_requests_when_other_idle"] = (
                stranded["queued_requests_area_when_other_idle"] / active_ms
                if active_ms else 0.0
            )
            stranded["mean_total_queued_work_ms_when_other_idle"] = (
                stranded["queued_work_ms_area_when_other_idle"] / active_ms
                if active_ms else 0.0
            )
            records.append(record)
        all_windows[str(width)] = records

    return {
        "load_gap_ms": load_gap,
        "busy_while_other_idle_ms": busy_idle_ms,
        "busy_while_other_idle_fraction": busy_idle_ms / total_duration if total_duration else 0.0,
        "queued_requests_while_other_idle": len(queued_ids),
        "queued_request_ms_while_other_idle": queued_wait_while_idle,
        "windows": all_windows,
    }
