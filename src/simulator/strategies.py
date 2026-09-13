from __future__ import annotations

import math
from bisect import bisect_left, bisect_right
from collections import defaultdict
from dataclasses import dataclass
from typing import Sequence

from .trace import TraceRequest


def task_weight(input_tokens: int) -> float:
    if input_tokens < 5_000:
        return 1.3
    if input_tokens < 20_000:
        return 2.5
    if input_tokens < 60_000:
        return 5.3
    if input_tokens < 120_000:
        return 8.9
    if input_tokens <= 300_000:
        return 11.3
    return 15.2


class ExternalDemandHistory:
    """Policy-independent historical demand from external requests only."""

    def __init__(self, requests: Sequence[TraceRequest]) -> None:
        observations: dict[int, list[tuple[float, int]]] = defaultdict(list)
        for request in requests:
            for block_id in dict.fromkeys(request.block_ids):
                observations[block_id].append((request.arrival_ms, request.input_tokens))
        self._values = {key: tuple(sorted(value)) for key, value in observations.items()}

    def window(self, block_id: int, time_ms: float,
               history_ms: float) -> tuple[tuple[float, int], ...]:
        values = self._values.get(block_id, ())
        times = [item[0] for item in values]
        left = bisect_right(times, time_ms - history_ms)
        right = bisect_right(times, time_ms)
        return values[left:right]

    def persistence(self, block_id: int, time_ms: float, history_ms: float) -> int:
        return len(self.window(block_id, time_ms, history_ms))

    def recency(self, block_id: int, time_ms: float, decay_seconds: float) -> float:
        values = self._values.get(block_id, ())
        times = [item[0] for item in values]
        right = bisect_right(times, time_ms)
        decay_ms = decay_seconds * 1000.0
        return sum(math.exp(-(time_ms - arrival) / decay_ms)
                   for arrival, _ in values[:right])

    def value_hat(self, block_id: int, time_ms: float, history_ms: float,
                  future_window_ms: float) -> float:
        scale = future_window_ms / history_ms
        return scale * sum(task_weight(tokens)
                           for _, tokens in self.window(block_id, time_ms, history_ms))


def quantile(values: Sequence[float], q: float) -> float:
    if not values:
        raise ValueError("quantile requires non-empty values")
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    low, high = math.floor(position), math.ceil(position)
    if low == high:
        return ordered[low]
    return ordered[low] * (high - position) + ordered[high] * (position - low)


def relative_load_penalty(target_load: float, all_loads: Sequence[float]) -> float:
    maximum = max(all_loads, default=0.0)
    return 0.0 if maximum == 0 else 0.5 * target_load / maximum


def request_kv_task_gate(source_load: float, target_load: float,
                         transferable_pages: int, source_pod: int,
                         target_pod: int, theta: float) -> bool:
    return (transferable_pages > 0 and source_pod != target_pod
            and source_load > theta * target_load)


def cost_aware_score(value_hat: float, v_ref: float, target_load: float,
                     all_loads: Sequence[float], transfer_ms: float,
                     t_ref_ms: float) -> tuple[float, float, float, float]:
    benefit = value_hat / v_ref
    load_penalty = relative_load_penalty(target_load, all_loads)
    transfer_penalty = 0.1 * transfer_ms / t_ref_ms
    return benefit - load_penalty - transfer_penalty, benefit, load_penalty, transfer_penalty


@dataclass(frozen=True)
class RankedCandidate:
    hash_id: int
    score: float
    depth: int


def deterministic_rank(items: Sequence[RankedCandidate]) -> list[RankedCandidate]:
    return sorted(items, key=lambda item: (-item.score, -item.depth, item.hash_id))
