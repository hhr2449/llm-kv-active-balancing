from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from .cache import TaskMainCache
from .trace import TraceRequest


@dataclass(frozen=True)
class AffinityDecision:
    pod_id: int
    hit_pages: int
    hit_tokens: int


def affinity_route(request: TraceRequest, caches: Sequence[TaskMainCache],
                   loads: Sequence[int]) -> AffinityDecision:
    if not caches or len(caches) != len(loads):
        raise ValueError("one Load per Pod is required")
    hits = [cache.lookup(request.block_ids) for cache in caches]
    pod = min(range(len(caches)), key=lambda index: (-hits[index], loads[index], index))
    return AffinityDecision(pod, hits[pod], request.hit_tokens(hits[pod]))


def least_other_pod(source: int, loads: Sequence[int]) -> int | None:
    others = [pod for pod in range(len(loads)) if pod != source]
    return min(others, key=lambda pod: (loads[pod], pod)) if others else None


def reactive_gate(source_load: int, target_load: int, theta: int = 2) -> bool:
    return source_load > theta * target_load
