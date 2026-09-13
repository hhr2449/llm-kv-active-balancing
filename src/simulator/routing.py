from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from .pod import Pod
from .trace import TraceRequest


@dataclass(frozen=True)
class RouteDecision:
    pod_id: int
    h_route_pages: int
    h_route_tokens: int


def route_request(policy: str, request: TraceRequest, pods: Sequence[Pod],
                  now_ms: float) -> RouteDecision:
    if policy == "FIXED":
        selected = pods[0]
        hit = selected.cache.lookup(request.block_ids)
    elif policy == "R_LEAST":
        selected = min(pods, key=lambda pod: (pod.load_ms(now_ms), pod.pod_id))
        hit = selected.cache.lookup(request.block_ids)
    elif policy == "R_AFF":
        hits = [(pod.cache.lookup(request.block_ids), pod) for pod in pods]
        hit, selected = min(
            hits,
            key=lambda item: (-item[0], item[1].load_ms(now_ms), item[1].pod_id),
        )
    else:
        raise ValueError(f"unknown routing policy: {policy}")
    return RouteDecision(selected.pod_id, hit, request.hit_tokens(hit))
