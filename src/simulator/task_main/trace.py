from __future__ import annotations

import math
from pathlib import Path
from typing import Sequence

from ..trace import TraceRequest, load_trace
from .config import PAGE_TOKENS


def full_page_prefix(request: TraceRequest, hit_pages: int) -> tuple[int, ...]:
    """Convert before constructing a transfer object; never transfer a partial tail."""
    request.hit_tokens(hit_pages)  # Bounds check shared with the trace primitive.
    pages = 0
    for valid in request.block_valid_tokens[:hit_pages]:
        if valid != PAGE_TOKENS:
            break
        pages += 1
    return request.block_ids[:pages]


def validate_requests(requests: Sequence[TraceRequest]) -> list[TraceRequest]:
    """Also validate direct synthetic inputs, without importing the old engine."""
    seen_ids: set[int] = set()
    identity: dict[int, tuple[int | None, int, int]] = {}
    for request in requests:
        if type(request.request_id) is not int or request.request_id < 0:
            raise ValueError("request IDs must be nonnegative integers")
        if request.request_id in seen_ids:
            raise ValueError("duplicate request ID")
        seen_ids.add(request.request_id)
        if not math.isfinite(request.arrival_ms) or request.arrival_ms < 0:
            raise ValueError("arrival time must be finite and nonnegative")
        expected = TraceRequest.from_record(request.request_id, {
            "timestamp": request.arrival_ms, "input_length": request.input_tokens,
            "output_length": request.output_tokens, "hash_ids": list(request.block_ids),
        }, PAGE_TOKENS)
        if request.block_valid_tokens != expected.block_valid_tokens:
            raise ValueError("inconsistent valid tokens")
        if len(set(request.block_ids)) != len(request.block_ids):
            raise ValueError("duplicate hash within request")
        for index, block in enumerate(request.block_ids):
            value = (request.block_ids[index - 1] if index else None,
                     index, request.block_valid_tokens[index])
            if block in identity and identity[block] != value:
                raise ValueError(f"conflicting Prefix identity for hash {block}")
            identity[block] = value
    return sorted(requests, key=lambda request: (request.arrival_ms, request.request_id))


def read_trace(path: str | Path) -> list[TraceRequest]:
    return validate_requests(load_trace(path, PAGE_TOKENS))


def separate_input_roles(requests: Sequence[TraceRequest], visibility_end_ms: int):
    """One shared pilot/formal boundary rule; does not mutate or execute requests."""
    replay = [r for r in requests if r.arrival_ms < visibility_end_ms]
    observation = [r for r in requests if r.arrival_ms <= visibility_end_ms]
    return replay, observation


def input_role_identity(replay, observation):
    from dataclasses import asdict
    import hashlib
    import json
    def identity(rows):
        return hashlib.sha256(json.dumps([asdict(r) for r in rows], sort_keys=True,
            separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    return dict(replay_request_count=len(replay), oracle_observation_request_count=len(observation),
                oracle_boundary_only_request_count=len(observation)-len(replay),
                replay_input_time_rule="arrival < visibility_end",
                oracle_observation_time_rule="arrival <= visibility_end",
                replay_request_identity_hash=identity(replay),
                oracle_observation_identity_hash=identity(observation))
