from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Sequence

from .pod import Pod
from .trace import TraceRequest


class OracleLeakageError(RuntimeError):
    pass


class FutureDemandIndex:
    """External-demand-only inverted index; no simulator outcomes are stored."""

    def __init__(self, requests: Sequence[TraceRequest]) -> None:
        values: dict[int, list[float]] = defaultdict(list)
        for request in requests:
            for block_id in dict.fromkeys(request.block_ids):
                values[block_id].append(request.arrival_ms)
        self._arrivals = {block: tuple(sorted(times)) for block, times in values.items()}

    def future_count(self, hash_id: int, time_ms: float, horizon_ms: float,
                     allowed_end_ms: float | None = None) -> int:
        end = time_ms + horizon_ms
        if allowed_end_ms is not None and end > allowed_end_ms:
            raise OracleLeakageError("future window crosses allowed split boundary")
        times = self._arrivals.get(hash_id, ())
        return bisect_right(times, end) - bisect_right(times, time_ms)

    def first_future_arrival(self, hash_id: int, time_ms: float,
                             horizon_ms: float) -> float | None:
        times = self._arrivals.get(hash_id, ())
        index = bisect_right(times, time_ms)
        return times[index] if index < len(times) and times[index] <= time_ms + horizon_ms else None


@dataclass(frozen=True)
class PrefixCandidate:
    hash_id: int
    depth: int
    valid_prefix_tokens: int
    source_pods: tuple[int, ...]
    current_replica_pods: tuple[int, ...]
    block_ids: tuple[int, ...]
    block_valid_tokens: tuple[int, ...]


class CandidateGenerator:
    """Online-known candidate space shared by future proactive policies."""

    def __init__(self) -> None:
        self._paths: dict[int, tuple[tuple[int, ...], tuple[int, ...]]] = {}
        self._children: dict[int | None, set[int]] = defaultdict(set)
        self._endpoints: set[int] = set()

    def observe(self, request: TraceRequest) -> None:
        self._endpoints.add(request.block_ids[-1])
        for index, block in enumerate(request.block_ids):
            path = request.block_ids[:index + 1]
            valid = request.block_valid_tokens[:index + 1]
            self._paths.setdefault(block, (path, valid))
            parent = request.block_ids[index - 1] if index else None
            self._children[parent].add(block)

    def online_hashes(self, pods: Sequence[Pod] = ()) -> tuple[int, ...]:
        branching = {parent for parent, children in self._children.items()
                     if parent is not None and len(children) > 1}
        published_endpoints = {
            block for pod in pods for block in pod.cache.resident_leaf_block_ids()
            if block in self._paths
        }
        return tuple(sorted(self._endpoints | branching | published_endpoints))

    def materialize(self, hash_id: int, pods: Sequence[Pod]) -> PrefixCandidate | None:
        path, valid = self._paths[hash_id]
        replicas = tuple(p.pod_id for p in pods if p.cache.lookup(path) == len(path))
        if not replicas or len(replicas) == len(pods):
            return None
        return PrefixCandidate(hash_id, len(path), sum(valid), replicas, replicas,
                               path, valid)

    def generate(self, pods: Sequence[Pod]) -> list[PrefixCandidate]:
        candidates = []
        for hash_id in self.online_hashes(pods):
            candidate = self.materialize(hash_id, pods)
            if candidate is not None:
                candidates.append(candidate)
        return candidates


class TokenBucket:
    def __init__(self, rate_bytes_per_s: float, burst_bytes: int,
                 start_ms: float = 0.0, initial_tokens: float | None = None) -> None:
        self.rate = rate_bytes_per_s
        self.burst = float(burst_bytes)
        self.tokens = float(burst_bytes if initial_tokens is None else initial_tokens)
        if not 0 <= self.tokens <= self.burst:
            raise ValueError("initial token count outside bucket")
        self.last_ms = start_ms

    def refill(self, now_ms: float) -> None:
        if now_ms < self.last_ms:
            raise AssertionError("token bucket time moved backwards")
        self.tokens = min(self.burst, self.tokens + self.rate * (now_ms - self.last_ms) / 1000.0)
        self.last_ms = now_ms

    def consume(self, byte_count: int, now_ms: float) -> bool:
        self.refill(now_ms)
        if byte_count > self.tokens:
            return False
        self.tokens -= byte_count
        return True

    def can_afford(self, byte_count: int, now_ms: float) -> bool:
        self.refill(now_ms)
        return byte_count <= self.tokens


def trigger_times(period_ms: float, phase_ms: float, end_ms: float):
    current = phase_ms
    while current < end_ms:
        yield current
        current += period_ms


@dataclass
class ReplicaGeneration:
    transfer_id: int
    pod_id: int
    block_id: int
    residency_generation: int
    ready_time: float
    first_use_time: float | None = None
    eviction_time: float | None = None


@dataclass
class ProactiveCopy:
    action_id: int
    transfer_id: int
    trigger_time: float
    candidate_hash: int
    source_pod: int
    target_pod: int
    prefix_depth: int
    transferable_pages: int
    oracle_score: int
    block_ids: tuple[int, ...]
    horizon_ms: float
    wire_pages: int
    wire_bytes: int
    complete_time: float
    newly_resident_pages: int = 0
    duplicate_wire_pages: int = 0
    generations: list[ReplicaGeneration] = field(default_factory=list)
    move_source_pages_freed: int = 0
    move_release_class: str | None = None
    move_release_blocked_shared: int = 0
    move_release_blocked_pinned: int = 0
    move_release_blocked_active_or_transfer: int = 0
