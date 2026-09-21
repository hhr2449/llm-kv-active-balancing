from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import math

from .config import LOAD_WINDOW_MS


@dataclass(frozen=True)
class LoadEntry:
    request_id: int
    pod_id: int
    account_time: float
    miss_tokens: int


class CommittedTokenLoad:
    """Recent assignments, not remaining work; the left window endpoint is open."""

    def __init__(self, num_pods: int) -> None:
        if type(num_pods) is not int or num_pods < 1:
            raise ValueError("num_pods must be positive")
        self._histories: list[deque[LoadEntry]] = [deque() for _ in range(num_pods)]
        self._totals = [0] * num_pods
        self._last_time = -math.inf
        self.entries: dict[int, LoadEntry] = {}

    def vector(self, now: float) -> tuple[int, ...]:
        if not math.isfinite(now) or now < self._last_time or now < 0:
            raise ValueError("Load queries must have finite, nondecreasing time")
        self._last_time = now
        for pod, history in enumerate(self._histories):
            while history and history[0].account_time <= now - LOAD_WINDOW_MS:
                self._totals[pod] -= history.popleft().miss_tokens
        return tuple(self._totals)

    def commit(self, request_id: int, pod_id: int, miss_tokens: int, now: float) -> None:
        if request_id in self.entries:
            raise ValueError("request already has a final Load assignment")
        if not 0 <= pod_id < len(self._histories):
            raise ValueError("invalid final Pod")
        if type(miss_tokens) is not int or miss_tokens < 0:
            raise ValueError("miss_tokens must be a nonnegative integer")
        self.vector(now)
        entry = LoadEntry(request_id, pod_id, now, miss_tokens)
        self.entries[request_id] = entry
        self._histories[pod_id].append(entry)
        self._totals[pod_id] += miss_tokens
