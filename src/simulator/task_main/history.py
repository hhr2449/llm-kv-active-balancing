from __future__ import annotations

from bisect import bisect_right, insort_right
from collections import defaultdict
import math

from .future_access import record_future_api_read
from .trace import TraceRequest, full_page_prefix

WEIGHTS = (1.3, 2.5, 5.3, 8.9, 11.3, 15.2)


def bucket(tokens: int) -> int:
    for index, boundary in enumerate((5000, 20000, 60000, 120000)):
        if tokens < boundary:
            return index
    return 4 if tokens <= 300000 else 5


def prefixes(path: tuple[int, ...]):
    return (path[:depth] for depth in range(1, len(path) + 1))


class ExternalDemandHistory:
    def __init__(self):
        self.events = defaultdict(list)
        self.request_ids = set()
        self._bucket_times = defaultdict(lambda: tuple([] for _ in range(6)))

    def observe(self, request: TraceRequest):
        if request.request_id in self.request_ids:
            return
        self.request_ids.add(request.request_id)
        for chain in prefixes(full_page_prefix(request, len(request.block_ids))):
            index = bucket(request.input_tokens)
            self.events[chain].append((request.arrival_ms, index))
            # Trace arrivals are normally monotonic.  insort also preserves the old
            # order-independent window semantics for synthetic or direct API use.
            insort_right(self._bucket_times[chain][index], request.arrival_ms)

    def bucket_counts(self, chain, now, window):
        return tuple(
            bisect_right(times, now) - bisect_right(times, now - window)
            for times in self._bucket_times.get(chain, ((),) * 6)
        )

    def count(self, chain, now, window):
        return sum(self.bucket_counts(chain, now, window))

    def benefit(self, chain, now, history_window, future_window):
        return future_window / history_window * sum(
            n * w for n, w in zip(self.bucket_counts(chain, now, history_window), WEIGHTS)
        )


class ReuseHistory:
    def __init__(self):
        self.events = defaultdict(list)
        self.request_ids = set()

    def observe(self, request: TraceRequest, hit_pages: int, completion_time: float):
        if request.request_id in self.request_ids:
            return
        self.request_ids.add(request.request_id)
        for chain in prefixes(full_page_prefix(request, hit_pages)):
            self.events[chain].append(completion_time)

    def score(self, chain, now, decay_ms=60000):
        return math.fsum(math.exp(-(now-time)/decay_ms)
                         for time in self.events.get(chain, ()) if time <= now)


class FutureDemandIndex:
    """Only arrival times per full-page chain; no request or simulator state retained."""
    __slots__ = ("_times", "visibility_end_ms")

    def __init__(self, requests, visibility_end_ms):
        times = defaultdict(list)
        seen = set()
        for request in requests:
            if request.request_id in seen:
                continue
            seen.add(request.request_id)
            for chain in prefixes(full_page_prefix(request, len(request.block_ids))):
                times[chain].append(request.arrival_ms)
        self._times = {chain: tuple(sorted(values)) for chain, values in times.items()}
        self.visibility_end_ms = visibility_end_ms

    def count(self, chain, now, window):
        record_future_api_read()
        if now + window > self.visibility_end_ms:
            raise ValueError("FUTURE_WINDOW_CENSORED")
        times = self._times.get(chain, ())
        return bisect_right(times, now + window) - bisect_right(times, now)
