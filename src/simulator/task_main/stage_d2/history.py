from __future__ import annotations

from collections import Counter, deque

from ..history import ExternalDemandHistory, prefixes
from ..trace import full_page_prefix


class StageD2DemandHistory(ExternalDemandHistory):
    """Exact Persistence counts with one monotonic sliding-window update.

    The frozen engine observes requests and opens their opportunity in
    nondecreasing time order, with one fixed history window. Instead of doing
    twelve bisects (two for each bucket) for every structural candidate, this
    ledger expires each request/chain event once and serves counts in O(1).
    Original bucket events remain unchanged for final-state audit evidence.
    """

    def __init__(self):
        super().__init__()
        self._online_counts = Counter()
        self._online_events = deque()
        self._prepared_window = None
        self._prepared_now = float("-inf")

    def observe(self, request):
        if request.request_id in self.request_ids:
            return
        path = full_page_prefix(request, len(request.block_ids))
        super().observe(request)
        for chain in prefixes(path):
            self._online_counts[chain] += 1
            self._online_events.append((request.arrival_ms, chain))

    def _prepare(self, now, window):
        if self._prepared_window is None:
            self._prepared_window = window
        if self._prepared_window != window or now < self._prepared_now:
            return False
        boundary = now-window
        while self._online_events and self._online_events[0][0] <= boundary:
            _, chain = self._online_events.popleft()
            self._online_counts[chain] -= 1
            if not self._online_counts[chain]:
                del self._online_counts[chain]
        self._prepared_now = now
        return True

    def count(self, chain, now, window):
        if self._prepare(now, window):
            return self._online_counts.get(chain, 0)
        # Direct API calls with a different window or decreasing time retain the
        # base class's exact order-independent semantics.
        return super().count(chain, now, window)
