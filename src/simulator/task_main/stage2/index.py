"""Mutation-maintained structural eligibility and sparse positive-score views.

No request-by-all-seen-prefix scan. Immutable full-page identities are shared by
forked workers. Only actual cache/transfer mutations change holder/coverage sets.
History scores themselves still use the frozen bisect/fsum implementations.
"""
from collections import Counter, defaultdict
import heapq

from ..candidates import Candidate
from ..history import ExternalDemandHistory, prefixes
from ..policies import Policy
from ..trace import full_page_prefix


def empty_buckets():
    return tuple([] for _ in range(6))


class IndexedDemand(ExternalDemandHistory):
    def __init__(self):
        super().__init__()
        self._bucket_times = defaultdict(empty_buckets)  # Picklable checkpoints.
        self.expirations = []
        self.active = Counter()

    def observe(self, request):
        if request.request_id in self.request_ids:
            return
        super().observe(request)
        chain = full_page_prefix(request, len(request.block_ids))
        heapq.heappush(self.expirations, (request.arrival_ms + 300000, request.request_id, chain))
        self.active.update(prefixes(chain))

    def advance(self, now):
        while self.expirations and self.expirations[0][0] <= now:
            _, _, chain = heapq.heappop(self.expirations)
            for prefix in prefixes(chain):
                self.active[prefix] -= 1
                if not self.active[prefix]:
                    del self.active[prefix]


class FutureWindow:
    def __init__(self, requests):
        self.requests = requests
        self.lo = self.hi = 0
        self.active = Counter()

    def advance(self, now):
        rows = self.requests
        while self.hi < len(rows) and rows[self.hi].arrival_ms <= now + 300000:
            r = rows[self.hi]
            self.active.update(prefixes(full_page_prefix(r, len(r.block_ids))))
            self.hi += 1
        while self.lo < self.hi and rows[self.lo].arrival_ms <= now:
            r = rows[self.lo]
            for chain in prefixes(full_page_prefix(r, len(r.block_ids))):
                self.active[chain] -= 1
                if not self.active[chain]:
                    del self.active[chain]
            self.lo += 1


class CandidateView:
    def __init__(self, universe, loads):
        self.universe, self.loads = universe, loads

    def __len__(self):
        return len(self.universe.structural)

    def __iter__(self):
        u = self.universe
        for chain in sorted(u.structural.intersection(u.positive)):
            holders = u.holders[chain]
            targets = tuple(sorted((p for p in range(u.pods)
                                    if p not in holders and not u.covered.get((chain, p), 0)),
                                   key=lambda p: (self.loads[p], p)))
            yield Candidate(chain, min(holders), targets[0], targets)


class IndexedUniverse:
    def __init__(self, pods, chains):
        self.pods = pods
        self.chains = chains  # last block -> full-page chain, immutable
        self.seen = set()
        self.holders = defaultdict(set)
        self.covered = Counter()
        self.categories = {}
        self.reasons = Counter()
        self.structural = set()
        self.positive = set()
        self.index_updates = 0

    def _refresh(self, chain):
        if chain not in self.seen:
            return
        self.index_updates += 1
        old = self.categories.get(chain)
        if old and old != 'ELIGIBLE':
            self.reasons[old] -= 1
        self.structural.discard(chain)
        holders = self.holders.get(chain, set())
        if not holders:
            category = 'NO_SOURCE'
        elif len(holders) == self.pods:
            category = 'NO_TARGET'
        elif all(p in holders or self.covered.get((chain, p), 0) for p in range(self.pods)):
            category = 'INFLIGHT_COVERED'
        else:
            category = 'ELIGIBLE'
            self.structural.add(chain)
        self.categories[chain] = category
        if category != 'ELIGIBLE':
            self.reasons[category] += 1

    def observe(self, request):
        for chain in prefixes(full_page_prefix(request, len(request.block_ids))):
            if chain not in self.seen:
                self.seen.add(chain)
                self._refresh(chain)

    def resident(self, pod, block, present):
        chain = self.chains.get(block)
        if chain is None:
            return
        if present:
            self.holders[chain].add(pod)
        else:
            self.holders[chain].discard(pod)
        self._refresh(chain)

    def inflight(self, transfer, change):
        if transfer.type != 'PROACTIVE':
            return
        for chain in prefixes(transfer.transferable_chain):
            key = (chain, transfer.target)
            self.covered[key] += change
            if not self.covered[key]:
                del self.covered[key]
            self._refresh(chain)

    def materialize(self, caches, loads, transfers):
        return CandidateView(self, loads), dict(sorted((k, v) for k, v in self.reasons.items() if v))


class IndexedPolicy(Policy):
    def __init__(self, config, demand, reuse, future, universe, requests):
        super().__init__(config, demand, reuse, future)
        self.universe = universe
        self.window = FutureWindow(requests) if future is not None else None

    def shortlist(self, candidates, now):
        if self.config.proactive_policy == 'FUTURE_DEMAND':
            if now + 300000 > self.config.visibility_end_ms:
                return [], 0, 'FUTURE_WINDOW_CENSORED'
            self.window.advance(now)
            self.universe.positive = self.window.active.keys()
        elif self.config.proactive_policy == 'PERSISTENCE':
            self.demand.advance(now)
            self.universe.positive = self.demand.active.keys()
        else:
            self.universe.positive = self.reuse.events.keys()
        try:
            return super().shortlist(candidates, now)
        finally:
            # Dict views are not pickleable. This view is derived per decision.
            self.universe.positive = set()


def static_metadata(requests):
    chains = {}
    blocks = set()
    for request in requests:
        blocks.update(request.block_ids)
        for chain in prefixes(full_page_prefix(request, len(request.block_ids))):
            chains.setdefault(chain[-1], chain)
    return chains, {b: i // 256 for i, b in enumerate(sorted(blocks))}
