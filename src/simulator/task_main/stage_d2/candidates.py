from __future__ import annotations

from collections import Counter

from ..candidates import Candidate, CandidateUniverse, inflight_cover_index
from ..history import prefixes
from ..trace import full_page_prefix


class StageD2CandidateUniverse(CandidateUniverse):
    """Exact frozen candidate semantics with cached order and endpoint lookup."""

    def __init__(self):
        super().__init__()
        self._ordered_seen = ()
        self._seen_version = 0
        self._ordered_version = -1

    def observe(self, request):
        before = len(self.seen)
        self.seen.update(prefixes(full_page_prefix(request, len(request.block_ids))))
        if len(self.seen) != before:
            self._seen_version += 1

    def _ordered(self):
        if self._ordered_version != self._seen_version:
            self._ordered_seen = tuple(sorted(self.seen))
            self._ordered_version = self._seen_version
        return self._ordered_seen

    def materialize(self, caches, loads, transfers):
        result, reasons = [], Counter()
        covered = inflight_cover_index(transfers)
        for chain in self._ordered():
            # Prefix closure plus stable ancestry makes endpoint residence exactly
            # equivalent to lookup(chain)==len(chain).
            holders = [pod for pod, cache in enumerate(caches) if chain[-1] in cache._pages]
            if not holders:
                reasons["NO_SOURCE"] += 1
                continue
            missing = [pod for pod in range(len(caches)) if pod not in holders]
            if not missing:
                reasons["NO_TARGET"] += 1
                continue
            eligible = [pod for pod in missing if (pod, chain) not in covered]
            if not eligible:
                reasons["INFLIGHT_COVERED"] += 1
                continue
            ordered = tuple(sorted(eligible, key=lambda pod: (loads[pod], pod)))
            result.append(Candidate(chain, min(holders), ordered[0], ordered))
        return result, dict(sorted(reasons.items()))
