from __future__ import annotations

from collections import Counter

from ..candidates import Candidate, CandidateUniverse, inflight_cover_index
from ..history import prefixes
from ..trace import full_page_prefix


class StageD1CandidateUniverse(CandidateUniverse):
    """Behavior-equivalent candidate materialization with cached online order.

    TaskMain Cache maintains Prefix closure and rejects conflicting ancestry.  A
    nonempty online chain is therefore fully resident exactly when its endpoint
    page is resident.  The frozen implementation proves the same fact by walking
    every ancestor for every (candidate, Pod) pair.  D1 uses the endpoint test and
    caches the lexicographic order until a request introduces a new Prefix.
    """

    def __init__(self):
        super().__init__()
        self._ordered_seen: tuple[tuple[int, ...], ...] = ()
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

    @staticmethod
    def _holds(cache, chain):
        # Candidate chains are nonempty. TaskMainCache.assert_invariants enforces
        # Prefix closure, and _check_path enforces stable ancestry on admission.
        return chain[-1] in cache._pages

    def materialize(self, caches, loads, transfers):
        candidates = []
        reasons = Counter()
        covered = inflight_cover_index(transfers)
        for chain in self._ordered():
            holders = [pod for pod, cache in enumerate(caches) if self._holds(cache, chain)]
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
            candidates.append(Candidate(chain, min(holders), ordered[0], ordered))
        return candidates, dict(sorted(reasons.items()))
