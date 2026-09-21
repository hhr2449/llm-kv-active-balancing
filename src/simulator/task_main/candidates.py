from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from .history import prefixes
from .trace import full_page_prefix


@dataclass(frozen=True)
class Candidate:
    chain_id: tuple[int, ...]
    source: int
    target: int
    eligible_targets: tuple[int, ...] = ()
    capacity_plan: object | None = None
    target_feasibility_evaluated: bool = False
    structural_target_count: int | None = None
    feasible_target_count: int | None = None
    selected_target_load_rank: int | None = None
    absolute_lowest_load_target_feasible: bool | None = None

    @property
    def depth(self):
        return len(self.chain_id)


def inflight_covers(chain, target, transfers):
    return any(row.type == "PROACTIVE" and row.status == "IN_FLIGHT" and
               row.target == target and row.transferable_chain[:len(chain)] == chain
               for row in transfers)


def inflight_cover_index(transfers):
    """Materialize the exact chain/target coverage relation once per opportunity.

    The former candidate loop rescanned the complete historical transfer ledger for
    every chain and target.  Completed transfers can never cover a candidate, so a
    set of prefixes from the currently active proactive transfers is equivalent.
    """
    covered = set()
    for row in transfers:
        if row.type != "PROACTIVE" or row.status != "IN_FLIGHT":
            continue
        chain = row.transferable_chain
        covered.update((row.target, chain[:depth]) for depth in range(1, len(chain) + 1))
    return covered


class CandidateUniverse:
    def __init__(self):
        self.seen = set()

    def observe(self, request):
        self.seen.update(prefixes(full_page_prefix(request, len(request.block_ids))))

    def materialize(self, caches, loads, transfers):
        candidates = []
        reasons = Counter()
        covered = inflight_cover_index(transfers)
        for chain in sorted(self.seen):
            holders = [pod for pod, cache in enumerate(caches) if cache.lookup(chain) == len(chain)]
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
            ordered = tuple(sorted(eligible, key=lambda p: (loads[p], p)))
            candidates.append(Candidate(chain, min(holders), ordered[0], ordered))
        return candidates, dict(sorted(reasons.items()))
