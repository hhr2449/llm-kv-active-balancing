from __future__ import annotations

from dataclasses import dataclass
import math

from .candidates import Candidate
from .config import PAGE_BYTES, BANDWIDTH_BYTES_PER_SECOND


def quantile(values, q=0.9):
    values = sorted(values)
    if not values:
        raise ValueError("empty quantile")
    location = (len(values)-1)*q
    lower, upper = math.floor(location), math.ceil(location)
    return values[lower] + (values[upper]-values[lower]) * (location-lower)


def cost_terms(benefit, target, loads, wire_pages):
    congestion = loads[target]/max(loads) if max(loads) else 0.0
    seconds = wire_pages*PAGE_BYTES/BANDWIDTH_BYTES_PER_SECOND
    return {"benefit": benefit, "congestion": congestion,
            "transfer_cost_seconds": seconds,
            "cost_score": benefit - 0.5*congestion - 0.1*seconds}


def cost_gate(score):
    return score > 0


@dataclass(frozen=True)
class RankedCandidate:
    candidate: Candidate
    score: float
    oracle_future_count: int | None = None
    persistence_count: int | None = None
    recency_score: float | None = None


class Policy:
    def __init__(self, config, demand, reuse, future=None):
        self.config, self.demand, self.reuse, self.future = config, demand, reuse, future
        if config.proactive_policy != "FUTURE_DEMAND" and future is not None:
            raise ValueError("history-only policy cannot receive future index")

    def shortlist(self, candidates, now):
        config = self.config
        if config.proactive_policy == "FUTURE_DEMAND" and now+config.future_window_ms > config.visibility_end_ms:
            return [], 0, "FUTURE_WINDOW_CENSORED"
        ranked = []
        for candidate in candidates:
            chain = candidate.chain_id
            if config.proactive_policy == "FUTURE_DEMAND":
                score = self.future.count(chain, now, config.future_window_ms)
                row = RankedCandidate(candidate, score, oracle_future_count=score)
            elif config.proactive_policy in {"PERSISTENCE", "PERSISTENCE_COST_AWARE"}:
                score = self.demand.count(chain, now, config.history_window_ms)
                row = RankedCandidate(candidate, score, persistence_count=score)
            elif config.proactive_policy == "RECENCY":
                score = self.reuse.score(chain, now, config.recency_decay_ms)
                row = RankedCandidate(candidate, score, recency_score=score)
            else:
                return [], 0, "NONE"
            if score > 0:
                ranked.append(row)
        ranked.sort(key=lambda row: (-row.score, -row.candidate.depth, row.candidate.chain_id))
        positive_count = len(ranked)
        if config.proactive_policy == "RECENCY" and ranked:
            threshold = quantile([row.score for row in ranked], config.recency_quantile)
            ranked = [row for row in ranked if row.score >= threshold]
        elif config.proactive_policy in {"PERSISTENCE", "PERSISTENCE_COST_AWARE"}:
            ranked = ranked[:config.shortlist_k]
        return ranked, positive_count, None if ranked else "ZERO_SCORE"

    def diagnostics(self, row, now, loads):
        if self.config.proactive_policy != "PERSISTENCE_COST_AWARE":
            return {}
        return cost_terms(self.demand.benefit(row.candidate.chain_id, now,
                          self.config.history_window_ms, self.config.future_window_ms),
                          row.candidate.target, loads, row.candidate.depth)
