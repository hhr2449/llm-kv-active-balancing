from __future__ import annotations

from dataclasses import replace

from .policies import cost_gate
from .records import CandidateDecisionRecord, ProactiveActionRecord


class ProactiveController:
    def __init__(self, config, universe, policy, caches, transfers):
        self.config, self.universe, self.policy = config, universe, policy
        self.caches, self.transfers = caches, transfers
        self.actions = []
        self.decisions = []
        self.by_transfer = {}

    def _resolve_target(self, candidate, loads):
        """Purely preflight every structural target, then select by (Load, Pod ID)."""
        targets = candidate.eligible_targets or (candidate.target,)
        feasible = []
        for target in targets:
            result = self.transfers.preflight_chain(candidate.chain_id, candidate.source, target)
            if result.plan is not None:
                feasible.append((target, result.plan))
        if feasible:
            target, plan = min(feasible, key=lambda item: (loads[item[0]], item[0]))
            rank = targets.index(target) + 1
        else:
            target, plan, rank = targets[0], None, None
        return replace(
            candidate, target=target, capacity_plan=plan,
            target_feasibility_evaluated=True,
            structural_target_count=len(targets), feasible_target_count=len(feasible),
            selected_target_load_rank=rank,
            absolute_lowest_load_target_feasible=any(target == targets[0] for target, _ in feasible),
        )

    def execute(self, opportunity):
        candidates, reasons = self.universe.materialize(
            self.caches, opportunity.load_vector, self.transfers.records)
        rows, positive, status = self.policy.shortlist(candidates, opportunity.opportunity_time)
        if not candidates and status != "FUTURE_WINDOW_CENSORED":
            status = next(iter(reasons), "NO_SOURCE")
        opportunity = replace(opportunity, policy=self.config.proactive_policy,
                              structural_candidate_count=len(candidates),
                              positive_candidate_count=positive, shortlist_count=len(rows),
                              structural_rejections=reasons, final_status=status or "ZERO_SCORE")
        # Persistence has at most K=10 rows.  Resolve all of them before recording
        # diagnostics so Line 5/7 retain identical target behavior and Line 7 has
        # a complete Score decomposition for every shortlist row.  Oracle and
        # Recency can have large shortlists; resolve them lazily only when reached.
        if self.config.proactive_policy in {"PERSISTENCE", "PERSISTENCE_COST_AWARE"}:
            rows = [replace(row, candidate=self._resolve_target(row.candidate,
                                                                 opportunity.load_vector))
                    for row in rows]
        audits = []
        for rank, row in enumerate(rows, 1):
            candidate = row.candidate
            audits.append(CandidateDecisionRecord(
                opportunity.opportunity_id, rank, candidate.chain_id,
                candidate.source, candidate.target, self.config.proactive_policy, row.score,
                oracle_future_count=row.oracle_future_count, persistence_count=row.persistence_count,
                recency_score=row.recency_score,
                target_feasibility_evaluated=candidate.target_feasibility_evaluated,
                structural_target_count=candidate.structural_target_count,
                feasible_target_count=candidate.feasible_target_count,
                selected_target_load_rank=candidate.selected_target_load_rank,
                absolute_lowest_load_target_feasible=candidate.absolute_lowest_load_target_feasible,
                **self.policy.diagnostics(row, opportunity.opportunity_time, opportunity.load_vector),
            ))
        self.decisions.extend(audits)
        for index, audit in enumerate(audits):
            opportunity = replace(opportunity, attempted_candidate_count=index+1)
            candidate = rows[index].candidate
            if not candidate.target_feasibility_evaluated:
                candidate = self._resolve_target(candidate, opportunity.load_vector)
                audit.target = candidate.target
                audit.target_feasibility_evaluated = True
                audit.structural_target_count = candidate.structural_target_count
                audit.feasible_target_count = candidate.feasible_target_count
                audit.selected_target_load_rank = candidate.selected_target_load_rank
                audit.absolute_lowest_load_target_feasible = candidate.absolute_lowest_load_target_feasible
            if audit.cost_score is not None and not cost_gate(audit.cost_score):
                audit.status = "COST_GATE_REJECTED"
                opportunity = replace(opportunity, final_status=audit.status)
                continue
            if candidate.capacity_plan is None:
                audit.status = "SKIP_NO_CAPACITY"
                opportunity = replace(opportunity, final_status=audit.status)
                continue
            transfer = self.transfers.start(candidate.capacity_plan, opportunity.request_id,
                                            opportunity.opportunity_time, "PROACTIVE")
            audit.status = "SELECTED"
            for rest in audits[index+1:]:
                rest.status = "NOT_ATTEMPTED_CAP_REACHED"
            diagnostic = {key: getattr(audit, key) for key in (
                "oracle_future_count", "persistence_count", "recency_score", "benefit",
                "congestion", "transfer_cost_seconds", "cost_score")}
            action = ProactiveActionRecord(
                len(self.actions), opportunity.opportunity_id, opportunity.request_id,
                self.config.proactive_policy, opportunity.opportunity_time,
                audit.chain_id, len(audit.chain_id), audit.source, audit.target, audit.policy_score,
                transfer.wire_pages, transfer.wire_tokens, transfer.wire_bytes, transfer.transfer_id,
                transfer.start_time, transfer.ready_time, opportunity.split, **diagnostic,
                structural_target_count=audit.structural_target_count,
                feasible_target_count=audit.feasible_target_count,
                selected_target_load_rank=audit.selected_target_load_rank,
                absolute_lowest_load_target_feasible=audit.absolute_lowest_load_target_feasible,
            )
            self.actions.append(action)
            self.by_transfer[transfer.transfer_id] = action
            return replace(opportunity, selected_chain_id=audit.chain_id,
                           selected_source=audit.source, selected_target=audit.target,
                           proactive_transfer_id=transfer.transfer_id, final_status="STARTED"), transfer
        return opportunity, None

    def complete(self, transfer):
        self.by_transfer[transfer.transfer_id].status = transfer.status
