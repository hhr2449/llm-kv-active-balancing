from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import heapq
import json
from typing import Callable, Sequence

from .cache import TaskMainCache
from .config import TaskMainConfig, STAGE_C_VERSION
from .future_access import FutureAccessAudit
from .load import CommittedTokenLoad
from .metrics import build_summary, target_feasibility_summary
from .records import EventRecord, OpportunityRecord, RequestRecord, RunResult
from .routing import affinity_route, least_other_pod, reactive_gate
from .trace import TraceRequest, full_page_prefix, validate_requests
from .transfer import IndependentTransfers
from .candidates import CandidateUniverse
from .history import ExternalDemandHistory, FutureDemandIndex, ReuseHistory
from .policies import Policy
from .proactive import ProactiveController


@dataclass(frozen=True)
class _Assignment:
    request: TraceRequest
    affinity_source: int
    final_pod: int
    route_hit_pages: int
    final_hit_pages: int
    loads_before_route: tuple[int, ...]
    account_time: float
    transfer_id: int | None = None
    fallback_reason: str | None = None
    target_feasibility_evaluated: bool = False
    structural_target_count: int | None = None
    feasible_target_count: int | None = None
    selected_target_load_rank: int | None = None
    absolute_lowest_load_target_feasible: bool | None = None


class TaskMainEngine:
    """Two baseline routers; time advances only through arrivals and wire ready."""

    def __init__(self, config: TaskMainConfig) -> None:
        self.config = config
        self.caches = [TaskMainCache(config.capacity_pages) for _ in range(config.num_pods)]
        self.load = CommittedTokenLoad(config.num_pods)
        self.transfers = IndependentTransfers(self.caches)
        self._ready: list[tuple[float, int, int]] = []
        self._pending: dict[int, _Assignment] = {}
        self._requests: list[RequestRecord] = []
        self._opportunities: list[OpportunityRecord] = []
        self._events: list[EventRecord] = []
        self._completed: set[int] = set()
        self._has_run = False
        self.universe = CandidateUniverse()
        self.demand_history = ExternalDemandHistory()
        self.reuse_history = ReuseHistory()
        self.future_index = None
        self.proactive = None
        self.future_access = FutureAccessAudit()
        self.copy_observations = []

    def _reactive_assignment(self, request: TraceRequest, source: int, hit_pages: int,
                             loads: tuple[int, ...], now: float) -> _Assignment:
        absolute_target = least_other_pod(source, loads)
        structural_count = len(loads) - 1
        if (absolute_target is None or
                not reactive_gate(loads[source], loads[absolute_target], self.config.theta)):
            return _Assignment(request, source, source, hit_pages, hit_pages, loads, now,
                               structural_target_count=structural_count)
        transferable = full_page_prefix(request, hit_pages)
        if not transferable:
            return _Assignment(request, source, source, hit_pages, hit_pages, loads, now,
                               structural_target_count=structural_count)

        ordered = sorted((pod for pod in range(len(loads)) if pod != source),
                         key=lambda pod: (loads[pod], pod))
        feasible = []
        for target in ordered:
            if self.caches[target].lookup(transferable) == len(transferable):
                # A direct target needs no transfer buffer or capacity plan.
                feasible.append((target, None, True))
                continue
            preflight = self.transfers.preflight_chain(transferable, source, target)
            if preflight.plan is not None:
                feasible.append((target, preflight.plan, False))
        diagnostics = dict(
            target_feasibility_evaluated=True,
            structural_target_count=len(ordered), feasible_target_count=len(feasible),
            selected_target_load_rank=None,
            absolute_lowest_load_target_feasible=any(row[0] == ordered[0] for row in feasible),
        )
        if not feasible:
            return _Assignment(
                request, source, source, hit_pages,
                self.caches[source].lookup(request.block_ids), loads, now,
                fallback_reason="REACTIVE_FALLBACK_CAPACITY", **diagnostics,
            )
        target, plan, direct = min(feasible, key=lambda row: (loads[row[0]], row[0]))
        diagnostics["selected_target_load_rank"] = ordered.index(target) + 1
        if direct:
            target_hit = self.caches[target].lookup(request.block_ids)
            return _Assignment(request, source, target, hit_pages, target_hit, loads, now,
                               **diagnostics)
        return self._start_reactive_plan(request, source, target, hit_pages, loads, now,
                                         plan, **diagnostics)

    def _start_reactive_plan(self, request: TraceRequest, source: int, target: int,
                             hit_pages: int, loads: tuple[int, ...], now: float, plan,
                             **diagnostics) -> _Assignment:
        transfer = self.transfers.start(plan, request.request_id, now)
        assignment = _Assignment(
            request, source, target, hit_pages, len(transfer.transferable_chain),
            loads, now, transfer.transfer_id, **diagnostics,
        )
        self._pending[request.request_id] = assignment
        heapq.heappush(self._ready, (transfer.ready_time, request.request_id, transfer.transfer_id))
        return assignment

    def _copy_or_fallback(self, request: TraceRequest, source: int, target: int,
                          hit_pages: int, loads: tuple[int, ...], now: float) -> _Assignment:
        preflight = self.transfers.preflight(request, hit_pages, source, target)
        if preflight.plan is None:
            return _Assignment(
                request, source, source, hit_pages,
                self.caches[source].lookup(request.block_ids), loads, now,
                fallback_reason=preflight.failure_reason,
            )
        # Commit only the Prefix guaranteed by this transfer, not a hypothetical
        # later arrival's publication. This hit/miss decision is never replanned.
        return self._start_reactive_plan(request, source, target, hit_pages, loads, now,
                                         preflight.plan)

    def _arrive(self, request: TraceRequest, now: float) -> None:
        self._events.append(EventRecord(now, "REQUEST_ARRIVAL", request.request_id))
        self.universe.observe(request)
        self.demand_history.observe(request)
        loads = self.load.vector(now)
        affinity = affinity_route(request, self.caches, loads)
        if self.config.routing_policy == "R_REQ_KV_TASK":
            assignment = self._reactive_assignment(request, affinity.pod_id,
                                                    affinity.hit_pages, loads, now)
        else:
            assignment = _Assignment(request, affinity.pod_id, affinity.pod_id,
                                     affinity.hit_pages, affinity.hit_pages, loads, now)
        miss_tokens = request.input_tokens - request.hit_tokens(assignment.final_hit_pages)
        self.load.commit(request.request_id, assignment.final_pod, miss_tokens, now)
        self._events.append(EventRecord(now, "FINAL_ASSIGNMENT", request.request_id,
                                        assignment.transfer_id))
        if assignment.transfer_id is not None:
            self._events.append(EventRecord(now, "TRANSFER_START", request.request_id,
                                            assignment.transfer_id))
        else:
            self._finish(assignment, now)

    def _finish(self, assignment: _Assignment, now: float, already_protected: bool = False) -> None:
        request = assignment.request
        if request.request_id in self._completed:
            raise AssertionError("request completed twice")
        cache = self.caches[assignment.final_pod]
        used_path = request.block_ids[:assignment.final_hit_pages]
        if cache.lookup(used_path) != len(used_path):
            raise AssertionError("committed request Prefix is no longer available")
        if not already_protected:
            cache.pin(used_path)
        cache.record_reuse(request.request_id, used_path, now)
        self.reuse_history.observe(request, assignment.final_hit_pages, now)
        self._events.append(EventRecord(now, "REQUEST_COMPLETE", request.request_id,
                                        assignment.transfer_id))
        plan = cache.plan_prompt(request.block_ids)
        skipped = plan is None
        inserted = 0 if plan is None else cache.publish_prompt(plan, now)
        status = "CACHE_ADMISSION_SKIP" if skipped else "PUBLISHED"
        self._events.append(EventRecord(now, status, request.request_id, assignment.transfer_id))
        cache.unpin(used_path)
        split = self.config.split_at(request.arrival_ms)
        final_tokens = request.hit_tokens(assignment.final_hit_pages)
        self._requests.append(RequestRecord(
            request.request_id, self.config.workload, request.arrival_ms, now,
            self.config.routing_policy, assignment.affinity_source, assignment.final_pod,
            request.input_tokens, request.output_tokens,
            assignment.route_hit_pages, request.hit_tokens(assignment.route_hit_pages),
            assignment.final_hit_pages, final_tokens, request.input_tokens - final_tokens,
            assignment.loads_before_route, assignment.account_time, assignment.transfer_id,
            assignment.fallback_reason, skipped, status, inserted, split,
            transferable_pages=len(full_page_prefix(request, assignment.route_hit_pages)),
            route_has_partial_tail=(assignment.route_hit_pages > 0 and
                                    request.block_valid_tokens[assignment.route_hit_pages - 1] < 512),
            reactive_gate_passed=(self.config.routing_policy == "R_REQ_KV_TASK" and
                (target := least_other_pod(assignment.affinity_source,
                                          assignment.loads_before_route)) is not None and
                reactive_gate(assignment.loads_before_route[assignment.affinity_source],
                              assignment.loads_before_route[target], self.config.theta)),
            target_feasibility_evaluated=assignment.target_feasibility_evaluated,
            structural_target_count=assignment.structural_target_count,
            feasible_target_count=assignment.feasible_target_count,
            selected_target_load_rank=assignment.selected_target_load_rank,
            absolute_lowest_load_target_feasible=assignment.absolute_lowest_load_target_feasible,
        ))
        self._completed.add(request.request_id)
        opportunity = OpportunityRecord(
            len(self._opportunities), request.request_id, now, self.config.workload,
            self.load.vector(now), tuple(cache.summary() for cache in self.caches), split,
        )
        self._events.append(EventRecord(now, "OPPORTUNITY", request.request_id,
                                        assignment.transfer_id))
        if self.proactive is not None:
            with self.future_access.decision(self.config.proactive_policy):
                opportunity, transfer = self.proactive.execute(opportunity)
            if transfer is not None:
                heapq.heappush(self._ready, (transfer.ready_time, request.request_id, transfer.transfer_id))
                self._events.append(EventRecord(now, "TRANSFER_START", request.request_id,
                                                transfer.transfer_id))
        self._opportunities.append(opportunity)
        for item in self.caches:
            item.assert_invariants()

    def run(self, requests: Sequence[TraceRequest], *,
            oracle_observation_requests: Sequence[TraceRequest] | None = None,
            progress_callback: Callable[[int, int, float], None] | None = None,
            progress_every_requests: int = 0) -> RunResult:
        if self._has_run:
            raise ValueError("create a new TaskMainEngine for each replay")
        requests = validate_requests(requests)
        if type(progress_every_requests) is not int or progress_every_requests < 0:
            raise ValueError("progress_every_requests must be a nonnegative integer")
        self._has_run = True
        if self.config.proactive_policy == "FUTURE_DEMAND":
            observation = requests
            if oracle_observation_requests is not None:
                observation = validate_requests(oracle_observation_requests)
                if any(r.arrival_ms > self.config.visibility_end_ms for r in observation):
                    raise ValueError("Oracle observation exceeds visibility")
                if [r for r in observation if r.arrival_ms < self.config.visibility_end_ms] != requests:
                    raise ValueError("Oracle observation must contain exactly the replay plus boundary-only requests")
            self.future_index = FutureDemandIndex(observation, self.config.visibility_end_ms)
        if self.config.proactive_policy != "NONE":
            self.proactive = ProactiveController(
                self.config, self.universe,
                Policy(self.config, self.demand_history, self.reuse_history, self.future_index),
                self.caches, self.transfers)
        index = 0
        now = 0.0
        while index < len(requests) or self._ready:
            arrival = requests[index].arrival_ms if index < len(requests) else float("inf")
            ready = self._ready[0][0] if self._ready else float("inf")
            now = min(arrival, ready)
            ready_requests = []
            # Publish ALL transfers at t before completing ANY waiting request.
            while self._ready and self._ready[0][0] == now:
                _, request_id, transfer_id = heapq.heappop(self._ready)
                transfer = self.transfers.complete(transfer_id, now)
                self._events.append(EventRecord(now, "TRANSFER_COMPLETE", request_id, transfer_id))
                if transfer.type == "PROACTIVE":
                    self.proactive.complete(transfer)
                    if self.config.protocol_version == STAGE_C_VERSION:
                        self.copy_observations.append({
                            "transfer_id": transfer_id, "target": transfer.target,
                            "ready_time": now, "chain_id": transfer.transferable_chain,
                            "generations": tuple(self.caches[transfer.target].page_state(b)["generation"]
                                                 for b in transfer.transferable_chain),
                        })
                else:
                    ready_requests.append(self._pending.pop(request_id))
            ready_requests.sort(key=lambda item: item.request.request_id)
            # Protect ready requests' committed hits across the stable resume batch.
            # Earlier Prompt publication may evict other leaves, never a pending hit.
            for assignment in ready_requests:
                self.caches[assignment.final_pod].pin(
                    assignment.request.block_ids[:assignment.final_hit_pages]
                )
            for assignment in ready_requests:
                self._finish(assignment, now, already_protected=True)
            while index < len(requests) and requests[index].arrival_ms == now:
                with self.future_access.decision(self.config.proactive_policy):
                    self._arrive(requests[index], now)
                index += 1
                if (progress_callback is not None and progress_every_requests and
                        (index % progress_every_requests == 0 or index == len(requests))):
                    progress_callback(index, len(requests), now)
        if self._pending:
            raise AssertionError("undrained reactive requests")
        self._requests.sort(key=lambda item: item.request_id)
        summary, validation = build_summary(
            self.config, self._requests, self.transfers.records, self._opportunities,
            self.caches, self.load, {request.request_id for request in requests}, self._events,
        )
        actions = self.proactive.actions if self.proactive else []
        decisions = self.proactive.decisions if self.proactive else []
        final_state = {
            "cache": [cache.snapshot() for cache in self.caches],
            "load_vector": self.load.vector(now),
            "load_entries": [asdict(self.load.entries[key]) for key in sorted(self.load.entries)],
            "transfers": [asdict(row) for row in self.transfers.records],
            "pending_requests": sorted(self._pending), "ready_events": list(self._ready),
        }
        if self.config.protocol_version == STAGE_C_VERSION:
            final_state.update(
                active_load_histories=[[asdict(entry) for entry in history] for history in self.load._histories],
                load_last_time=self.load._last_time,
                candidate_universe=sorted(self.universe.seen),
                demand_history=[(chain, events) for chain, events in sorted(self.demand_history.events.items())],
                demand_request_ids=sorted(self.demand_history.request_ids),
                reuse_history=[(chain, events) for chain, events in sorted(self.reuse_history.events.items())],
                reuse_request_ids=sorted(self.reuse_history.request_ids),
            )
        summary["final_state_digest"] = hashlib.sha256(json.dumps(
            final_state, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
        summary["proactive_copy_count"] = len(actions)
        summary["policy_diagnostics"] = {
            "policy": self.config.proactive_policy,
            "future_access": self.future_access.snapshot(),
            "cost_gate_rejected_count": sum(row.status == "COST_GATE_REJECTED" for row in decisions),
        }
        summary["target_feasibility_diagnostics"]["PROACTIVE"] = target_feasibility_summary(decisions)
        validation["stage_b_policies_implemented"] = True
        validation["checks"]["proactive_action_cap"] = (
            len({row.opportunity_id for row in actions}) == len(actions))
        validation["checks"]["proactive_actions_complete"] = all(row.status == "COMPLETED" for row in actions)
        validation["checks"]["proactive_action_transfer_conservation"] = (
            {row.transfer_id for row in actions} ==
            {row.transfer_id for row in self.transfers.records if row.type == "PROACTIVE"})
        validation["checks"]["target_feasibility_diagnostics_consistent"] = all(
            (not row.target_feasibility_evaluated and
             row.feasible_target_count is None and
             row.selected_target_load_rank is None and
             row.absolute_lowest_load_target_feasible is None) or
            (row.target_feasibility_evaluated and
             row.structural_target_count is not None and row.structural_target_count > 0 and
             row.feasible_target_count is not None and
             0 <= row.feasible_target_count <= row.structural_target_count and
             ((row.feasible_target_count == 0 and row.selected_target_load_rank is None) or
              (row.feasible_target_count > 0 and row.selected_target_load_rank is not None and
               1 <= row.selected_target_load_rank <= row.structural_target_count)) and
             row.absolute_lowest_load_target_feasible is not None)
            for row in [*self._requests, *decisions]
        )
        if not all(validation["checks"].values()):
            raise AssertionError(validation)
        result = RunResult(self._requests, self.transfers.records, self._opportunities,
                           self._events, summary, validation, actions, decisions, final_state)
        if self.config.protocol_version == STAGE_C_VERSION:
            result.copy_observation_records = self.copy_observations
            result.reuse_observation_records = [dict(asdict(event), target=pod)
                for pod, cache in enumerate(self.caches) for event in cache.reuse_events]
            from .final_metrics import finalize_metrics
            from .validation import validate_metrics
            from .equivalence import logical_state_projection, projection_digest
            summary["final_metrics"] = finalize_metrics(result, self.config)
            summary["final_logical_state_digest"] = projection_digest(logical_state_projection(result))
            validation["checks"].update(validate_metrics(result, self.config))
            validation["status"] = "PASS" if all(validation["checks"].values()) else "INVALID"
            if validation["status"] != "PASS":
                raise AssertionError(validation)
        return result
