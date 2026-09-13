from __future__ import annotations

import argparse
import heapq
import json
import hashlib
import subprocess
from dataclasses import dataclass
from pathlib import Path

from .cache import CapacityAdmissionError
from .config import SimulatorConfig
from .events import Event, EventType, RequestState
from .metrics import RequestResult, distribution, summarize, write_outputs
from .pod import Pod
from .routing import RouteDecision, route_request
from .service import PrefillServiceModel, ServiceEstimate
from .trace import TraceRequest, load_trace
from .transfer import Transfer, transferable_prefix_pages
from .tickets import ReactiveNodeGate, ReactiveTransferTicket, TicketState
from .oracle import (CandidateGenerator, FutureDemandIndex, OracleLeakageError,
                     ProactiveCopy, ReplicaGeneration, TokenBucket, trigger_times)
from .strategies import (ExternalDemandHistory, cost_aware_score, quantile,
                         request_kv_task_gate)


@dataclass
class _Runtime:
    request: TraceRequest
    state: RequestState | None = None
    pod_id: int | None = None
    h_route_pages: int | None = None
    h_route_tokens: int | None = None
    queued_estimated_service_ms: float | None = None
    service_start_ms: float | None = None
    estimate: ServiceEstimate | None = None
    transfer: Transfer | None = None
    p2p_assisted: bool = False
    ticket: ReactiveTransferTicket | None = None


class SimulatorEngine:
    def __init__(self, config: SimulatorConfig) -> None:
        self.config = config

    def run(self, requests: list[TraceRequest]) -> tuple[list[RequestResult], dict]:
        future_index = FutureDemandIndex(requests)
        demand_history = ExternalDemandHistory(requests)
        if self.config.proactive_enabled:
            def audit_distribution(values):
                ordered_values = sorted(values)
                def pct(q):
                    if not ordered_values:
                        return None
                    position = (len(ordered_values) - 1) * q
                    low, high = int(position), min(int(position) + 1, len(ordered_values) - 1)
                    fraction = position - low
                    return ordered_values[low] * (1 - fraction) + ordered_values[high] * fraction
                return {"min": ordered_values[0] if ordered_values else None,
                        "p10": pct(.10), "p25": pct(.25), "p50": pct(.50),
                        "p75": pct(.75), "p90": pct(.90), "p95": pct(.95),
                        "p99": pct(.99), "max": ordered_values[-1] if ordered_values else None}
            if self.config.split_config is None:
                raise ValueError("proactive policies require split boundaries")
            if self.config.summary_split == "DEVELOPMENT":
                requests = [r for r in requests if r.split == "DEVELOPMENT"]
            elif not (self.config.summary_split == "EVALUATION"
                      and self.config.protocol_version == "TASKMAIN_V1_1"):
                raise ValueError(
                    "proactive policies outside DEVELOPMENT require the frozen "
                    "TASKMAIN_V1_1 formal Evaluation protocol"
                )
        if self.config.o2p_arrival_cutoff_ms is not None:
            requests = [r for r in requests
                        if r.arrival_ms <= self.config.o2p_arrival_cutoff_ms]
        runtimes = {request.request_id: _Runtime(request) for request in requests}
        if len(runtimes) != len(requests):
            raise ValueError("request_id values must be unique")
        pods = [Pod(i, self.config.cache_capacity_pages) for i in range(self.config.num_pods)]
        service = PrefillServiceModel(
            self.config.base_latency_ms, self.config.prefill_tokens_per_second
        )
        events: list[Event] = []
        next_event_id = 0
        next_transfer_id = 0
        next_ticket_id = 0
        node_gate = ReactiveNodeGate()
        candidate_generator = CandidateGenerator()
        token_bucket = TokenBucket(
            self.config.proactive_byte_rate, self.config.proactive_burst_bytes,
            initial_tokens=self.config.proactive_initial_tokens,
        )
        proactive_actions: dict[int, ProactiveCopy] = {}
        proactive_transfer_ids: set[int] = set()
        proactive_inflight: set[tuple[int, int]] = set()
        current_generations: dict[tuple[int, int], ReplicaGeneration] = {}
        generation_sequence: dict[tuple[int, int], int] = {}
        seen_evictions = [0] * len(pods)
        proactive_stats = {
            "trigger_count": 0, "censored_trigger_count": 0,
            "oracle_future_reads_outside_allowed_split": 0,
            "candidate_count": 0, "positive_candidate_count": 0,
            "future_demand_scores": [], "actions_started": 0,
            "actions_completed": 0, "skip_endpoint_busy": 0,
            "skip_reactive_priority": 0, "skip_capacity": 0,
            "skip_no_free_capacity": 0,
            "skip_budget": 0, "skip_action_cap": 0,
            "budget_insufficient_current_tokens": 0,
            "action_exceeds_bucket_cap": 0,
            "wire_pages": 0, "wire_bytes": 0, "newly_resident_pages": 0,
            "duplicate_wire_pages": 0, "evicted_pages_caused_at_admission": 0,
            "endpoint_busy_time_ms": 0.0,
            "reactive_endpoint_blocked_by_proactive_ms": 0.0,
            "reactive_ticket_blocked_by_proactive_count": 0,
            "selected_candidate_ranks": [], "rank_gt_1_count": 0,
            "top_k_exhausted_count": 0, "no_positive_candidate_count": 0,
            "recency_thresholds": [], "recency_tie_counts": [],
            "cost_benefit_terms": [], "cost_load_penalties": [],
            "cost_transfer_penalties": [], "cost_final_scores": [],
            "cost_filter_rejected_count": 0,
            "selected_cost_scores": [],
            "cost_reference_values": [], "cost_reference_transfer_ms": [],
            "oracle_future_reads": 0,
            "oracle_future_reads_outside_allowed_trace": 0,
            "oracle_future_reads_outside_protocol": 0,
            "move_actions_started": 0, "move_actions_completed": 0,
            "move_source_release_attempts": 0,
            "move_source_pages_freed": 0, "move_source_tokens_freed": 0,
            "move_zero_release_count": 0, "move_partial_release_count": 0,
            "move_full_release_count": 0,
            "move_release_blocked_shared": 0,
            "move_release_blocked_pinned": 0,
            "move_release_blocked_active_or_transfer": 0,
            "o2p_candidate_states": [], "o2p_forced_action_success": False,
            "o2p_forced_action_failure_reason": None,
            "o2a_candidate_states": [],
            "o2a_closed_state_fingerprints": {},
            "o2a_closed_validation_fingerprints": {},
        }
        task_activity = {
            "total_requests": 0, "task_gate_true": 0,
            "stayed_local_gate_false": 0,
            "stayed_local_no_transferable_prefix": 0,
            "stayed_local_source_equals_target": 0,
        }
        start_scheduled = [False] * len(pods)
        transfer_stats = {
            "transfer_attempts": 0, "transfer_admitted": 0, "transfer_completed": 0,
            "endpoint_busy_fallback": 0, "capacity_fallback": 0,
            "matched_prefix_pages": 0, "transferable_prefix_pages": 0,
            "wire_pages": 0, "wire_bytes": 0, "newly_resident_pages": 0,
            "duplicate_wire_pages": 0, "transfer_latencies_ms": [],
            "p2p_assisted_requests": 0, "p2p_assisted_h_used_tokens": 0,
            "source_pod_distribution": [0] * len(pods),
            "target_pod_distribution": [0] * len(pods),
            "candidate_requests": 0, "ticket_created": 0,
            "transfer_admission_timeouts": 0, "timeout_wait_ms": 0.0,
            "replan_count": 0, "source_replan_count": 0, "target_replan_count": 0,
            "endpoint_wait": 0, "capacity_wait": 0,
            "capacity_blocked_at_admission": 0,
            "no_cache_source": 0, "cache_gate_reject": 0,
            "source_is_best_target": 0, "relative_gate_reject": 0,
            "absolute_gate_reject": 0, "target_already_has_prefix": 0,
            "no_legal_target": 0, "replan_cancel": 0,
            "replan_direct_route": 0, "successful_transfer": 0,
            "fallback": 0, "ticket_admission_waits_ms": [],
            "ticket_target_estimate_errors_ms": [],
            "admission_no_cache_source": 0, "admission_no_legal_target": 0,
            "admission_target_already_has_prefix": 0,
            "admission_cache_gate_reject": 0,
            "admission_relative_gate_reject": 0,
            "admission_absolute_gate_reject": 0,
        }

        def schedule(time_ms: float, kind: EventType, request_id: int,
                     pod_id: int | None = None) -> None:
            nonlocal next_event_id
            heapq.heappush(events, Event.create(time_ms, next_event_id, kind, request_id))
            next_event_id += 1
            if kind is EventType.SERVICE_START:
                if pod_id is None or start_scheduled[pod_id]:
                    raise AssertionError("invalid duplicate SERVICE_START")
                start_scheduled[pod_id] = True

        def enqueue_ready(runtime: _Runtime, decision: RouteDecision,
                          now: float, fallback: bool = False) -> None:
            request = runtime.request
            pod = pods[decision.pod_id]
            if request.request_id in load_owner:
                raise AssertionError("request already has GPU Load ownership")
            if runtime.ticket is not None and runtime.ticket.remote_pressure_owner_pod >= 0:
                raise AssertionError("GPU Load overlaps RemotePressure")
            runtime.state = RequestState.FALLBACK if fallback else RequestState.READY
            runtime.pod_id = pod.pod_id
            runtime.h_route_pages = decision.h_route_pages
            runtime.h_route_tokens = decision.h_route_tokens
            estimate = service.estimate(request, decision.h_route_pages).service_ms
            runtime.queued_estimated_service_ms = estimate
            pod.enqueue(request.request_id, estimate)
            load_owner[request.request_id] = pod.pod_id
            runtime.state = RequestState.READY
            pod.observe_load(now)
            if pod.idle and not start_scheduled[pod.pod_id]:
                schedule(now, EventType.SERVICE_START, pod.queue[0].request_id, pod.pod_id)

        def fallback_aff(runtime: _Runtime, now: float) -> None:
            ticket = runtime.ticket
            if ticket is not None and ticket.remote_pressure_owner_pod >= 0:
                pods[ticket.remote_pressure_owner_pod].remove_remote_pressure(ticket.ticket_id)
                ticket.remote_pressure_owner_pod = -1
            if ticket is not None:
                node_gate.remove(ticket)
                if ticket.state not in {TicketState.TIMED_OUT, TicketState.CANCELLED}:
                    ticket.state = TicketState.FALLBACK
            transfer_stats["fallback"] += 1
            decision = route_request("R_AFF", runtime.request, pods, now)
            enqueue_ready(runtime, decision, now, fallback=True)

        def best_target(request: TraceRequest, source_id: int, now: float) -> Pod | None:
            candidates = [p for p in pods if p.pod_id != source_id]
            if not candidates:
                return None
            return min(candidates, key=lambda p: (
                p.load_ms(now), -p.cache.lookup(request.block_ids), p.pod_id
            ))

        def create_b0_ticket(runtime: _Runtime, now: float,
                             source_decision: RouteDecision, target: Pod) -> None:
            nonlocal next_ticket_id
            request = runtime.request
            transferable = transferable_prefix_pages(
                request, source_decision.h_route_pages,
                self.config.partial_page_mode, self.config.page_tokens
            )
            planned = max(transferable, target.cache.lookup(request.block_ids))
            planned_tokens = request.hit_tokens(planned)
            target_estimate = service.estimate(request, planned).service_ms
            ticket = ReactiveTransferTicket(
                next_ticket_id, request.request_id, now,
                now + self.config.admission_timeout_ms,
                source_decision.pod_id, target.pod_id,
                source_decision.h_route_pages, source_decision.h_route_tokens,
                transferable, transferable, planned, planned_tokens,
                pods[source_decision.pod_id].load_ms(now), target.load_ms(now),
                target.pod_id, target_estimate, next_ticket_id,
            )
            next_ticket_id += 1
            runtime.ticket = ticket
            target.add_remote_pressure(ticket.ticket_id, target_estimate)
            node_gate.enqueue(ticket)
            transfer_stats["ticket_created"] += 1
            transfer_stats["transfer_attempts"] += 1
            schedule(now, EventType.TRANSFER_ENQUEUE, request.request_id)
            schedule(ticket.deadline_ms, EventType.TRANSFER_TIMEOUT, request.request_id)

        def b0_arrival(runtime: _Runtime, now: float) -> None:
            request = runtime.request
            source_decision = route_request("R_AFF", request, pods, now)
            if source_decision.h_route_pages == 0:
                if self.config.routing_policy == "R_REQ_KV_TASK":
                    task_activity["stayed_local_no_transferable_prefix"] += 1
                transfer_stats["no_cache_source"] += 1
                fallback_aff(runtime, now)
                return
            transfer_stats["candidate_requests"] += 1
            transferable = transferable_prefix_pages(
                request, source_decision.h_route_pages,
                self.config.partial_page_mode, self.config.page_tokens
            )
            if self.config.routing_policy == "R_REQ_KV_TASK":
                target = min(pods, key=lambda p: (p.load_ms(now), p.pod_id))
                source = pods[source_decision.pod_id]
                if transferable <= 0:
                    task_activity["stayed_local_no_transferable_prefix"] += 1
                    transfer_stats["source_is_best_target"] += 1
                    fallback_aff(runtime, now)
                    return
                if target.pod_id == source.pod_id:
                    task_activity["stayed_local_source_equals_target"] += 1
                    transfer_stats["source_is_best_target"] += 1
                    fallback_aff(runtime, now)
                    return
                if not request_kv_task_gate(
                    source.load_ms(now), target.load_ms(now), transferable,
                    source.pod_id, target.pod_id, self.config.theta_simple
                ):
                    task_activity["stayed_local_gate_false"] += 1
                    transfer_stats["relative_gate_reject"] += 1
                    fallback_aff(runtime, now)
                    return
                task_activity["task_gate_true"] += 1
                if target.cache.lookup(request.block_ids[:transferable]) >= transferable:
                    hit = target.cache.lookup(request.block_ids)
                    enqueue_ready(runtime, RouteDecision(target.pod_id, hit,
                                  request.hit_tokens(hit)), now)
                    return
                create_b0_ticket(runtime, now, source_decision, target)
                return
            hit_fraction = request.hit_tokens(transferable) / request.input_tokens
            if hit_fraction < self.config.cache_hit_threshold:
                transfer_stats["cache_gate_reject"] += 1
                fallback_aff(runtime, now)
                return
            target = best_target(request, source_decision.pod_id, now)
            if target is None:
                transfer_stats["no_legal_target"] += 1
                fallback_aff(runtime, now)
                return
            source = pods[source_decision.pod_id]
            if source.load_ms(now) <= target.load_ms(now):
                transfer_stats["source_is_best_target"] += 1
                fallback_aff(runtime, now)
                return
            if target.cache.lookup(request.block_ids[:transferable]) >= transferable:
                transfer_stats["target_already_has_prefix"] += 1
                hit = target.cache.lookup(request.block_ids)
                enqueue_ready(runtime, RouteDecision(target.pod_id, hit,
                              request.hit_tokens(hit)), now)
                return
            if source.load_ms(now) <= target.load_ms(now) * self.config.relative_load_threshold:
                transfer_stats["relative_gate_reject"] += 1
                fallback_aff(runtime, now)
                return
            if source.load_ms(now) - target.load_ms(now) < self.config.absolute_load_gap_ms:
                transfer_stats["absolute_gate_reject"] += 1
                fallback_aff(runtime, now)
                return
            create_b0_ticket(runtime, now, source_decision, target)

        for request in requests:
            schedule(request.arrival_ms, EventType.REQUEST_ARRIVAL, request.request_id)
        if self.config.proactive_enabled:
            complete_trigger_end = self.config.oracle_visibility_end_ms
            if self.config.proactive_strategy == "FUTURE_DEMAND":
                complete_trigger_end -= self.config.oracle_horizon_ms
            if self.config.proactive_trigger_latest_ms is not None:
                complete_trigger_end = min(complete_trigger_end,
                                           self.config.proactive_trigger_latest_ms)
            if self.config.proactive_action == "O2A_CLOSED_LOOP":
                trigger_iter = sorted({self.config.o2a_probe_time_ms,
                                       *(action[0] for action in self.config.o2a_committed_actions)})
            else:
                trigger_iter = (
                [self.config.o2p_forced_time_ms]
                if self.config.proactive_action in {"O2P_NO_COPY", "O2P_FORCED_COPY"}
                else trigger_times(self.config.trigger_period_ms,
                                   self.config.trigger_phase_ms,
                                   self.config.oracle_visibility_end_ms)
                )
            for trigger in trigger_iter:
                if trigger <= complete_trigger_end:
                    schedule(trigger, EventType.TRIGGER_TICK, -1)
                else:
                    proactive_stats["censored_trigger_count"] += 1

        results: list[RequestResult] = []
        completed_ids: set[int] = set()
        load_owner: dict[int, int] = {}
        first_arrival_ms = min((r.arrival_ms for r in requests), default=0.0)
        last_accounting_ms = first_arrival_ms
        last_completion_ms = first_arrival_ms
        pressure_intervals: list[dict] = []

        def state_fingerprint(now: float) -> str:
            token_bucket.refill(now)
            def object_state(value):
                if value is None:
                    return None
                return tuple(sorted((key, repr(item)) for key, item in vars(value).items()))
            snapshot = (
                now, token_bucket.tokens,
                tuple((p.pod_id, p.cache.diagnostic_snapshot(), tuple(p.queue),
                       p.running_request_id, p.running_start_ms, p.running_done_ms,
                       p.active_transfer_id, tuple(sorted(p.remote_pressure.items())))
                      for p in pods),
                tuple(sorted((rid, rt.state.name if rt.state else None, rt.pod_id,
                              rt.h_route_pages, rt.queued_estimated_service_ms,
                              rt.service_start_ms, object_state(rt.estimate),
                              object_state(rt.transfer), object_state(rt.ticket),
                              rt.p2p_assisted)
                             for rid, rt in runtimes.items() if rt.state is not None)),
                tuple(object_state(ticket) for ticket in node_gate.queued()),
                tuple(sorted(proactive_inflight)),
            )
            return hashlib.sha256(repr(snapshot).encode()).hexdigest()

        def validation_fingerprint(now: float) -> str:
            """Smoke-only full checkpoint audit, including event queue and counters."""
            structural = state_fingerprint(now)
            counters = tuple(sorted(
                (key, repr(value)) for key, value in proactive_stats.items()
                if key not in {"o2a_candidate_states", "o2a_closed_state_fingerprints",
                               "o2a_closed_validation_fingerprints",
                               "o2p_state_fingerprint", "o2p_forced_action_success",
                               "o2p_forced_action_failure_reason"}
            ))
            snapshot = (
                structural,
                tuple(sorted((item.time_ms, item.priority, item.event_id,
                              item.event_type.value, item.request_id) for item in events)),
                tuple(sorted((key, repr(value)) for key, value in transfer_stats.items())),
                tuple(sorted(task_activity.items())), counters,
            )
            return hashlib.sha256(repr(snapshot).encode()).hexdigest()

        def reconcile_evictions() -> None:
            for pod in pods:
                records = pod.cache.eviction_records
                for time_ms, block_id in records[seen_evictions[pod.pod_id]:]:
                    generation = current_generations.pop((pod.pod_id, block_id), None)
                    if generation is not None:
                        generation.eviction_time = time_ms
                seen_evictions[pod.pod_id] = len(records)

        while events:
            event = heapq.heappop(events)
            now = event.time_ms
            if now > last_accounting_ms:
                start_loads = [p.load_ms(last_accounting_ms) for p in pods]
                end_loads = [p.load_ms(now) for p in pods]
                pressure_intervals.append({
                    "start_ms": last_accounting_ms, "end_ms": now,
                    "start_loads": start_loads, "end_loads": end_loads,
                    "queue_ids": [tuple(item.request_id for item in p.queue) for p in pods],
                    "queue_lengths": [len(p.queue) for p in pods],
                    "running": [not p.idle for p in pods],
                    "resident_pages": [p.cache.resident_pages for p in pods],
                    "active_private_pages": [p.cache.active_private_pages for p in pods],
                    "transfer_temporary_pages": [p.cache.transfer_temporary_pages for p in pods],
                    "memory_used_pages": [p.cache.memory_used_pages for p in pods],
                })
                for pod in pods:
                    pod.integrate_load(last_accounting_ms, now)
                last_accounting_ms = now

            for pod in pods:
                pod.cache.accounting_time_ms = now

            reconcile_evictions()

            if event.event_type is EventType.TRIGGER_TICK:
                proactive_stats["trigger_count"] += 1
                closed_action = None
                if self.config.proactive_action == "O2A_CLOSED_LOOP":
                    closed_action = next((action for action in self.config.o2a_committed_actions
                                          if action[0] == now), None)
                closed_probe = (self.config.proactive_action == "O2A_CLOSED_LOOP"
                                and self.config.o2a_probe_time_ms == now
                                and closed_action is None)
                forced_prefix = (closed_action[1] if closed_action is not None
                                 else self.config.o2p_forced_prefix_id)
                forced_source = (closed_action[2] if closed_action is not None
                                 else self.config.o2p_forced_source_pod)
                forced_target = (closed_action[3] if closed_action is not None
                                 else self.config.o2p_forced_target_pod)
                forced_copy = (self.config.proactive_action == "O2P_FORCED_COPY"
                               or closed_action is not None)
                ranked = []
                online_hashes = candidate_generator.online_hashes(pods)
                proactive_stats["candidate_count"] += len(online_hashes)
                for hash_id in online_hashes:
                    if self.config.proactive_strategy == "FUTURE_DEMAND":
                        try:
                            score = future_index.future_count(
                                hash_id, now, self.config.oracle_horizon_ms,
                                self.config.oracle_visibility_end_ms,
                            )
                            proactive_stats["oracle_future_reads"] += 1
                        except OracleLeakageError:
                            proactive_stats["oracle_future_reads_outside_allowed_split"] += 1
                            proactive_stats["oracle_future_reads_outside_protocol"] += 1
                            continue
                    elif self.config.proactive_strategy == "PERSIST":
                        score = demand_history.persistence(
                            hash_id, now, self.config.persistence_history_ms
                        )
                    elif self.config.proactive_strategy == "RECENCY":
                        score = demand_history.recency(
                            hash_id, now, self.config.recency_decay_seconds
                        )
                    else:
                        score = demand_history.value_hat(
                            hash_id, now, self.config.persistence_history_ms,
                            self.config.oracle_horizon_ms,
                        )
                    if score > 0:
                        candidate = candidate_generator.materialize(hash_id, pods)
                        if candidate is None:
                            continue
                        ranked.append((score, candidate))
                        proactive_stats["future_demand_scores"].append(score)
                        if self.config.proactive_strategy == "COST_AWARE":
                            pages = candidate.depth - (
                                candidate.block_valid_tokens[-1] < self.config.page_tokens
                            )
                            if pages > 0:
                                wire = pages * self.config.page_bytes
                                proactive_stats["cost_reference_values"].append(score)
                                proactive_stats["cost_reference_transfer_ms"].append(
                                    self.config.control_latency_ms + wire
                                    / self.config.effective_bandwidth_bytes_per_s * 1000
                                )
                proactive_stats["positive_candidate_count"] += len(ranked)
                ranked.sort(key=lambda item: (-item[0], -item[1].depth, item[1].hash_id))
                if not ranked:
                    proactive_stats["no_positive_candidate_count"] += 1
                if self.config.proactive_strategy in {"PERSIST", "COST_AWARE"}:
                    ranked = ranked[:self.config.persistence_top_k]
                elif self.config.proactive_strategy == "RECENCY" and ranked:
                    threshold = quantile([item[0] for item in ranked],
                                         self.config.recency_selection_quantile)
                    proactive_stats["recency_thresholds"].append(threshold)
                    proactive_stats["recency_tie_counts"].append(
                        sum(abs(item[0] - threshold) < 1e-12 for item in ranked)
                    )
                    ranked = [item for item in ranked if item[0] >= threshold]
                if self.config.proactive_action == "O2P_NO_COPY":
                    proactive_stats["o2p_state_fingerprint"] = state_fingerprint(now)
                    continue
                if self.config.proactive_action in {"O2P_PROBE", "O2A_PROBE"} or closed_probe:
                    fingerprint = state_fingerprint(now)
                    eligible = []
                    for score, candidate in ranked:
                        transferable = candidate.depth - (
                            candidate.block_valid_tokens[-1] < self.config.page_tokens
                        )
                        if transferable <= 0:
                            continue
                        sources = [pods[i] for i in candidate.source_pods
                                   if not pods[i].transfer_busy]
                        if not sources:
                            continue
                        source = min(sources, key=lambda p: (p.load_ms(now), p.pod_id))
                        wire_bytes = transferable * self.config.page_bytes
                        path = candidate.block_ids[:transferable]
                        legal_targets = [p for p in pods if not node_gate.queued()
                                         and p.pod_id != source.pod_id
                                         and p.cache.lookup(path) < transferable
                                         and not p.transfer_busy
                                         and p.cache.can_admit_temporary(transferable)]
                        affordable = token_bucket.can_afford(wire_bytes, now)
                        eligible.append({
                            "prefix_id": candidate.hash_id,
                            "prefix_depth_pages": candidate.depth,
                            "transferable_pages": transferable,
                            "prefix_valid_tokens": candidate.valid_prefix_tokens,
                            "source_pod": source.pod_id,
                            "future_demand_score": score,
                            "legal_targets": ([p.pod_id for p in sorted(
                                legal_targets, key=lambda p: p.pod_id)] if affordable else []),
                            "o1_target": (min(legal_targets, key=lambda p: (
                                p.load_ms(now), -p.cache.lookup(candidate.block_ids), p.pod_id
                            )).pod_id if legal_targets and affordable else None),
                            "budget_affordable": affordable,
                        })
                    if self.config.proactive_action == "O2A_PROBE" or closed_probe:
                        proactive_stats["o2a_candidate_states"].append({
                            "time_ms": now,
                            "state_fingerprint": fingerprint,
                            "total_eligible_prefix_count": len(eligible),
                            "candidates": [dict(item, o1_rank=rank)
                                           for rank, item in enumerate(eligible[:5], start=1)],
                        })
                        if closed_probe:
                            proactive_stats["o2a_closed_state_fingerprints"][str(now)] = fingerprint
                            proactive_stats["o2a_closed_validation_fingerprints"][str(now)] = validation_fingerprint(now)
                        continue
                    for item in eligible:
                        proactive_stats["o2p_candidate_states"].append({
                            "time_ms": now, "prefix_id": item["prefix_id"],
                            "prefix_depth_pages": item["prefix_depth_pages"],
                            "transferable_pages": item["transferable_pages"],
                            "prefix_valid_tokens": item["prefix_valid_tokens"],
                            "source_pod": item["source_pod"],
                            "o1_target": item["o1_target"],
                            "legal_targets": item["legal_targets"],
                            "budget_affordable": item["budget_affordable"],
                            "state_fingerprint": fingerprint,
                        })
                        break
                    continue
                if forced_copy:
                    fingerprint = state_fingerprint(now)
                    proactive_stats["o2p_state_fingerprint"] = fingerprint
                    proactive_stats["o2a_closed_state_fingerprints"][str(now)] = fingerprint
                    proactive_stats["o2a_closed_validation_fingerprints"][str(now)] = validation_fingerprint(now)
                    ranked = [item for item in ranked
                              if item[1].hash_id == forced_prefix]
                started = 0
                for candidate_rank, (score, candidate) in enumerate(ranked, start=1):
                    if started >= self.config.max_proactive_actions_per_tick:
                        proactive_stats["skip_action_cap"] += 1
                        continue
                    # Older reactive work has strict priority over background copies.
                    if node_gate.queued():
                        proactive_stats["skip_reactive_priority"] += 1
                        continue
                    sources = [pods[i] for i in candidate.source_pods
                               if not pods[i].transfer_busy]
                    if forced_copy:
                        sources = [p for p in sources
                                   if p.pod_id == forced_source]
                    if not sources:
                        proactive_stats["skip_endpoint_busy"] += 1
                        continue
                    transferable = candidate.depth
                    if candidate.block_valid_tokens[-1] < self.config.page_tokens:
                        transferable -= 1
                    if transferable <= 0:
                        continue
                    path = candidate.block_ids[:transferable]
                    selected_targets: set[int] = set()
                    for _ in range(self.config.proactive_fanout_targets):
                        if started >= self.config.max_proactive_actions_per_tick:
                            proactive_stats["skip_action_cap"] += 1
                            break
                        sources = [pods[i] for i in candidate.source_pods
                                   if not pods[i].transfer_busy]
                        if not sources:
                            proactive_stats["skip_endpoint_busy"] += 1
                            break
                        source = min(sources, key=lambda p: (p.load_ms(now), p.pod_id))
                        targets = [p for p in pods if p.pod_id != source.pod_id
                                   and p.pod_id not in selected_targets
                                   and p.cache.lookup(candidate.block_ids) < candidate.depth
                                   and not p.transfer_busy
                                   and (candidate.hash_id, p.pod_id) not in proactive_inflight]
                        if forced_copy:
                            targets = [p for p in targets
                                       if p.pod_id == forced_target
                                       and p.cache.lookup(path) < transferable]
                        if not targets:
                            proactive_stats["skip_endpoint_busy"] += 1
                            break
                        target = min(targets, key=lambda p: (
                            p.load_ms(now), -p.cache.lookup(candidate.block_ids), p.pod_id
                        ))
                        wire_bytes = transferable * self.config.page_bytes
                        selected_cost_score = None
                        if self.config.proactive_strategy == "COST_AWARE":
                            transfer_ms_for_score = self.config.control_latency_ms + (
                                wire_bytes / self.config.effective_bandwidth_bytes_per_s * 1000
                            )
                            final_score, benefit, load_penalty, transfer_penalty = cost_aware_score(
                                score, self.config.cost_v_ref, target.load_ms(now),
                                [p.load_ms(now) for p in pods], transfer_ms_for_score,
                                self.config.cost_t_ref_ms,
                            )
                            proactive_stats["cost_benefit_terms"].append(benefit)
                            proactive_stats["cost_load_penalties"].append(load_penalty)
                            proactive_stats["cost_transfer_penalties"].append(transfer_penalty)
                            proactive_stats["cost_final_scores"].append(final_score)
                            if final_score <= 0:
                                proactive_stats["cost_filter_rejected_count"] += 1
                                selected_targets.add(target.pod_id)
                                continue
                            selected_cost_score = final_score
                        if not token_bucket.can_afford(wire_bytes, now):
                            proactive_stats["skip_budget"] += 1
                            if wire_bytes > token_bucket.burst:
                                proactive_stats["action_exceeds_bucket_cap"] += 1
                            else:
                                proactive_stats["budget_insufficient_current_tokens"] += 1
                            break
                        if self.config.proactive_no_evict_admission and not target.cache.has_free_capacity(transferable):
                            proactive_stats["skip_no_free_capacity"] += 1
                            selected_targets.add(target.pod_id)
                            continue
                        if not target.cache.can_admit_temporary(transferable):
                            proactive_stats["skip_capacity"] += 1
                            selected_targets.add(target.pod_id)
                            continue
                        transfer_id = next_transfer_id
                        next_transfer_id += 1
                        evictions_before = target.cache.eviction_count
                        source.cache.pin(path, transferable)
                        try:
                            target.cache.reserve_transfer_temporary(transfer_id, transferable)
                        except CapacityAdmissionError:
                            source.cache.unpin(path, transferable)
                            proactive_stats["skip_capacity"] += 1
                            continue
                        if not token_bucket.consume(wire_bytes, now):
                            raise AssertionError("proactive budget changed during atomic admission")
                        proactive_stats["evicted_pages_caused_at_admission"] += (
                            target.cache.eviction_count - evictions_before
                        )
                        latency = self.config.control_latency_ms + (
                            wire_bytes / self.config.effective_bandwidth_bytes_per_s * 1000
                        )
                        action_id = len(proactive_actions)
                        action = ProactiveCopy(
                            action_id, transfer_id, now, candidate.hash_id,
                            source.pod_id, target.pod_id, candidate.depth, transferable,
                            score, path, self.config.oracle_horizon_ms, transferable,
                            wire_bytes, now + latency,
                        )
                        proactive_actions[action_id] = action
                        proactive_transfer_ids.add(transfer_id)
                        proactive_inflight.add((candidate.hash_id, target.pod_id))
                        selected_targets.add(target.pod_id)
                        source.active_transfer_id = target.active_transfer_id = transfer_id
                        source.endpoint_busy_time_ms += latency
                        target.endpoint_busy_time_ms += latency
                        proactive_stats["endpoint_busy_time_ms"] += 2 * latency
                        proactive_stats["actions_started"] += 1
                        if forced_copy:
                            proactive_stats["o2p_forced_action_success"] = True
                        if self.config.proactive_action == "MOVE_SAFE":
                            proactive_stats["move_actions_started"] += 1
                        if selected_cost_score is not None:
                            proactive_stats["selected_cost_scores"].append(selected_cost_score)
                        proactive_stats["selected_candidate_ranks"].append(candidate_rank)
                        if candidate_rank > 1:
                            proactive_stats["rank_gt_1_count"] += 1
                        proactive_stats["wire_pages"] += transferable
                        proactive_stats["wire_bytes"] += wire_bytes
                        schedule(action.complete_time, EventType.PROACTIVE_TRANSFER_COMPLETE,
                                 -action_id - 1)
                        started += 1
                if ranked and started == 0:
                    proactive_stats["top_k_exhausted_count"] += 1
                if (forced_copy
                        and not proactive_stats["o2p_forced_action_success"]):
                    proactive_stats["o2p_forced_action_failure_reason"] = "branch_action_not_admitted"
                continue

            if event.event_type is EventType.PROACTIVE_TRANSFER_COMPLETE:
                action = proactive_actions[-event.request_id - 1]
                source, target = pods[action.source_pod], pods[action.target_pod]
                if source.active_transfer_id != action.transfer_id or target.active_transfer_id != action.transfer_id:
                    raise AssertionError("proactive endpoint ownership mismatch")
                prior_hit = target.cache.lookup(action.block_ids)
                inserted = target.cache.commit_transfer_temporary(
                    action.transfer_id, action.block_ids, now
                )
                source.cache.unpin(action.block_ids, action.transferable_pages)
                source.active_transfer_id = target.active_transfer_id = None
                action.newly_resident_pages = inserted
                action.duplicate_wire_pages = action.wire_pages - inserted
                if self.config.proactive_action == "MOVE_SAFE":
                    protected_blocks: set[int] = set()
                    for other in proactive_actions.values():
                        if (other.transfer_id != action.transfer_id
                                and other.transfer_id in proactive_transfer_ids
                                and other.source_pod == source.pod_id):
                            protected_blocks.update(other.block_ids)
                    for other_runtime in runtimes.values():
                        transfer = other_runtime.transfer
                        if (other_runtime.state is RequestState.WAITING_TRANSFER
                                and transfer is not None
                                and transfer.source_pod_id == source.pod_id):
                            protected_blocks.update(
                                other_runtime.request.block_ids[:transfer.transferable_prefix_pages]
                            )
                    release = source.cache.release_leaf_exclusive_suffix(
                        action.block_ids, protected_blocks
                    )
                    freed = release["pages_freed"]
                    action.move_source_pages_freed = freed
                    action.move_release_blocked_shared = release["blocked_shared"]
                    action.move_release_blocked_pinned = release["blocked_pinned"]
                    action.move_release_blocked_active_or_transfer = release[
                        "blocked_active_or_transfer"
                    ]
                    action.move_release_class = (
                        "ZERO" if freed == 0 else
                        "FULL" if freed == action.transferable_pages else "PARTIAL"
                    )
                    proactive_stats["move_actions_completed"] += 1
                    proactive_stats["move_source_release_attempts"] += 1
                    proactive_stats["move_source_pages_freed"] += freed
                    proactive_stats["move_source_tokens_freed"] += (
                        freed * self.config.page_tokens
                    )
                    proactive_stats[f"move_{action.move_release_class.lower()}_release_count"] += 1
                    proactive_stats["move_release_blocked_shared"] += release["blocked_shared"]
                    proactive_stats["move_release_blocked_pinned"] += release["blocked_pinned"]
                    proactive_stats["move_release_blocked_active_or_transfer"] += release[
                        "blocked_active_or_transfer"
                    ]
                for block_id in action.block_ids[prior_hit:]:
                    key = (target.pod_id, block_id)
                    sequence = generation_sequence.get(key, 0) + 1
                    generation_sequence[key] = sequence
                    generation = ReplicaGeneration(action.transfer_id, target.pod_id,
                                                   block_id, sequence, now)
                    current_generations[key] = generation
                    action.generations.append(generation)
                proactive_stats["actions_completed"] += 1
                proactive_stats["newly_resident_pages"] += inserted
                proactive_stats["duplicate_wire_pages"] += action.duplicate_wire_pages
                proactive_transfer_ids.remove(action.transfer_id)
                proactive_inflight.remove((action.candidate_hash, target.pod_id))
                if node_gate.queued():
                    schedule(now, EventType.TRANSFER_ADMIT,
                             node_gate.queued()[0].request_id)
                continue

            runtime = runtimes[event.request_id]
            request = runtime.request

            if event.event_type is EventType.REQUEST_ARRIVAL:
                if runtime.state is not None:
                    raise AssertionError("request arrived more than once")
                runtime.state = RequestState.ARRIVED
                candidate_generator.observe(request)
                if self.config.routing_policy == "R_REQ_KV_TASK":
                    task_activity["total_requests"] += 1
                if self.config.routing_policy in {"R_REQ_KV", "R_REQ_KV_TASK"} and self.config.transfer_enabled:
                    b0_arrival(runtime, now)
                    continue
                if self.config.routing_policy != "R_REQ_KV_SIMPLE" or not self.config.transfer_enabled:
                    policy = "R_AFF" if self.config.routing_policy in {"R_REQ_KV_SIMPLE", "R_REQ_KV", "R_REQ_KV_TASK"} else self.config.routing_policy
                    enqueue_ready(runtime, route_request(policy, request, pods, now), now)
                    continue

                source_decision = route_request("R_AFF", request, pods, now)
                source = pods[source_decision.pod_id]
                target = min(pods, key=lambda p: (p.load_ms(now), p.pod_id))
                matched = source_decision.h_route_pages
                transferable = transferable_prefix_pages(
                    request, matched, self.config.partial_page_mode,
                    self.config.page_tokens
                )
                target_hit = target.cache.lookup(request.block_ids[:transferable])
                source_load, target_load = source.load_ms(now), target.load_ms(now)
                if matched == 0 or source.pod_id == target.pod_id:
                    enqueue_ready(runtime, source_decision, now)
                elif target_hit >= transferable:
                    full_hit = target.cache.lookup(request.block_ids)
                    enqueue_ready(runtime, RouteDecision(target.pod_id, full_hit,
                                  request.hit_tokens(full_hit)), now)
                elif source_load <= target_load * self.config.theta_simple:
                    enqueue_ready(runtime, source_decision, now)
                else:
                    transfer_stats["transfer_attempts"] += 1
                    transfer = Transfer(
                        next_transfer_id, request.request_id, source.pod_id, target.pod_id,
                        matched, transferable, transferable,
                        transferable * self.config.page_bytes,
                    )
                    next_transfer_id += 1
                    runtime.transfer = transfer
                    schedule(now, EventType.TRANSFER_ADMIT, request.request_id)

            elif event.event_type is EventType.TRANSFER_ENQUEUE:
                schedule(now, EventType.TRANSFER_ADMIT, request.request_id)

            elif event.event_type is EventType.TRANSFER_TIMEOUT:
                ticket = runtime.ticket
                if ticket is None or ticket.state is not TicketState.QUEUED:
                    continue
                ticket.state = TicketState.TIMED_OUT
                transfer_stats["transfer_admission_timeouts"] += 1
                transfer_stats["timeout_wait_ms"] += now - ticket.enqueue_time_ms
                fallback_aff(runtime, now)
                if node_gate.queued():
                    schedule(now, EventType.TRANSFER_ADMIT,
                             node_gate.queued()[0].request_id)

            elif event.event_type is EventType.TRANSFER_ADMIT:
                if self.config.routing_policy in {"R_REQ_KV", "R_REQ_KV_TASK"} and self.config.transfer_enabled:
                    for ticket in list(node_gate.queued()):
                        rt = runtimes[ticket.request_id]
                        req = rt.request
                        source_ep, target_ep = pods[ticket.source_pod], pods[ticket.target_pod]
                        if source_ep.transfer_busy or target_ep.transfer_busy:
                            blocking_ids = {source_ep.active_transfer_id,
                                            target_ep.active_transfer_id} & proactive_transfer_ids
                            if blocking_ids and not getattr(ticket, "proactive_block_recorded", False):
                                ticket.proactive_block_recorded = True
                                proactive_stats["reactive_ticket_blocked_by_proactive_count"] += 1
                                completion = max(
                                    a.complete_time for a in proactive_actions.values()
                                    if a.transfer_id in blocking_ids
                                )
                                proactive_stats["reactive_endpoint_blocked_by_proactive_ms"] += max(0.0, completion - now)
                            if not ticket.endpoint_blocked:
                                transfer_stats["endpoint_wait"] += 1
                                ticket.endpoint_blocked = True
                            continue
                        old_source, old_target = ticket.source_pod, ticket.target_pod
                        pods[ticket.remote_pressure_owner_pod].remove_remote_pressure(ticket.ticket_id)
                        ticket.remote_pressure_owner_pod = -1
                        source_decision = route_request("R_AFF", req, pods, now)
                        target = (min(pods, key=lambda p: (p.load_ms(now), p.pod_id))
                                  if self.config.routing_policy == "R_REQ_KV_TASK"
                                  else best_target(req, source_decision.pod_id, now))
                        transferable = transferable_prefix_pages(
                            req, source_decision.h_route_pages,
                            self.config.partial_page_mode, self.config.page_tokens
                        )
                        def cancel(reason: str, direct_target: Pod | None = None) -> None:
                            transfer_stats[f"admission_{reason}"] += 1
                            transfer_stats["replan_cancel"] += 1
                            ticket.state = TicketState.CANCELLED
                            node_gate.remove(ticket)
                            if direct_target is not None:
                                hit = direct_target.cache.lookup(req.block_ids)
                                transfer_stats["replan_direct_route"] += 1
                                enqueue_ready(rt, RouteDecision(direct_target.pod_id, hit,
                                              req.hit_tokens(hit)), now)
                            else:
                                fallback_aff(rt, now)
                        if source_decision.h_route_pages == 0 or transferable == 0:
                            cancel("no_cache_source")
                            continue
                        if target is None:
                            cancel("no_legal_target")
                            continue
                        if (self.config.routing_policy == "R_REQ_KV_TASK"
                                and target.pod_id == source_decision.pod_id):
                            cancel("relative_gate_reject")
                            continue
                        target_hit = target.cache.lookup(req.block_ids)
                        if target.cache.lookup(req.block_ids[:transferable]) >= transferable:
                            cancel("target_already_has_prefix", target)
                            continue
                        source = pods[source_decision.pod_id]
                        hit_fraction = req.hit_tokens(transferable) / req.input_tokens
                        if (self.config.routing_policy != "R_REQ_KV_TASK"
                                and hit_fraction < self.config.cache_hit_threshold):
                            cancel("cache_gate_reject")
                            continue
                        theta = (self.config.theta_simple if self.config.routing_policy == "R_REQ_KV_TASK"
                                 else self.config.relative_load_threshold)
                        if (self.config.routing_policy == "R_REQ_KV_TASK"
                                and not request_kv_task_gate(
                                    source.load_ms(now), target.load_ms(now), transferable,
                                    source.pod_id, target.pod_id, theta
                                )) or (self.config.routing_policy != "R_REQ_KV_TASK"
                                       and source.load_ms(now) <= target.load_ms(now) * theta):
                            cancel("relative_gate_reject")
                            continue
                        if (self.config.routing_policy != "R_REQ_KV_TASK"
                                and source.load_ms(now) - target.load_ms(now) < self.config.absolute_load_gap_ms):
                            cancel("absolute_gate_reject")
                            continue
                        if old_source != source.pod_id or old_target != target.pod_id:
                            transfer_stats["replan_count"] += 1
                            if old_source != source.pod_id:
                                transfer_stats["source_replan_count"] += 1
                            if old_target != target.pod_id:
                                transfer_stats["target_replan_count"] += 1
                        planned = max(transferable, target_hit)
                        target_estimate = service.estimate(req, planned).service_ms
                        ticket.source_pod, ticket.target_pod = source.pod_id, target.pod_id
                        ticket.source_hit_pages = source_decision.h_route_pages
                        ticket.source_hit_tokens = source_decision.h_route_tokens
                        ticket.transferable_pages = ticket.wire_pages = transferable
                        ticket.planned_post_transfer_hit_pages = planned
                        ticket.planned_post_transfer_hit_tokens = req.hit_tokens(planned)
                        ticket.target_estimated_service_ms = target_estimate
                        ticket.remote_pressure_owner_pod = target.pod_id
                        target.add_remote_pressure(ticket.ticket_id, target_estimate)
                        if source.transfer_busy or target.transfer_busy:
                            continue
                        source.cache.pin(req.block_ids, transferable)
                        try:
                            target.cache.reserve_transfer_temporary(ticket.ticket_id, transferable)
                        except CapacityAdmissionError:
                            source.cache.unpin(req.block_ids, transferable)
                            if transferable > (target.cache.capacity_pages or float("inf")):
                                cancel("no_legal_target")
                            else:
                                if not ticket.capacity_blocked:
                                    transfer_stats["capacity_wait"] += 1
                                    transfer_stats["capacity_blocked_at_admission"] += 1
                                    ticket.capacity_blocked = True
                            continue
                        ticket.state = TicketState.TRANSFER_RUNNING
                        node_gate.remove(ticket)
                        transfer = Transfer(
                            ticket.ticket_id, req.request_id, source.pod_id, target.pod_id,
                            source_decision.h_route_pages, transferable, transferable,
                            transferable * self.config.page_bytes,
                            admit_ms=now,
                        )
                        transfer.complete_ms = now + self.config.control_latency_ms + (
                            transfer.wire_bytes / self.config.effective_bandwidth_bytes_per_s * 1000
                        )
                        rt.transfer = transfer
                        rt.state = RequestState.WAITING_TRANSFER
                        source.active_transfer_id = target.active_transfer_id = transfer.transfer_id
                        source.endpoint_busy_time_ms += transfer.latency_ms
                        target.endpoint_busy_time_ms += transfer.latency_ms
                        transfer_stats["transfer_admitted"] += 1
                        transfer_stats["ticket_admission_waits_ms"].append(now - ticket.enqueue_time_ms)
                        schedule(transfer.complete_ms, EventType.TRANSFER_COMPLETE, req.request_id)
                    continue

                transfer = runtime.transfer
                if transfer is None or runtime.state is not RequestState.ARRIVED:
                    raise AssertionError("invalid TRANSFER_ADMIT state")
                source, target = pods[transfer.source_pod_id], pods[transfer.target_pod_id]
                if source.transfer_busy or target.transfer_busy:
                    transfer_stats["endpoint_busy_fallback"] += 1
                    fallback_aff(runtime, now)
                    continue
                source.cache.pin(request.block_ids, transfer.transferable_prefix_pages)
                try:
                    target.cache.reserve_transfer_temporary(
                        transfer.transfer_id, transfer.wire_pages
                    )
                except CapacityAdmissionError:
                    source.cache.unpin(request.block_ids, transfer.transferable_prefix_pages)
                    transfer_stats["capacity_fallback"] += 1
                    fallback_aff(runtime, now)
                    continue
                transfer.admit_ms = now
                transfer_ms = self.config.control_latency_ms + (
                    transfer.wire_bytes / self.config.effective_bandwidth_bytes_per_s * 1000.0
                )
                transfer.complete_ms = now + transfer_ms
                source.active_transfer_id = target.active_transfer_id = transfer.transfer_id
                source.endpoint_busy_time_ms += transfer_ms
                target.endpoint_busy_time_ms += transfer_ms
                runtime.state = RequestState.WAITING_TRANSFER
                transfer_stats["transfer_admitted"] += 1
                schedule(transfer.complete_ms, EventType.TRANSFER_COMPLETE, request.request_id)

            elif event.event_type is EventType.TRANSFER_COMPLETE:
                transfer = runtime.transfer
                if transfer is None or runtime.state is not RequestState.WAITING_TRANSFER:
                    raise AssertionError("invalid TRANSFER_COMPLETE state")
                source, target = pods[transfer.source_pod_id], pods[transfer.target_pod_id]
                if source.active_transfer_id != transfer.transfer_id or target.active_transfer_id != transfer.transfer_id:
                    raise AssertionError("P2P endpoint ownership mismatch")
                path = request.block_ids[:transfer.transferable_prefix_pages]
                newly_resident = target.cache.commit_transfer_temporary(
                    transfer.transfer_id, path, now
                )
                if newly_resident:
                    for block_id in path[-newly_resident:]:
                        current_generations.pop((target.pod_id, block_id), None)
                source.cache.unpin(request.block_ids, transfer.transferable_prefix_pages)
                source.active_transfer_id = target.active_transfer_id = None
                if runtime.ticket is not None:
                    ticket = runtime.ticket
                    if ticket.remote_pressure_owner_pod >= 0:
                        pods[ticket.remote_pressure_owner_pod].remove_remote_pressure(ticket.ticket_id)
                        ticket.remote_pressure_owner_pod = -1
                    ticket.state = TicketState.COMPLETED
                transfer.newly_resident_pages = newly_resident
                transfer_stats["transfer_completed"] += 1
                if runtime.ticket is not None:
                    transfer_stats["successful_transfer"] += 1
                transfer_stats["matched_prefix_pages"] += transfer.matched_prefix_pages
                transfer_stats["transferable_prefix_pages"] += transfer.transferable_prefix_pages
                transfer_stats["wire_pages"] += transfer.wire_pages
                transfer_stats["wire_bytes"] += transfer.wire_bytes
                transfer_stats["newly_resident_pages"] += newly_resident
                transfer_stats["duplicate_wire_pages"] += transfer.wire_pages - newly_resident
                transfer_stats["transfer_latencies_ms"].append(transfer.latency_ms)
                transfer_stats["source_pod_distribution"][source.pod_id] += 1
                transfer_stats["target_pod_distribution"][target.pod_id] += 1
                runtime.p2p_assisted = True
                full_hit = target.cache.lookup(request.block_ids)
                enqueue_ready(runtime, RouteDecision(target.pod_id, full_hit,
                              request.hit_tokens(full_hit)), now)
                if node_gate.queued():
                    schedule(now, EventType.TRANSFER_ADMIT,
                             node_gate.queued()[0].request_id)

            elif event.event_type is EventType.SERVICE_START:
                if runtime.pod_id is None:
                    raise AssertionError("SERVICE_START before routing")
                pod = pods[runtime.pod_id]
                start_scheduled[pod.pod_id] = False
                work = pod.take_next()
                if work is None:
                    continue
                if work.request_id != event.request_id or load_owner.get(request.request_id) != pod.pod_id:
                    raise AssertionError("FCFS or queued Load ownership violated")
                if runtime.queued_estimated_service_ms != work.estimated_service_ms:
                    raise AssertionError("queued estimate mismatch")
                runtime.queued_estimated_service_ms = None
                runtime.service_start_ms = now
                h_used = pod.cache.lookup(request.block_ids)
                for block_id in request.block_ids[:h_used]:
                    generation = current_generations.get((pod.pod_id, block_id))
                    if generation is not None and generation.first_use_time is None:
                        generation.first_use_time = now
                runtime.estimate = service.estimate(request, h_used)
                pod.cache.pin_and_refresh(request.block_ids, h_used, now)
                try:
                    pod.cache.reserve_active_private(request.request_id,
                                                     len(request.block_ids) - h_used)
                except CapacityAdmissionError:
                    pod.cache.unpin(request.block_ids, h_used)
                    raise
                pod.start_running(request.request_id, now, runtime.estimate.service_ms)
                runtime.state = RequestState.RUNNING
                pod.observe_load(now)
                schedule(now + runtime.estimate.service_ms, EventType.SERVICE_DONE,
                         request.request_id)

            elif event.event_type is EventType.SERVICE_DONE:
                if runtime.pod_id is None or runtime.state is not RequestState.RUNNING:
                    raise AssertionError("invalid SERVICE_DONE state")
                pod = pods[runtime.pod_id]
                if request.request_id in completed_ids or runtime.estimate is None or runtime.service_start_ms is None:
                    raise AssertionError("invalid duplicate SERVICE_DONE")
                pod.finish(request.request_id, now)
                inserted = pod.cache.commit_active(request.request_id, request.block_ids, now)
                if inserted:
                    for block_id in request.block_ids[-inserted:]:
                        current_generations.pop((pod.pod_id, block_id), None)
                pod.cache.unpin(request.block_ids, runtime.estimate.hit_pages)
                if load_owner.pop(request.request_id, None) != pod.pod_id:
                    raise AssertionError("running Load owner mismatch")
                estimate = runtime.estimate
                route_estimate = service.estimate(request, runtime.h_route_pages or 0).service_ms
                transfer_wait = 0.0
                source_id = target_id = None
                if runtime.transfer is not None:
                    source_id, target_id = runtime.transfer.source_pod_id, runtime.transfer.target_pod_id
                    if runtime.p2p_assisted:
                        transfer_wait = runtime.transfer.latency_ms
                        transfer_stats["p2p_assisted_requests"] += 1
                        transfer_stats["p2p_assisted_h_used_tokens"] += estimate.hit_tokens
                result = RequestResult(
                    request.request_id, pod.pod_id, request.arrival_ms,
                    runtime.service_start_ms, now, request.input_tokens,
                    request.output_tokens, runtime.h_route_pages or 0,
                    runtime.h_route_tokens or 0, estimate.hit_pages,
                    estimate.hit_tokens, estimate.miss_tokens,
                    runtime.service_start_ms - request.arrival_ms - transfer_wait,
                    estimate.service_ms, now - request.arrival_ms,
                    estimate.hit_pages, inserted, route_estimate,
                    route_estimate - estimate.service_ms,
                    route_estimate / estimate.service_ms,
                    runtime.p2p_assisted, source_id, target_id, transfer_wait,
                    runtime.ticket.target_estimated_service_ms if runtime.ticket else None,
                    request.split,
                    runtime.ticket is not None,
                    runtime.ticket.state.name if runtime.ticket else None,
                    runtime.transfer.wire_pages if runtime.p2p_assisted and runtime.transfer else 0,
                    runtime.transfer.wire_bytes if runtime.p2p_assisted and runtime.transfer else 0,
                    (runtime.transfer.admit_ms - runtime.ticket.enqueue_time_ms
                     if runtime.p2p_assisted and runtime.transfer
                     and runtime.transfer.admit_ms is not None and runtime.ticket else None),
                )
                results.append(result)
                completed_ids.add(request.request_id)
                runtime.state = RequestState.DONE
                last_completion_ms = max(last_completion_ms, now)
                pod.observe_load(now)
                if pod.queue:
                    schedule(now, EventType.SERVICE_START, pod.queue[0].request_id, pod.pod_id)
                if node_gate.queued():
                    schedule(now, EventType.TRANSFER_ADMIT,
                             node_gate.queued()[0].request_id)

        if len(completed_ids) != len(requests) or load_owner:
            raise AssertionError("incomplete requests or ghost Load ownership")
        if any(p.queue or not p.idle or p.transfer_busy or p.remote_pressure for p in pods):
            raise AssertionError("Pod retains queue/running/endpoint state")
        if node_gate.queued():
            raise AssertionError("NodeGate did not drain")
        reconcile_evictions()
        ordered = sorted(results, key=lambda x: x.request_id)
        duration_ms = last_completion_ms - first_arrival_ms
        summary = summarize(
            ordered, pods, duration_ms, self.config.routing_policy,
            transfer_stats, pressure_intervals, self.config
        )
        if self.config.routing_policy == "R_REQ_KV_TASK":
            task_activity.update({
                "tickets_created": transfer_stats["ticket_created"],
                "transfers_started": transfer_stats["transfer_admitted"],
                "transfers_completed": transfer_stats["transfer_completed"],
                "ticket_fallbacks": transfer_stats["ticket_created"] - transfer_stats["transfer_completed"],
                "ticket_timeouts": transfer_stats["transfer_admission_timeouts"],
                "replans": transfer_stats["replan_count"],
            })
            partition = (task_activity["task_gate_true"]
                         + task_activity["stayed_local_gate_false"]
                         + task_activity["stayed_local_no_transferable_prefix"]
                         + task_activity["stayed_local_source_equals_target"])
            if partition != task_activity["total_requests"]:
                raise AssertionError("R_REQ_KV_TASK activity partition violated")
            if not (task_activity["transfers_completed"] <= task_activity["transfers_started"]
                    <= task_activity["tickets_created"]):
                raise AssertionError("R_REQ_KV_TASK ticket/transfer counters violated")
            if (task_activity["transfers_completed"] + task_activity["ticket_fallbacks"]
                    != task_activity["tickets_created"]):
                raise AssertionError("R_REQ_KV_TASK terminal ticket conservation violated")
            summary["r_req_kv_task_activity"] = task_activity
        if self.config.proactive_enabled:
            categories = {name: 0 for name in (
                "USEFUL", "NO_FUTURE_DEMAND", "NOT_USED_AT_TARGET",
                "EVICTED_BEFORE_USE", "TOO_LATE", "DUPLICATE_WIRE", "CENSORED"
            )}
            action_records = []
            page_seconds = 0.0
            actually_used = 0
            ready_before = 0
            for action in proactive_actions.values():
                first_relevant = future_index.first_future_arrival(
                    action.candidate_hash, action.trigger_time, action.horizon_ms
                )
                first_use_values = [g.first_use_time for g in action.generations
                                    if g.first_use_time is not None]
                first_use = min(first_use_values, default=None)
                first_eviction_values = [g.eviction_time for g in action.generations
                                         if g.eviction_time is not None]
                first_eviction = min(first_eviction_values, default=None)
                censored = action.complete_time + action.horizon_ms > self.config.oracle_visibility_end_ms
                if censored:
                    category = "CENSORED"
                elif action.newly_resident_pages == 0:
                    category = "DUPLICATE_WIRE"
                elif action.oracle_score == 0:
                    category = "NO_FUTURE_DEMAND"
                elif first_relevant is not None and first_relevant <= action.complete_time:
                    category = "TOO_LATE"
                elif first_use is not None:
                    category = "USEFUL"
                elif first_eviction is not None:
                    category = "EVICTED_BEFORE_USE"
                else:
                    category = "NOT_USED_AT_TARGET"
                categories[category] += 1
                actually_used += first_use is not None
                ready_before += first_relevant is not None and action.complete_time < first_relevant
                for generation in action.generations:
                    end = generation.eviction_time or min(
                        self.config.oracle_visibility_end_ms,
                        generation.ready_time + action.horizon_ms,
                    )
                    page_seconds += max(0.0, end - generation.ready_time) / 1000.0
                action_records.append({
                    "action_id": action.action_id, "trigger_time": action.trigger_time,
                    "candidate_hash": action.candidate_hash,
                    "source_pod": action.source_pod, "target_pod": action.target_pod,
                    "prefix_depth": action.prefix_depth,
                    "transferable_pages": action.transferable_pages,
                    "future_demand_within_w": action.oracle_score,
                    "ready_time": action.complete_time,
                    "ready_before_first_relevant_arrival": (
                        first_relevant is not None and action.complete_time < first_relevant
                    ),
                    "actually_used": first_use is not None,
                    "first_use_time": first_use,
                    "evicted_before_first_use": first_eviction is not None and first_use is None,
                    "duplicate_wire_pages": action.duplicate_wire_pages,
                    "newly_resident_pages": action.newly_resident_pages,
                    "replica_lifetime_ms": None if not action.generations else max(
                        0.0, (first_eviction or self.config.oracle_visibility_end_ms)
                        - action.complete_time),
                    "time_to_first_use_ms": None if first_use is None else first_use - action.complete_time,
                    "classification": category,
                    "move_source_pages_freed": action.move_source_pages_freed,
                    "move_release_class": action.move_release_class,
                    "move_release_blocked_shared": action.move_release_blocked_shared,
                    "move_release_blocked_pinned": action.move_release_blocked_pinned,
                    "move_release_blocked_active_or_transfer": (
                        action.move_release_blocked_active_or_transfer
                    ),
                })
            proactive_stats["future_demand_distribution"] = distribution(
                proactive_stats.pop("future_demand_scores")
            )
            candidate_groups: dict[tuple[float, int], list[dict]] = {}
            for record in action_records:
                candidate_groups.setdefault(
                    (record["trigger_time"], record["candidate_hash"]), []
                ).append(record)
            created_records = [r for r in action_records if r["newly_resident_pages"] > 0]
            used_records = [r for r in created_records if r["actually_used"]]
            useful_wire = sum(r["transferable_pages"] * self.config.page_bytes
                              for r in used_records)
            proactive_stats.update({
                "replica_actually_used_actions": actually_used,
                "actual_use_rate": len(used_records) / len(created_records) if created_records else 0.0,
                "replicas_created": len(created_records),
                "replicas_used": len(used_records),
                "replica_actual_use_rate": len(used_records) / len(created_records) if created_records else 0.0,
                "candidate_count_with_actions": len(candidate_groups),
                "candidate_any_replica_used": sum(
                    any(record["actually_used"] for record in records)
                    for records in candidate_groups.values()
                ),
                "candidate_any_use_rate": (
                    sum(any(record["actually_used"] for record in records)
                        for records in candidate_groups.values()) / len(candidate_groups)
                    if candidate_groups else 0.0
                ),
                "replicas_per_candidate": distribution(
                    [len(records) for records in candidate_groups.values()]
                ),
                "useful_replica_wire_bytes": useful_wire,
                "useful_wire_fraction": useful_wire / proactive_stats["wire_bytes"]
                    if proactive_stats["wire_bytes"] else 0.0,
                "additional_redundancy_pages": proactive_stats["newly_resident_pages"],
                "ready_before_first_relevant_arrival_actions": ready_before,
                "wasted_copy_breakdown": categories,
                "proactive_replica_page_seconds": page_seconds,
                "action_records": action_records,
                "token_bucket_final_tokens": token_bucket.tokens,
                "later_normal_eviction_count": sum(
                    sum(self.config.split_config.development_start_ms <= time_ms
                        < self.config.split_config.warmup_start_ms
                        for time_ms in p.cache.eviction_timestamps_ms)
                    for p in pods
                ) - proactive_stats["evicted_pages_caused_at_admission"],
                "binding_constraint": (
                    "BOTH" if proactive_stats["skip_action_cap"] and proactive_stats["skip_budget"]
                    else "ACTION_CAP" if proactive_stats["skip_action_cap"]
                    else "BYTE_BUCKET" if proactive_stats["skip_budget"] else "NONE"
                ),
            })
            proactive_stats["rank_gt_1_fraction"] = (
                proactive_stats["rank_gt_1_count"] / len(proactive_stats["selected_candidate_ranks"])
                if proactive_stats["selected_candidate_ranks"] else 0.0
            )
            proactive_stats["selected_candidate_rank_distribution"] = audit_distribution(
                proactive_stats["selected_candidate_ranks"]
            )
            if self.config.proactive_strategy == "COST_AWARE":
                for field in ("cost_benefit_terms", "cost_load_penalties",
                              "cost_transfer_penalties", "cost_final_scores",
                              "selected_cost_scores"):
                    proactive_stats[field + "_distribution"] = audit_distribution(
                        proactive_stats[field]
                    )
                total_scores = len(proactive_stats["cost_final_scores"])
                positive_scores = sum(value > 0 for value in proactive_stats["cost_final_scores"])
                proactive_stats["score_gt_zero_count"] = positive_scores
                proactive_stats["score_gt_zero_fraction"] = (
                    positive_scores / total_scores if total_scores else 0.0
                )
                proactive_stats["score_le_zero_count"] = total_scores - positive_scores
            summary["proactive"] = proactive_stats
            summary["network_cost"] = {
                "reactive_wire_bytes": transfer_stats["wire_bytes"],
                "proactive_wire_bytes": proactive_stats["wire_bytes"],
                "total_wire_bytes": transfer_stats["wire_bytes"] + proactive_stats["wire_bytes"],
                "reactive_transfers": transfer_stats["transfer_completed"],
                "proactive_transfers": proactive_stats["actions_completed"],
                "reactive_endpoint_busy_time_ms": sum(p.endpoint_busy_time_ms for p in pods)
                    - proactive_stats["endpoint_busy_time_ms"],
                "proactive_endpoint_busy_time_ms": proactive_stats["endpoint_busy_time_ms"],
                "total_endpoint_busy_time_ms": sum(p.endpoint_busy_time_ms for p in pods),
            }
        return ordered, summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Kernel v3b reactive P2P simulator")
    parser.add_argument("--trace", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    config = SimulatorConfig.from_yaml(args.config)
    requests = load_trace(args.trace, config.page_tokens, config.split_config)
    try:
        results, summary = SimulatorEngine(config).run(requests)
    except CapacityAdmissionError as exc:
        args.output.mkdir(parents=True, exist_ok=True)
        path = args.output / "failure.json"
        path.write_text(json.dumps(exc.diagnostic, indent=2) + "\n", encoding="utf-8")
        parser.exit(2, f"{exc}\ndiagnostic: {path}\n")
    digest = hashlib.sha256()
    with args.trace.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[2],
        check=False, capture_output=True, text=True,
    ).stdout.strip() or None
    from .config import B0_IMPL_VERSION, METRIC_VERSION
    summary["provenance"] = {
        "git_commit": commit,
        "b0_impl_version": B0_IMPL_VERSION,
        "metric_version": METRIC_VERSION,
        "trace_path": str(args.trace.resolve()),
        "trace_sha256": digest.hexdigest(),
        "split_boundaries_ms": config.split_config.as_dict() if config.split_config else None,
        "num_pods": config.num_pods,
        "capacity_pages": config.cache_capacity_pages,
        "service": {"base_latency_ms": config.base_latency_ms,
                    "prefill_tokens_per_second": config.prefill_tokens_per_second,
                    "parameter_status": "development / nominal simulator assumptions"},
        "routing": {"policy": config.routing_policy,
                    "cache_hit_threshold": config.cache_hit_threshold,
                    "relative_load_threshold": config.relative_load_threshold,
                    "absolute_load_gap_ms": config.absolute_load_gap_ms},
        "p2p": {"enabled": config.transfer_enabled,
                "admission_timeout_ms": config.admission_timeout_ms,
                "partial_page_mode": config.partial_page_mode,
                "transfer_mode": config.transfer_mode,
                "page_bytes": config.page_bytes,
                "effective_bandwidth_bytes_per_s": config.effective_bandwidth_bytes_per_s,
                "control_latency_ms": config.control_latency_ms,
                "parameter_status": "development / nominal simulator assumptions"},
        "random_seed": config.random_seed,
        "arrival_mode": config.arrival_mode,
        "summary_split": config.summary_split,
        "protocol_version": config.protocol_version,
        "oracle": {"policy": "T_ORACLE_FUTURE_DEMAND_REFERENCE",
                   "horizon_ms": config.oracle_horizon_ms,
                   "visibility_end_ms": config.oracle_visibility_end_ms},
        "proactive": {"enabled": config.proactive_enabled,
                      "action": config.proactive_action,
                      "strategy": config.proactive_strategy,
                      "trigger_period_ms": config.trigger_period_ms,
                      "trigger_phase_ms": config.trigger_phase_ms,
                      "max_proactive_actions_per_tick": config.max_proactive_actions_per_tick,
                      "byte_rate": config.proactive_byte_rate,
                      "burst_bytes": config.proactive_burst_bytes,
                      "no_evict_admission": config.proactive_no_evict_admission,
                      "fanout_targets": config.proactive_fanout_targets,
                      "trigger_latest_ms": config.proactive_trigger_latest_ms,
                      "initial_tokens": config.proactive_initial_tokens,
                      "persistence_history_ms": config.persistence_history_ms,
                      "persistence_top_k": config.persistence_top_k,
                      "recency_selection_quantile": config.recency_selection_quantile,
                      "recency_decay_seconds": config.recency_decay_seconds,
                      "cost_v_ref": config.cost_v_ref,
                      "cost_t_ref_ms": config.cost_t_ref_ms,
                      "parameter_status": "development simulator intervention budget"},
    }
    write_outputs(args.output, results, summary)
    print(f"completed {summary['request_count']} requests")
    print(f"wrote results to {args.output}")


if __name__ == "__main__":
    main()
