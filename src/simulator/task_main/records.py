from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class RequestRecord:
    request_id: int
    workload: str
    arrival_time: float
    completion_time: float
    routing_policy: str
    affinity_source: int
    final_pod: int
    input_tokens: int
    output_tokens: int
    route_hit_pages: int
    route_hit_tokens: int
    final_hit_pages: int
    final_hit_tokens: int
    miss_tokens: int
    load_vector_before_route: tuple[int, ...]
    load_account_time: float
    reactive_transfer_id: int | None
    reactive_fallback_reason: str | None
    cache_admission_skip: bool
    cache_admission_status: str
    cache_inserted_pages: int
    split: str
    transferable_pages: int = 0
    route_has_partial_tail: bool = False
    reactive_gate_passed: bool = False
    target_feasibility_evaluated: bool = False
    structural_target_count: int | None = None
    feasible_target_count: int | None = None
    selected_target_load_rank: int | None = None
    absolute_lowest_load_target_feasible: bool | None = None


@dataclass
class TransferRecord:
    transfer_id: int
    request_id: int
    type: str
    source: int
    target: int
    transferable_chain: tuple[int, ...]
    wire_pages: int
    wire_tokens: int
    wire_bytes: int
    start_time: float
    ready_time: float
    temporary_pages: int
    newly_resident_pages: int = 0
    duplicate_pages: int = 0
    status: str = "IN_FLIGHT"


@dataclass(frozen=True)
class OpportunityRecord:
    opportunity_id: int
    request_id: int
    opportunity_time: float
    workload: str
    load_vector: tuple[int, ...]
    cache_summaries: tuple[dict, ...]
    split: str
    policy: str = "NONE"
    structural_candidate_count: int = 0
    positive_candidate_count: int = 0
    shortlist_count: int = 0
    attempted_candidate_count: int = 0
    selected_chain_id: tuple[int, ...] | None = None
    selected_source: int | None = None
    selected_target: int | None = None
    proactive_transfer_id: int | None = None
    final_status: str = "NONE"
    structural_rejections: dict = field(default_factory=dict)


@dataclass
class CandidateDecisionRecord:
    opportunity_id: int
    rank: int
    chain_id: tuple[int, ...]
    source: int
    target: int
    policy: str
    policy_score: float
    oracle_future_count: int | None = None
    persistence_count: int | None = None
    recency_score: float | None = None
    benefit: float | None = None
    congestion: float | None = None
    transfer_cost_seconds: float | None = None
    cost_score: float | None = None
    target_feasibility_evaluated: bool = False
    structural_target_count: int | None = None
    feasible_target_count: int | None = None
    selected_target_load_rank: int | None = None
    absolute_lowest_load_target_feasible: bool | None = None
    status: str = "NOT_ATTEMPTED"


@dataclass
class ProactiveActionRecord:
    action_id: int
    opportunity_id: int
    request_id: int
    policy: str
    decision_time: float
    chain_id: tuple[int, ...]
    chain_depth_pages: int
    source: int
    target: int
    policy_score: float
    wire_pages: int
    wire_tokens: int
    wire_bytes: int
    transfer_id: int
    start_time: float
    ready_time: float
    split: str
    oracle_future_count: int | None = None
    persistence_count: int | None = None
    recency_score: float | None = None
    benefit: float | None = None
    congestion: float | None = None
    transfer_cost_seconds: float | None = None
    cost_score: float | None = None
    structural_target_count: int | None = None
    feasible_target_count: int | None = None
    selected_target_load_rank: int | None = None
    absolute_lowest_load_target_feasible: bool | None = None
    status: str = "IN_FLIGHT"


@dataclass(frozen=True)
class EventRecord:
    time: float
    type: str
    request_id: int
    transfer_id: int | None = None


@dataclass
class RunResult:
    request_records: list[RequestRecord]
    transfer_records: list[TransferRecord]
    opportunity_records: list[OpportunityRecord]
    event_records: list[EventRecord]
    summary: dict
    validation: dict
    proactive_action_records: list[ProactiveActionRecord] = field(default_factory=list)
    candidate_decision_records: list[CandidateDecisionRecord] = field(default_factory=list)
    final_state: dict = field(default_factory=dict)
    copy_observation_records: list[dict] = field(default_factory=list)
    reuse_observation_records: list[dict] = field(default_factory=list)
