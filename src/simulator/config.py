from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

B0_IMPL_VERSION = "r_req_kv_v1"
METRIC_VERSION = "v1"


@dataclass(frozen=True)
class SplitConfig:
    development_start_ms: float = 0.0
    warmup_start_ms: float = 900000.0
    evaluation_start_ms: float = 1500000.0
    observation_tail_start_ms: float = 2700000.0

    def __post_init__(self) -> None:
        values = (self.development_start_ms, self.warmup_start_ms,
                  self.evaluation_start_ms, self.observation_tail_start_ms)
        if tuple(sorted(values)) != values or len(set(values)) != len(values):
            raise ValueError("split boundaries must be strictly increasing")

    def classify(self, timestamp_ms: float) -> str:
        if timestamp_ms < self.warmup_start_ms:
            return "DEVELOPMENT"
        if timestamp_ms < self.evaluation_start_ms:
            return "WARMUP"
        if timestamp_ms < self.observation_tail_start_ms:
            return "EVALUATION"
        return "OBSERVATION_TAIL"

    def as_dict(self) -> dict:
        return {
            "development": [self.development_start_ms, self.warmup_start_ms],
            "warmup": [self.warmup_start_ms, self.evaluation_start_ms],
            "evaluation": [self.evaluation_start_ms, self.observation_tail_start_ms],
            "observation_tail": [self.observation_tail_start_ms, None],
        }


@dataclass(frozen=True)
class SimulatorConfig:
    page_tokens: int
    base_latency_ms: float
    prefill_tokens_per_second: float
    cache_capacity_pages: int | None = None
    num_pods: int = 1
    routing_policy: str = "FIXED"
    transfer_enabled: bool = False
    theta_simple: float = 1.5
    page_bytes: int = 1
    effective_bandwidth_bytes_per_s: float = 1.0
    control_latency_ms: float = 0.0
    partial_page_mode: str = "FULL_PAGE_ONLY"
    transfer_mode: str = "FULL_PREFIX"
    cache_hit_threshold: float = 0.3
    relative_load_threshold: float = 1.5
    absolute_load_gap_ms: float = 500.0
    admission_timeout_ms: float = 5000.0
    split_config: SplitConfig | None = None
    random_seed: int = 0
    arrival_mode: str = "RAW_BUCKETED"
    summary_split: str = "EVALUATION"
    proactive_enabled: bool = False
    trigger_period_ms: float = 3000.0
    trigger_phase_ms: float = 0.0
    max_proactive_actions_per_tick: int = 1
    proactive_byte_rate: float = 0.0
    proactive_burst_bytes: int = 0
    oracle_horizon_ms: float = 30000.0
    oracle_visibility_end_ms: float = 900000.0
    proactive_no_evict_admission: bool = False
    proactive_fanout_targets: int = 1
    proactive_trigger_latest_ms: float | None = None
    proactive_strategy: str = "FUTURE_DEMAND"
    proactive_information_scope: str | None = None
    proactive_initial_tokens: float | None = None
    persistence_history_ms: float = 60000.0
    persistence_top_k: int = 10
    recency_selection_quantile: float = 0.9
    recency_decay_seconds: float = 60.0
    cost_v_ref: float = 1.0
    cost_t_ref_ms: float = 1.0
    protocol_version: str | None = None
    proactive_action: str = "COPY"
    o2p_forced_time_ms: float | None = None
    o2p_forced_prefix_id: int | None = None
    o2p_forced_source_pod: int | None = None
    o2p_forced_target_pod: int | None = None
    o2p_arrival_cutoff_ms: float | None = None
    o2a_committed_actions: tuple[tuple[float, int, int, int], ...] = ()
    o2a_probe_time_ms: float | None = None
    reactive_information_scope: str | None = None

    def __post_init__(self) -> None:
        if self.page_tokens <= 0:
            raise ValueError("page_tokens must be > 0")
        if self.base_latency_ms <= 0:
            raise ValueError("base_latency_ms must be > 0")
        if self.prefill_tokens_per_second <= 0:
            raise ValueError("prefill_tokens_per_second must be > 0")
        if self.cache_capacity_pages is not None and self.cache_capacity_pages < 0:
            raise ValueError("cache.capacity_pages must be >= 0 or null")
        if self.num_pods <= 0:
            raise ValueError("cluster.num_pods must be > 0")
        if self.routing_policy not in {"FIXED", "R_AFF", "R_LEAST", "R_REQ_KV_SIMPLE", "R_REQ_KV", "R_REQ_KV_TASK", "R_REACTIVE_ECT_V1"}:
            raise ValueError("unsupported routing.policy")
        if self.reactive_information_scope not in {None, "current_state_only"}:
            raise ValueError("unsupported routing.information_scope")
        if self.routing_policy == "R_REACTIVE_ECT_V1":
            if self.reactive_information_scope != "current_state_only":
                raise ValueError(
                    "R_REACTIVE_ECT_V1 requires routing.information_scope=current_state_only"
                )
            if not self.transfer_enabled:
                raise ValueError("R_REACTIVE_ECT_V1 requires transfer.enabled")
            if (self.proactive_enabled
                    and self.proactive_information_scope != "history_current_only"):
                raise ValueError(
                    "R_REACTIVE_ECT_V1 only permits the frozen history_current_only "
                    "proactive controller"
                )
        if self.theta_simple <= 0 or self.page_bytes <= 0:
            raise ValueError("transfer theta/page_bytes must be positive")
        if self.effective_bandwidth_bytes_per_s <= 0 or self.control_latency_ms < 0:
            raise ValueError("invalid transfer timing parameters")
        if self.partial_page_mode not in {"FULL_PAGE_ONLY", "TRACE_PAGE"}:
            raise ValueError("invalid transfer.partial_page_mode")
        if self.transfer_mode != "FULL_PREFIX":
            raise ValueError("Kernel v3a only supports FULL_PREFIX")
        if not 0 <= self.cache_hit_threshold <= 1:
            raise ValueError("cache_hit_threshold must be in [0,1]")
        if self.relative_load_threshold < 0 or self.absolute_load_gap_ms < 0:
            raise ValueError("load gates must be non-negative")
        if self.admission_timeout_ms < 0:
            raise ValueError("admission_timeout_ms must be non-negative")
        if self.summary_split not in {"DEVELOPMENT", "WARMUP", "EVALUATION", "OBSERVATION_TAIL"}:
            raise ValueError("invalid experiment.summary_split")
        if self.trigger_period_ms <= 0 or self.trigger_phase_ms < 0:
            raise ValueError("invalid proactive trigger period/phase")
        if self.max_proactive_actions_per_tick < 0:
            raise ValueError("max proactive actions must be non-negative")
        if self.proactive_byte_rate < 0 or self.proactive_burst_bytes < 0:
            raise ValueError("invalid proactive token bucket")
        if self.oracle_horizon_ms <= 0:
            raise ValueError("oracle horizon must be positive")
        if self.proactive_fanout_targets not in {1, 2}:
            raise ValueError("proactive fanout_targets must be 1 or 2")
        if self.proactive_strategy not in {"FUTURE_DEMAND", "PERSIST", "RECENCY", "COST_AWARE"}:
            raise ValueError("unsupported proactive.strategy")
        if self.proactive_information_scope not in {None, "history_current_only"}:
            raise ValueError("unsupported proactive.information_scope")
        if self.proactive_information_scope == "history_current_only":
            if not self.proactive_enabled:
                raise ValueError("history_current_only requires proactive.enabled")
            if self.proactive_strategy not in {"PERSIST", "COST_AWARE"}:
                raise ValueError(
                    "history_current_only only supports PERSIST or COST_AWARE"
                )
            if (self.routing_policy not in {"R_REQ_KV", "R_REACTIVE_ECT_V1"}
                    or not self.transfer_enabled):
                raise ValueError(
                    "history_current_only requires an R_REQ_KV_V1 or "
                    "R_REACTIVE_ECT_V1 base"
                )
        if self.persistence_history_ms <= 0 or self.persistence_top_k <= 0:
            raise ValueError("invalid persistence parameters")
        if not 0 <= self.recency_selection_quantile <= 1 or self.recency_decay_seconds <= 0:
            raise ValueError("invalid recency parameters")
        if self.cost_v_ref <= 0 or self.cost_t_ref_ms <= 0:
            raise ValueError("cost references must be positive")
        if self.proactive_action not in {"COPY", "MOVE_SAFE", "O2P_PROBE", "O2A_PROBE",
                                         "O2A_CLOSED_LOOP",
                                         "O2P_NO_COPY", "O2P_FORCED_COPY"}:
            raise ValueError("unsupported proactive.action")
        if self.proactive_action in {"O2P_NO_COPY", "O2P_FORCED_COPY"} \
                and self.o2p_forced_time_ms is None:
            raise ValueError("O2P branch action requires forced_time_ms")
        if self.proactive_action == "O2P_FORCED_COPY" and None in {
                self.o2p_forced_prefix_id, self.o2p_forced_source_pod,
                self.o2p_forced_target_pod}:
            raise ValueError("O2P forced COPY requires prefix/source/target")
        if self.proactive_action == "O2A_CLOSED_LOOP" and self.o2a_probe_time_ms is None:
            raise ValueError("O2-A closed-loop replay requires probe_time_ms")
        times = [action[0] for action in self.o2a_committed_actions]
        if times != sorted(set(times)):
            raise ValueError("O2-A committed action times must be unique and sorted")

    @classmethod
    def from_yaml(cls, path: str | Path) -> "SimulatorConfig":
        with Path(path).open("r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle)
        if not isinstance(raw, dict) or not isinstance(raw.get("simulator"), dict):
            raise ValueError("config must contain a 'simulator' mapping")
        cfg = raw["simulator"]
        cache = raw.get("cache")
        if not isinstance(cache, dict) or "capacity_pages" not in cache:
            raise ValueError("config must contain cache.capacity_pages")
        cluster = raw.get("cluster")
        if not isinstance(cluster, dict) or "num_pods" not in cluster:
            raise ValueError("config must contain cluster.num_pods")
        routing = raw.get("routing")
        if not isinstance(routing, dict) or "policy" not in routing:
            raise ValueError("config must contain routing.policy")
        transfer = raw.get("transfer", {})
        if not isinstance(transfer, dict):
            raise ValueError("transfer must be a mapping")
        split_raw = raw.get("split")
        split = None
        if split_raw is not None:
            split = SplitConfig(
                float(split_raw["development_start_ms"]),
                float(split_raw["warmup_start_ms"]),
                float(split_raw["evaluation_start_ms"]),
                float(split_raw["observation_tail_start_ms"]),
            )
        service_cfg = raw.get("service", cfg)
        proactive = raw.get("proactive", {})
        oracle = raw.get("oracle", {})
        if "page_tokens" not in cfg:
            raise ValueError("missing simulator config key: page_tokens")
        missing_service = {"base_latency_ms", "prefill_tokens_per_second"} - service_cfg.keys()
        if missing_service:
            raise ValueError(f"missing service config keys: {sorted(missing_service)}")
        capacity = cache["capacity_pages"]
        return cls(
            page_tokens=int(cfg["page_tokens"]),
            base_latency_ms=float(service_cfg["base_latency_ms"]),
            prefill_tokens_per_second=float(service_cfg["prefill_tokens_per_second"]),
            cache_capacity_pages=None if capacity is None else int(capacity),
            num_pods=int(cluster["num_pods"]),
            routing_policy=str(routing["policy"]),
            reactive_information_scope=(
                None if routing.get("information_scope") is None
                else str(routing["information_scope"])
            ),
            transfer_enabled=bool(transfer.get("enabled", False)),
            theta_simple=float(transfer.get("theta_simple", 1.5)),
            page_bytes=int(transfer.get("page_bytes", 1)),
            effective_bandwidth_bytes_per_s=float(
                transfer.get("effective_bandwidth_bytes_per_s", 1.0)
            ),
            control_latency_ms=float(transfer.get("control_latency_ms", 0.0)),
            partial_page_mode=str(transfer.get("partial_page_mode", "FULL_PAGE_ONLY")),
            transfer_mode=str(transfer.get("transfer_mode", "FULL_PREFIX")),
            cache_hit_threshold=float(routing.get("cache_hit_threshold",
                                                  transfer.get("cache_hit_threshold", 0.3))),
            relative_load_threshold=float(routing.get("relative_load_threshold",
                                                      transfer.get("relative_load_threshold", 1.5))),
            absolute_load_gap_ms=float(routing.get("absolute_load_gap_ms",
                                                   transfer.get("absolute_load_gap_ms", 500.0))),
            admission_timeout_ms=float(transfer.get("admission_timeout_ms", 5000.0)),
            split_config=split,
            random_seed=int(raw.get("experiment", {}).get("random_seed", 0)),
            arrival_mode=str(raw.get("experiment", {}).get("arrival_mode", "RAW_BUCKETED")),
            summary_split=str(raw.get("experiment", {}).get("summary_split", "EVALUATION")),
            proactive_enabled=bool(proactive.get("enabled", False)),
            trigger_period_ms=float(proactive.get("trigger_period_ms", 3000.0)),
            trigger_phase_ms=float(proactive.get("trigger_phase_ms", 0.0)),
            max_proactive_actions_per_tick=int(
                proactive.get("max_proactive_actions_per_tick", 1)
            ),
            proactive_byte_rate=float(proactive.get("proactive_byte_rate", 0.0)),
            proactive_burst_bytes=int(proactive.get("proactive_burst_bytes", 0)),
            oracle_horizon_ms=float(oracle.get("horizon_ms", 30000.0)),
            oracle_visibility_end_ms=float(oracle.get("visibility_end_ms", 900000.0)),
            proactive_no_evict_admission=bool(
                proactive.get("no_evict_admission", False)
            ),
            proactive_fanout_targets=int(proactive.get("fanout_targets", 1)),
            proactive_trigger_latest_ms=(
                None if proactive.get("trigger_latest_ms") is None
                else float(proactive["trigger_latest_ms"])
            ),
            proactive_strategy=str(proactive.get("strategy", "FUTURE_DEMAND")),
            proactive_information_scope=(
                None if proactive.get("information_scope") is None
                else str(proactive["information_scope"])
            ),
            proactive_initial_tokens=(None if proactive.get("initial_tokens") is None
                                      else float(proactive["initial_tokens"])),
            persistence_history_ms=float(proactive.get("persistence_history_ms", 60000.0)),
            persistence_top_k=int(proactive.get("persistence_top_k", 10)),
            recency_selection_quantile=float(proactive.get("recency_selection_quantile", 0.9)),
            recency_decay_seconds=float(proactive.get("recency_decay_seconds", 60.0)),
            cost_v_ref=float(proactive.get("cost_v_ref", 1.0)),
            cost_t_ref_ms=float(proactive.get("cost_t_ref_ms", 1.0)),
            protocol_version=(None if raw.get("experiment", {}).get("protocol_version") is None
                              else str(raw["experiment"]["protocol_version"])),
            proactive_action=str(proactive.get("action", "COPY")),
            o2p_forced_time_ms=(None if proactive.get("forced_time_ms") is None
                                else float(proactive["forced_time_ms"])),
            o2p_forced_prefix_id=(None if proactive.get("forced_prefix_id") is None
                                  else int(proactive["forced_prefix_id"])),
            o2p_forced_source_pod=(None if proactive.get("forced_source_pod") is None
                                   else int(proactive["forced_source_pod"])),
            o2p_forced_target_pod=(None if proactive.get("forced_target_pod") is None
                                   else int(proactive["forced_target_pod"])),
            o2p_arrival_cutoff_ms=(None if proactive.get("arrival_cutoff_ms") is None
                                   else float(proactive["arrival_cutoff_ms"])),
        )
