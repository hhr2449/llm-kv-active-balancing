from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from pathlib import Path

import yaml

from ..config import (BANDWIDTH_BYTES_PER_SECOND, LOAD_WINDOW_MS, PAGE_BYTES,
                      PAGE_TOKENS, STAGE_C_VERSION)

STUDY_VERSION = "TASK_MAIN_STAGE_D1_SENSITIVITY_V1"
TRACE_IDENTITIES = {
    "conversation": "b8cbb061a85206d729d91cdc2981f43c9e0d99209dce588d3af5f7934408b9df",
    "toolagent": "48a2db1a13d3bc05e6330140c64f604ba366df20d3c9e128b5c35a01c1fa5f71",
}
FAMILIES = frozenset({"R_LEAST", "THETA", "PERSISTENCE_K", "RECENCY_Q", "ORACLE_W", "N"})


@dataclass(frozen=True)
class StageD1Config:
    workload: str
    family: str
    case_id: str
    routing_policy: str
    proactive_policy: str
    num_pods: int = 4
    capacity_pages: int = 585
    protocol_version: str = STAGE_C_VERSION
    study_version: str = STUDY_VERSION
    page_tokens: int = PAGE_TOKENS
    page_bytes: int = PAGE_BYTES
    bandwidth_bytes_per_second: int = BANDWIDTH_BYTES_PER_SECOND
    load_window_ms: int = LOAD_WINDOW_MS
    theta: float = 2.0
    history_window_ms: int = 300000
    future_window_ms: int = 300000
    shortlist_k: int = 10
    recency_decay_ms: int = 60000
    recency_quantile: float = 0.9
    visibility_end_ms: int = 3537000
    experiment_kind: str = "sensitivity"
    line_id: int = 0
    evaluation_start_ms: int = 1500000
    evaluation_end_ms: int = 2700000
    random_seed: int = 20260911
    random_gini_repetitions: int = 200
    lambda_load: float = 0.5
    lambda_cost: float = 0.1
    action: str = "COPY"
    trace_path: str = ""
    trace_sha256: str = ""

    def __post_init__(self) -> None:
        if self.workload not in TRACE_IDENTITIES:
            raise ValueError("Stage D1 requires a frozen workload")
        if self.family not in FAMILIES:
            raise ValueError("unknown Stage D1 family")
        if not isinstance(self.case_id, str) or not self.case_id:
            raise ValueError("case_id must be nonempty")
        if self.protocol_version != STAGE_C_VERSION or self.study_version != STUDY_VERSION:
            raise ValueError("Stage D1 preserves Stage C protocol semantics")
        if self.experiment_kind != "sensitivity" or self.line_id != 0:
            raise ValueError("Stage D1 config identity mismatch")
        if self.routing_policy not in {"R_AFF", "R_LEAST", "R_REQ_KV_TASK"}:
            raise ValueError("unsupported Stage D1 routing")
        if self.proactive_policy not in {"NONE", "FUTURE_DEMAND", "PERSISTENCE", "RECENCY"}:
            raise ValueError("unsupported Stage D1 proactive policy")
        if self.proactive_policy != "NONE" and self.routing_policy != "R_AFF":
            raise ValueError("proactive sensitivity requires R_AFF")
        integer_fields = ("num_pods", "capacity_pages", "page_tokens", "page_bytes",
                          "bandwidth_bytes_per_second", "load_window_ms", "history_window_ms",
                          "future_window_ms", "shortlist_k", "recency_decay_ms",
                          "visibility_end_ms", "evaluation_start_ms", "evaluation_end_ms",
                          "random_seed", "random_gini_repetitions")
        if any(type(getattr(self, name)) is not int for name in integer_fields):
            raise ValueError("integer Stage D1 fields must not use bool/float")
        if type(self.theta) not in {int, float} or type(self.recency_quantile) not in {int, float}:
            raise ValueError("theta/q must be numeric")
        frozen = {
            "capacity_pages": 585, "page_tokens": PAGE_TOKENS, "page_bytes": PAGE_BYTES,
            "bandwidth_bytes_per_second": BANDWIDTH_BYTES_PER_SECOND,
            "load_window_ms": LOAD_WINDOW_MS, "recency_decay_ms": 60000,
            "visibility_end_ms": 3537000, "random_seed": 20260911,
            "random_gini_repetitions": 200, "lambda_load": .5, "lambda_cost": .1,
            "action": "COPY",
        }
        for name, expected in frozen.items():
            if getattr(self, name) != expected:
                raise ValueError(f"Stage D1 freezes {name}={expected}")
        if self.trace_path != f"data/mooncake/{self.workload}_trace.jsonl" or self.trace_sha256 != TRACE_IDENTITIES[self.workload]:
            raise ValueError("Stage D1 trace identity mismatch")
        self._validate_fixed_case()

    def _validate_fixed_case(self) -> None:
        common = dict(N=self.num_pods, routing=self.routing_policy, policy=self.proactive_policy,
                      W=self.future_window_ms, h=self.history_window_ms, K=self.shortlist_k,
                      q=float(self.recency_quantile), theta=float(self.theta),
                      start=self.evaluation_start_ms, end=self.evaluation_end_ms)
        expected = None
        if self.family == "R_LEAST" and self.case_id == "rleast":
            expected = dict(N=4, routing="R_LEAST", policy="NONE", W=300000, h=300000,
                            K=10, q=.9, theta=2., start=1500000, end=2700000)
        elif self.family == "THETA" and self.case_id in {"theta_1p5", "theta_3"}:
            value = 1.5 if self.case_id == "theta_1p5" else 3.
            expected = dict(N=4, routing="R_REQ_KV_TASK", policy="NONE", W=300000,
                            h=300000, K=10, q=.9, theta=value, start=1500000, end=2700000)
        elif self.family == "PERSISTENCE_K" and self.case_id in {
                "persist_h60_k5", "persist_h60_k20", "persist_h300_k5", "persist_h300_k20"}:
            parts = self.case_id.split("_")
            h = 60000 if parts[1] == "h60" else 300000
            k = int(parts[2][1:])
            expected = dict(N=4, routing="R_AFF", policy="PERSISTENCE", W=300000,
                            h=h, K=k, q=.9, theta=2., start=1500000, end=2700000)
        elif self.family == "RECENCY_Q" and self.case_id == "recency_q0p8":
            expected = dict(N=4, routing="R_AFF", policy="RECENCY", W=300000, h=300000,
                            K=10, q=.8, theta=2., start=1500000, end=2700000)
        elif self.family == "ORACLE_W" and self.case_id in {"oracle_w1m", "oracle_w5m", "oracle_w30m"}:
            window = {"oracle_w1m": 60000, "oracle_w5m": 300000,
                      "oracle_w30m": 1800000}[self.case_id]
            expected = dict(N=4, routing="R_AFF", policy="FUTURE_DEMAND", W=window,
                            h=300000, K=10, q=.9, theta=2., start=600000, end=1500000)
        elif self.family == "N" and self.case_id in {
                "n2_aff", "n2_reqkv", "n2_oracle", "n2_persist300", "n2_recency"}:
            routing, policy = {
                "n2_aff": ("R_AFF", "NONE"), "n2_reqkv": ("R_REQ_KV_TASK", "NONE"),
                "n2_oracle": ("R_AFF", "FUTURE_DEMAND"),
                "n2_persist300": ("R_AFF", "PERSISTENCE"),
                "n2_recency": ("R_AFF", "RECENCY"),
            }[self.case_id]
            expected = dict(N=2, routing=routing, policy=policy, W=300000, h=300000,
                            K=10, q=.9, theta=2., start=1500000, end=2700000)
        if expected is None or common != expected:
            raise ValueError(f"config is outside the frozen Stage D1 matrix: {self.family}/{self.case_id}")

    def split_at(self, arrival_time: float) -> str:
        if arrival_time < self.evaluation_start_ms:
            return "WARMUP"
        if arrival_time < self.evaluation_end_ms:
            return "EVALUATION"
        return "OBSERVATION_TAIL"

    def as_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_yaml(cls, path: str | Path) -> "StageD1Config":
        raw = yaml.safe_load(Path(path).read_text())
        if not isinstance(raw, dict):
            raise ValueError("Stage D1 config must be a mapping")
        allowed = {item.name for item in fields(cls)}
        unknown = set(raw)-allowed
        if unknown:
            raise ValueError(f"unknown Stage D1 fields: {sorted(unknown)}")
        return cls(**raw)
