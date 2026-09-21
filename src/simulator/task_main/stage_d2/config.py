from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from pathlib import Path

import yaml

from ..config import (BANDWIDTH_BYTES_PER_SECOND, LOAD_WINDOW_MS, PAGE_BYTES,
                      PAGE_TOKENS, STAGE_C_VERSION)

STUDY_VERSION = "TASK_MAIN_STAGE_D2_MOVE_ABLATION_V1"
TRACE_IDENTITIES = {
    "conversation": "b8cbb061a85206d729d91cdc2981f43c9e0d99209dce588d3af5f7934408b9df",
    "toolagent": "48a2db1a13d3bc05e6330140c64f604ba366df20d3c9e128b5c35a01c1fa5f71",
}


@dataclass(frozen=True)
class StageD2Config:
    workload: str
    case_id: str
    proactive_policy: str
    action: str = "MOVE"
    routing_policy: str = "R_AFF"
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
    experiment_kind: str = "move_ablation"
    line_id: int = 0
    evaluation_start_ms: int = 1500000
    evaluation_end_ms: int = 2700000
    random_seed: int = 20260911
    random_gini_repetitions: int = 200
    lambda_load: float = 0.5
    lambda_cost: float = 0.1
    trace_path: str = ""
    trace_sha256: str = ""

    def __post_init__(self):
        if self.workload not in TRACE_IDENTITIES:
            raise ValueError("Stage D2 requires a frozen workload")
        if self.case_id not in {"oracle_move", "persist300_move"}:
            raise ValueError("Stage D2 has exactly two cases per workload")
        expected_policy = "FUTURE_DEMAND" if self.case_id == "oracle_move" else "PERSISTENCE"
        if self.proactive_policy != expected_policy:
            raise ValueError("Stage D2 case/policy mismatch")
        if self.action not in {"COPY", "MOVE"}:
            raise ValueError("Stage D2 supports only COPY or MOVE")
        if self.protocol_version != STAGE_C_VERSION or self.study_version != STUDY_VERSION:
            raise ValueError("Stage D2 preserves frozen Stage C base semantics")
        frozen = {
            "routing_policy": "R_AFF", "num_pods": 4, "capacity_pages": 585,
            "page_tokens": PAGE_TOKENS, "page_bytes": PAGE_BYTES,
            "bandwidth_bytes_per_second": BANDWIDTH_BYTES_PER_SECOND,
            "load_window_ms": LOAD_WINDOW_MS, "theta": 2.0,
            "history_window_ms": 300000, "future_window_ms": 300000,
            "shortlist_k": 10, "recency_decay_ms": 60000,
            "recency_quantile": 0.9, "visibility_end_ms": 3537000,
            "experiment_kind": "move_ablation", "line_id": 0,
            "evaluation_start_ms": 1500000, "evaluation_end_ms": 2700000,
            "random_seed": 20260911, "random_gini_repetitions": 200,
            "lambda_load": 0.5, "lambda_cost": 0.1,
        }
        for key, expected in frozen.items():
            if getattr(self, key) != expected:
                raise ValueError(f"Stage D2 freezes {key}={expected}")
        integer_fields = (
            "num_pods", "capacity_pages", "page_tokens", "page_bytes",
            "bandwidth_bytes_per_second", "load_window_ms", "history_window_ms",
            "future_window_ms", "shortlist_k", "recency_decay_ms",
            "visibility_end_ms", "line_id", "evaluation_start_ms",
            "evaluation_end_ms", "random_seed", "random_gini_repetitions",
        )
        if any(type(getattr(self, key)) is not int for key in integer_fields):
            raise ValueError("integer Stage D2 fields must not use bool/float")
        if self.trace_path != f"data/mooncake/{self.workload}_trace.jsonl":
            raise ValueError("Stage D2 trace path mismatch")
        if self.trace_sha256 != TRACE_IDENTITIES[self.workload]:
            raise ValueError("Stage D2 trace identity mismatch")

    def split_at(self, arrival_time):
        if arrival_time < self.evaluation_start_ms:
            return "WARMUP"
        if arrival_time < self.evaluation_end_ms:
            return "EVALUATION"
        return "OBSERVATION_TAIL"

    def as_dict(self):
        return asdict(self)

    @classmethod
    def from_yaml(cls, path: str | Path):
        raw = yaml.safe_load(Path(path).read_text())
        if not isinstance(raw, dict):
            raise ValueError("Stage D2 config must be a mapping")
        allowed = {field.name for field in fields(cls)}
        unknown = set(raw)-allowed
        if unknown:
            raise ValueError(f"unknown Stage D2 fields: {sorted(unknown)}")
        value = cls(**raw)
        if value.action != "MOVE":
            raise ValueError("persisted Stage D2 experiment configs must use MOVE")
        return value


BEHAVIOR_FIELDS = (
    "workload", "routing_policy", "num_pods", "capacity_pages", "protocol_version",
    "page_tokens", "page_bytes", "bandwidth_bytes_per_second", "load_window_ms",
    "theta", "proactive_policy", "history_window_ms", "future_window_ms",
    "shortlist_k", "recency_decay_ms", "recency_quantile", "visibility_end_ms",
    "evaluation_start_ms", "evaluation_end_ms", "random_seed",
    "random_gini_repetitions", "lambda_load", "lambda_cost", "trace_path",
    "trace_sha256",
)


def behavior_projection(config):
    value = config.as_dict() if hasattr(config, "as_dict") else dict(config)
    return {key: value[key] for key in BEHAVIOR_FIELDS}
