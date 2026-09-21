from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from pathlib import Path

import yaml

PROTOCOL_VERSION = "TASK_MAIN_V1_STAGE_A"
STAGE_B_VERSION = "TASK_MAIN_V1_STAGE_B"
STAGE_C_VERSION = "TASK_MAIN_V1_STAGE_C"
PAGE_TOKENS = 512
PAGE_BYTES = 14 * 1024 * 1024
BANDWIDTH_BYTES_PER_SECOND = 25_000_000_000
LOAD_WINDOW_MS = 60_000


@dataclass(frozen=True)
class TaskMainConfig:
    workload: str = "synthetic"
    routing_policy: str = "R_AFF"
    num_pods: int = 4
    capacity_pages: int = 585
    protocol_version: str = PROTOCOL_VERSION
    page_tokens: int = PAGE_TOKENS
    page_bytes: int = PAGE_BYTES
    bandwidth_bytes_per_second: int = BANDWIDTH_BYTES_PER_SECOND
    load_window_ms: int = LOAD_WINDOW_MS
    theta: int = 2
    proactive_policy: str = "NONE"
    history_window_ms: int = 300000
    future_window_ms: int = 300000
    shortlist_k: int = 10
    recency_decay_ms: int = 60000
    recency_quantile: float = 0.9
    visibility_end_ms: int = 3537000

    experiment_kind: str = "synthetic"
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
        if not isinstance(self.workload, str) or not self.workload.strip():
            raise ValueError("workload must be a nonempty string")
        if self.routing_policy not in {"R_AFF", "R_REQ_KV_TASK"}:
            raise ValueError("Stage A only allows R_AFF and R_REQ_KV_TASK")
        for name in ("num_pods", "capacity_pages", "page_tokens", "page_bytes",
                     "bandwidth_bytes_per_second", "load_window_ms", "theta"):
            if type(getattr(self, name)) is not int:
                raise ValueError(f"{name} must be an integer")
        if self.num_pods < 1 or self.capacity_pages < 0:
            raise ValueError("num_pods must be positive and capacity_pages nonnegative")
        if self.protocol_version not in {PROTOCOL_VERSION, STAGE_B_VERSION, STAGE_C_VERSION}:
            raise ValueError("unsupported TaskMain protocol_version")
        if self.proactive_policy not in {"NONE", "FUTURE_DEMAND", "PERSISTENCE", "RECENCY",
                                         "PERSISTENCE_COST_AWARE"}:
            raise ValueError("unsupported proactive policy")
        if self.proactive_policy != "NONE" and (
            self.protocol_version not in {STAGE_B_VERSION, STAGE_C_VERSION} or self.routing_policy != "R_AFF"
        ):
            raise ValueError("proactive requires Stage B and R_AFF")
        if type(self.history_window_ms) is not int or self.history_window_ms not in {60000, 300000}:
            raise ValueError("history window must be 60s or 300s")
        if self.proactive_policy == "PERSISTENCE_COST_AWARE" and self.history_window_ms != 300000:
            raise ValueError("Cost-aware requires h=W=300s")
        frozen = {
            "page_tokens": PAGE_TOKENS,
            "page_bytes": PAGE_BYTES,
            "bandwidth_bytes_per_second": BANDWIDTH_BYTES_PER_SECOND,
            "load_window_ms": LOAD_WINDOW_MS, "theta": 2,
            "future_window_ms": 300000, "shortlist_k": 10, "recency_decay_ms": 60000,
            "recency_quantile": 0.9,
            "visibility_end_ms": 960000 if self.protocol_version == STAGE_C_VERSION and self.experiment_kind == "pilot" else 3537000,
            "random_seed": 20260911, "random_gini_repetitions": 200,
            "lambda_load": 0.5, "lambda_cost": 0.1, "action": "COPY",
            "evaluation_start_ms": 300000 if self.experiment_kind == "pilot" else 1500000,
            "evaluation_end_ms": 600000 if self.experiment_kind == "pilot" else 2700000,
        }
        for key, value in frozen.items():
            if type(getattr(self, key)) is not type(value) or getattr(self, key) != value:
                raise ValueError(f"Stage A freezes {key}={value}")

        if self.experiment_kind not in {"synthetic", "formal", "pilot"}:
            raise ValueError("unknown experiment kind")
        if self.experiment_kind != "synthetic":
            if self.protocol_version != STAGE_C_VERSION or type(self.line_id) is not int or self.line_id not in range(1, 8):
                raise ValueError("matrix configs require Stage C and line 1..7")
            expected = [("R_AFF", "NONE", 300000), ("R_REQ_KV_TASK", "NONE", 300000),
                        ("R_AFF", "FUTURE_DEMAND", 300000), ("R_AFF", "PERSISTENCE", 60000),
                        ("R_AFF", "PERSISTENCE", 300000), ("R_AFF", "RECENCY", 300000),
                        ("R_AFF", "PERSISTENCE_COST_AWARE", 300000)][self.line_id-1]
            if (self.routing_policy, self.proactive_policy, self.history_window_ms) != expected:
                raise ValueError("line does not match frozen matrix")
            identities = {"conversation": "b8cbb061a85206d729d91cdc2981f43c9e0d99209dce588d3af5f7934408b9df",
                          "toolagent": "48a2db1a13d3bc05e6330140c64f604ba366df20d3c9e128b5c35a01c1fa5f71"}
            if (self.trace_sha256 != identities.get(self.workload) or
                self.trace_path != f"data/mooncake/{self.workload}_trace.jsonl"):
                raise ValueError("matrix trace identity mismatch")
            if self.num_pods != 4 or self.capacity_pages != 585:
                raise ValueError("matrix requires N=4, capacity=585")

    def split_at(self, arrival_time):
        if self.experiment_kind == "pilot":
            return ("WARMUP" if arrival_time < self.evaluation_start_ms else
                    "EVALUATION" if arrival_time < self.evaluation_end_ms else "OBSERVATION_TAIL")
        return classify_split(arrival_time)

    @classmethod
    def from_yaml(cls, path: str | Path) -> "TaskMainConfig":
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("TaskMain config must be a mapping")
        allowed = {field.name for field in fields(cls)}
        unknown = raw.keys() - allowed
        if unknown:
            raise ValueError(f"unsupported Stage A config fields: {sorted(unknown)}")
        if raw.get("protocol_version") not in {PROTOCOL_VERSION, STAGE_B_VERSION, STAGE_C_VERSION}:
            raise ValueError("explicit Stage A protocol_version is required")
        config = cls(**raw)
        if config.num_pods != 4 or config.capacity_pages != 585:
            raise ValueError("file-based main configs require N=4 and capacity=585")
        return config

    def as_dict(self) -> dict:
        return asdict(self)


def classify_split(arrival_time: float) -> str:
    if arrival_time < 900_000:
        return "DEVELOPMENT"
    if arrival_time < 1_500_000:
        return "WARMUP"
    if arrival_time < 2_700_000:
        return "EVALUATION"
    return "OBSERVATION_TAIL"
