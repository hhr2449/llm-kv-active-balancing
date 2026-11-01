from __future__ import annotations

from dataclasses import dataclass


@dataclass
class MoveActionRecord:
    action_id: int
    opportunity_id: int
    request_id: int
    action_type: str
    policy: str
    decision_time: float
    chain_id: tuple[int, ...]
    chain_depth_pages: int
    source: int
    target: int
    wire_pages: int
    wire_tokens: int
    wire_bytes: int
    transfer_id: int
    start_time: float
    ready_time: float
    split: str
    source_generation_vector: tuple[int, ...]
    predicted_releasable_pages: int | None
    actual_source_released_pages: int
    actual_source_released_tokens: int
    actual_source_released_bytes: int
    release_fraction: float
    release_status: str
    release_stop_reason: str
    target_observation_status: str | None = None


@dataclass
class MoveSourceReleaseRecord:
    transfer_id: int
    ready_time: float
    source: int
    target: int
    chain_id: tuple[int, ...]
    source_generation_vector: tuple[int, ...]
    released_blocks_deep_to_root: tuple[int, ...]
    actual_source_released_pages: int
    release_fraction: float
    release_status: str
    release_stop_reason: str
