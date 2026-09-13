from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence


@dataclass(frozen=True)
class TraceRequest:
    request_id: int
    arrival_ms: float
    input_tokens: int
    output_tokens: int
    block_ids: tuple[int, ...]
    block_valid_tokens: tuple[int, ...]
    split: str | None = None

    @classmethod
    def from_record(
        cls, request_id: int, record: dict, page_tokens: int = 512,
        split_config=None,
    ) -> "TraceRequest":
        required = {"timestamp", "input_length", "output_length", "hash_ids"}
        missing = required - record.keys()
        if missing:
            raise ValueError(f"request {request_id}: missing fields {sorted(missing)}")
        arrival = record["timestamp"]
        input_tokens = record["input_length"]
        output_tokens = record["output_length"]
        ids = record["hash_ids"]
        if not isinstance(arrival, (int, float)) or arrival < 0:
            raise ValueError(f"request {request_id}: invalid timestamp")
        if not isinstance(input_tokens, int) or input_tokens <= 0:
            raise ValueError(f"request {request_id}: input_length must be positive")
        if not isinstance(output_tokens, int) or output_tokens < 0:
            raise ValueError(f"request {request_id}: output_length must be non-negative")
        if not isinstance(ids, list) or not all(isinstance(x, int) for x in ids):
            raise ValueError(f"request {request_id}: hash_ids must be a list of integers")
        expected_pages = math.ceil(input_tokens / page_tokens)
        if len(ids) != expected_pages:
            raise ValueError(
                f"request {request_id}: len(hash_ids)={len(ids)} != "
                f"ceil(input_length/{page_tokens})={expected_pages}"
            )
        tail = input_tokens - page_tokens * (expected_pages - 1)
        valid = (page_tokens,) * (expected_pages - 1) + (tail,)
        return cls(
            request_id=request_id,
            arrival_ms=float(arrival),
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            block_ids=tuple(ids),
            block_valid_tokens=valid,
            split=split_config.classify(float(arrival)) if split_config else None,
        )

    def hit_tokens(self, hit_pages: int) -> int:
        if not 0 <= hit_pages <= len(self.block_ids):
            raise ValueError("hit_pages outside request path")
        return sum(self.block_valid_tokens[:hit_pages])


def load_trace(path: str | Path, page_tokens: int = 512, split_config=None) -> list[TraceRequest]:
    requests: list[TraceRequest] = []
    previous_arrival = -math.inf
    depths: dict[int, int] = {}
    parents: dict[int, int | None] = {}
    valid_tokens: dict[int, int] = {}
    with Path(path).open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"line {line_number}: invalid JSON") from exc
            req = TraceRequest.from_record(len(requests), record, page_tokens, split_config)
            if req.arrival_ms < previous_arrival:
                raise ValueError(f"request {req.request_id}: timestamps are not non-decreasing")
            previous_arrival = req.arrival_ms
            if len(req.block_ids) != len(set(req.block_ids)):
                raise ValueError(f"request {req.request_id}: duplicate hash within request")
            for depth, block_id in enumerate(req.block_ids):
                parent = req.block_ids[depth - 1] if depth else None
                if block_id in depths and depths[block_id] != depth:
                    raise ValueError(f"hash {block_id}: conflicting depths")
                if block_id in parents and parents[block_id] != parent:
                    raise ValueError(f"hash {block_id}: conflicting parents")
                depths[block_id] = depth
                parents[block_id] = parent
                block_valid = req.block_valid_tokens[depth]
                if block_id in valid_tokens and valid_tokens[block_id] != block_valid:
                    raise ValueError(f"hash {block_id}: conflicting valid token counts")
                valid_tokens[block_id] = block_valid
            requests.append(req)
    return requests


def iter_arrival_batches(requests: Sequence[TraceRequest]) -> Iterator[list[TraceRequest]]:
    batch: list[TraceRequest] = []
    for request in requests:
        if batch and request.arrival_ms != batch[0].arrival_ms:
            yield batch
            batch = []
        batch.append(request)
    if batch:
        yield batch
