from __future__ import annotations

from dataclasses import dataclass

from .trace import TraceRequest


def transferable_prefix_pages(request: TraceRequest, matched_pages: int,
                              mode: str, page_tokens: int) -> int:
    if mode == "TRACE_PAGE":
        return matched_pages
    if mode != "FULL_PAGE_ONLY":
        raise ValueError(f"unsupported partial page mode: {mode}")
    if (matched_pages and matched_pages == len(request.block_ids)
            and request.block_valid_tokens[-1] < page_tokens):
        return matched_pages - 1
    return matched_pages


@dataclass
class Transfer:
    transfer_id: int
    request_id: int
    source_pod_id: int
    target_pod_id: int
    matched_prefix_pages: int
    transferable_prefix_pages: int
    wire_pages: int
    wire_bytes: int
    admit_ms: float | None = None
    complete_ms: float | None = None
    newly_resident_pages: int = 0

    @property
    def latency_ms(self) -> float:
        if self.admit_ms is None or self.complete_ms is None:
            return 0.0
        return self.complete_ms - self.admit_ms
