from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Sequence

from .cache import AdmissionPlan, TaskMainCache
from .config import BANDWIDTH_BYTES_PER_SECOND, PAGE_BYTES, PAGE_TOKENS
from .records import TransferRecord
from .trace import TraceRequest, full_page_prefix


@dataclass(frozen=True)
class TransferPlan:
    source: int
    target: int
    path: tuple[int, ...]
    target_admission: AdmissionPlan


@dataclass(frozen=True)
class PreflightResult:
    plan: TransferPlan | None
    failure_reason: str | None


class IndependentTransfers:
    """Every transfer independently receives 25 GB/s, including shared endpoints."""

    def __init__(self, caches: Sequence[TaskMainCache]) -> None:
        self.caches = tuple(caches)
        self.records: list[TransferRecord] = []

    def preflight(self, request: TraceRequest, hit_pages: int,
                  source: int, target: int) -> PreflightResult:
        path = full_page_prefix(request, hit_pages)
        return self.preflight_chain(path, source, target)

    def preflight_chain(self, path: tuple[int, ...], source: int, target: int) -> PreflightResult:
        if not path:
            return PreflightResult(None, "REACTIVE_FALLBACK_NO_TRANSFERABLE_PREFIX")
        if source == target or not (0 <= source < len(self.caches)
                                    and 0 <= target < len(self.caches)):
            return PreflightResult(None, "REACTIVE_FALLBACK_INVALID_ENDPOINT")
        if self.caches[source].lookup(path) != len(path):
            return PreflightResult(None, "REACTIVE_FALLBACK_SOURCE_UNAVAILABLE")
        admission = self.caches[target].plan_temporary(len(path))
        if admission is None:
            return PreflightResult(None, "REACTIVE_FALLBACK_CAPACITY")
        return PreflightResult(TransferPlan(source, target, path, admission), None)

    def start(self, plan: TransferPlan, request_id: int, now: float,
              transfer_type: str = "REACTIVE") -> TransferRecord:
        if transfer_type not in {"REACTIVE", "PROACTIVE"}:
            raise ValueError("unsupported transfer type")
        source, target = self.caches[plan.source], self.caches[plan.target]
        # Verify everything before either Cache changes. There is no event interleave
        # between this validation and the two commits.
        target.validate_plan(plan.target_admission, "TEMPORARY")
        if not plan.path or source.lookup(plan.path) != len(plan.path):
            raise ValueError("source changed after transfer preflight")
        if plan.target_admission.additional_pages != len(plan.path):
            raise ValueError("Temporary admission does not match wire chain")
        wire_pages = len(plan.path)
        wire_bytes = wire_pages * PAGE_BYTES
        ready = now + wire_bytes / BANDWIDTH_BYTES_PER_SECOND * 1000.0
        if not math.isfinite(now) or now < 0 or not math.isfinite(ready) or ready <= now:
            raise ValueError("invalid or unrepresentable transfer time")
        transfer_id = len(self.records)
        source.pin(plan.path)
        target.reserve_temporary(transfer_id, plan.target_admission, now)
        record = TransferRecord(
            transfer_id, request_id, transfer_type, plan.source, plan.target,
            plan.path, wire_pages, wire_pages * PAGE_TOKENS, wire_bytes,
            now, ready, wire_pages,
        )
        self.records.append(record)
        return record

    def complete(self, transfer_id: int, now: float) -> TransferRecord:
        record = self.records[transfer_id]
        if record.status != "IN_FLIGHT" or now != record.ready_time:
            raise ValueError("transfer may complete only once, exactly at ready time")
        source, target = self.caches[record.source], self.caches[record.target]
        inserted = target.complete_transfer(transfer_id, record.transferable_chain, now)
        source.unpin(record.transferable_chain)
        record.newly_resident_pages = inserted
        record.duplicate_pages = record.wire_pages - inserted
        record.status = "COMPLETED"
        return record
