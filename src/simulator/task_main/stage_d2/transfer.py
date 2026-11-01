from __future__ import annotations

from dataclasses import dataclass

from ..transfer import IndependentTransfers


@dataclass
class MoveMetadata:
    transfer_id: int
    source_generation_vector: tuple[int, ...]
    predicted_releasable_pages: int
    release: dict | None = None
    target_commit_failed: bool = False


class StageD2Transfers(IndependentTransfers):
    def __init__(self, caches, action):
        super().__init__(caches)
        self.action = action
        self.move_metadata = {}

    def start(self, plan, request_id, now, transfer_type="REACTIVE"):
        generations = predicted = None
        if self.action == "MOVE" and transfer_type == "PROACTIVE":
            source = self.caches[plan.source]
            generations = source.source_generations(plan.path)
            predicted = source.predicted_releasable_pages(plan.path, generations)
        record = super().start(plan, request_id, now, transfer_type)
        if generations is not None:
            self.move_metadata[record.transfer_id] = MoveMetadata(
                record.transfer_id, generations, predicted)
        return record

    def complete(self, transfer_id, now):
        metadata = self.move_metadata.get(transfer_id)
        if metadata is None:
            return super().complete(transfer_id, now)
        record = self.records[transfer_id]
        if record.status != "IN_FLIGHT" or now != record.ready_time:
            raise ValueError("transfer may complete only once, exactly at ready time")
        source, target = self.caches[record.source], self.caches[record.target]
        try:
            inserted = target.complete_transfer(transfer_id, record.transferable_chain, now)
        except BaseException:
            # Target commit is atomic in TaskMainCache. Restore only temporary
            # reservations/pins; no source deletion has happened.
            target.cancel_temporary(transfer_id)
            source.unpin(record.transferable_chain)
            metadata.target_commit_failed = True
            metadata.release = {
                "released_blocks_deep_to_root": (),
                "actual_source_released_pages": 0,
                "release_fraction": 0.0,
                "release_status": "ZERO_RELEASE",
                "release_stop_reason": "TARGET_COMMIT_FAILED",
                "ready_time": now,
            }
            record.status = "FAILED_TARGET_COMMIT"
            raise
        # Target is now Published before the MOVE pin is removed or source state
        # is changed. Other pins and committed protections remain effective.
        source.unpin(record.transferable_chain)
        release = source.safe_release_suffix(
            record.transferable_chain, metadata.source_generation_vector, now)
        metadata.release = release
        record.newly_resident_pages = inserted
        record.duplicate_pages = record.wire_pages-inserted
        record.status = "COMPLETED"
        return record
