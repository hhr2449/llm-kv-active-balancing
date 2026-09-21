from __future__ import annotations

from dataclasses import asdict
from statistics import median

from ..config import PAGE_BYTES, PAGE_TOKENS
from ..engine import TaskMainEngine
from ..transfer import IndependentTransfers
from .cache import StageD2Cache
from .candidates import StageD2CandidateUniverse
from .history import StageD2DemandHistory
from .metrics import build_d2_metrics, validate_d2
from .records import MoveActionRecord, MoveSourceReleaseRecord
from .transfer import StageD2Transfers


class StageD2Engine(TaskMainEngine):
    """Frozen TaskMain engine with isolated COPY-then-safe-release MOVE."""

    def __init__(self, config):
        super().__init__(config)
        self._move_committed = {}
        if config.action == "MOVE":
            self.caches = [StageD2Cache(config.capacity_pages)
                           for _ in range(config.num_pods)]
            self.transfers = StageD2Transfers(self.caches, config.action)
            self.universe = StageD2CandidateUniverse()
            self.demand_history = StageD2DemandHistory()

    def _start_reactive_plan(self, request, source, target, hit_pages, loads, now, plan,
                             **diagnostics):
        assignment = super()._start_reactive_plan(
            request, source, target, hit_pages, loads, now, plan, **diagnostics)
        if self.config.action == "MOVE":
            path = request.block_ids[:assignment.final_hit_pages]
            self.caches[target].protect_committed(path)
            self._move_committed[request.request_id] = (target, path)
        return assignment

    def _finish(self, assignment, now, already_protected=False):
        protected = self._move_committed.get(assignment.request.request_id)
        try:
            return super()._finish(assignment, now, already_protected)
        finally:
            if protected is not None:
                pod, path = self._move_committed.pop(assignment.request.request_id)
                self.caches[pod].unprotect_committed(path)

    def run(self, requests, **kwargs):
        result = super().run(requests, **kwargs)
        if self.config.action == "COPY":
            return result
        observations = {row["action_id"]: row["status"]
                        for row in result.summary["final_metrics"]["wasted"]["observations"]}
        move_actions, release_records = [], []
        for action in result.proactive_action_records:
            metadata = self.transfers.move_metadata[action.transfer_id]
            release = metadata.release
            if release is None or metadata.target_commit_failed:
                raise AssertionError("successful MOVE lacks a source-release decision")
            move_actions.append(MoveActionRecord(
                action.action_id, action.opportunity_id, action.request_id, "MOVE",
                action.policy, action.decision_time, action.chain_id,
                action.chain_depth_pages, action.source, action.target,
                action.wire_pages, action.wire_tokens, action.wire_bytes,
                action.transfer_id, action.start_time, action.ready_time, action.split,
                metadata.source_generation_vector, metadata.predicted_releasable_pages,
                release["actual_source_released_pages"],
                release["actual_source_released_pages"]*PAGE_TOKENS,
                release["actual_source_released_pages"]*PAGE_BYTES,
                release["release_fraction"], release["release_status"],
                release["release_stop_reason"], observations.get(action.action_id),
            ))
            release_records.append(MoveSourceReleaseRecord(
                action.transfer_id, action.ready_time, action.source, action.target,
                action.chain_id, metadata.source_generation_vector,
                release["released_blocks_deep_to_root"],
                release["actual_source_released_pages"], release["release_fraction"],
                release["release_status"], release["release_stop_reason"],
            ))
        result.move_action_records = move_actions
        result.move_source_release_records = release_records
        result.summary["provenance"].update(
            stage_d2_study_version=self.config.study_version,
            action_semantics="COPY_THEN_SAFE_RELEASE",
            source_release_accounting="separate_from_ordinary_lru_evictions",
        )
        result.summary["proactive_move_count"] = len(move_actions)
        depths = [row.chain_depth_pages for row in move_actions]
        ordered_depths = sorted(depths)
        p90_position = (len(ordered_depths)-1)*.9 if ordered_depths else 0
        p90_lower = int(p90_position)
        p90_upper = min(p90_lower+1, len(ordered_depths)-1) if ordered_depths else 0
        p90 = (ordered_depths[p90_lower]+(
            ordered_depths[p90_upper]-ordered_depths[p90_lower])*(p90_position-p90_lower)
            if ordered_depths else None)
        result.summary["stage_d2_action_diagnostics"] = {
            "chain_depth_mean": sum(depths)/len(depths) if depths else None,
            "chain_depth_median": median(depths) if depths else None,
            "chain_depth_p90": p90,
        }
        result.summary["stage_d2_metrics"] = build_d2_metrics(result, self.config)
        result.validation["checks"].update(validate_d2(result, self.config))
        result.validation["status"] = (
            "PASS" if all(result.validation["checks"].values()) else "INVALID")
        if result.validation["status"] != "PASS":
            raise AssertionError(result.validation)
        return result
