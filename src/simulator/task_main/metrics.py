from __future__ import annotations

from collections import Counter
from dataclasses import asdict
import json
from pathlib import Path
from statistics import median
from typing import Sequence

from .cache import TaskMainCache
from .config import PAGE_BYTES, PAGE_TOKENS, TaskMainConfig
from .isolation import dependency_audit
from .load import CommittedTokenLoad
from .records import EventRecord, OpportunityRecord, RequestRecord, RunResult, TransferRecord


def target_feasibility_summary(rows) -> dict:
    evaluated = [row for row in rows if row.target_feasibility_evaluated]
    ranks = [row.selected_target_load_rank for row in evaluated
             if row.selected_target_load_rank is not None]
    return {
        "evaluated_decision_count": len(evaluated),
        "no_feasible_target_count": sum(row.feasible_target_count == 0 for row in evaluated),
        "selected_rank_1_count": sum(rank == 1 for rank in ranks),
        "selected_rank_2_count": sum(rank == 2 for rank in ranks),
        "selected_rank_ge_3_count": sum(rank >= 3 for rank in ranks),
        "feasibility_changed_target_count": sum(rank > 1 for rank in ranks),
        "absolute_lowest_load_target_infeasible_count": sum(
            row.absolute_lowest_load_target_feasible is False for row in evaluated
        ),
        "structural_target_count_sum": sum(row.structural_target_count or 0 for row in evaluated),
        "feasible_target_count_sum": sum(row.feasible_target_count or 0 for row in evaluated),
    }


def build_summary(config: TaskMainConfig, requests: Sequence[RequestRecord],
                  transfers: Sequence[TransferRecord], opportunities: Sequence[OpportunityRecord],
                  caches: Sequence[TaskMainCache], load: CommittedTokenLoad,
                  expected_request_ids: set[int], events: Sequence[EventRecord]) -> tuple[dict, dict]:
    request_ids = [row.request_id for row in requests]
    opportunity_ids = [row.request_id for row in opportunities]
    transfers_by_id = {row.transfer_id: row for row in transfers}
    checks = {
        "request_completion_conservation": (
            set(request_ids) == expected_request_ids and len(request_ids) == len(expected_request_ids)
        ),
        "one_opportunity_per_request": Counter(opportunity_ids) == Counter(request_ids),
        "one_load_entry_per_request": set(load.entries) == expected_request_ids,
        "one_final_assignment_per_request": Counter(
            row.request_id for row in events if row.type == "FINAL_ASSIGNMENT"
        ) == Counter(request_ids),
        "one_completion_event_per_request": Counter(
            row.request_id for row in events if row.type == "REQUEST_COMPLETE"
        ) == Counter(request_ids),
        "transfer_start_complete_conservation": (
            Counter(row.transfer_id for row in events if row.type == "TRANSFER_START") ==
            Counter(row.transfer_id for row in events if row.type == "TRANSFER_COMPLETE") ==
            Counter(row.transfer_id for row in transfers)
        ),
        "no_zero_page_transfer": all(row.wire_pages > 0 for row in transfers),
        "empty_transferable_stays_source": all(
            row.final_pod == row.affinity_source and row.reactive_transfer_id is None
            for row in requests if row.reactive_gate_passed and row.transferable_pages == 0
        ),
        "hit_miss_token_conservation": all(
            row.final_hit_tokens + row.miss_tokens == row.input_tokens for row in requests
        ),
        "immediate_committed_load": all(
            load.entries[row.request_id].pod_id == row.final_pod
            and load.entries[row.request_id].miss_tokens == row.miss_tokens
            and load.entries[row.request_id].account_time == row.load_account_time == row.arrival_time
            for row in requests
        ),
        "completion_at_arrival_or_ready": all(
            row.completion_time == (
                row.arrival_time if row.reactive_transfer_id is None
                else transfers_by_id[row.reactive_transfer_id].ready_time
            ) for row in requests
        ),
        "all_transfers_complete": all(row.status == "COMPLETED" for row in transfers),
        "full_wire_dedup_conservation": all(
            row.wire_pages == row.temporary_pages == row.newly_resident_pages + row.duplicate_pages
            and row.wire_tokens == row.wire_pages * PAGE_TOKENS
            and row.wire_bytes == row.wire_pages * PAGE_BYTES
            for row in transfers
        ),
        "capacity_never_exceeded": all(
            cache.peak_memory_pages <= config.capacity_pages for cache in caches
        ),
        "temporary_and_pins_drained": all(
            cache.temporary_pages == cache.pinned_pages == 0 for cache in caches
        ),
    }
    by_request = {row.request_id: row for row in requests}
    checks["opportunity_after_completion"] = all(
        row.opportunity_time == by_request[row.request_id].completion_time
        for row in opportunities
    )
    isolation = dependency_audit()
    checks["execution_dependency_isolation"] = isolation["status"] == "PASS"
    if not all(checks.values()):
        raise AssertionError({key: value for key, value in checks.items() if not value})
    provenance = {
        "protocol_version": config.protocol_version,
        "stage": config.protocol_version.rsplit("_", 1)[-1],
        "config": config.as_dict(), "time_unit": "ms",
        "load_definition": "recent_committed_miss_prefill_tokens",
        "load_window": "(t-60000ms,t]", "load_entry_time": "final_assignment_time",
        "reactive_decision_order": "strict_load_gate_then_full_page_prefix_direct_or_copy",
        "reactive_gate_false": "keep_affinity_source_no_wire",
        "reactive_empty_transferable": "keep_affinity_source_no_wire",
        "target_selection": "capacity_feasible_targets_then_load_pod_id",
        "reactive_gate_target": "absolute_lowest_load_other_pod_before_capacity_filter",
        "committed_hit_protection": "source_pin_and_full_wire_buffer_then_ready_batch_target_pin",
        "direct_target_hit": "actual_published_prefix_valid_tokens",
        "network_model": "no_contention_independent_transfer",
        "bandwidth_scope": "per_transfer_not_cluster_shared",
        "page_bytes_assumption": "Qwen_equivalent_simulation_not_Mooncake_measurement",
        "proactive_opportunity": ("record_only_once_after_request_completion_and_prompt_attempt"
                                  if config.proactive_policy == "NONE" else "per_completion_cap_one_copy"),
        "dependency_audit": isolation,
    }
    summary = {
        "provenance": provenance, "metric_scope": f"STAGE_{provenance['stage']}_FULL_REPLAY",
        "workload": config.workload, "request_count": len(requests),
        "input_tokens": sum(row.input_tokens for row in requests),
        "output_tokens_metadata": sum(row.output_tokens for row in requests),
        "final_hit_tokens": sum(row.final_hit_tokens for row in requests),
        "committed_miss_tokens": sum(row.miss_tokens for row in requests),
        "split_counts": dict(sorted(Counter(row.split for row in requests).items())),
        "transfer_count": len(transfers),
        "wire_pages": sum(row.wire_pages for row in transfers),
        "wire_tokens": sum(row.wire_tokens for row in transfers),
        "wire_bytes": sum(row.wire_bytes for row in transfers),
        "reactive_fallbacks": dict(sorted(Counter(
            row.reactive_fallback_reason for row in requests if row.reactive_fallback_reason
        ).items())),
        "cache_admission_skips": sum(row.cache_admission_skip for row in requests),
        "opportunity_count": len(opportunities), "proactive_copy_count": 0,
        "per_pod_cache": [cache.summary() for cache in caches],
        "target_feasibility_diagnostics": {
            "REACTIVE": target_feasibility_summary(requests),
        },
    }
    wire_pages = [row.wire_pages for row in transfers]
    summary["sanity"] = {
        "reactive_gate_pass_count": sum(row.reactive_gate_passed for row in requests),
        "direct_target_count": sum(row.final_pod != row.affinity_source and
                                   row.reactive_transfer_id is None for row in requests),
        "reactive_copy_count": sum(row.type == "REACTIVE" for row in transfers),
        "reactive_fallback_count": sum(row.reactive_fallback_reason is not None for row in requests),
        "requests_with_partial_tail_local_hit": sum(row.route_has_partial_tail for row in requests),
        "transferable_zero_count": sum(row.transferable_pages == 0 for row in requests),
        "gate_true_transferable_zero_count": sum(row.reactive_gate_passed and
                                                  row.transferable_pages == 0 for row in requests),
        "wire_pages_min_median_max": ([min(wire_pages), median(wire_pages), max(wire_pages)]
                                       if wire_pages else [None, None, None]),
    }
    return summary, {"status": "PASS", "checks": checks,
                     "protocol_version": config.protocol_version,
                     "stage_b_policies_implemented": False}


def write_outputs(directory: str | Path, result: RunResult) -> None:
    directory = Path(directory)
    # A distinct, new directory prevents accidental replacement of any old run.
    directory.mkdir(parents=True, exist_ok=False)
    outputs = [
        ("request_records.jsonl", result.request_records),
        ("transfer_records.jsonl", result.transfer_records),
        ("opportunity_records.jsonl", result.opportunity_records),
        ("event_records.jsonl", result.event_records),
    ]
    if result.summary["provenance"]["stage"] in {"B", "C"}:
        outputs.extend([
            ("proactive_action_records.jsonl", result.proactive_action_records),
            ("candidate_decision_records.jsonl", result.candidate_decision_records),
        ])
    for filename, rows in outputs:
        with (directory / filename).open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(asdict(row), sort_keys=True, allow_nan=False) + "\n")
    for filename, value in (("summary.json", result.summary), ("validation.json", result.validation)):
        (directory / filename).write_text(
            json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8",
        )

    if result.summary["provenance"]["stage"] == "C":
        for name, rows in (("copy_observation_records", result.copy_observation_records),
                           ("reuse_observation_records", result.reuse_observation_records)):
            with (directory / (name + ".jsonl")).open("w") as handle:
                for row in rows:
                    handle.write(json.dumps(row, sort_keys=True, allow_nan=False) + "\n")
        (directory / "final_state.json").write_text(json.dumps(result.final_state, sort_keys=True, allow_nan=False) + "\n")


def read_outputs(directory):
    """Load persisted evidence for read-only validation without replaying the simulator."""
    from .records import CandidateDecisionRecord, ProactiveActionRecord
    directory = Path(directory)
    def rows(name, cls=None):
        values = [json.loads(line) for line in (directory/(name+".jsonl")).read_text().splitlines()]
        return [cls(**row) for row in values] if cls else values
    return RunResult(
        request_records=rows("request_records", RequestRecord),
        transfer_records=rows("transfer_records", TransferRecord),
        opportunity_records=rows("opportunity_records", OpportunityRecord),
        event_records=rows("event_records", EventRecord),
        summary=json.loads((directory/"summary.json").read_text()),
        validation=json.loads((directory/"validation.json").read_text()),
        proactive_action_records=rows("proactive_action_records", ProactiveActionRecord),
        candidate_decision_records=rows("candidate_decision_records", CandidateDecisionRecord),
        final_state=json.loads((directory/"final_state.json").read_text()),
        copy_observation_records=rows("copy_observation_records"),
        reuse_observation_records=rows("reuse_observation_records"),
    )
