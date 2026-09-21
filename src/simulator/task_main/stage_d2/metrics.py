from __future__ import annotations

import math
from statistics import median

from ..config import PAGE_BYTES, PAGE_TOKENS


def _quantile(values, q):
    if not values:
        return None
    values = sorted(values)
    position = (len(values)-1)*q
    lower = int(position)
    upper = min(lower+1, len(values)-1)
    return values[lower]+(values[upper]-values[lower])*(position-lower)


def _release_summary(rows):
    fractions = [row.release_fraction for row in rows]
    pages = sum(row.actual_source_released_pages for row in rows)
    return {
        "move_actions": len(rows),
        "zero_release_actions": sum(row.release_status == "ZERO_RELEASE" for row in rows),
        "partial_release_actions": sum(row.release_status == "PARTIAL_RELEASE" for row in rows),
        "full_release_actions": sum(row.release_status == "FULL_RELEASE" for row in rows),
        "move_source_release_events": sum(row.actual_source_released_pages > 0 for row in rows),
        "move_source_release_pages": pages,
        "source_released_pages": pages,
        "source_released_tokens": pages*PAGE_TOKENS,
        "source_released_bytes": pages*PAGE_BYTES,
        "release_fraction_mean": math.fsum(fractions)/len(fractions) if fractions else None,
        "release_fraction_median": median(fractions) if fractions else None,
        "release_fraction_p75": _quantile(fractions, .75),
        "release_fraction_p90": _quantile(fractions, .90),
        "release_fraction_p95": _quantile(fractions, .95),
    }


def build_d2_metrics(result, config):
    start, end = config.evaluation_start_ms, config.evaluation_end_ms
    eval_actions = [row for row in result.move_action_records if start <= row.start_time < end]
    full_actions = list(result.move_action_records)
    evictions = [record for cache in result.final_state["cache"]
                 for record in cache.get("evictions", ())]
    requests = result.request_records
    decisions = result.candidate_decision_records
    opportunity_time = {row.opportunity_id: row.opportunity_time
                        for row in result.opportunity_records}
    wasted = result.summary["final_metrics"]["wasted"]
    return {
        "target_side": {
            "unused_transfer_wire_ratio": wasted["wasted_copy_ratio"],
            "unused_move_wire_ratio": wasted["wasted_copy_ratio"],
            "observed_transfer_count": wasted["observed_copy_count"],
            "censored_transfer_count": wasted["censored_copy_count"],
            "observation_coverage": wasted["observation_coverage"],
        },
        "release_evaluation": _release_summary(eval_actions),
        "release_full_run": _release_summary(full_actions),
        "capacity_churn": {
            "ordinary_lru_evictions_evaluation": sum(start <= row[0] < end for row in evictions),
            "ordinary_lru_evictions_full_run": len(evictions),
            "ordinary_cache_turnover_pages_evaluation": sum(start <= row[0] < end for row in evictions),
            "ordinary_cache_turnover_pages_full_run": len(evictions),
            "cache_admission_skips_evaluation": sum(
                row.cache_admission_skip and start <= row.arrival_time < end for row in requests),
            "cache_admission_skips_full_run": sum(row.cache_admission_skip for row in requests),
            "no_capacity_candidate_skips_evaluation": sum(
                row.status == "SKIP_NO_CAPACITY" and
                start <= opportunity_time[row.opportunity_id] < end for row in decisions),
            "no_capacity_candidate_skips_full_run": sum(
                row.status == "SKIP_NO_CAPACITY" for row in decisions),
        },
    }


def validate_d2(result, config):
    rows = result.move_action_records
    transfers = {row.transfer_id: row for row in result.transfer_records}
    checks = {
        "d2_action_is_move": config.action == "MOVE" and all(row.action_type == "MOVE" for row in rows),
        "one_move_record_per_proactive_action": len(rows) == len(result.proactive_action_records),
        "move_transfer_join": all(
            row.transfer_id in transfers and transfers[row.transfer_id].type == "PROACTIVE" and
            transfers[row.transfer_id].status == "COMPLETED" for row in rows),
        "source_generation_vector_complete": all(
            len(row.source_generation_vector) == row.chain_depth_pages for row in rows),
        "release_units_conserved": all(
            0 <= row.actual_source_released_pages <= row.chain_depth_pages and
            row.actual_source_released_tokens == row.actual_source_released_pages*PAGE_TOKENS and
            row.actual_source_released_bytes == row.actual_source_released_pages*PAGE_BYTES and
            math.isclose(row.release_fraction,
                         row.actual_source_released_pages/row.chain_depth_pages) for row in rows),
        "release_status_consistent": all(
            (row.release_status == "ZERO_RELEASE") == (row.actual_source_released_pages == 0) and
            (row.release_status == "FULL_RELEASE") ==
                (row.actual_source_released_pages == row.chain_depth_pages) and
            (row.release_status == "PARTIAL_RELEASE") ==
                (0 < row.actual_source_released_pages < row.chain_depth_pages)
            for row in rows),
        "move_release_records_conserved": len(result.move_source_release_records) == len(rows),
        "committed_protections_drained": all(
            not cache.get("committed_protections") for cache in result.final_state["cache"]),
        "move_no_extra_opportunity": len(result.opportunity_records) == len(result.request_records),
        "move_load_conservation": len(result.final_state["load_entries"]) == len(result.request_records),
    }
    return checks
