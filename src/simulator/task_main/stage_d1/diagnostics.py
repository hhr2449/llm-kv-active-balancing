from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
import math
from statistics import median


def _quantile(values, q):
    if not values:
        return None
    values = sorted(values)
    position = (len(values)-1)*q
    lower = int(position)
    return values[lower]+(values[min(lower+1, len(values)-1)]-values[lower])*(position-lower)


def _phase(time, config):
    if time < config.evaluation_start_ms:
        return "PRE_EVAL"
    if time < config.evaluation_end_ms:
        return "EVAL"
    return "TAIL"


def _new_page_use(result, config):
    copies = {row["transfer_id"]: row for row in result.copy_observation_records}
    uses = defaultdict(list)
    for row in result.reuse_observation_records:
        if row["path"]:
            uses[(row["target"], row["path"][0])].append(row)
    for rows in uses.values():
        rows.sort(key=lambda row: (row["time"], row["request_id"]))
    total_new = used_new = 0
    for action in result.proactive_action_records:
        if not config.evaluation_start_ms <= action.start_time < config.evaluation_end_ms:
            continue
        copy = copies[action.transfer_id]
        deadline = action.ready_time+config.future_window_ms
        duplicate = next(row.duplicate_pages for row in result.transfer_records
                         if row.transfer_id == action.transfer_id)
        hit_depth = 0
        for use in uses.get((action.target, action.chain_id[0]), ()):
            if use["time"] <= action.ready_time:
                continue
            if use["time"] > deadline:
                break
            limit = min(len(action.chain_id), len(use["path"]))
            depth = 0
            while (depth < limit and use["path"][depth] == action.chain_id[depth] and
                   use["generations"][depth] == copy["generations"][depth]):
                depth += 1
            hit_depth = max(hit_depth, depth)
        newly = action.wire_pages-duplicate
        total_new += newly
        used_new += max(0, hit_depth-duplicate)
    return dict(newly_published_pages=total_new, new_pages_ever_hit=used_new,
                new_page_observed_use_ratio=(used_new/total_new if total_new else None),
                interpretation="generation-matched observed use; not causal saved tokens")


def build_diagnostics(result, config):
    requests = result.request_records
    actions = result.proactive_action_records
    opportunities = result.opportunity_records
    transfers = result.transfer_records
    eval_actions = [row for row in actions
                    if config.evaluation_start_ms <= row.start_time < config.evaluation_end_ms]
    depths = [row.chain_depth_pages for row in eval_actions]
    phase_gate = defaultdict(int)
    phase_direct = defaultdict(int)
    phase_fallback = defaultdict(int)
    for row in requests:
        phase = _phase(row.arrival_time, config)
        phase_gate[phase] += row.reactive_gate_passed
        phase_direct[phase] += (row.final_pod != row.affinity_source and
                                row.reactive_transfer_id is None)
        phase_fallback[phase] += row.reactive_fallback_reason is not None
    phase_reactive = defaultdict(int)
    for row in transfers:
        if row.type == "REACTIVE":
            phase_reactive[_phase(row.start_time, config)] += 1
    cache_evictions = [event for snapshot in result.final_state["cache"]
                       for event in snapshot.get("evictions", [])]
    eval_evictions = [event for event in cache_evictions
                      if config.evaluation_start_ms <= event[0] < config.evaluation_end_ms]
    eval_opportunities = [row for row in opportunities
                          if config.evaluation_start_ms <= row.opportunity_time < config.evaluation_end_ms]
    diagnostics = {
        "gate_pass_count_pre_eval": phase_gate["PRE_EVAL"],
        "gate_pass_count_evaluation": phase_gate["EVAL"],
        "gate_pass_count_full_run": sum(phase_gate.values()),
        "reactive_copy_started_pre_eval": phase_reactive["PRE_EVAL"],
        "reactive_copy_started_evaluation": phase_reactive["EVAL"],
        "reactive_copy_started_full_run": sum(phase_reactive.values()),
        "direct_target_count_pre_eval": phase_direct["PRE_EVAL"],
        "direct_target_count_evaluation": phase_direct["EVAL"],
        "direct_target_count_full_run": sum(phase_direct.values()),
        "fallback_count_pre_eval": phase_fallback["PRE_EVAL"],
        "fallback_count_evaluation": phase_fallback["EVAL"],
        "fallback_count_full_run": sum(phase_fallback.values()),
        "positive_candidate_count_evaluation": sum(row.positive_candidate_count for row in eval_opportunities),
        "shortlist_count_evaluation": sum(row.shortlist_count for row in eval_opportunities),
        "positive_candidate_count_full_run": sum(row.positive_candidate_count for row in opportunities),
        "shortlist_count_full_run": sum(row.shortlist_count for row in opportunities),
        "proactive_action_count_evaluation": len(eval_actions),
        "proactive_action_count_full_run": len(actions),
        "chain_depth_mean": (math.fsum(depths)/len(depths) if depths else None),
        "chain_depth_median": (median(depths) if depths else None),
        "chain_depth_p90": _quantile(depths, .9),
        "cache_eviction_count_evaluation_all_causes": len(eval_evictions),
        "cache_eviction_count_full_run_all_causes": len(cache_evictions),
        "cache_turnover_pages_evaluation_all_causes": len(eval_evictions),
        "cache_turnover_pages_full_run_all_causes": len(cache_evictions),
        "cache_admission_skip_evaluation": sum(
            row.cache_admission_skip for row in requests
            if config.evaluation_start_ms <= row.arrival_time < config.evaluation_end_ms),
        "cache_admission_skip_full_run": sum(row.cache_admission_skip for row in requests),
        "no_capacity_candidate_skips_evaluation": sum(
            row.status == "SKIP_NO_CAPACITY" and
            config.evaluation_start_ms <= opportunities[row.opportunity_id].opportunity_time < config.evaluation_end_ms
            for row in result.candidate_decision_records),
        "no_capacity_candidate_skips_full_run": sum(
            row.status == "SKIP_NO_CAPACITY" for row in result.candidate_decision_records),
        "proactive_eviction_attribution_support": "UNSUPPORTED_BY_CURRENT_ARTIFACTS",
    }
    diagnostics.update(_new_page_use(result, config) if actions else {
        "newly_published_pages": 0, "new_pages_ever_hit": 0,
        "new_page_observed_use_ratio": None,
        "interpretation": "no proactive action in this case",
    })
    return diagnostics


def validate_d1(result, config):
    diagnostics = result.summary["stage_d1_diagnostics"]
    checks = {
        "stage_d1_fixed_matrix": config.study_version == "TASK_MAIN_STAGE_D1_SENSITIVITY_V1",
        "rleast_has_no_wire_or_action": (config.routing_policy != "R_LEAST" or
            (not result.transfer_records and not result.proactive_action_records)),
        "w_common_support_split": (config.family != "ORACLE_W" or
            (config.evaluation_start_ms, config.evaluation_end_ms) == (600000, 1500000)),
        "w_oracle_decision_window_complete": (config.family != "ORACLE_W" or all(
            row.opportunity_time+config.future_window_ms <= 3300000
            for row in result.opportunity_records
            if config.evaluation_start_ms <= row.opportunity_time < config.evaluation_end_ms)),
        "w_waste_uncensored": (config.family != "ORACLE_W" or
            result.summary["final_metrics"]["wasted"]["censored_copy_count"] == 0),
        "new_page_use_bounded": (diagnostics["new_page_observed_use_ratio"] is None or
            0 <= diagnostics["new_page_observed_use_ratio"] <= 1),
    }
    return checks
