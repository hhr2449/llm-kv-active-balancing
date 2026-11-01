"""Hard gates: fail closed; validation never repairs or tunes an experiment."""
import math

from .equivalence import execution_projection, logical_state_projection, projection_digest

# These fields describe file/input identity or reporting scope, never policy state.
# Input identity and scope still MUST match for a Line 5/7 comparison.
STATIC_EXCLUDED = {"proactive_policy", "line_id"}


def static_gate_a(line5, line7):
    left, right = line5.as_dict(), line7.as_dict()
    return (line5.proactive_policy == "PERSISTENCE" and line7.proactive_policy == "PERSISTENCE_COST_AWARE"
            and line5.history_window_ms == line7.history_window_ms == 300000
            and {k: v for k, v in left.items() if k not in STATIC_EXCLUDED} ==
                {k: v for k, v in right.items() if k not in STATIC_EXCLUDED})


def future_gate(policy, audit):
    total = audit["decision_future_reads"]
    oracle, history = audit["oracle_decision_future_reads"], audit["history_decision_future_reads"]
    return (total == oracle+history and oracle >= 0 and history == 0 and
            (policy == "FUTURE_DEMAND" or total == 0))


def cost_diagnostics_valid(result):
    return (result.summary["policy_diagnostics"]["policy"] == "PERSISTENCE_COST_AWARE"
            and result.summary["policy_diagnostics"]["cost_gate_rejected_count"] == 0
            and all(r.policy == "PERSISTENCE_COST_AWARE" and
                    all(getattr(r, key) is not None for key in
                        ("benefit", "congestion", "transfer_cost_seconds", "cost_score"))
                    and r.cost_score > 0 and math.isclose(
                        r.cost_score, r.benefit-.5*r.congestion-.1*r.transfer_cost_seconds,
                        rel_tol=1e-14, abs_tol=1e-14)
                    for r in result.candidate_decision_records))


def gate_a_evidence(result):
    return dict(execution_projection_sha256=projection_digest(execution_projection(result)),
                final_logical_state_digest=projection_digest(logical_state_projection(result)),
                system_metrics=result.summary.get("final_metrics"),
                policy=result.summary["policy_diagnostics"]["policy"],
                cost_diagnostics_valid=(cost_diagnostics_valid(result)
                    if result.summary["policy_diagnostics"]["policy"] == "PERSISTENCE_COST_AWARE" else None))


def compare_gate_a(left_config, right_config, left, right):
    checks = dict(static_config=static_gate_a(left_config, right_config),
                  distinct_truthful_policy=(left["policy"] == "PERSISTENCE" and
                                            right["policy"] == "PERSISTENCE_COST_AWARE"),
                  execution_equivalent=left["execution_projection_sha256"] == right["execution_projection_sha256"],
                  logical_state_equivalent=left["final_logical_state_digest"] == right["final_logical_state_digest"],
                  system_metrics_equivalent=left["system_metrics"] == right["system_metrics"],
                  cost_diagnostics=right["cost_diagnostics_valid"] is True)
    return dict(status="PASS" if all(checks.values()) else "INVALID", checks=checks)


def validate_metrics(result, config):
    metrics = result.summary["final_metrics"]
    r, g, w = metrics["requests"], metrics["gini"], metrics["wasted"]
    rows = [x for x in result.request_records if config.evaluation_start_ms <= x.arrival_time < config.evaluation_end_ms]
    network = {(x["phase"], x["type"]): x for x in metrics["network"]}
    fields = ("transfer_started_count", "transfer_completed_count", "wire_pages", "wire_tokens", "wire_bytes")
    checks = {
        "evaluation_request_cohort": r["request_count"] == len(rows),
        "saved_actual_longest_hit": r["saved_prefill_tokens"] == sum(x.final_hit_tokens for x in rows),
        "bucket_request_conservation": sum(b["request_count"] for b in r["buckets"]) == len(rows),
        "bucket_input_conservation": sum(b["input_tokens"] for b in r["buckets"]) == r["total_input_tokens"] == sum(x.input_tokens for x in rows),
        "bucket_saved_conservation": sum(b["saved_tokens"] for b in r["buckets"]) == r["saved_prefill_tokens"],
        "weighted_saved_recomputable": math.fsum(b["saved_tokens"]*b["multiplier"] for b in r["buckets"]) == r["weighted_saved_tokens"],
        "six_buckets_retained": len(r["buckets"]) == 6,
        "gini_fixed_windows": g["total_window_count"] == (config.evaluation_end_ms-config.evaluation_start_ms)//300000,
        "gini_request_conservation": sum(v["request_count"] for v in g["windows"]) == len(rows) and all(
            len(v["counts"]) == config.num_pods and sum(v["counts"]) == v["request_count"] for v in g["windows"]),
        "network_type_conservation": all(network[(phase, "TOTAL")][key] ==
            network[(phase, "REACTIVE")][key]+network[(phase, "PROACTIVE")][key]
            for phase in ("PRE_EVAL", "EVAL", "TAIL", "FULL_RUN") for key in fields),
        "network_phase_conservation": all(network[("FULL_RUN", kind)][key] ==
            sum(network[(phase, kind)][key] for phase in ("PRE_EVAL", "EVAL", "TAIL"))
            for kind in ("REACTIVE", "PROACTIVE", "TOTAL") for key in fields),
        "waste_same_action_cohort": w["proactive_copy_count"] == network[("EVAL", "PROACTIVE")]["transfer_started_count"] and
            w["total_proactive_wire_tokens_for_same_cohort"] == network[("EVAL", "PROACTIVE")]["wire_tokens"],
        "waste_count_conservation": w["proactive_copy_count"] == w["observed_copy_count"]+w["censored_copy_count"] and
            w["observed_copy_count"] == w["used_copy_count"]+w["unused_copy_count"],
        "waste_token_conservation": w["total_proactive_wire_tokens_for_same_cohort"] == w["observed_wire_tokens"]+w["censored_wire_tokens"],
        "waste_censor_boundary": all((a["status"] == "CENSORED") == (a["observation_end"] > config.visibility_end_ms) for a in w["observations"]),
        "gate_b_decision_future_reads": future_gate(config.proactive_policy, result.summary["policy_diagnostics"]["future_access"]),
    }
    by_request = {r.request_id: r for r in result.request_records}
    checks["reuse_observations_match_actual_requests"] = all(
        (request := by_request[u["request_id"]]).final_pod == u["target"] and
        request.completion_time == u["time"] and request.final_hit_pages == len(u["path"]) == len(u["generations"])
        for u in result.reuse_observation_records)
    if config.proactive_policy == "PERSISTENCE_COST_AWARE":
        checks["cost_score_diagnostics"] = cost_diagnostics_valid(result)
    return checks
