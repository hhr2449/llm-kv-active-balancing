"""Explicit execution projection; diagnostic exclusions are a protocol contract."""
from dataclasses import asdict

ACTION_FIELDS = ("chain_id", "chain_depth_pages", "source", "target", "wire_pages",
                 "wire_tokens", "wire_bytes", "start_time", "ready_time", "status")
OPPORTUNITY_FIELDS = ("opportunity_time", "request_id", "structural_candidate_count",
                      "positive_candidate_count", "shortlist_count", "attempted_candidate_count",
                      "final_status", "structural_rejections")
DECISION_FIELDS = ("rank", "chain_id", "source", "target", "policy_score", "status",
                   "target_feasibility_evaluated", "structural_target_count",
                   "feasible_target_count", "selected_target_load_rank",
                   "absolute_lowest_load_target_feasible")


def execution_projection(result):
    actions = {row.opportunity_id: row for row in result.proactive_action_records}
    decisions = {}
    for row in result.candidate_decision_records:
        decisions.setdefault(row.opportunity_id, []).append(
            {key: getattr(row, key) for key in DECISION_FIELDS})
    opportunities = []
    for row in result.opportunity_records:
        value = {key: getattr(row, key) for key in OPPORTUNITY_FIELDS}
        action = actions.get(row.opportunity_id)
        value["action"] = {key: getattr(action, key) for key in ACTION_FIELDS} if action else None
        value["shortlist"] = decisions.get(row.opportunity_id, [])
        opportunities.append(value)
    return {
        "opportunities": opportunities,
        "action_order": [{key: getattr(row, key) for key in ACTION_FIELDS}
                         for row in result.proactive_action_records],
        "requests": [asdict(row) for row in result.request_records],
        "transfers": [asdict(row) for row in result.transfer_records],
        "events": [asdict(row) for row in result.event_records],
        "final_state": result.final_state,
        # Only these two explicitly identified metadata groups are excluded;
        # every current or newly added system result is compared automatically.
        "system_summary": {key: value for key, value in result.summary.items()
                           if key not in {"provenance", "policy_diagnostics"}},
    }


def projection_digest(value):
    import hashlib
    import json
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def logical_state_projection(result):
    """Explicit behavior state schema, excluding report/provenance/score metadata."""
    state = result.final_state if hasattr(result, "final_state") else result
    cache_fields = ("capacity_pages", "pages", "temporary", "version", "access_order", "generation")
    return {
        "cache": [{key: cache[key] for key in cache_fields} for cache in state["cache"]],
        "load_vector": state["load_vector"], "load_entries": state["load_entries"],
        "active_load_histories": state.get("active_load_histories", []),
        "load_last_time": state.get("load_last_time"),
        "candidate_universe": state.get("candidate_universe", []),
        "demand_history": state.get("demand_history", []),
        "demand_request_ids": state.get("demand_request_ids", []),
        "reuse_history": state.get("reuse_history", []),
        "reuse_request_ids": state.get("reuse_request_ids", []),
        "inflight_transfers": [row for row in state["transfers"] if row["status"] == "IN_FLIGHT"],
        "pending_requests": state["pending_requests"], "ready_events": state["ready_events"],
    }
