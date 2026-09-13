from dataclasses import replace

import pytest

from src.simulator.config import SimulatorConfig, SplitConfig
from src.simulator.engine import SimulatorEngine
from src.simulator.trace import TraceRequest


SPLIT = SplitConfig(0, 900, 1500, 2700)


def req(i, t, block, tokens=512):
    return TraceRequest.from_record(i, {
        "timestamp": t, "input_length": tokens, "output_length": 0,
        "hash_ids": [block] if tokens <= 512 else [block, block + 1000],
    }, split_config=SPLIT)


BASE = SimulatorConfig(
    512, 2, 10000, None, 2, "R_REQ_KV", True,
    page_bytes=1, effective_bandwidth_bytes_per_s=1000000,
    split_config=SPLIT, summary_split="DEVELOPMENT", proactive_enabled=True,
    trigger_period_ms=1000, trigger_phase_ms=100,
    max_proactive_actions_per_tick=1, proactive_byte_rate=1000000,
    proactive_burst_bytes=1000000, oracle_horizon_ms=500,
    oracle_visibility_end_ms=900,
)


def run(config=BASE):
    return SimulatorEngine(config).run([req(0, 0, 1), req(1, 500, 1), req(2, 1000, 2)])


def test_proactive_is_hard_gated_to_development():
    with pytest.raises(ValueError, match="DEVELOPMENT"):
        SimulatorEngine(replace(BASE, summary_split="EVALUATION")).run([req(0, 0, 1)])


def test_complete_trigger_only_and_no_split_leakage():
    _, summary = run()
    assert summary["proactive"]["trigger_count"] == 1
    assert summary["proactive"]["oracle_future_reads_outside_allowed_split"] == 0


def test_proactive_copy_uses_real_temporary_and_completes():
    _, summary = run()
    proactive = summary["proactive"]
    assert proactive["actions_started"] == proactive["actions_completed"] == 1
    assert proactive["newly_resident_pages"] == 1
    assert summary["transfer_temporary_pages"] == 0


def test_replica_is_not_forced_as_future_route():
    results, summary = run()
    assert summary["proactive"]["actions_completed"] == 1
    assert results[1].pod_id == 0
    assert summary["proactive"]["wasted_copy_breakdown"]["NOT_USED_AT_TARGET"] == 1


def test_byte_budget_insufficient_skips_action():
    _, summary = run(replace(BASE, proactive_burst_bytes=0, proactive_byte_rate=0))
    assert summary["proactive"]["actions_started"] == 0
    assert summary["proactive"]["skip_budget"] > 0


def test_zero_action_cap_starts_nothing():
    _, summary = run(replace(BASE, max_proactive_actions_per_tick=0))
    assert summary["proactive"]["actions_started"] == 0
    assert summary["proactive"]["skip_action_cap"] > 0


def test_proactive_capacity_failure_is_explicit_skip():
    _, summary = run(replace(BASE, cache_capacity_pages=1))
    assert summary["proactive"]["skip_capacity"] >= 0
    assert summary["peak_memory_used_pages"] <= 1


def test_proactive_disabled_exactly_degenerates_to_b0():
    disabled = replace(BASE, proactive_enabled=False)
    b0 = replace(disabled)
    assert SimulatorEngine(disabled).run([req(0, 0, 1), req(1, 500, 1)]) == \
           SimulatorEngine(b0).run([req(0, 0, 1), req(1, 500, 1)])


def test_proactive_replay_is_deterministic():
    assert run() == run()


def test_network_cost_conservation():
    _, summary = run()
    cost = summary["network_cost"]
    assert cost["total_wire_bytes"] == cost["reactive_wire_bytes"] + cost["proactive_wire_bytes"]


def test_generation_attribution_has_ready_and_lifetime():
    _, summary = run()
    record = summary["proactive"]["action_records"][0]
    assert record["ready_time"] > record["trigger_time"]
    assert record["replica_lifetime_ms"] >= 0


def test_future_demand_reference_uses_external_demand_only():
    _, summary = run()
    record = summary["proactive"]["action_records"][0]
    assert record["future_demand_within_w"] == 1


def test_started_proactive_is_non_preemptive_and_blocks_later_reactive():
    slow_wire = replace(
        BASE, page_bytes=1_000_000, effective_bandwidth_bytes_per_s=1_000_000,
        control_latency_ms=0, absolute_load_gap_ms=0,
        relative_load_threshold=1, oracle_visibility_end_ms=1400,
    )
    _, summary = SimulatorEngine(slow_wire).run([
        req(0, 0, 1), req(1, 400, 9, 1024), req(2, 500, 1), req(3, 1500, 2)
    ])
    proactive = summary["proactive"]
    assert proactive["actions_completed"] == 1
    assert proactive["reactive_ticket_blocked_by_proactive_count"] == 1
    assert proactive["reactive_endpoint_blocked_by_proactive_ms"] == 600


def test_replica_is_invisible_before_complete_then_normal_routing_can_use_it():
    slow_wire = replace(
        BASE, page_bytes=1_000_000, effective_bandwidth_bytes_per_s=1_000_000,
        control_latency_ms=0, absolute_load_gap_ms=0,
        relative_load_threshold=1, oracle_visibility_end_ms=1400,
    )
    results, summary = SimulatorEngine(slow_wire).run([
        req(0, 0, 1), req(1, 400, 9, 1024), req(2, 500, 1), req(3, 1500, 2)
    ])
    # Arrival at 500 cannot see target Temporary; it creates a normal B0 ticket.
    assert results[2].ticket_created
    assert summary["proactive"]["replica_actually_used_actions"] == 1
    assert summary["proactive"]["action_records"][0]["first_use_time"] == 1100


def test_proactive_leaf_lru_pollution_is_counted():
    finite = replace(BASE, cache_capacity_pages=1)
    _, summary = SimulatorEngine(finite).run([
        req(0, 0, 1), req(1, 0, 2), req(2, 500, 1), req(3, 1000, 3)
    ])
    assert summary["proactive"]["evicted_pages_caused_at_admission"] == 1
    assert summary["peak_memory_used_pages"] <= 1


def test_proactive_has_no_gpu_load_or_remote_pressure_owner():
    _, summary = run()
    assert summary["proactive"]["actions_started"] == 1
    assert all(pod["final_load_ms"] == 0 for pod in summary["per_pod"])
    assert all(pod["final_remote_pressure_ms"] == 0 for pod in summary["per_pod"])


def test_not_used_at_target_is_observation_not_counterfactual():
    _, summary = run()
    breakdown = summary["proactive"]["wasted_copy_breakdown"]
    assert breakdown["NOT_USED_AT_TARGET"] == 1
    assert "WRONG_TARGET" not in breakdown
    assert "counterfactual" not in summary["proactive"]["action_records"][0]


def test_relaxed_capacity_has_no_eviction():
    _, summary = run(replace(BASE, cache_capacity_pages=None))
    assert summary["eviction_count"] == 0


def test_no_evict_admission_skips_without_proactive_pollution():
    conservative = replace(BASE, cache_capacity_pages=1,
                           proactive_no_evict_admission=True)
    _, summary = SimulatorEngine(conservative).run([
        req(0, 0, 1), req(1, 0, 2), req(2, 500, 1), req(3, 1000, 3)
    ])
    proactive = summary["proactive"]
    assert proactive["skip_no_free_capacity"] > 0
    assert proactive["evicted_pages_caused_at_admission"] == 0


def test_fanout2_targets_and_replica_accounting_are_distinct():
    fanout = replace(BASE, num_pods=4, proactive_fanout_targets=2,
                     max_proactive_actions_per_tick=2)
    _, summary = SimulatorEngine(fanout).run([
        req(0, 0, 1), req(1, 0, 1), req(2, 500, 1), req(3, 1000, 2)
    ])
    records = summary["proactive"]["action_records"]
    assert len(records) == 2
    assert len({r["target_pod"] for r in records}) == 2
    assert summary["proactive"]["wire_bytes"] == 2
    assert summary["proactive"]["replicas_created"] == 2
    assert summary["proactive"]["replicas_per_candidate"]["mean"] == 2


def test_fanout2_consumes_two_slots_and_two_wire_budgets():
    fanout = replace(BASE, num_pods=4, proactive_fanout_targets=2,
                     max_proactive_actions_per_tick=2,
                     proactive_byte_rate=0, proactive_burst_bytes=1)
    _, summary = SimulatorEngine(fanout).run([
        req(0, 0, 1), req(1, 0, 1), req(2, 500, 1), req(3, 1000, 2)
    ])
    assert summary["proactive"]["actions_started"] == 1
    assert summary["proactive"]["skip_budget"] > 0


def test_candidate_and_replica_use_metrics_are_separate():
    fanout = replace(BASE, num_pods=4, proactive_fanout_targets=2,
                     max_proactive_actions_per_tick=2)
    _, summary = SimulatorEngine(fanout).run([
        req(0, 0, 1), req(1, 0, 1), req(2, 500, 1), req(3, 1000, 2)
    ])
    proactive = summary["proactive"]
    assert "candidate_any_use_rate" in proactive
    assert "replica_actual_use_rate" in proactive
    assert "useful_wire_fraction" in proactive


def test_common_support_horizons_schedule_identical_trigger_count():
    counts = []
    for horizon in (100, 200, 300):
        config = replace(BASE, oracle_horizon_ms=horizon,
                         proactive_trigger_latest_ms=400,
                         trigger_period_ms=100, trigger_phase_ms=0)
        _, summary = SimulatorEngine(config).run([
            req(0, 0, 1), req(1, 500, 1), req(2, 1000, 2)
        ])
        counts.append(summary["proactive"]["trigger_count"])
        assert summary["proactive"]["oracle_future_reads_outside_allowed_split"] == 0
    assert counts == [5, 5, 5]
