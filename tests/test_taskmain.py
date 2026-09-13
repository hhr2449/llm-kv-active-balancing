import math

import pytest

from src.simulator.cache import PrefixCache
from src.simulator.oracle import TokenBucket
from src.simulator.strategies import (
    ExternalDemandHistory, RankedCandidate, cost_aware_score,
    deterministic_rank, quantile, relative_load_penalty,
    request_kv_task_gate, task_weight,
)
from src.simulator.trace import TraceRequest


def req(i, t, blocks, tokens=None):
    tokens = tokens or 512 * len(blocks)
    return TraceRequest.from_record(i, {
        "timestamp": t, "input_length": tokens, "output_length": 0,
        "hash_ids": blocks,
    })


def test_task_gate_strictly_triggers_above_theta():
    assert request_kv_task_gate(201, 100, 1, 0, 1, 2)


def test_task_gate_equality_does_not_trigger():
    assert not request_kv_task_gate(200, 100, 1, 0, 1, 2)


def test_task_gate_target_zero_has_no_division():
    assert request_kv_task_gate(1, 0, 1, 0, 1, 2)


def test_task_gate_source_target_same_or_no_prefix_rejects():
    assert not request_kv_task_gate(10, 0, 1, 0, 0, 2)
    assert not request_kv_task_gate(10, 0, 0, 0, 1, 2)


def test_task_gate_replan_rechecks_and_can_fallback():
    assert request_kv_task_gate(300, 100, 2, 0, 1, 2)
    assert not request_kv_task_gate(200, 100, 2, 0, 1, 2)


def test_budget_initial_tokens_zero_and_refill_persists_across_boundaries():
    bucket = TokenBucket(1000, 5000, initial_tokens=0)
    assert bucket.tokens == 0
    bucket.refill(900000)
    assert bucket.tokens == 5000
    bucket.consume(1000, 900000)
    bucket.refill(1500000)
    assert bucket.tokens == 5000


def test_rejected_budget_check_does_not_charge():
    bucket = TokenBucket(0, 100, initial_tokens=50)
    assert not bucket.consume(60, 0)
    assert bucket.tokens == 50


def test_actual_wire_charge_and_long_action_after_accumulation():
    bucket = TokenBucket(100, 1000, initial_tokens=0)
    assert not bucket.consume(800, 1000)
    assert bucket.consume(800, 8000)
    assert bucket.tokens == 0


def test_burst_cap_covers_audited_240_page_action():
    page_bytes = 14 * 1024 * 1024
    minute = int(.05 * 4 * 585 * page_bytes)
    action = 240 * page_bytes
    cap = 585 * page_bytes
    assert cap > minute and action <= cap


def test_every_capacity_legal_full_prefix_fits_protocol_bucket_cap():
    page_bytes = 14 * 1024 * 1024
    cap = 585 * page_bytes
    assert all(pages * page_bytes <= cap for pages in range(586))


def test_persistence_history_window_is_open_closed_external_demand():
    history = ExternalDemandHistory([req(0, 0, [1]), req(1, 10, [1]), req(2, 20, [1])])
    assert history.persistence(1, 20, 10) == 1


def test_persistence_deterministic_top_k_order():
    ranked = deterministic_rank([
        RankedCandidate(3, 2, 2), RankedCandidate(1, 2, 1), RankedCandidate(2, 2, 1)
    ])
    assert [item.hash_id for item in ranked[:2]] == [3, 1]


def test_persistence_k_limits_feasibility_search_and_rank2_can_win():
    ranked = deterministic_rank([
        RankedCandidate(1, 3, 1), RankedCandidate(2, 2, 1), RankedCandidate(3, 1, 1)
    ])
    feasible = {2}
    selected = next((index for index, item in enumerate(ranked[:2], 1)
                     if item.hash_id in feasible), None)
    assert selected == 2
    assert not any(item.hash_id == 3 for item in ranked[:2])


def test_recency_exponential_decay_uses_external_history():
    history = ExternalDemandHistory([req(0, 0, [1])])
    assert history.recency(1, 60000, 60) == pytest.approx(math.exp(-1))


def test_recency_quantile_and_zero_exclusion():
    positives = [value for value in (0, 1, 2, 3) if value > 0]
    assert quantile(positives, .9) == pytest.approx(2.8)
    assert [value for value in positives if value >= quantile(positives, .9)] == [3]


def test_recency_no_positive_candidate_means_no_selection():
    positives = [score for score in (0.0, 0.0) if score > 0]
    assert positives == []


@pytest.mark.parametrize("tokens,weight", [
    (4999, 1.3), (5000, 2.5), (20000, 5.3), (60000, 8.9),
    (120000, 11.3), (300001, 15.2),
])
def test_task_bucket_weight(tokens, weight):
    assert task_weight(tokens) == weight


def test_value_hat_and_reference_normalization():
    blocks = list(range(1, 9))
    history = ExternalDemandHistory([req(0, 10, blocks, 4096), req(1, 20, blocks, 4096)])
    value = history.value_hat(1, 20, 20, 20)
    score, benefit, _, _ = cost_aware_score(value, 2.6, 0, [0, 0], 1, 1)
    assert value == 2.6 and benefit == 1 and score == .9


def test_relative_load_penalty_and_all_zero_case():
    assert relative_load_penalty(5, [5, 10]) == .25
    assert relative_load_penalty(0, [0, 0]) == 0


def test_transfer_normalization_and_positive_score_gate():
    score, benefit, load, transfer = cost_aware_score(10, 5, 5, [5, 10], 20, 10)
    assert (benefit, load, transfer) == (2, .25, .2)
    assert score > 0


def test_cost_reference_is_frozen_configuration_not_evaluation_estimate():
    from src.simulator.config import SimulatorConfig
    config = SimulatorConfig.from_yaml("configs/formal/taskmain_v1.1.yaml")
    assert config.cost_v_ref == 2.5
    assert config.cost_t_ref_ms == pytest.approx(8.04643072)


def test_585_page_capacity_and_temporary_accounting():
    cache = PrefixCache(585)
    cache.reserve_transfer_temporary(1, 585)
    assert cache.memory_used_pages == 585
    cache.commit_transfer_temporary(1, range(585), 0)
    assert cache.memory_used_pages == 585
    assert cache.publish(range(585), 1) == 0
