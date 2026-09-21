from dataclasses import replace

import pytest

from src.simulator.task_main.candidates import Candidate
from src.simulator.task_main.engine import TaskMainEngine
from src.simulator.task_main.equivalence import execution_projection
from src.simulator.task_main.history import ExternalDemandHistory, ReuseHistory, WEIGHTS, bucket
from src.simulator.task_main.policies import Policy, cost_gate, cost_terms


@pytest.mark.parametrize("tokens,index", [(4999, 0), (5000, 1), (19999, 1), (20000, 2),
    (59999, 2), (60000, 3), (119999, 3), (120000, 4), (300000, 4), (300001, 5)])
def test_six_bucket_boundaries(tokens, index):
    assert bucket(tokens) == index


def test_bucket_history_and_forecast_scaling(req):
    history = ExternalDemandHistory()
    for rid, tokens in enumerate((512, 5000, 20000, 60000, 120000, 300001)):
        pages = (tokens+511)//512
        history.observe(req(rid, path=(1,)+tuple(range(1000+rid*1000, 1000+rid*1000+pages-1)), tokens=tokens))
    assert history.bucket_counts((1,), 0, 60000) == (1, 1, 1, 1, 1, 1)
    assert history.benefit((1,), 0, 60000, 300000) == pytest.approx(5*sum(WEIGHTS))


def test_cost_formula_seconds_zero_load_and_congestion_range():
    zero = cost_terms(1.3, 0, (0, 0), 585)
    assert zero["congestion"] == 0
    assert zero["transfer_cost_seconds"] == 585*14*1024*1024/25e9
    for loads in ((1, 1), (100, 0), (1, 100)):
        terms = cost_terms(1.3, 1, loads, 585)
        assert 0 <= terms["congestion"] <= 1
        assert terms["cost_score"] == 1.3-.5*terms["congestion"]-.1*terms["transfer_cost_seconds"]


@pytest.mark.parametrize("score,allowed", [(-1, False), (0, False), (1e-15, True)])
def test_cost_gate_strictly_positive(score, allowed):
    assert cost_gate(score) == allowed


def test_full_585_page_minimum_legal_score_is_strictly_positive():
    minimum = cost_terms(1.3, 0, (1, 1), 585)["cost_score"]
    assert minimum == pytest.approx(0.76564865024) and minimum > 0


def test_weighted_benefit_cannot_reorder_persistence_top10(req, cfg):
    history = ExternalDemandHistory()
    history.observe(req(0, path=(1,)))
    history.observe(req(1, path=(1,)))
    history.observe(req(2, path=tuple(range(2, 2+(300001+511)//512)), tokens=300001))
    for rid in range(3, 15):
        history.observe(req(rid, path=(rid+1000,)))
    candidates = [Candidate((1,), 0, 1), Candidate((2,), 0, 1)] + [Candidate((i+1000,), 0, 1) for i in range(3, 15)]
    policy = Policy(cfg("PERSISTENCE_COST_AWARE"), history, ReuseHistory())
    rows, _, _ = policy.shortlist(candidates, 0)
    assert len(rows) == 10 and [r.candidate.chain_id for r in rows[:2]] == [(1,), (2,)]
    assert policy.diagnostics(rows[0], 0, (0, 0))["benefit"] < policy.diagnostics(rows[1], 0, (0, 0))["benefit"]


def test_line5_line7_execution_equivalent_with_truthful_complete_diagnostics(req, cfg):
    trace = [req(0, path=(1, 2, 3)), req(1, 0, (1, 2)), req(2, 1, (4, 5)),
             req(3, 2, (1, 2, 6)), req(4, 300001, (7, 8)), req(5, 3537001, (7, 8))]
    plain = TaskMainEngine(cfg(num_pods=3, capacity_pages=4)).run(trace)
    cost = TaskMainEngine(cfg("PERSISTENCE_COST_AWARE", num_pods=3, capacity_pages=4)).run(trace)
    assert plain.proactive_action_records and cost.proactive_action_records
    assert execution_projection(plain) == execution_projection(cost)
    assert plain.summary["final_state_digest"] == cost.summary["final_state_digest"]
    assert {a.policy for a in plain.proactive_action_records} == {"PERSISTENCE"}
    assert {a.policy for a in cost.proactive_action_records} == {"PERSISTENCE_COST_AWARE"}
    assert all(a.cost_score is None for a in plain.proactive_action_records)
    assert cost.summary["policy_diagnostics"]["cost_gate_rejected_count"] == 0
    for audit in cost.candidate_decision_records:
        assert audit.cost_score > 0
        assert audit.cost_score == audit.benefit-.5*audit.congestion-.1*audit.transfer_cost_seconds
    # Projection must notice real execution changes, not merely produce an empty signature.
    cost.proactive_action_records[0].target += 1
    assert execution_projection(plain) != execution_projection(cost)
