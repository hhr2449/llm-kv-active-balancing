import math
from types import SimpleNamespace

import pytest

from src.simulator.task_main.cache import TaskMainCache
from src.simulator.task_main.candidates import CandidateUniverse
from src.simulator.task_main.engine import TaskMainEngine
from src.simulator.task_main.history import ExternalDemandHistory, ReuseHistory
from src.simulator.task_main.policies import Policy, quantile


def test_actual_full_page_hit_ancestors_once_and_partial_excluded(req):
    history = ReuseHistory()
    request = req(path=(1, 2, 3), tokens=1100)
    history.observe(request, 3, 10)
    history.observe(request, 3, 10)
    assert dict(history.events) == {(1,): [10], (1, 2): [10]}
    assert history.score((1,), 60010) == pytest.approx(math.exp(-1))
    assert history.score((1,), 9) == 0


def test_cold_arrival_and_copy_ready_probe_do_not_create_reuse(req, cfg):
    sim = TaskMainEngine(cfg())
    result = sim.run([req()])
    assert len(result.proactive_action_records) == 1
    assert dict(sim.reuse_history.events) == {}
    for cache in sim.caches:
        cache.lookup((1,))
        assert cache.reuse_events == []
    assert sim.demand_history.count((1,), 100, 60000) == 1


def test_recency_cold_has_no_action_later_actual_hit_has_action(req, cfg):
    result = TaskMainEngine(cfg("RECENCY")).run([req(), req(1, 10)])
    assert result.opportunity_records[0].final_status == "ZERO_SCORE"
    assert result.opportunity_records[1].final_status == "STARTED"
    assert result.proactive_action_records[0].recency_score == 1


@pytest.mark.parametrize("values,expected", [([1], 1), ([0, 10], 9), ([1, 2, 3], 2.8),
                                            ([4, 4, 4], 4)])
def test_quantile_linear_interpolation(values, expected):
    assert quantile(values) == pytest.approx(expected)


def test_quantile_filters_inflight_first_but_not_capacity(req, cfg, publish):
    universe, reuse = CandidateUniverse(), ReuseHistory()
    caches = [TaskMainCache(3), TaskMainCache(3)]
    index = 0
    for chain, count in (((1,), 1), ((2,), 2), ((3,), 10)):
        publish(caches[0], chain)
        for _ in range(count):
            request = req(index, path=chain)
            universe.observe(request)
            reuse.observe(request, 1, 0)
            index += 1
    publish(caches[1], (8, 9, 10))
    caches[1].pin((8, 9, 10))  # zero allocatable capacity must not prune structural set
    flight = SimpleNamespace(type="PROACTIVE", status="IN_FLIGHT", target=1,
                             transferable_chain=(3, 30))
    candidates, _ = universe.materialize(caches, (0, 0), [flight])
    rows, positive, _ = Policy(cfg("RECENCY"), ExternalDemandHistory(), reuse).shortlist(candidates, 0)
    assert positive == 2 and [r.candidate.chain_id for r in rows] == [(2,)]
    assert caches[1].plan_temporary(1) is None


def test_recency_score_ties_depth_then_stable_id(req, cfg, publish):
    universe, reuse = CandidateUniverse(), ReuseHistory()
    caches = [TaskMainCache(4), TaskMainCache(4)]
    for i, chain in enumerate(((1, 2), (3, 4))):
        request = req(i, path=chain)
        universe.observe(request)
        reuse.observe(request, 2, 0)
        publish(caches[0], chain)
    candidates, _ = universe.materialize(caches, (0, 0), [])
    rows, positive, _ = Policy(cfg("RECENCY"), ExternalDemandHistory(), reuse).shortlist(candidates, 0)
    assert positive == 4
    assert [row.candidate.chain_id for row in rows] == [(1, 2), (3, 4), (1,), (3,)]
