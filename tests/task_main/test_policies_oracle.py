import pytest

from src.simulator.task_main.candidates import Candidate, CandidateUniverse
from src.simulator.task_main.history import ExternalDemandHistory, FutureDemandIndex, ReuseHistory
from src.simulator.task_main.policies import Policy


def test_future_window_left_excluded_right_included_and_request_once(req):
    first = req(0, 10, (1, 2))
    future = FutureDemandIndex([first, first, req(1, 20, (1, 2)), req(2, 20.001)], 100)
    assert future.count((1,), 10, 10) == 1
    assert future.count((1, 2), 0, 20) == 2


def test_oracle_positive_score_depth_id_order_without_k(req, cfg):
    requests = [req(i, 10, (i+1,)) for i in range(12)] + [req(12, 10, (1, 100))]
    future = FutureDemandIndex(requests, 3537000)
    policy = Policy(cfg("FUTURE_DEMAND"), ExternalDemandHistory(), ReuseHistory(), future)
    candidates = [Candidate((i+1,), 0, 1) for i in range(12)] + [Candidate((1, 100), 0, 1), Candidate((99,), 0, 1)]
    rows, positive, _ = policy.shortlist(candidates, 0)
    assert positive == len(rows) == 13  # no Oracle Top10
    assert [r.candidate.chain_id for r in rows[:3]] == [(1,), (1, 100), (2,)]
    assert all(row.oracle_future_count > 0 for row in rows)


def test_future_cannot_register_online_candidates(req):
    universe = CandidateUniverse()
    universe.observe(req(path=(1,)))
    future = FutureDemandIndex([req(1, 100, (9, 10))], 3537000)
    assert future.count((9,), 0, 300000) == 1
    assert universe.seen == {(1,)}
    assert not hasattr(future, "requests") and not hasattr(future, "routing")
    with pytest.raises(AttributeError):
        future.future_load = (1, 2)


def test_future_visibility_boundary_and_zero_score(req, cfg):
    policy = Policy(cfg("FUTURE_DEMAND"), ExternalDemandHistory(), ReuseHistory(),
                    FutureDemandIndex([req()], 3537000))
    candidate = [Candidate((1,), 0, 1)]
    assert policy.shortlist(candidate, 3237000)[2] == "ZERO_SCORE"
    assert policy.shortlist(candidate, 3237000.001) == ([], 0, "FUTURE_WINDOW_CENSORED")
    with pytest.raises(ValueError, match="CENSORED"):
        policy.future.count((1,), 3237001, 300000)


def test_oracle_partial_tail_does_not_become_scored_chain(req):
    future = FutureDemandIndex([req(0, 10, (1, 2), 600)], 3537000)
    assert future.count((1,), 0, 100) == 1
    assert future.count((1, 2), 0, 100) == 0
