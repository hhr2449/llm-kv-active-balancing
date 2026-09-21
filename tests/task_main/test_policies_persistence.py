from src.simulator.task_main.candidates import Candidate
from src.simulator.task_main.engine import TaskMainEngine
from src.simulator.task_main.config import TaskMainConfig
from src.simulator.task_main.history import ExternalDemandHistory, ReuseHistory
from src.simulator.task_main.policies import Policy


def test_arrival_registers_every_ancestor_once_and_exact_window(req):
    history = ExternalDemandHistory()
    request = req(0, 10, (1, 2, 3))
    history.observe(request)
    history.observe(request)
    assert [history.count(c, 10, 60000) for c in ((1,), (1, 2), (1, 2, 3))] == [1, 1, 1]
    assert history.count((1,), 60009.999, 60000) == 1
    assert history.count((1,), 60010, 60000) == 0
    assert history.count((1,), 9, 60000) == 0


def test_persistence_count_depth_id_positive_top10(req, cfg):
    history = ExternalDemandHistory()
    for i in range(12):
        history.observe(req(i, 0, (i+1,)))
    history.observe(req(12, 0, (1, 100)))
    candidates = [Candidate((i+1,), 0, 1) for i in range(13)] + [Candidate((1, 100), 0, 1)]
    rows, positive, status = Policy(cfg(), history, ReuseHistory()).shortlist(candidates, 0)
    assert positive == 13 and len(rows) == 10 and status is None
    assert [row.candidate.chain_id for row in rows[:3]] == [(1,), (1, 100), (2,)]
    assert all(row.persistence_count > 0 for row in rows)


def test_arrived_reactive_waiter_is_demand_before_reuse(req, monkeypatch):
    sim = TaskMainEngine(TaskMainConfig(routing_policy="R_REQ_KV_TASK", num_pods=2))
    original = sim._arrive
    observations = []
    def inspect(request, now):
        if request.request_id == 2:
            assert 1 in sim._pending
            assert sim.demand_history.count((1, 2), now, 60000) == 1
            assert sim.reuse_history.score((1,), now) == 0
            observations.append(True)
        return original(request, now)
    monkeypatch.setattr(sim, "_arrive", inspect)
    sim.run([req(0), req(1, 1, (1, 2)), req(2, 1.1, (3,))])
    assert observations == [True]
    assert sim.reuse_history.events[(1,)] == [sim.transfers.records[0].ready_time]


def test_top10_shortlist_is_not_ten_actions(req, cfg):
    result = TaskMainEngine(cfg()).run([req(path=tuple(range(1, 13)))])
    assert result.opportunity_records[0].shortlist_count == 10
    assert len(result.proactive_action_records) == 1
    assert result.proactive_action_records[0].chain_depth_pages == 12


def test_history_only_continues_after_future_visibility(req, cfg):
    result = TaskMainEngine(cfg()).run([req(0, 3537001)])
    assert result.opportunity_records[0].final_status == "STARTED"
