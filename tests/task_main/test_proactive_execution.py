from dataclasses import asdict

from src.simulator.task_main.cache import TaskMainCache
from src.simulator.task_main.candidates import CandidateUniverse
from src.simulator.task_main.engine import TaskMainEngine
from src.simulator.task_main.history import ExternalDemandHistory, ReuseHistory
from src.simulator.task_main.metrics import write_outputs
from src.simulator.task_main.policies import Policy
from src.simulator.task_main.proactive import ProactiveController
from src.simulator.task_main.records import OpportunityRecord
from src.simulator.task_main.transfer import IndependentTransfers


def test_capacity_infeasible_lowest_target_uses_feasible_second_target(req, cfg, publish):
    caches = [TaskMainCache(2) for _ in range(3)]
    publish(caches[0], (1, 2))
    publish(caches[1], (9,))
    caches[1].pin((9,))
    universe, demand = CandidateUniverse(), ExternalDemandHistory()
    request = req(path=(1, 2))
    universe.observe(request)
    demand.observe(request)
    network = IndependentTransfers(caches)
    controller = ProactiveController(cfg(), universe, Policy(cfg(), demand, ReuseHistory()), caches, network)
    opportunity = OpportunityRecord(0, 0, 0, "synthetic", (0, 0, 5), (), "DEVELOPMENT")
    before_evictions = [list(cache.eviction_records) for cache in caches]
    result, transfer = controller.execute(opportunity)
    assert result.attempted_candidate_count == 1
    assert [row.status for row in controller.decisions] == [
        "SELECTED", "NOT_ATTEMPTED_CAP_REACHED"]
    assert transfer.transferable_chain == (1, 2) and transfer.target == 2
    assert controller.decisions[0].selected_target_load_rank == 2
    assert not controller.decisions[0].absolute_lowest_load_target_feasible
    assert controller.decisions[0].structural_target_count == 2
    assert controller.decisions[0].feasible_target_count == 1
    assert before_evictions == [cache.eviction_records for cache in caches]
    assert len(network.records) == 1 and caches[1].temporary_pages == 0
    assert caches[2].temporary_pages == 2


def test_success_stops_scanning_demand_load_reuse_opportunity_conserved(req, cfg):
    sim = TaskMainEngine(cfg())
    result = sim.run([req(path=(1, 2, 3))])
    assert len(result.proactive_action_records) == len(result.opportunity_records) == 1
    assert result.opportunity_records[0].attempted_candidate_count == 1
    assert [row.status for row in result.candidate_decision_records] == [
        "SELECTED", "NOT_ATTEMPTED_CAP_REACHED", "NOT_ATTEMPTED_CAP_REACHED"]
    assert len(sim.load.entries) == 1 and sum(result.final_state["load_vector"]) == 1536
    assert sim.demand_history.count((1,), 100, 60000) == 1
    assert dict(sim.reuse_history.events) == {}
    assert sim.caches[1].lookup((1, 2, 3)) == 3
    assert sim.caches[1].page_state(3)["lru_time"] == result.proactive_action_records[0].ready_time
    assert all(cache.reuse_events == [] for cache in sim.caches)


def test_inflight_invisible_full_temporary_source_pin_and_concurrent_copies(req, cfg, monkeypatch):
    sim = TaskMainEngine(cfg(num_pods=3, capacity_pages=4))
    original = sim._arrive
    observations = []
    def inspect(request, now):
        if request.request_id == 1:
            assert sim.caches[0].pinned_pages == 2
            assert sim.caches[0].page_state(1)["lru_time"] == 0
            assert sim.caches[1].temporary_pages == 2
            assert sim.caches[1].lookup((1, 2)) == 0
            observations.append(True)
        original(request, now)
    monkeypatch.setattr(sim, "_arrive", inspect)
    result = sim.run([req(0, path=(1, 2)), req(1, .1, (1, 2)), req(2, .2, (1, 2))])
    assert observations == [True]
    actions = result.proactive_action_records
    assert len(actions) >= 2 and actions[1].start_time < actions[0].ready_time
    assert actions[0].source == actions[1].source == 0
    assert len({a.opportunity_id for a in actions}) == len(actions)
    assert all(cache.pinned_pages == cache.temporary_pages == 0 for cache in sim.caches)


def test_proactive_ready_before_same_timestamp_arrival_does_not_create_opportunity(req, cfg):
    from src.simulator.task_main.config import PAGE_BYTES
    ready = PAGE_BYTES/25e9*1000
    result = TaskMainEngine(cfg(num_pods=2)).run([req(0), req(1, ready)])
    events = [r for r in result.event_records if r.time == ready]
    assert events[0].type == "TRANSFER_COMPLETE" and events[1].type == "REQUEST_ARRIVAL"
    assert result.request_records[1].final_pod == 1
    assert result.request_records[1].final_hit_tokens == 512
    assert len(result.opportunity_records) == 2


def test_oracle_censored_opportunity_not_zero_future_or_missing_opportunity(req, cfg):
    result = TaskMainEngine(cfg("FUTURE_DEMAND")).run([req(0, 3237001)])
    assert len(result.opportunity_records) == 1
    assert result.opportunity_records[0].final_status == "FUTURE_WINDOW_CENSORED"
    assert not result.proactive_action_records


def test_stale_source_preflight_is_read_only(req, publish):
    caches = [TaskMainCache(1), TaskMainCache(1)]
    publish(caches[0], (1,))
    publish(caches[0], (2,))
    before = [cache.snapshot() for cache in caches]
    network = IndependentTransfers(caches)
    assert network.preflight_chain((1,), 0, 1).failure_reason == "REACTIVE_FALLBACK_SOURCE_UNAVAILABLE"
    assert before == [cache.snapshot() for cache in caches] and not network.records


def test_records_and_deterministic_replay_include_auditable_shortlist(req, cfg, tmp_path):
    requests = [req(0, path=(1, 2)), req(1, 10, (1, 2)), req(2, 20, (3,))]
    first = TaskMainEngine(cfg("PERSISTENCE_COST_AWARE")).run(requests)
    second = TaskMainEngine(cfg("PERSISTENCE_COST_AWARE")).run(requests)
    assert first == second
    write_outputs(tmp_path/"one", first)
    write_outputs(tmp_path/"two", second)
    for path in (tmp_path/"one").iterdir():
        assert path.read_bytes() == (tmp_path/"two"/path.name).read_bytes()
    assert (tmp_path/"one/proactive_action_records.jsonl").stat().st_size > 0
    assert all(r.routing_policy == "R_AFF" for r in first.request_records)
