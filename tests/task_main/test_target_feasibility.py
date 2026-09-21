from copy import deepcopy

from src.simulator.task_main.cache import TaskMainCache
from src.simulator.task_main.candidates import CandidateUniverse
from src.simulator.task_main.config import STAGE_B_VERSION, TaskMainConfig
from src.simulator.task_main.engine import TaskMainEngine
from src.simulator.task_main.history import ExternalDemandHistory, ReuseHistory
from src.simulator.task_main.policies import Policy
from src.simulator.task_main.proactive import ProactiveController
from src.simulator.task_main.records import OpportunityRecord
from src.simulator.task_main.transfer import IndependentTransfers


def publish(cache, path, now=0):
    plan = cache.plan_prompt(path)
    assert plan is not None
    cache.publish_prompt(plan, now)


def pinned_full(cache, path):
    publish(cache, path)
    cache.pin(path)


def proactive(req, caches, loads, path=(1,), inflight=()):
    universe, demand, reuse = CandidateUniverse(), ExternalDemandHistory(), ReuseHistory()
    request = req(path=path)
    universe.observe(request)
    demand.observe(request)
    network = IndependentTransfers(caches)
    network.records.extend(inflight)
    config = TaskMainConfig(protocol_version=STAGE_B_VERSION, proactive_policy="PERSISTENCE",
                            num_pods=len(caches), capacity_pages=caches[0].capacity_pages)
    controller = ProactiveController(config, universe, Policy(config, demand, reuse), caches, network)
    opportunity = OpportunityRecord(0, 0, 0, "synthetic", tuple(loads), (), "DEVELOPMENT")
    result, transfer = controller.execute(opportunity)
    return controller, network, result, transfer, demand, reuse


def test_proactive_lowest_load_feasible_still_selected(req):
    caches = [TaskMainCache(2) for _ in range(3)]
    publish(caches[0], (1,))
    controller, _, _, transfer, _, _ = proactive(req, caches, (100, 0, 10))
    assert transfer.target == 1
    row = controller.decisions[0]
    assert row.selected_target_load_rank == 1
    assert row.absolute_lowest_load_target_feasible
    assert row.structural_target_count == row.feasible_target_count == 2


def test_proactive_lowest_two_infeasible_third_feasible(req):
    caches = [TaskMainCache(1) for _ in range(4)]
    publish(caches[0], (1,))
    pinned_full(caches[1], (8,))
    pinned_full(caches[2], (9,))
    controller, _, _, transfer, _, _ = proactive(req, caches, (100, 0, 1, 2))
    assert transfer.target == 3
    row = controller.decisions[0]
    assert row.selected_target_load_rank == 3 and row.feasible_target_count == 1
    assert not row.absolute_lowest_load_target_feasible


def test_proactive_all_targets_infeasible_skips_and_probes_are_pure(req):
    caches = [TaskMainCache(1) for _ in range(3)]
    publish(caches[0], (1,))
    pinned_full(caches[1], (8,))
    pinned_full(caches[2], (9,))
    snapshots = [deepcopy(cache.snapshot()) for cache in caches]
    evictions = [list(cache.eviction_records) for cache in caches]
    controller, network, result, transfer, demand, reuse = proactive(req, caches, (100, 0, 1))
    assert transfer is None and result.final_status == "SKIP_NO_CAPACITY"
    row = controller.decisions[0]
    assert row.status == "SKIP_NO_CAPACITY" and row.feasible_target_count == 0
    assert row.selected_target_load_rank is None
    assert snapshots == [cache.snapshot() for cache in caches]
    assert evictions == [cache.eviction_records for cache in caches]
    assert not network.records and all(cache.temporary_pages == 0 for cache in caches)
    assert [cache.pinned_pages for cache in caches] == [0, 1, 1]
    assert demand.request_ids == {0} and reuse.request_ids == set()


def test_proactive_only_selected_target_commits_and_tie_uses_pod_id(req):
    caches = [TaskMainCache(2) for _ in range(4)]
    publish(caches[0], (1,))
    before = [deepcopy(cache.snapshot()) for cache in caches]
    controller, network, _, transfer, _, _ = proactive(req, caches, (100, 5, 5, 9))
    assert transfer.target == 1 and controller.decisions[0].selected_target_load_rank == 1
    assert caches[1].temporary_pages == 1
    assert caches[2].snapshot() == before[2] and caches[3].snapshot() == before[3]
    assert len(network.records) == 1


def test_proactive_inflight_covered_target_not_probed(req):
    from types import SimpleNamespace
    caches = [TaskMainCache(2) for _ in range(3)]
    publish(caches[0], (1,))
    flight = SimpleNamespace(type="PROACTIVE", status="IN_FLIGHT", target=1,
                             transferable_chain=(1, 2))
    controller, _, _, transfer, _, _ = proactive(req, caches, (100, 0, 5), inflight=(flight,))
    assert transfer.target == 2
    row = controller.decisions[0]
    assert row.structural_target_count == row.feasible_target_count == 1
    assert row.selected_target_load_rank == 1


def reactive_engine(pods=3, capacity=1):
    return TaskMainEngine(TaskMainConfig(routing_policy="R_REQ_KV_TASK", num_pods=pods,
                                         capacity_pages=capacity))


def test_reactive_gate_false_does_not_probe_capacity(req):
    sim = reactive_engine()
    publish(sim.caches[0], (1,))
    pinned_full(sim.caches[1], (8,))
    before = [deepcopy(cache.snapshot()) for cache in sim.caches]
    assignment = sim._reactive_assignment(req(path=(1,)), 0, 1, (0, 0, 0), 0)
    assert assignment.final_pod == 0 and not assignment.target_feasibility_evaluated
    assert before == [cache.snapshot() for cache in sim.caches]


def test_reactive_lowest_load_feasible_selected_and_only_it_reserves(req):
    sim = reactive_engine()
    publish(sim.caches[0], (1,))
    assignment = sim._reactive_assignment(req(path=(1,)), 0, 1, (100, 0, 10), 0)
    assert assignment.final_pod == 1 and assignment.selected_target_load_rank == 1
    assert assignment.feasible_target_count == 2
    assert sim.caches[1].temporary_pages == 1 and sim.caches[2].temporary_pages == 0


def test_reactive_lowest_infeasible_uses_second_feasible(req):
    sim = reactive_engine()
    publish(sim.caches[0], (1,))
    pinned_full(sim.caches[1], (8,))
    before_lowest = deepcopy(sim.caches[1].snapshot())
    assignment = sim._reactive_assignment(req(path=(1,)), 0, 1, (100, 0, 10), 0)
    assert assignment.final_pod == 2 and assignment.selected_target_load_rank == 2
    assert assignment.feasible_target_count == 1
    assert not assignment.absolute_lowest_load_target_feasible
    assert sim.caches[1].snapshot() == before_lowest
    assert sim.caches[2].temporary_pages == 1


def test_reactive_no_feasible_target_falls_back_source_without_mutation(req):
    sim = reactive_engine()
    publish(sim.caches[0], (1,))
    pinned_full(sim.caches[1], (8,))
    pinned_full(sim.caches[2], (9,))
    before = [deepcopy(cache.snapshot()) for cache in sim.caches]
    assignment = sim._reactive_assignment(req(path=(1,)), 0, 1, (100, 0, 1), 0)
    assert assignment.final_pod == 0
    assert assignment.fallback_reason == "REACTIVE_FALLBACK_CAPACITY"
    assert assignment.feasible_target_count == 0
    assert assignment.selected_target_load_rank is None
    assert before == [cache.snapshot() for cache in sim.caches]
    assert not sim.transfers.records


def test_reactive_existing_prefix_is_zero_capacity_direct_target(req):
    sim = reactive_engine()
    publish(sim.caches[0], (1,))
    pinned_full(sim.caches[1], (8,))  # absolute lowest target cannot COPY
    publish(sim.caches[2], (1,))      # second target can route directly at zero transfer capacity
    assignment = sim._reactive_assignment(req(path=(1,)), 0, 1, (100, 0, 10), 0)
    assert assignment.final_pod == 2 and assignment.transfer_id is None
    assert assignment.selected_target_load_rank == 2 and assignment.feasible_target_count == 1
    assert not sim.transfers.records and sim.caches[2].temporary_pages == 0


def test_reactive_empty_transferable_frozen_stay_source(req):
    sim = reactive_engine(capacity=2)
    request = req(path=(1,), tokens=100)
    publish(sim.caches[0], (1,))
    assignment = sim._reactive_assignment(request, 0, 1, (100, 0, 10), 0)
    assert assignment.final_pod == 0 and assignment.transfer_id is None
    assert not assignment.target_feasibility_evaluated and not sim.transfers.records
