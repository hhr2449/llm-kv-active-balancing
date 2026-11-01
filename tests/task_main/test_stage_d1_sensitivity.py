from dataclasses import replace
from pathlib import Path

import pytest

from scripts.task_main_d1.run_stage_d1 import validate_formal_reference
from src.simulator.task_main.cache import TaskMainCache
from src.simulator.task_main.candidates import CandidateUniverse
from src.simulator.task_main.engine import TaskMainEngine
from src.simulator.task_main.records import TransferRecord
from src.simulator.task_main.stage_d1.candidates import StageD1CandidateUniverse
from src.simulator.task_main.stage_d1.config import StageD1Config
from src.simulator.task_main.stage_d1.engine import StageD1Engine
from src.simulator.task_main.stage_d1.isolation import stage_d1_dependency_audit


ROOT = Path(__file__).resolve().parents[2]


def load(workload, case):
    return StageD1Config.from_yaml(
        ROOT/f"configs/task_main/stage_d1/{workload}/{case}.yaml")


def test_fixed_matrix_has_exactly_32_predeclared_cases():
    configs = [StageD1Config.from_yaml(path)
               for path in (ROOT/"configs/task_main/stage_d1").rglob("*.yaml")]
    assert len(configs) == len({(row.workload, row.case_id) for row in configs}) == 32
    assert sum(row.family == "R_LEAST" for row in configs) == 2
    assert sum(row.family == "THETA" for row in configs) == 4
    assert sum(row.family == "PERSISTENCE_K" for row in configs) == 8
    assert sum(row.family == "RECENCY_Q" for row in configs) == 2
    assert sum(row.family == "ORACLE_W" for row in configs) == 6
    assert sum(row.family == "N" for row in configs) == 10


@pytest.mark.parametrize("mutation", [
    {"shortlist_k": 7}, {"recency_quantile": .85}, {"theta": 1.8},
    {"capacity_pages": 1170}, {"action": "MOVE"},
])
def test_fixed_matrix_rejects_unapproved_points(mutation):
    with pytest.raises(ValueError):
        replace(load("conversation", "persist_h60_k5"), **mutation)


def test_oracle_w_uses_one_common_support_split_and_all_three_windows():
    rows = [load("conversation", f"oracle_w{minutes}m") for minutes in (1, 5, 30)]
    assert {(row.evaluation_start_ms, row.evaluation_end_ms) for row in rows} == {(600000, 1500000)}
    assert {row.future_window_ms for row in rows} == {60000, 300000, 1800000}
    assert max(row.evaluation_end_ms+row.future_window_ms for row in rows) == 3300000


def test_n2_keeps_per_pod_capacity_instead_of_total_capacity():
    rows = [load("toolagent", name) for name in
            ("n2_aff", "n2_reqkv", "n2_oracle", "n2_persist300", "n2_recency")]
    assert all(row.num_pods == 2 and row.capacity_pages == 585 for row in rows)


def test_rleast_ignores_cache_affinity_and_uses_load_then_pod_id(req):
    config = load("conversation", "rleast")
    result = StageD1Engine(config).run([req(0, 0, (1,)), req(1, 0, (1,))])
    first, second = result.request_records
    assert first.final_pod == 0
    assert second.final_pod == 1
    assert second.route_hit_tokens == second.final_hit_tokens == 0
    assert result.transfer_records == result.proactive_action_records == []
    assert len(result.opportunity_records) == 2


@pytest.mark.parametrize("case", ["n2_aff", "n2_reqkv", "n2_oracle", "n2_persist300", "n2_recency"])
def test_stage_d1_engine_preserves_existing_routing_and_policy_behavior(case, req):
    config = load("conversation", case)
    requests = [req(0, 0, (1,)), req(1, 3000, (1, 2)), req(2, 6000, (1, 3))]
    observation = requests if config.proactive_policy == "FUTURE_DEMAND" else None
    expected = TaskMainEngine(config).run(requests, oracle_observation_requests=observation)
    actual = StageD1Engine(config).run(requests, oracle_observation_requests=observation)
    assert actual == expected


def test_stage_d1_dependency_and_formal_reference_identity_pass():
    assert stage_d1_dependency_audit()["status"] == "PASS"
    identity = validate_formal_reference(ROOT)
    assert identity["status"] == "PASS"
    assert identity["checks"]["frozen_formal_source_files_unchanged"]


def test_optimized_candidate_universe_matches_frozen_materialization(req):
    frozen, optimized = CandidateUniverse(), StageD1CandidateUniverse()
    requests = [req(0, 0, (1, 2, 3)), req(1, 1, (1, 4)), req(2, 2, (5, 6))]
    for request in requests:
        frozen.observe(request)
        optimized.observe(request)
    caches = [TaskMainCache(12) for _ in range(4)]
    for cache, paths in zip(caches, [((1, 2, 3), (5, 6)), ((1,),), ((1, 4),), ()]):
        for path in paths:
            plan = cache.plan_prompt(path)
            assert plan is not None
            cache.publish_prompt(plan, 0)
    transfers = [TransferRecord(
        0, 0, "PROACTIVE", 0, 3, (1, 2, 3), 3, 1536, 3*14680064,
        1, 2, 3,
    )]
    for loads in ((0, 0, 0, 0), (9, 3, 7, 1)):
        assert optimized.materialize(caches, loads, transfers) == frozen.materialize(
            caches, loads, transfers)
    # Re-observing known Prefixes must preserve cached order and exact output.
    optimized.observe(requests[0])
    assert optimized.materialize(caches, (4, 3, 2, 1), []) == frozen.materialize(
        caches, (4, 3, 2, 1), [])
