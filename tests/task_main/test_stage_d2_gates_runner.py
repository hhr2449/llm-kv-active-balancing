from dataclasses import asdict
import json
from pathlib import Path

from scripts.task_main_d2.run_stage_d2 import OUTPUT_FILES, output_hashes, write_d2_outputs
from src.simulator.task_main.cache import TaskMainCache
from src.simulator.task_main.candidates import CandidateUniverse
from src.simulator.task_main.history import ExternalDemandHistory
from src.simulator.task_main.stage_d2.candidates import StageD2CandidateUniverse
from src.simulator.task_main.stage_d2.cache import StageD2Cache
from src.simulator.task_main.stage_d2.config import StageD2Config
from src.simulator.task_main.stage_d2.engine import StageD2Engine
from src.simulator.task_main.stage_d2.history import StageD2DemandHistory
from src.simulator.task_main.stage_d2.isolation import stage_d2_dependency_audit
from src.simulator.task_main.stage_d2.reference import validate_copy_references
from src.simulator.task_main.stage_d2.reporting import _raw_copy_churn
from src.simulator.task_main.validation import gate_a_evidence


ROOT = Path(__file__).resolve().parents[2]
TRACE_SHA = "b8cbb061a85206d729d91cdc2981f43c9e0d99209dce588d3af5f7934408b9df"


def config(policy="FUTURE_DEMAND"):
    return StageD2Config(
        workload="conversation",
        case_id="oracle_move" if policy == "FUTURE_DEMAND" else "persist300_move",
        proactive_policy=policy,
        trace_path="data/mooncake/conversation_trace.jsonl",
        trace_sha256=TRACE_SHA,
    )


def put(cache, path):
    plan = cache.plan_prompt(path)
    assert plan is not None
    cache.publish_prompt(plan, 0)


def test_optimized_candidate_universe_matches_frozen_base(req):
    requests = [req(0, 0, (1, 2, 3)), req(1, 1, (1, 2, 4)), req(2, 2, (5,))]
    base, optimized = CandidateUniverse(), StageD2CandidateUniverse()
    for request in requests:
        base.observe(request)
        optimized.observe(request)
    caches = [TaskMainCache(20) for _ in range(4)]
    put(caches[0], (1, 2, 3))
    put(caches[1], (1, 2, 4))
    put(caches[2], (5,))
    loads = (9, 2, 3, 1)
    expected = base.materialize(caches, loads, ())
    actual = optimized.materialize(caches, loads, ())
    assert actual == expected


def test_temporary_preflight_cache_matches_frozen_leaf_lru_for_every_wire_size():
    base, optimized = TaskMainCache(8), StageD2Cache(8)
    for cache in (base, optimized):
        put(cache, (1, 2, 3))
        put(cache, (1, 2, 4))
        put(cache, (5, 6))
        cache.pin((1, 2, 4))
    for wire_pages in range(1, 12):
        assert optimized.plan_temporary(wire_pages) == base.plan_temporary(wire_pages)
        # Repeated calls exercise the cached sequence without changing state.
        assert optimized.plan_temporary(wire_pages) == base.plan_temporary(wire_pages)
    for cache in (base, optimized):
        cache.unpin((1, 2, 4))
    for wire_pages in range(1, 12):
        assert optimized.plan_temporary(wire_pages) == base.plan_temporary(wire_pages)


def test_sliding_persistence_counts_match_frozen_bisect_semantics(req):
    base, optimized = ExternalDemandHistory(), StageD2DemandHistory()
    requests = [
        req(0, 0, (1, 2)), req(1, 1, (1, 3)), req(2, 60000, (1, 2)),
        req(3, 60001, (4,)), req(4, 120000, (1, 2, 5)),
    ]
    seen = {(1,), (1, 2), (1, 3), (1, 2, 5), (4,)}
    for request in requests:
        base.observe(request)
        optimized.observe(request)
        for chain in seen:
            assert optimized.count(chain, request.arrival_ms, 60000) == base.count(
                chain, request.arrival_ms, 60000)
    # A decreasing-time/direct-API query uses the exact fallback.
    for chain in seen:
        assert optimized.count(chain, 60000, 30000) == base.count(chain, 60000, 30000)


def test_copy_reference_gate_is_readable():
    configs = [StageD2Config.from_yaml(path) for path in
               sorted((ROOT/"configs/task_main/stage_d2").rglob("*.yaml"))]
    evidence = validate_copy_references(ROOT, configs)
    assert evidence["status"] == "PASS"
    assert evidence["formal_manifest_sha256"] == (
        "7499b1e08fe0f4b7a8bbd72c83b120c16ebfc6a647d797095ca469c056a4debf")
    assert evidence["formal_source_provenance_sha256"] == (
        "25ddfae1be89148d8f30a39b03fd0b4764b0703041d4a7be4e00d5b69f4eb318")
    assert len(evidence["references"]) == 4


def test_raw_copy_churn_recomputes_formal_fields_from_primary_ledgers(tmp_path):
    final_state = {"cache": [
        {"evictions": [[9, 1, 1], [15, 2, 1]]},
        {"evictions": [[20, 3, 1], [31, 4, 1]]},
    ]}
    (tmp_path/"final_state.json").write_text(json.dumps(final_state))
    requests = [
        {"arrival_time": 12, "cache_admission_skip": True},
        {"arrival_time": 35, "cache_admission_skip": True},
        {"arrival_time": 18, "cache_admission_skip": False},
    ]
    opportunities = [
        {"opportunity_id": 1, "opportunity_time": 14},
        {"opportunity_id": 2, "opportunity_time": 40},
    ]
    decisions = [
        {"opportunity_id": 1, "status": "SKIP_NO_CAPACITY"},
        {"opportunity_id": 1, "status": "SKIP_NO_CAPACITY"},
        {"opportunity_id": 2, "status": "SKIP_NO_CAPACITY"},
        {"opportunity_id": 2, "status": "SELECTED"},
    ]
    for name, rows in (("request_records", requests),
                       ("opportunity_records", opportunities),
                       ("candidate_decision_records", decisions)):
        (tmp_path/f"{name}.jsonl").write_text(
            "".join(json.dumps(row, sort_keys=True)+"\n" for row in rows))
    assert _raw_copy_churn(tmp_path, 10, 30) == {
        "ordinary_lru_evictions_evaluation": 2,
        "ordinary_lru_evictions_full_run": 4,
        "ordinary_cache_turnover_pages_evaluation": 2,
        "ordinary_cache_turnover_pages_full_run": 4,
        "cache_admission_skips_evaluation": 1,
        "cache_admission_skips_full_run": 2,
        "no_capacity_candidate_skips_evaluation": 2,
        "no_capacity_candidate_skips_full_run": 3,
    }


def test_stage_d2_dependency_audit_passes():
    audit = stage_d2_dependency_audit()
    assert audit["status"] == "PASS"
    assert audit["violations"] == []


def test_d2_output_writer_persists_complete_replay_artifact(req, tmp_path):
    requests = [req(0, 0, (1,)), req(1, 1, (1,)), req(2, 2, (1,))]
    result = StageD2Engine(config("PERSISTENCE")).run(requests)
    target = tmp_path/"run_1"
    write_d2_outputs(target, result)
    (target/"execution_evidence.json").write_text(
        json.dumps(gate_a_evidence(result), sort_keys=True)+"\n")
    assert set(output_hashes(target)) == OUTPUT_FILES
    actions = [json.loads(line) for line in
               (target/"move_action_records.jsonl").read_text().splitlines()]
    releases = [json.loads(line) for line in
                (target/"move_source_release_records.jsonl").read_text().splitlines()]
    assert len(actions) == len(releases) > 0
    assert all(row["action_type"] == "MOVE" for row in actions)


def test_move_double_replay_projection_is_deterministic(req):
    requests = [req(0, 0, (1,)), req(1, 1, (1,)), req(2, 2, (1,))]
    first = StageD2Engine(config("PERSISTENCE")).run(requests)
    second = StageD2Engine(config("PERSISTENCE")).run(requests)
    projection = lambda result: {
        "requests": [asdict(row) for row in result.request_records],
        "transfers": [asdict(row) for row in result.transfer_records],
        "actions": [asdict(row) for row in result.move_action_records],
        "releases": [asdict(row) for row in result.move_source_release_records],
        "final_state": result.final_state,
        "metrics": result.summary["stage_d2_metrics"],
    }
    assert projection(first) == projection(second)
