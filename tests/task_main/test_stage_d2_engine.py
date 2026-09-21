from dataclasses import replace
from pathlib import Path

from src.simulator.task_main.engine import TaskMainEngine
from src.simulator.task_main.equivalence import execution_projection
from src.simulator.task_main.stage_d2.config import StageD2Config
from src.simulator.task_main.stage_d2.engine import StageD2Engine


def config(policy="FUTURE_DEMAND", action="MOVE"):
    return StageD2Config(
        workload="conversation",
        case_id="oracle_move" if policy == "FUTURE_DEMAND" else "persist300_move",
        proactive_policy=policy, action=action,
        trace_path="data/mooncake/conversation_trace.jsonl",
        trace_sha256="b8cbb061a85206d729d91cdc2981f43c9e0d99209dce588d3af5f7934408b9df",
    )


def test_copy_mode_execution_projection_is_exactly_frozen(req):
    cfg = config(action="COPY")
    requests = [req(0, 0, (1,)), req(1, 1, (1, 2)), req(2, 2, (1,))]
    expected = TaskMainEngine(cfg).run(requests, oracle_observation_requests=requests)
    actual = StageD2Engine(cfg).run(requests, oracle_observation_requests=requests)
    assert execution_projection(actual) == execution_projection(expected)
    assert not hasattr(actual, "move_action_records")


def test_move_completion_precedes_same_timestamp_arrival(req):
    cfg = config()
    ready = 14680064/25000000000*1000
    requests = [req(0, 0, (1,)), req(1, ready, (1,))]
    result = StageD2Engine(cfg).run(requests, oracle_observation_requests=requests)
    assert len(result.move_action_records) == 1
    move = result.move_action_records[0]
    second = result.request_records[1]
    assert move.ready_time == ready
    assert move.release_status == "FULL_RELEASE"
    assert second.arrival_time == ready
    assert second.final_pod == move.target
    assert second.final_hit_pages == 1


def test_move_records_units_state_and_validation(req):
    cfg = config(policy="PERSISTENCE")
    requests = [req(0, 0, (1,)), req(1, 1, (1,)), req(2, 2, (1,))]
    result = StageD2Engine(cfg).run(requests)
    assert result.validation["status"] == "PASS"
    assert all(result.validation["checks"].values())
    assert result.summary["provenance"]["action_semantics"] == "COPY_THEN_SAFE_RELEASE"
    assert result.summary["stage_d2_metrics"]["target_side"][
        "unused_move_wire_ratio"] == result.summary["final_metrics"]["wasted"][
            "wasted_copy_ratio"]
    assert result.summary["proactive_move_count"] == len(result.move_action_records)
    assert all(not cache["committed_protections"] for cache in result.final_state["cache"])


def test_move_does_not_create_load_history_or_extra_opportunity(req):
    cfg = config()
    requests = [req(0, 0, (1,)), req(1, 1, (1,))]
    engine = StageD2Engine(cfg)
    result = engine.run(requests, oracle_observation_requests=requests)
    assert len(result.request_records) == len(result.opportunity_records) == len(engine.load.entries) == 2
    assert engine.demand_history.request_ids == engine.reuse_history.request_ids == {0, 1}
    assert all(row.type == "PROACTIVE" for row in result.transfer_records)
