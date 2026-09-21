from dataclasses import asdict
import json

from src.simulator.task_main.config import TaskMainConfig
from src.simulator.task_main.engine import TaskMainEngine
from src.simulator.task_main.metrics import write_outputs
from src.simulator.task_main.trace import TraceRequest


def request(rid=0, time=0, path=(1,), tokens=None, output=0):
    return TraceRequest.from_record(rid, {
        "timestamp": time, "input_length": tokens if tokens is not None else len(path) * 512,
        "output_length": output, "hash_ids": list(path),
    })


def test_cold_request_does_not_see_own_load_and_completes_immediately():
    engine = TaskMainEngine(TaskMainConfig())
    result = engine.run([request()])
    row = result.request_records[0]
    assert row.final_pod == row.affinity_source == 0
    assert row.load_vector_before_route == (0, 0, 0, 0)
    assert row.arrival_time == row.load_account_time == row.completion_time == 0
    assert row.miss_tokens == 512 and row.final_hit_tokens == 0
    assert result.opportunity_records[0].load_vector == (512, 0, 0, 0)
    assert len(result.opportunity_records) == 1


def test_same_timestamp_requests_complete_and_publish_in_request_id_order():
    result = TaskMainEngine(TaskMainConfig()).run([request(1), request(0)])
    a, b = result.request_records
    assert a.final_pod == b.final_pod == 0  # affinity wins over a colder low-Load Pod
    assert b.final_hit_tokens == 512 and b.miss_tokens == 0
    assert b.load_vector_before_route == (512, 0, 0, 0)
    assert [r.request_id for r in result.opportunity_records] == [0, 1]
    assert [row.type for row in result.event_records[:5]] == [
        "REQUEST_ARRIVAL", "FINAL_ASSIGNMENT", "REQUEST_COMPLETE", "PUBLISHED", "OPPORTUNITY",
    ]


def test_prompt_admission_skip_keeps_request_successful_and_old_cache_unchanged():
    engine = TaskMainEngine(TaskMainConfig(num_pods=1, capacity_pages=2))
    cache = engine.caches[0]
    cache.publish_prompt(cache.plan_prompt((8,)), 0)
    before = cache.snapshot()["pages"]
    result = engine.run([request(path=(1, 2, 3))])
    row = result.request_records[0]
    assert row.cache_admission_skip and row.cache_admission_status == "CACHE_ADMISSION_SKIP"
    assert row.completion_time == 0 and row.miss_tokens == 1536
    assert cache.eviction_records == [] and cache.snapshot()["pages"] == before
    assert len(result.opportunity_records) == 1


def test_p_only_outputs_do_not_create_completion_kv():
    a = TaskMainEngine(TaskMainConfig(num_pods=1))
    b = TaskMainEngine(TaskMainConfig(num_pods=1))
    out_a = a.run([request(tokens=100, output=0)])
    out_b = b.run([request(tokens=100, output=10000000)])
    assert a.caches[0].snapshot() == b.caches[0].snapshot()
    assert a.caches[0].resident_pages == 1
    assert out_a.opportunity_records == out_b.opportunity_records
    assert out_b.request_records[0].output_tokens == 10000000


def test_state_and_load_cross_evaluation_boundary_without_reset():
    result = TaskMainEngine(TaskMainConfig()).run([
        request(0, 1499999), request(1, 1500000), request(2, 2700000),
    ])
    a, b, c = result.request_records
    assert [r.split for r in (a, b, c)] == ["WARMUP", "EVALUATION", "OBSERVATION_TAIL"]
    assert b.final_hit_tokens == c.final_hit_tokens == 512
    assert b.load_vector_before_route == (512, 0, 0, 0)
    assert c.load_vector_before_route == (0, 0, 0, 0)


def test_deterministic_records_summary_validation_and_output_roundtrip(tmp_path):
    requests = [request(0), request(1), request(2, 2, (2,)), request(3, 60000, (3,))]
    config = TaskMainConfig(num_pods=2, capacity_pages=2)
    first = TaskMainEngine(config).run(requests)
    second = TaskMainEngine(config).run(requests)
    assert first == second
    output = tmp_path / "run"
    write_outputs(output, first)
    expected = json.loads(json.dumps([asdict(row) for row in first.request_records]))
    actual = [json.loads(line) for line in (output / "request_records.jsonl").read_text().splitlines()]
    assert actual == expected
    assert json.loads((output / "validation.json").read_text())["status"] == "PASS"
    assert len((output / "opportunity_records.jsonl").read_text().splitlines()) == len(requests)
    assert json.loads((output / "summary.json").read_text())["provenance"]["time_unit"] == "ms"


def test_empty_input_is_a_valid_zero_request_run():
    result = TaskMainEngine(TaskMainConfig()).run([])
    assert result.request_records == result.transfer_records == result.opportunity_records == []
    assert result.validation["status"] == "PASS"
