import pytest

from src.simulator.config import SimulatorConfig, SplitConfig
from src.simulator.engine import SimulatorEngine
from src.simulator.trace import TraceRequest


SPLIT = SplitConfig()


def request(request_id, arrival, block):
    return TraceRequest.from_record(request_id, {
        "timestamp": arrival, "input_length": 512, "output_length": 1,
        "hash_ids": [block],
    }, split_config=SPLIT)


def config():
    return SimulatorConfig(512, 2.0, 10000.0, None, 1, "FIXED",
                           split_config=SPLIT)


def test_split_boundaries_are_half_open_and_unique():
    points = [(0, "DEVELOPMENT"), (899999, "DEVELOPMENT"),
              (900000, "WARMUP"), (1499999, "WARMUP"),
              (1500000, "EVALUATION"), (2699999, "EVALUATION"),
              (2700000, "OBSERVATION_TAIL")]
    requests = [request(i, timestamp, i + 1) for i, (timestamp, _) in enumerate(points)]
    assert [r.split for r in requests] == [expected for _, expected in points]
    assert len(requests) == sum(sum(r.split == name for name in
        ("DEVELOPMENT", "WARMUP", "EVALUATION", "OBSERVATION_TAIL"))
        for r in requests)


def test_state_crosses_splits_and_summary_is_evaluation_only():
    requests = [request(0, 100, 1), request(1, 900000, 2),
                request(2, 1500000, 2), request(3, 2700000, 3)]
    results, summary = SimulatorEngine(config()).run(requests)
    assert results[2].h_used_pages == 1  # warmup publication remains resident
    assert summary["request_count"] == 1
    assert summary["input_tokens"] == 512
    assert summary["completion_latency_ms"]["mean"] == 2.0
    assert summary["split_counts"] == {
        "DEVELOPMENT": 1, "WARMUP": 1, "EVALUATION": 1,
        "OBSERVATION_TAIL": 1,
    }


def test_evaluation_start_does_not_clear_cache():
    results, _ = SimulatorEngine(config()).run(
        [request(0, 899000, 7), request(1, 1500000, 7)]
    )
    assert results[1].h_route_pages == results[1].h_used_pages == 1


def test_summary_counts_only_evaluation_requests():
    _, summary = SimulatorEngine(config()).run(
        [request(0, 0, 1), request(1, 900000, 2), request(2, 1500000, 3)]
    )
    assert summary["request_count"] == 1
    assert summary["h_used_tokens"] == 0


def test_warmup_changes_evaluation_initial_cache_state():
    results, _ = SimulatorEngine(config()).run(
        [request(0, 1499000, 11), request(1, 1500000, 11)]
    )
    assert results[1].service_time_ms == 2.0


def test_observation_tail_is_excluded_from_evaluation_latency():
    _, summary = SimulatorEngine(config()).run(
        [request(0, 1500000, 1), request(1, 2700000, 2)]
    )
    assert summary["completion_latency_ms"]["mean"] == pytest.approx(53.2)


def test_no_request_is_counted_in_multiple_splits():
    requests = [request(i, t, i + 1) for i, t in enumerate(
        (0, 900000, 1500000, 2700000))]
    assert all(sum(r.split == name for name in
                   ("DEVELOPMENT", "WARMUP", "EVALUATION", "OBSERVATION_TAIL")) == 1
               for r in requests)


def test_split_request_count_is_conserved():
    requests = [request(i, t, i + 1) for i, t in enumerate(
        (0, 1, 900000, 1500000, 2700000, 3000000))]
    _, summary = SimulatorEngine(config()).run(requests)
    assert sum(summary["split_counts"].values()) == len(requests)


def test_split_replay_is_deterministic():
    requests = [request(0, 900000, 1), request(1, 1500000, 1),
                request(2, 2700000, 2)]
    first = SimulatorEngine(config()).run(requests)
    second = SimulatorEngine(config()).run(requests)
    assert first == second
