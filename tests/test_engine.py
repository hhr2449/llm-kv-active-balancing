import pytest

from src.simulator.cache import CapacityAdmissionError
from src.simulator.config import SimulatorConfig
from src.simulator.engine import SimulatorEngine
from src.simulator.trace import TraceRequest


CONFIG = SimulatorConfig(page_tokens=512, base_latency_ms=2.0, prefill_tokens_per_second=1000.0)


def req(request_id: int, arrival: float, tokens: int, blocks: list[int]) -> TraceRequest:
    return TraceRequest.from_record(
        request_id,
        {"timestamp": arrival, "input_length": tokens, "output_length": 1, "hash_ids": blocks},
    )


def run(*requests: TraceRequest):
    return SimulatorEngine(CONFIG).run(list(requests))


def test_cold_request() -> None:
    results, summary = run(req(0, 0, 600, [1, 2]))
    result = results[0]
    assert result.h_route_pages == result.h_used_pages == 0
    assert result.miss_tokens == 600
    assert result.service_time_ms == pytest.approx(602.0)
    assert result.cache_inserted_pages == 2
    assert summary["cache_resident_pages"] == 2


def test_full_reuse() -> None:
    results, _ = run(req(0, 0, 600, [1, 2]), req(1, 1000, 600, [1, 2]))
    reused = results[1]
    assert reused.h_route_pages == reused.h_used_pages == 2
    assert reused.h_used_tokens == 600
    assert reused.miss_tokens == 0
    assert reused.service_time_ms == CONFIG.base_latency_ms


def test_partial_prefix_reuse() -> None:
    results, _ = run(req(0, 0, 1024, [1, 2]), req(1, 2000, 1024, [1, 3]))
    reused = results[1]
    assert reused.h_used_pages == 1
    assert reused.h_used_tokens == 512
    assert reused.miss_tokens == 512


def test_in_flight_kv_is_invisible() -> None:
    results, _ = run(req(0, 0, 1024, [1, 2]), req(1, 100, 1024, [1, 2]))
    second = results[1]
    assert second.h_route_pages == 0
    assert second.h_used_pages == 2
    assert second.queue_time_ms > 0


def test_same_timestamp_batch_routes_cold_then_relooks_up_at_service_start() -> None:
    results, _ = run(req(0, 0, 512, [1]), req(1, 0, 512, [1]))
    first, second = results
    assert first.h_route_pages == 0
    assert second.h_route_pages == 0
    assert second.h_used_pages == 1
    assert second.miss_tokens == 0
    assert second.service_start_ms == pytest.approx(first.service_done_ms)


def test_deterministic_replay() -> None:
    requests = [req(0, 0, 512, [1]), req(1, 0, 1024, [1, 2]), req(2, 1, 512, [3])]
    first, first_summary = SimulatorEngine(CONFIG).run(requests)
    second, second_summary = SimulatorEngine(CONFIG).run(requests)
    assert first == second
    assert first_summary == second_summary


def test_finite_capacity_engine_never_overflows() -> None:
    config = SimulatorConfig(512, 2.0, 1000.0, cache_capacity_pages=2)
    requests = [req(0, 0, 1024, [1, 2]), req(1, 2000, 1024, [3, 4])]
    _, summary = SimulatorEngine(config).run(requests)
    assert summary["peak_memory_used_pages"] <= 2
    assert summary["memory_used_pages"] <= 2
    assert summary["peak_active_private_pages"] == 2
    assert summary["active_private_pages"] == 0
    assert summary["evicted_pages"] == 2
    assert summary["capacity_admission_failures"] == 0


def test_engine_impossible_admission_is_explicit() -> None:
    config = SimulatorConfig(512, 2.0, 1000.0, cache_capacity_pages=1)
    with pytest.raises(CapacityAdmissionError) as error:
        SimulatorEngine(config).run([req(0, 0, 1024, [1, 2])])
    assert error.value.diagnostic["requested_active_private_pages"] == 2


def test_infinite_capacity_retains_all_pages() -> None:
    requests = [req(0, 0, 1024, [1, 2]), req(1, 2000, 1024, [3, 4])]
    _, summary = SimulatorEngine(CONFIG).run(requests)
    assert summary["cache_capacity_pages"] is None
    assert summary["cache_resident_pages"] == 4
    assert summary["eviction_count"] == 0


def test_same_timestamp_routing_sees_prior_load_reservation() -> None:
    config = SimulatorConfig(512, 2.0, 1000.0, None, 2, "R_LEAST")
    results, summary = SimulatorEngine(config).run(
        [req(0, 0, 512, [1]), req(1, 0, 512, [2])]
    )
    assert [result.pod_id for result in results] == [0, 1]
    assert [pod["routed_requests"] for pod in summary["per_pod"]] == [1, 1]


def test_same_timestamp_does_not_see_unfinished_kv() -> None:
    config = SimulatorConfig(512, 2.0, 1000.0, None, 2, "R_AFF")
    results, _ = SimulatorEngine(config).run(
        [req(0, 0, 512, [1]), req(1, 0, 512, [1])]
    )
    assert [result.h_route_pages for result in results] == [0, 0]
    assert [result.pod_id for result in results] == [0, 1]


def test_actual_h_used_replaces_cold_queue_estimate_without_ghost_load() -> None:
    config = SimulatorConfig(512, 2.0, 1000.0, None, 1, "FIXED")
    results, summary = SimulatorEngine(config).run(
        [req(0, 0, 512, [1]), req(1, 0, 512, [1])]
    )
    assert results[1].h_route_pages == 0
    assert results[1].h_used_pages == 1
    assert results[1].service_time_ms == 2.0
    assert summary["per_pod"][0]["final_load_ms"] == 0.0
    assert summary["per_pod"][0]["routed_requests"] == 2


def test_request_contributes_to_exactly_one_pod_and_replay_is_deterministic() -> None:
    config = SimulatorConfig(512, 2.0, 1000.0, 4, 2, "R_AFF")
    requests = [req(0, 0, 512, [1]), req(1, 0, 1024, [1, 2]), req(2, 1, 512, [3])]
    first, first_summary = SimulatorEngine(config).run(requests)
    second, second_summary = SimulatorEngine(config).run(requests)
    assert first == second
    assert first_summary == second_summary
    assert sum(pod["routed_requests"] for pod in first_summary["per_pod"]) == len(requests)
    assert all(pod["final_load_ms"] == 0.0 for pod in first_summary["per_pod"])


def test_single_pod_fixed_routing_matches_single_pod_aff() -> None:
    requests = [req(0, 0, 512, [1]), req(1, 0, 1024, [1, 2])]
    fixed = SimulatorConfig(512, 2.0, 1000.0, None, 1, "FIXED")
    affinity = SimulatorConfig(512, 2.0, 1000.0, None, 1, "R_AFF")
    fixed_results, _ = SimulatorEngine(fixed).run(requests)
    affinity_results, _ = SimulatorEngine(affinity).run(requests)
    assert fixed_results == affinity_results
