from src.simulator.pod import Pod
from src.simulator.routing import route_request
from src.simulator.trace import TraceRequest


def req(blocks: list[int]) -> TraceRequest:
    return TraceRequest.from_record(
        0,
        {"timestamp": 0, "input_length": 512 * len(blocks),
         "output_length": 1, "hash_ids": blocks},
    )


def test_aff_cold_equal_load_uses_pod_id_tie_break() -> None:
    pods = [Pod(0), Pod(1)]
    assert route_request("R_AFF", req([1]), pods, 0).pod_id == 0


def test_aff_selects_longest_prefix_hit() -> None:
    pods = [Pod(0), Pod(1)]
    pods[0].cache.publish([1], time_ms=0)
    pods[1].cache.publish([1, 2], time_ms=0)
    decision = route_request("R_AFF", req([1, 2, 3]), pods, 1)
    assert decision.pod_id == 1
    assert decision.h_route_pages == 2


def test_aff_equal_hit_selects_lower_load() -> None:
    pods = [Pod(0), Pod(1)]
    for pod in pods:
        pod.cache.publish([1], time_ms=0)
    pods[0].enqueue(9, 10.0)
    assert route_request("R_AFF", req([1, 2]), pods, 1).pod_id == 1


def test_least_ignores_higher_cache_hit() -> None:
    pods = [Pod(0), Pod(1)]
    pods[0].cache.publish([1, 2], time_ms=0)
    pods[0].enqueue(9, 10.0)
    decision = route_request("R_LEAST", req([1, 2]), pods, 1)
    assert decision.pod_id == 1
    assert decision.h_route_pages == 0
