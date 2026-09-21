from src.simulator.task_main.cache import TaskMainCache
from src.simulator.task_main.routing import affinity_route, least_other_pod, reactive_gate
from src.simulator.task_main.trace import TraceRequest


def request():
    return TraceRequest.from_record(0, {
        "timestamp": 0, "input_length": 1024, "output_length": 0, "hash_ids": [1, 2],
    })


def publish(cache, path):
    cache.publish_prompt(cache.plan_prompt(path), 0)


def test_cold_affinity_uses_lowest_pod_id():
    caches = [TaskMainCache(3) for _ in range(4)]
    assert affinity_route(request(), caches, [0] * 4).pod_id == 0


def test_longest_affinity_prefix_precedes_lower_load():
    caches = [TaskMainCache(3) for _ in range(3)]
    publish(caches[0], (1,))
    publish(caches[1], (1, 2))
    assert affinity_route(request(), caches, [0, 99999, 0]).pod_id == 1


def test_affinity_ties_use_load_then_pod_id_without_probe_mutation():
    caches = [TaskMainCache(3) for _ in range(3)]
    for cache in caches:
        publish(cache, (1,))
    before = [cache.snapshot() for cache in caches]
    assert affinity_route(request(), caches, [5, 2, 2]).pod_id == 1
    assert [cache.snapshot() for cache in caches] == before


def test_target_excludes_source_and_breaks_load_ties_by_id():
    assert least_other_pod(0, [0, 1, 1]) == 1
    assert least_other_pod(0, [0]) is None


def test_reactive_gate_is_strict_multiplication_and_handles_zero():
    assert reactive_gate(201, 100)
    assert not reactive_gate(200, 100)
    assert reactive_gate(1, 0)
    assert not reactive_gate(0, 0)
