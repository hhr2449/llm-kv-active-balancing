import pytest

from src.simulator.pod import Pod


def test_queued_estimate_adds_and_is_exactly_removed_at_start() -> None:
    pod = Pod(0)
    pod.enqueue(1, 100.0)
    assert pod.load_ms(0) == 100.0
    work = pod.take_next()
    assert work is not None and work.estimated_service_ms == 100.0
    assert pod.queued_estimated_service_ms == 0.0
    pod.start_running(1, 0, 10.0)
    assert pod.load_ms(0) == 10.0
    assert pod.load_ms(5) == 5.0
    pod.finish(1, 10)
    assert pod.load_ms(10) == 0.0


def test_load_integral_uses_linear_running_remaining_work() -> None:
    pod = Pod(0)
    pod.start_running(1, 0, 10.0)
    pod.integrate_load(0, 10)
    assert pod.load_area_ms2 == pytest.approx(50.0)


def test_pod_caches_are_isolated_and_eviction_is_local() -> None:
    first, second = Pod(0, 1), Pod(1, 2)
    first.cache.publish([1], time_ms=0)
    second.cache.publish([1], time_ms=0)
    second.cache.publish([2], time_ms=1)
    first.cache.publish([3], time_ms=2)
    assert first.cache.lookup([1]) == 0
    assert second.cache.lookup([1]) == 1
    assert first.cache.eviction_count == 1
    assert second.cache.eviction_count == 0
