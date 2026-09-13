import pytest

from src.simulator.cache import CapacityAdmissionError, PrefixCache
from src.simulator.config import SimulatorConfig
from src.simulator.engine import SimulatorEngine
from src.simulator.pod import Pod
from src.simulator.trace import TraceRequest
from src.simulator.transfer import transferable_prefix_pages


def req(i: int, arrival: float, blocks: list[int], tokens: int | None = None):
    tokens = tokens if tokens is not None else 512 * len(blocks)
    return TraceRequest.from_record(i, {
        "timestamp": arrival, "input_length": tokens,
        "output_length": 1, "hash_ids": blocks,
    })


def p2p_config(capacity=None, mode="FULL_PAGE_ONLY"):
    return SimulatorConfig(512, 2.0, 1000.0, capacity, 2, "R_REQ_KV_SIMPLE",
                           True, 1.5, 1024, 1_024_000.0, 1.0, mode, "FULL_PREFIX")


def test_successful_transfer_redirects_and_increases_h_used() -> None:
    requests = [req(0, 0, [1]), req(1, 600, [1, 3]), req(2, 600, [1, 2])]
    results, summary = SimulatorEngine(p2p_config()).run(requests)
    assisted = results[2]
    assert assisted.p2p_assisted
    assert assisted.source_pod_id == 0 and assisted.target_pod_id == 1
    assert assisted.h_used_pages >= 1
    assert summary["transfer"]["transfer_admitted"] == 1
    assert summary["transfer"]["transfer_completed"] == 1
    assert summary["transfer"]["p2p_assisted_requests"] == 1


def test_endpoint_busy_uses_fallback_and_dual_end_single_flight() -> None:
    requests = [req(0, 0, [1]), req(1, 600, [1, 3]),
                req(2, 600, [1, 2]), req(3, 600, [1, 4])]
    _, summary = SimulatorEngine(p2p_config()).run(requests)
    assert summary["transfer"]["transfer_admitted"] == 1
    assert summary["transfer"]["endpoint_busy_fallback"] >= 1
    assert all(pod["endpoint_utilization"] <= 1 for pod in summary["per_pod"])


def test_transfer_pin_temp_capacity_visibility_and_atomic_commit() -> None:
    source, target = PrefixCache(3), PrefixCache(3)
    source.publish([1, 2], time_ms=0)
    target.publish([1], time_ms=0)
    source.pin([1, 2], 2)
    target.reserve_transfer_temporary(7, 2)
    assert source.is_pinned([1, 2])
    assert target.transfer_temporary_pages == 2
    assert target.memory_used_pages == 3
    assert target.lookup([1, 2]) == 1
    assert target.commit_transfer_temporary(7, [1, 2], time_ms=1) == 1
    assert target.transfer_temporary_pages == 0
    assert target.memory_used_pages == 2
    assert target.lookup([1, 2]) == 2
    source.unpin([1, 2], 2)
    assert not source.is_pinned([1, 2])


def test_full_prefix_duplicate_wire_can_exceed_new_residency() -> None:
    target = PrefixCache(4)
    target.publish([1], time_ms=0)
    target.reserve_transfer_temporary(8, 2)
    inserted = target.commit_transfer_temporary(8, [1, 2], time_ms=1)
    assert inserted == 1
    assert 2 - inserted == 1


def test_transfer_pin_blocks_eviction_and_capacity_failure_is_clean() -> None:
    cache = PrefixCache(1)
    cache.publish([1])
    cache.pin([1], 1)
    with pytest.raises(CapacityAdmissionError):
        cache.reserve_transfer_temporary(3, 1)
    assert cache.lookup([1]) == 1
    assert cache.transfer_temporary_pages == 0
    assert cache.memory_used_pages == 1


def test_target_capacity_failure_uses_unified_affinity_fallback() -> None:
    requests = [
        req(0, 0, [1]), req(1, 0, [9]),
        req(2, 599.5, [9]),
        req(3, 600, [1]), req(4, 600, [1]), req(5, 600, [1]),
    ]
    results, summary = SimulatorEngine(p2p_config(capacity=1)).run(requests)
    assert summary["transfer"]["capacity_fallback"] == 1
    assert not results[5].p2p_assisted
    assert results[5].pod_id == 0
    assert summary["transfer_temporary_pages"] == 0


def test_partial_page_modes_do_not_change_local_cache_reuse() -> None:
    request = req(0, 0, [1, 2], tokens=600)
    assert transferable_prefix_pages(request, 2, "FULL_PAGE_ONLY", 512) == 1
    assert transferable_prefix_pages(request, 2, "TRACE_PAGE", 512) == 2
    cache = PrefixCache()
    cache.publish(request.block_ids)
    assert cache.lookup(request.block_ids) == 2


def test_source_target_zero_hit_and_equivalent_target_do_not_transfer() -> None:
    cold, cold_summary = SimulatorEngine(p2p_config()).run([req(0, 0, [9])])
    assert not cold[0].p2p_assisted
    assert cold_summary["transfer"]["transfer_attempts"] == 0

    same_source, same_summary = SimulatorEngine(p2p_config()).run(
        [req(0, 0, [1]), req(1, 600, [1, 2])]
    )
    assert not same_source[1].p2p_assisted
    assert same_summary["transfer"]["transfer_attempts"] == 0


def test_p2p_disabled_exactly_uses_affinity_path() -> None:
    requests = [req(0, 0, [1]), req(1, 600, [1, 2])]
    disabled = SimulatorConfig(512, 2.0, 1000.0, None, 2,
                               "R_REQ_KV_SIMPLE", False)
    affinity = SimulatorConfig(512, 2.0, 1000.0, None, 2, "R_AFF")
    a, _ = SimulatorEngine(disabled).run(requests)
    b, _ = SimulatorEngine(affinity).run(requests)
    assert a == b
