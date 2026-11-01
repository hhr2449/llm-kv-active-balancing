from dataclasses import replace

import pytest

from src.simulator.task_main.cache import TaskMainCache
from src.simulator.task_main.config import PAGE_BYTES, TaskMainConfig
from src.simulator.task_main.load import CommittedTokenLoad
from src.simulator.task_main.trace import TraceRequest, full_page_prefix, validate_requests
from src.simulator.task_main.transfer import IndependentTransfers


def request(rid=0, time=0, path=(1, 2), tokens=None, output=0):
    return TraceRequest.from_record(rid, {
        "timestamp": time, "input_length": tokens if tokens is not None else len(path) * 512,
        "output_length": output, "hash_ids": list(path),
    })


def publish(cache, path, now=0):
    plan = cache.plan_prompt(path)
    assert plan is not None
    return cache.publish_prompt(plan, now)


def test_load_accounts_at_assignment_and_expires_at_exact_left_boundary():
    load = CommittedTokenLoad(2)
    assert load.vector(10) == (0, 0)
    load.commit(0, 1, 100, 10)
    assert load.vector(10) == (0, 100)
    load.commit(1, 1, 20, 11)
    assert load.vector(60009.999) == (0, 120)
    assert load.vector(60010) == (0, 20)
    assert load.vector(60011) == (0, 0)
    with pytest.raises(ValueError, match="already"):
        load.commit(0, 0, 100, 60012)


def test_full_page_conversion_preserves_local_partial_tail():
    req = request(tokens=600)
    assert full_page_prefix(req, 2) == (1,)
    cache = TaskMainCache(2)
    publish(cache, req.block_ids)
    assert req.hit_tokens(cache.lookup(req.block_ids)) == 600
    assert full_page_prefix(request(path=(3,), tokens=10), 1) == ()


def test_copy_preflight_failure_has_no_eviction_temporary_or_wire():
    source, target = TaskMainCache(3), TaskMainCache(3)
    publish(source, (1, 2, 3))
    publish(target, (7, 8))
    target.pin((7,))  # Leaf 8 is evictable but insufficient; its parent is protected.
    network = IndependentTransfers([source, target])
    before = [cache.snapshot() for cache in (source, target)]
    result = network.preflight(request(path=(1, 2, 3)), 3, 0, 1)
    assert result.failure_reason == "REACTIVE_FALLBACK_CAPACITY"
    assert result.plan is None
    assert before == [cache.snapshot() for cache in (source, target)]
    assert network.records == []


def test_source_pin_prevents_leaf_eviction_without_refreshing_access():
    source, target = TaskMainCache(2), TaskMainCache(2)
    publish(source, (1, 2), 1)
    original = [source.page_state(block) for block in (1, 2)]
    network = IndependentTransfers([source, target])
    preflight = network.preflight(request(), 2, 0, 1)
    transfer = network.start(preflight.plan, 0, 10)
    assert source.plan_prompt((3, 4)) is None
    for block, old in zip((1, 2), original):
        assert source.page_state(block)["lru_time"] == old["lru_time"]
        assert source.page_state(block)["access_order"] == old["access_order"]
    assert source.reuse_events == []
    network.complete(transfer.transfer_id, transfer.ready_time)
    assert source.pinned_pages == 0


def test_full_wire_temporary_and_dedup_new_copy_lru_no_reuse():
    source, target = TaskMainCache(3), TaskMainCache(3)
    publish(source, (1, 2), 1)
    publish(target, (1,), 2)
    old = target.page_state(1)
    network = IndependentTransfers([source, target])
    before = target.snapshot()
    preflight = network.preflight(request(), 2, 0, 1)
    assert target.snapshot() == before
    transfer = network.start(preflight.plan, 0, 10)
    assert target.lookup((1, 2)) == 1
    assert target.temporary_pages == 2 and target.memory_pages == 3
    assert transfer.wire_tokens == 1024 and transfer.wire_bytes == 2 * PAGE_BYTES
    assert transfer.ready_time == 10 + 2 * PAGE_BYTES / 25e9 * 1000
    network.complete(transfer.transfer_id, transfer.ready_time)
    assert target.lookup((1, 2)) == 2 and target.temporary_pages == 0
    assert (transfer.newly_resident_pages, transfer.duplicate_pages) == (1, 1)
    assert target.page_state(1) == old
    assert target.page_state(2)["lru_time"] == transfer.ready_time
    assert target.reuse_events == source.reuse_events == []
    # A later real request, in contrast to copy/probe, refreshes both reused pages.
    target.lookup((1, 2))
    assert target.page_state(1) == old
    target.record_reuse(1, (1, 2), transfer.ready_time + 1)
    assert target.page_state(1)["lru_time"] == transfer.ready_time + 1
    assert len(target.reuse_events) == 1


def test_concurrent_same_endpoints_have_separate_full_buffers_and_bandwidth():
    source, target = TaskMainCache(4), TaskMainCache(4)
    publish(source, (1, 2))
    network = IndependentTransfers([source, target])
    one = network.start(network.preflight(request(), 2, 0, 1).plan, 0, 10)
    two = network.start(network.preflight(request(1), 2, 0, 1).plan, 1, 10)
    assert one.ready_time == two.ready_time == 10 + 2 * PAGE_BYTES / 25e9 * 1000
    assert target.temporary_pages == target.memory_pages == 4
    assert target.lookup((1, 2)) == 0
    assert source.page_state(1)["pins"] == 2
    network.complete(one.transfer_id, one.ready_time)
    assert target.resident_pages == 2 and target.temporary_pages == 2
    network.complete(two.transfer_id, two.ready_time)
    assert (two.newly_resident_pages, two.duplicate_pages) == (0, 2)
    assert target.memory_pages == 2 and target.pinned_pages == source.pinned_pages == 0
    assert target.peak_memory_pages == 4


def test_prompt_preflight_failure_does_not_partially_evict():
    cache = TaskMainCache(3)
    publish(cache, (1, 2), 1)
    cache.pin((1,))
    before = cache.snapshot()
    assert cache.plan_prompt((3, 4, 5)) is None
    assert cache.snapshot() == before


def test_leaf_lru_parent_eligibility_and_prompt_ancestor_protection():
    cache = TaskMainCache(3)
    publish(cache, (1, 2), 1)
    publish(cache, (3,), 2)
    plan = cache.plan_prompt((4, 5))
    assert plan.evictions == (2, 1)
    cache.publish_prompt(plan, 3)
    assert cache.lookup((3,)) == 1 and cache.lookup((4, 5)) == 2
    # Existing ancestor 3 may not be evicted to fund insertion of its own suffix.
    plan = cache.plan_prompt((3, 6, 7))
    assert plan.evictions == (5, 4)
    cache.publish_prompt(plan, 4)
    assert cache.lookup((3, 6, 7)) == 3


def test_stale_admission_plan_rejected_before_mutation():
    cache = TaskMainCache(2)
    publish(cache, (1,))
    plan = cache.plan_prompt((2, 3))
    cache.pin((1,))
    before = cache.snapshot()
    with pytest.raises(ValueError, match="stale"):
        cache.publish_prompt(plan, 1)
    assert cache.snapshot() == before


@pytest.mark.parametrize("changes", [
    {"routing_policy": "R_REACTIVE_ECT_V1"}, {"routing_policy": "ORACLE"},
    {"theta": 3}, {"page_tokens": 256}, {"page_bytes": 1},
    {"bandwidth_bytes_per_second": 1}, {"load_window_ms": 300000},
    {"protocol_version": "TASKMAIN_V1_1"}, {"num_pods": True},
])
def test_config_rejects_non_stage_a_semantics(changes):
    with pytest.raises(ValueError):
        TaskMainConfig(**changes)


@pytest.mark.parametrize("key", [
    "service", "base_latency_ms", "prefill_tokens_per_second", "proactive",
    "trigger_period_ms", "proactive_byte_rate", "cost_v_ref", "cost_t_ref_ms",
    "admission_timeout_ms", "control_latency_ms", "oracle", "information_scope",
])
def test_yaml_allowlist_rejects_legacy_knobs(tmp_path, key):
    path = tmp_path / "config.yaml"
    path.write_text(f"protocol_version: TASK_MAIN_V1_STAGE_A\n{key}: 1\n")
    with pytest.raises(ValueError, match="unsupported"):
        TaskMainConfig.from_yaml(path)


def test_trace_identity_validation_and_stable_same_timestamp_order():
    assert [r.request_id for r in validate_requests([request(2), request(1)])] == [1, 2]
    with pytest.raises(ValueError, match="duplicate request"):
        validate_requests([request(), request()])
    with pytest.raises(ValueError, match="conflicting Prefix"):
        validate_requests([request(path=(1, 2)), request(1, path=(3, 2))])
    with pytest.raises(ValueError, match="finite"):
        validate_requests([replace(request(), arrival_ms=float("nan"))])


def test_transfer_source_failure_is_nonmutating_and_early_complete_rejected():
    source, target = TaskMainCache(2), TaskMainCache(2)
    network = IndependentTransfers([source, target])
    assert network.preflight(request(), 2, 0, 1).failure_reason == "REACTIVE_FALLBACK_SOURCE_UNAVAILABLE"
    publish(source, (1, 2))
    transfer = network.start(network.preflight(request(), 2, 0, 1).plan, 0, 10)
    before = [cache.snapshot() for cache in (source, target)]
    with pytest.raises(ValueError, match="exactly at ready"):
        network.complete(transfer.transfer_id, 10)
    assert before == [cache.snapshot() for cache in (source, target)]
