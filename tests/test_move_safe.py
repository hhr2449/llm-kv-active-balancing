from dataclasses import replace

from src.simulator.cache import PrefixCache
from src.simulator.config import SimulatorConfig, SplitConfig
from src.simulator.engine import SimulatorEngine
from src.simulator.trace import TraceRequest


def request(request_id, arrival_ms, block_id):
    return TraceRequest.from_record(request_id, {
        "timestamp": arrival_ms, "input_length": 512, "output_length": 0,
        "hash_ids": [block_id],
    }, split_config=SplitConfig(0, 900, 1500, 2700))


BASE = SimulatorConfig(
    512, 2, 10000, None, 2, "R_AFF", False,
    page_bytes=1, effective_bandwidth_bytes_per_s=1_000_000,
    split_config=SplitConfig(0, 900, 1500, 2700), summary_split="DEVELOPMENT",
    proactive_enabled=True, proactive_action="MOVE_SAFE", proactive_strategy="PERSIST",
    persistence_history_ms=600, trigger_period_ms=100, trigger_phase_ms=100,
    proactive_byte_rate=1_000_000, proactive_burst_bytes=1_000_000,
    oracle_horizon_ms=500, oracle_visibility_end_ms=900,
)


def test_leaf_exclusive_suffix_can_be_released_after_target_commit():
    source, target = PrefixCache(), PrefixCache()
    source.publish([1, 2, 3])
    target.reserve_transfer_temporary(7, 3)
    assert source.lookup([1, 2, 3]) == 3
    target.commit_transfer_temporary(7, [1, 2, 3], 1)
    released = source.release_leaf_exclusive_suffix([1, 2, 3])
    assert released["pages_freed"] == 3
    assert target.lookup([1, 2, 3]) == 3


def test_transfer_failure_does_not_release_source():
    source, target = PrefixCache(), PrefixCache()
    source.publish([1, 2])
    source.pin([1, 2], 2)
    target.reserve_transfer_temporary(8, 2)
    target.release_transfer_temporary(8)
    source.unpin([1, 2], 2)
    assert source.lookup([1, 2]) == 2


def test_shared_ancestor_is_retained_and_partial_release_is_legal():
    cache = PrefixCache()
    cache.publish([1, 2])
    cache.publish([1, 3])
    released = cache.release_leaf_exclusive_suffix([1, 2])
    assert released == {"pages_freed": 1, "blocked_shared": 1,
                        "blocked_pinned": 0, "blocked_active_or_transfer": 0}
    assert cache.lookup([1, 3]) == 2
    assert cache.lookup([1, 2]) == 1


def test_pinned_page_is_not_released():
    cache = PrefixCache()
    cache.publish([1, 2])
    cache.pin([1, 2], 2)
    released = cache.release_leaf_exclusive_suffix([1, 2])
    assert released["pages_freed"] == 0
    assert released["blocked_pinned"] == 1
    assert cache.lookup([1, 2]) == 2


def test_other_transfer_source_pin_is_reported_and_retained():
    cache = PrefixCache()
    cache.publish([1])
    cache.pin([1], 1)
    released = cache.release_leaf_exclusive_suffix([1], protected_transfer_blocks=[1])
    assert released["pages_freed"] == 0
    assert released["blocked_active_or_transfer"] == 1


def test_zero_release_move_is_legal_and_does_not_damage_other_prefix():
    cache = PrefixCache()
    cache.publish([1, 2])
    cache.publish([1, 3])
    cache.pin([1, 2], 2)
    released = cache.release_leaf_exclusive_suffix([1, 2])
    assert released["pages_freed"] == 0
    assert cache.lookup([1, 3]) == 2


def test_transfer_admission_failure_does_not_attempt_source_release():
    config = replace(BASE, cache_capacity_pages=1, proactive_burst_bytes=0,
                     proactive_byte_rate=0)
    _, summary = SimulatorEngine(config).run([
        request(0, 0, 1), request(1, 500, 1), request(2, 1000, 2)
    ])
    assert summary["proactive"]["actions_started"] == 0
    assert summary["proactive"]["move_source_release_attempts"] == 0


def test_move_release_occurs_only_on_completed_action():
    _, summary = SimulatorEngine(BASE).run([
        request(0, 0, 1), request(1, 500, 1), request(2, 1000, 2)
    ])
    proactive = summary["proactive"]
    assert proactive["move_actions_started"] == proactive["move_actions_completed"] > 0
    assert proactive["move_source_release_attempts"] == proactive["move_actions_completed"]
    assert proactive["move_source_pages_freed"] > 0


def test_proactive_disabled_is_unchanged_by_move_setting():
    requests = [request(0, 0, 1), request(1, 500, 1)]
    copy = replace(BASE, proactive_enabled=False, proactive_action="COPY")
    move = replace(BASE, proactive_enabled=False, proactive_action="MOVE_SAFE")
    assert SimulatorEngine(copy).run(requests) == SimulatorEngine(move).run(requests)
