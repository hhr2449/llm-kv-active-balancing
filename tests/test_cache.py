import pytest

from src.simulator.cache import CapacityAdmissionError, PrefixCache


def test_cold_lookup_and_publish() -> None:
    cache = PrefixCache()
    assert cache.lookup([1, 2]) == 0
    assert cache.publish([1, 2]) == 2
    assert cache.lookup([1, 2]) == 2
    assert cache.resident_pages == 2


def test_lookup_is_longest_contiguous_prefix_not_set_intersection() -> None:
    cache = PrefixCache()
    cache.publish([1, 2, 3])
    assert cache.lookup([1, 9, 3]) == 1
    assert cache.lookup([9, 2, 3]) == 0


def test_duplicate_publish_does_not_duplicate_residency() -> None:
    cache = PrefixCache()
    assert cache.publish([1, 2]) == 2
    assert cache.publish([1, 2]) == 0
    assert cache.resident_pages == 2


def test_capacity_above_working_set_never_evicts() -> None:
    cache = PrefixCache(capacity_pages=4)
    cache.publish([1, 2], time_ms=0)
    cache.publish([1, 3], time_ms=1)
    assert cache.eviction_count == 0
    assert cache.resident_pages == 3


def test_leaf_lru_eviction_preserves_internal_prefix() -> None:
    cache = PrefixCache(capacity_pages=2)
    cache.publish([1, 2], time_ms=0)
    cache.publish([3], time_ms=1)
    assert cache.lookup([1, 2]) == 1
    assert cache.lookup([3]) == 1
    assert cache.evicted_pages == 1


def test_parent_becomes_evictable_only_after_leaf_is_deleted() -> None:
    cache = PrefixCache(capacity_pages=2)
    cache.publish([1, 2], time_ms=0)
    cache.publish([3, 4], time_ms=1)
    assert cache.lookup([1]) == 0
    assert cache.lookup([3, 4]) == 2
    assert cache.evicted_pages == 2


def test_h_used_refreshes_lru() -> None:
    cache = PrefixCache(capacity_pages=3)
    cache.publish([1], time_ms=0)
    cache.publish([2], time_ms=1)
    cache.pin_and_refresh([1], 1, time_ms=2)
    cache.unpin([1], 1)
    cache.publish([3], time_ms=3)
    cache.publish([4], time_ms=4)
    assert cache.lookup([1]) == 1
    assert cache.lookup([2]) == 0


def test_routing_lookup_does_not_refresh_lru() -> None:
    cache = PrefixCache(capacity_pages=3)
    cache.publish([1], time_ms=0)
    cache.publish([2], time_ms=1)
    assert cache.lookup([1]) == 1
    cache.publish([3], time_ms=2)
    cache.publish([4], time_ms=3)
    assert cache.lookup([1]) == 0
    assert cache.lookup([2]) == 1


def test_pinned_block_cannot_be_evicted_but_remains_visible() -> None:
    cache = PrefixCache(capacity_pages=1)
    cache.publish([1], time_ms=0)
    cache.pin_and_refresh([1], 1, time_ms=1)
    assert cache.lookup([1]) == 1
    assert cache.memory_used_pages == 1
    with pytest.raises(CapacityAdmissionError):
        cache.reserve_active_private(7, 1)
    assert cache.lookup([1]) == 1
    assert cache.is_pinned([1])
    assert cache.memory_used_pages == 1


def test_active_private_is_accounted_but_invisible_then_committed() -> None:
    cache = PrefixCache(capacity_pages=2)
    cache.reserve_active_private(7, 2)
    assert cache.active_private_pages == 2
    assert cache.memory_used_pages == 2
    assert cache.lookup([1, 2]) == 0
    inserted = cache.commit_active(7, [1, 2], time_ms=1)
    assert inserted == 2
    assert cache.active_private_pages == 0
    assert cache.resident_pages == 2
    assert cache.lookup([1, 2]) == 2
    assert cache.memory_used_pages == 2


def test_duplicate_publish_does_not_refresh_or_consume_capacity() -> None:
    cache = PrefixCache(capacity_pages=2)
    cache.publish([1], time_ms=0)
    cache.publish([2], time_ms=1)
    assert cache.publish([1], time_ms=10) == 0
    cache.publish([3], time_ms=11)
    assert cache.lookup([1]) == 0
    assert cache.lookup([2]) == 1
    assert cache.resident_pages == 2


def test_impossible_admission_is_explicit_and_never_overflows() -> None:
    cache = PrefixCache(capacity_pages=1)
    with pytest.raises(CapacityAdmissionError) as error:
        cache.reserve_active_private(9, 2)
    assert error.value.diagnostic["request_id"] == 9
    assert cache.capacity_admission_failures == 1
    assert cache.memory_used_pages == 0
    assert cache.peak_memory_used_pages <= 1
