from src.simulator.cache import PrefixCache


def test_no_evict_preflight_does_not_change_cache_or_lru():
    cache = PrefixCache(1)
    cache.publish([1], time_ms=10)
    before = (cache.resident_pages, cache.eviction_count, cache.lookup([1]))
    assert not cache.has_free_capacity(1)
    after = (cache.resident_pages, cache.eviction_count, cache.lookup([1]))
    assert after == before


def test_normal_path_can_still_evict_after_no_evict_preflight():
    cache = PrefixCache(1)
    cache.publish([1], time_ms=10)
    assert not cache.has_free_capacity(1)
    cache.publish([2], time_ms=20)
    assert cache.lookup([1]) == 0
    assert cache.lookup([2]) == 1
    assert cache.eviction_count == 1


def test_committed_proactive_style_replica_remains_normal_lru_victim():
    cache = PrefixCache(1)
    cache.reserve_transfer_temporary(7, 1)
    cache.commit_transfer_temporary(7, [1], 10)
    cache.publish([2], 20)
    assert cache.lookup([1]) == 0
    assert cache.lookup([2]) == 1
