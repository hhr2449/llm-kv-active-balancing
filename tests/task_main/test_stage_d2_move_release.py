from dataclasses import replace
from pathlib import Path

import pytest

from src.simulator.task_main.stage_d2.cache import StageD2Cache
from src.simulator.task_main.stage_d2.config import StageD2Config
from src.simulator.task_main.stage_d2.transfer import StageD2Transfers


ROOT = Path(__file__).resolve().parents[2]


def config(case="oracle_move", action="MOVE"):
    policy = "FUTURE_DEMAND" if case == "oracle_move" else "PERSISTENCE"
    return StageD2Config(
        workload="conversation", case_id=case, proactive_policy=policy, action=action,
        trace_path="data/mooncake/conversation_trace.jsonl",
        trace_sha256="b8cbb061a85206d729d91cdc2981f43c9e0d99209dce588d3af5f7934408b9df",
    )


def put(cache, path, now=0):
    plan = cache.plan_prompt(path)
    assert plan is not None
    cache.publish_prompt(plan, now)


def release(cache, path):
    generations = cache.source_generations(path)
    before = {block: cache.page_state(block) for block in path}
    result = cache.safe_release_suffix(path, generations, 10)
    return result, before


def test_simple_leaf_chain_full_release_without_lru_eviction():
    cache = StageD2Cache(10)
    put(cache, (1, 2, 3))
    result, _ = release(cache, (1, 2, 3))
    assert result["released_blocks_deep_to_root"] == (3, 2, 1)
    assert result["release_status"] == result["release_stop_reason"] == "FULL_RELEASE"
    assert cache.resident_pages == 0
    assert cache.eviction_records == []


def test_shared_ancestor_releases_only_safe_suffix_without_touching_other_branch():
    cache = StageD2Cache(10)
    put(cache, (1, 2, 3))
    put(cache, (1, 2, 4))
    result, before = release(cache, (1, 2, 3))
    assert result["released_blocks_deep_to_root"] == (3,)
    assert result["release_status"] == "PARTIAL_RELEASE"
    assert result["release_stop_reason"] == "SHARED_OR_NONLEAF"
    assert cache.lookup((1, 2, 4)) == 3
    assert cache.page_state(2)["lru_time"] == before[2]["lru_time"]


def test_deeper_descendant_makes_moved_endpoint_nonleaf_and_zero_release():
    cache = StageD2Cache(10)
    put(cache, (1, 2, 3, 4))
    generations = cache.source_generations((1, 2, 3))
    result = cache.safe_release_suffix((1, 2, 3), generations, 10)
    assert result["release_status"] == "ZERO_RELEASE"
    assert result["release_stop_reason"] == "SHARED_OR_NONLEAF"
    assert cache.lookup((1, 2, 3, 4)) == 4


def test_active_pin_stops_before_pinned_page_after_releasing_deeper_suffix():
    cache = StageD2Cache(10)
    put(cache, (1, 2, 3))
    cache.pin((1, 2))
    generations = cache.source_generations((1, 2, 3))
    result = cache.safe_release_suffix((1, 2, 3), generations, 10)
    assert result["released_blocks_deep_to_root"] == (3,)
    assert result["release_stop_reason"] == "ACTIVE_PIN"
    cache.unpin((1, 2))


def test_committed_hit_protection_stops_release_without_refreshing_lru():
    cache = StageD2Cache(10)
    put(cache, (1, 2, 3))
    before = cache.page_state(2)
    cache.protect_committed((1, 2))
    generations = cache.source_generations((1, 2, 3))
    result = cache.safe_release_suffix((1, 2, 3), generations, 10)
    assert result["released_blocks_deep_to_root"] == (3,)
    assert result["release_stop_reason"] == "COMMITTED_PROTECTION"
    assert cache.page_state(2)["lru_time"] == before["lru_time"]
    assert cache.page_state(2)["access_order"] == before["access_order"]
    cache.unprotect_committed((1, 2))


def test_generation_mismatch_never_deletes_replacement():
    cache = StageD2Cache(10)
    put(cache, (1, 2, 3))
    generations = cache.source_generations((1, 2, 3))
    cache._pages[3].residency_generation += 1
    result = cache.safe_release_suffix((1, 2, 3), generations, 10)
    assert result["release_status"] == "ZERO_RELEASE"
    assert result["release_stop_reason"] == "GENERATION_CHANGED"
    assert cache.lookup((1, 2, 3)) == 3


def test_predicted_release_is_read_only_and_matches_unchanged_ready_state():
    cache = StageD2Cache(10)
    put(cache, (1, 2, 3))
    generations = cache.source_generations((1, 2, 3))
    snapshot = cache.snapshot()
    assert cache.predicted_releasable_pages((1, 2, 3), generations) == 3
    assert cache.snapshot() == snapshot
    assert cache.safe_release_suffix((1, 2, 3), generations, 10)[
        "actual_source_released_pages"] == 3


def test_move_full_wire_inflight_source_visible_target_hidden_then_safe_release():
    caches = [StageD2Cache(10), StageD2Cache(10)]
    put(caches[0], (1, 2, 3))
    transfers = StageD2Transfers(caches, "MOVE")
    preflight = transfers.preflight_chain((1, 2, 3), 0, 1)
    record = transfers.start(preflight.plan, 7, 0, "PROACTIVE")
    assert caches[0].lookup((1, 2, 3)) == 3
    assert caches[1].lookup((1, 2, 3)) == 0
    assert caches[1].temporary_pages == 3
    transfers.complete(record.transfer_id, record.ready_time)
    assert caches[1].lookup((1, 2, 3)) == 3
    assert caches[0].lookup((1, 2, 3)) == 0
    metadata = transfers.move_metadata[record.transfer_id]
    assert metadata.predicted_releasable_pages == 3
    assert metadata.release["release_status"] == "FULL_RELEASE"


class FailCommitCache(StageD2Cache):
    def complete_transfer(self, transfer_id, path, now):
        raise RuntimeError("synthetic target commit failure")


def test_target_commit_failure_preserves_source_and_cleans_move_protection():
    source, target = StageD2Cache(10), FailCommitCache(10)
    put(source, (1, 2, 3))
    transfers = StageD2Transfers((source, target), "MOVE")
    plan = transfers.preflight_chain((1, 2, 3), 0, 1).plan
    record = transfers.start(plan, 7, 0, "PROACTIVE")
    with pytest.raises(RuntimeError, match="synthetic target"):
        transfers.complete(record.transfer_id, record.ready_time)
    assert source.lookup((1, 2, 3)) == 3
    assert source.pinned_pages == 0
    assert target.lookup((1, 2, 3)) == 0
    assert target.temporary_pages == 0
    assert transfers.move_metadata[record.transfer_id].release[
        "release_stop_reason"] == "TARGET_COMMIT_FAILED"


def test_config_is_fixed_to_four_move_cases_and_rejects_unapproved_action():
    paths = list((ROOT/"configs/task_main/stage_d2").rglob("*.yaml"))
    assert len(paths) == 4
    assert all(StageD2Config.from_yaml(path).action == "MOVE" for path in paths)
    with pytest.raises(ValueError):
        replace(config(), action="DELETE")
