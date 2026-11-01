"""Non-injected R_REQ_KV_TASK replay: no injected routing or Load entries."""
from collections import Counter

import pytest

from src.simulator.task_main.config import PAGE_BYTES, TaskMainConfig
from src.simulator.task_main.engine import TaskMainEngine
from src.simulator.task_main.trace import TraceRequest


def request(rid, time=0, path=(1,), tokens=None):
    return TraceRequest.from_record(rid, {
        "timestamp": time, "input_length": len(path) * 512 if tokens is None else tokens,
        "output_length": 0, "hash_ids": list(path),
    })


def engine(capacity=8, pods=2):
    return TaskMainEngine(TaskMainConfig(routing_policy="R_REQ_KV_TASK",
                                        num_pods=pods, capacity_pages=capacity))


def preload(sim, pod, path):
    cache = sim.caches[pod]
    cache.publish_prompt(cache.plan_prompt(path), 0)


@pytest.mark.parametrize("source_path,tokens", [((1,), 512), ((1, 2), 600)])
def test_gate_false_stays_source_even_when_target_owns_transferable_prefix(source_path, tokens):
    sim = engine()
    preload(sim, 0, source_path)
    preload(sim, 1, (1,))
    result = sim.run([request(0, path=source_path, tokens=tokens)])
    row = result.request_records[0]
    assert row.load_vector_before_route == (0, 0)
    assert row.final_pod == row.affinity_source == 0
    assert row.final_hit_tokens == tokens
    assert row.completion_time == 0 and result.transfer_records == []


@pytest.mark.parametrize("source_tokens,expected_target", [(1024, 0), (1025, 1)])
def test_strict_gate_equality_and_one_token_above_in_production(source_tokens, expected_target):
    sim = engine()
    path = (1, 2) if source_tokens == 1024 else (1, 2, 3)
    result = sim.run([request(0, path=path, tokens=source_tokens),
                      request(1, 1, (4,)), request(2, 2)])
    row = result.request_records[2]
    assert row.load_vector_before_route == (source_tokens, 512)
    assert row.affinity_source == 0 and row.final_pod == expected_target
    assert len(result.transfer_records) == expected_target
    assert row.reactive_fallback_reason is None


def test_gate_true_direct_target_compares_full_page_prefix_and_charges_actual_miss():
    sim = engine()
    preload(sim, 0, (1,))
    preload(sim, 1, (1,))
    result = sim.run([request(0, path=(1, 2), tokens=600),
                      request(1, 1, (1, 2), 600)])
    row = result.request_records[1]
    assert row.load_vector_before_route == (88, 0)  # target=0 handled without division
    assert row.affinity_source == 0 and row.final_pod == 1
    assert row.route_hit_tokens == 600 and row.final_hit_tokens == 512
    assert row.miss_tokens == 88 and row.load_account_time == row.completion_time == 1
    assert result.transfer_records == [] and row.reactive_fallback_reason is None
    assert sim.load.entries[1].pod_id == 1 and sim.load.entries[1].miss_tokens == 88
    assert sim.caches[1].lookup((1, 2)) == 2
    assert [event.path for event in sim.caches[1].reuse_events] == [(1,)]


def test_empty_transferable_prefix_gate_true_stays_source_without_wire():
    sim = engine()
    result = sim.run([request(0, tokens=100), request(1, 1, tokens=100)])
    row = result.request_records[1]
    assert row.affinity_source == row.final_pod == 0
    assert row.route_hit_tokens == row.final_hit_tokens == 100
    assert row.miss_tokens == 0 and result.transfer_records == []
    assert row.reactive_fallback_reason is None
    assert row.reactive_gate_passed and row.transferable_pages == 0
    assert result.summary["sanity"]["gate_true_transferable_zero_count"] == 1


def test_empty_transferable_prefix_gate_false_stays_source_without_wire():
    sim = engine()
    preload(sim, 0, (1,))
    result = sim.run([request(0, tokens=100)])
    row = result.request_records[0]
    assert not row.reactive_gate_passed and row.transferable_pages == 0
    assert row.route_hit_tokens == row.final_hit_tokens == 100
    assert row.affinity_source == row.final_pod == 0
    assert row.miss_tokens == 0 and result.transfer_records == []


def test_same_ready_committed_hit_protection_survives_earlier_prompt_admission(monkeypatch):
    sim = engine(capacity=3)
    preload(sim, 0, (1,))
    preload(sim, 0, (2,))
    target = sim.caches[1]
    pin_checks = []
    original_pin = target.pin

    def checked_pin(path):
        before = [(target.page_state(block)["lru_time"],
                   target.page_state(block)["access_order"]) for block in path]
        reuse_count = len(target.reuse_events)
        original_pin(path)
        assert before == [(target.page_state(block)["lru_time"],
                           target.page_state(block)["access_order"]) for block in path]
        assert len(target.reuse_events) == reuse_count
        pin_checks.append(tuple(path))

    monkeypatch.setattr(target, "pin", checked_pin)
    original_plan = target.plan_prompt
    admission_checks = []

    def checked_plan(path):
        if tuple(path) == (1, 3, 4):
            # Both transfers are Published, but B has not resumed or reused yet.
            assert target.lookup((2,)) == 1 and target.page_state(2)["pins"] == 1
            assert [event.request_id for event in target.reuse_events] == [1]
            admission_checks.append(target.page_state(2)["lru_time"])
        return original_plan(path)

    monkeypatch.setattr(target, "plan_prompt", checked_plan)
    # Oversized cold Prompt commits 3072 Load to source without evicting its roots.
    # Both following gates pass naturally; their one-page wires have equal ready.
    result = sim.run([request(0, path=(10, 11, 12, 13, 14, 15)),
                      request(1, 1, (1, 3, 4)), request(2, 1, (2,))])
    a, b = result.request_records[1:]
    ready = result.transfer_records[0].ready_time
    assert result.transfer_records[1].ready_time == ready
    assert a.cache_admission_skip  # B's protected root cannot fund A's two-page suffix.
    assert b.final_hit_pages == b.route_hit_pages == 1 and b.final_hit_tokens == 512
    assert sim.load.entries[2].miss_tokens == b.miss_tokens == 0
    assert pin_checks == [(1,), (2,)] and admission_checks == [ready]
    assert target.eviction_records == []
    assert all(cache.pinned_pages == cache.temporary_pages == 0 for cache in sim.caches)


def test_copy_load_temporary_ready_order_and_unique_opportunity_in_production():
    sim = engine()
    ready = 1 + PAGE_BYTES / 25e9 * 1000
    result = sim.run([request(0), request(1, 1, (1, 2)),
                      request(2, 1.1, (3,)), request(3, ready, (1, 2))])
    copied, during, after = result.request_records[1:]
    assert copied.final_pod == 1 and copied.final_hit_tokens == copied.miss_tokens == 512
    assert copied.load_account_time == 1 and copied.completion_time == ready
    assert during.load_vector_before_route == (512, 512)
    intermediate = next(row for row in result.opportunity_records if row.request_id == 2)
    assert intermediate.cache_summaries[1]["temporary_pages"] == 1
    assert intermediate.cache_summaries[1]["published_pages"] == 0
    assert after.final_pod == 1 and after.route_hit_tokens == after.final_hit_tokens == 1024
    assert [row.type for row in result.event_records if row.time == ready] == [
        "TRANSFER_COMPLETE", "REQUEST_COMPLETE", "PUBLISHED", "OPPORTUNITY",
        "REQUEST_ARRIVAL", "FINAL_ASSIGNMENT", "REQUEST_COMPLETE", "PUBLISHED", "OPPORTUNITY",
    ]
    assert Counter(row.request_id for row in result.opportunity_records) == dict.fromkeys(range(4), 1)
    assert len(sim.load.entries) == 4 and result.validation["status"] == "PASS"


def test_same_endpoint_concurrent_copy_and_capacity_fallback_in_production():
    sim = engine(capacity=2)
    result = sim.run([request(0), request(1, 1), request(2, 1), request(3, 1)])
    transfers = result.transfer_records
    assert len(transfers) == 2
    assert all(row.source == 0 and row.target == 1 for row in transfers)
    assert transfers[0].ready_time == transfers[1].ready_time == 1 + PAGE_BYTES / 25e9 * 1000
    failed = result.request_records[3]
    assert failed.final_pod == failed.affinity_source == 0
    assert failed.reactive_fallback_reason == "REACTIVE_FALLBACK_CAPACITY"
    assert failed.completion_time == 1 and failed.final_hit_tokens == 512
    assert failed.reactive_transfer_id is None
    assert sim.caches[1].eviction_records == []
    assert sim.caches[1].peak_memory_pages == 2
    assert transfers[1].newly_resident_pages == 0 and transfers[1].duplicate_pages == 1
    events = [row for row in result.event_records if row.time == transfers[0].ready_time]
    assert [(row.type, row.request_id) for row in events[:2]] == [
        ("TRANSFER_COMPLETE", 1), ("TRANSFER_COMPLETE", 2),
    ]
    assert [row.request_id for row in events if row.type == "REQUEST_COMPLETE"] == [1, 2]
    assert result.validation["status"] == "PASS"


def test_partial_tail_copy_commits_only_full_page_hit():
    sim = engine()
    result = sim.run([request(0, path=(1, 2), tokens=600),
                      request(1, 1, (1, 2), 600)])
    row = result.request_records[1]
    transfer = result.transfer_records[0]
    assert transfer.transferable_chain == (1,) and transfer.wire_tokens == 512
    assert row.route_hit_tokens == 600 and row.final_hit_tokens == 512 and row.miss_tokens == 88
    assert row.load_account_time == 1 < row.completion_time


def test_expired_source_load_disables_gate_at_exact_sixty_seconds():
    result = engine().run([request(0), request(1, 60000)])
    row = result.request_records[1]
    assert row.load_vector_before_route == (0, 0) and row.final_pod == 0
    assert result.transfer_records == []


def test_other_target_tie_uses_pod_id_in_four_pod_production_run():
    result = engine(pods=4).run([request(0), request(1, 1)])
    assert result.request_records[1].load_vector_before_route == (512, 0, 0, 0)
    assert result.transfer_records[0].target == 1


def test_single_pod_synthetic_topology_keeps_source():
    result = engine(pods=1).run([request(0), request(1, 1)])
    assert [row.final_pod for row in result.request_records] == [0, 0]
    assert result.transfer_records == []


@pytest.mark.parametrize("policy", ["R_AFF", "R_REQ_KV_TASK"])
def test_both_baselines_deterministic_from_cold_cache(policy):
    config = TaskMainConfig(routing_policy=policy, num_pods=2, capacity_pages=3)
    trace = [request(0), request(1, 1), request(2, 1), request(3, 2, (1, 2)),
             request(4, 60001, (3,))]
    first = TaskMainEngine(config).run(trace)
    second = TaskMainEngine(config).run(trace)
    assert first == second
    assert first.validation["status"] == "PASS"
    assert len(first.opportunity_records) == len(trace)
