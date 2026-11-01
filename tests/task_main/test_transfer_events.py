"""Additional isolated event-loop cases; production routing has its own tests."""
from collections import Counter

from src.simulator.task_main.config import PAGE_BYTES, TaskMainConfig
from src.simulator.task_main.engine import TaskMainEngine, _Assignment
from src.simulator.task_main.trace import TraceRequest


def request(rid, time=0, path=(1, 2), tokens=1024):
    return TraceRequest.from_record(rid, {
        "timestamp": time, "input_length": tokens, "output_length": 0,
        "hash_ids": list(path),
    })


class InjectedAssignmentEngine(TaskMainEngine):
    """Fixture only: chosen request IDs attempt copy to Pod 1 regardless of gate."""

    def __init__(self, copy_ids, capacity=8):
        super().__init__(TaskMainConfig(routing_policy="R_REQ_KV_TASK", num_pods=2,
                                        capacity_pages=capacity))
        self.copy_ids = set(copy_ids)

    def _reactive_assignment(self, req, source, hit_pages, loads, now):
        if req.request_id in self.copy_ids:
            return self._copy_or_fallback(req, source, 1, hit_pages, loads, now)
        return _Assignment(req, source, source, hit_pages, hit_pages, loads, now)


def preload(engine, pod, path):
    cache = engine.caches[pod]
    cache.publish_prompt(cache.plan_prompt(path), 0)


def test_inflight_load_visible_immediately_but_copy_and_opportunity_wait_for_ready():
    engine = InjectedAssignmentEngine({0})
    preload(engine, 0, (1,))
    result = engine.run([request(0), request(1, 0.1, (3,), 512)])
    first, second = result.request_records
    ready = PAGE_BYTES / 25e9 * 1000
    assert first.load_account_time == 0 < first.completion_time == ready
    assert first.miss_tokens == 512 and first.final_hit_tokens == 512
    assert second.load_vector_before_route == (0, 512)
    assert [row.request_id for row in result.opportunity_records] == [1, 0]
    intermediate = result.opportunity_records[0].cache_summaries[1]
    assert intermediate["temporary_pages"] == 1 and intermediate["published_pages"] == 0
    assert Counter(row.request_id for row in result.opportunity_records) == {0: 1, 1: 1}
    assert len(engine.load.entries) == 2


def test_all_transfers_publish_before_stable_resumes_then_new_arrival():
    engine = InjectedAssignmentEngine({0, 1})
    preload(engine, 0, (1,))
    ready = PAGE_BYTES / 25e9 * 1000
    result = engine.run([request(1), request(0), request(2, ready)])
    events = [row for row in result.event_records if row.time == ready]
    assert [(row.type, row.request_id) for row in events[:2]] == [
        ("TRANSFER_COMPLETE", 0), ("TRANSFER_COMPLETE", 1),
    ]
    assert [(row.type, row.request_id) for row in events[2:]] == [
        ("REQUEST_COMPLETE", 0), ("PUBLISHED", 0), ("OPPORTUNITY", 0),
        ("REQUEST_COMPLETE", 1), ("PUBLISHED", 1), ("OPPORTUNITY", 1),
        ("REQUEST_ARRIVAL", 2), ("FINAL_ASSIGNMENT", 2),
        ("REQUEST_COMPLETE", 2), ("PUBLISHED", 2), ("OPPORTUNITY", 2),
    ]
    assert result.request_records[2].final_hit_tokens == 1024
    assert result.transfer_records[1].duplicate_pages == 1
    assert all(row.ready_time == ready for row in result.transfer_records)


def test_copy_capacity_failure_falls_back_without_losing_request_or_partial_eviction():
    engine = InjectedAssignmentEngine({0, 1}, capacity=2)
    preload(engine, 0, (1, 2))
    # The first independent full buffer fills target capacity until ready.
    result = engine.run([request(0), request(1)])
    first, failed = result.request_records
    assert first.final_pod == 1 and failed.final_pod == failed.affinity_source == 0
    assert failed.reactive_fallback_reason == "REACTIVE_FALLBACK_CAPACITY"
    assert failed.completion_time == 0 and failed.final_hit_tokens == 1024
    assert failed.reactive_transfer_id is None and len(result.transfer_records) == 1
    assert engine.caches[1].eviction_records == []
    assert result.validation["status"] == "PASS"


def test_partial_tail_is_local_only_and_miss_committed_before_ready():
    engine = InjectedAssignmentEngine({0})
    preload(engine, 0, (1, 2))
    result = engine.run([request(0, tokens=600)])
    row = result.request_records[0]
    assert row.route_hit_tokens == 600 and row.final_hit_tokens == 512
    assert row.miss_tokens == 88 and engine.load.entries[0].miss_tokens == 88
    assert result.transfer_records[0].transferable_chain == (1,)
    assert engine.caches[1].lookup((1, 2)) == 2


def test_no_full_page_fallback_has_no_zero_wire_transfer():
    engine = InjectedAssignmentEngine({0})
    preload(engine, 0, (1,))
    result = engine.run([request(0, path=(1,), tokens=100)])
    assert result.transfer_records == []
    assert result.request_records[0].final_hit_tokens == 100
    assert result.request_records[0].reactive_fallback_reason == "REACTIVE_FALLBACK_NO_TRANSFERABLE_PREFIX"


def test_reactive_event_replay_is_deterministic_under_injected_assignments():
    def run():
        engine = InjectedAssignmentEngine({0, 1}, capacity=3)
        preload(engine, 0, (1,))
        return engine.run([request(0), request(1)])
    assert run() == run()

