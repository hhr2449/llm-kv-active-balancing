from src.simulator.config import SimulatorConfig
from src.simulator.engine import SimulatorEngine
from src.simulator.pod import Pod
from src.simulator.tickets import ReactiveNodeGate, ReactiveTransferTicket
from src.simulator.trace import TraceRequest


def ticket(i: int, source: int, target: int, enqueue: float = 0):
    return ReactiveTransferTicket(i, i, enqueue, enqueue + 100, source, target,
                                  1, 512, 1, 1, 1, 512, 10, 0,
                                  target, 10, i)


def req(i, arrival, blocks):
    return TraceRequest.from_record(i, {
        "timestamp": arrival, "input_length": 512 * len(blocks),
        "output_length": 1, "hash_ids": blocks,
    })


def config(**overrides):
    values = dict(page_tokens=512, base_latency_ms=2, prefill_tokens_per_second=1000,
                  cache_capacity_pages=None, num_pods=2, routing_policy="R_REQ_KV",
                  transfer_enabled=True, theta_simple=1.5, page_bytes=1024,
                  effective_bandwidth_bytes_per_s=1_024_000,
                  control_latency_ms=1, partial_page_mode="FULL_PAGE_ONLY",
                  transfer_mode="FULL_PREFIX", cache_hit_threshold=0.3,
                  relative_load_threshold=1.5, absolute_load_gap_ms=0,
                  admission_timeout_ms=5000)
    values.update(overrides)
    return SimulatorConfig(**values)


def scenario():
    return [req(0, 0, [1]), req(1, 600, [1, 3]), req(2, 600, [1, 2])]


def test_fifo_order_and_disjoint_feasible_scan() -> None:
    gate = ReactiveNodeGate()
    newer = ticket(2, 2, 3, 0)
    older = ticket(1, 0, 1, 0)
    gate.enqueue(newer)
    gate.enqueue(older)
    assert [t.ticket_id for t in gate.queued()] == [1, 2]
    feasible = gate.earliest_endpoint_feasible(lambda source, target: source not in {0, 1})
    assert [t.ticket_id for t in feasible] == [2]


def test_remote_pressure_has_exactly_one_owner_and_drains() -> None:
    first, second = Pod(0), Pod(1)
    second.add_remote_pressure(7, 12.0)
    assert first.committed_remote_pressure_ms == 0
    assert second.committed_remote_pressure_ms == 12
    assert second.remove_remote_pressure(7) == 12
    assert second.committed_remote_pressure_ms == 0


def test_b0_gate_transfer_and_remote_to_gpu_handoff() -> None:
    results, summary = SimulatorEngine(config()).run(scenario())
    assert results[2].p2p_assisted
    assert summary["transfer"]["ticket_created"] == 1
    assert summary["transfer"]["transfer_completed"] == 1
    assert all(p["final_remote_pressure_ms"] == 0 for p in summary["per_pod"])
    assert all(p["final_load_ms"] == 0 for p in summary["per_pod"])


def test_cache_and_absolute_gates_reject_without_ticket() -> None:
    _, cache_summary = SimulatorEngine(config(cache_hit_threshold=0.9)).run(scenario())
    assert cache_summary["transfer"]["cache_gate_reject"] >= 1
    assert cache_summary["transfer"]["ticket_created"] == 0
    _, abs_summary = SimulatorEngine(config(absolute_load_gap_ms=1000)).run(scenario())
    assert abs_summary["transfer"]["absolute_gate_reject"] >= 1
    assert abs_summary["transfer"]["ticket_created"] == 0


def test_complete_wins_over_timeout_at_same_timestamp() -> None:
    cfg = config(control_latency_ms=4, admission_timeout_ms=5)
    results, summary = SimulatorEngine(cfg).run(scenario())
    assert results[2].p2p_assisted
    assert summary["transfer"]["transfer_admission_timeouts"] == 0


def test_zero_timeout_cleans_pressure_and_falls_back() -> None:
    results, summary = SimulatorEngine(config(admission_timeout_ms=0)).run(scenario())
    assert not results[2].p2p_assisted
    assert summary["transfer"]["transfer_admission_timeouts"] == 1
    assert all(p["final_remote_pressure_ms"] == 0 for p in summary["per_pod"])
