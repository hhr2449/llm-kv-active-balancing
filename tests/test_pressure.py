from types import SimpleNamespace

from src.simulator.pressure import pressure_diagnostics


def test_busy_while_other_idle_integrates_wall_and_request_time() -> None:
    intervals = [{
        "start_ms": 0.0, "end_ms": 10.0,
        "start_loads": [10.0, 0.0], "end_loads": [5.0, 0.0],
        "queue_ids": [(1, 2), ()], "queue_lengths": [2, 0],
        "running": [True, False],
    }]
    result = pressure_diagnostics([], intervals, 2, windows_ms=(10,))
    assert result["busy_while_other_idle_ms"] == 10.0
    assert result["busy_while_other_idle_fraction"] == 1.0
    assert result["queued_requests_while_other_idle"] == 2
    assert result["queued_request_ms_while_other_idle"] == 20.0
    assert result["load_gap_ms"]["p50"] == 7.5


def test_short_window_request_and_wait_aggregation() -> None:
    request = SimpleNamespace(
        arrival_ms=1.0, service_start_ms=7.0, queue_time_ms=6.0,
        pod_id=0, input_tokens=100, miss_tokens=80,
    )
    result = pressure_diagnostics([request], [], 1, windows_ms=(5,))
    windows = result["windows"]["5"]
    assert windows[0]["pods"][0]["request_count"] == 1
    assert windows[0]["pods"][0]["queue_wait_ms"] == 4.0
    assert windows[1]["pods"][0]["executed_miss_tokens"] == 80
    assert windows[1]["pods"][0]["queue_wait_ms"] == 2.0
