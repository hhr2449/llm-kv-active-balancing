import pytest

from src.simulator.service import PrefillServiceModel
from src.simulator.trace import TraceRequest


def request() -> TraceRequest:
    return TraceRequest.from_record(
        0,
        {"timestamp": 0, "input_length": 600, "output_length": 10, "hash_ids": [1, 2]},
    )


def test_service_time_uses_miss_tokens() -> None:
    estimate = PrefillServiceModel(3.0, 1000.0).estimate(request(), 1)
    assert estimate.hit_tokens == 512
    assert estimate.miss_tokens == 88
    assert estimate.service_ms == pytest.approx(91.0)


def test_full_hit_still_pays_positive_base_latency() -> None:
    estimate = PrefillServiceModel(3.0, 1000.0).estimate(request(), 2)
    assert estimate.miss_tokens == 0
    assert estimate.service_ms == 3.0
