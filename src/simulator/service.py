from __future__ import annotations

from dataclasses import dataclass

from .trace import TraceRequest


@dataclass(frozen=True)
class ServiceEstimate:
    hit_pages: int
    hit_tokens: int
    miss_tokens: int
    service_ms: float


@dataclass(frozen=True)
class PrefillServiceModel:
    base_latency_ms: float
    prefill_tokens_per_second: float

    def estimate(self, request: TraceRequest, hit_pages: int) -> ServiceEstimate:
        hit_tokens = request.hit_tokens(hit_pages)
        miss_tokens = request.input_tokens - hit_tokens
        service_ms = (
            self.base_latency_ms
            + miss_tokens / self.prefill_tokens_per_second * 1000.0
        )
        if service_ms < self.base_latency_ms or self.base_latency_ms <= 0:
            raise AssertionError("service time invariant violated")
        return ServiceEstimate(hit_pages, hit_tokens, miss_tokens, service_ms)
