from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from .cache import PrefixCache


@dataclass(frozen=True)
class QueuedWork:
    request_id: int
    estimated_service_ms: float


class Pod:
    """One independent FCFS Prefill server and Prefix Cache."""

    def __init__(self, pod_id: int, cache_capacity_pages: int | None = None) -> None:
        self.pod_id = pod_id
        self.cache = PrefixCache(cache_capacity_pages)
        self.queue: deque[QueuedWork] = deque()
        self.queued_estimated_service_ms = 0.0
        self.running_request_id: int | None = None
        self.running_start_ms: float | None = None
        self.running_done_ms: float | None = None
        self.running_service_ms = 0.0
        self.busy_time_ms = 0.0
        self.load_area_ms2 = 0.0
        self.max_load_ms = 0.0
        self.max_queue_length = 0
        self.active_transfer_id: int | None = None
        self.endpoint_busy_time_ms = 0.0
        self.remote_pressure: dict[int, float] = {}
        self.remote_pressure_area_ms2 = 0.0
        self.peak_remote_pressure_ms = 0.0

    @property
    def transfer_busy(self) -> bool:
        return self.active_transfer_id is not None

    @property
    def idle(self) -> bool:
        return self.running_request_id is None

    def running_remaining_service_ms(self, now_ms: float) -> float:
        if self.running_done_ms is None:
            return 0.0
        return max(0.0, self.running_done_ms - now_ms)

    def gpu_load_ms(self, now_ms: float) -> float:
        return self.running_remaining_service_ms(now_ms) + self.queued_estimated_service_ms

    @property
    def committed_remote_pressure_ms(self) -> float:
        return sum(self.remote_pressure.values())

    def load_ms(self, now_ms: float) -> float:
        return self.gpu_load_ms(now_ms) + self.committed_remote_pressure_ms

    def add_remote_pressure(self, ticket_id: int, estimate_ms: float) -> None:
        if ticket_id in self.remote_pressure:
            raise AssertionError("duplicate RemotePressure ownership")
        self.remote_pressure[ticket_id] = estimate_ms
        self.peak_remote_pressure_ms = max(
            self.peak_remote_pressure_ms, self.committed_remote_pressure_ms
        )

    def remove_remote_pressure(self, ticket_id: int) -> float:
        if ticket_id not in self.remote_pressure:
            raise AssertionError("missing RemotePressure ownership")
        return self.remote_pressure.pop(ticket_id)

    def observe_load(self, now_ms: float) -> None:
        self.max_load_ms = max(self.max_load_ms, self.load_ms(now_ms))

    def integrate_load(self, start_ms: float, end_ms: float) -> None:
        if end_ms < start_ms:
            raise AssertionError("time moved backwards")
        if end_ms == start_ms:
            return
        start_load = self.load_ms(start_ms)
        end_load = self.load_ms(end_ms)
        self.load_area_ms2 += (start_load + end_load) * (end_ms - start_ms) / 2.0
        self.remote_pressure_area_ms2 += self.committed_remote_pressure_ms * (end_ms - start_ms)

    def enqueue(self, request_id: int, estimated_service_ms: float) -> None:
        if estimated_service_ms <= 0:
            raise ValueError("queued estimated service must be positive")
        self.queue.append(QueuedWork(request_id, estimated_service_ms))
        self.queued_estimated_service_ms += estimated_service_ms
        self.max_queue_length = max(self.max_queue_length, len(self.queue))

    def take_next(self) -> QueuedWork | None:
        if not self.idle or not self.queue:
            return None
        work = self.queue.popleft()
        self.queued_estimated_service_ms -= work.estimated_service_ms
        if not self.queue:
            self.queued_estimated_service_ms = 0.0
        if self.queued_estimated_service_ms < 0:
            raise AssertionError("negative queued estimated load")
        return work

    def start_running(self, request_id: int, now_ms: float,
                      actual_service_ms: float) -> None:
        if not self.idle:
            raise AssertionError("Pod already has a running request")
        if actual_service_ms <= 0:
            raise ValueError("actual service must be positive")
        self.running_request_id = request_id
        self.running_start_ms = now_ms
        self.running_service_ms = actual_service_ms
        self.running_done_ms = now_ms + actual_service_ms
        self.busy_time_ms += actual_service_ms

    def finish(self, request_id: int, now_ms: float) -> None:
        if self.running_request_id != request_id:
            raise AssertionError("SERVICE_DONE does not match running request")
        if self.running_done_ms is None or abs(self.running_done_ms - now_ms) > 1e-6:
            raise AssertionError("SERVICE_DONE time does not match running service")
        self.running_request_id = None
        self.running_start_ms = None
        self.running_done_ms = None
        self.running_service_ms = 0.0
