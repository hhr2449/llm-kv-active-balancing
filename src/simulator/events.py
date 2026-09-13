from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class EventType(str, Enum):
    REQUEST_ARRIVAL = "REQUEST_ARRIVAL"
    SERVICE_START = "SERVICE_START"
    SERVICE_DONE = "SERVICE_DONE"
    TRANSFER_ADMIT = "TRANSFER_ADMIT"
    TRANSFER_COMPLETE = "TRANSFER_COMPLETE"
    TRANSFER_ENQUEUE = "TRANSFER_ENQUEUE"
    TRANSFER_TIMEOUT = "TRANSFER_TIMEOUT"
    TRIGGER_TICK = "TRIGGER_TICK"
    PROACTIVE_TRANSFER_COMPLETE = "PROACTIVE_TRANSFER_COMPLETE"


class RequestState(str, Enum):
    ARRIVED = "ARRIVED"
    WAITING_TRANSFER = "WAITING_TRANSFER"
    READY = "READY"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FALLBACK = "FALLBACK"


EVENT_PRIORITY = {
    EventType.SERVICE_DONE: 0,
    EventType.TRANSFER_COMPLETE: 0,
    EventType.TRANSFER_TIMEOUT: 1,
    EventType.REQUEST_ARRIVAL: 2,
    EventType.TRANSFER_ENQUEUE: 3,
    EventType.TRANSFER_ADMIT: 3,
    EventType.SERVICE_START: 4,
    EventType.TRIGGER_TICK: 5,
    EventType.PROACTIVE_TRANSFER_COMPLETE: 0,
}


@dataclass(order=True, frozen=True)
class Event:
    time_ms: float
    priority: int
    event_id: int
    event_type: EventType = field(compare=False)
    request_id: int = field(compare=False)

    @classmethod
    def create(
        cls, time_ms: float, event_id: int, event_type: EventType, request_id: int
    ) -> "Event":
        return cls(time_ms, EVENT_PRIORITY[event_type], event_id, event_type, request_id)
