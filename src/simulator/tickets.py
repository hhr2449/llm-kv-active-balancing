from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class TicketState(str, Enum):
    QUEUED = "QUEUED"
    ADMITTED = "ADMITTED"
    TRANSFER_RUNNING = "TRANSFER_RUNNING"
    COMPLETED = "COMPLETED"
    REPLANNED = "REPLANNED"
    FALLBACK = "FALLBACK"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"


@dataclass
class ReactiveTransferTicket:
    ticket_id: int
    request_id: int
    enqueue_time_ms: float
    deadline_ms: float
    source_pod: int
    target_pod: int
    source_hit_pages: int
    source_hit_tokens: int
    transferable_pages: int
    wire_pages: int
    planned_post_transfer_hit_pages: int
    planned_post_transfer_hit_tokens: int
    source_load_snapshot: float
    target_load_snapshot: float
    remote_pressure_owner_pod: int
    target_estimated_service_ms: float
    stable_sequence_id: int
    state: TicketState = TicketState.QUEUED
    initial_queue_position: int = 0
    endpoint_blocked: bool = False
    capacity_blocked: bool = False
    proactive_block_recorded: bool = False


class ReactiveNodeGate:
    def __init__(self) -> None:
        self._tickets: list[ReactiveTransferTicket] = []

    def enqueue(self, ticket: ReactiveTransferTicket) -> None:
        if ticket.state is not TicketState.QUEUED:
            raise AssertionError("only QUEUED tickets can enter NodeGate")
        ticket.initial_queue_position = len(self._tickets)
        self._tickets.append(ticket)
        self._tickets.sort(key=lambda t: (t.enqueue_time_ms, t.ticket_id))

    def queued(self) -> list[ReactiveTransferTicket]:
        return [t for t in self._tickets if t.state is TicketState.QUEUED]

    def remove(self, ticket: ReactiveTransferTicket) -> None:
        if ticket in self._tickets:
            self._tickets.remove(ticket)

    def earliest_endpoint_feasible(self, endpoint_free) -> list[ReactiveTransferTicket]:
        return [t for t in self.queued() if endpoint_free(t.source_pod, t.target_pod)]
