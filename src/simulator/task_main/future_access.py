"""Count actual future API accesses within a decision, independently of index loading."""
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, asdict

_CURRENT = ContextVar("task_main_decision_future_audit", default=None)


@dataclass
class FutureAccessAudit:
    oracle_decision_future_reads: int = 0
    history_decision_future_reads: int = 0

    @contextmanager
    def decision(self, policy):
        token = _CURRENT.set((self, policy))
        try:
            yield
        finally:
            _CURRENT.reset(token)

    def snapshot(self):
        return dict(asdict(self), decision_future_reads=(self.oracle_decision_future_reads +
                                                       self.history_decision_future_reads))


def record_future_api_read():
    context = _CURRENT.get()
    if context is not None:
        audit, policy = context
        if policy == "FUTURE_DEMAND":
            audit.oracle_decision_future_reads += 1
        else:
            audit.history_decision_future_reads += 1
