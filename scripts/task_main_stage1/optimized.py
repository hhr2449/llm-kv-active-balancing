"""Equivalent Stage 0 execution; only derived summaries/preflight scratch are optimized."""
from src.simulator.task_main.cache import AdmissionPlan
from src.simulator.task_main.stage0.cache import DiagnosticCache
from src.simulator.task_main.stage0.engine import CapacityStudyEngine


class MemoizedDiagnosticCache(DiagnosticCache):
    def __init__(self, capacity):
        super().__init__(capacity)
        self._eager_version = self._version
        self._eager_summary = super().summary()

    def _plan(self, additional, protected, kind, path=()):
        # With no capacity deficit, canonical scratch heap cannot evict anything.
        if self.memory_pages + additional <= self.capacity_pages:
            return AdmissionPlan(self._version, kind, path, additional, ())
        return super()._plan(additional, protected, kind, path)

    def unpin(self, path):
        super().unpin(path)
        # Update derived memo only during a real mutation, never during a query.
        self._eager_summary = super().summary()
        self._eager_version = self._version

    def summary(self):
        if self._eager_version == self._version:
            return dict(self._eager_summary)
        return super().summary()


class OptimizedStudyEngine(CapacityStudyEngine):
    def __init__(self, config):
        super().__init__(config)
        self.caches = [MemoizedDiagnosticCache(config.capacity_pages) for _ in range(config.num_pods)]
        self.transfers.caches = self.caches
