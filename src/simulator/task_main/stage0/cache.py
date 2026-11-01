import math
from ..cache import TaskMainCache


class DiagnosticCache(TaskMainCache):
    """Exact mutation-event occupancy ledger; probes do not append samples."""

    def __init__(self, capacity_pages):
        super().__init__(0 if capacity_pages is None else capacity_pages)
        if capacity_pages is None:
            self.capacity_pages = math.inf
        self.eviction_cause='request'
        self.ordinary_evictions=[]
        self.occupancy=[dict(time_ms=0., published=0, temporary=0, physical=0, pinned=0)]

    def sample(self):
        return dict(published=self.resident_pages,temporary=self.temporary_pages,
                    physical=self.memory_pages,pinned=self.pinned_pages,
                    free=None if math.isinf(self.capacity_pages) else self.capacity_pages-self.memory_pages)

    def _record(self, now):
        self.occupancy.append(dict(time_ms=now, **self.sample()))

    def _evict(self, plan, now):
        super()._evict(plan,now)
        if plan.evictions:
            self.ordinary_evictions.append(dict(time_ms=now,cause=self.eviction_cause,
                                               pages=len(plan.evictions),blocks=plan.evictions))

    def publish_prompt(self, plan, now):
        value=super().publish_prompt(plan,now);self._record(now);return value

    def reserve_temporary(self, transfer_id, plan, now):
        value=super().reserve_temporary(transfer_id,plan,now);self._record(now);return value

    def complete_transfer(self, transfer_id, path, now):
        before=self.memory_pages;wire=self._temporary[transfer_id]
        value=super().complete_transfer(transfer_id,path,now)
        assert self.memory_pages == before-wire+value
        self._record(now);return value

    def summary(self):
        # Canonical summary caches serialization as an optimization. Here even
        # memoization is restored so diagnostics leave the entire object unchanged.
        old=(self._summary_cache_version,self._summary_cache)
        value=super().summary()
        self._summary_cache_version,self._summary_cache=old
        if math.isinf(self.capacity_pages):value['capacity_pages']=None
        return value

    def snapshot(self):
        value=super().snapshot()
        if math.isinf(self.capacity_pages):value['capacity_pages']=None
        return value

    def failure_reason(self, path, kind):
        pages=len(path) if not isinstance(path,int) else path
        if not math.isinf(self.capacity_pages) and pages>self.capacity_pages:
            return 'full_path_over_capacity' if kind=='prompt' else 'chain_larger_than_capacity'
        plan=self.plan_prompt(path) if kind=='prompt' else self.plan_temporary(pages)
        return 'insufficient_evictable_capacity' if plan is None else None
