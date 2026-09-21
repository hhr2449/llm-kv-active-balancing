from __future__ import annotations

from ..engine import TaskMainEngine, _Assignment
from ..records import EventRecord
from .candidates import StageD1CandidateUniverse


class StageD1Engine(TaskMainEngine):
    """Stage C engine with the isolated R_LEAST auxiliary routing branch."""

    def __init__(self, config):
        super().__init__(config)
        self.universe = StageD1CandidateUniverse()

    def _arrive(self, request, now):
        if self.config.routing_policy != "R_LEAST":
            return super()._arrive(request, now)
        self._events.append(EventRecord(now, "REQUEST_ARRIVAL", request.request_id))
        self.universe.observe(request)
        self.demand_history.observe(request)
        loads = self.load.vector(now)
        pod = min(range(len(loads)), key=lambda item: (loads[item], item))
        hit_pages = self.caches[pod].lookup(request.block_ids)
        assignment = _Assignment(request, pod, pod, hit_pages, hit_pages, loads, now)
        miss_tokens = request.input_tokens-request.hit_tokens(hit_pages)
        self.load.commit(request.request_id, pod, miss_tokens, now)
        self._events.append(EventRecord(now, "FINAL_ASSIGNMENT", request.request_id))
        self._finish(assignment, now)
