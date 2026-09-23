"""Isolated Stage 2 event loop: frozen Stage 0 ordering, arrival-based route switch.

The loop is deliberately local so Stage 0/1 source and existing artifacts remain
frozen. Checkpoints occur only after a complete timestamp batch, including ready
batches, pin protection and all same-time arrivals.
"""
from collections import Counter
from dataclasses import asdict
import copy
import heapq
import math
from types import SimpleNamespace

from ..stage0.engine import CapacityStudyEngine, DiagnosticController, DiagnosticTransfers
from ..records import EventRecord, RunResult
from ..trace import validate_requests
from ..history import FutureDemandIndex
from .utilization import finalize_metrics
from ..metrics import build_summary, target_feasibility_summary
from ..equivalence import projection_digest, logical_state_projection
from ..validation import validate_metrics
from .cache import IndexedCache
from .index import IndexedDemand, IndexedUniverse, IndexedPolicy, static_metadata
from .ledger import DecisionLedger, StreamingController


class IndexedTransfers(DiagnosticTransfers):
    def start(self, *args, **kwargs):
        result = super().start(*args, **kwargs)
        self.universe.inflight(result, 1)
        return result

    def complete(self, *args, **kwargs):
        result = super().complete(*args, **kwargs)
        self.universe.inflight(result, -1)
        return result


class Stage2Engine(CapacityStudyEngine):
    def __init__(self, config, metadata=None, future=None, audit_directory=None):
        super().__init__(config)
        self.metadata = metadata
        self.shared_future = future
        self.audit_directory = audit_directory
        self.demand_history = IndexedDemand()
        if metadata is not None:
            self.install_metadata(metadata)

    def install_metadata(self, metadata):
        self.metadata = metadata
        chains, chunks = metadata
        self.universe = IndexedUniverse(self.config.num_pods, chains)
        self.caches = [IndexedCache(self.config.capacity_pages, p, self.universe, chunks)
                       for p in range(self.config.num_pods)]
        self.transfers = IndexedTransfers(self.caches)
        self.transfers.universe = self.universe
        self.transfers.capacity_failures = self.capacity_failures

    def _with_route(self, arrival, method, *args, **kwargs):
        original = self.config
        effective = copy.copy(original)
        object.__setattr__(effective, 'routing_policy', original.route_at(arrival))
        self.config = effective
        try:
            return method(*args, **kwargs)
        finally:
            self.config = original

    def _arrive(self, request, now):
        return self._with_route(request.arrival_ms, super()._arrive, request, now)

    def _finish(self, assignment, now, already_protected=False):
        return self._with_route(assignment.request.arrival_ms, super()._finish,
                               assignment, now, already_protected)

    def run(self, requests, *, oracle_observation_requests=None,
            progress_callback=None, progress_every_requests=0,
            checkpoint_callback=None, resume=False):
        if not self._has_run:
            requests = validate_requests(requests)
            self._has_run = True
            self._input_count = len(requests)
            if self.metadata is None:
                self.install_metadata(static_metadata(requests))
            if self.config.proactive_policy == 'FUTURE_DEMAND':
                self.future_index = self.shared_future or FutureDemandIndex(requests, self.config.visibility_end_ms)
            if self.config.proactive_policy != 'NONE':
                controller = StreamingController if self.audit_directory else DiagnosticController
                self.proactive = controller(self.config, self.universe,
                    IndexedPolicy(self.config, self.demand_history, self.reuse_history,
                                  self.future_index, self.universe, requests), self.caches, self.transfers)
                if self.audit_directory:
                    self.proactive.decisions = DecisionLedger(self.audit_directory)
            self._cursor = 0
            self._now = 0.
        elif not resume:
            raise ValueError('already started; resume a verified checkpoint, never restart')
        if len(requests) != self._input_count:
            raise ValueError('checkpoint input count mismatch')
        index, now = self._cursor, self._now
        while index<len(requests) or self._ready:
            now=min(requests[index].arrival_ms if index<len(requests) else math.inf,
                    self._ready[0][0] if self._ready else math.inf)
            ready_requests=[]
            while self._ready and self._ready[0][0]==now:
                _,request_id,transfer_id=heapq.heappop(self._ready)
                transfer=self.transfers.complete(transfer_id,now)
                self._events.append(EventRecord(now,'TRANSFER_COMPLETE',request_id,transfer_id))
                if transfer.type=='PROACTIVE':
                    self.proactive.complete(transfer)
                    self.copy_observations.append(dict(transfer_id=transfer_id,target=transfer.target,
                        ready_time=now,chain_id=transfer.transferable_chain,
                        generations=tuple(self.caches[transfer.target].page_state(b)['generation'] for b in transfer.transferable_chain)))
                else:ready_requests.append(self._pending.pop(request_id))
            ready_requests.sort(key=lambda a:a.request.request_id)
            for a in ready_requests:self.caches[a.final_pod].pin(a.request.block_ids[:a.final_hit_pages])
            for a in ready_requests:self._finish(a,now,already_protected=True)
            while index<len(requests) and requests[index].arrival_ms==now:
                with self.future_access.decision(self.config.proactive_policy):self._arrive(requests[index],now)
                index+=1
                if progress_callback and progress_every_requests and (index%progress_every_requests==0 or index==len(requests)):
                    progress_callback(index,len(requests),now)
            self._cursor, self._now = index, now
            if checkpoint_callback:
                checkpoint_callback(self, index, now)
        for cache in self.caches:
            cache.full_check()
        assert not self._pending
        self._requests.sort(key=lambda r:r.request_id)
        # Existing validation compares peak <= capacity. Mathematical infinity is
        # only an internal bound; serialized config retains explicit mode + null.
        validation_config=SimpleNamespace(**self.config.as_dict())
        validation_config.capacity_pages=math.inf if self.config.capacity_pages is None else self.config.capacity_pages
        validation_config.as_dict=self.config.as_dict
        summary,validation=build_summary(validation_config,self._requests,self.transfers.records,
            self._opportunities,self.caches,self.load,{r.request_id for r in requests},self._events)
        actions=self.proactive.actions if self.proactive else []
        decisions=self.proactive.decisions if self.proactive else []
        state=dict(cache=[c.snapshot() for c in self.caches],load_vector=self.load.vector(now),
                   load_entries=[asdict(self.load.entries[k]) for k in sorted(self.load.entries)],
                   transfers=[asdict(t) for t in self.transfers.records],pending_requests=[],ready_events=[],
                   active_load_histories=[[asdict(e) for e in h] for h in self.load._histories],load_last_time=self.load._last_time,
                   candidate_universe=sorted(self.universe.seen),
                   demand_history=sorted(self.demand_history.events.items()),demand_request_ids=sorted(self.demand_history.request_ids),
                   reuse_history=sorted(self.reuse_history.events.items()),reuse_request_ids=sorted(self.reuse_history.request_ids))
        summary['final_state_digest']=projection_digest(state)
        summary['proactive_copy_count']=len(actions)
        summary['policy_diagnostics']=dict(policy=self.config.proactive_policy,future_access=self.future_access.snapshot(),
                                           cost_gate_rejected_count=sum(d.status=='COST_GATE_REJECTED' for d in decisions))
        summary['target_feasibility_diagnostics']['PROACTIVE']=target_feasibility_summary(decisions)
        result=RunResult(self._requests,self.transfers.records,self._opportunities,self._events,summary,validation,actions,decisions,state)
        result.copy_observation_records=self.copy_observations
        result.reuse_observation_records=[dict(asdict(e),target=p) for p,c in enumerate(self.caches) for e in c.reuse_events]
        summary['final_metrics']=finalize_metrics(result,self.config)
        summary['final_logical_state_digest']=projection_digest(logical_state_projection(result))
        # Canonical Line 5/7 equivalence assumes every score > 0. Capacity studies
        # instead validate the gate's actual decisions, including legal rejection.
        metric_config=SimpleNamespace(**self.config.as_dict())
        if metric_config.proactive_policy=='PERSISTENCE_COST_AWARE':metric_config.proactive_policy='NONE'
        validation['checks'].update(validate_metrics(result,metric_config))
        validation['checks']['proactive_action_cap']=len({a.opportunity_id for a in actions})==len(actions)
        validation['checks']['proactive_actions_complete']=all(a.status=='COMPLETED' for a in actions)
        validation['checks']['proactive_action_transfer_conservation']=({a.transfer_id for a in actions}=={t.transfer_id for t in self.transfers.records if t.type=='PROACTIVE'})
        validation['checks']['cost_gate_truthful']=all(d.cost_score is None or (
            math.isclose(d.cost_score,d.benefit-.5*d.congestion-.1*d.transfer_cost_seconds,rel_tol=1e-14,abs_tol=1e-14)
            and (d.status!='COST_GATE_REJECTED' or d.cost_score<=0)
            and (d.status not in {'SELECTED','SKIP_NO_CAPACITY'} or d.cost_score>0)) for d in decisions)
        # Stage 2 phase diagnostics are built by the runner, without materializing
        # every historical shortlisted decision into a second in-memory list.
        summary['provenance'].update(study_version=self.config.study_version,
            saved_semantics='actual_final_target_longest_prefix_at_execution',
            reactive_load_semantics='arrival-time provisional guaranteed miss, reconciled at actual ready; original timestamp retained')
        validation['checks']['stage0_one_request_per_input']=len(result.request_records)==len(requests)
        validation['checks']['stage0_capacity_accounting']=all(c.peak_memory_pages<=c.capacity_pages for c in self.caches)
        validation['checks']['stage0_saved_load_consistent']=all(self.load.entries[r.request_id].miss_tokens==r.miss_tokens for r in result.request_records)
        validation['status']='PASS' if all(validation['checks'].values()) else 'INVALID'
        assert validation['status']=='PASS',validation
        return result
