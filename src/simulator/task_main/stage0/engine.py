from collections import Counter
from dataclasses import asdict
import heapq
from types import SimpleNamespace
import math
from dataclasses import replace

from ..engine import TaskMainEngine, _Assignment
from ..proactive import ProactiveController
from ..records import EventRecord, RunResult
from ..trace import validate_requests
from ..history import FutureDemandIndex
from ..policies import Policy
from ..final_metrics import finalize_metrics
from ..metrics import build_summary, target_feasibility_summary
from ..equivalence import projection_digest, logical_state_projection
from ..validation import validate_metrics
from ..routing import affinity_route, least_other_pod, reactive_gate
from ..trace import full_page_prefix
from ..transfer import IndependentTransfers
from .cache import DiagnosticCache
from .load import ObservationalLoad


class DiagnosticController(ProactiveController):
    def execute(self, opportunity):
        self.transfers.decision_context = (opportunity.opportunity_time, opportunity.request_id)
        try:
            return super().execute(opportunity)
        finally:
            self.transfers.decision_context = None


class DiagnosticTransfers(IndependentTransfers):
    decision_context = None

    def preflight_chain(self, path, source, target):
        result = super().preflight_chain(path, source, target)
        if self.decision_context is not None and result.failure_reason == 'REACTIVE_FALLBACK_CAPACITY':
            now, request_id = self.decision_context
            self.capacity_failures.append(dict(time_ms=now,pod=target,type='proactive_target_preflight',
                                               reason=self.caches[target].failure_reason(len(path),'transfer'),
                                               request_id=request_id))
        return result

    def start(self, plan, request_id, now, transfer_type='REACTIVE'):
        target=self.caches[plan.target]
        old=target.eviction_cause
        target.eviction_cause=transfer_type.lower()+'_transfer'
        try:
            return super().start(plan,request_id,now,transfer_type)
        finally:
            target.eviction_cause=old


class CapacityStudyEngine(TaskMainEngine):
    """Stage 0 semantic revision: actual ready hit, pure queries, unified diagnostics."""
    proactive_controller_class = DiagnosticController

    def __init__(self, config):
        # Initialize only the base machinery with a serialization-safe adapter.
        # Actual study capacity is installed below; canonical config remains frozen.
        base = SimpleNamespace(**config.as_dict())
        base.capacity_pages = 0 if config.capacity_pages is None else config.capacity_pages
        super().__init__(base)
        self.config = config
        self.caches=[DiagnosticCache(config.capacity_pages) for _ in range(config.num_pods)]
        self.load=ObservationalLoad(config.num_pods)
        self.transfers=DiagnosticTransfers(self.caches)
        self.capacity_failures=[]
        self.transfers.capacity_failures=self.capacity_failures
        self.request_capacity=[]
        self.saved_corrections=[]

    def _arrive(self, request, now):
        if self.config.routing_policy!='R_LEAST':
            return super()._arrive(request,now)
        self._events.append(EventRecord(now,'REQUEST_ARRIVAL',request.request_id))
        self.universe.observe(request);self.demand_history.observe(request)
        loads=self.load.vector(now)
        pod=min(range(len(loads)),key=lambda p:(loads[p],p))
        hit=self.caches[pod].lookup(request.block_ids)
        assignment=_Assignment(request,pod,pod,hit,hit,loads,now)
        self.load.commit(request.request_id,pod,request.input_tokens-request.hit_tokens(hit),now)
        self._events.append(EventRecord(now,'FINAL_ASSIGNMENT',request.request_id))
        self._finish(assignment,now)

    def _reactive_assignment(self, request, source, hit_pages, loads, now):
        target=least_other_pod(source,loads)
        path=full_page_prefix(request,hit_pages)
        if target is not None and reactive_gate(loads[source],loads[target],self.config.theta) and path:
            for p,cache in enumerate(self.caches):
                if p==source or cache.lookup(path)==len(path):continue
                reason=cache.failure_reason(len(path),'transfer')
                if reason:self.capacity_failures.append(dict(time_ms=now,pod=p,type='reactive_target_preflight',reason=reason,request_id=request.request_id))
        return super()._reactive_assignment(request,source,hit_pages,loads,now)

    def _finish(self, assignment, now, already_protected=False):
        cache=self.caches[assignment.final_pod];request=assignment.request
        actual=cache.lookup(request.block_ids)
        if actual!=assignment.final_hit_pages:
            self.saved_corrections.append(dict(request_id=request.request_id,time_ms=now,
                                               committed_hit_pages=assignment.final_hit_pages,actual_hit_pages=actual))
            if already_protected:
                cache.pin(request.block_ids[:actual])
                cache.unpin(request.block_ids[:assignment.final_hit_pages])
            assignment=replace(assignment,final_hit_pages=actual)
            self.load.reconcile_hit(request.request_id,request.input_tokens-request.hit_tokens(actual))
        reason=cache.failure_reason(request.block_ids,'prompt')
        self.request_capacity.append(dict(request_id=request.request_id,pod=assignment.final_pod,
                                         time_ms=now,full_path_over_capacity=(cache.capacity_pages is not None and len(request.block_ids)>cache.capacity_pages),
                                         cache_admission_failure=reason))
        if reason:self.capacity_failures.append(dict(time_ms=now,pod=assignment.final_pod,type='request_admission',reason=reason,request_id=request.request_id))
        value=super()._finish(assignment,now,already_protected)
        # Exact physical samples exist at every occupancy mutation; this extra
        # request boundary records pins after release without touching LRU/Load.
        cache._record(now)
        return value

    def run(self, requests, *, oracle_observation_requests=None,
            progress_callback=None, progress_every_requests=0):
        # This loop preserves canonical ready-before-arrival and stable ready-batch
        # ordering. The versioned study owns its validation/provenance boundary.
        if self._has_run:raise ValueError('create a new engine per replay')
        requests=validate_requests(requests);self._has_run=True
        if self.config.proactive_policy=='FUTURE_DEMAND':
            observation=requests if oracle_observation_requests is None else validate_requests(oracle_observation_requests)
            if oracle_observation_requests is not None:
                if any(r.arrival_ms>self.config.visibility_end_ms for r in observation):
                    raise ValueError('Oracle observation exceeds visibility')
                if [r for r in observation if r.arrival_ms<self.config.visibility_end_ms]!=requests:
                    raise ValueError('Oracle observation must equal replay plus boundary-only requests')
            self.future_index=FutureDemandIndex(observation,self.config.visibility_end_ms)
        if self.config.proactive_policy!='NONE':
            self.proactive=DiagnosticController(self.config,self.universe,
                Policy(self.config,self.demand_history,self.reuse_history,self.future_index),self.caches,self.transfers)
        index=0;now=0.
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
        from .diagnostics import build_diagnostics
        summary['stage0_diagnostics']=build_diagnostics(result,self)
        summary['provenance'].update(study_version=self.config.study_version,
            saved_semantics='actual_final_target_longest_prefix_at_execution',
            reactive_load_semantics='arrival-time provisional guaranteed miss, reconciled at actual ready; original timestamp retained')
        validation['checks']['stage0_one_request_per_input']=len(result.request_records)==len(requests)
        validation['checks']['stage0_capacity_accounting']=all(c.peak_memory_pages<=c.capacity_pages for c in self.caches)
        validation['checks']['stage0_saved_load_consistent']=all(self.load.entries[r.request_id].miss_tokens==r.miss_tokens for r in result.request_records)
        validation['status']='PASS' if all(validation['checks'].values()) else 'INVALID'
        assert validation['status']=='PASS',validation
        return result
