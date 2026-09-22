"""Read-only aggregation. All intervals half-open; no diagnostic calls change state."""
from collections import Counter

from ..final_metrics import gini, random_gini_baseline, request_metrics, ratio


def occupancy_metrics(events, start, end):
    state=dict(published=0,temporary=0,physical=0,pinned=0)
    area=0.;peak=0;cursor=start
    for row in events:
        t=row['time_ms']
        if t<=start:
            state=row
            if t==start:peak=max(peak,row['physical'])
            continue
        if t>=end:break
        area+=(t-cursor)*state['physical'];peak=max(peak,state['physical'],row['physical'])
        cursor=t;state=row
    area+=(end-cursor)*state['physical'];peak=max(peak,state['physical'])
    return dict(time_weighted_mean_occupancy_pages=area/(end-start) if end>start else None,
                peak_occupancy_pages=peak,end_occupancy_pages=state['physical'])


def build_diagnostics(result, engine):
    config=engine.config
    full_end=max(config.visibility_end_ms,max((e.time for e in result.event_records),default=0)+1)
    scopes=[('FULL_REPLAY',0,full_end),('EVALUATION',config.evaluation_start_ms,config.evaluation_end_ms)]
    output=dict(schema_version='TASK_MAIN_STAGE0_DIAGNOSTICS_V1',capacity_mode=config.capacity_mode,
                capacity_pages_per_pod=config.capacity_pages,active_runtime_kv='未建模',
                occupancy_definition='PublishedUnique + independent full-wire Temporary; pinned is a subset',
                saved_corrections=engine.saved_corrections,
                capacity_failures=engine.capacity_failures,
                request_capacity=engine.request_capacity,
                occupancy_trajectories={str(p):c.occupancy for p,c in enumerate(engine.caches)},scopes={})
    for scope,start,end in scopes:
        rs=[r for r in result.request_records if start<=r.arrival_time<end]
        ts=[t for t in result.transfer_records if start<=t.start_time<end]
        ops=[o for o in result.opportunity_records if start<=o.opportunity_time<end]
        op_ids={o.opportunity_id for o in ops}
        ds=[d for d in result.candidate_decision_records if d.opportunity_id in op_ids]
        failures=[f for f in engine.capacity_failures if start<=f['time_ms']<end]
        pods=[]
        for p,c in enumerate(engine.caches):
            pr=[r for r in rs if r.final_pod==p]
            ev=[e for e in c.ordinary_evictions if start<=e['time_ms']<end]
            pf=[f for f in failures if f['pod']==p]
            entry=dict(pod=p,request_count=len(pr),request_share=ratio(len(pr),len(rs)),
                       miss_prefill_tokens=sum(r.miss_tokens for r in pr),
                       ordinary_eviction_events=len(ev),ordinary_evicted_pages=sum(e['pages'] for e in ev),
                       capacity_rejects_by_type_reason=dict(Counter(f['type']+':'+f['reason'] for f in pf)),
                       **occupancy_metrics(c.occupancy,start,end))
            entry['evictions_by_cause']={cause:dict(events=sum(e['cause']==cause for e in ev),pages=sum(e['pages'] for e in ev if e['cause']==cause))
                                         for cause in ['request','reactive_transfer','proactive_transfer']}
            entry['wire']={kind:dict(sent_bytes=sum(t.wire_bytes for t in ts if t.source==p and t.type==kind),
                                     received_bytes=sum(t.wire_bytes for t in ts if t.target==p and t.type==kind))
                           for kind in ['REACTIVE','PROACTIVE']}
            pods.append(entry)
        counts=[p['request_count'] for p in pods];miss=[p['miss_prefill_tokens'] for p in pods]
        rand,p95=random_gini_baseline(config.workload,start,end,config.num_pods,len(rs),config.random_seed,config.random_gini_repetitions)
        cluster=request_metrics(rs,start,end)
        cluster.update(top1_request_share=ratio(max(counts),len(rs)),active_pod_count=sum(n>0 for n in counts),
                       raw_request_gini=gini(counts),random_gini_mean=rand,random_gini_p95=p95,skew_ratio=ratio(gini(counts),rand),
                       total_miss_prefill_tokens=sum(miss),max_pod_miss_prefill_tokens=max(miss),
                       cache_mean_occupancy_gini=gini([p['time_weighted_mean_occupancy_pages'] or 0 for p in pods]),
                       cache_end_occupancy_gini=gini([p['end_occupancy_pages'] for p in pods]))
        reactive=[t for t in ts if t.type=='REACTIVE'];proactive=[t for t in ts if t.type=='PROACTIVE']
        evaluated=[d for d in ds if d.cost_score is not None and d.status not in {'NOT_ATTEMPTED','NOT_ATTEMPTED_CAP_REACHED'}]
        scores=[d for d in ds if d.cost_score is not None]
        request_ids={r.request_id for r in rs}
        cap=[r for r in engine.request_capacity if r['request_id'] in request_ids]
        output['scopes'][scope]=dict(interval_ms=[start,end],per_pod=pods,cluster=cluster,
            capacity=dict(full_path_over_capacity=sum(r['full_path_over_capacity'] for r in cap),
                          chain_larger_than_capacity=sum(f['reason']=='chain_larger_than_capacity' for f in failures),
                          insufficient_evictable_capacity=sum(f['reason']=='insufficient_evictable_capacity' for f in failures),
                          request_skipped_due_to_capacity=0,request_truncated_due_to_capacity=0,
                          cache_admission_rejected=sum(r.cache_admission_skip for r in rs)),
            reactive=dict(overload_gate_evaluated=sum(r.routing_policy=='R_REQ_KV_TASK' and config.num_pods>1 for r in rs),
                          overload_gate_pass=sum(r.reactive_gate_passed for r in rs),
                          direct_target=sum(r.routing_policy=='R_REQ_KV_TASK' and r.final_pod!=r.affinity_source and r.reactive_transfer_id is None for r in rs),
                          copy_started=len(reactive),copy_ready=sum(t.status=='COMPLETED' for t in reactive),
                          copy_failed=sum(t.status=='FAILED' for t in reactive),
                          fallback_reasons=dict(Counter(r.reactive_fallback_reason for r in rs if r.reactive_fallback_reason))),
            proactive=dict(opportunity=len(ops),candidate=sum(o.structural_candidate_count for o in ops),
                           positive_candidate=sum(o.positive_candidate_count for o in ops),shortlist=sum(o.shortlist_count for o in ops),
                           attempt=sum(o.attempted_candidate_count for o in ops),started=len(proactive),
                           ready=sum(t.status=='COMPLETED' for t in proactive),failed=sum(t.status=='FAILED' for t in proactive),
                           admission_failed_attempt=sum(d.status=='SKIP_NO_CAPACITY' for d in ds),
                           inflight_at_evaluation_start=sum(t.type=='PROACTIVE' and t.start_time<config.evaluation_start_ms<=t.ready_time for t in result.transfer_records),
                           inflight_at_evaluation_end=sum(t.type=='PROACTIVE' and t.start_time<config.evaluation_end_ms<=t.ready_time for t in result.transfer_records)),
            cost_aware=dict(score_gate_evaluated=len(evaluated),score_gate_pass=sum(d.cost_score>0 for d in evaluated),
                            score_gate_reject=sum(d.cost_score<=0 for d in evaluated),
                            shortlist_score_computed=len(scores),shortlist_score_positive=sum(d.cost_score>0 for d in scores)))
    return output
