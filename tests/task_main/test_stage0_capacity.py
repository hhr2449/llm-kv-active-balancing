from copy import deepcopy
from dataclasses import asdict, replace
import hashlib
import json
import pickle

import pytest

from src.simulator.task_main.cache import TaskMainCache
from src.simulator.task_main.config import TaskMainConfig
from src.simulator.task_main.engine import TaskMainEngine
from src.simulator.task_main.routing import affinity_route
from src.simulator.task_main.stage0.cache import DiagnosticCache
from src.simulator.task_main.stage0.config import CapacityStudyConfig
from src.simulator.task_main.stage0.engine import CapacityStudyEngine, DiagnosticTransfers
from src.simulator.task_main.stage0.load import ObservationalLoad
from src.simulator.task_main.stage0.diagnostics import occupancy_metrics, build_diagnostics
from scripts.task_main_stage0.profile_workload import profile


def state_bytes(engine):
    # Include physical state, optimization caches, Load ledger and history indexes;
    # omit defaultdict's unpicklable factory, not any stored state.
    state = ([c.__dict__ for c in engine.caches], engine.load.__dict__,
             engine.universe.__dict__, dict(engine.demand_history.events),
             dict(engine.demand_history._bucket_times), engine.demand_history.request_ids,
             dict(engine.reuse_history.events), engine.reuse_history.request_ids,
             engine.transfers.records, engine._ready, engine._pending,
             engine.capacity_failures, engine.saved_corrections,
             engine.future_access.snapshot())
    return pickle.dumps(state)


def test_capacity_physical_accounting_pin_and_publish_conservation(publish):
    caches=[DiagnosticCache(4),DiagnosticCache(4)]
    publish(caches[0],(1,2));publish(caches[1],(1,))
    transfers=DiagnosticTransfers(caches)
    plan=transfers.preflight_chain((1,2),0,1).plan
    t=transfers.start(plan,0,1)
    assert caches[0].pinned_pages==2 and caches[0].memory_pages==2
    assert caches[1].memory_pages==3 and caches[1].temporary_pages==2
    transfers.complete(t.transfer_id,t.ready_time)
    assert caches[1].memory_pages==2 and t.newly_resident_pages+t.duplicate_pages==2
    assert caches[0].pinned_pages==0
    assert all(c.memory_pages<=c.capacity_pages for c in caches)


@pytest.mark.parametrize('mode,capacity',[('finite',1),('finite',0),('infinite',None)])
def test_over_capacity_requests_never_skipped_or_truncated(req,mode,capacity):
    engine=CapacityStudyEngine(CapacityStudyConfig(capacity_mode=mode,capacity_pages=capacity,num_pods=2))
    rs=[req(0,0,(1,2,3)),req(1,1,(1,2,3))]
    result=engine.run(rs)
    assert len(result.request_records)==2
    assert sum(r.input_tokens for r in result.request_records)==3072
    cap=result.summary['stage0_diagnostics']['scopes']['FULL_REPLAY']['capacity']
    assert cap['request_skipped_due_to_capacity']==cap['request_truncated_due_to_capacity']==0
    assert cap['full_path_over_capacity']==(0 if mode=='infinite' else 2)


def test_chain_reject_reason_and_no_partial_transfer(publish):
    source,target=DiagnosticCache(3),DiagnosticCache(2)
    publish(source,(1,2,3));network=DiagnosticTransfers([source,target])
    before=deepcopy(target.__dict__)
    assert target.failure_reason(3,'transfer')=='chain_larger_than_capacity'
    assert network.preflight_chain((1,2,3),0,1).plan is None
    assert target.__dict__==before and not network.records
    publish(target,(8,9));target.pin((8,9))
    assert target.failure_reason(1,'transfer')=='insufficient_evictable_capacity'
    assert network.preflight_chain((1,),0,1).plan is None


@pytest.mark.parametrize('chosen',range(16))
def test_n16_affinity_and_feasible_target_first_all_pods(req,publish,chosen):
    engine=CapacityStudyEngine(CapacityStudyConfig(num_pods=16,capacity_pages=1,routing_policy='R_REQ_KV_TASK'))
    source=(chosen+1)%16
    publish(engine.caches[source],(1,))
    for p,c in enumerate(engine.caches):
        if p not in {source,chosen}:
            publish(c,(100+p,));c.pin((100+p,))
    loads=tuple(100 if p==source else 0 for p in range(16))
    assignment=engine._reactive_assignment(req(path=(1,)),source,1,loads,1)
    assert assignment.final_pod==chosen
    assert assignment.structural_target_count==15 and assignment.feasible_target_count==1
    # Unique actual hit on each of the 16 Pod IDs wins over Load.
    fresh=[TaskMainCache(1) for _ in range(16)];publish(fresh[chosen],(1,))
    assert affinity_route(req(path=(1,)),fresh,tuple(range(16))).pod_id==chosen


def test_n16_least_all_pods_and_ties(req):
    e=CapacityStudyEngine(CapacityStudyConfig(routing_policy='R_LEAST',num_pods=16,capacity_pages=2))
    result=e.run([req(i,i,(100+i,)) for i in range(32)])
    assert [r.final_pod for r in result.request_records]==list(range(16))*2
    assert result.summary['stage0_diagnostics']['scopes']['FULL_REPLAY']['cluster']['active_pod_count']==16


def test_saved_manual_ready_actual_hit_and_load_correction(req):
    rs=[req(0,0,(1,2),600),req(1,1,(1,2),600),req(2,1.1,(1,2),600)]
    old=TaskMainEngine(TaskMainConfig(num_pods=2,capacity_pages=6,routing_policy='R_REQ_KV_TASK')).run(rs)
    engine=CapacityStudyEngine(CapacityStudyConfig(num_pods=2,capacity_pages=6,routing_policy='R_REQ_KV_TASK'))
    result=engine.run(rs)
    assert [r.final_hit_tokens for r in old.request_records]==[0,512,512]
    assert [r.final_hit_tokens for r in result.request_records]==[0,512,600]
    assert result.summary['final_hit_tokens']==1112
    assert sum(r.miss_tokens for r in result.request_records)==688
    assert len(engine.load.entries)==3 and engine.load.vector(2)==(600,88)
    assert len(result.summary['stage0_diagnostics']['saved_corrections'])==1


def test_all_queries_nonmutating_including_future_load_probe(req,publish):
    engine=CapacityStudyEngine(CapacityStudyConfig(num_pods=2,capacity_pages=3,proactive_policy='PERSISTENCE'))
    publish(engine.caches[0],(1,2))
    engine.universe.observe(req(path=(1,2)))
    engine.demand_history.observe(req(path=(1,2)))
    engine.load.commit(0,0,100,0)
    def digest():return hashlib.sha256(state_bytes(engine)).hexdigest()
    before=digest()
    for _ in range(3):
        engine.caches[0].lookup((1,2));engine.caches[0].summary();engine.caches[0].sample()
        engine.caches[1].plan_prompt((1,2));engine.transfers.preflight_chain((1,2),0,1)
        engine.universe.materialize(engine.caches,(100,0),engine.transfers.records)
        engine.demand_history.count((1,),5,60000)
        assert engine.load.vector(60001)==(0,0)
        assert engine.load.vector(1)==(100,0)
    assert digest()==before
    # A diagnostic future query cannot prevent a later chronological real commit.
    engine.load.commit(1,1,40,2)
    assert engine.load.vector(2)==(100,40)


@pytest.mark.parametrize('cause',['request','reactive_transfer','proactive_transfer'])
def test_eviction_attribution_events_vs_pages(publish,cause):
    source,target=DiagnosticCache(3),DiagnosticCache(2)
    publish(source,(1,2));publish(target,(8,9))
    if cause=='request':publish(target,(1,2),1)
    else:
        network=DiagnosticTransfers([source,target]);plan=network.preflight_chain((1,2),0,1).plan
        network.start(plan,0,1,'REACTIVE' if cause.startswith('reactive') else 'PROACTIVE')
    assert len(target.ordinary_evictions)==1
    assert target.ordinary_evictions[0]['cause']==cause and target.ordinary_evictions[0]['pages']==2
    assert occupancy_metrics(target.occupancy,0,2)['time_weighted_mean_occupancy_pages']==2


@pytest.mark.parametrize('policy',['NONE','PERSISTENCE','RECENCY','FUTURE_DEMAND','PERSISTENCE_COST_AWARE'])
@pytest.mark.parametrize('mode',['finite','infinite'])
def test_every_policy_uniform_diagnostics_and_determinism(req,policy,mode):
    cfg=CapacityStudyConfig(num_pods=16,capacity_pages=None if mode=='infinite' else 4,
                            capacity_mode=mode,proactive_policy=policy)
    rs=[req(0,1500000,(1,2)),req(1,1500001,(1,2)),req(2,1500002,(3,4)),req(3,1500010,(1,2))]
    a=CapacityStudyEngine(cfg);first=a.run(rs);second=CapacityStudyEngine(cfg).run(rs)
    assert first.validation['status']=='PASS'
    assert json.dumps(first.summary,sort_keys=True)==json.dumps(second.summary,sort_keys=True)
    diagnostics=first.summary['stage0_diagnostics']['scopes']['EVALUATION']
    assert len(diagnostics['per_pod'])==16 and 'ordinary_evicted_pages' in diagnostics['per_pod'][0]
    if mode=='infinite':
        assert all(not c.eviction_records for c in a.caches)
        assert not a.capacity_failures
    if policy=='PERSISTENCE_COST_AWARE':
        assert diagnostics['reactive']['overload_gate_pass']==0
        assert diagnostics['cost_aware']['score_gate_pass']>0
    before=state_bytes(a)
    build_diagnostics(first,a)
    assert state_bytes(a)==before


def test_infinite_independent_pods_same_routing_and_transfer_timing(req):
    rows=[req(0,0,(1,2),600),req(1,1,(1,2),600),req(2,2,(8,9))]
    finite=CapacityStudyEngine(CapacityStudyConfig(num_pods=2,capacity_pages=20,routing_policy='R_REQ_KV_TASK')).run(rows)
    infinite=CapacityStudyEngine(CapacityStudyConfig(num_pods=2,capacity_mode='infinite',capacity_pages=None,routing_policy='R_REQ_KV_TASK')).run(rows)
    assert finite.request_records==infinite.request_records and finite.transfer_records==infinite.transfer_records
    assert infinite.final_state['cache'][0]['pages']!=infinite.final_state['cache'][1]['pages']


def test_infinite_config_explicit_and_canonical_matrix_not_relaxed(tmp_path):
    path=tmp_path/'infinite.yaml'
    path.write_text('study_version: TASK_MAIN_CAPACITY_STAGE0_V1\ncapacity_mode: infinite\ncapacity_pages: null\nnum_pods: 16\n')
    assert CapacityStudyConfig.from_yaml(path).capacity_pages is None
    with pytest.raises(ValueError):CapacityStudyConfig(capacity_mode='infinite')
    with pytest.raises(ValueError):TaskMainConfig.from_yaml(path)


def test_profiler_path_identity_window_reuse_and_buckets(req):
    rs=[req(0,0,(1,2)),req(1,1000,(1,3)),req(2,61000,(1,2))]
    p=profile(rs,'synthetic',62000)
    row=p['workload_capacity_profile'][0]
    assert row['unique_full_page_prefix_pages']==3 and row['total_full_page_references']==6
    assert row['average_references_per_unique_page']==2 and row['reused_page_fraction']==pytest.approx(2/3)
    assert row['reuse_pair_count']==3 and row['reuse_within_60s_fraction']==pytest.approx(2/3)
    window=next(r for r in p['working_set_timeseries'] if r['time_ms']==61000 and r['window_ms']==60000)
    assert window['unique_full_page_prefix_pages']==2 and window['total_prefix_page_references']==2
    assert sum(b['request_count'] for b in p['prompt_bucket_support'] if b['scope']=='FULL_REPLAY')==3


def test_profiler_partial_pages_capacity_and_scope_endpoints(req):
    rs=[req(0,1499999,(1,2),600),req(1,1500000,tuple(range(100,686))),req(2,2700000,(1,2),600)]
    values=profile(rs,'synthetic')['workload_capacity_profile']
    full,evaluation=values
    assert full['request_count']==3 and evaluation['request_count']==1
    assert full['total_full_page_references']==588
    assert evaluation['full_path_over_capacity_585_request_fraction']==1
    assert evaluation['full_path_over_capacity_585_token_fraction']==1
    assert evaluation['full_path_over_capacity_1170_count']==0


def test_stage0_dependency_isolation():
    from src.simulator.task_main.stage0.isolation import stage0_dependency_audit
    assert stage0_dependency_audit()['status']=='PASS'
