from dataclasses import asdict
import json
import pickle
from types import SimpleNamespace

import pytest

from src.simulator.task_main.stage0.config import CapacityStudyConfig
from src.simulator.task_main.stage0.engine import CapacityStudyEngine
from scripts.task_main_stage1.diagnostics import build, interval
from scripts.task_main_stage1.run import atomic, digest, sha, verify_done, config_for, validate
from test_stage0_capacity import state_bytes


@pytest.mark.parametrize('route',['R_AFF','R_LEAST','R_REQ_KV_TASK'])
@pytest.mark.parametrize('capacity',[585,None])
def test_stage1_schema_determinism_and_read_only(req,route,capacity):
    cfg=CapacityStudyConfig(num_pods=16,capacity_pages=capacity,capacity_mode='infinite' if capacity is None else 'finite',routing_policy=route)
    inputs=[req(i,i,(100+i,)) for i in range(16)]
    inputs += [req(16,1499999,(1,2),600),req(17,1500000,(1,2),600),req(18,1500000.1,(1,2),600),req(19,2700000,(1,2),600)]
    first=CapacityStudyEngine(cfg); result=first.run(inputs)
    before=state_bytes(first); diagnostics=build(result,first,inputs,3537000)
    assert before==state_bytes(first)
    assert all(validate(result,first,inputs,diagnostics).values())
    second=CapacityStudyEngine(cfg);other=second.run(inputs)
    assert digest(asdict(result))==digest(asdict(other))
    assert digest(diagnostics)==digest(build(other,second,inputs,3537000))
    phases={p['scope']:p for p in diagnostics['phases']}
    assert [phases[k]['core']['request_count'] for k in ['PRE_EVAL','EVALUATION','POST_EVAL','FULL_RUN']]==[17,2,1,20]
    assert sum(p['core']['request_count'] for p in diagnostics['five_minute'])==20
    assert all(p['core']['proactive_started']==0 for p in diagnostics['phases'])
    if capacity is None:assert all(p['core']['eviction_pages']==0 for p in diagnostics['phases'])
    dist=diagnostics['effective_replica_distribution']
    assert sum(dist['pages_by_replica_count'].values())==dist['external_unique_full_pages']
    if route=='R_LEAST':assert phases['FULL_RUN']['core']['active_pod_count']==16


def test_ready_time_and_start_time_are_distinct(req):
    cfg=CapacityStudyConfig(routing_policy='R_REQ_KV_TASK')
    engine=CapacityStudyEngine(cfg)
    result=engine.run([req(0,0,(1,2),600),req(1,1499999,(1,2),600)])
    # Explicit independent record near a boundary, no mutation of online replay.
    from src.simulator.task_main.records import TransferRecord
    result.transfer_records=[TransferRecord(0,1,'REACTIVE',0,1,(1,),1,512,14680064,1499999,1500001,1,status='COMPLETED')]
    pre=interval(result,engine,'PRE',0,1500000)['reactive']
    evaluation=interval(result,engine,'EVAL',1500000,2700000)['reactive']
    assert pre['copy_started']==pre['copy_ready_start_cohort']==1
    assert pre['copy_ready_events']==0 and evaluation['copy_ready_events']==1
    assert pre['inflight_at_end']==evaluation['inflight_at_start']==1
    assert evaluation['wire_bytes']==0


def test_completed_marker_requires_all_hashes_and_identity(tmp_path):
    atomic(tmp_path/'result.json',{'value':1})
    identity={'config':'a'}
    assert not verify_done(tmp_path,identity)
    atomic(tmp_path/'COMPLETE.json',dict(identity=identity,artifacts={'result.json':sha(tmp_path/'result.json')}))
    assert verify_done(tmp_path,identity)
    with pytest.raises(ValueError,match='identity'):verify_done(tmp_path,{'config':'b'})
    atomic(tmp_path/'result.json',{'value':2})
    with pytest.raises(ValueError,match='Corrupt'):verify_done(tmp_path,identity)


def test_frozen_matrix_configs():
    for w in ['conversation','toolagent']:
        for c in [585,1170,2340,None]:
            for route in ['R_AFF','R_LEAST','R_REQ_KV_TASK']:
                cfg=config_for(w,route,c)
                assert cfg.num_pods==16 and cfg.proactive_policy=='NONE'
                assert cfg.study_version=='TASK_MAIN_CAPACITY_STAGE0_V1'
                assert cfg.evaluation_start_ms==1500000 and cfg.evaluation_end_ms==2700000
                assert cfg.capacity_mode==('infinite' if c is None else 'finite')


@pytest.mark.parametrize('capacity',[2,585,None])
@pytest.mark.parametrize('route',['R_AFF','R_LEAST','R_REQ_KV_TASK'])
def test_optimized_matches_entire_stage0_result(req,capacity,route):
    from scripts.task_main_stage1.optimized import OptimizedStudyEngine
    cfg=CapacityStudyConfig(capacity_pages=capacity,capacity_mode='infinite' if capacity is None else 'finite',routing_policy=route)
    inputs=[req(i,i,(i+100,)) for i in range(32)]
    inputs += [req(32,40,(1,2),600),req(33,41,(1,2),600),req(34,41.1,(1,2),600)]
    original=CapacityStudyEngine(cfg); reference=original.run(inputs)
    optimized=OptimizedStudyEngine(cfg); result=optimized.run(inputs)
    assert asdict(reference)==asdict(result)
    assert build(reference,original,inputs,100)==build(result,optimized,inputs,100)
    before=state_bytes(optimized)
    for c in optimized.caches:
        c.summary();c.plan_prompt((100,));c.plan_temporary(1)
    build(result,optimized,inputs,100)
    assert state_bytes(optimized)==before


def test_runner_resume_skips_verified_case_and_restarts_partial(tmp_path,req,monkeypatch):
    from scripts.task_main_stage1 import run
    from scripts.task_main_stage1.optimized import OptimizedStudyEngine
    cfg=config_for('conversation','R_AFF',585)
    case=dict(config=cfg.as_dict(),case='R_AFF__585',workload='conversation')
    inputs=[req(0,0,(1,)),req(1,1,(1,))]
    manifest=dict(cases=[case])
    real=OptimizedStudyEngine.run
    def interrupted(*args,**kwargs):raise KeyboardInterrupt('simulated interruption')
    monkeypatch.setattr(OptimizedStudyEngine,'run',interrupted)
    with pytest.raises(KeyboardInterrupt):run.run_case(tmp_path,case,inputs,manifest,kind='pilot',limit_ms=10,repeat=True)
    checkpoint=tmp_path/'checkpoints/conversation/R_AFF__585.json'
    assert json.loads(checkpoint.read_text())['status']=='FAILED'
    assert not (tmp_path/'conversation/R_AFF__585/COMPLETE.json').exists()
    assert len(list((tmp_path/'conversation').glob('*.partial.*')))==1
    monkeypatch.setattr(OptimizedStudyEngine,'run',real)
    perf=run.run_case(tmp_path,case,inputs,manifest,kind='pilot',limit_ms=10,repeat=True,resume=True)
    mark=tmp_path/'conversation/R_AFF__585/COMPLETE.json';before=mark.read_bytes()
    monkeypatch.setattr(OptimizedStudyEngine,'run',interrupted)
    assert run.run_case(tmp_path,case,inputs,manifest,kind='pilot',limit_ms=10,repeat=True,resume=True)==perf
    assert mark.read_bytes()==before


def test_collect_rejects_pilot_as_formal(tmp_path):
    from scripts.task_main_stage1.run import collect
    case={'workload':'conversation','case':'x','config':{}}
    manifest={'cases':[case]};directory=tmp_path/'conversation/x'
    atomic(directory/'COMPLETE.json',dict(identity={'manifest_sha256':digest(manifest),'kind':'pilot'},artifacts={}))
    with pytest.raises(ValueError,match='Non-formal'):collect(tmp_path,manifest)


def test_status_works_while_runner_lock_is_held(tmp_path):
    import os,fcntl,subprocess,sys
    from scripts.task_main_stage1.run import prepare,ROOT
    prepare(tmp_path)
    with (tmp_path/'.runner.lock').open('w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        proc=subprocess.run([sys.executable,'-m','scripts.task_main_stage1.run','status','--root',str(tmp_path)],cwd=ROOT,
                            capture_output=True,text=True,env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1'))
        assert proc.returncode==0,proc.stderr
        assert json.loads(proc.stdout)['completed']==0
