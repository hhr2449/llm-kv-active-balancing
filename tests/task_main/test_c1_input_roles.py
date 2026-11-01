from pathlib import Path
import json

import pytest

from src.simulator.task_main.config import TaskMainConfig, STAGE_C_VERSION
from src.simulator.task_main.engine import TaskMainEngine
from src.simulator.task_main.equivalence import execution_projection, projection_digest
from src.simulator.task_main.future_access import FutureAccessAudit
from src.simulator.task_main.history import FutureDemandIndex
from src.simulator.task_main.metrics import write_outputs
from src.simulator.task_main.trace import separate_input_roles, input_role_identity, read_trace


@pytest.mark.parametrize("visibility",[960000,3537000])
def test_exact_endpoint_shared_pilot_formal_input_rule(req,visibility):
    t=visibility-300000
    trace=[req(0,t,(1,)),req(1,visibility,(1,)),req(2,visibility+1,(1,))]
    replay,observation=separate_input_roles(trace,visibility)
    assert [r.request_id for r in replay]==[0]
    assert [r.request_id for r in observation]==[0,1]
    audit=FutureAccessAudit()
    index=FutureDemandIndex(observation,visibility)
    assert audit.snapshot()['decision_future_reads']==0
    with audit.decision('FUTURE_DEMAND'):
        assert index.count((1,),t,300000)==1
    assert audit.snapshot()['oracle_decision_future_reads']==1
    with pytest.raises(ValueError,match='FUTURE_WINDOW_CENSORED'):
        index.count((1,),t+1,300000)
    identity=input_role_identity(replay,observation)
    assert identity['replay_request_count']==1
    assert identity['oracle_observation_request_count']==2
    assert identity['oracle_boundary_only_request_count']==1
    assert identity['replay_request_identity_hash']!=identity['oracle_observation_identity_hash']


def boundary_input(req):
    end=3537000
    trace=[req(0,end-300000,(1,)),req(1,end,(1,)),req(2,end,(8,9)),req(3,end+1,(99,))]
    return separate_input_roles(trace,end)


def test_boundary_existing_chain_counts_but_future_only_prefix_never_online(req):
    replay,observation=boundary_input(req)
    sim=TaskMainEngine(TaskMainConfig(protocol_version=STAGE_C_VERSION,proactive_policy='FUTURE_DEMAND'))
    result=sim.run(replay,oracle_observation_requests=observation)
    assert sim.future_index.count((1,),3237000,300000)==1
    assert sim.future_index.count((8,9),3237000,300000)==1
    assert sim.universe.seen=={(1,)}
    assert set(sim.demand_history.events)=={(1,)}
    assert sim.demand_history.request_ids==sim.reuse_history.request_ids=={0}
    assert set(sim.load.entries)=={0}
    assert {r.request_id for r in result.request_records}=={0}
    assert {r.request_id for r in result.opportunity_records}=={0}
    assert all(c.lookup((8,))==0 and c.lookup((99,))==0 for c in sim.caches)
    assert all(d.chain_id==(1,) for d in result.candidate_decision_records)
    assert all(a.request_id==0 and a.chain_id==(1,) for a in result.proactive_action_records)
    assert sim.reuse_history.events=={}
    assert result.summary['policy_diagnostics']['future_access']['oracle_decision_future_reads']>0


@pytest.mark.parametrize('policy',['NONE','PERSISTENCE','RECENCY','PERSISTENCE_COST_AWARE'])
def test_history_execution_ignores_observation_boundary_completely(req,policy):
    replay,observation=boundary_input(req)
    config=TaskMainConfig(protocol_version=STAGE_C_VERSION,proactive_policy=policy)
    plain=TaskMainEngine(config)
    extra=TaskMainEngine(config)
    one=plain.run(replay)
    two=extra.run(replay,oracle_observation_requests=observation)
    assert execution_projection(one)==execution_projection(two)
    assert one.summary==two.summary and one.validation==two.validation
    assert extra.future_index is None
    assert two.summary['policy_diagnostics']['future_access']['decision_future_reads']==0
    class Unreadable:
        def __iter__(self): raise AssertionError('history engine accessed observations')
    TaskMainEngine(config).run(replay,oracle_observation_requests=Unreadable())


def test_boundary_oracle_replay_all_output_hashes_deterministic(req,tmp_path):
    replay,observation=boundary_input(req)
    config=TaskMainConfig(protocol_version=STAGE_C_VERSION,proactive_policy='FUTURE_DEMAND')
    for repeat in (1,2):
        sim=TaskMainEngine(config)
        result=sim.run(replay,oracle_observation_requests=observation)
        result.summary['provenance']['input_identity']=input_role_identity(replay,observation)
        write_outputs(tmp_path/str(repeat),result)
    assert {p.name:p.read_bytes() for p in (tmp_path/'1').iterdir()}=={p.name:p.read_bytes() for p in (tmp_path/'2').iterdir()}


@pytest.mark.parametrize('workload,boundary,expected,old',[
    ('conversation',14,921,907),('toolagent',22,834,821)])
def test_readonly_original_trace_c1_regression(workload,boundary,expected,old):
    requests=read_trace(Path(f'data/mooncake/{workload}_trace.jsonl'))
    replay,observation=separate_input_roles(requests,960000)
    assert len(observation)-len(replay)==boundary
    assert all(r.arrival_ms<960000 for r in replay)
    assert sum(r.arrival_ms==960000 for r in observation)==boundary
    assert FutureDemandIndex(observation,960000).count((0,),660000,300000)==expected
    assert FutureDemandIndex(replay,960000).count((0,),660000,300000)==old


def test_oracle_rejects_observation_missing_replay_or_beyond_visibility(req):
    replay,observation=boundary_input(req)
    config=TaskMainConfig(protocol_version=STAGE_C_VERSION,proactive_policy='FUTURE_DEMAND')
    with pytest.raises(ValueError,match='exactly the replay'):
        TaskMainEngine(config).run(replay,oracle_observation_requests=observation[1:])
    with pytest.raises(ValueError,match='exceeds visibility'):
        TaskMainEngine(config).run(replay,oracle_observation_requests=observation+[req(9,3537001,(10,))])
