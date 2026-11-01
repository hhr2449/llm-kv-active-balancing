import copy
from dataclasses import replace
import json
from pathlib import Path
import subprocess
import sys

import pytest

from src.simulator.task_main.config import TaskMainConfig, STAGE_C_VERSION
from src.simulator.task_main.engine import TaskMainEngine
from src.simulator.task_main.equivalence import execution_projection, logical_state_projection, projection_digest
from src.simulator.task_main.future_access import FutureAccessAudit
from src.simulator.task_main.history import FutureDemandIndex
from src.simulator.task_main.metrics import write_outputs
from src.simulator.task_main.reporting import deltas, build_tables, write_tables, main_row
from src.simulator.task_main.validation import (static_gate_a, future_gate, gate_a_evidence,
    compare_gate_a, validate_metrics)


def config(policy="PERSISTENCE", **kw):
    return TaskMainConfig(protocol_version=STAGE_C_VERSION, proactive_policy=policy, **kw)


@pytest.fixture
def pair(req):
    trace=[req(0,1499999,(1,2)),req(1,1500001,(1,2)),req(2,1500002,(1,2,3)),
           req(3,1800005,(1,2)),req(4,2700000,(4,5))]
    a,b=config(line_id=5),config("PERSISTENCE_COST_AWARE",line_id=7)
    return a,b,TaskMainEngine(a).run(trace),TaskMainEngine(b).run(trace)


def test_static_matrix_formal_and_pilot_equivalence_and_only_split_differs():
    for workload in ("conversation","toolagent"):
        matrices=[]
        for kind in ("formal","pilot"):
            matrix=[TaskMainConfig.from_yaml(f"configs/task_main/{kind}/{workload}/line_{i}.yaml") for i in range(1,8)]
            assert static_gate_a(matrix[4],matrix[6])
            assert [c.line_id for c in matrix]==list(range(1,8))
            matrices.append(matrix)
        for a,b in zip(*matrices):
            assert {k:v for k,v in a.as_dict().items() if k not in {"experiment_kind","evaluation_start_ms","evaluation_end_ms","visibility_end_ms"}} == {k:v for k,v in b.as_dict().items() if k not in {"experiment_kind","evaluation_start_ms","evaluation_end_ms","visibility_end_ms"}}


def test_static_gate_detects_behavior_parameter_change():
    assert not static_gate_a(config(),config("PERSISTENCE_COST_AWARE",capacity_pages=584))
    assert not static_gate_a(config(history_window_ms=60000),config("PERSISTENCE_COST_AWARE"))


def test_execution_logical_and_system_equivalence_with_real_score_diagnostics(pair):
    a,b,left,right=pair
    gate=compare_gate_a(a,b,gate_a_evidence(left),gate_a_evidence(right))
    assert gate["status"]=="PASS"
    assert left.proactive_action_records and right.candidate_decision_records
    assert left.summary["policy_diagnostics"]["policy"]!=right.summary["policy_diagnostics"]["policy"]
    assert all(r.cost_score>0 for r in right.candidate_decision_records)
    assert left.summary["final_metrics"]==right.summary["final_metrics"]
    assert all(k in right.final_state for k in ("demand_history","reuse_history","candidate_universe","active_load_histories"))


def test_execution_projection_excludes_only_explicit_diagnostic_groups(pair):
    _,_,left,right=pair
    before=execution_projection(right)
    right.proactive_action_records[0].cost_score=999
    right.summary["provenance"]["output_path"]="different"
    right.summary["policy_diagnostics"]["report_only"]=123
    assert before==execution_projection(right)==execution_projection(left)


def test_logical_projection_excludes_report_fields_but_not_lru_and_history(pair):
    _,_,left,_=pair
    original=projection_digest(logical_state_projection(left))
    state=copy.deepcopy(left.final_state)
    state.update(policy="other",provenance={},output_path="other",cost_score=99,report_metrics={})
    assert projection_digest(logical_state_projection(state))==original
    state["reuse_history"].append(([999], [123]))
    assert projection_digest(logical_state_projection(state))!=original


@pytest.mark.parametrize("mutation", ["action", "shortlist", "assignment", "cache", "load", "demand", "reuse", "inflight", "summary"])
def test_gate_a_detects_behavior_mutation(pair,mutation):
    a,b,left,right=pair
    if mutation=="action": right.proactive_action_records[0].target+=1
    elif mutation=="shortlist": right.candidate_decision_records[0].chain_id=(999,)
    elif mutation=="assignment": right.request_records[0]=replace(right.request_records[0],final_pod=3)
    elif mutation=="cache": right.final_state["cache"][0]["access_order"]+=1
    elif mutation=="load": right.final_state["load_vector"]=(999,0,0,0)
    elif mutation=="demand": right.final_state["demand_history"].append(((999,),[(1,0)]))
    elif mutation=="reuse": right.final_state["reuse_history"].append(((999,),[1]))
    elif mutation=="inflight": right.final_state["ready_events"].append((123,1,1))
    else: right.summary["final_metrics"]["requests"]["saved_prefill_tokens"]+=1
    assert compare_gate_a(a,b,gate_a_evidence(left),gate_a_evidence(right))["status"]=="INVALID"


def test_future_access_count_is_incremented_by_api_not_loading_or_policy_name(req):
    audit=FutureAccessAudit(); index=FutureDemandIndex([req(),req(1,10)],3537000)
    with audit.decision("FUTURE_DEMAND"):
        pass
    assert audit.snapshot()["decision_future_reads"]==0
    with audit.decision("FUTURE_DEMAND"):
        index.count((1,),0,300000);index.count((99,),0,300000)
    assert audit.snapshot()["oracle_decision_future_reads"]==2
    with audit.decision("PERSISTENCE"):
        index.count((1,),0,300000)
    assert audit.snapshot()["history_decision_future_reads"]==1
    assert not future_gate("PERSISTENCE",audit.snapshot())


@pytest.mark.parametrize("policy",["NONE","PERSISTENCE","RECENCY","PERSISTENCE_COST_AWARE"])
def test_history_decision_future_reads_zero_in_production(req,policy):
    result=TaskMainEngine(config(policy)).run([req(),req(1,10)])
    assert result.summary["policy_diagnostics"]["future_access"]["decision_future_reads"]==0
    assert result.validation["checks"]["gate_b_decision_future_reads"]


def test_oracle_api_instrumentation_in_real_decision_path(req):
    result=TaskMainEngine(config("FUTURE_DEMAND")).run([req(),req(1,10)])
    audit=result.summary["policy_diagnostics"]["future_access"]
    assert audit["oracle_decision_future_reads"]>0 and audit["history_decision_future_reads"]==0


def test_posthoc_outside_decision_context_is_not_counted(req,pair):
    audit=FutureAccessAudit();index=FutureDemandIndex([req(),req(1,10)],3537000)
    with audit.decision("PERSISTENCE"):
        pass
    index.count((1,),0,300000)
    _,_,result,_=pair
    before=audit.snapshot()
    from src.simulator.task_main.final_metrics import finalize_metrics
    finalize_metrics(result,config())
    assert audit.snapshot()==before==dict(oracle_decision_future_reads=0,history_decision_future_reads=0,decision_future_reads=0)


def test_injected_history_future_read_invalidates_normal_engine_path(req,monkeypatch):
    from src.simulator.task_main.policies import Policy
    old=Policy.shortlist
    index=FutureDemandIndex([req(),req(1,10)],3537000)
    def leaking(self,*args,**kwargs):
        index.count((1,),0,300000)
        return old(self,*args,**kwargs)
    monkeypatch.setattr(Policy,"shortlist",leaking)
    with pytest.raises(AssertionError,match="gate_b_decision_future_reads"):
        TaskMainEngine(config()).run([req(),req(1,10)])


def test_rate_absolute_pp_and_no_relative_percent_on_zero_na(pair):
    _,_,left,_=pair
    row=main_row(left.summary)
    baseline=dict(row,request_hit_rate=0,token_hit_rate=None,transfer_count=0)
    d=deltas(row,baseline)
    assert d["delta_request_hit_rate"]==row["request_hit_rate"]
    assert d["delta_request_hit_rate_pp"]==row["request_hit_rate"]*100
    assert d["delta_token_hit_rate"] is d["delta_token_hit_rate_pp"] is None
    assert d["delta_transfer_count"]==row["transfer_count"]
    assert not any("percent" in k or "relative" in k for k in d)


def test_all_nine_table_families_and_oracle_bucket_deltas(pair,tmp_path):
    _,_,left,_=pair
    summaries=[]
    for workload in ("conversation","toolagent"):
        for i in range(1,8):
            s=copy.deepcopy(left.summary)
            s["provenance"]["config"].update(workload=workload,line_id=i)
            s["final_metrics"]["requests"]["saved_prefill_tokens"]=100*i
            for b in s["final_metrics"]["requests"]["buckets"]:
                b["saved_tokens"]=i; b["weighted_saved_tokens"]=i*b["multiplier"]
            summaries.append(s)
    tables=write_tables(tmp_path/"tables",list(reversed(summaries)))
    assert len(tables)==9 and len(tables["strategy_workload"])==14
    assert len(tables["prompt_bucket"])==84 and len(tables["gini_window"])==56
    for row in tables["baseline_delta"]:
        assert row["baseline_line_id"]==2 and row["delta_saved_prefill_tokens"]==100*(row["line_id"]-2)
    for row in tables["oracle_reference_delta"]:
        assert row["name"]=="Oracle reference delta" and row["delta_B1_saved_tokens"]==1
        assert row["delta_B6_weighted_saved_tokens"]==pytest.approx(15.2)
        assert row["baseline_wasted_copy_ratio"] is None
    assert "NA" in (tmp_path/"tables/oracle_reference_delta.csv").read_text()
    assert all(all(k in row for k in ("W","h","K","q","theta","N","action")) for rows in tables.values() for row in rows)


def test_metrics_validation_detects_conservation_failure(pair):
    a,_,left,_=pair
    left.summary["final_metrics"]["requests"]["buckets"][0]["saved_tokens"]+=1
    assert not validate_metrics(left,a)["bucket_saved_conservation"]


def test_c_observation_records_outputs_and_deterministic_replay(req,tmp_path):
    c=config();trace=[req(0,1500000,(1,2)),req(1,1500010,(1,2))]
    results=[TaskMainEngine(c).run(trace) for _ in range(2)]
    for i,result in enumerate(results): write_outputs(tmp_path/str(i),result)
    assert {p.name:p.read_bytes() for p in (tmp_path/"0").iterdir()}=={p.name:p.read_bytes() for p in (tmp_path/"1").iterdir()}
    result=results[0]
    assert result.copy_observation_records and result.reuse_observation_records
    assert result.summary["final_metrics"]["wasted"]["used_copy_count"]>=1
    assert (tmp_path/"0/final_state.json").exists()
    from src.simulator.task_main.metrics import read_outputs
    from src.simulator.task_main.final_metrics import finalize_metrics
    loaded = read_outputs(tmp_path/"0")
    assert gate_a_evidence(loaded)["execution_projection_sha256"] == gate_a_evidence(result)["execution_projection_sha256"]
    assert projection_digest(finalize_metrics(loaded,c)) == projection_digest(result.summary["final_metrics"])


def test_stage_c_all_policies_execute_under_runtime_import_guard():
    code='''
from src.simulator.task_main.isolation import install_legacy_import_guard
install_legacy_import_guard()
from src.simulator.task_main.config import TaskMainConfig,STAGE_C_VERSION
from src.simulator.task_main.engine import TaskMainEngine
from src.simulator.task_main.trace import TraceRequest
trace=[TraceRequest.from_record(i,dict(timestamp=1500000+i*10,input_length=512,output_length=0,hash_ids=[1])) for i in range(3)]
for policy in ('NONE','FUTURE_DEMAND','PERSISTENCE','RECENCY','PERSISTENCE_COST_AWARE'):
 r=TaskMainEngine(TaskMainConfig(protocol_version=STAGE_C_VERSION,proactive_policy=policy)).run(trace)
 assert r.validation['status']=='PASS'
'''
    subprocess.run([sys.executable,"-c",code],check=True,capture_output=True,text=True)
