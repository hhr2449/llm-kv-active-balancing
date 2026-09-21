from dataclasses import replace
from types import SimpleNamespace as NS
import math
import random
import pytest

from src.simulator.task_main.config import TaskMainConfig, STAGE_C_VERSION
from src.simulator.task_main.engine import TaskMainEngine
from src.simulator.task_main.final_metrics import (request_metrics, gini, random_gini_baseline,
    gini_metrics, aggregate_gini, wasted_metrics, network_metrics)
from src.simulator.task_main.history import WEIGHTS


def request(time=1500000, tokens=600, hit=512, pod=0):
    return NS(arrival_time=time, completion_time=time+1000, input_tokens=tokens,
              final_hit_tokens=hit, miss_tokens=tokens-hit, final_pod=pod)


def action(aid=0, pages=2, start=1500000, ready=1500001, target=1):
    return NS(action_id=aid, transfer_id=aid, chain_id=tuple(range(1, pages+1)),
              chain_depth_pages=pages, wire_tokens=pages*512, start_time=start, ready_time=ready, target=target)


def copy(a):
    return dict(transfer_id=a.transfer_id, target=a.target, chain_id=a.chain_id,
                ready_time=a.ready_time, generations=tuple(range(10, 10+a.chain_depth_pages)))


def use(a, time=None, **changes):
    row = dict(target=a.target, request_id=9, time=a.ready_time+1 if time is None else time,
               path=a.chain_id, generations=copy(a)["generations"])
    row.update(changes)
    return row


def waste(actions, reuses=(), visibility=3537000):
    return wasted_metrics(actions, [copy(a) for a in actions], reuses, 1500000, 2700000, visibility)


def transfer(kind="PROACTIVE", start=1500000, ready=1500001, status="COMPLETED", pages=2):
    return NS(type=kind, start_time=start, ready_time=ready, status=status,
              wire_pages=pages, wire_tokens=pages*512, wire_bytes=pages*14680064)


def test_actual_longest_hit_once_and_arrival_cohort():
    rows = [request(time=1499999,hit=600), request(hit=512),request(time=2700000,hit=600)]
    m = request_metrics(rows,1500000,2700000)
    assert m["saved_prefill_tokens"] == 512 and m["request_count"] == 1
    assert m["token_hit_rate"] == 512/600 and m["request_hit_rate"] == 1


def test_partial_valid_tokens_and_conservation_in_production(req):
    c = TaskMainConfig(protocol_version=STAGE_C_VERSION)
    result = TaskMainEngine(c).run([req(0,1499999,(1,2),600),req(1,1500000,(1,2),600)])
    m = result.summary["final_metrics"]["requests"]
    assert m["saved_prefill_tokens"] == m["total_input_tokens"] == 600
    assert all(r.final_hit_tokens+r.miss_tokens == r.input_tokens for r in result.request_records)


@pytest.mark.parametrize("tokens,index", [(0,0),(4999,0),(5000,1),(19999,1),(20000,2),(59999,2),
    (60000,3),(119999,3),(120000,4),(300000,4),(300001,5)])
def test_all_bucket_endpoints(tokens,index):
    m=request_metrics([request(tokens=tokens,hit=tokens)],1500000,2700000)
    assert m["buckets"][index]["request_count"] == 1
    assert m["buckets"][index]["saved_tokens"] == tokens


def test_bucket_global_conservation_and_exact_weighted_sum():
    rows=[request(tokens=t,hit=t//2) for t in (500,5000,20000,60000,120000,300001)]
    m=request_metrics(rows,1500000,2700000)
    assert sum(b["request_count"] for b in m["buckets"]) == m["request_count"] == 6
    assert sum(b["input_tokens"] for b in m["buckets"]) == m["total_input_tokens"]
    assert sum(b["saved_tokens"] for b in m["buckets"]) == m["saved_prefill_tokens"]
    assert m["weighted_saved_tokens"] == math.fsum(r.final_hit_tokens*w for r,w in zip(rows,WEIGHTS))
    assert math.fsum(b["weighted_saved_tokens"] for b in m["buckets"]) == m["weighted_saved_tokens"]


def test_empty_buckets_and_zero_denominators_are_na():
    m=request_metrics([],1500000,2700000)
    assert len(m["buckets"]) == 6 and m["request_hit_rate"] is m["token_hit_rate"] is None
    assert all(b["request_hit_rate"] is b["token_hit_rate"] is None for b in m["buckets"])


def test_gini_arrival_windows_final_pods_zero_pods_and_boundaries():
    times=[1499999,1500000,1799999,1800000,2100000,2400000,2699999,2700000]
    rows=[request(time=t,pod=1) for t in times]
    m=gini_metrics(rows,TaskMainConfig())
    assert [w["counts"] for w in m["windows"]] == [[0,2,0,0],[0,1,0,0],[0,1,0,0],[0,2,0,0]]
    assert all(w["actual_gini"]==.75 and sum(w["counts"])==w["request_count"] for w in m["windows"])
    assert m["total_window_count"]==m["valid_ratio_window_count"]==4
    assert [w["window_start_ms"] for w in m["windows"]]==[1500000,1800000,2100000,2400000]


def test_random_baseline_is_uniform_200_repetitions_deterministic_and_cached():
    key=("synthetic",1500000,1800000,4,7,20260911,200)
    random_gini_baseline.cache_clear()
    first=random_gini_baseline(*key)
    assert first is random_gini_baseline(*key)
    assert random_gini_baseline.cache_info().hits==1
    rng=random.Random(20260911); values=[]
    for _ in range(200):
        counts=[0]*4
        for _ in range(7): counts[rng.randrange(4)]+=1
        values.append(gini(counts))
    assert first[0]==math.fsum(values)/200
    ordered=sorted(values)
    assert first[1]==pytest.approx(ordered[189]+.05*(ordered[190]-ordered[189]))
    random_gini_baseline.cache_clear()
    assert first==random_gini_baseline(*key)


def test_random_mean_zero_returns_na_and_all_pods_retained():
    m=gini_metrics([],TaskMainConfig())
    assert m["skew_ratio"] is None and m["valid_ratio_window_count"]==0
    assert all(w["counts"]==[0]*4 and w["random_gini_mean"]==0 and w["skew_ratio"] is None for w in m["windows"])


def test_median_ratios_not_ratio_medians_and_na_not_zero():
    windows=[dict(actual_gini=a,random_gini_mean=b,random_gini_p95=b,skew_ratio=a/b if b else None)
             for a,b in ((.1,.1),(.5,.2),(.9,.3),(0,0))]
    m=aggregate_gini(windows)
    assert m["skew_ratio"]==2.5
    assert m["skew_ratio"]!=m["median_actual_gini"]/m["median_random_gini_mean"]
    assert m["valid_ratio_window_count"]==3 and m["total_window_count"]==4


@pytest.mark.parametrize("time_offset,expected", [(0,"UNUSED"),(1,"USED"),(300000,"USED"),(300000.001,"UNUSED")])
def test_waste_observation_open_left_closed_right(time_offset,expected):
    a=action(); m=waste([a],[use(a,a.ready_time+time_offset)])
    assert m["observations"][0]["status"]==expected


def test_future_demand_without_actual_reuse_is_unused():
    assert waste([action()])["unused_copy_count"]==1


@pytest.mark.parametrize("changes", [dict(target=0),dict(path=(1,),generations=(10,)),
    dict(generations=(10,99)),dict(path=(1,9)),dict(generations=(20,21))])
def test_other_target_partial_hit_or_reinsert_generation_never_uses_old_copy(changes):
    a=action()
    assert waste([a],[use(a,**changes)])["unused_copy_count"]==1


def test_generation_eviction_reinsert_from_real_cache(publish):
    from src.simulator.task_main.cache import TaskMainCache
    from dataclasses import asdict
    cache=TaskMainCache(2);publish(cache,(1,2),1500001)
    a=action();e=copy(a);e["generations"]=tuple(cache.page_state(i)["generation"] for i in (1,2))
    publish(cache,(3,4),1500002);publish(cache,(1,2),1500003)
    cache.record_reuse(9,(1,2),1500004)
    m=wasted_metrics([a],[e],[dict(asdict(cache.reuse_events[0]),target=1)],1500000,2700000,3537000)
    assert cache.eviction_records and m["unused_copy_count"]==1


@pytest.mark.parametrize("extra,expected", [(0,"USED"),(-1,"CENSORED")])
def test_censor_visibility_inclusive_and_early_use_does_not_override(extra,expected):
    a=action();m=waste([a],[use(a)],visibility=a.ready_time+300000+extra)
    assert m["observations"][0]["status"]==expected
    if expected=="CENSORED":
        assert m["wasted_copy_ratio"] is None and m["observation_coverage"]==0


def test_censored_and_unused_costs_retained_in_network():
    a=action();m=waste([a],visibility=1600000)
    net=network_metrics([transfer()],1500000,2700000,3537000)
    assert m["censored_copy_count"]==1 and m["observed_wire_tokens"]==0
    assert next(n for n in net if n["phase"]=="EVAL" and n["type"]=="PROACTIVE")["wire_tokens"]==1024


def test_waste_wire_token_weight_not_action_count():
    small,big=action(0,1),action(1,3)
    m=waste([small,big],[use(small)])
    assert m["wasted_copy_ratio"]==.75 and m["unused_copy_count"]==m["used_copy_count"]==1
    assert m["observed_wire_tokens"]==m["total_proactive_wire_tokens_for_same_cohort"]==2048


def test_same_start_cohort_excludes_warmup_and_tail_actions():
    m=waste([action(0,start=1499999),action(1),action(2,start=2700000)])
    assert m["proactive_copy_count"]==1 and m["observations"][0]["action_id"]==1


@pytest.mark.parametrize("routing", ["R_AFF","R_REQ_KV_TASK"])
def test_lines_one_two_waste_and_coverage_na(req,routing):
    r=TaskMainEngine(TaskMainConfig(protocol_version=STAGE_C_VERSION,routing_policy=routing)).run([req()])
    w=r.summary["final_metrics"]["wasted"]
    assert w["wasted_copy_ratio"] is w["observation_coverage"] is None


def test_network_start_completed_separate_and_type_conservation():
    rows=[transfer(),transfer("REACTIVE",status="IN_FLIGHT",pages=1)]
    net={(n["phase"],n["type"]):n for n in network_metrics(rows,1500000,2700000,3537000)}
    assert net[("EVAL","REACTIVE")]["transfer_completed_count"]==0
    assert net[("EVAL","TOTAL")]["transfer_started_count"]==2
    assert net[("EVAL","TOTAL")]["transfer_completed_count"]==1
    assert net[("EVAL","TOTAL")]["wire_tokens"]==1536


@pytest.mark.parametrize("start,phase", [(0,"PRE_EVAL"),(1499999,"PRE_EVAL"),(1500000,"EVAL"),
    (2699999,"EVAL"),(2700000,"TAIL"),(3536999,"TAIL")])
def test_network_start_phase_including_cross_boundary_ready(start,phase):
    net=network_metrics([transfer(start=start,ready=start+1000000)],1500000,2700000,3537000)
    assert [n["phase"] for n in net if n["type"]=="TOTAL" and n["transfer_started_count"]]==[phase,"FULL_RUN"]


def test_missing_copy_evidence_fails_closed():
    with pytest.raises(ValueError,match="residency"):
        wasted_metrics([action()],[],[],1500000,2700000,3537000)
