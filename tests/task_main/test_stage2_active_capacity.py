from copy import deepcopy
from dataclasses import fields
import json
import pickle
from types import SimpleNamespace

import pytest

from src.simulator.task_main.stage0.config import CapacityStudyConfig
from src.simulator.task_main.stage0.engine import CapacityStudyEngine
from src.simulator.task_main.stage2.config import Stage2Config
from src.simulator.task_main.stage2.engine import Stage2Engine
from src.simulator.task_main.stage2.index import static_metadata
from src.simulator.task_main.stage2.utilization import copy_utilization, waste_summary
from src.simulator.task_main.final_metrics import wasted_metrics
from scripts.task_main_stage2.diagnostics import build, validate


def reference_config(cfg):
    return CapacityStudyConfig(**{f.name: getattr(cfg, f.name) for f in fields(CapacityStudyConfig)})


@pytest.mark.parametrize('policy', ['FUTURE_DEMAND', 'PERSISTENCE', 'RECENCY'])
@pytest.mark.parametrize('capacity', [0, 2, 8, None])
def test_sparse_candidates_cache_and_fixed_metrics_match_frozen_synthetic(req, policy, capacity):
    cfg = Stage2Config(proactive_policy=policy, num_pods=3, capacity_pages=capacity,
                       capacity_mode='infinite' if capacity is None else 'finite')
    rs = [req(i, 1499000+i*73, (1, 2+i%3)) for i in range(24)]
    rs += [req(24, 1800000, (1, 2)), req(25, 3500000, (1, 2))]
    engine = Stage2Engine(cfg)
    actual = engine.run(rs)
    expected = CapacityStudyEngine(reference_config(cfg)).run(rs)
    for attr in ('request_records', 'transfer_records', 'opportunity_records', 'event_records',
                 'proactive_action_records', 'candidate_decision_records', 'final_state'):
        assert getattr(actual, attr) == getattr(expected, attr), attr
    assert actual.summary['final_metrics'] == expected.summary['final_metrics']
    for cache in engine.caches:
        cache.full_check()
    assert all(validate(actual, engine, rs, build(actual, engine, rs)).values())


@pytest.mark.parametrize('schedule,initial,before,after', [
    ('C', 'R_REQ_KV_TASK', 'R_REQ_KV_TASK', 'R_AFF'),
    ('D', 'R_AFF', 'R_AFF', 'R_REQ_KV_TASK')])
def test_boundary_by_arrival_no_reset_and_preboundary_transfer_completes(req, schedule, initial, before, after):
    cfg = Stage2Config(num_pods=2, capacity_pages=10, proactive_policy='NONE',
                       routing_schedule=schedule, routing_policy=initial)
    rs = [req(0, 1499900, (1, 2), 600), req(1, 1499999.8, (1, 2), 600),
          req(2, 1500000, (1, 2), 600), req(3, 2700000, (1, 2), 600),
          req(4, 3500000, (1, 2), 600)]
    engine = Stage2Engine(cfg)
    result = engine.run(rs)
    assert [r.routing_policy for r in result.request_records] == [before, before, after, after, after]
    assert len(engine.load.entries) == len(rs)
    assert engine.demand_history.request_ids == {0, 1, 2, 3, 4}
    assert result.request_records[2].final_hit_tokens >= 512
    diagnostics = build(result, engine, rs)
    assert all(validate(result, engine, rs, diagnostics).values())
    if schedule == 'C':
        transfer = result.transfer_records[0]
        assert transfer.start_time < 1500000 < transfer.ready_time
        record = result.request_records[1]
        assert record.routing_policy == 'R_REQ_KV_TASK' and record.completion_time > 1500000
        pre, evaluation = diagnostics['phases'][:2]
        assert pre['reactive']['copy_started'] == 1 and pre['reactive']['copy_ready_events'] == 0
        assert evaluation['reactive']['copy_started'] == 0 and evaluation['reactive']['copy_ready_events'] == 1
        assert evaluation['reactive']['gate_evaluated'] == 0


def evidence(ready, uses=(), still=True):
    action = SimpleNamespace(action_id=0, transfer_id=0, chain_id=(1, 2), target=1,
        start_time=ready-1, ready_time=ready, chain_depth_pages=2,
        wire_pages=2, wire_tokens=1024, wire_bytes=29360128)
    requests = [SimpleNamespace(request_id=i, final_pod=target, completion_time=t, final_hit_pages=len(path))
                for i, (t, target, path, generations) in enumerate(uses)]
    reuses = [dict(request_id=i, time=t, target=target, path=path, generations=generations)
              for i, (t, target, path, generations) in enumerate(uses)]
    return SimpleNamespace(proactive_action_records=[action], request_records=requests,
        reuse_observation_records=reuses,
        copy_observation_records=[dict(transfer_id=0, generations=(4, 5))],
        transfer_records=[SimpleNamespace(transfer_id=0, newly_resident_pages=1, duplicate_pages=1)],
        final_state={'cache': [{'pages': []}, {'pages': [[1, None, 0, 0, 0, 4], [2, 1, 0, 0, 0, 5]] if still else []}]})


@pytest.mark.parametrize('ready,uses,status', [
    (1000, (), 'UNUSED'),  # infinite still resident is included in fixed-window unused
    (3400000, (), 'CENSORED'),
    (1000, ((301000, 1, (1, 2), (4, 5)),), 'USED'),  # upper boundary inclusive
    (1000, ((1000, 1, (1, 2), (4, 5)),), 'UNUSED'),  # frozen lower boundary exclusive
    (1000, ((2000, 0, (1, 2), (4, 5)),), 'UNUSED'),
    (1000, ((2000, 1, (1, 2), (4, 6)),), 'UNUSED'),
    (1000, ((2000, 1, (1,), (4,)),), 'UNUSED'),
    (1000, ((2000, 1, (1, 2), (9, 5)),), 'UNUSED'),
    (3400000, ((3450000, 1, (1, 2), (4, 5)),), 'CENSORED'),
])
def test_300s_utilization_censoring_generation_and_infinite_residency(ready, uses, status):
    result = evidence(ready, uses)
    rows = copy_utilization(result)
    assert rows[0]['status'] == status
    assert rows[0]['wire_pages'] == 2 and rows[0]['new_page_denominator'] == 1
    expected = wasted_metrics(result.proactive_action_records, result.copy_observation_records,
                              result.reuse_observation_records, 0, 3537000, 3537000)
    assert waste_summary(rows) == expected
    if not uses:
        assert rows[0]['lifecycle_full_chain_outcome'] == 'UNRESOLVED_STILL_RESIDENT'


def test_lifecycle_use_after_300s_is_separate():
    row = copy_utilization(evidence(1000, ((400000, 1, (1, 2), (4, 5)),)))[0]
    assert row['status'] == 'UNUSED' and row['lifecycle_full_chain_outcome'] == 'USED'
    assert row['lifecycle_unresolved_new_pages'] == 0


@pytest.mark.parametrize('policy', ['PERSISTENCE', 'FUTURE_DEMAND', 'RECENCY'])
def test_wire_phase_type_and_new_page_denominators(req, policy):
    cfg = Stage2Config(proactive_policy=policy, capacity_pages=None, capacity_mode='infinite', num_pods=2)
    rs = [req(0, 1499999.8, (1, 2)), req(1, 1500000, (1, 2)), req(2, 2700000, (1, 2))]
    engine = Stage2Engine(cfg)
    result = engine.run(rs)
    data = build(result, engine, rs)
    phases = {p['scope']: p for p in data['phases']}
    assert phases['FULL_RUN']['core']['total_wire_bytes'] == sum(
        phases[s]['core']['total_wire_bytes'] for s in ('PRE_EVAL', 'EVALUATION', 'POST_EVAL'))
    for row in data['phases']:
        c = row['core']
        assert c['total_wire_bytes'] == c['reactive_wire_bytes']+c['proactive_wire_bytes']
    for row in data['copy_utilization']:
        assert row['new_pages_observed_used'] <= row['newly_published_pages'] <= row['wire_pages']


def test_timestamp_checkpoint_resume_and_streamed_decisions(req, tmp_path):
    from scripts.task_main_stage2.run import save_checkpoint, load_checkpoint
    cfg = Stage2Config(proactive_policy='PERSISTENCE', num_pods=3, capacity_pages=6)
    rs = [req(i, 1499999+i//2, (1, 2+i%3)) for i in range(18)]
    identity = {'test': 'synthetic'}
    class Stop(Exception):
        pass
    engine = Stage2Engine(cfg, audit_directory=tmp_path/'segments')
    def checkpoint(e, done, now):
        if done >= 8:
            save_checkpoint(tmp_path/'checkpoint', identity, e, 12.)
            assert done == len(rs) or rs[done].arrival_ms > now
            raise Stop()
    with pytest.raises(Stop):
        engine.run(rs, checkpoint_callback=checkpoint)
    resumed, elapsed = load_checkpoint(tmp_path/'checkpoint', identity)
    actual = resumed.run(rs, resume=True)
    expected = Stage2Engine(cfg).run(rs)
    assert elapsed == 12.
    for attr in ('request_records', 'transfer_records', 'opportunity_records', 'event_records', 'final_state'):
        assert getattr(actual, attr) == getattr(expected, attr)
    assert list(actual.candidate_decision_records) == expected.candidate_decision_records
    assert actual.summary['final_metrics'] == expected.summary['final_metrics']
    with pytest.raises(ValueError):
        load_checkpoint(tmp_path/'checkpoint', {'test': 'changed'})


def test_queries_do_not_change_cache_or_history_state(req):
    cfg = Stage2Config(proactive_policy='PERSISTENCE', num_pods=2, capacity_pages=3)
    rows = [req(0, 0, (1, 2)), req(1, 1, (1, 3))]
    engine = Stage2Engine(cfg, metadata=static_metadata(rows))
    engine.run(rows)
    before = pickle.dumps(engine)
    for cache in engine.caches:
        cache.summary(); cache.sample(); cache.plan_prompt((1, 2)); cache.lookup((1, 2))
    engine.universe.materialize(engine.caches, (0, 0), engine.transfers.records)
    engine.demand_history.count((1,), 3, 300000)
    assert pickle.dumps(engine) == before


def test_matrix_excludes_baseline_and_unrequested_policies():
    from scripts.task_main_stage2.common import matrix, check_frozen
    cases = matrix()
    assert len(cases) == 28
    assert sum(c['family'] == 'ACTIVE' for c in cases) == 24
    assert sum(c['workload'] == 'conversation' for c in cases) == 14
    check_frozen()


def test_atomic_complete_is_never_replayed(req, tmp_path, monkeypatch):
    import time
    from scripts.task_main_stage2 import run
    from scripts.task_main_stage2.common import case_identity, verify_done
    cfg = Stage2Config(proactive_policy='PERSISTENCE', num_pods=2, capacity_pages=3)
    case = dict(workload=cfg.workload, case='unit', config=cfg.as_dict(), family='ACTIVE')
    manifest = dict(head='test-head', source_sha256={'test': 'hash'})
    monkeypatch.setattr(run, 'head', lambda: 'test-head')
    monkeypatch.setattr(run, 'source_identity', lambda: {'test': 'hash'})
    rs = [req(0, 1500000, (1, 2)), req(1, 1500010, (1, 2))]
    args = (tmp_path, case, rs, static_metadata(rs), None, manifest, False, 600, 1, 1, time.monotonic())
    run.run_case(*args)
    assert verify_done(tmp_path/cfg.workload/'unit', case_identity(manifest, case))
    monkeypatch.setattr(Stage2Engine, 'run', lambda *a, **kw: pytest.fail('COMPLETE case replayed'))
    run.run_case(*args)
    (tmp_path/cfg.workload/'unit'/'config.json').write_text('{}')
    with pytest.raises(ValueError, match='hash mismatch'):
        run.run_case(*args)


def test_consolidation_reuses_baselines_and_computes_signed_contrasts(tmp_path, monkeypatch):
    # Artifact-only integration fixture: no simulator/trace replay is involved.
    import gzip
    from scripts.task_main_stage2 import collect as module
    from scripts.task_main_stage2.common import matrix, sha, atomic, write_csv, case_identity
    baseline_dir = tmp_path/'baseline'
    configs = tmp_path/'configs'
    output = tmp_path/'stage2'
    baseline_dir.mkdir(); configs.mkdir(); output.mkdir()
    monkeypatch.setattr(module, 'ROOT', tmp_path)
    monkeypatch.setattr(module, 'STAGE1', baseline_dir)
    monkeypatch.setattr(module, 'CONFIGS', configs)
    scopes = [('PRE_EVAL', 0, 1500000), ('EVALUATION', 1500000, 2700000),
              ('POST_EVAL', 2700000, 3537000), ('FULL_RUN', 0, 3537000)]
    def core(saved):
        return dict(saved_prefill_tokens=saved, weighted_saved_tokens=1.3*saved,
            token_hit_rate=saved/10000, total_input_tokens=10000, request_count=10,
            total_miss_prefill_tokens=10000-saved, reactive_wire_bytes=0,
            total_wire_bytes=0, proactive_wire_bytes=0, eviction_pages=0,
            cache_admission_rejected=0, capacity_preflight_failures=0)
    rows, pods, reactive = [], [], []
    for w in ['conversation', 'toolagent']:
        for cap in [585, 1170, 2340, None]:
            for route, saved in [('R_AFF', 1000), ('R_LEAST', 900), ('R_REQ_KV_TASK', 1100)]:
                meta = dict(workload=w, capacity_pages=cap, routing_policy=route)
                for scope, lo, hi in scopes:
                    row = dict(meta, scope=scope, start_ms=lo, end_ms=hi, **core(saved))
                    rows.append(row)
                    pods.extend(dict(meta, scope=scope, pod=p) for p in range(16))
                    reactive.append(dict(meta, scope=scope))
                d = baseline_dir/w/(route+'__'+('infinite' if cap is None else str(cap)))
                atomic(d/'stage1_diagnostics.json', dict(effective_replica_distribution={}, hotspot_coverage='unimplemented'))
                atomic(d/'COMPLETE.json', dict(artifacts={'stage1_diagnostics.json': sha(d/'stage1_diagnostics.json')}))
    for name, values in [('stage1_baseline_capacity', rows), ('stage1_per_pod', pods),
                          ('stage1_reactive_diagnostics', reactive), ('stage1_timeseries', [])]:
        write_csv(baseline_dir/(name+'.csv'), values)
    atomic(baseline_dir/'manifest.json', {'fixture': True})
    atomic(configs/'stage1_reference.json', dict(artifact_sha256={str(p.relative_to(tmp_path)): sha(p) for p in baseline_dir.glob('*.csv')},
        stage1_manifest_sha256=sha(baseline_dir/'manifest.json')))
    manifest = dict(cases=matrix(), head='synthetic-fixture', source_sha256={},
                    stage1_reference_sha256=sha(configs/'stage1_reference.json'))
    atomic(output/'manifest.json', manifest)
    module.collect(output)
    assert json.loads((output/'stage2_validation.json').read_text())['status'] == 'INCOMPLETE'
    assert not (output/'stage2_results.md').exists()
    for case in manifest['cases']:
        cfg = case['config']; directory = output/case['workload']/case['case']
        directory.mkdir(parents=True)
        saved = 1050 if cfg['routing_schedule'] == 'C' else 1080 if cfg['routing_schedule'] == 'D' else 1010
        data = dict(phases=[dict(scope=s, start_ms=lo, end_ms=hi, core=core(saved),
                    per_pod=[dict(pod=p) for p in range(16)], reactive={}, proactive={}, utilization={}) for s, lo, hi in scopes],
                    five_minute=[], copy_utilization=[], effective_replica_distribution={}, hotspot_coverage='unimplemented')
        with gzip.open(directory/'diagnostics.json.gz', 'wt') as f:
            json.dump(data, f)
        atomic(directory/'performance.json', dict(head=manifest['head'], workers=4, hostname='unit-fixture'))
        atomic(directory/'validation.json', dict(status='PASS', checks={'fixture': True}, replay_count=1))
        atomic(directory/'COMPLETE.json', dict(status='PASS', identity=case_identity(manifest, case),
            artifacts={p.name: sha(p) for p in directory.iterdir()}))
    module.collect(output)
    assert json.loads((output/'stage2_validation.json').read_text())['status'] == 'STAGE2_ACTIVE_CAPACITY_PASS'
    capacity = module.read_csv(output/'stage2_capacity_6strategy.csv')
    assert len(capacity) == 48
    assert all(r['delta_saved_prefill_tokens_vs_req'] == -90 for r in capacity if r['family'] == 'ACTIVE')
    contrasts = module.read_csv(output/'stage2_reactive_ablation.csv')
    assert {r['ablation']: r['saved_prefill_tokens'] for r in contrasts if r['row_type'] == 'CONTRAST'} == {
        'C-A': 50, 'B-D': 20, 'B-C': 50, 'D-A': 80}
