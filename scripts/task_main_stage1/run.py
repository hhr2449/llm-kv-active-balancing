"""Atomic, hash-verified, resumable case runner. Invoke from repository root."""
import argparse
from contextlib import redirect_stdout, redirect_stderr
from dataclasses import asdict
import fcntl
import hashlib
import json
import os
from pathlib import Path
import resource
import socket
import subprocess
import sys
import time
import traceback
import uuid
import csv
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import yaml

from src.simulator.task_main.stage0.config import CapacityStudyConfig
from src.simulator.task_main.stage0.engine import CapacityStudyEngine
from src.simulator.task_main.trace import read_trace
from src.simulator.task_main.metrics import write_outputs
from .diagnostics import build

ROOT = Path(__file__).resolve().parents[2]
DEFAULT = ROOT/'results/task_main/stage1_n16_capacity'
ROUTES = ['R_AFF', 'R_LEAST', 'R_REQ_KV_TASK']
CAPACITIES = [585, 1170, 2340, None]
WORKLOADS = ['conversation', 'toolagent']

def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(1024*1024), b''): h.update(chunk)
    return h.hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value): return hashlib.sha256(canonical(value).encode()).hexdigest()


def atomic(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name+'.tmp.'+uuid.uuid4().hex)
    with tmp.open('w') as f:
        json.dump(value, f, indent=2, sort_keys=True, allow_nan=False); f.write('\n'); f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path)


def head(): return subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()


def source_identity():
    paths = [*ROOT.glob('src/**/*.py'), *ROOT.glob('scripts/task_main_stage1/*.py'),
             *ROOT.glob('configs/task_main/**/*.yaml'), ROOT/'tests/task_main/test_stage1_capacity.py',
             ROOT/'configs/task_main/stage1_n16_capacity/stage0_freeze.json']
    return {str(p.relative_to(ROOT)): sha(p) for p in sorted(paths)}


def config_for(workload, route, capacity):
    path=ROOT/'configs/task_main/stage1_n16_capacity'/workload/(name_for(route,capacity)+'.yaml')
    cfg=CapacityStudyConfig.from_yaml(path)
    if (cfg.workload,cfg.routing_policy,cfg.capacity_pages,cfg.num_pods,cfg.proactive_policy,cfg.experiment_kind) != (workload,route,capacity,16,'NONE','capacity_study'):
        raise ValueError('Stage 1 config disagrees with frozen matrix: '+str(path))
    return cfg


def name_for(route, capacity): return route+'__'+('infinite' if capacity is None else str(capacity))


def prepare(root):
    frozen=json.loads((ROOT/'configs/task_main/stage1_n16_capacity/stage0_freeze.json').read_text())
    if any(sha(ROOT/p)!=h for p,h in frozen.items()):
        raise ValueError('Frozen Stage 0/TaskMain source changed')
    root.mkdir(parents=True, exist_ok=True)
    path = root/'manifest.json'
    if root.is_relative_to(ROOT/'results') and not root.is_relative_to(DEFAULT):
        raise ValueError('Stage 1 outputs must stay within stage1_n16_capacity')
    cases = [dict(workload=w, case=name_for(r,c), config=config_for(w,r,c).as_dict()) for w in WORKLOADS for c in CAPACITIES for r in ROUTES]
    manifest = dict(version='STAGE1_N16_CAPACITY_V1', head=head(), source_sha256=source_identity(), cases=cases,
                    semantic_version='TASK_MAIN_CAPACITY_STAGE0_V1', evaluation_interval_ms=[1500000,2700000],
                    random_seed=20260911, random_repetitions=200)
    if path.exists():
        prior=json.loads(path.read_text())
        if prior != manifest: raise ValueError('Manifest identity changed; use a fresh output directory')
        return prior
    for case in cases: atomic(root/'configs'/case['workload']/(case['case']+'.json'), case['config'])
    atomic(path, manifest)
    return manifest


def proc_io():
    return {k:int(v.strip()) for k,v in (line.split(':') for line in Path('/proc/self/io').read_text().splitlines())}


def verify_done(directory, identity):
    mark=directory/'COMPLETE.json'
    if not mark.exists(): return False
    value=json.loads(mark.read_text())
    if value['identity'] != identity: raise ValueError('Completed case identity mismatch: '+str(directory))
    for p,h in value['artifacts'].items():
        if not (directory/p).is_file() or sha(directory/p)!=h: raise ValueError('Corrupt completed case: '+str(directory/p))
    return True


def run_case(root, case, inputs, manifest, kind='formal', limit_ms=None, repeat=False, resume=False, optimized=True):
    cfg=CapacityStudyConfig(**case['config'])
    if 'source_sha256' in manifest and source_identity()!=manifest['source_sha256']:
        raise ValueError('Source/config/tests changed since manifest was frozen')
    selected=[r for r in inputs if r.arrival_ms < (limit_ms or cfg.visibility_end_ms)]
    identity=dict(engine_variant='optimized' if optimized else 'stage0', manifest_sha256=digest(manifest), config_sha256=digest(cfg.as_dict()), head=head(),
                  source_sha256=digest(source_identity()), kind=kind, replay_end_ms=limit_ms or cfg.visibility_end_ms,
                  deterministic_repetitions=2 if repeat else 1)
    directory=root/cfg.workload/case['case']
    if directory.exists():
        if resume and verify_done(directory,identity):
            print('SKIP verified complete '+str(directory),flush=True); return json.loads((directory/'performance.json').read_text())
        raise ValueError('Existing result is incomplete or resume not requested: '+str(directory))
    log=root/'logs'/cfg.workload/(case['case']+'.log');log.parent.mkdir(parents=True,exist_ok=True)
    checkpoint=root/'checkpoints'/cfg.workload/(case['case']+'.json')
    partial=directory.with_name(directory.name+'.partial.'+uuid.uuid4().hex)
    partial.mkdir(parents=True,exist_ok=False)
    run_id=uuid.uuid4().hex
    atomic(checkpoint,dict(status='RUNNING',partial=str(partial),run_id=run_id,identity=identity))
    start=time.perf_counter();usage=resource.getrusage(resource.RUSAGE_SELF);io=proc_io()
    try:
        with log.open('a',buffering=1) as stream, redirect_stdout(stream), redirect_stderr(stream):
            print('START',identity,flush=True)
            replay_round=1
            def progress(done,total,now):
                elapsed=time.perf_counter()-start
                overall_done=(replay_round-1)*total+done
                overall_total=total*(2 if repeat else 1)
                eta=elapsed*(overall_total-overall_done)/overall_done
                print(f'replay={replay_round} processed={done}/{total} simulation_ms={now} elapsed={elapsed:.2f}s ETA={eta:.2f}s RSS_peak_MiB={resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024:.1f}',flush=True)
            from .optimized import OptimizedStudyEngine
            engine_class=OptimizedStudyEngine if optimized else CapacityStudyEngine
            engine=engine_class(cfg)
            result=engine.run(selected,progress_callback=progress,progress_every_requests=250)
            diagnostics=build(result,engine,selected,limit_ms or cfg.visibility_end_ms)
            checks=validate(result,engine,selected,diagnostics)
            result_hash=digest(asdict(result));diagnostic_hash=digest(diagnostics)
            if repeat:
                replay_round=2
                print('DETERMINISM_SECOND_REPLAY',flush=True)
                second_engine=engine_class(cfg); second=second_engine.run(selected,progress_callback=progress,progress_every_requests=250)
                second_diagnostics=build(second,second_engine,selected,limit_ms or cfg.visibility_end_ms)
                assert result_hash==digest(asdict(second)), 'Replay nondeterminism'
                assert diagnostic_hash==digest(second_diagnostics), 'Diagnostics nondeterminism'
            write_outputs(partial/'replay',result)
            atomic(partial/'stage1_diagnostics.json',diagnostics)
            atomic(partial/'config.json',cfg.as_dict())
            atomic(partial/'identity.json',dict(identity,workload=cfg.workload,routing=cfg.routing_policy,num_pods=cfg.num_pods,
                capacity_mode=cfg.capacity_mode,capacity_pages=cfg.capacity_pages,run_id=run_id,hostname=socket.gethostname(),
                semantic_version=cfg.study_version,evaluation_interval_ms=[cfg.evaluation_start_ms,cfg.evaluation_end_ms],
                result_sha256=result_hash,diagnostics_sha256=diagnostic_hash))
            atomic(partial/'stage1_validation.json',dict(status='PASS',checks=checks,deterministic=repeat,
                   result_sha256=result_hash,diagnostics_sha256=diagnostic_hash))
            elapsed=time.perf_counter()-start;after=resource.getrusage(resource.RUSAGE_SELF);after_io=proc_io()
            cpu=after.ru_utime+after.ru_stime-usage.ru_utime-usage.ru_stime
            perf=dict(wall_seconds=elapsed,cpu_seconds=cpu,cpu_utilization_percent=100*cpu/elapsed,
                peak_rss_mib=after.ru_maxrss/1024,io_delta={k:after_io[k]-io[k] for k in io},
                request_count=len(selected),repetitions=2 if repeat else 1,
                rss_scope='process lifetime high-water mark',includes='replay, diagnostics, determinism if requested, result serialization')
            atomic(partial/'performance.json',perf)
            if head()!=identity['head'] or digest(source_identity())!=identity['source_sha256']:
                raise ValueError('Source or HEAD changed during case; output remains partial')
            artifacts={str(p.relative_to(partial)):sha(p) for p in partial.rglob('*') if p.is_file()}
            atomic(partial/'COMPLETE.json',dict(status='PASS',identity=identity,artifacts=artifacts))
            os.rename(partial,directory)
            atomic(checkpoint,dict(status='COMPLETE',directory=str(directory),run_id=run_id,identity=identity))
        print(f'COMPLETE {cfg.workload}/{case["case"]} wall={elapsed:.2f}s CPU={perf["cpu_utilization_percent"]:.1f}% peak_RSS={perf["peak_rss_mib"]:.1f}MiB',flush=True)
        return perf
    except BaseException as exc:
        atomic(checkpoint,dict(status='FAILED',partial=str(partial),error=repr(exc),identity=identity))
        with log.open('a') as f: traceback.print_exc(file=f)
        raise


def validate(result,engine,inputs,diagnostics):
    records=result.request_records
    checks=dict(stage0=result.validation['status']=='PASS',
        request_ids={r.request_id for r in records}=={r.request_id for r in inputs} and len(records)==len(inputs),
        tokens=sum(r.input_tokens for r in records)==sum(r.input_tokens for r in inputs),
        saved_miss_conservation=all(r.final_hit_tokens+r.miss_tokens==r.input_tokens for r in records),
        no_proactive=not result.proactive_action_records and all(t.type=='REACTIVE' for t in result.transfer_records),
        occupancy=all(row['physical']==row['published']+row['temporary'] and 0<=row['physical']<=c.capacity_pages
                      for c in engine.caches for row in c.occupancy),
        infinite_no_eviction_or_reject=engine.config.capacity_mode!='infinite' or
            (not engine.capacity_failures and not any(c.eviction_records for c in engine.caches) and not any(r.cache_admission_skip for r in records)),
        all_pods=all(len(p['per_pod'])==16 for p in diagnostics['phases']),
        no_skipped_or_truncated=all(p['core']['request_skipped_due_to_capacity']==p['core']['request_truncated_due_to_capacity']==0 for p in diagnostics['phases']))
    canonical(diagnostics)  # Reject NaN/Infinity, but allow defined null denominators.
    assert all(checks.values()),checks
    return checks


def write_csv(path, rows):
    if not rows:return
    fields=sorted(set().union(*(r.keys() for r in rows)))
    tmp=path.with_suffix('.csv.tmp')
    with tmp.open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader()
        writer.writerows({k:canonical(v) if isinstance(v,(dict,list)) else v for k,v in r.items()} for r in rows)
    os.replace(tmp,path)


def collect(root,manifest):
    tables={k:[] for k in ['baseline_capacity','per_pod','timeseries','reactive_diagnostics']};complete=[];missing=[]
    for case in manifest['cases']:
        d=root/case['workload']/case['case']
        if not (d/'COMPLETE.json').exists():missing.append(case['workload']+'/'+case['case']);continue
        mark=json.loads((d/'COMPLETE.json').read_text())
        if mark['identity']['manifest_sha256']!=digest(manifest):raise ValueError('Mixed manifests')
        verify_done(d,mark['identity'])
        if mark['identity']['kind'] != 'run': raise ValueError('Non-formal case cannot enter formal aggregate')
        if mark['identity']['source_sha256']!=digest(manifest['source_sha256']):raise ValueError('Mixed case source identities')
        data=json.loads((d/'stage1_diagnostics.json').read_text());cfg=case['config']
        meta={k:cfg[k] for k in ['workload','routing_policy','capacity_mode','capacity_pages','num_pods']}
        for phase in data['phases']:
            window={k:phase[k] for k in ['scope','start_ms','end_ms']}
            tables['baseline_capacity'].append(dict(meta,**window,**phase['core']))
            tables['reactive_diagnostics'].append(dict(meta,**window,**phase['reactive']))
            tables['per_pod'].extend(dict(meta,**window,**pod) for pod in phase['per_pod'])
        for phase in data['five_minute']:
            tables['timeseries'].append(dict(meta,**phase))
        complete.append(dict(case=case['workload']+'/'+case['case'],performance=json.loads((d/'performance.json').read_text()),
                             validation=json.loads((d/'stage1_validation.json').read_text())))
    for name,rows in tables.items():write_csv(root/('stage1_'+name+'.csv'),rows)
    result=dict(status='PASS' if len(complete)==24 else 'INCOMPLETE',completed=len(complete),expected=24,missing=missing,cases=complete)
    atomic(root/'stage1_validation.json',result)
    print(f'{result["status"]}: {len(complete)}/24 cases',flush=True)


_WORKER_CONTEXT = None


def _worker(case):
    root, inputs, manifest, options = _WORKER_CONTEXT
    return run_case(root, case, inputs, manifest, **options)


def main():
    os.chdir(ROOT)
    parser=argparse.ArgumentParser();parser.add_argument('mode',choices=['prepare','benchmark','pilot','run','collect','status'])
    parser.add_argument('--root',type=Path,default=DEFAULT);parser.add_argument('--workload',choices=WORKLOADS)
    parser.add_argument('--resume',action='store_true');parser.add_argument('--repeat',action='store_true')
    parser.add_argument('--limit-ms',type=int,default=120000)
    parser.add_argument('--workers',type=int,default=1)
    parser.add_argument('--reference-engine',action='store_true')
    parser.add_argument('--preflight',type=Path)
    args=parser.parse_args()
    if args.workers < 1: parser.error('--workers must be positive')
    root=args.root.resolve();root.mkdir(parents=True,exist_ok=True)
    with (root/'.runner.lock').open('w') as lock:
        if args.mode!='status': fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        manifest=json.loads((root/'manifest.json').read_text()) if args.mode=='status' else prepare(root)
        if args.mode in ['prepare','status']:
            states=[]
            for case in manifest['cases']:
                d=root/case['workload']/case['case']; checkpoint=root/'checkpoints'/case['workload']/(case['case']+'.json')
                state='NOT_STARTED'
                if (d/'COMPLETE.json').exists():
                    mark=json.loads((d/'COMPLETE.json').read_text())
                    verify_done(d,mark['identity']);state='COMPLETE_VERIFIED'
                elif checkpoint.exists():
                    record=json.loads(checkpoint.read_text())
                    state='INCOMPLETE_'+record['status']
                states.append(dict(workload=case['workload'],case=case['case'],status=state))
            print(json.dumps(dict(head=head(),cases=states,completed=sum(s['status']=='COMPLETE_VERIFIED' for s in states)),indent=2));return
        if args.mode=='collect':collect(root,manifest);return
        if args.workload is None:parser.error('--workload is required')
        cfg=config_for(args.workload,'R_AFF',585)
        assert sha(ROOT/cfg.trace_path)==cfg.trace_sha256,'Trace identity changed'
        inputs=read_trace(ROOT/cfg.trace_path)
        cases=[c for c in manifest['cases'] if c['workload']==args.workload]
        if args.mode=='benchmark':cases=[c for c in cases if c['config']['routing_policy']=='R_REQ_KV_TASK' and c['config']['capacity_pages']==1170]
        if args.mode=='pilot':cases=[c for c in cases if (c['config']['routing_policy'],c['config']['capacity_pages']) in [('R_AFF',585),('R_REQ_KV_TASK',585),('R_REQ_KV_TASK',None)]]
        if args.mode=='run':
            if args.preflight is None: parser.error('formal run requires --preflight receipt')
            receipt=json.loads(args.preflight.read_text())
            if receipt.get('status')!='PASS' or receipt.get('source_sha256')!=source_identity():
                raise ValueError('Preflight missing/stale; rerun tests and pilot for current source')
        options=dict(kind=args.mode,limit_ms=args.limit_ms if args.mode=='pilot' else None,
                     repeat=args.repeat or args.mode=='pilot',resume=args.resume,optimized=not args.reference_engine)
        start=time.perf_counter()
        global _WORKER_CONTEXT
        _WORKER_CONTEXT=(root,inputs,manifest,options)
        if args.workers==1:
            for i,case in enumerate(cases,1):
                elapsed=time.perf_counter()-start
                print(f'[{i}/{len(cases)}] {args.workload} {case["case"]} total_elapsed={elapsed:.1f}s ETA={elapsed/(i-1)*(len(cases)-i+1) if i>1 else "pending"}',flush=True)
                _worker(case)
        else:
            # Fork shares immutable parsed trace via COW; only independent cases run concurrently.
            with ProcessPoolExecutor(max_workers=args.workers,mp_context=multiprocessing.get_context('fork')) as pool:
                futures={pool.submit(_worker,c):c for c in cases}
                for i,future in enumerate(as_completed(futures),1):
                    future.result()
                    elapsed=time.perf_counter()-start
                    print(f'[{i}/{len(cases)}] complete {futures[future]["case"]} total_elapsed={elapsed:.1f}s ETA={elapsed/i*(len(cases)-i):.1f}s',flush=True)
        if args.mode=='run':collect(root,manifest)


if __name__=='__main__':main()
