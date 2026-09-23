"""Single-pass runner. No repeat/pilot/sample-replay option exists."""
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import redirect_stdout, redirect_stderr
from dataclasses import asdict
import fcntl
import gzip
import json
import multiprocessing
import os
from pathlib import Path
import pickle
import resource
import shutil
import socket
import sys
import time
import traceback
import uuid

from src.simulator.task_main.history import FutureDemandIndex
from src.simulator.task_main.stage2.config import Stage2Config
from src.simulator.task_main.stage2.engine import Stage2Engine
from src.simulator.task_main.stage2.index import static_metadata
from src.simulator.task_main.trace import read_trace
from .common import (ROOT, DEFAULT, WORKLOADS, atomic, canonical, digest, sha, head,
                     prepare, source_identity, case_identity, verify_done)
from .diagnostics import build, validate


def save_checkpoint(directory, identity, engine, elapsed):
    directory.mkdir(parents=True, exist_ok=True)
    if engine.proactive and hasattr(engine.proactive.decisions, 'flush'):
        engine.proactive.decisions.flush(force=True)
    path = directory/('engine.'+uuid.uuid4().hex+'.pickle.gz')
    with path.open('wb') as raw:
        with gzip.GzipFile(fileobj=raw, mode='wb', compresslevel=1) as f:
            pickle.dump(engine, f, protocol=5)
        raw.flush()
        os.fsync(raw.fileno())
    mark = directory/'resume.json'
    previous = json.loads(mark.read_text()) if mark.exists() else None
    atomic(mark, dict(identity=identity, engine=path.name, sha256=sha(path), elapsed_seconds=elapsed,
                      processed=getattr(engine, '_cursor', 0), simulation_ms=getattr(engine, '_now', 0)))
    if previous:
        (directory/previous['engine']).unlink()


def load_checkpoint(directory, identity):
    mark = json.loads((directory/'resume.json').read_text())
    if mark['identity'] != identity or sha(directory/mark['engine']) != mark['sha256']:
        raise ValueError('checkpoint identity/hash mismatch')
    # Only locally produced, identity-verified checkpoint files are accepted.
    with gzip.open(directory/mark['engine'], 'rb') as f:
        engine = pickle.load(f)
    if engine.proactive and hasattr(engine.proactive.decisions, 'segment_sha256'):
        for p, expected in engine.proactive.decisions.segment_sha256.items():
            if sha(p) != expected:
                raise ValueError('checkpoint candidate segment corrupt: ' + p)
    return engine, mark['elapsed_seconds']


def export_replay(directory, result):
    directory.mkdir(parents=True)
    for name in ('request_records', 'transfer_records', 'opportunity_records', 'event_records',
                 'proactive_action_records', 'candidate_decision_records',
                 'copy_observation_records', 'reuse_observation_records'):
        with gzip.open(directory/(name+'.jsonl.gz'), 'wt', compresslevel=1) as f:
            for row in getattr(result, name):
                f.write(canonical(row if isinstance(row, dict) else asdict(row))+'\n')
    for name in ('summary', 'validation', 'final_state'):
        with gzip.open(directory/(name+'.json.gz'), 'wt', compresslevel=1) as f:
            json.dump(getattr(result, name), f, sort_keys=True, allow_nan=False)


def run_case(root, case, inputs, metadata, future, manifest, resume, checkpoint_seconds, ordinal, workers, total_start):
    cfg = Stage2Config(**case['config'])
    identity = case_identity(manifest, case)
    directory = root/cfg.workload/case['case']
    if verify_done(directory, identity):
        print('SKIP COMPLETE (never replay) '+cfg.workload+'/'+case['case'], flush=True)
        return
    check = root/'checkpoints'/cfg.workload/case['case']
    check.mkdir(parents=True, exist_ok=True)
    log = root/'logs'/cfg.workload/(case['case']+'.log')
    log.parent.mkdir(parents=True, exist_ok=True)
    with (check/'case.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (check/'resume.json').exists():
            if not resume:
                raise ValueError('case has checkpoint; use --resume, never restart')
            engine, prior_elapsed = load_checkpoint(check, identity)
            engine._resumed_from = getattr(engine, '_cursor', 0)
        else:
            if directory.exists() or (check/'STARTED.json').exists():
                raise ValueError('started case lacks valid checkpoint; refusing silent replay')
            engine = Stage2Engine(cfg, metadata, future if cfg.proactive_policy == 'FUTURE_DEMAND' else None,
                                  audit_directory=check/'candidate_segments')
            prior_elapsed = 0.
            save_checkpoint(check, identity, engine, 0.)
            atomic(check/'STARTED.json', dict(identity=identity, host=socket.gethostname(), pid=os.getpid()))
        start = time.monotonic()
        last_checkpoint = start
        usage = resource.getrusage(resource.RUSAGE_SELF)
        last_index = getattr(engine, '_cursor', 0)
        partial = directory.with_name(directory.name+'.partial.'+uuid.uuid4().hex)
        try:
            with log.open('a', buffering=1) as stream, redirect_stdout(stream), redirect_stderr(stream):
                print('RESUME' if engine._has_run else 'START', identity, flush=True)
                def progress(done, total, now):
                    elapsed = prior_elapsed + time.monotonic()-start
                    eta = elapsed * (total-done)/done if done else None
                    message = dict(case_index=ordinal, cases_on_host=14, workload=cfg.workload,
                        policy=cfg.proactive_policy if case['family'] == 'ACTIVE' else cfg.routing_schedule,
                        capacity='infinite' if cfg.capacity_pages is None else cfg.capacity_pages,
                        processed=done, total=total, simulation_ms=now, case_elapsed_seconds=elapsed,
                        total_elapsed_seconds=time.monotonic()-total_start, eta_seconds=eta,
                        peak_rss_mib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024, workers=workers)
                    print(f'[case {ordinal} / 14] '+canonical(message), flush=True)
                    atomic(check/'progress.json', message)
                progress(getattr(engine, '_cursor', 0), len(inputs), getattr(engine, '_now', 0))
                def checkpoint(current, done, now):
                    nonlocal last_checkpoint, last_index
                    if time.monotonic()-last_checkpoint >= checkpoint_seconds or (done == len(inputs) and not current._ready):
                        save_checkpoint(check, identity, current, prior_elapsed+time.monotonic()-start)
                        last_checkpoint = time.monotonic()
                        last_index = done
                        print(f'CHECKPOINT processed={done} simulation_ms={now}', flush=True)
                result = engine.run(inputs, resume=engine._has_run, checkpoint_callback=checkpoint,
                                    progress_callback=progress, progress_every_requests=250)
                print('REPLAY_COMPLETE; saving single-pass evidence and calculating metrics; no second replay', flush=True)
                partial.mkdir(parents=True)
                export_replay(partial/'replay', result)
                diagnostics = build(result, engine, inputs)
                checks = validate(result, engine, inputs, diagnostics)
                with gzip.open(partial/'diagnostics.json.gz', 'wt', compresslevel=1) as f:
                    json.dump(diagnostics, f, sort_keys=True, allow_nan=False)
                atomic(partial/'config.json', cfg.as_dict())
                atomic(partial/'validation.json', dict(status='PASS', checks=checks, replay_count=1, deterministic_rerun=False))
                after = resource.getrusage(resource.RUSAGE_SELF)
                perf = dict(wall_seconds=prior_elapsed+time.monotonic()-start, session_wall_seconds=time.monotonic()-start,
                    session_cpu_seconds=after.ru_utime+after.ru_stime-usage.ru_utime-usage.ru_stime,
                    peak_rss_mib=after.ru_maxrss/1024, workers=workers, hostname=socket.gethostname(),
                    checkpoint_seconds=checkpoint_seconds, resumed_from_request=getattr(engine, '_resumed_from', 0),
                    last_checkpoint_request=last_index, requests=len(inputs), replay_count=1,
                    rss_scope='worker lifetime high water', head=manifest['head'])
                atomic(partial/'performance.json', perf)
                if head() != identity['head'] or source_identity() != manifest['source_sha256']:
                    raise ValueError('source or HEAD changed during execution')
                artifacts = {str(p.relative_to(partial)): sha(p) for p in partial.rglob('*') if p.is_file()}
                atomic(partial/'COMPLETE.json', dict(status='PASS', identity=identity, artifacts=artifacts))
                os.rename(partial, directory)
                atomic(check/'status.json', dict(status='COMPLETE', directory=str(directory), identity=identity))
                # The atomic COMPLETE output contains every diagnostic row. Keep
                # the resume receipt, remove only this case's redundant segments.
                if (check/'candidate_segments').exists():
                    shutil.rmtree(check/'candidate_segments')
                for path in check.glob('engine.*.pickle.gz'):
                    path.unlink()
            print(f'COMPLETE [case {ordinal} / 14] {cfg.workload}/{case["case"]} elapsed={perf["wall_seconds"]:.1f}s', flush=True)
        except BaseException as exc:
            atomic(check/'status.json', dict(status='FAILED_RESUMABLE', error=repr(exc), identity=identity))
            with log.open('a') as f:
                traceback.print_exc(file=f)
            raise


_CONTEXT = None


def worker(item):
    case, ordinal = item
    root, inputs, metadata, future, manifest, options = _CONTEXT
    return run_case(root, case, inputs, metadata, future, manifest, ordinal=ordinal, **options)


def status(root):
    manifest = json.loads((root/'manifest.json').read_text())
    for workload in WORKLOADS:
        complete = []
        for case in (c for c in manifest['cases'] if c['workload'] == workload):
            d = root/workload/case['case']
            if (d/'COMPLETE.json').exists():
                complete.append(case['case'])
            else:
                p = root/'checkpoints'/workload/case['case']/'progress.json'
                if p.exists():
                    print(workload, case['case'], p.read_text().strip())
        print(workload, f'{len(complete)}/14 COMPLETE', ', '.join(complete))


def main():
    os.chdir(ROOT)
    p = argparse.ArgumentParser()
    p.add_argument('mode', choices=['prepare', 'run', 'status', 'collect'])
    p.add_argument('--root', type=Path, default=DEFAULT)
    p.add_argument('--preflight', type=Path, default=ROOT/'results/task_main/stage2_active_capacity/preflight/preflight.json')
    p.add_argument('--workload', choices=WORKLOADS)
    p.add_argument('--workers', type=int, default=4)
    p.add_argument('--checkpoint-seconds', type=int, default=600)
    p.add_argument('--resume', action='store_true')
    args = p.parse_args()
    root = args.root.resolve()
    if args.mode == 'status':
        return status(root)
    if args.mode == 'collect':
        from .collect import collect
        return collect(root)
    manifest = prepare(root, args.preflight)
    if args.mode == 'prepare':
        print('PREPARED 28 new cases; no replay started', root)
        return
    if not args.workload or args.workers < 1 or args.checkpoint_seconds < 1:
        p.error('run requires workload, positive workers and checkpoint interval')
    global _CONTEXT
    cases = [c for c in manifest['cases'] if c['workload'] == args.workload]
    todo = [(c, i) for i, c in enumerate(cases, 1) if not verify_done(root/c['workload']/c['case'], case_identity(manifest, c))]
    if not todo:
        print('14/14 COMPLETE; no replay or trace parsing')
        return
    cfg = Stage2Config(**cases[0]['config'])
    if sha(ROOT/cfg.trace_path) != cfg.trace_sha256:
        raise ValueError('trace hash differs from frozen Stage 1 input')
    started = time.monotonic()
    host_path = root/'hosts'/(args.workload+'.json')
    host = json.loads(host_path.read_text()) if host_path.exists() else dict(sessions=[])
    host.update(head=manifest['head'], workload=args.workload, hostname=socket.gethostname(), workers=args.workers,
                cpu_quota=Path('/sys/fs/cgroup/cpu.max').read_text().strip(),
                memory_limit_bytes=Path('/sys/fs/cgroup/memory.max').read_text().strip(),
                free_disk_bytes_at_start=shutil.disk_usage(root).free)
    host['sessions'].append(dict(start_epoch=time.time(), status='RUNNING', cases_scheduled=len(todo)))
    atomic(host_path, host)
    inputs = [r for r in read_trace(ROOT/cfg.trace_path) if r.arrival_ms < cfg.visibility_end_ms]
    metadata = static_metadata(inputs)
    future = FutureDemandIndex(inputs, cfg.visibility_end_ms)
    print('INPUT_READY', len(inputs), 'requests', len(metadata[0]), 'full-page identities; one parse, fork-shared metadata', flush=True)
    _CONTEXT = (root, inputs, metadata, future, manifest,
        dict(resume=args.resume, checkpoint_seconds=args.checkpoint_seconds, workers=args.workers, total_start=started))
    # maxtasksperchild semantics: release a large completed case before loading
    # the next batch. Fork preserves immutable parsed trace and prefix metadata.
    errors = []
    for offset in range(0, len(todo), args.workers):
        batch = todo[offset:offset+args.workers]
        with ProcessPoolExecutor(max_workers=min(args.workers, len(batch)), mp_context=multiprocessing.get_context('fork')) as pool:
            futures = {pool.submit(worker, item): item[0]['case'] for item in batch}
            for task in as_completed(futures):
                try:
                    task.result()
                except Exception as exc:
                    errors.append((futures[task], repr(exc)))
                    print('FAILED', futures[task], repr(exc), flush=True)
        if errors:
            host['sessions'][-1].update(status='FAILED_RESUMABLE', elapsed_seconds=time.monotonic()-started, errors=errors)
            atomic(host_path, host)
            raise RuntimeError(errors)
    host['sessions'][-1].update(status='COMPLETE', elapsed_seconds=time.monotonic()-started, end_epoch=time.time())
    atomic(host_path, host)
    print(args.workload, '14/14 COMPLETE; run collect on Server A after fetching the other workload', flush=True)


if __name__ == '__main__':
    main()
