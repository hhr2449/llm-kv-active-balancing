import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess
import uuid

from src.simulator.task_main.stage2.config import Stage2Config

ROOT = Path(__file__).resolve().parents[2]
DEFAULT = ROOT / 'results/task_main/stage2_active_capacity/run_20260923_01'
STAGE1 = ROOT / 'results/task_main/stage1_n16_capacity/run_20260922_01'
CONFIGS = ROOT / 'configs/task_main/stage2_active_capacity'
WORKLOADS = ('conversation', 'toolagent')
POLICIES = ('FUTURE_DEMAND', 'PERSISTENCE', 'RECENCY')
CAPACITIES = (585, 1170, 2340, None)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(1024*1024), b''):
            h.update(chunk)
    return h.hexdigest()


def atomic(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.tmp.' + uuid.uuid4().hex)
    with temp.open('w') as f:
        json.dump(value, f, indent=2, sort_keys=True, allow_nan=False)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)


def head():
    return subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()


def source_identity():
    paths = [*ROOT.glob('src/**/*.py'), *ROOT.glob('scripts/task_main_stage[012]/*.py'),
             *ROOT.glob('tests/task_main/*.py'), *CONFIGS.rglob('*.yaml'),
             CONFIGS/'stage1_reference.json']
    return {str(p.relative_to(ROOT)): sha(p) for p in sorted(paths)}


def check_frozen():
    reference = json.loads((CONFIGS/'stage1_reference.json').read_text())
    for p, h in reference['frozen_source_sha256'].items():
        if sha(ROOT/p) != h:
            raise ValueError('Stage 0/1 frozen file changed: ' + p)
    return reference


def matrix():
    cases = []
    for workload in WORKLOADS:
        for cap in CAPACITIES:
            for policy in POLICIES:
                name = policy + '__' + ('infinite' if cap is None else str(cap))
                cfg = Stage2Config.from_yaml(CONFIGS/workload/(name+'.yaml'))
                assert (cfg.workload, cfg.capacity_pages, cfg.proactive_policy, cfg.routing_schedule) == (workload, cap, policy, 'CONSTANT')
                cases.append(dict(workload=workload, case=name, config=cfg.as_dict(), family='ACTIVE'))
        for schedule in ('C', 'D'):
            name = 'ABLATION_' + schedule + '__2340'
            cfg = Stage2Config.from_yaml(CONFIGS/workload/(name+'.yaml'))
            assert cfg.workload == workload and cfg.routing_schedule == schedule
            cases.append(dict(workload=workload, case=name, config=cfg.as_dict(), family='ABLATION'))
    assert len(cases) == 28
    return cases


def prepare(root, receipt):
    root = Path(root).resolve()
    if not root.is_relative_to(ROOT/'results/task_main/stage2_active_capacity'):
        raise ValueError('output must be a new Stage 2 directory')
    check_frozen()
    preflight = json.loads(Path(receipt).read_text())
    if preflight['status'] != 'STAGE2_PREFLIGHT_PASS' or preflight['source_sha256'] != source_identity():
        raise ValueError('missing/stale Stage 2 preflight receipt')
    value = dict(version='ACTIVE_CAPACITY_STAGE2_V1', head=head(), source_sha256=source_identity(),
                 cases=matrix(), replay_count_per_case=1, determinism_rerun=False,
                 visibility_end_ms=3537000, evaluation_interval_ms=[1500000, 2700000],
                 stage1_reference_sha256=sha(CONFIGS/'stage1_reference.json'),
                 preflight_sha256=sha(receipt), checkpoint='complete timestamp batches',
                 preflight_replays='synthetic_only')
    path = root/'manifest.json'
    if path.exists():
        if json.loads(path.read_text()) != value:
            raise ValueError('manifest differs; do not mix HEAD/config/input identities')
    else:
        root.mkdir(parents=True, exist_ok=True)
        for name in ('logs', 'checkpoints', 'hosts'):
            (root/name).mkdir(exist_ok=True)
        for case in value['cases']:
            atomic(root/'configs'/case['workload']/(case['case']+'.json'), case['config'])
        atomic(path, value)
    return value


def case_identity(manifest, case):
    return dict(manifest_sha256=digest(manifest), config_sha256=digest(case['config']),
                source_sha256=digest(manifest['source_sha256']), head=manifest['head'],
                trace_sha256=case['config']['trace_sha256'], replay_count=1)


def verify_done(directory, identity):
    marker = directory/'COMPLETE.json'
    if not marker.exists():
        return False
    mark = json.loads(marker.read_text())
    if mark['identity'] != identity or mark['status'] != 'PASS':
        raise ValueError('completed case identity/status differs: ' + str(directory))
    for path, expected in mark['artifacts'].items():
        if sha(directory/path) != expected:
            raise ValueError('completed artifact hash mismatch: ' + str(directory/path))
    return True


def write_csv(path, rows):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted(set().union(*(r.keys() for r in rows))) if rows else ['status']
    tmp = path.with_name(path.name+'.tmp')
    with tmp.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: canonical(v) if isinstance(v, (dict, list, tuple)) else v for k, v in row.items()})
    os.replace(tmp, path)
