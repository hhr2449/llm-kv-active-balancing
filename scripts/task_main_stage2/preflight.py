"""Synthetic-only performance/equivalence and a portable unit-test receipt."""
import argparse
from dataclasses import fields
import json
from pathlib import Path
import resource
import time
import xml.etree.ElementTree as ET

from src.simulator.task_main.trace import TraceRequest
from src.simulator.task_main.stage0.config import CapacityStudyConfig
from src.simulator.task_main.stage0.engine import CapacityStudyEngine
from src.simulator.task_main.stage2.config import Stage2Config
from src.simulator.task_main.stage2.engine import Stage2Engine
from .common import ROOT, atomic, sha, digest, source_identity, check_frozen, matrix


def synthetic_benchmark(output):
    rows = []
    for i in range(240):
        branch = i % 20
        path = tuple(branch*100+j for j in range(12))
        rows.append(TraceRequest.from_record(i, dict(timestamp=1490000+i*1000,
            input_length=len(path)*512, output_length=0, hash_ids=list(path))))
    evidence = []
    for policy in ('FUTURE_DEMAND', 'PERSISTENCE', 'RECENCY'):
        for capacity in (24, None):
            cfg = Stage2Config(proactive_policy=policy, capacity_pages=capacity,
                               capacity_mode='infinite' if capacity is None else 'finite', num_pods=16)
            ref_cfg = CapacityStudyConfig(**{f.name: getattr(cfg, f.name) for f in fields(CapacityStudyConfig)})
            times = []
            signatures = []
            for cls, config in ((CapacityStudyEngine, ref_cfg), (Stage2Engine, cfg)):
                start = time.monotonic()
                engine = cls(config)
                result = engine.run(rows)
                times.append(time.monotonic()-start)
                from dataclasses import asdict
                signatures.append(digest(dict(
                    records={key: [asdict(r) for r in getattr(result, key)] for key in
                        ('request_records', 'transfer_records', 'opportunity_records', 'event_records',
                         'proactive_action_records', 'candidate_decision_records')},
                    state=result.final_state, metrics=result.summary['final_metrics'])))
            assert signatures[0] == signatures[1]
            row = dict(policy=policy, capacity=capacity, requests=len(rows), pods=16,
                reference_seconds=times[0], indexed_seconds=times[1], speedup=times[0]/times[1],
                exact_signature=signatures[0], equivalence=True,
                peak_process_rss_mib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024)
            evidence.append(row)
            print(policy, capacity, 'synthetic exact match', f'speedup={row["speedup"]:.2f}x', flush=True)
    atomic(output, dict(status='PASS', kind='SYNTHETIC_ONLY', formal_cases_executed=0,
                        source_sha256=source_identity(), comparisons=evidence,
                        limitation='Small synthetic cases establish behavior and local costs, not formal runtime/RSS upper bounds.'))


def receipt(junit, benchmark, output):
    tree = ET.parse(junit).getroot()
    cases = tree.findall('.//testcase')
    if not cases or any(tree.findall('.//'+tag) for tag in ('failure', 'error', 'skipped')):
        raise ValueError('all unit/Stage 0 tests must pass with zero skips')
    if not any('stage2' in c.attrib.get('classname', '') for c in cases):
        raise ValueError('Stage 2 synthetic tests missing')
    if not any('stage0' in c.attrib.get('classname', '') for c in cases):
        raise ValueError('Stage 0 regressions missing')
    performance = json.loads(Path(benchmark).read_text())
    if performance['status'] != 'PASS' or performance['source_sha256'] != source_identity():
        raise ValueError('stale or failed synthetic benchmark')
    check_frozen()
    matrix()
    atomic(output, dict(status='STAGE2_PREFLIGHT_PASS', source_sha256=source_identity(),
        pytest_passed=len(cases), failed=0, skipped=0, junit_sha256=sha(junit),
        benchmark_sha256=sha(benchmark), synthetic_performance=performance,
        formal_cases_started=0, baseline_cases_rerun=0,
        expected_new_cases=28, active_cases=24, ablation_cases=4,
        execution_rule='one replay per new case; COMPLETE always skipped; timestamp-batch checkpoint continuation'))
    print('STAGE2_PREFLIGHT_PASS', len(cases), 'tests; no formal replay started')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('mode', choices=['benchmark', 'receipt'])
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--junit', type=Path)
    p.add_argument('--benchmark', type=Path)
    a = p.parse_args()
    if a.mode == 'benchmark':
        synthetic_benchmark(a.output)
    else:
        receipt(a.junit, a.benchmark, a.output)


if __name__ == '__main__':
    main()
