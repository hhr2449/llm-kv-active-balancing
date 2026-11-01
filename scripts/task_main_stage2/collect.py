"""Merge immutable Stage 1 evidence and 28 COMPLETE single-replay Stage 2 cases."""
import csv
import gzip
import json
from pathlib import Path

from .common import ROOT, STAGE1, CONFIGS, atomic, sha, digest, write_csv, case_identity, verify_done


def read_csv(path):
    def value(s):
        if s == '':
            return None
        try:
            return json.loads(s)
        except (ValueError, TypeError):
            return s
    with path.open() as f:
        return [{k: value(v) for k, v in row.items()} for row in csv.DictReader(f)]


def enrich_baseline(row):
    row = dict(row)
    row.update(strategy=row['routing_policy'], family='BASELINE', provenance='STAGE1_REUSED', stage2_replays=0)
    row['total_wire_bytes'] = row['reactive_wire_bytes']
    row['proactive_wire_bytes'] = 0
    return row


DIFFERENCES = ('saved_prefill_tokens', 'weighted_saved_tokens', 'token_hit_rate', 'total_miss_prefill_tokens',
               'skew_ratio', 'raw_request_gini', 'top1_request_share', 'active_pod_count',
               'workload_gini', 'max_pod_workload_share', 'total_wire_bytes', 'reactive_wire_bytes', 'proactive_wire_bytes')


def differences(left, right):
    return {k: None if left.get(k) is None or right.get(k) is None else left[k]-right[k] for k in DIFFERENCES}


def collect(root):
    root = Path(root)
    manifest = json.loads((root/'manifest.json').read_text())
    reference = json.loads((CONFIGS/'stage1_reference.json').read_text())
    if sha(CONFIGS/'stage1_reference.json') != manifest['stage1_reference_sha256']:
        raise ValueError('baseline reference changed')
    for p, h in reference['artifact_sha256'].items():
        if sha(ROOT/p) != h:
            raise ValueError('Stage 1 artifact hash mismatch: '+p)
    if sha(STAGE1/'manifest.json') != reference['stage1_manifest_sha256']:
        raise ValueError('Stage 1 manifest changed')
    missing = []
    for case in manifest['cases']:
        if not verify_done(root/case['workload']/case['case'], case_identity(manifest, case)):
            missing.append(case['workload']+'/'+case['case'])
    if missing:
        atomic(root/'stage2_validation.json', dict(status='INCOMPLETE', expected=28, completed=28-len(missing), missing=missing))
        print('INCOMPLETE', 28-len(missing), '/ 28; no final report generated')
        return
    baseline = read_csv(STAGE1/'stage1_baseline_capacity.csv')
    assert len(baseline) == 96
    phases = [enrich_baseline(row) for row in baseline]
    pods = [dict(r, strategy=r['routing_policy'], family='BASELINE', provenance='STAGE1_REUSED') for r in read_csv(STAGE1/'stage1_per_pod.csv')]
    series = [dict(r, strategy=r['routing_policy'], family='BASELINE', provenance='STAGE1_REUSED') for r in read_csv(STAGE1/'stage1_timeseries.csv')]
    reactive = [dict(r, strategy=r['routing_policy'], family='BASELINE', provenance='STAGE1_REUSED') for r in read_csv(STAGE1/'stage1_reactive_diagnostics.csv')]
    reactive_index = {(r['workload'], r['capacity_pages'], r['strategy'], r['scope']): r for r in reactive}
    for row in phases:
        evidence = reactive_index[row['workload'], row['capacity_pages'], row['strategy'], row['scope']]
        if 'wire_pages' in evidence:
            row.update(total_wire_pages=evidence['wire_pages'], reactive_wire_pages=evidence['wire_pages'],
                       proactive_wire_pages=0, total_wire_tokens=evidence['wire_pages']*512,
                       reactive_wire_tokens=evidence['wire_pages']*512, proactive_wire_tokens=0)
        members = [p for p in pods if (p['workload'], p['capacity_pages'], p['strategy'], p['scope']) ==
                   (row['workload'], row['capacity_pages'], row['strategy'], row['scope'])]
        for cause in ('request', 'reactive_transfer', 'proactive_transfer'):
            for suffix in ('eviction_events', 'evicted_pages'):
                key = cause+'_'+suffix
                row[key] = sum(p.get(key, 0) for p in members)
    evaluation_file = STAGE1/'stage1_evaluation_24_cases.csv'
    if evaluation_file.exists():
        evaluation = read_csv(evaluation_file)
        assert len(evaluation) == 24
        for row in evaluation:
            cap = None if row['capacity_pages'] in {None, 'infinite'} else row['capacity_pages']
            preserved = next(r for r in phases if (r['workload'], r['capacity_pages'], r['strategy'], r['scope']) ==
                             (row['workload'], cap, row['routing_policy'], 'EVALUATION'))
            for metric in ('saved_prefill_tokens', 'weighted_saved_tokens', 'token_hit_rate', 'total_miss_prefill_tokens'):
                assert row[metric] == preserved[metric], 'Stage 1 evaluation table mismatch'
    proactive, utilization, utilization_summary, replicas, performance = [], [], [], [], []
    for case in manifest['cases']:
        cfg = case['config']
        directory = root/case['workload']/case['case']
        validation = json.loads((directory/'validation.json').read_text())
        if validation['status'] != 'PASS' or validation['replay_count'] != 1 or not all(validation['checks'].values()):
            raise ValueError('case validation failed: '+str(directory))
        with gzip.open(directory/'diagnostics.json.gz', 'rt') as f:
            data = json.load(f)
        meta = dict(workload=cfg['workload'], num_pods=16, capacity_pages=cfg['capacity_pages'],
                    capacity_mode=cfg['capacity_mode'], routing_policy=cfg['routing_policy'],
                    strategy=cfg['proactive_policy'] if case['family'] == 'ACTIVE' else cfg['routing_schedule'],
                    family=case['family'], provenance='STAGE2_SINGLE_REPLAY', case=case['case'], stage2_replays=1)
        performance.append(dict(meta, **json.loads((directory/'performance.json').read_text())))
        for row in data['phases']:
            scope = {k: row[k] for k in ('scope', 'start_ms', 'end_ms')}
            phases.append(dict(meta, **scope, **row['core']))
            pods.extend(dict(meta, **scope, **pod) for pod in row['per_pod'])
            reactive.append(dict(meta, **scope, **row['reactive']))
            proactive.append(dict(meta, **scope, **row['proactive']))
            utilization_summary.append(dict(meta, **scope, **row['utilization']))
        series.extend(dict(meta, **row) for row in data['five_minute'])
        utilization.extend(dict(meta, **row) for row in data['copy_utilization'])
        replicas.append(dict(meta, **data['effective_replica_distribution'], hotspot_coverage=data['hotspot_coverage']))
    # Import existing replica snapshots; this reads artifacts and never simulates.
    for workload in ('conversation', 'toolagent'):
        for cap in ('585', '1170', '2340', 'infinite'):
            for route in ('R_AFF', 'R_LEAST', 'R_REQ_KV_TASK'):
                d = STAGE1/workload/(route+'__'+cap)
                mark = json.loads((d/'COMPLETE.json').read_text())
                expected = mark['artifacts']['stage1_diagnostics.json']
                if sha(d/'stage1_diagnostics.json') != expected:
                    raise ValueError('baseline replica evidence hash mismatch')
                data = json.loads((d/'stage1_diagnostics.json').read_text())
                replicas.append(dict(workload=workload, capacity_pages=None if cap == 'infinite' else int(cap),
                    strategy=route, family='BASELINE', provenance='STAGE1_REUSED',
                    **data['effective_replica_distribution'], hotspot_coverage=data['hotspot_coverage']))
    # Full phase cost accounting, including inherited reactive Wire.
    req = {(r['workload'], r['capacity_pages'], r['scope']): r for r in phases if r['strategy'] == 'R_REQ_KV_TASK'}
    for row in phases:
        if row['family'] != 'ABLATION':
            delta = differences(row, req[row['workload'], row['capacity_pages'], row['scope']])
            row.update({'delta_'+k+'_vs_req': v for k, v in delta.items()})
    capacity = [r for r in phases if r['scope'] == 'EVALUATION' and r['family'] != 'ABLATION']
    assert len(capacity) == 48 and sum(r['family'] == 'ACTIVE' for r in capacity) == 24
    assert len(phases) == 208 and len(pods) == 3328
    for row in phases:
        baseline_row = req[row['workload'], row['capacity_pages'], row['scope']]
        assert (row['request_count'], row['total_input_tokens']) == (
            baseline_row['request_count'], baseline_row['total_input_tokens']), 'different request/token cohort'
    ablation = []
    interpretations = {'C-A': 'early reactive historical-state effect with AFF during evaluation',
        'B-D': 'early reactive historical-state effect with REQ during evaluation',
        'B-C': 'continue reactive after early REQ history',
        'D-A': 'enable reactive after early AFF history'}
    for workload in ('conversation', 'toolagent'):
        for scope in ('PRE_EVAL', 'EVALUATION', 'POST_EVAL', 'FULL_RUN'):
            selected = {}
            for label, strategy in [('A', 'R_AFF'), ('B', 'R_REQ_KV_TASK'), ('C', 'C'), ('D', 'D')]:
                row = next(r for r in phases if (r['workload'], r['capacity_pages'], r['scope'], r['strategy']) == (workload, 2340, scope, strategy))
                selected[label] = row
                ablation.append(dict(row, row_type='CASE', ablation=label))
            for contrast, meaning in interpretations.items():
                left, right = contrast.split('-')
                ablation.append(dict(workload=workload, capacity_pages=2340, scope=scope, row_type='CONTRAST',
                                     ablation=contrast, effect='reactive intervention effect', interpretation=meaning,
                                     **differences(selected[left], selected[right])))
    tables = dict(stage2_capacity_6strategy=capacity,
        stage2_active_only=[r for r in capacity if r['family'] == 'ACTIVE'], stage2_phase_metrics=phases,
        stage2_per_pod=pods, stage2_timeseries=series, stage2_proactive_diagnostics=proactive,
        stage2_copy_utilization=utilization, stage2_copy_utilization_summary=utilization_summary,
        stage2_reactive_ablation=ablation, stage2_reactive_diagnostics=reactive,
        stage2_replica_layout=replicas, stage2_performance=performance)
    for name, rows in tables.items():
        write_csv(root/(name+'.csv'), rows)
    receipt = dict(status='STAGE2_ACTIVE_CAPACITY_PASS', active_completed=24, ablation_completed=4,
        reused_stage1_baselines=24, comparison_cases=48, ablation_comparison_cases=8,
        replay_count_per_new_case=1, head=manifest['head'],
        server_provenance=[{k: r[k] for k in ('workload', 'case', 'hostname', 'head', 'workers')} for r in performance],
        skipped=0, truncated=0,
        infinite_eviction_or_reject=sum(r['eviction_pages']+r['cache_admission_rejected']+r['capacity_preflight_failures']
            for r in capacity if r['capacity_pages'] is None),
        artifact_sha256={name+'.csv': sha(root/(name+'.csv')) for name in tables},
        stage1_reference_sha256=manifest['stage1_reference_sha256'], missing=[])
    receipt['host_sessions'] = {w: json.loads((root/'hosts'/(w+'.json')).read_text())
        if (root/'hosts'/(w+'.json')).exists() else {'status': 'HOST_TIMING_NOT_FETCHED'}
        for w in ('conversation', 'toolagent')}
    assert receipt['infinite_eviction_or_reject'] == 0
    assert all(p['head'] == manifest['head'] for p in performance)
    atomic(root/'stage2_validation.json', receipt)
    report(root, capacity, ablation, phases, utilization_summary, performance, receipt)
    print('STAGE2_ACTIVE_CAPACITY_PASS: 24 active + 4 ablation; 24 baselines reused')


def report(root, capacity, ablation, phases, utilization, performance, receipt):
    def table(rows, fields):
        def fmt(v):
            if v is None:
                return 'null'
            if isinstance(v, float):
                return f'{v:.6g}'
            return str(v)
        return ['| '+' | '.join(fields)+' |', '| '+' | '.join(['---']*len(fields))+' |'] + [
            '| '+' | '.join(fmt(row.get(k)) for k in fields)+' |' for row in rows]
    lines = ['# Stage 2 Results', '', '**STAGE2_ACTIVE_CAPACITY_PASS**', '',
        '24 个 proactive + 4 个 reactive 消融 case 均完成一次 replay；24 个 Stage 1 基线直接复用。',
        f'HEAD：`{receipt["head"]}`。skipped=0、truncated=0；infinite eviction/reject=0。', '',
        '评价窗口 [25,45) min，完整 replay 至 3537000 ms 并排空 transfer。没有在 25/45 分钟重置状态。', '',
        'Oracle 是当前 Future-Demand Reference，不是 global optimum。正、零、负差值均按观测保留；'
        'C_ref 只表示 REQ 接近其自身 infinite reuse 水平。WeightedSaved 是收益代理，不是真实 TTFT；'
        '收益差和 total Wire 成本差分别报告，不推断系统净收益。', '']
    core = ['capacity_pages', 'strategy', 'saved_prefill_tokens', 'delta_saved_prefill_tokens_vs_req',
        'token_hit_rate', 'weighted_saved_tokens', 'delta_weighted_saved_tokens_vs_req',
        'total_miss_prefill_tokens', 'delta_total_miss_prefill_tokens_vs_req', 'skew_ratio',
        'raw_request_gini', 'top1_request_share', 'active_pod_count', 'workload_gini',
        'max_pod_workload_share', 'eviction_pages']
    for workload in ('conversation', 'toolagent'):
        lines += ['## '+workload+'：六策略容量响应', ''] + table([r for r in capacity if r['workload'] == workload], core) + ['']
    lines += ['## 成本：按 transfer start cohort', ''] + table(
        [r for r in phases if r['scope'] in {'PRE_EVAL', 'EVALUATION', 'FULL_RUN'} and r['family'] != 'ABLATION'],
        ['workload', 'capacity_pages', 'strategy', 'scope', 'total_wire_bytes', 'reactive_wire_bytes',
         'proactive_wire_bytes', 'delta_total_wire_bytes_vs_req']) + ['']
    lines += ['## Infinite-capacity', '', '以下差值均相对于同 workload 的 REQ infinite 基线。', ''] + table(
        [r for r in capacity if r['capacity_pages'] is None and r['family'] == 'ACTIVE'],
        ['workload', 'strategy', 'delta_saved_prefill_tokens_vs_req', 'delta_total_miss_prefill_tokens_vs_req',
         'delta_skew_ratio_vs_req', 'delta_workload_gini_vs_req', 'total_wire_bytes', 'delta_total_wire_bytes_vs_req']) + ['']
    lines += ['## C_low / C_ref', '', '请求分布和 miss-prefill 工作量分布分别使用 request Gini 与 workload Gini。'
        '每 Pod share/occupancy/eviction 原因见 stage2_per_pod.csv；连续 occupancy 轨迹在各 case diagnostics.json.gz。'
        '共同 external 全页集合上的最终 replica 分布见 stage2_replica_layout.csv；hotspot coverage 未实现，不补跑。', ''] + table(
        [r for r in capacity if r['capacity_pages'] in {585, 2340}],
        ['workload', 'capacity_pages', 'strategy', 'mean_occupancy_pages', 'peak_pod_occupancy_pages',
         'mean_occupancy_gini', 'eviction_events', 'eviction_pages', 'raw_request_gini', 'workload_gini']) + ['']
    lines += ['## COPY 利用率', '', '按 action start cohort，观察 (ready, ready+300s]。完整观察且未用计 UNUSED，'
        '即使仍驻留；不足 300s 计 CENSORED。生命周期仍驻留且未用为 unresolved，和固定窗口统计独立。'
        'action/page/byte 分母在逐 action CSV 中明确给出。new-page observed use 对 censored cohort 是已观测下界。', ''] + table(
        [r for r in utilization if r['scope'] == 'EVALUATION' and r['family'] == 'ACTIVE'],
        ['workload', 'capacity_pages', 'strategy', 'action_count', 'used_300s_actions', 'unused_300s_actions',
         'right_censored_actions', 'unused_300s_wire_byte_ratio', 'still_resident_pages', 'lifecycle_unresolved_actions']) + ['']
    lines += ['## Reactive 时段消融', '', '差值统一解释为 reactive intervention effect，包含 routing/direct target/COPY '
        '及整个闭环历史状态，不单独归因于 cache placement 或 COPY。PRE_EVAL/POST_EVAL/FULL_RUN 及 gate/start/ready '
        'cohort 见两个 reactive CSV。', ''] + table([r for r in ablation if r['scope'] == 'EVALUATION'],
        ['workload', 'row_type', 'ablation', *DIFFERENCES, 'interpretation']) + ['']
    lines += ['## 性能与身份', '', 'wall_seconds 包含有效运行会话的 replay/checkpoint/导出/指标时间；'
        '并行 case 时间之和不是主机墙钟时间。checkpoint 每 600s、在完整时间戳批次之后保存；COMPLETE 永不重跑。', ''] + table(
        performance, ['workload', 'case', 'wall_seconds', 'session_cpu_seconds', 'peak_rss_mib', 'workers', 'hostname', 'head']) + ['']
    lines += ['主机墙钟会话时间与 CPU/RAM 配额：', '', '```json',
              json.dumps(receipt['host_sessions'], indent=2, ensure_ascii=False), '```', '']
    path = root/'stage2_results.md'
    path.write_text('\n'.join(lines))
