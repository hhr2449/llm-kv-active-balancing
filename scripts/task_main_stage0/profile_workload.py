"""Linear-reference, offline workload profiling. Never invokes a simulator."""
from __future__ import annotations

import argparse
from collections import Counter, deque
import csv
import hashlib
import json
from pathlib import Path
import resource
import time

from src.simulator.task_main.trace import read_trace, full_page_prefix
from src.simulator.task_main.history import bucket, WEIGHTS

ROOT = Path(__file__).resolve().parents[2]
BUCKETS = ('<5k', '[5k,20k)', '[20k,60k)', '[60k,120k)', '[120k,300k]', '>300k')


def quantiles(values):
    values = sorted(values)
    def q(frac):
        if not values:
            return None
        p = (len(values)-1)*frac
        i = int(p)
        return values[i] + (values[min(i+1, len(values)-1)]-values[i])*(p-i)
    return dict(p50=q(.5), p90=q(.9), p99=q(.99), max=max(values) if values else None)


def write_csv(path, rows):
    if not rows:
        return
    keys = list(dict.fromkeys(k for row in rows for k in row))
    with Path(path).open('w') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def profile(requests, workload, end_ms=3537000, progress=False):
    # read_trace validates globally unique hash -> (parent, depth, valid tokens).
    # Intern (parent node ID, block hash), never allocate O(depth^2) path tuples.
    nodes = {}
    paths = []
    for r in requests:
        parent = 0
        ids = []
        for block in full_page_prefix(r, len(r.block_ids)):
            key = (parent, block)
            parent = nodes.setdefault(key, len(nodes)+1)
            ids.append(parent)
        paths.append(tuple(ids))
    rows, buckets, refs_rows, ws_summary, series, reuse_rows = [], [], [], [], [], []
    scopes = [('FULL_REPLAY', 0, end_ms), ('EVALUATION', 1500000, 2700000)]
    previous = {}
    intervals = {scope: [] for scope, _, _ in scopes}
    for r, path in zip(requests, paths):
        if r.arrival_ms >= end_ms:
            continue
        for node in path:
            if node in previous:
                gap = r.arrival_ms-previous[node]
                for scope, lo, hi in scopes:
                    if lo <= r.arrival_ms < hi:
                        intervals[scope].append(gap)
            previous[node] = r.arrival_ms
    for scope, lo, hi in scopes:
        pairs = [(r, p) for r, p in zip(requests, paths) if lo <= r.arrival_ms < hi]
        rs = [r for r, _ in pairs]
        counts = Counter(n for _, path in pairs for n in path)
        n, tokens = len(rs), sum(r.input_tokens for r in rs)
        total, unique = sum(counts.values()), len(counts)
        row = dict(workload=workload, scope=scope, start_ms=lo, end_ms=hi,
                   request_count=n, input_tokens=tokens,
                   **{'prompt_tokens_'+k:v for k,v in quantiles([r.input_tokens for r in rs]).items()},
                   **{'prompt_pages_'+k:v for k,v in quantiles([len(r.block_ids) for r in rs]).items()},
                   unique_full_page_prefix_pages=unique, total_full_page_references=total,
                   unique_pages_per_reference=unique/total if total else None,
                   sharing_ratio=1-unique/total if total else None,
                   average_references_per_unique_page=total/unique if unique else None,
                   reused_page_fraction=sum(v>1 for v in counts.values())/unique if unique else None)
        for cap in (585,1170,2340):
            over = [r for r in rs if len(r.block_ids)>cap]
            row.update({f'full_path_over_capacity_{cap}_count':len(over),
                        f'full_path_over_capacity_{cap}_request_fraction':len(over)/n if n else None,
                        f'full_path_over_capacity_{cap}_token_fraction':sum(r.input_tokens for r in over)/tokens if tokens else None})
        gaps = intervals[scope]
        row.update({'reuse_gap_ms_'+k:v for k,v in quantiles(gaps).items()})
        row.update(reuse_pair_count=len(gaps), reuse_within_60s_fraction=sum(g<=60000 for g in gaps)/len(gaps) if gaps else None,
                   reuse_within_300s_fraction=sum(g<=300000 for g in gaps)/len(gaps) if gaps else None)
        rows.append(row)
        for gap,count in sorted(Counter(gaps).items()):
            reuse_rows.append(dict(workload=workload,scope=scope,gap_ms=gap,pair_count=count))
        for ref_count,page_count in sorted(Counter(counts.values()).items()):
            refs_rows.append(dict(workload=workload,scope=scope,reference_count=ref_count,unique_page_count=page_count))
        for b,label in enumerate(BUCKETS):
            members=[r for r in rs if bucket(r.input_tokens)==b]
            inp=sum(r.input_tokens for r in members)
            buckets.append(dict(workload=workload,scope=scope,bucket=f'B{b+1}',range=label,weight=WEIGHTS[b],
                                request_count=len(members),input_tokens=inp,request_fraction=len(members)/n if n else None,
                                token_fraction=inp/tokens if tokens else None))
    # Fixed 1-second trailing windows, not request-weighted windows. Window (t-W,t].
    # Replay itself is [0,end); endpoint-only trace rows are observation-only.
    for width in (60000,300000):
        active=Counter();queue=deque();pos=0;total=0;window_rows=[]
        for t in range(0,int(end_ms)+1,1000):
            while pos<len(requests) and requests[pos].arrival_ms<=t and requests[pos].arrival_ms<end_ms:
                r,path=requests[pos],paths[pos];queue.append((r.arrival_ms,path));active.update(path);total+=len(path);pos+=1
            while queue and queue[0][0]<=t-width:
                _,path=queue.popleft();total-=len(path)
                for node in path:
                    active[node]-=1
                    if active[node]==0:del active[node]
            window_rows.append(dict(workload=workload,window_ms=width,time_ms=t,
                                    unique_full_page_prefix_pages=len(active),total_prefix_page_references=total,
                                    sharing_ratio=1-len(active)/total if total else None))
        series.extend(window_rows)
        for scope,lo,hi in scopes:
            # Evaluation samples include only [25,45) min; past context is retained.
            selected=[r for r in window_rows if lo<=r['time_ms']<hi]
            ws_summary.append(dict(workload=workload,scope=scope,window_ms=width,sample_period_ms=1000,
                                   sample_count=len(selected),
                                   **{'unique_pages_'+k:v for k,v in quantiles([r['unique_full_page_prefix_pages'] for r in selected]).items()},
                                   **{'page_references_'+k:v for k,v in quantiles([r['total_prefix_page_references'] for r in selected]).items()},
                                   mean_sharing_ratio=sum(r['sharing_ratio'] for r in selected if r['sharing_ratio'] is not None)/sum(r['sharing_ratio'] is not None for r in selected) if any(r['sharing_ratio'] is not None for r in selected) else None))
        if progress:
            print(f'{workload}: processed={len(requests)}/{len(requests)} window={width}ms complete',flush=True)
    return dict(workload_capacity_profile=rows,working_set_summary=ws_summary,prompt_bucket_support=buckets,
                working_set_timeseries=series,prefix_reference_counts=refs_rows,reuse_gap_distribution=reuse_rows)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--workload',choices=['conversation','toolagent'],required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--benchmark-seconds',type=int,default=0)
    args=parser.parse_args()
    if args.output.exists():
        raise SystemExit('Refusing to overwrite an existing profile directory')
    start=time.perf_counter();trace=ROOT/f'data/mooncake/{args.workload}_trace.jsonl'
    requests=read_trace(trace);total=len(requests);parse_time=time.perf_counter()-start
    end=3537000
    if args.benchmark_seconds:
        end=args.benchmark_seconds*1000
    selected=[r for r in requests if r.arrival_ms<end]
    ref_total=sum(len(full_page_prefix(r,len(r.block_ids))) for r in requests if r.arrival_ms<3537000)
    ref_selected=sum(len(full_page_prefix(r,len(r.block_ids))) for r in selected)
    t=time.perf_counter();outputs=profile(selected,args.workload,end,True);compute=time.perf_counter()-t
    args.output.mkdir(parents=True)
    for name,rows in outputs.items():write_csv(args.output/(name+'.csv'),rows)
    elapsed=time.perf_counter()-start
    perf=dict(workload=args.workload,benchmark_seconds=args.benchmark_seconds,parsed_request_count=total,
              processed_request_count=len(selected),processed_full_page_references=ref_selected,
              full_replay_full_page_references=ref_total,parse_wall_seconds=parse_time,compute_wall_seconds=compute,
              wall_seconds=elapsed,peak_rss_mib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024,
              estimated_full_wall_seconds=parse_time+compute*max(ref_total/max(1,ref_selected),3537000/end),
              memory_estimate_method='Full trace parsed even in benchmark; additional index memory scales with unique full pages.',
              trace_sha256=hashlib.sha256(trace.read_bytes()).hexdigest())
    (args.output/'performance.json').write_text(json.dumps(perf,indent=2)+'\n')
    print(json.dumps(perf),flush=True)


if __name__=='__main__':main()
