"""Read-only Stage 1 aggregation; arrival cohorts, start-time wire, ready-time events."""
from collections import Counter
from src.simulator.task_main.final_metrics import request_metrics, random_gini_baseline, gini, ratio, quantile
from src.simulator.task_main.stage0.diagnostics import occupancy_metrics


def interval(result, engine, scope, start, end):
    cfg = engine.config
    rs = [r for r in result.request_records if start <= r.arrival_time < end]
    ts = [t for t in result.transfer_records if start <= t.start_time < end]
    failures = [f for f in engine.capacity_failures if start <= f['time_ms'] < end]
    pods = []
    for pod, cache in enumerate(engine.caches):
        members = [r for r in rs if r.final_pod == pod]
        ev = [e for e in cache.ordinary_evictions if start <= e['time_ms'] < end]
        occ = occupancy_metrics(cache.occupancy, start, end)
        entry = dict(pod=pod, request_count=len(members), request_share=ratio(len(members), len(rs)),
                     miss_prefill_tokens=sum(r.miss_tokens for r in members), **occ,
                     mean_occupancy_ratio=ratio(occ['time_weighted_mean_occupancy_pages'], cfg.capacity_pages),
                     peak_occupancy_ratio=ratio(occ['peak_occupancy_pages'], cfg.capacity_pages),
                     ordinary_eviction_events=len(ev), ordinary_evicted_pages=sum(e['pages'] for e in ev),
                     capacity_failure_count=sum(f['pod'] == pod for f in failures))
        for cause in ['request', 'reactive_transfer', 'proactive_transfer']:
            entry[cause+'_eviction_events'] = sum(e['cause'] == cause for e in ev)
            entry[cause+'_evicted_pages'] = sum(e['pages'] for e in ev if e['cause'] == cause)
        for kind in ['REACTIVE', 'PROACTIVE']:
            entry[kind.lower()+'_wire_sent_bytes'] = sum(t.wire_bytes for t in ts if t.source == pod and t.type == kind)
            entry[kind.lower()+'_wire_received_bytes'] = sum(t.wire_bytes for t in ts if t.target == pod and t.type == kind)
        pods.append(entry)
    counts = [p['request_count'] for p in pods]
    miss = [p['miss_prefill_tokens'] for p in pods]
    for p in pods:
        p['miss_prefill_share'] = ratio(p['miss_prefill_tokens'], sum(miss))
    rand, p95 = random_gini_baseline(cfg.workload, start, end, cfg.num_pods, len(rs), cfg.random_seed, cfg.random_gini_repetitions)
    metrics = request_metrics(rs, start, end)
    fractions = [r.final_hit_tokens/r.input_tokens if r.input_tokens else 0 for r in rs]
    metrics.update(hit_fraction_distribution={k: quantile(fractions, q) if fractions else None
                   for k, q in [('p0', 0), ('p50', .5), ('p90', .9), ('p99', .99), ('max', 1)]},
                   top1_request_share=ratio(max(counts), len(rs)), active_pod_count=sum(c > 0 for c in counts),
                   raw_request_gini=gini(counts), random_gini_mean=rand, random_gini_p95=p95,
                   random_seed=cfg.random_seed, random_repetitions=cfg.random_gini_repetitions,
                   skew_ratio=ratio(gini(counts), rand), total_miss_prefill_tokens=sum(miss),
                   max_pod_miss_prefill_tokens=max(miss), max_pod_workload_share=ratio(max(miss), sum(miss)),
                   workload_gini=gini(miss),
                   mean_occupancy_pages=sum(p['time_weighted_mean_occupancy_pages'] for p in pods)/cfg.num_pods,
                   peak_pod_occupancy_pages=max(p['peak_occupancy_pages'] for p in pods),
                   mean_occupancy_gini=gini([p['time_weighted_mean_occupancy_pages'] for p in pods]),
                   end_occupancy_gini=gini([p['end_occupancy_pages'] for p in pods]),
                   eviction_events=sum(p['ordinary_eviction_events'] for p in pods),
                   eviction_pages=sum(p['ordinary_evicted_pages'] for p in pods),
                   capacity_preflight_failures=len(failures), cache_admission_rejected=sum(r.cache_admission_skip for r in rs))
    ids = {r.request_id for r in rs}
    metrics.update(full_path_over_capacity=sum(r['full_path_over_capacity'] for r in engine.request_capacity if r['request_id'] in ids),
                   chain_larger_than_capacity=sum(f['reason'] == 'chain_larger_than_capacity' for f in failures),
                   insufficient_evictable_capacity=sum(f['reason'] == 'insufficient_evictable_capacity' for f in failures),
                   request_skipped_due_to_capacity=0, request_truncated_due_to_capacity=0)
    reactive = [t for t in ts if t.type == 'REACTIVE']
    fallbacks = dict(Counter(r.reactive_fallback_reason for r in rs if r.reactive_fallback_reason))
    behavior = dict(gate_evaluated=sum(r.routing_policy == 'R_REQ_KV_TASK' for r in rs),
                    gate_pass=sum(r.reactive_gate_passed for r in rs),
                    direct_target=sum(r.routing_policy == 'R_REQ_KV_TASK' and r.final_pod != r.affinity_source and r.reactive_transfer_id is None for r in rs),
                    copy_started=len(reactive),
                    copy_ready_start_cohort=sum(t.status == 'COMPLETED' for t in reactive),
                    copy_ready_events=sum(t.type == 'REACTIVE' and t.status == 'COMPLETED' and start <= t.ready_time < end for t in result.transfer_records),
                    copy_failed_start_cohort=sum(t.status == 'FAILED' for t in reactive),
                    fallback=sum(fallbacks.values()), fallback_reasons=fallbacks,
                    wire_bytes=sum(t.wire_bytes for t in reactive), wire_pages=sum(t.wire_pages for t in reactive),
                    inflight_at_start=sum(t.type == 'REACTIVE' and t.start_time < start <= t.ready_time for t in result.transfer_records),
                    inflight_at_end=sum(t.type == 'REACTIVE' and t.start_time < end <= t.ready_time for t in result.transfer_records))
    metrics.update(reactive_actions=behavior['direct_target']+behavior['copy_started'], reactive_wire_bytes=behavior['wire_bytes'],
                   proactive_started=sum(t.type == 'PROACTIVE' for t in ts))
    return dict(scope=scope, start_ms=start, end_ms=end, core=metrics, per_pod=pods, reactive=behavior)


def build(result, engine, inputs, replay_end_ms):
    end = max(replay_end_ms, max((e.time for e in result.event_records), default=0)+1)
    cfg = engine.config
    phases = [('PRE_EVAL', 0, min(end, cfg.evaluation_start_ms)),
              ('EVALUATION', cfg.evaluation_start_ms, min(end, cfg.evaluation_end_ms)),
              ('POST_EVAL', cfg.evaluation_end_ms, end), ('FULL_RUN', 0, end)]
    phases = [interval(result, engine, name, lo, hi) for name, lo, hi in phases if lo < hi]
    series = [interval(result, engine, 'FIVE_MINUTES', lo, min(lo+300000, end)) for lo in range(0, int(end), 300000)]
    # True page identity is validated by read_trace: each block has unique ancestry/depth/valid tokens.
    # Restrict to full pages observed in the shared external input; include zero resident replicas.
    universe = {b for r in inputs for b, v in zip(r.block_ids, r.block_valid_tokens) if v == 512}
    replicas = Counter(b for c in engine.caches for b in c._pages if b in universe)
    distribution = Counter(replicas[b] for b in universe)
    return dict(schema_version='TASK_MAIN_STAGE1_DIAGNOSTICS_V1', phases=phases, five_minute=series,
                effective_replica_distribution=dict(time_ms=end, definition='end-of-run published full-page replicas, zero included; not runtime KV or partial tails',
                    external_unique_full_pages=len(universe), pages_by_replica_count=dict(sorted(distribution.items()))),
                hotspot_coverage='not implemented in Stage 0; no policy-conditioned hotspot set is introduced',
                null_semantics='infinite occupancy ratios and empty-cohort ratios/quantiles are intentionally null')
