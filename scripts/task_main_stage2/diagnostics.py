from collections import Counter

from scripts.task_main_stage1.diagnostics import build as base_diagnostics
from src.simulator.task_main.final_metrics import ratio


def build(result, engine, inputs):
    data = base_diagnostics(result, engine, inputs, engine.config.visibility_end_ms)
    data['schema_version'] = 'ACTIVE_CAPACITY_STAGE2_DIAGNOSTICS_V1'
    data['copy_utilization'] = result.summary['stage2_copy_utilization']
    failed = Counter(d.opportunity_id for d in result.candidate_decision_records if d.status == 'SKIP_NO_CAPACITY')
    for row in data['phases'] + data['five_minute']:
        start, end = row['start_ms'], row['end_ms']
        transfers = [t for t in result.transfer_records if start <= t.start_time < end]
        ops = [o for o in result.opportunity_records if start <= o.opportunity_time < end]
        proactive = [t for t in transfers if t.type == 'PROACTIVE']
        row['proactive'] = dict(opportunity=len(ops), candidate=sum(o.structural_candidate_count for o in ops),
            positive_candidate=sum(o.positive_candidate_count for o in ops),
            shortlist=sum(o.shortlist_count for o in ops), attempt=sum(o.attempted_candidate_count for o in ops),
            started=len(proactive), ready_start_cohort=sum(t.status == 'COMPLETED' for t in proactive),
            ready_events=sum(t.type == 'PROACTIVE' and start <= t.ready_time < end and t.status == 'COMPLETED'
                             for t in result.transfer_records),
            failed_start_cohort=sum(t.status == 'FAILED' for t in proactive),
            admission_failed_attempt=sum(failed[o.opportunity_id] for o in ops),
            fallback=0, fallback_definition='proactive has no request-routing fallback; admission skips reported separately',
            no_action_reasons=dict(Counter(o.final_status for o in ops if o.final_status != 'STARTED')),
            inflight_at_start=sum(t.type == 'PROACTIVE' and t.start_time < start <= t.ready_time for t in result.transfer_records),
            inflight_at_end=sum(t.type == 'PROACTIVE' and t.start_time < end <= t.ready_time for t in result.transfer_records))
        for kind in ('TOTAL', 'REACTIVE', 'PROACTIVE'):
            cohort = [t for t in transfers if kind == 'TOTAL' or t.type == kind]
            for unit in ('bytes', 'pages', 'tokens'):
                row['core'][kind.lower()+'_wire_'+unit] = sum(getattr(t, 'wire_'+unit) for t in cohort)
        for cause in ('request', 'reactive_transfer', 'proactive_transfer'):
            for suffix in ('eviction_events', 'evicted_pages'):
                row['core'][cause+'_'+suffix] = sum(p[cause+'_'+suffix] for p in row['per_pod'])
        copies = [c for c in data['copy_utilization'] if start <= c['start_time'] < end]
        observed = [c for c in copies if c['status'] != 'CENSORED']
        row['utilization'] = dict(action_count=len(copies), observed_300s_actions=len(observed),
            used_300s_actions=sum(c['status'] == 'USED' for c in observed),
            unused_300s_actions=sum(c['status'] == 'UNUSED' for c in observed),
            right_censored_actions=len(copies)-len(observed),
            observed_wire_bytes=sum(c['wire_bytes'] for c in observed),
            unused_300s_wire_bytes=sum(c['unused_observed_wire_bytes'] for c in observed),
            unused_300s_action_ratio=ratio(sum(c['status'] == 'UNUSED' for c in observed), len(observed)),
            unused_300s_wire_byte_ratio=ratio(sum(c['unused_observed_wire_bytes'] for c in observed), sum(c['wire_bytes'] for c in observed)),
            any_prefix_observed_reuse_actions=sum(c['any_prefix_observed_reuse'] for c in copies),
            newly_published_pages=sum(c['newly_published_pages'] for c in copies),
            new_pages_observed_used=sum(c['new_pages_observed_used'] for c in copies),
            duplicate_wire_pages=sum(c['duplicate_wire_pages'] for c in copies),
            still_resident_pages=sum(c['still_resident_pages'] for c in copies),
            lifecycle_unresolved_actions=sum(c['lifecycle_full_chain_outcome'] == 'UNRESOLVED_STILL_RESIDENT' for c in copies))
    data['occupancy_trajectories'] = {str(p): c.occupancy for p, c in enumerate(engine.caches)}
    data['ordinary_evictions'] = {str(p): c.ordinary_evictions for p, c in enumerate(engine.caches)}
    return data


def validate(result, engine, inputs, diagnostics):
    rows = result.request_records
    cfg = engine.config
    checks = dict(engine=result.validation['status'] == 'PASS',
        all_requests_completed_once=len(rows) == len(inputs) and {r.request_id for r in rows} == {r.request_id for r in inputs},
        full_input_tokens=sum(r.input_tokens for r in rows) == sum(r.input_tokens for r in inputs),
        hit_miss_conservation=all(r.final_hit_tokens+r.miss_tokens == r.input_tokens for r in rows),
        arrival_based_switch=all(r.routing_policy == cfg.route_at(r.arrival_time) for r in rows),
        no_skips_or_truncation=all(p['core']['request_skipped_due_to_capacity'] == p['core']['request_truncated_due_to_capacity'] == 0
                                  for p in diagnostics['phases']),
        all_pods=all(len(p['per_pod']) == cfg.num_pods for p in diagnostics['phases']),
        infinite_no_eviction_or_reject=cfg.capacity_mode != 'infinite' or
            (not engine.capacity_failures and not any(c.eviction_records for c in engine.caches)
             and not any(r.cache_admission_skip for r in rows)),
        wire_conservation=all(p['core']['total_wire_bytes'] == p['core']['reactive_wire_bytes'] + p['core']['proactive_wire_bytes']
                              for p in diagnostics['phases']),
        capacity=all(c.peak_memory_pages <= c.capacity_pages for c in engine.caches),
        correct_censoring=all((c['status'] == 'CENSORED') == (c['observation_end'] > cfg.visibility_end_ms)
                             for c in diagnostics['copy_utilization']))
    assert all(checks.values()), checks
    return checks
