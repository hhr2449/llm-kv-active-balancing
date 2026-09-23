"""Indexed attribution to real execution hits and exact residency generations."""
from bisect import bisect_right
from collections import defaultdict

from ..final_metrics import request_metrics, gini_metrics, network_metrics, ratio


def phase(t):
    return 'PRE_EVAL' if t < 1500000 else 'EVALUATION' if t < 2700000 else 'POST_EVAL'


def copy_utilization(result, visibility=3537000):
    # Page-indexed observations avoid sorting/scanning an entire Pod history for
    # every COPY. Only true execution-hit records enter this index.
    uses = defaultdict(list)
    by_request = {r.request_id: r for r in result.request_records}
    for row in result.reuse_observation_records:
        request = by_request[row['request_id']]
        assert (request.final_pod, request.completion_time, request.final_hit_pages) == (
            row['target'], row['time'], len(row['path']))
        for b, generation in zip(row['path'], row['generations']):
            uses[(row['target'], b, generation)].append((row['time'], row['request_id']))
    for times in uses.values():
        times.sort()
    raw_uses = {(r['target'], r['request_id']): r for r in result.reuse_observation_records}
    copies = {c['transfer_id']: c for c in result.copy_observation_records}
    transfers = {t.transfer_id: t for t in result.transfer_records}
    final_generations = [{row[0]: row[-1] for row in c['pages']} for c in result.final_state['cache']]
    output = []
    for action in result.proactive_action_records:
        copy = copies[action.transfer_id]
        generations = tuple(copy['generations'])
        assert len(generations) == len(action.chain_id)
        end = action.ready_time + 300000
        first = None
        hit_pages = []
        lifetime_hit_pages = []
        lifetime_first = None
        for i, (b, generation) in enumerate(zip(action.chain_id, generations)):
            times = uses.get((action.target, b, generation), ())
            pos = bisect_right(times, (action.ready_time, float('inf')))
            hit_pages.append(pos < len(times) and times[pos][0] <= min(end, visibility))
            lifetime_hit_pages.append(pos < len(times) and times[pos][0] <= visibility)
            if i == len(action.chain_id) - 1:
                for stamp, request_id in times[pos:]:
                    if stamp > visibility:
                        break
                    use = raw_uses[action.target, request_id]
                    depth = len(action.chain_id)
                    if (tuple(use['path'][:depth]) == tuple(action.chain_id) and
                            tuple(use['generations'][:depth]) == generations):
                        lifetime_first = (stamp, request_id)
                        if stamp <= end:
                            first = lifetime_first
                        break
        censored = end > visibility
        status = 'CENSORED' if censored else 'USED' if first else 'UNUSED'
        transfer = transfers[action.transfer_id]
        resident = [final_generations[action.target].get(b) == g for b, g in zip(action.chain_id, generations)]
        lifetime = 'USED' if lifetime_first else 'UNRESOLVED_STILL_RESIDENT' if all(resident) else 'ENDED_WITHOUT_FULL_CHAIN_USE'
        output.append(dict(action_id=action.action_id, transfer_id=action.transfer_id,
            chain_id=action.chain_id, target=action.target, start_time=action.start_time,
            ready_time=action.ready_time, observation_end=end, generations=generations,
            wire_tokens=action.wire_tokens, status=status,
            first_use_request_id=first[1] if first and not censored else None,
            first_use_time=first[0] if first and not censored else None,
            start_cohort=phase(action.start_time), ready_phase=phase(action.ready_time),
            crosses_eval_start=action.start_time < 1500000 <= action.ready_time,
            crosses_eval_end=action.start_time < 2700000 <= action.ready_time,
            wire_pages=action.wire_pages, wire_bytes=action.wire_bytes,
            action_denominator=1, observed_action_denominator=int(not censored),
            observed_wire_pages_denominator=0 if censored else action.wire_pages,
            observed_wire_bytes_denominator=0 if censored else action.wire_bytes,
            unused_observed_wire_bytes=action.wire_bytes if status == 'UNUSED' else 0,
            any_prefix_observed_reuse=any(hit_pages), full_chain_observed_reuse=first is not None,
            newly_published_pages=transfer.newly_resident_pages, duplicate_wire_pages=transfer.duplicate_pages,
            new_pages_observed_used=sum(hit_pages[transfer.duplicate_pages:]),
            new_page_denominator=transfer.newly_resident_pages,
            new_page_use_coverage='PARTIAL_RIGHT_CENSORED' if censored else 'COMPLETE_300S',
            still_resident_pages=sum(resident), lifecycle_full_chain_outcome=lifetime,
            lifecycle_unresolved_new_pages=sum(resident[i] and not lifetime_hit_pages[i]
                for i in range(transfer.duplicate_pages, len(resident))),
            interpretation='observed reuse attribution, not causal Saved; fixed window (ready, ready+300s]'))
    return output


CANONICAL_FIELDS = ('action_id', 'transfer_id', 'chain_id', 'target', 'start_time', 'ready_time',
                    'observation_end', 'generations', 'wire_tokens', 'status', 'first_use_request_id', 'first_use_time')


def waste_summary(rows):
    observations = [{k: row[k] for k in CANONICAL_FIELDS} for row in rows]
    observed = [r for r in rows if r['status'] != 'CENSORED']
    total = sum(r['wire_tokens'] for r in rows)
    tokens = sum(r['wire_tokens'] for r in observed)
    unused = sum(r['wire_tokens'] for r in observed if r['status'] == 'UNUSED')
    return dict(proactive_copy_count=len(rows), observed_copy_count=len(observed),
                used_copy_count=sum(r['status'] == 'USED' for r in rows),
                unused_copy_count=sum(r['status'] == 'UNUSED' for r in rows),
                censored_copy_count=len(rows)-len(observed),
                total_proactive_wire_tokens_for_same_cohort=total, observed_wire_tokens=tokens,
                censored_wire_tokens=total-tokens, unused_observed_wire_tokens=unused,
                wasted_copy_ratio=ratio(unused, tokens), observation_coverage=ratio(tokens, total),
                observations=observations)


def finalize_metrics(result, config):
    start, end = config.evaluation_start_ms, config.evaluation_end_ms
    rows = copy_utilization(result, config.visibility_end_ms)
    result.summary['stage2_copy_utilization'] = rows
    return dict(scope=dict(arrival_cohort=[start, end], transfer_start_cohort=[start, end],
                           visibility_end_ms=config.visibility_end_ms),
                requests=request_metrics(result.request_records, start, end),
                gini=gini_metrics(result.request_records, config),
                network=network_metrics(result.transfer_records, start, end, config.visibility_end_ms),
                wasted=waste_summary([r for r in rows if start <= r['start_time'] < end]))
