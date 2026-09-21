"""Pure post-hoc TaskMain metrics. No policy or simulator-state mutations."""
from functools import lru_cache
import math
import random
from statistics import median

from .history import WEIGHTS, bucket


def ratio(numerator, denominator):
    return numerator / denominator if denominator else None


def request_metrics(requests, start, end):
    rows = [r for r in requests if start <= r.arrival_time < end]
    buckets = []
    for i, weight in enumerate(WEIGHTS):
        members = [r for r in rows if bucket(r.input_tokens) == i]
        saved = sum(r.final_hit_tokens for r in members)
        tokens = sum(r.input_tokens for r in members)
        hits = sum(r.final_hit_tokens > 0 for r in members)
        buckets.append(dict(bucket=f"B{i+1}", multiplier=weight, request_count=len(members),
                            input_tokens=tokens, saved_tokens=saved, request_hit_count=hits,
                            request_hit_rate=ratio(hits, len(members)), token_hit_rate=ratio(saved, tokens),
                            weighted_saved_tokens=saved * weight))
    saved = sum(b["saved_tokens"] for b in buckets)
    tokens = sum(b["input_tokens"] for b in buckets)
    hits = sum(b["request_hit_count"] for b in buckets)
    return dict(request_count=len(rows), total_input_tokens=tokens, saved_prefill_tokens=saved,
                request_hit_count=hits, request_hit_rate=ratio(hits, len(rows)),
                token_hit_rate=ratio(saved, tokens),
                weighted_saved_tokens=math.fsum(b["weighted_saved_tokens"] for b in buckets),
                buckets=buckets)


def gini(counts):
    total = sum(counts)
    if not total:
        return 0.0
    values = sorted(counts)
    n = len(values)
    return sum((2*i-n-1)*v for i, v in enumerate(values, 1)) / (n*total)


def quantile(values, q):
    values = sorted(values)
    position = (len(values)-1)*q
    lower = int(position)
    return values[lower] + (values[min(lower+1, len(values)-1)]-values[lower])*(position-lower)


@lru_cache(maxsize=None)
def random_gini_baseline(workload, start, end, n, request_count, seed=20260911, repetitions=200):
    """Immutable return; every cache key starts the same fixed-seed RNG stream."""
    rng = random.Random(seed)
    values = []
    for _ in range(repetitions):
        counts = [0]*n
        for _ in range(request_count):
            counts[rng.randrange(n)] += 1
        values.append(gini(counts))
    return math.fsum(values)/repetitions, quantile(values, .95)


def aggregate_gini(windows):
    def med(key):
        values = [w[key] for w in windows if w[key] is not None]
        return median(values) if values else None
    return dict(median_actual_gini=med("actual_gini"), median_random_gini_mean=med("random_gini_mean"),
                median_random_gini_p95=med("random_gini_p95"), skew_ratio=med("skew_ratio"),
                valid_ratio_window_count=sum(w["skew_ratio"] is not None for w in windows),
                total_window_count=len(windows), windows=windows)


def gini_metrics(requests, config):
    windows = []
    for start in range(config.evaluation_start_ms, config.evaluation_end_ms, 300000):
        end = start+300000
        counts = [0]*config.num_pods
        for row in requests:
            if start <= row.arrival_time < end:
                counts[row.final_pod] += 1
        avg, p95 = random_gini_baseline(config.workload, start, end, config.num_pods,
                                      sum(counts), config.random_seed, config.random_gini_repetitions)
        actual = gini(counts)
        windows.append(dict(window_start_ms=start, window_end_ms=end, counts=counts,
                            request_count=sum(counts), actual_gini=actual, random_gini_mean=avg,
                            random_gini_p95=p95, skew_ratio=ratio(actual, avg),
                            seed=config.random_seed, repetitions=config.random_gini_repetitions))
    return aggregate_gini(windows)


def network_metrics(transfers, start, end, visibility):
    phases = {"PRE_EVAL": (0, start), "EVAL": (start, end), "TAIL": (end, visibility),
              "FULL_RUN": (-math.inf, math.inf)}
    result = []
    for phase, (lo, hi) in phases.items():
        cohort = [t for t in transfers if lo <= t.start_time < hi]
        for kind in ("REACTIVE", "PROACTIVE", "TOTAL"):
            rows = [t for t in cohort if kind == "TOTAL" or t.type == kind]
            result.append(dict(phase=phase, type=kind, transfer_started_count=len(rows),
                               transfer_completed_count=sum(t.status == "COMPLETED" for t in rows),
                               wire_pages=sum(t.wire_pages for t in rows),
                               wire_tokens=sum(t.wire_tokens for t in rows),
                               wire_bytes=sum(t.wire_bytes for t in rows)))
    return result


def wasted_metrics(actions, copies, reuses, start, end, visibility, window=300000):
    evidence = {c["transfer_id"]: c for c in copies}
    target_uses = {}
    for use in reuses:
        target_uses.setdefault(use["target"], []).append(use)
    observations = []
    for action in actions:
        if not start <= action.start_time < end:
            continue
        copy = evidence.get(action.transfer_id)
        # Missing ready evidence is an implementation error, never silently UNUSED.
        if copy is None or len(copy["generations"]) != action.chain_depth_pages:
            raise ValueError("missing copy residency evidence")
        deadline = action.ready_time+window
        status = "CENSORED" if deadline > visibility else "UNUSED"
        first_use = None
        if status != "CENSORED":
            depth = action.chain_depth_pages
            for use in sorted(target_uses.get(action.target, ()), key=lambda u: (u["time"], u["request_id"])):
                if (action.ready_time < use["time"] <= deadline and
                    tuple(use["path"][:depth]) == tuple(action.chain_id) and
                    tuple(use["generations"][:depth]) == tuple(copy["generations"])):
                    status, first_use = "USED", use
                    break
        observations.append(dict(action_id=action.action_id, transfer_id=action.transfer_id,
            chain_id=action.chain_id, target=action.target, start_time=action.start_time,
            ready_time=action.ready_time, observation_end=deadline, generations=copy["generations"],
            wire_tokens=action.wire_tokens, status=status,
            first_use_request_id=first_use["request_id"] if first_use else None,
            first_use_time=first_use["time"] if first_use else None))
    observed = [a for a in observations if a["status"] != "CENSORED"]
    unused_tokens = sum(a["wire_tokens"] for a in observed if a["status"] == "UNUSED")
    observed_tokens = sum(a["wire_tokens"] for a in observed)
    total_tokens = sum(a["wire_tokens"] for a in observations)
    return dict(proactive_copy_count=len(observations), observed_copy_count=len(observed),
                used_copy_count=sum(a["status"] == "USED" for a in observations),
                unused_copy_count=sum(a["status"] == "UNUSED" for a in observations),
                censored_copy_count=len(observations)-len(observed),
                total_proactive_wire_tokens_for_same_cohort=total_tokens,
                observed_wire_tokens=observed_tokens, censored_wire_tokens=total_tokens-observed_tokens,
                unused_observed_wire_tokens=unused_tokens,
                wasted_copy_ratio=ratio(unused_tokens, observed_tokens),
                observation_coverage=ratio(observed_tokens, total_tokens), observations=observations)


def finalize_metrics(result, config):
    start, end = config.evaluation_start_ms, config.evaluation_end_ms
    return dict(scope=dict(arrival_cohort=[start, end], transfer_start_cohort=[start, end],
                           visibility_end_ms=config.visibility_end_ms),
                requests=request_metrics(result.request_records, start, end),
                gini=gini_metrics(result.request_records, config),
                network=network_metrics(result.transfer_records, start, end, config.visibility_end_ms),
                wasted=wasted_metrics(result.proactive_action_records, result.copy_observation_records,
                                      result.reuse_observation_records, start, end,
                                      config.visibility_end_ms, config.future_window_ms))
