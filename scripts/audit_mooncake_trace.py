#!/usr/bin/env python3
"""
Audit Mooncake FAST'25 traces before T2 simulator preprocessing.

Usage:
    python audit_mooncake_trace.py \
        --conversation /path/to/conversation_trace.jsonl \
        --toolagent /path/to/toolagent_trace.jsonl \
        --out-dir ./trace_audit

No third-party dependencies are required.
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
import math
import statistics
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

BLOCK_TOKENS = 512


def percentile(sorted_values: Sequence[float], q: float) -> Optional[float]:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    pos = (len(sorted_values) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(sorted_values[lo])
    w = pos - lo
    return float(sorted_values[lo] * (1 - w) + sorted_values[hi] * w)


def stats(values: Sequence[float]) -> dict:
    if not values:
        return {
            "count": 0, "min": None, "mean": None,
            "p50": None, "p90": None, "p95": None, "p99": None,
            "max": None,
        }
    x = sorted(values)
    return {
        "count": len(x),
        "min": x[0],
        "mean": statistics.fmean(x),
        "p50": percentile(x, 0.50),
        "p90": percentile(x, 0.90),
        "p95": percentile(x, 0.95),
        "p99": percentile(x, 0.99),
        "max": x[-1],
    }


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class AuditResult:
    workload: str
    path: str
    sha256: str
    rows: int
    invalid_json: int
    required_field_violations: int
    timestamps_non_decreasing: bool
    timestamp_order_violations: int
    timestamp_min_ms: Optional[float]
    timestamp_max_ms: Optional[float]
    duration_ms: Optional[float]
    unique_timestamps: int
    same_timestamp_rows: int
    same_timestamp_fraction: float
    positive_distinct_timestamp_delta_stats_ms: dict
    positive_distinct_timestamp_delta_top20: list
    input_length_stats: dict
    output_length_stats: dict
    hash_count_stats: dict
    unique_hash_ids: int
    total_hash_refs: int
    repeated_hash_refs: int
    repeated_hash_fraction: float
    ceil_relation_violations: int
    floor_relation_matches: int
    empty_hash_positive_input: int
    invalid_input_length: int
    partial_request_count: int
    partial_request_fraction: float
    tail_valid_tokens_stats: dict
    hash_depth_conflicts: int
    hash_parent_conflicts: int
    tail_valid_length_conflicts: int
    partial_vs_full_hash_conflicts: int
    duplicate_hash_within_request: int
    infinite_history_hit_pages: int
    total_trace_pages: int
    infinite_history_page_hit_ratio: float
    infinite_history_hit_tokens: int
    total_input_tokens: int
    infinite_history_token_hit_ratio: float


def audit_trace(path: Path, workload: str) -> Tuple[AuditResult, dict]:
    required = {"timestamp", "input_length", "output_length", "hash_ids"}

    rows = 0
    invalid_json = 0
    required_field_violations = 0
    timestamp_order_violations = 0
    prev_ts = None

    timestamps: List[float] = []
    input_lengths: List[int] = []
    output_lengths: List[int] = []
    hash_counts: List[int] = []
    tail_valids: List[int] = []

    ceil_relation_violations = 0
    floor_relation_matches = 0
    empty_hash_positive_input = 0
    invalid_input_length = 0
    partial_request_count = 0
    duplicate_hash_within_request = 0

    total_hash_refs = 0
    hash_ref_counts = collections.Counter()

    # Prefix-aware consistency audits.
    # hash_id -> observed depths (0-based)
    hash_depths: Dict[int, set] = collections.defaultdict(set)
    # hash_id -> parent hash IDs (None for depth 0)
    hash_parents: Dict[int, set] = collections.defaultdict(set)

    # Tail semantics.
    # hash_id -> partial valid lengths observed when hash is final partial page
    tail_partial_lengths: Dict[int, set] = collections.defaultdict(set)
    hash_seen_full = set()
    hash_seen_partial = set()

    # Infinite-history reuse diagnostic.
    seen_hashes = set()
    infinite_history_hit_pages = 0
    infinite_history_hit_tokens = 0
    total_input_tokens = 0

    sample_violations = {
        "ceil_relation": [],
        "parent_conflict_candidates": [],
        "depth_conflict_candidates": [],
        "tail_length_conflict_candidates": [],
    }

    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                rec = json.loads(line)
            except Exception as e:
                invalid_json += 1
                continue

            rows += 1

            if not required.issubset(rec):
                required_field_violations += 1
                continue

            ts = rec["timestamp"]
            n = rec["input_length"]
            out = rec["output_length"]
            ids = rec["hash_ids"]

            if not isinstance(ids, list):
                required_field_violations += 1
                continue

            timestamps.append(ts)
            input_lengths.append(n)
            output_lengths.append(out)
            hash_counts.append(len(ids))

            if prev_ts is not None and ts < prev_ts:
                timestamp_order_violations += 1
            prev_ts = ts

            if not isinstance(n, int) or n <= 0:
                invalid_input_length += 1
                continue

            total_input_tokens += n

            if len(ids) != len(set(ids)):
                duplicate_hash_within_request += 1

            expected_ceil = math.ceil(n / BLOCK_TOKENS)
            expected_floor = math.floor(n / BLOCK_TOKENS)

            if len(ids) != expected_ceil:
                ceil_relation_violations += 1
                if len(sample_violations["ceil_relation"]) < 20:
                    sample_violations["ceil_relation"].append({
                        "line": line_no,
                        "input_length": n,
                        "hash_count": len(ids),
                        "expected_ceil": expected_ceil,
                    })

            if len(ids) == expected_floor:
                floor_relation_matches += 1

            if n > 0 and not ids:
                empty_hash_positive_input += 1

            rem = n % BLOCK_TOKENS
            if rem:
                partial_request_count += 1
                tail_valids.append(rem)
            elif n > 0:
                tail_valids.append(BLOCK_TOKENS)

            # All blocks before final are expected full; final block may be partial.
            for depth, hid in enumerate(ids):
                parent = ids[depth - 1] if depth > 0 else None
                hash_depths[hid].add(depth)
                hash_parents[hid].add(parent)
                hash_ref_counts[hid] += 1
                total_hash_refs += 1

                if depth < len(ids) - 1:
                    hash_seen_full.add(hid)
                else:
                    valid = rem if rem else BLOCK_TOKENS
                    if valid < BLOCK_TOKENS:
                        hash_seen_partial.add(hid)
                        tail_partial_lengths[hid].add(valid)
                    else:
                        hash_seen_full.add(hid)

            # Infinite-history prefix reuse:
            # Since Mooncake IDs are cumulative prefix hashes, stop at first unseen page.
            k = 0
            for hid in ids:
                if hid in seen_hashes:
                    k += 1
                else:
                    break

            infinite_history_hit_pages += k
            infinite_history_hit_tokens += min(n, k * BLOCK_TOKENS)

            for hid in ids:
                seen_hashes.add(hid)

    unique_ts = sorted(set(timestamps))
    distinct_deltas = [
        unique_ts[i] - unique_ts[i - 1]
        for i in range(1, len(unique_ts))
        if unique_ts[i] > unique_ts[i - 1]
    ]
    delta_counts = collections.Counter(distinct_deltas)

    ts_counts = collections.Counter(timestamps)
    same_timestamp_rows = sum(c for c in ts_counts.values() if c > 1)

    depth_conflict_ids = [hid for hid, ds in hash_depths.items() if len(ds) > 1]
    parent_conflict_ids = [hid for hid, ps in hash_parents.items() if len(ps) > 1]
    tail_length_conflict_ids = [
        hid for hid, ls in tail_partial_lengths.items() if len(ls) > 1
    ]
    partial_vs_full_conflict_ids = list(hash_seen_partial & hash_seen_full)

    repeated_hash_refs = sum(max(0, c - 1) for c in hash_ref_counts.values())

    result = AuditResult(
        workload=workload,
        path=str(path),
        sha256=sha256_file(path),
        rows=rows,
        invalid_json=invalid_json,
        required_field_violations=required_field_violations,
        timestamps_non_decreasing=(timestamp_order_violations == 0),
        timestamp_order_violations=timestamp_order_violations,
        timestamp_min_ms=min(timestamps) if timestamps else None,
        timestamp_max_ms=max(timestamps) if timestamps else None,
        duration_ms=(max(timestamps) - min(timestamps)) if timestamps else None,
        unique_timestamps=len(unique_ts),
        same_timestamp_rows=same_timestamp_rows,
        same_timestamp_fraction=(same_timestamp_rows / rows if rows else 0.0),
        positive_distinct_timestamp_delta_stats_ms=stats(distinct_deltas),
        positive_distinct_timestamp_delta_top20=delta_counts.most_common(20),
        input_length_stats=stats(input_lengths),
        output_length_stats=stats(output_lengths),
        hash_count_stats=stats(hash_counts),
        unique_hash_ids=len(hash_ref_counts),
        total_hash_refs=total_hash_refs,
        repeated_hash_refs=repeated_hash_refs,
        repeated_hash_fraction=(
            repeated_hash_refs / total_hash_refs if total_hash_refs else 0.0
        ),
        ceil_relation_violations=ceil_relation_violations,
        floor_relation_matches=floor_relation_matches,
        empty_hash_positive_input=empty_hash_positive_input,
        invalid_input_length=invalid_input_length,
        partial_request_count=partial_request_count,
        partial_request_fraction=(
            partial_request_count / rows if rows else 0.0
        ),
        tail_valid_tokens_stats=stats(tail_valids),
        hash_depth_conflicts=len(depth_conflict_ids),
        hash_parent_conflicts=len(parent_conflict_ids),
        tail_valid_length_conflicts=len(tail_length_conflict_ids),
        partial_vs_full_hash_conflicts=len(partial_vs_full_conflict_ids),
        duplicate_hash_within_request=duplicate_hash_within_request,
        infinite_history_hit_pages=infinite_history_hit_pages,
        total_trace_pages=total_hash_refs,
        infinite_history_page_hit_ratio=(
            infinite_history_hit_pages / total_hash_refs if total_hash_refs else 0.0
        ),
        infinite_history_hit_tokens=infinite_history_hit_tokens,
        total_input_tokens=total_input_tokens,
        infinite_history_token_hit_ratio=(
            infinite_history_hit_tokens / total_input_tokens
            if total_input_tokens else 0.0
        ),
    )

    details = {
        "depth_conflict_ids_first100": depth_conflict_ids[:100],
        "parent_conflict_ids_first100": parent_conflict_ids[:100],
        "tail_valid_length_conflict_ids_first100": tail_length_conflict_ids[:100],
        "partial_vs_full_hash_conflict_ids_first100": partial_vs_full_conflict_ids[:100],
        "samples": sample_violations,
    }

    return result, details


def fmt(v, digits=4):
    if v is None:
        return "NA"
    if isinstance(v, float):
        return f"{v:.{digits}f}"
    return str(v)


def result_to_markdown(r: AuditResult) -> str:
    hard_ok = (
        r.invalid_json == 0
        and r.required_field_violations == 0
        and r.timestamps_non_decreasing
        and r.ceil_relation_violations == 0
        and r.empty_hash_positive_input == 0
        and r.invalid_input_length == 0
        and r.hash_depth_conflicts == 0
        and r.hash_parent_conflicts == 0
        and r.tail_valid_length_conflicts == 0
        and r.partial_vs_full_hash_conflicts == 0
        and r.duplicate_hash_within_request == 0
    )

    s = []
    s.append(f"## {r.workload}\n")
    s.append(f"- Status: **{'PASS' if hard_ok else 'REVIEW'}**")
    s.append(f"- Path: `{r.path}`")
    s.append(f"- SHA256: `{r.sha256}`")
    s.append(f"- Requests: {r.rows:,}")
    s.append(
        f"- Time: {fmt(r.timestamp_min_ms)} → {fmt(r.timestamp_max_ms)} ms "
        f"(duration {fmt(r.duration_ms)} ms)"
    )
    s.append(f"- Unique timestamps: {r.unique_timestamps:,}")
    s.append(
        f"- Rows in duplicated timestamp groups: {r.same_timestamp_rows:,} "
        f"({r.same_timestamp_fraction:.2%})"
    )
    s.append("")

    s.append("### Hard invariants\n")
    s.append("| Check | Violations |")
    s.append("|---|---:|")
    s.append(f"| invalid JSON | {r.invalid_json} |")
    s.append(f"| missing/invalid required fields | {r.required_field_violations} |")
    s.append(f"| timestamp order | {r.timestamp_order_violations} |")
    s.append(f"| `len(hash_ids) != ceil(input/512)` | {r.ceil_relation_violations} |")
    s.append(f"| positive input with empty hashes | {r.empty_hash_positive_input} |")
    s.append(f"| invalid input length | {r.invalid_input_length} |")
    s.append(f"| hash depth conflicts | {r.hash_depth_conflicts} |")
    s.append(f"| hash parent conflicts | {r.hash_parent_conflicts} |")
    s.append(f"| tail valid-length conflicts | {r.tail_valid_length_conflicts} |")
    s.append(f"| same hash seen partial and full | {r.partial_vs_full_hash_conflicts} |")
    s.append(f"| duplicate hash within request | {r.duplicate_hash_within_request} |")
    s.append("")

    s.append("### Length / hash statistics\n")
    for name, st in [
        ("Input tokens", r.input_length_stats),
        ("Output tokens", r.output_length_stats),
        ("Hash pages/request", r.hash_count_stats),
        ("Tail valid tokens", r.tail_valid_tokens_stats),
        ("Positive distinct timestamp delta (ms)", r.positive_distinct_timestamp_delta_stats_ms),
    ]:
        s.append(
            f"- {name}: mean={fmt(st['mean'],2)}, p50={fmt(st['p50'],2)}, "
            f"p90={fmt(st['p90'],2)}, p95={fmt(st['p95'],2)}, "
            f"p99={fmt(st['p99'],2)}, max={fmt(st['max'],2)}"
        )
    s.append("")
    s.append(
        f"- Partial-tail requests: {r.partial_request_count:,} "
        f"({r.partial_request_fraction:.2%})"
    )
    s.append(f"- Unique hash IDs: {r.unique_hash_ids:,}")
    s.append(f"- Total hash refs: {r.total_hash_refs:,}")
    s.append(
        f"- Repeated hash refs: {r.repeated_hash_refs:,} "
        f"({r.repeated_hash_fraction:.2%})"
    )
    s.append(
        f"- Infinite-history prefix page hit ratio (diagnostic only): "
        f"{r.infinite_history_page_hit_ratio:.2%}"
    )
    s.append(
        f"- Infinite-history prefix token hit ratio (diagnostic only): "
        f"{r.infinite_history_token_hit_ratio:.2%}"
    )
    s.append(
        f"- Top positive distinct timestamp deltas: "
        f"`{r.positive_distinct_timestamp_delta_top20}`"
    )
    s.append("")
    return "\n".join(s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--conversation", type=Path)
    ap.add_argument("--toolagent", type=Path)
    ap.add_argument("--out-dir", type=Path, default=Path("trace_audit"))
    args = ap.parse_args()

    inputs = []
    if args.conversation:
        inputs.append(("conversation", args.conversation))
    if args.toolagent:
        inputs.append(("toolagent", args.toolagent))
    if not inputs:
        ap.error("provide --conversation and/or --toolagent")

    args.out_dir.mkdir(parents=True, exist_ok=True)

    all_results = {}
    md = ["# Mooncake Trace Full Audit Results", ""]

    for workload, path in inputs:
        result, details = audit_trace(path, workload)
        all_results[workload] = {
            "summary": asdict(result),
            "details": details,
        }
        md.append(result_to_markdown(result))

    (args.out_dir / "trace_audit.json").write_text(
        json.dumps(all_results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (args.out_dir / "trace_audit.md").write_text(
        "\n".join(md),
        encoding="utf-8",
    )

    print(f"Wrote: {args.out_dir / 'trace_audit.json'}")
    print(f"Wrote: {args.out_dir / 'trace_audit.md'}")


if __name__ == "__main__":
    main()
