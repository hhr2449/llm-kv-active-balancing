"""CSV/JSON tables shared by pilot and future formal runs; NA is never zero."""
import csv
import json
from pathlib import Path

DELTA_FIELDS = ("saved_prefill_tokens", "weighted_saved_tokens", "request_hit_rate", "token_hit_rate",
                "skew_ratio", "reactive_wire_bytes", "proactive_wire_bytes", "total_wire_bytes", "transfer_count")


def parameters(config):
    return dict(protocol_version=config["protocol_version"], experiment_kind=config["experiment_kind"],
                workload=config["workload"], line_id=config["line_id"], policy=config["proactive_policy"],
                routing=config["routing_policy"], W=config["future_window_ms"]/1000,
                h=config["history_window_ms"]/1000, K=config["shortlist_k"], q=config["recency_quantile"],
                theta=config["theta"], N=config["num_pods"], action=config["action"])


def main_row(summary):
    metrics = summary["final_metrics"]
    row = parameters(summary["provenance"]["config"])
    row.update({k: v for k, v in metrics["requests"].items() if k != "buckets"})
    row.update({k: v for k, v in metrics["gini"].items() if k != "windows"})
    row.update({k: v for k, v in metrics["wasted"].items() if k != "observations"})
    for network in metrics["network"]:
        if network["phase"] == "EVAL":
            row.update({network["type"].lower()+"_"+key: value for key, value in network.items()
                        if key not in {"phase", "type"}})
    row["transfer_count"] = row["total_transfer_started_count"]
    return row


def deltas(row, baseline):
    result = {k: row[k] for k in ("workload", "line_id", "policy")}
    result["baseline_line_id"] = 2
    for key in DELTA_FIELDS:
        value = None if row[key] is None or baseline[key] is None else row[key]-baseline[key]
        result["delta_"+key] = value
        if key in {"request_hit_rate", "token_hit_rate"}:
            result["delta_"+key+"_pp"] = None if value is None else value*100
    return result


def build_tables(summaries):
    mains = [main_row(s) for s in summaries]
    mains.sort(key=lambda r: (r["workload"], r["line_id"]))
    by_key = {(r["workload"], r["line_id"]): r for r in mains}
    if len(by_key) != len(mains):
        raise ValueError("duplicate workload/line")
    workloads = sorted({r["workload"] for r in mains})
    if any({r["line_id"] for r in mains if r["workload"] == w} != set(range(1, 8)) for w in workloads):
        raise ValueError("report requires all seven lines per workload")
    tables = {w+"_main": [r for r in mains if r["workload"] == w] for w in workloads}
    tables.update(strategy_workload=mains, baseline_delta=[], oracle_reference_delta=[], prompt_bucket=[],
                  gini_window=[], transfer_cost=[], wasted_copy=[])
    for row in mains:
        delta = dict(parameters(next(s["provenance"]["config"] for s in summaries
            if s["provenance"]["config"]["workload"] == row["workload"] and
               s["provenance"]["config"]["line_id"] == row["line_id"])),
                     **deltas(row, by_key[(row["workload"], 2)]))
        tables["baseline_delta"].append(delta)
        if row["line_id"] == 3:
            tables["oracle_reference_delta"].append(dict(delta, name="Oracle reference delta",
                wasted_copy_ratio=row["wasted_copy_ratio"], baseline_wasted_copy_ratio=None))
    summary_by_key = {(s["provenance"]["config"]["workload"], s["provenance"]["config"]["line_id"]): s for s in summaries}
    for summary in summaries:
        config, m = summary["provenance"]["config"], summary["final_metrics"]
        meta = parameters(config)
        for bucket in m["requests"]["buckets"]:
            tables["prompt_bucket"].append(dict(meta, **bucket))
            if config["line_id"] == 3:
                baseline = next(b for b in summary_by_key[(config["workload"], 2)]["final_metrics"]["requests"]["buckets"] if b["bucket"] == bucket["bucket"])
                oracle = next(r for r in tables["oracle_reference_delta"] if r["workload"] == config["workload"])
                for key in ("saved_tokens", "weighted_saved_tokens"):
                    oracle[f"delta_{bucket['bucket']}_{key}"] = bucket[key]-baseline[key]
        tables["gini_window"].extend(dict(meta, **w) for w in m["gini"]["windows"])
        tables["transfer_cost"].extend(dict(meta, **n) for n in m["network"])
        tables["wasted_copy"].append(dict(meta, **{k: v for k, v in m["wasted"].items() if k != "observations"}))
    return tables


def write_tables(directory, summaries):
    directory = Path(directory)
    tables = build_tables(summaries)
    directory.mkdir(parents=True, exist_ok=False)
    for name, rows in tables.items():
        (directory/(name+".json")).write_text(json.dumps(rows, indent=2, sort_keys=True, allow_nan=False)+"\n")
        keys = list(dict.fromkeys(key for row in rows for key in row))
        with (directory/(name+".csv")).open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=keys)
            writer.writeheader()
            for row in rows:
                writer.writerow({k: ("NA" if row.get(k) is None else json.dumps(row[k])
                                     if isinstance(row[k], (list, dict)) else row[k]) for k in keys})
    return tables
