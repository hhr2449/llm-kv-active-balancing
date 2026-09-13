from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "results/summary"
DOC = ROOT / "docs/results/overall_results.md"
SOURCES = (
    ("TASKMAIN", "Conversation", "TaskMain Conversation", "results/taskmain_evaluation/conversation/conversation_main_summary.csv"),
    ("TASKMAIN", "ToolAgent", "TaskMain ToolAgent", "results/taskmain_evaluation/toolagent/toolagent_main_summary.csv"),
    ("MATCHED", "Both", "Matched M01-M04", "results/taskmain_evaluation/matched/matched_main_summary.csv"),
    ("S01_ORACLE_WINDOW", "Both", "S01", "results/taskmain_sweeps/S01_oracle_window/oracle_window_summary.csv"),
    ("S02_PERSISTENCE", "Both", "S02", "results/taskmain_sweeps/S02_persistence_h_k/persistence_h_k_summary.csv"),
    ("S03_RECENCY", "Both", "S03", "results/taskmain_sweeps/S03_recency/recency_summary.csv"),
    ("S04_THETA", "Both", "S04", "results/taskmain_sweeps/S04_theta/theta_summary.csv"),
    ("S05_NUM_PODS", "Both", "S05", "results/taskmain_sweeps/S05_num_pods/num_pods_summary.csv"),
    ("S06_COPY_MOVE", "Both", "S06", "results/taskmain_sweeps/S06_copy_vs_move/copy_vs_move_summary.csv"),
    ("S07_COST_AWARE", "Both", "S07", "results/taskmain_sweeps/S07_cost_aware/cost_aware_summary.csv"),
)
COMMON_METRICS = (
    "mean_completion_ms", "p50_completion_ms", "p95_completion_ms",
    "request_hit_rate", "token_hit_rate", "saved_prefill_tokens",
    "proactive_transfer_count", "proactive_wire_bytes", "total_wire_bytes", "evictions",
    "unused_replica_count", "wasted_copy_count",
)


def read_csv(relative: str) -> list[dict[str, str]]:
    with (ROOT / relative).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def value(row: dict[str, str], *names: str) -> str:
    for name in names:
        if row.get(name, "") != "":
            return row[name]
    return ""


def number(raw: str) -> float:
    return float(raw)


def normalize(category: str, row: dict[str, str], source: str) -> dict[str, str]:
    policy = value(row, "policy")
    if not policy:
        if category == "S06_COPY_MOVE":
            policy = "Persist h=300s K=10"
        elif category == "S07_COST_AWARE":
            policy = "Persist h=300s K=10"
        else:
            policy = value(row, "scheduling", "action")
    if category == "S01_ORACLE_WINDOW":
        policy = "TaskMain-Oracle"
    elif category == "S02_PERSISTENCE":
        policy = f"Persist h={row['h_seconds']}s K={row['K']}"
    elif category == "S03_RECENCY":
        policy = f"Recency q={row['selection_quantile']}"
    elif category == "S04_THETA":
        policy = "R_REQ_KV_TASK"
    n = value(row, "N") or ("4" if category != "S05_NUM_PODS" else "")
    action = value(row, "action")
    if not action and category in {"TASKMAIN", "MATCHED", "S01_ORACLE_WINDOW",
                                   "S02_PERSISTENCE", "S03_RECENCY", "S07_COST_AWARE"}:
        action = "COPY" if value(row, "proactive_transfer_count") not in {"", "0"} else ""
    theta = value(row, "theta")
    oracle_window = value(row, "W_seconds")
    persistence_h = value(row, "h_seconds")
    persistence_k = value(row, "K")
    recency_quantile = value(row, "selection_quantile")
    recency_decay = value(row, "decay_seconds")
    scheduling = value(row, "scheduling")
    if "R_REQ_KV_TASK" in policy and not theta:
        theta = "2.0"
    if ("Oracle" in policy or policy == "Matched-O1") and not oracle_window:
        oracle_window = "300"
    if "Persist h=60" in policy:
        persistence_h, persistence_k = persistence_h or "60", persistence_k or "10"
    elif "Persist h=300" in policy or policy == "Cost-aware":
        persistence_h, persistence_k = persistence_h or "300", persistence_k or "10"
    if "Recency" in policy:
        recency_quantile, recency_decay = recency_quantile or "0.9", recency_decay or "60"
    if category == "S07_COST_AWARE":
        scheduling = row["scheduling"]
    elif action == "COPY" and not scheduling:
        scheduling = "COST_AWARE" if policy == "Cost-aware" else "TRIGGER_ONLY"
    return {
        "category": category,
        "experiment_id": value(row, "experiment_id"),
        "workload": value(row, "workload"),
        "policy": policy,
        "protocol_version": "TaskMain-v1.1",
        "result_source": value(row, "result_source") or "original_result",
        "N": n,
        "theta": theta,
        "oracle_window_seconds": oracle_window,
        "persistence_h_seconds": persistence_h,
        "persistence_k": persistence_k,
        "recency_quantile": recency_quantile,
        "recency_decay_seconds": recency_decay,
        "action": action,
        "scheduling": scheduling,
        "capacity_pages_per_pod": "585",
        "mean_completion_ms": value(row, "mean_completion_ms"),
        "p50_completion_ms": value(row, "p50_completion_ms"),
        "p95_completion_ms": value(row, "p95_completion_ms"),
        "mean_queue_ms": value(row, "mean_queue_ms"),
        "mean_service_ms": value(row, "mean_service_ms"),
        "request_hit_rate": value(row, "request_hit_rate"),
        "token_hit_rate": value(row, "token_hit_rate"),
        "saved_prefill_tokens": value(row, "saved_prefill_tokens"),
        "reactive_transfer_count": value(row, "reactive_transfer_count"),
        "proactive_transfer_count": value(row, "proactive_transfer_count"),
        "reactive_wire_bytes": value(row, "reactive_wire_bytes"),
        "proactive_wire_bytes": value(row, "proactive_wire_bytes"),
        "total_wire_bytes": value(row, "total_wire_bytes", "wire_bytes"),
        "evictions": value(row, "evictions"),
        "fallbacks": value(row, "fallbacks"),
        "unused_replica_count": value(row, "unused_replica_count"),
        "wasted_copy_count": value(row, "wasted_copy_count"),
        "matched_delta_mean_completion_pct": value(row, "delta_mean_completion_pct"),
        "matched_delta_p95_pct": value(row, "delta_p95_completion_pct"),
        "matched_delta_token_hit_pp": value(row, "delta_token_hit_pp"),
        "matched_delta_saved_tokens": value(row, "delta_saved_prefill_tokens"),
        "matched_delta_total_wire_bytes": value(row, "delta_total_wire_bytes"),
        "matched_delta_evictions": value(row, "delta_evictions"),
        "source_file": source,
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise ValueError(f"refusing to write empty table: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def assert_same(label: str, left: dict[str, str], right: dict[str, str],
                fields: Iterable[str] = COMMON_METRICS) -> None:
    differences = {field: (left.get(field, ""), right.get(field, ""))
                   for field in fields if left.get(field, "") != right.get(field, "")}
    if differences:
        raise ValueError(f"reuse mismatch {label}: {differences}")


def consistency_checks(tables: dict[str, list[dict[str, str]]]) -> None:
    main_c, main_t = tables["TaskMain Conversation"], tables["TaskMain ToolAgent"]
    by_id = {r["experiment_id"].split("_", 1)[0]: r for r in main_c + main_t}
    s02 = tables["S02"]
    for eid, workload, h in (("E04", "Conversation", "60"), ("E05", "Conversation", "300"),
                             ("E11", "ToolAgent", "60"), ("E12", "ToolAgent", "300")):
        reused = next(r for r in s02 if r["workload"] == workload and r["h_seconds"] == h and r["K"] == "10")
        assert_same(f"{eid}/S02", by_id[eid], reused)
    for eid, workload in (("E06", "Conversation"), ("E13", "ToolAgent")):
        reused = next(r for r in tables["S03"] if r["workload"] == workload and r["selection_quantile"] == "0.9")
        assert_same(f"{eid}/S03", by_id[eid], reused)
    for eid, workload in (("E02", "Conversation"), ("E09", "ToolAgent")):
        reused = next(r for r in tables["S04"] if r["workload"] == workload and r["theta"] == "2.0")
        fields = tuple(f for f in COMMON_METRICS if not f.startswith("proactive") and "replica" not in f and f != "wasted_copy_count")
        assert_same(f"{eid}/S04", by_id[eid], reused, fields)
    for workload, ids in (("Conversation", ("E01", "E02", "E03", "E04", "E05", "E06", "E07")),
                          ("ToolAgent", ("E08", "E09", "E10", "E11", "E12", "E13", "E14"))):
        reused_rows = [r for r in tables["S05"] if r["workload"] == workload and r["N"] == "4"]
        for eid, reused in zip(ids, reused_rows, strict=True):
            assert_same(f"{eid}/S05", by_id[eid], reused)
    for eid, workload, action in (("E05", "Conversation", "COPY"), ("E12", "ToolAgent", "COPY")):
        reused = next(r for r in tables["S06"] if r["workload"] == workload and r["action"] == action)
        assert_same(f"{eid}/S06", by_id[eid], reused)
    for eid, workload, scheduling in (("E05", "Conversation", "Trigger-only"),
                                      ("E07", "Conversation", "Cost-aware"),
                                      ("E12", "ToolAgent", "Trigger-only"),
                                      ("E14", "ToolAgent", "Cost-aware")):
        reused = next(r for r in tables["S07"] if r["workload"] == workload and r["scheduling"] == scheduling)
        assert_same(f"{eid}/S07", by_id[eid], reused)


def make_main(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    output = []
    for workload in ("Conversation", "ToolAgent"):
        group = [r for r in rows if r["workload"] == workload]
        aff, task = group[0], group[1]
        for row in group:
            item = dict(row)
            item.update({
                "delta_mean_vs_r_aff_pct": (number(row["mean_completion_ms"]) / number(aff["mean_completion_ms"]) - 1) * 100,
                "delta_mean_vs_r_req_kv_task_pct": (number(row["mean_completion_ms"]) / number(task["mean_completion_ms"]) - 1) * 100,
                "delta_p95_vs_r_aff_pct": (number(row["p95_completion_ms"]) / number(aff["p95_completion_ms"]) - 1) * 100,
                "delta_token_hit_vs_r_aff_pp": (number(row["token_hit_rate"]) - number(aff["token_hit_rate"])) * 100,
                "delta_token_hit_vs_r_req_kv_task_pp": (number(row["token_hit_rate"]) - number(task["token_hit_rate"])) * 100,
                "delta_saved_tokens_vs_r_aff": int(row["saved_prefill_tokens"]) - int(aff["saved_prefill_tokens"]),
            })
            output.append(item)
    return output


def make_sweeps(tables: dict[str, list[dict[str, str]]]) -> list[dict[str, str]]:
    output = []
    specs = (
        ("S01", "S01_ORACLE_WINDOW", "oracle_window_seconds", "W_seconds"),
        ("S02", "S02_PERSISTENCE", "persistence_h_k", ""),
        ("S03", "S03_RECENCY", "recency_quantile", "selection_quantile"),
        ("S04", "S04_THETA", "theta", "theta"),
        ("S05", "S05_NUM_PODS", "N", "N"),
        ("S06", "S06_COPY_MOVE", "action", "action"),
        ("S07", "S07_COST_AWARE", "scheduling", "scheduling"),
    )
    for key, sweep_id, parameter_name, parameter_field in specs:
        for row in tables[key]:
            transfer_count = sum(int(value(row, name) or 0) for name in
                                 ("reactive_transfer_count", "proactive_transfer_count"))
            parameter_value = (f"h={row['h_seconds']},K={row['K']}" if key == "S02"
                               else value(row, parameter_field))
            output.append({
                "sweep_id": sweep_id, "workload": row["workload"],
                "policy": value(row, "policy", "scheduling", "action"),
                "parameter_name": parameter_name, "parameter_value": parameter_value,
                "h_seconds": value(row, "h_seconds"), "K": value(row, "K"),
                "mean_completion_ms": row["mean_completion_ms"],
                "p95_completion_ms": row["p95_completion_ms"],
                "token_hit_rate": row["token_hit_rate"],
                "saved_prefill_tokens": row["saved_prefill_tokens"],
                "transfer_count": str(transfer_count),
                "wire_bytes": value(row, "total_wire_bytes", "proactive_wire_bytes"),
                "evictions": row["evictions"],
                "result_source": value(row, "result_source") or "original_result",
                "source_file": next(source for category, _, name, source in SOURCES if name == key),
            })
    return output


def pct(raw: str) -> str:
    return f"{number(raw) * 100:.3f}%" if raw else "N/A"


def md_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    return ["| " + " | ".join(headers) + " |",
            "|" + "|".join("---" for _ in headers) + "|"] + [
        "| " + " | ".join(map(str, row)) + " |" for row in rows]


def make_markdown(tables: dict[str, list[dict[str, str]]], main: list[dict[str, Any]]) -> str:
    lines = ["# TaskMain-v1.1 Results Overview", "",
             "All latency values are milliseconds, wire values are bytes, and hit rates are shown as percentages.", "",
             "## 1. Main Task Results", ""]
    for workload in ("Conversation", "ToolAgent"):
        rows = [r for r in main if r["workload"] == workload]
        lines += [f"### {workload}", ""] + md_table(
            ["Policy", "Mean", "P95", "Token hit", "Saved prefill", "Transfers", "Wire", "Mean Δ vs R_AFF"],
            [[r["policy"], f"{number(r['mean_completion_ms']):.2f}", f"{number(r['p95_completion_ms']):.2f}",
              pct(r["token_hit_rate"]), r["saved_prefill_tokens"],
              int(value(r, "reactive_transfer_count") or 0) + int(value(r, "proactive_transfer_count") or 0),
              r["total_wire_bytes"], f"{r['delta_mean_vs_r_aff_pct']:.3f}%"] for r in rows]) + [""]
    matched = tables["Matched M01-M04"]
    lines += ["## 2. Matched Comparison", ""] + md_table(
        ["Workload", "Baseline", "Matched-O1", "Mean Δ", "P95 Δ", "Hit Δ", "Saved Δ", "Wire Δ"],
        [[matched[i]["workload"], f"{number(matched[i]['mean_completion_ms']):.2f}",
          f"{number(matched[i+1]['mean_completion_ms']):.2f}",
          f"{number(matched[i+1]['delta_mean_completion_pct']):.3f}%",
          f"{number(matched[i+1]['delta_p95_completion_pct']):.3f}%",
          f"{number(matched[i+1]['delta_token_hit_pp']):.3f} pp",
          matched[i+1]["delta_saved_prefill_tokens"], matched[i+1]["delta_total_wire_bytes"]]
         for i in (0, 2)]) + [""]
    s01 = tables["S01"]
    lines += ["## 3. S01 Oracle Window", ""]
    for workload in ("Conversation", "ToolAgent"):
        lines += [f"### {workload}", ""] + md_table(
            ["W (s)", "Mean", "P95", "Token hit", "Saved", "Proactive transfers"],
            [[r["W_seconds"], f"{number(r['mean_completion_ms']):.2f}", f"{number(r['p95_completion_ms']):.2f}", pct(r["token_hit_rate"]), r["saved_prefill_tokens"], r["proactive_transfer_count"]]
             for r in s01 if r["workload"] == workload]) + [""]
    s02 = tables["S02"]
    lines += ["## 4. S02 Persistence h/K", ""]
    for workload in ("Conversation", "ToolAgent"):
        subset = [r for r in s02 if r["workload"] == workload]
        lines += [f"### {workload}: Mean completion", ""] + md_table(
            ["h / K", "5", "10", "20"],
            [[h] + [f"{number(next(r['mean_completion_ms'] for r in subset if r['h_seconds']==h and r['K']==k)):.2f}" for k in ("5", "10", "20")]
             for h in ("60", "300")]) + [""]
        lines += md_table(["h", "K", "P95", "Token hit"],
                          [[r["h_seconds"], r["K"], f"{number(r['p95_completion_ms']):.2f}", pct(r["token_hit_rate"])] for r in subset]) + [""]
    lines += ["## 5. S03 Recency", ""] + md_table(
        ["Workload", "q", "Mean", "P95", "Token hit"],
        [[r["workload"], r["selection_quantile"], f"{number(r['mean_completion_ms']):.2f}", f"{number(r['p95_completion_ms']):.2f}", pct(r["token_hit_rate"])] for r in tables["S03"]]) + [""]
    lines += ["## 6. S04 Theta", ""] + md_table(
        ["Workload", "theta", "Mean", "P95", "Token hit", "Reactive transfers"],
        [[r["workload"], r["theta"], f"{number(r['mean_completion_ms']):.2f}", f"{number(r['p95_completion_ms']):.2f}", pct(r["token_hit_rate"]), r["reactive_transfer_count"]] for r in tables["S04"]]) + [""]
    lines += ["## 7. S05 Number of Pods", "",
              "N=2 在当前 service model 下发生严重系统过载；该 sweep 是整体 cluster-size sensitivity，不能当作单一缓存或路由参数比较。", ""] + md_table(
        ["Workload", "Policy", "N", "Mean", "P95", "Token hit"],
        [[r["workload"], r["policy"], r["N"], f"{number(r['mean_completion_ms']):.2f}", f"{number(r['p95_completion_ms']):.2f}", pct(r["token_hit_rate"])] for r in tables["S05"]]) + [""]
    lines += ["## 8. S06 COPY vs MOVE", ""] + md_table(
        ["Workload", "Action", "Mean", "P95", "Token hit", "Evictions", "Pages freed", "Pages/move", "Zero/partial/full"],
        [[r["workload"], r["action"], f"{number(r['mean_completion_ms']):.2f}", f"{number(r['p95_completion_ms']):.2f}", pct(r["token_hit_rate"]), r["evictions"], value(r,"move_source_pages_freed") or "N/A", value(r,"source_pages_freed_per_move_completed") or "N/A", "/".join(value(r,k) or "N/A" for k in ("zero_release_fraction","partial_release_fraction","full_release_fraction"))] for r in tables["S06"]]) + [""]
    lines += ["## 9. S07 Cost-aware", ""] + md_table(
        ["Workload", "Scheduling", "Mean", "P95", "Token hit", "Saved", "Transfers", "Wire", "Evictions", "Cost rejects"],
        [[r["workload"], r["scheduling"], f"{number(r['mean_completion_ms']):.2f}", f"{number(r['p95_completion_ms']):.2f}", pct(r["token_hit_rate"]), r["saved_prefill_tokens"], r["proactive_transfer_count"], r["total_wire_bytes"], r["evictions"], r["cost_filter_rejected_count"]] for r in tables["S07"]]) + [""]
    lines += ["## 10. Direct Observations", "",
              "- Conversation 主实验各策略的 mean completion 差异整体约在 1% 内。",
              "- ToolAgent 上多个主策略相对 R_AFF 有数个百分点的 completion 差异。",
              "- Matched-O1 相对同底座 R_REQ_KV_V1 的 mean completion improvement 小于 1%。",
              "- S02 的 h/K 结果没有统一单调趋势。",
              "- S03 q=0.8 与 q=0.9 的差异很小。",
              "- S05 的 N=2 配置出现严重过载。",
              "- S06 中 MOVE_SAFE 减少 eviction，但 completion 未改善。",
              "- S07 两个 Cost-aware 行的 cost_filter_rejected_count 均为 0。", ""]
    return "\n".join(lines)


def main() -> None:
    tables: dict[str, list[dict[str, str]]] = {}
    manifest = []
    master = []
    for category, workload, name, source in SOURCES:
        path = ROOT / source
        if path.exists():
            rows = read_csv(source)
            if workload != "Both":
                for row in rows:
                    row.setdefault("workload", workload)
            status = "loaded"
            master.extend(normalize(category, row, source) for row in rows)
        else:
            rows, status = [], "missing"
        tables[name] = rows
        manifest.append({"category": category, "workload": workload,
                         "experiment/sweep": name, "source_csv": source,
                         "rows_loaded": len(rows), "status": status})
    missing = [row for row in manifest if row["status"] == "missing"]
    if missing:
        write_csv(OUT / "result_manifest.csv", manifest)
        raise FileNotFoundError(f"missing result sources: {missing}")
    consistency_checks(tables)
    main_rows = make_main([normalize("TASKMAIN", row, source)
                           for _, _, name, source in SOURCES[:2]
                           for row in tables[name]])
    write_csv(OUT / "master_results.csv", master)
    write_csv(OUT / "main_results.csv", main_rows)
    write_csv(OUT / "sweep_results.csv", make_sweeps(tables))
    write_csv(OUT / "result_manifest.csv", manifest)
    DOC.parent.mkdir(parents=True, exist_ok=True)
    DOC.write_text(make_markdown(tables, main_rows), encoding="utf-8")
    print("reuse_consistency=passed")
    print("NEW_SIMULATION_RUNS=0")


if __name__ == "__main__":
    main()
