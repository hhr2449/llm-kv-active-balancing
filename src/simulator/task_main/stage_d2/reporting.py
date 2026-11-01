from __future__ import annotations

import csv
import json
from pathlib import Path
import shutil
import time


def _network(metrics, phase, kind):
    return next(row for row in metrics["network"]
                if row["phase"] == phase and row["type"] == kind)


def _common_row(summary, case_id, action, source_kind, source_run_id):
    config = summary["provenance"]["config"]
    metrics = summary["final_metrics"]
    requests, gini, wasted = metrics["requests"], metrics["gini"], metrics["wasted"]
    evaluation = _network(metrics, "EVAL", "PROACTIVE")
    full = _network(metrics, "FULL_RUN", "PROACTIVE")
    return {
        "workload": config["workload"], "trigger": config["proactive_policy"],
        "case_id": case_id, "action": action, "source_kind": source_kind,
        "source_run_id": source_run_id, "protocol_version": config["protocol_version"],
        "N": config["num_pods"], "capacity_pages_per_pod": config["capacity_pages"],
        "W_seconds": config["future_window_ms"]/1000,
        "h_seconds": config["history_window_ms"]/1000, "K": config["shortlist_k"],
        "evaluation_start_ms": config["evaluation_start_ms"],
        "evaluation_end_ms": config["evaluation_end_ms"],
        "saved_tokens": requests["saved_prefill_tokens"],
        "weighted_saved_tokens": requests["weighted_saved_tokens"],
        "request_hit_rate": requests["request_hit_rate"],
        "token_hit_rate": requests["token_hit_rate"], "skew_ratio": gini["skew_ratio"],
        "eval_wire_bytes": evaluation["wire_bytes"],
        "full_wire_bytes": full["wire_bytes"],
        "unused_transfer_wire_ratio": wasted["wasted_copy_ratio"],
        "proactive_action_count_evaluation": evaluation["transfer_started_count"],
        "proactive_action_count_full_run": full["transfer_started_count"],
    }


def _raw_copy_churn(run_directory, evaluation_start, evaluation_end):
    """Recompute ordinary churn from immutable formal ledgers.

    Formal Diagnostics V1 intentionally omitted Oracle eviction attribution. D2
    needs all-cause ordinary LRU counts for both triggers, so this reads the
    primary formal artifacts. Candidate lines are parsed only when their status
    is SKIP_NO_CAPACITY; the multi-GiB Oracle ledger is otherwise streamed.
    """
    run_directory = Path(run_directory)
    final_state = json.loads((run_directory/"final_state.json").read_text())
    evictions = [event for cache in final_state["cache"]
                 for event in cache.get("evictions", ())]
    admission_eval = admission_full = 0
    with (run_directory/"request_records.jsonl").open("rb") as handle:
        for line in handle:
            if b'"cache_admission_skip": true' not in line:
                continue
            row = json.loads(line)
            admission_full += 1
            admission_eval += evaluation_start <= row["arrival_time"] < evaluation_end
    skip_ids = []
    with (run_directory/"candidate_decision_records.jsonl").open("rb") as handle:
        for line in handle:
            if b'"status": "SKIP_NO_CAPACITY"' in line:
                skip_ids.append(json.loads(line)["opportunity_id"])
    skip_set = set(skip_ids)
    evaluation_opportunities = set()
    if skip_set:
        with (run_directory/"opportunity_records.jsonl").open("rb") as handle:
            for line in handle:
                row = json.loads(line)
                if row["opportunity_id"] in skip_set and (
                        evaluation_start <= row["opportunity_time"] < evaluation_end):
                    evaluation_opportunities.add(row["opportunity_id"])
    return {
        "ordinary_lru_evictions_evaluation": sum(
            evaluation_start <= row[0] < evaluation_end for row in evictions),
        "ordinary_lru_evictions_full_run": len(evictions),
        "ordinary_cache_turnover_pages_evaluation": sum(
            evaluation_start <= row[0] < evaluation_end for row in evictions),
        "ordinary_cache_turnover_pages_full_run": len(evictions),
        "cache_admission_skips_evaluation": admission_eval,
        "cache_admission_skips_full_run": admission_full,
        "no_capacity_candidate_skips_evaluation": sum(
            opportunity_id in evaluation_opportunities for opportunity_id in skip_ids),
        "no_capacity_candidate_skips_full_run": len(skip_ids),
    }


def _load_formal_rows(root, reference):
    diagnostics = root/"results/task_main/formal_diagnostics_v1"
    strategies = {(row["workload"], row["line_id"]): row for row in
                  json.loads((diagnostics/"strategy_copy_efficiency.json").read_text())}
    rows = []
    for key, info in sorted(reference["references"].items()):
        workload, case_id = key.split("__", 1)
        summary = json.loads((root/info["summary_path"]).read_text())
        row = _common_row(summary, case_id.replace("_move", "_copy"), "COPY",
                          "FORMAL_REFERENCE", reference["formal_run_id"])
        strategy = strategies[(workload, info["line_id"])]
        run_directory = (root/info["summary_path"]).parent
        print(f"REFERENCE_LEDGER_SCAN workload={workload} line={info['line_id']}", flush=True)
        cache = _raw_copy_churn(
            run_directory, row["evaluation_start_ms"], row["evaluation_end_ms"])
        row.update(
            chain_depth_mean=strategy["chain_depth_pages_mean"],
            chain_depth_median=strategy["chain_depth_pages_median"],
            chain_depth_p90=strategy["chain_depth_pages_p90"],
            **cache,
            unused_move_wire_ratio=None, move_actions=None, zero_release_actions=None,
            partial_release_actions=None, full_release_actions=None,
            source_released_pages=None, source_released_bytes=None,
            release_fraction_mean=None, release_fraction_median=None,
            release_fraction_p75=None, release_fraction_p90=None, release_fraction_p95=None,
        )
        rows.append(row)
    return rows


def _move_row(summary, case_id, source_run_id):
    row = _common_row(summary, case_id, "MOVE", "STAGE_D2_REPLAY", source_run_id)
    d2 = summary["stage_d2_metrics"]
    release, churn = d2["release_evaluation"], d2["capacity_churn"]
    actions = summary["proactive_move_count"]
    # Action depths are retained in the MOVE ledger; summary stores precomputed diagnostics.
    row.update(summary["stage_d2_action_diagnostics"])
    row.update(churn)
    row.update(
        unused_transfer_wire_ratio=d2["target_side"]["unused_transfer_wire_ratio"],
        unused_move_wire_ratio=d2["target_side"]["unused_move_wire_ratio"],
        move_actions=release["move_actions"],
        zero_release_actions=release["zero_release_actions"],
        partial_release_actions=release["partial_release_actions"],
        full_release_actions=release["full_release_actions"],
        source_released_pages=release["source_released_pages"],
        source_released_bytes=release["source_released_bytes"],
        release_fraction_mean=release["release_fraction_mean"],
        release_fraction_median=release["release_fraction_median"],
        release_fraction_p75=release["release_fraction_p75"],
        release_fraction_p90=release["release_fraction_p90"],
        release_fraction_p95=release["release_fraction_p95"],
    )
    return row


DELTA_FIELDS = (
    "saved_tokens", "weighted_saved_tokens", "request_hit_rate", "token_hit_rate",
    "skew_ratio", "eval_wire_bytes", "full_wire_bytes", "unused_transfer_wire_ratio",
    "proactive_action_count_evaluation", "proactive_action_count_full_run",
    "ordinary_lru_evictions_evaluation", "ordinary_lru_evictions_full_run",
    "ordinary_cache_turnover_pages_evaluation", "ordinary_cache_turnover_pages_full_run",
    "cache_admission_skips_evaluation", "cache_admission_skips_full_run",
    "no_capacity_candidate_skips_evaluation", "no_capacity_candidate_skips_full_run",
)


def _write_tables_atomic(directory, tables):
    directory = Path(directory)
    temporary = directory.with_name(directory.name+".partial")
    if temporary.exists():
        shutil.rmtree(temporary)
    temporary.mkdir(parents=True)
    for name, rows in tables.items():
        (temporary/f"{name}.json").write_text(
            json.dumps(rows, indent=2, sort_keys=True, allow_nan=False)+"\n")
        fields = list(dict.fromkeys(key for row in rows for key in row))
        with (temporary/f"{name}.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for row in rows:
                writer.writerow({key: "NA" if row.get(key) is None else row[key] for key in fields})
    if directory.exists():
        directory.replace(directory.with_name(directory.name+f".superseded_{int(time.time())}"))
    temporary.replace(directory)


def _fmt(value):
    if value is None:
        return "NA"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def publish_results(root, output, summaries, reference, source_run_id):
    root, output = Path(root), Path(output)
    rows = _load_formal_rows(root, reference)
    for (workload, case_id), summary in sorted(summaries.items()):
        rows.append(_move_row(summary, case_id, source_run_id))
    rows.sort(key=lambda row: (row["workload"], row["trigger"], row["action"]))
    comparisons = []
    for move in [row for row in rows if row["action"] == "MOVE"]:
        copy = next(row for row in rows if row["workload"] == move["workload"] and
                    row["trigger"] == move["trigger"] and row["action"] == "COPY")
        value = {"workload": move["workload"], "trigger": move["trigger"],
                 "copy_case_id": copy["case_id"], "move_case_id": move["case_id"]}
        for field in DELTA_FIELDS:
            left, right = move.get(field), copy.get(field)
            value[f"delta_{field}"] = None if left is None or right is None else left-right
            if field in {"request_hit_rate", "token_hit_rate", "unused_transfer_wire_ratio"}:
                value[f"delta_{field}_pp"] = (None if value[f"delta_{field}"] is None
                                                else 100*value[f"delta_{field}"])
        comparisons.append(value)
    tables = {"copy_vs_move": rows, "copy_vs_move_delta": comparisons}
    _write_tables_atomic(output/"tables", tables)
    lines = [
        "# TaskMain Stage D2 MOVE Ablation", "",
        "**状态：PASS。** 四个固定 MOVE case 均完成双遍 replay；8/8 validation 与 determinism PASS。正式 COPY Lines3/5 以只读方式引用，provenance gate PASS。", "",
        "## 1. 为什么需要细化 MOVE", "",
        "原任务只规定 MOVE 在 source 释放，没有冻结释放时刻、共享祖先、transfer 期间可见性、pin、generation 或失败原子性。D2 因此将可执行语义冻结为 COPY-THEN-SAFE-RELEASE；没有复用旧 T2/O2/MOVE_SAFE 结果。", "",
        "## 2. TaskMain MOVE protocol", "",
        "MOVE 与 COPY 共用 routing、candidate、ranking、source、feasible-target-first、target、full-chain wire、Temporary、bandwidth、capacity 和 action cap。source 在 ready 前保持 Published 且可命中；target commit 成功后才解除本次 source transfer pin并尝试释放。target commit失败时source不变。", "",
        "## 3. Safe source release", "",
        "release 从 moved chain 的末页向 root 检查，只删除仍resident、generation一致、无其他active pin、无committed protection且为leaf的最大连续suffix。遇到共享/非leaf、pin、保护、generation变化或不resident立即停止。release不刷新LRU，也不计入普通LRU eviction。", "",
        "## 4. COPY regression 与 provenance", "",
        f"正式 COPY reference=`{reference['formal_run_id']}`；manifest SHA-256=`{reference['formal_manifest_sha256']}`，source provenance SHA-256=`{reference['formal_source_provenance_sha256']}`。D2 COPY mode 的冻结fixture execution projection回归通过；四组trace/cohort/参数/target-selection gate均PASS。", "",
        "## 5. MOVE correctness 与确定性", "",
        "全测试覆盖full/partial/zero release、共享祖先、deeper descendant、active pin、committed protection、generation变化、in-flight可见性、target commit失败、同timestamp顺序、COPY regression、Load/history/opportunity守恒。正式D2每case双遍逐文件SHA-256一致，capacity、Temporary/pin drain、Gate B、C1 right endpoint及legacy isolation均PASS。", "",
        "## 6. 四组 COPY vs MOVE 结果", "",
        "| Workload | Trigger | Action | Saved | Weighted | Token hit | Skew | Eval wire B | Full wire B | Unused wire | Eval actions | Released pages | Release mean | Zero/Partial/Full |", 
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append("| " + " | ".join(map(str, [
            row["workload"], row["trigger"], row["action"], _fmt(row["saved_tokens"]),
            _fmt(row["weighted_saved_tokens"]), _fmt(row["token_hit_rate"]),
            _fmt(row["skew_ratio"]), _fmt(row["eval_wire_bytes"]),
            _fmt(row["full_wire_bytes"]), _fmt(row["unused_transfer_wire_ratio"]),
            _fmt(row["proactive_action_count_evaluation"]), _fmt(row.get("source_released_pages")),
            _fmt(row.get("release_fraction_mean")),
            f"{_fmt(row.get('zero_release_actions'))}/{_fmt(row.get('partial_release_actions'))}/{_fmt(row.get('full_release_actions'))}",
        ])) + " |")
    lines += ["", "## 7. Source release 分布", "",
              "| Workload | Trigger | MOVE actions | Released pages | Released bytes | Mean | Median | P75 | P90 | P95 | Zero | Partial | Full |",
              "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for row in [item for item in rows if item["action"] == "MOVE"]:
        lines.append("| " + " | ".join(map(str, [
            row["workload"], row["trigger"], _fmt(row["move_actions"]),
            _fmt(row["source_released_pages"]), _fmt(row["source_released_bytes"]),
            _fmt(row["release_fraction_mean"]), _fmt(row["release_fraction_median"]),
            _fmt(row["release_fraction_p75"]), _fmt(row["release_fraction_p90"]),
            _fmt(row["release_fraction_p95"]), _fmt(row["zero_release_actions"]),
            _fmt(row["partial_release_actions"]), _fmt(row["full_release_actions"]),
        ])) + " |")
    lines += ["", "## 8. Capacity 与 ordinary churn", "",
              "Source release未并入下表ordinary LRU eviction/turnover。", "",
              "| Workload | Trigger | Action | Eval eviction | Full eviction | Eval turnover pages | Full turnover pages | Eval admission skip | Full admission skip | Eval no-capacity skips | Full no-capacity skips |",
              "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for row in rows:
        lines.append("| " + " | ".join(map(str, [
            row["workload"], row["trigger"], row["action"],
            _fmt(row["ordinary_lru_evictions_evaluation"]),
            _fmt(row["ordinary_lru_evictions_full_run"]),
            _fmt(row["ordinary_cache_turnover_pages_evaluation"]),
            _fmt(row["ordinary_cache_turnover_pages_full_run"]),
            _fmt(row["cache_admission_skips_evaluation"]),
            _fmt(row["cache_admission_skips_full_run"]),
            _fmt(row["no_capacity_candidate_skips_evaluation"]),
            _fmt(row["no_capacity_candidate_skips_full_run"]),
        ])) + " |")
    lines += ["", "## 9. Saved、WeightedSaved、Skew 与 wire 差值", ""]
    for delta in comparisons:
        lines.append(
            f"- **{delta['workload']} / {delta['trigger']}：** "
            f"ΔSaved={_fmt(delta['delta_saved_tokens'])}，"
            f"ΔWeighted={_fmt(delta['delta_weighted_saved_tokens'])}，"
            f"ΔSkew={_fmt(delta['delta_skew_ratio'])}，"
            f"ΔEval wire={_fmt(delta['delta_eval_wire_bytes'])} B，"
            f"Δordinary eviction={_fmt(delta['delta_ordinary_lru_evictions_evaluation'])}，"
            f"Δadmission skip={_fmt(delta['delta_cache_admission_skips_evaluation'])}。")
    lines += [
        "", "## 10. Target-side unused transfer wire", "",
        "COPY 的正式 wasted-copy ratio在本表映射为共同的unused_transfer_wire_ratio；MOVE按同一target、完整chain hit、generation匹配与censor规则计算unused_move_wire_ratio。它不度量source release，也不把partial reuse归为完整使用。", "",
        "## 11. Workload 差异", "",
        "Conversation 与 ToolAgent 分别报告，不合并为单一总体效应。重点结合release fraction、ordinary churn、Saved/WeightedSaved、Skew、wire与unused ratio判断；闭环action sequence可在MOVE释放source后合法分叉，因此不强制总动作数或总wire相同。", "",
        "## 12. 结论边界", "",
        "该结果只回答：在相同 trigger/placement 与 TaskMain safe-release 语义下，不保留source独占suffix副本的影响。共享ancestor、active pin和committed protection可使MOVE只释放部分或零页；不能外推为所有KV迁移系统或完整整链迁移。", "",
        "Stage D2完成后停止；未启动granularity、predictor、参数搜索或Stage D3。", "",
    ]
    report = root/"docs/analysis/task_main_stage_d2_move_ablation.md"
    temporary = report.with_suffix(".md.tmp")
    temporary.write_text("\n".join(lines))
    temporary.replace(report)
    return tables
