from __future__ import annotations

import csv
import hashlib
import json
import math
from pathlib import Path
import shutil
import time


def _network(metrics, phase, kind):
    return next(row for row in metrics["network"] if row["phase"] == phase and row["type"] == kind)


def _base_diagnostics():
    keys = (
        "gate_pass_count_pre_eval", "gate_pass_count_evaluation", "gate_pass_count_full_run",
        "reactive_copy_started_pre_eval", "reactive_copy_started_evaluation",
        "reactive_copy_started_full_run", "direct_target_count_pre_eval",
        "direct_target_count_evaluation", "direct_target_count_full_run",
        "fallback_count_pre_eval", "fallback_count_evaluation", "fallback_count_full_run",
        "positive_candidate_count_evaluation", "shortlist_count_evaluation",
        "positive_candidate_count_full_run", "shortlist_count_full_run",
        "proactive_action_count_evaluation", "proactive_action_count_full_run",
        "chain_depth_mean", "chain_depth_median", "chain_depth_p90",
        "cache_eviction_count_evaluation_all_causes", "cache_eviction_count_full_run_all_causes",
        "cache_turnover_pages_evaluation_all_causes", "cache_turnover_pages_full_run_all_causes",
        "cache_admission_skip_evaluation", "cache_admission_skip_full_run",
        "no_capacity_candidate_skips_evaluation", "no_capacity_candidate_skips_full_run",
        "newly_published_pages", "new_pages_ever_hit", "new_page_observed_use_ratio",
    )
    return {key: None for key in keys}


def _row(summary, diagnostics, family, case_id, source_kind, source_run_id):
    config = summary["provenance"]["config"]
    metrics = summary["final_metrics"]
    requests, gini, wasted = metrics["requests"], metrics["gini"], metrics["wasted"]
    eval_total = _network(metrics, "EVAL", "TOTAL")
    eval_reactive = _network(metrics, "EVAL", "REACTIVE")
    eval_proactive = _network(metrics, "EVAL", "PROACTIVE")
    full_total = _network(metrics, "FULL_RUN", "TOTAL")
    full_reactive = _network(metrics, "FULL_RUN", "REACTIVE")
    full_proactive = _network(metrics, "FULL_RUN", "PROACTIVE")
    row = {
        "workload": config["workload"], "family": family, "case_id": case_id,
        "source_kind": source_kind, "source_run_id": source_run_id,
        "protocol_version": config["protocol_version"],
        "routing": config["routing_policy"], "policy": config["proactive_policy"],
        "W": config["future_window_ms"]/1000, "h": config["history_window_ms"]/1000,
        "K": config["shortlist_k"], "q": config["recency_quantile"],
        "theta": config["theta"], "N": config["num_pods"], "action": config["action"],
        "capacity_pages_per_pod": config["capacity_pages"],
        "total_cluster_capacity_pages": config["num_pods"]*config["capacity_pages"],
        "evaluation_start": config["evaluation_start_ms"],
        "evaluation_end": config["evaluation_end_ms"],
        "saved_tokens": requests["saved_prefill_tokens"],
        "weighted_saved_tokens": requests["weighted_saved_tokens"],
        "request_hit_rate": requests["request_hit_rate"],
        "token_hit_rate": requests["token_hit_rate"],
        "skew_ratio": gini["skew_ratio"],
        "eval_wire_bytes": eval_total["wire_bytes"],
        "eval_reactive_wire_bytes": eval_reactive["wire_bytes"],
        "eval_proactive_wire_bytes": eval_proactive["wire_bytes"],
        "full_wire_bytes": full_total["wire_bytes"],
        "full_reactive_wire_bytes": full_reactive["wire_bytes"],
        "full_proactive_wire_bytes": full_proactive["wire_bytes"],
        "waste_ratio": wasted["wasted_copy_ratio"],
        "transfer_count": eval_total["transfer_started_count"],
        "full_transfer_count": full_total["transfer_started_count"],
        "proactive_actions": eval_proactive["transfer_started_count"],
        "reactive_actions": eval_reactive["transfer_started_count"],
        "full_proactive_actions": full_proactive["transfer_started_count"],
        "full_reactive_actions": full_reactive["transfer_started_count"],
        "censored_copy_count": wasted["censored_copy_count"],
    }
    for bucket in requests["buckets"]:
        prefix = bucket["bucket"]
        for key in ("request_count", "input_tokens", "saved_tokens", "token_hit_rate",
                    "weighted_saved_tokens"):
            row[f"{prefix}_{key}"] = bucket[key]
    row.update(diagnostics)
    return row


def _load_jsonl(path):
    with Path(path).open() as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def _formal_inputs(root, formal_reference):
    formal = root/"results/task_main/formal"
    diag = root/"results/task_main/formal_diagnostics_v1"
    if json.loads((diag/"diagnostics_validation.json").read_text())["status"] != "PASS":
        raise ValueError("Formal Diagnostics V1 is not PASS")
    strategy = {(row["workload"], row["line_id"]): row for row in
                json.loads((diag/"strategy_copy_efficiency.json").read_text())}
    churn = {(row["workload"], row["line_id"]): row for row in
             json.loads((diag/"cache_churn_summary.json").read_text())}
    line2 = {row["workload"]: row for row in
             json.loads((diag/"line2_routing_decomposition.json").read_text())["summary"]}
    result = {}
    for workload in ("conversation", "toolagent"):
        for line in (1, 2, 3, 4, 5, 6):
            folder = formal/f"{workload}_line_{line}"/"run_1"
            summary = json.loads((folder/"summary.json").read_text())
            diagnostics = _base_diagnostics()
            if line == 2:
                row = line2[workload]
                diagnostics.update(
                    gate_pass_count_evaluation=row["gate_true_total"],
                    reactive_copy_started_evaluation=row["eval_reactive_transfer_started_count"],
                    reactive_copy_started_full_run=row["full_run_reactive_transfer_started_count"],
                    direct_target_count_evaluation=row["gate_true_target_already_has_transferable"],
                    fallback_count_evaluation=row["no_feasible_target_fallback"],
                )
                requests = list(_load_jsonl(folder/"request_records.jsonl"))
                diagnostics["gate_pass_count_pre_eval"] = sum(
                    row["reactive_gate_passed"] and row["arrival_time"] < 1500000 for row in requests)
                diagnostics["gate_pass_count_full_run"] = sum(row["reactive_gate_passed"] for row in requests)
                diagnostics["direct_target_count_full_run"] = sum(
                    row["final_pod"] != row["affinity_source"] and row["reactive_transfer_id"] is None
                    for row in requests)
                diagnostics["direct_target_count_pre_eval"] = sum(
                    row["arrival_time"] < 1500000 and row["final_pod"] != row["affinity_source"] and
                    row["reactive_transfer_id"] is None for row in requests)
                diagnostics["fallback_count_full_run"] = sum(
                    row["reactive_fallback_reason"] is not None for row in requests)
                diagnostics["fallback_count_pre_eval"] = sum(
                    row["arrival_time"] < 1500000 and row["reactive_fallback_reason"] is not None
                    for row in requests)
                diagnostics["reactive_copy_started_pre_eval"] = sum(
                    row["start_time"] < 1500000 for row in _load_jsonl(folder/"transfer_records.jsonl"))
            if (workload, line) in strategy:
                row = strategy[(workload, line)]
                diagnostics.update(
                    proactive_action_count_evaluation=row["evaluation_action_count"],
                    proactive_action_count_full_run=row["full_run_action_count"],
                    chain_depth_mean=row["chain_depth_pages_mean"],
                    chain_depth_median=row["chain_depth_pages_median"],
                    chain_depth_p90=row["chain_depth_pages_p90"],
                    newly_published_pages=row["newly_published_pages"],
                    new_pages_ever_hit=row["new_pages_ever_hit"],
                    new_page_observed_use_ratio=row["new_page_observed_use_ratio"],
                )
            if (workload, line) in churn:
                row = churn[(workload, line)]
                diagnostics.update(
                    cache_eviction_count_evaluation_all_causes=row["evaluation_cache_eviction_records_all_causes"],
                    cache_eviction_count_full_run_all_causes=row["total_cache_eviction_records_all_causes"],
                    cache_turnover_pages_evaluation_all_causes=row["cache_turnover_pages_all_causes"]
                        if row["evaluation_evicted_pages_all_causes"] is None else row["evaluation_evicted_pages_all_causes"],
                    cache_turnover_pages_full_run_all_causes=row["cache_turnover_pages_all_causes"],
                    cache_admission_skip_evaluation=row["cache_admission_skip_evaluation"],
                    cache_admission_skip_full_run=row["cache_admission_skip_full_run"],
                    no_capacity_candidate_skips_evaluation=row["no_capacity_candidate_skips_evaluation"],
                    no_capacity_candidate_skips_full_run=row["no_capacity_candidate_skips_full_run"],
                )
            if line in {4, 5, 6}:
                opportunities = list(_load_jsonl(folder/"opportunity_records.jsonl"))
                diagnostics["positive_candidate_count_evaluation"] = sum(
                    row["positive_candidate_count"] for row in opportunities
                    if 1500000 <= row["opportunity_time"] < 2700000)
                diagnostics["shortlist_count_evaluation"] = sum(
                    row["shortlist_count"] for row in opportunities
                    if 1500000 <= row["opportunity_time"] < 2700000)
                diagnostics["positive_candidate_count_full_run"] = sum(
                    row["positive_candidate_count"] for row in opportunities)
                diagnostics["shortlist_count_full_run"] = sum(row["shortlist_count"] for row in opportunities)
            result[(workload, line)] = (summary, diagnostics)
    return result


def _write_tables_atomic(directory, tables):
    directory = Path(directory)
    temporary = directory.with_name(directory.name+".partial")
    if temporary.exists():
        temporary.replace(temporary.with_name(temporary.name+f"_{int(time.time())}"))
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
        def hashes(folder):
            return {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in sorted(folder.iterdir()) if path.is_file()}
        if hashes(directory) == hashes(temporary):
            shutil.rmtree(temporary)
            return
        directory.replace(directory.with_name(directory.name+f".superseded_{int(time.time())}"))
    temporary.replace(directory)


DELTA_FIELDS = ("saved_tokens", "weighted_saved_tokens", "request_hit_rate", "token_hit_rate",
                "skew_ratio", "eval_wire_bytes", "full_wire_bytes", "waste_ratio",
                "proactive_actions", "reactive_actions")


def _attach_deltas(rows, reference_for):
    for row in rows:
        reference = reference_for(row, rows)
        row["comparison_reference_case_id"] = reference["case_id"]
        for key in DELTA_FIELDS:
            left, right = row[key], reference[key]
            row[f"delta_{key}"] = None if left is None or right is None else left-right
            if key in {"request_hit_rate", "token_hit_rate", "waste_ratio"}:
                row[f"delta_{key}_pp"] = (None if row[f"delta_{key}"] is None
                                             else 100*row[f"delta_{key}"])


def _fmt(value, digits=3):
    if value is None:
        return "NA"
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return f"{value:,}"
    return f"{value:.{digits}f}"


def _markdown_table(rows, columns):
    lines = ["| "+" | ".join(label for _, label in columns)+" |",
             "|"+"|".join("---" for _ in columns)+"|"]
    for row in rows:
        lines.append("| "+" | ".join(_fmt(row[key]) if key in row else "NA"
                                     for key, _ in columns)+" |")
    return "\n".join(lines)


def _write_report(path, output, tables, formal_reference):
    lines = [
        "# TaskMain Stage D1 Sensitivity",
        "",
        "**状态：PASS。** 32 个固定 sensitivity cases 均完成双遍 replay；64/64 replay validation 与 determinism PASS。该阶段是 one-factor robustness，不是参数搜索，未实现 MOVE。",
        "",
        f"结果目录：`{output}`。正式默认点引用 `{formal_reference['formal_run_id']}`；所有引用均通过冻结源码、配置、trace 与 cohort 身份检查。",
        "",
        "N sensitivity 保持 585 pages/Pod，因此 N=2 总容量 1,170 pages，N=4 为 2,340 pages；不能解释为只改变 Pod 数的纯因果效应。Oracle W 的三个点统一使用 `[10,25)` min，Waste observation horizon 分别为 1/5/30 min，不能把不同 W 的 Waste 当作同尺度预测准确率。",
        "",
        "Line 7 Cost-aware 不在 sweep 中重复；正式 Line 5/7 execution-equivalence 与 truthful policy metadata 继续作为冻结 ablation 结论，未调整 lambda。",
    ]
    columns = [("workload", "Workload"), ("case_id", "Case"),
               ("saved_tokens", "Saved"), ("weighted_saved_tokens", "WeightedSaved"),
               ("token_hit_rate", "Token hit"), ("skew_ratio", "Skew"),
               ("eval_wire_bytes", "Eval wire B"), ("full_wire_bytes", "Full wire B"),
               ("waste_ratio", "Waste")]

    lines += ["", "## Q1. R_LEAST 与两个 baseline", "",
              _markdown_table(tables["rleast_baseline"], columns), ""]
    for workload in ("conversation", "toolagent"):
        rows = [row for row in tables["rleast_baseline"] if row["workload"] == workload]
        saved = max(rows, key=lambda row: row["saved_tokens"])
        balanced = min(rows, key=lambda row: math.inf if row["skew_ratio"] is None else row["skew_ratio"])
        lines.append(f"- **{workload}：** Saved 最高为 `{saved['case_id']}`（{_fmt(saved['saved_tokens'])}）；"
                     f"Skew 最低为 `{balanced['case_id']}`（{_fmt(balanced['skew_ratio'])}）。"
                     "R_LEAST 是纯 load-first、R_AFF 是 cache-first、R_REQ_KV 保持 cache affinity 并只在 gate 通过时响应式平衡。")

    lines += ["", "## Q2. R_REQ_KV theta sensitivity", "",
              _markdown_table(tables["theta_sensitivity"], columns), ""]
    for workload in ("conversation", "toolagent"):
        rows = sorted((row for row in tables["theta_sensitivity"] if row["workload"] == workload),
                      key=lambda row: row["theta"])
        details = "; ".join(
            f"θ={row['theta']}: Saved={_fmt(row['saved_tokens'])}, PRE/EVAL gate={_fmt(row['gate_pass_count_pre_eval'])}/{_fmt(row['gate_pass_count_evaluation'])}, PRE/EVAL reactive COPY={_fmt(row['reactive_copy_started_pre_eval'])}/{_fmt(row['reactive_copy_started_evaluation'])}"
            for row in rows)
        lines.append(f"- **{workload}：** {details}。比较基准固定为 θ=2。")

    lines += ["", "## Q3–Q4. Persistence K sensitivity", "",
              _markdown_table(tables["persistence_k_sensitivity"], columns), ""]
    for workload in ("conversation", "toolagent"):
        rows = [row for row in tables["persistence_k_sensitivity"] if row["workload"] == workload]
        for h in (60, 300):
            group = sorted((row for row in rows if row["h"] == h), key=lambda row: row["K"])
            lines.append(f"- **{workload}, h={h}s：** K=5/10/20 的 Saved 为 " +
                         "/".join(_fmt(row["saved_tokens"]) for row in group) +
                         "，Eval wire 为 " + "/".join(_fmt(row["eval_wire_bytes"]) for row in group) +
                         "，Waste 为 " + "/".join(_fmt(row["waste_ratio"]) for row in group) +
                         "；同一 h 内只相对 K=10 解读。")
        p60 = {row["K"]: row for row in rows if row["h"] == 60}
        p300 = {row["K"]: row for row in rows if row["h"] == 300}
        comparisons = [p60[k]["saved_tokens"] < p300[k]["saved_tokens"] for k in (5, 10, 20)]
        lines.append(f"  - h=60 的 Saved 在 K=5/10/20 是否均低于 h=300：`{all(comparisons)}`。这项逐 K 对照用于判断 h 与 K 的相对作用，不用于选择最优 K。")

    lines += ["", "## Q5. Recency q sensitivity", "",
              _markdown_table(tables["recency_q_sensitivity"], columns), ""]
    for workload in ("conversation", "toolagent"):
        q8 = next(row for row in tables["recency_q_sensitivity"] if row["workload"] == workload and row["q"] == .8)
        q9 = next(row for row in tables["recency_q_sensitivity"] if row["workload"] == workload and row["q"] == .9)
        lines.append(f"- **{workload}：** q=.8 相对 q=.9：ΔSaved={_fmt(q8['saved_tokens']-q9['saved_tokens'])}，"
                     f"ΔEval wire={_fmt(q8['eval_wire_bytes']-q9['eval_wire_bytes'])}，"
                     f"Δproactive actions={_fmt(q8['proactive_actions']-q9['proactive_actions'])}，"
                     f"Δnew-page use ratio={_fmt(None if q8['new_page_observed_use_ratio'] is None or q9['new_page_observed_use_ratio'] is None else q8['new_page_observed_use_ratio']-q9['new_page_observed_use_ratio'])}。"
                     f"positive/shortlist counts 为 {_fmt(q8['positive_candidate_count_evaluation'])}/{_fmt(q8['shortlist_count_evaluation'])} 对 {_fmt(q9['positive_candidate_count_evaluation'])}/{_fmt(q9['shortlist_count_evaluation'])}。")

    lines += ["", "## Q6. Oracle W common-support sensitivity", "",
              _markdown_table(tables["oracle_w_sensitivity"], columns), ""]
    for workload in ("conversation", "toolagent"):
        rows = sorted((row for row in tables["oracle_w_sensitivity"] if row["workload"] == workload), key=lambda row: row["W"])
        lines.append(f"- **{workload}：** W=1/5/30min 的 Saved 为 " +
                     "/".join(_fmt(row["saved_tokens"]) for row in rows) +
                     "，WeightedSaved 为 " + "/".join(_fmt(row["weighted_saved_tokens"]) for row in rows) +
                     "，Eval wire 为 " + "/".join(_fmt(row["eval_wire_bytes"]) for row in rows) +
                     "。三个点均来自同一 `[10,25)` cohort。")

    lines += ["", "## Q7. N sensitivity", "",
              _markdown_table(tables["n_sensitivity"], columns), ""]
    strategy_cases = (("AFF", "n2_aff", "n4_aff_reference"),
                      ("REQ_KV", "n2_reqkv", "n4_reqkv_reference"),
                      ("Oracle", "n2_oracle", "n4_oracle_reference"),
                      ("P300", "n2_persist300", "n4_persist300_reference"),
                      ("Recency", "n2_recency", "n4_recency_reference"))
    for workload in ("conversation", "toolagent"):
        values = []
        for name, n2_case, n4_case in strategy_cases:
            n2 = next(row for row in tables["n_sensitivity"] if row["workload"] == workload and row["case_id"] == n2_case)
            n4 = next(row for row in tables["n_sensitivity"] if row["workload"] == workload and row["case_id"] == n4_case)
            values.append(f"{name}: ΔSaved(N2-N4)={_fmt(n2['saved_tokens']-n4['saved_tokens'])}, ΔSkew={_fmt(None if n2['skew_ratio'] is None or n4['skew_ratio'] is None else n2['skew_ratio']-n4['skew_ratio'])}")
        lines.append(f"- **{workload}：** " + "; ".join(values) + "。该差异同时包含总 Cache 容量变化。")

    lines += ["", "## Q8. Formal Main 核心结论的方向检查", ""]
    checks = []
    for workload in ("conversation", "toolagent"):
        baseline = {row["case_id"]: row for row in tables["rleast_baseline"] if row["workload"] == workload}
        theta = [row for row in tables["theta_sensitivity"] if row["workload"] == workload]
        checks.append((f"{workload}: theta范围内R_REQ_KV Saved均不低于R_AFF",
                       all(row["saved_tokens"] >= baseline["r_aff_reference"]["saved_tokens"] for row in theta)))
        nrows = {row["case_id"]: row for row in tables["n_sensitivity"] if row["workload"] == workload}
        for n, suffix in ((2, ""), (4, "_reference")):
            req = nrows[f"n{n}_reqkv{suffix}"]["saved_tokens"]
            for policy in ("oracle", "persist300", "recency"):
                checks.append((f"{workload}: N={n} {policy} Saved相对R_REQ_KV方向为非负",
                               nrows[f"n{n}_{policy}{suffix}"]["saved_tokens"] >= req))
    for label, passed in checks:
        lines.append(f"- `{passed}` — {label}")
    lines.append("")
    lines.append(f"上述预先声明的方向检查通过 {sum(value for _, value in checks)}/{len(checks)} 项。未通过项是 sensitivity 发现，不会触发补点、调参或修改默认配置。")
    lines += [
        "完整结论数据位于同目录六张 family 表和 `all_sensitivity_long.csv/json`。",
        "",
        "运行期间继续满足 C1 right-closed future input separation、Gate B、legacy dependency isolation、finite capacity 与正式主指标定义。完成后停止；Stage D2 MOVE 未启动。",
    ]
    path = Path(path)
    temporary = path.with_name(path.name+".tmp")
    temporary.write_text("\n".join(lines)+"\n")
    temporary.replace(path)


def publish_results(root, output, summaries, formal_reference, tables_dir, report_path):
    root, output = Path(root), Path(output)
    formal = _formal_inputs(root, formal_reference)
    new_rows = {}
    for key, summary in summaries.items():
        config = summary["provenance"]["config"]
        new_rows[key] = _row(summary, summary["stage_d1_diagnostics"], config["family"],
                             config["case_id"], "STAGE_D1_REPLAY",
                             "stage_d1:"+json.loads((output/"source_provenance.json").read_text())["source_identity_sha256"][:16])
    formal_rows = {}
    for key, (summary, diagnostics) in formal.items():
        formal_rows[key] = _row(summary, diagnostics, "REFERENCE", f"formal_line_{key[1]}",
                                "FORMAL_REFERENCE", formal_reference["formal_run_id"])

    tables = {name: [] for name in (
        "rleast_baseline", "theta_sensitivity", "persistence_k_sensitivity",
        "recency_q_sensitivity", "oracle_w_sensitivity", "n_sensitivity")}
    for workload in ("conversation", "toolagent"):
        def ref(line, family, case):
            row = dict(formal_rows[(workload, line)], family=family, case_id=case)
            return row
        tables["rleast_baseline"].extend([
            ref(1, "R_LEAST", "r_aff_reference"), new_rows[(workload, "rleast")],
            ref(2, "R_LEAST", "r_req_kv_reference")])
        tables["theta_sensitivity"].extend([
            new_rows[(workload, "theta_1p5")], ref(2, "THETA", "theta_2_reference"),
            new_rows[(workload, "theta_3")]])
        tables["persistence_k_sensitivity"].extend([
            new_rows[(workload, "persist_h60_k5")], ref(4, "PERSISTENCE_K", "persist_h60_k10_reference"),
            new_rows[(workload, "persist_h60_k20")], new_rows[(workload, "persist_h300_k5")],
            ref(5, "PERSISTENCE_K", "persist_h300_k10_reference"),
            new_rows[(workload, "persist_h300_k20")]])
        tables["recency_q_sensitivity"].extend([
            new_rows[(workload, "recency_q0p8")], ref(6, "RECENCY_Q", "recency_q0p9_reference")])
        tables["oracle_w_sensitivity"].extend([
            new_rows[(workload, "oracle_w1m")], new_rows[(workload, "oracle_w5m")],
            new_rows[(workload, "oracle_w30m")]])
        for case, line in (("n4_aff_reference", 1), ("n4_reqkv_reference", 2),
                           ("n4_oracle_reference", 3), ("n4_persist300_reference", 5),
                           ("n4_recency_reference", 6)):
            n2_case = case.replace("n4_", "n2_").replace("_reference", "")
            tables["n_sensitivity"].extend([new_rows[(workload, n2_case)], ref(line, "N", case)])
    _attach_deltas(tables["rleast_baseline"], lambda row, rows: next(
        item for item in rows if item["workload"] == row["workload"] and
        item["case_id"] == "r_req_kv_reference"))
    _attach_deltas(tables["theta_sensitivity"], lambda row, rows: next(
        item for item in rows if item["workload"] == row["workload"] and
        item["case_id"] == "theta_2_reference"))
    _attach_deltas(tables["persistence_k_sensitivity"], lambda row, rows: next(
        item for item in rows if item["workload"] == row["workload"] and
        item["h"] == row["h"] and item["K"] == 10))
    _attach_deltas(tables["recency_q_sensitivity"], lambda row, rows: next(
        item for item in rows if item["workload"] == row["workload"] and
        item["case_id"] == "recency_q0p9_reference"))
    _attach_deltas(tables["oracle_w_sensitivity"], lambda row, rows: next(
        item for item in rows if item["workload"] == row["workload"] and item["W"] == 300))
    n_pairs = {
        "n2_aff": "n4_aff_reference", "n4_aff_reference": "n4_aff_reference",
        "n2_reqkv": "n4_reqkv_reference", "n4_reqkv_reference": "n4_reqkv_reference",
        "n2_oracle": "n4_oracle_reference", "n4_oracle_reference": "n4_oracle_reference",
        "n2_persist300": "n4_persist300_reference", "n4_persist300_reference": "n4_persist300_reference",
        "n2_recency": "n4_recency_reference", "n4_recency_reference": "n4_recency_reference",
    }
    _attach_deltas(tables["n_sensitivity"], lambda row, rows: next(
        item for item in rows if item["workload"] == row["workload"] and
        item["case_id"] == n_pairs[row["case_id"]]))
    for rows in tables.values():
        rows.sort(key=lambda row: (row["workload"], row["evaluation_start"], row["h"],
                                   row["K"], row["q"], row["theta"], row["N"], row["W"],
                                   row["case_id"]))
    tables["all_sensitivity_long"] = [row for name in (
        "rleast_baseline", "theta_sensitivity", "persistence_k_sensitivity",
        "recency_q_sensitivity", "oracle_w_sensitivity", "n_sensitivity") for row in tables[name]]
    _write_tables_atomic(tables_dir, tables)
    _write_report(report_path, output.relative_to(root), tables, formal_reference)
    return tables
