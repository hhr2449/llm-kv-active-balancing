from __future__ import annotations

import math
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch

from .common import (
    BUCKET_LABELS,
    MAIN_ORDER,
    PALETTE,
    WORKLOAD_COLORS,
    WORKLOAD_TITLES,
    FigureContext,
    assert_close,
    finite,
    format_signed_pct,
    integer,
    number,
    panel_label,
    read_csv,
    strategy_label,
    style_axis,
)


def build_fig01(ctx: FigureContext) -> None:
    source = read_csv("baseline_comparison.csv")
    rows: list[dict[str, Any]] = []
    for row in source:
        rows.append(
            {
                "workload": row["workload"],
                "strategy": row["baseline"],
                "saved_million": number(row["Saved_M"]),
                "skew_ratio": number(row["Skew"]),
                "canonical_case_id": row["canonical_case_id"],
            }
        )
    fig, axes = plt.subplots(1, 2, figsize=(9.2, 3.8))
    for ax, workload, letter in zip(axes, ["conversation", "toolagent"], ["(a)", "(b)"]):
        subset = [r for r in rows if r["workload"] == workload]
        for row in subset:
            ax.scatter(
                row["skew_ratio"], row["saved_million"], s=66,
                color=PALETTE[row["strategy"]], edgecolor="white", linewidth=0.7, zorder=3,
            )
            ax.annotate(
                row["strategy"], (row["skew_ratio"], row["saved_million"]),
                xytext=(5, 5), textcoords="offset points", fontsize=8,
            )
        ax.set_title(WORKLOAD_TITLES[workload])
        ax.set_xlabel("Request-distribution skew ratio")
        ax.set_ylabel("Saved Prefill Tokens (M)")
        style_axis(ax)
        panel_label(ax, letter)
    fig.suptitle("Baseline routing trade-off", y=1.02, fontsize=12)
    fig.tight_layout()
    ctx.add(
        figure_id="fig01_baseline_tradeoff", title="Baseline routing trade-off",
        script="scripts/task_main/final_figures/fig01_baselines.py",
        input_tables=["baseline_comparison.csv"], rows=rows,
        fieldnames=list(rows[0]), fig=fig, cohort="MAIN_25_45",
        notes="Scatter coordinates are canonical Saved_M and Skew; no workload averaging.",
    )


def build_fig02(ctx: FigureContext) -> None:
    source = read_csv("main_7line_combined.csv")
    canonical = {row["canonical_case_id"]: row for row in read_csv("final_results_long.csv")}
    baseline_saved = {
        row["workload"]: number(row["Saved_M"])
        for row in source if integer(row["Line"]) == 2
    }
    rows: list[dict[str, Any]] = []
    for row in source:
        label = strategy_label(row["Strategy"])
        saved_m = number(row["Saved_M"])
        delta_saved_pct = (saved_m - baseline_saved[row["workload"]]) / baseline_saved[row["workload"]] * 100
        assert_close(delta_saved_pct, number(row["Delta_Saved_vs_Line2_pct"]))
        raw_unused_ratio = number(canonical[row["canonical_case_id"]]["unused_transfer_wire_ratio"])
        unused_wire_pct = raw_unused_ratio * 100 if finite(raw_unused_ratio) else math.nan
        if finite(unused_wire_pct):
            assert_close(unused_wire_pct, number(row["Unused_Wire_pct"]))
        rows.append(
            {
                "workload": row["workload"], "line": integer(row["Line"]), "strategy": label,
                "saved_million": saved_m, "line2_saved_million": baseline_saved[row["workload"]],
                "delta_saved_pct": delta_saved_pct,
                "skew_ratio": number(row["Skew"]), "eval_wire_tb": number(row["Eval_Wire_TB"]),
                "unused_transfer_wire_ratio_raw": raw_unused_ratio, "unused_wire_pct": unused_wire_pct,
                "canonical_case_id": row["canonical_case_id"],
            }
        )
    order = MAIN_ORDER
    x = np.arange(len(order))
    width = 0.36
    fig, axes = plt.subplots(2, 2, figsize=(12.2, 7.4))
    specs = [
        ("delta_saved_pct", "Δ Saved Tokens vs R_REQ_KV (%)", True),
        ("skew_ratio", "Request-distribution skew ratio", False),
        ("eval_wire_tb", "Evaluation wire (TB)", False),
        ("unused_wire_pct", "Unused transfer wire (%)", False),
    ]
    workloads = ["conversation", "toolagent"]
    hatches = {"conversation": "", "toolagent": "///"}
    for ax, (field, ylabel, zero), letter in zip(axes.flat, specs, ["(a)", "(b)", "(c)", "(d)"]):
        for wi, workload in enumerate(workloads):
            lookup = {(r["strategy"]): r for r in rows if r["workload"] == workload}
            vals = [lookup[name][field] for name in order]
            bars = ax.bar(
                x + (wi - 0.5) * width, vals, width,
                color=[PALETTE[name] for name in order], hatch=hatches[workload],
                edgecolor="#333333", linewidth=0.45, zorder=2,
            )
            if field == "unused_wire_pct":
                for bar, value in zip(bars, vals):
                    if math.isnan(value):
                        bar.set_visible(False)
        ax.set_ylabel(ylabel)
        ax.set_xticks(x, [name.replace("Persistence-", "P-").replace("Persistence+Cost", "P+Cost") for name in order], rotation=28, ha="right")
        style_axis(ax, zero_line=zero)
        panel_label(ax, letter)
    axes[0, 0].legend(
        handles=[Patch(facecolor="#bbbbbb", edgecolor="#333333", label="Conversation"),
                 Patch(facecolor="#bbbbbb", edgecolor="#333333", hatch="///", label="ToolAgent")],
        loc="best", ncol=2,
    )
    axes[1, 1].text(
        0.02, 0.05, "NA: Lines 1–2 have no proactive transfers", transform=axes[1, 1].transAxes,
        fontsize=8, color="#555555",
    )
    fig.text(0.5, 0.005, "Cost-aware gate inactive under task-specified scaling (Line 5 = Line 7 execution)", ha="center", fontsize=9)
    fig.suptitle("Formal Main: benefit, balance, transfer cost, and efficiency", y=0.995, fontsize=12)
    fig.tight_layout(rect=(0, 0.03, 1, 0.97))
    ctx.add(
        figure_id="fig02_main_7line", title="Formal Main 7-line comparison",
        script="scripts/task_main/final_figures/fig02_main.py",
        input_tables=["main_7line_combined.csv", "final_results_long.csv"], rows=rows, fieldnames=list(rows[0]), fig=fig,
        cohort="MAIN_25_45", notes="NA unused-wire values are omitted, not plotted as zero.",
    )
    ctx.record_check("figure2_percentages_recomputed_from_raw", True)


def build_fig03(ctx: FigureContext) -> None:
    source = read_csv("oracle_headroom.csv")
    rows: list[dict[str, Any]] = []
    for row in source:
        baseline_saved = number(row["R_REQ_KV_Saved"])
        oracle_saved = number(row["Oracle_Saved"])
        baseline_weighted = number(row["R_REQ_KV_Weighted"])
        oracle_weighted = number(row["Oracle_Weighted"])
        delta_saved_pct = (oracle_saved - baseline_saved) / baseline_saved * 100
        delta_weighted_pct = (oracle_weighted - baseline_weighted) / baseline_weighted * 100
        assert_close(delta_saved_pct, number(row["Delta_Saved_pct"]))
        assert_close(delta_weighted_pct, number(row["Delta_Weighted_pct"]))
        rows.append(
            {
                "workload": row["workload"], "baseline_saved": baseline_saved,
                "oracle_saved": oracle_saved, "delta_saved": oracle_saved - baseline_saved,
                "delta_saved_pct": delta_saved_pct, "baseline_weighted": baseline_weighted,
                "oracle_weighted": oracle_weighted, "delta_weighted": oracle_weighted - baseline_weighted,
                "delta_weighted_pct": delta_weighted_pct, "comparison_status": row["comparison_status"],
            }
        )
    expected = {"conversation": 56.82, "toolagent": 7.05}
    for row in rows:
        if round(row["delta_saved_pct"], 2) != expected[row["workload"]]:
            raise AssertionError("Oracle Saved delta does not match the frozen expected percentage")
    fig, axes = plt.subplots(2, 2, figsize=(8.8, 6.5))
    for ri, row in enumerate(rows):
        for ci, (base_key, oracle_key, ylabel, delta_key) in enumerate(
            [("baseline_saved", "oracle_saved", "Saved Prefill Tokens (M)", "delta_saved_pct"),
             ("baseline_weighted", "oracle_weighted", "WeightedSaved (M)", "delta_weighted_pct")]
        ):
            ax = axes[ri, ci]
            vals = [row[base_key] / 1e6, row[oracle_key] / 1e6]
            bars = ax.bar([0, 1], vals, color=[PALETTE["R_REQ_KV"], PALETTE["Oracle"]], width=0.62, zorder=2)
            ax.set_xticks([0, 1], ["R_REQ_KV", "Oracle"])
            ax.set_ylabel(ylabel)
            ax.set_title(WORKLOAD_TITLES[row["workload"]])
            ax.text(1, vals[1], format_signed_pct(row[delta_key]), ha="center", va="bottom", weight="bold", fontsize=9)
            style_axis(ax)
            panel_label(ax, f"({chr(97 + ri * 2 + ci)})")
    fig.suptitle("Future-Demand Oracle reference headroom", y=1.01, fontsize=12)
    fig.tight_layout()
    ctx.add(
        figure_id="fig03_oracle_headroom", title="Oracle reference headroom",
        script="scripts/task_main/final_figures/fig03_oracle.py",
        input_tables=["oracle_headroom.csv"], rows=rows, fieldnames=list(rows[0]), fig=fig,
        cohort="MAIN_25_45", notes="Percentages are recomputed from canonical raw token values.",
    )
    ctx.record_check("figure3_oracle_delta_matches_canonical", True)
    ctx.record_check("figure3_expected_saved_percentages", True)


def _bucket_rows() -> list[dict[str, Any]]:
    selected = {"3": "Oracle", "5": "Persistence-300s", "6": "Recency"}
    rows = []
    for row in read_csv("prompt_bucket_final.csv"):
        if row["line"] not in selected:
            continue
        rows.append(
            {
                "workload": row["workload"], "line": integer(row["line"]),
                "strategy": selected[row["line"]], "bucket": row["bucket"],
                "bucket_label": BUCKET_LABELS[row["bucket"]],
                "delta_saved_tokens": number(row["delta_saved_vs_line2"]),
                "delta_weighted_saved": number(row["delta_weighted_vs_line2"]),
                "evaluation_label": row["evaluation_label"],
            }
        )
    return rows


def _plot_bucket_metric(ctx: FigureContext, figure_id: str, metric: str, ylabel: str, title: str) -> None:
    rows = _bucket_rows()
    strategies = ["Oracle", "Persistence-300s", "Recency"]
    buckets = list(BUCKET_LABELS)
    x = np.arange(len(buckets))
    width = 0.25
    fig, axes = plt.subplots(1, 2, figsize=(11.2, 4.1), sharex=True)
    for ax, workload, letter in zip(axes, ["conversation", "toolagent"], ["(a)", "(b)"]):
        for si, strategy in enumerate(strategies):
            lookup = {r["bucket"]: r for r in rows if r["workload"] == workload and r["strategy"] == strategy}
            vals = [lookup[b][metric] / 1e6 for b in buckets]
            ax.bar(x + (si - 1) * width, vals, width, color=PALETTE[strategy], label=strategy, zorder=2)
        ax.set_xticks(x, [BUCKET_LABELS[b] for b in buckets], rotation=22, ha="right")
        ax.set_xlabel("Prompt length (tokens)")
        ax.set_ylabel(ylabel)
        ax.set_title(WORKLOAD_TITLES[workload])
        style_axis(ax, zero_line=True)
        panel_label(ax, letter)
    axes[0].legend(ncol=1, fontsize=8)
    fig.suptitle(title, y=1.01, fontsize=12)
    fig.tight_layout()
    ctx.add(
        figure_id=figure_id, title=title, script="scripts/task_main/final_figures/fig04_buckets.py",
        input_tables=["prompt_bucket_final.csv"], rows=rows, fieldnames=list(rows[0]), fig=fig,
        cohort="MAIN_25_45", notes="All six buckets are retained, including zero and negative deltas.",
    )


def build_fig04(ctx: FigureContext) -> None:
    _plot_bucket_metric(
        ctx, "fig04_prompt_bucket_contribution", "delta_weighted_saved",
        "Δ WeightedSaved vs R_REQ_KV (M)", "Prompt-bucket contribution to WeightedSaved",
    )
    _plot_bucket_metric(
        ctx, "fig04b_prompt_bucket_saved_delta", "delta_saved_tokens",
        "Δ Saved Tokens vs R_REQ_KV (M)", "Prompt-bucket contribution to Saved Tokens",
    )


def _persistence_rows() -> list[dict[str, Any]]:
    rows = []
    for row in read_csv("persistence_sensitivity_final.csv"):
        rows.append(
            {
                "workload": row["workload"], "h_seconds": number(row["h"]), "K": integer(row["K"]),
                "saved_tokens": number(row["Saved"]), "eval_wire_bytes": number(row["Eval_Wire"]),
                "unused_wire_ratio": number(row["Waste"]), "chain_depth_mean": number(row["chain_depth_mean"]),
                "evaluation_label": row["evaluation_label"], "canonical_case_id": row["canonical_case_id"],
            }
        )
    return rows


def _plot_persistence(ctx: FigureContext, figure_id: str, metric: str, ylabel: str, title: str, divisor: float = 1.0) -> None:
    rows = _persistence_rows()
    fig, axes = plt.subplots(1, 2, figsize=(9.6, 3.9), sharex=True)
    for ax, workload, letter in zip(axes, ["conversation", "toolagent"], ["(a)", "(b)"]):
        for h, label in [(60.0, "h=60s"), (300.0, "h=300s")]:
            subset = sorted([r for r in rows if r["workload"] == workload and r["h_seconds"] == h], key=lambda r: r["K"])
            ax.plot(
                [r["K"] for r in subset], [r[metric] / divisor for r in subset], marker="o", linewidth=1.8,
                color=PALETTE["Persistence-60s" if h == 60 else "Persistence-300s"], label=label,
            )
        ax.set_xticks([5, 10, 20])
        ax.set_xlabel("Shortlist size K")
        ax.set_ylabel(ylabel)
        ax.set_title(WORKLOAD_TITLES[workload])
        style_axis(ax)
        panel_label(ax, letter)
    axes[0].legend()
    fig.suptitle(title, y=1.01, fontsize=12)
    fig.tight_layout()
    ctx.add(
        figure_id=figure_id, title=title, script="scripts/task_main/final_figures/fig05_persistence.py",
        input_tables=["persistence_sensitivity_final.csv"], rows=rows, fieldnames=list(rows[0]), fig=fig,
        cohort="MAIN_25_45", notes="The figure describes sensitivity; it does not declare an optimal K.",
    )


def build_fig05(ctx: FigureContext) -> None:
    rows = _persistence_rows()
    combos = {(r["h_seconds"], r["K"]) for r in rows}
    ctx.record_check("figure5_persistence_grid", combos == {(h, k) for h in [60.0, 300.0] for k in [5, 10, 20]})
    _plot_persistence(ctx, "fig05_persistence_sensitivity", "saved_tokens", "Saved Prefill Tokens (M)", "Persistence sensitivity: benefit", 1e6)
    _plot_persistence(ctx, "fig05b_persistence_wire", "eval_wire_bytes", "Evaluation wire (TB)", "Persistence sensitivity: transfer cost", 1e12)
    _plot_persistence(ctx, "fig05c_persistence_waste", "unused_wire_ratio", "Unused transfer wire (%)", "Persistence sensitivity: copy efficiency", 0.01)


def build_fig06(ctx: FigureContext) -> None:
    rows = []
    for row in read_csv("oracle_w_sensitivity_final.csv"):
        rows.append(
            {
                "workload": row["workload"], "W_minutes": number(row["W_minutes"]),
                "saved_tokens": number(row["Saved"]), "eval_wire_bytes": number(row["Eval_Wire"]),
                "skew_ratio": number(row["Skew"]), "unused_ratio": number(row["Unused_ratio"]),
                "evaluation_label": row["evaluation_label"], "canonical_case_id": row["canonical_case_id"],
            }
        )
    ctx.record_check("figure6_oracle_w_grid", {r["W_minutes"] for r in rows} == {1.0, 5.0, 30.0})
    ctx.record_check("figure6_common_cohort", {r["evaluation_label"] for r in rows} == {"W_SENS_10_25"})
    fig, axes = plt.subplots(2, 3, figsize=(11.8, 6.3), sharex=True)
    metrics = [
        ("saved_tokens", 1e6, "Saved Prefill Tokens (M)"),
        ("eval_wire_bytes", 1e12, "Evaluation wire (TB)"),
        ("skew_ratio", 1.0, "Skew ratio"),
    ]
    for ri, workload in enumerate(["conversation", "toolagent"]):
        subset = sorted([r for r in rows if r["workload"] == workload], key=lambda r: r["W_minutes"])
        for ci, (metric, divisor, ylabel) in enumerate(metrics):
            ax = axes[ri, ci]
            ax.plot(
                [r["W_minutes"] for r in subset], [r[metric] / divisor for r in subset], marker="o",
                color=WORKLOAD_COLORS[workload], linewidth=1.8,
            )
            ax.set_xticks([1, 5, 30], ["1", "5", "30"])
            ax.set_xlabel("Future horizon W (min)")
            ax.set_ylabel(ylabel)
            ax.set_title(WORKLOAD_TITLES[workload])
            style_axis(ax)
            panel_label(ax, f"({chr(97 + ri * 3 + ci)})")
    fig.suptitle("Oracle future-horizon sensitivity (common [10,25) min cohort)", y=1.01, fontsize=12)
    fig.tight_layout()
    ctx.add(
        figure_id="fig06_oracle_horizon", title="Oracle future-horizon sensitivity",
        script="scripts/task_main/final_figures/fig06_oracle_w.py",
        input_tables=["oracle_w_sensitivity_final.csv"], rows=rows, fieldnames=list(rows[0]), fig=fig,
        cohort="W_SENS_10_25", notes="W uses true numerical positions on a log-scaled axis; Formal Main is excluded.",
    )


def build_fig07(ctx: FigureContext) -> None:
    theta_rows = []
    for row in read_csv("theta_sensitivity_final.csv"):
        theta_rows.append(
            {
                "workload": row["workload"], "theta": number(row["theta"]),
                "saved_tokens": number(row["Saved"]), "skew_ratio": number(row["Skew"]),
                "full_reactive_wire_bytes": number(row["Full_reactive_wire"]),
                "evaluation_label": row["evaluation_label"], "canonical_case_id": row["canonical_case_id"],
            }
        )
    ctx.record_check("figure7_theta_grid", {r["theta"] for r in theta_rows} == {1.5, 2.0, 3.0})
    fig, axes = plt.subplots(2, 2, figsize=(8.8, 6.2), sharex=True)
    for ri, workload in enumerate(["conversation", "toolagent"]):
        subset = sorted([r for r in theta_rows if r["workload"] == workload], key=lambda r: r["theta"])
        for ci, (metric, divisor, ylabel) in enumerate(
            [("saved_tokens", 1e6, "Saved Prefill Tokens (M)"), ("skew_ratio", 1, "Skew ratio")]
        ):
            ax = axes[ri, ci]
            ax.plot([r["theta"] for r in subset], [r[metric] / divisor for r in subset], marker="o", color=PALETTE["R_REQ_KV"], linewidth=1.8)
            ax.set_xticks([1.5, 2, 3])
            ax.set_xlabel("Reactive gate θ")
            ax.set_ylabel(ylabel)
            ax.set_title(WORKLOAD_TITLES[workload])
            style_axis(ax)
            panel_label(ax, f"({chr(97 + ri * 2 + ci)})")
    fig.suptitle("R_REQ_KV robustness to reactive gate θ", y=1.01, fontsize=12)
    fig.tight_layout()
    ctx.add(
        figure_id="fig07a_theta_sensitivity", title="Reactive gate sensitivity",
        script="scripts/task_main/final_figures/fig07_robustness.py",
        input_tables=["theta_sensitivity_final.csv"], rows=theta_rows, fieldnames=list(theta_rows[0]), fig=fig,
        cohort="MAIN_25_45", notes="Only R_REQ_KV is shown.",
    )

    n_strategy_labels = {
        "R_AFF / NONE": "R_AFF",
        "R_REQ_KV_TASK / NONE": "R_REQ_KV",
        "R_AFF / FUTURE_DEMAND": "Oracle",
        "R_AFF / PERSISTENCE": "Persistence-300s",
        "R_AFF / RECENCY": "Recency",
    }
    n_rows = []
    for row in read_csv("n_sensitivity_final.csv"):
        n_rows.append(
            {
                "workload": row["workload"], "N": integer(row["N"]),
                "capacity_pages_per_pod": 585, "total_cluster_capacity_pages": integer(row["total_cluster_capacity_pages"]),
                "strategy": n_strategy_labels[row["strategy"]], "saved_tokens": number(row["Saved"]),
                "skew_ratio": number(row["Skew"]), "eval_wire_bytes": number(row["Wire"]),
                "evaluation_label": row["evaluation_label"], "warning": row["warning"],
                "canonical_case_id": row["canonical_case_id"],
            }
        )
    ctx.record_check("figure7_n_grid", {r["N"] for r in n_rows} == {2, 4})
    ctx.record_check("figure7_per_pod_capacity_fixed", {r["capacity_pages_per_pod"] for r in n_rows} == {585})
    strategies = ["R_AFF", "R_REQ_KV", "Oracle", "Persistence-300s", "Recency"]
    x = np.arange(len(strategies))
    width = 0.36
    fig, axes = plt.subplots(2, 2, figsize=(10.5, 6.8), sharex=True)
    for ri, workload in enumerate(["conversation", "toolagent"]):
        for ci, (metric, divisor, ylabel) in enumerate(
            [("saved_tokens", 1e6, "Saved Prefill Tokens (M)"), ("skew_ratio", 1, "Skew ratio")]
        ):
            ax = axes[ri, ci]
            for ni, n in enumerate([2, 4]):
                lookup = {r["strategy"]: r for r in n_rows if r["workload"] == workload and r["N"] == n}
                vals = [lookup[s][metric] / divisor for s in strategies]
                ax.bar(x + (ni - 0.5) * width, vals, width, label=f"N={n}", color="#9bb7d4" if n == 2 else "#315f8c", zorder=2)
            ax.set_xticks(x, [s.replace("Persistence-300s", "P-300s") for s in strategies], rotation=24, ha="right")
            ax.set_ylabel(ylabel)
            ax.set_title(WORKLOAD_TITLES[workload])
            style_axis(ax)
            panel_label(ax, f"({chr(97 + ri * 2 + ci)})")
    axes[0, 0].legend(ncol=2)
    fig.text(0.5, 0.005, "Per-Pod capacity is fixed at 585 pages; changing N also changes total cluster cache capacity.", ha="center", fontsize=9, weight="bold")
    fig.suptitle("Robustness to cluster size and total cache capacity", y=0.995, fontsize=12)
    fig.tight_layout(rect=(0, 0.04, 1, 0.97))
    ctx.add(
        figure_id="fig07b_n_sensitivity", title="Cluster-size sensitivity",
        script="scripts/task_main/final_figures/fig07_robustness.py",
        input_tables=["n_sensitivity_final.csv"], rows=n_rows, fieldnames=list(n_rows[0]), fig=fig,
        cohort="MAIN_25_45", notes="Per-Pod capacity is fixed; N changes total cache capacity.",
    )


def build_fig08(ctx: FigureContext) -> None:
    raw_rows = read_csv("copy_vs_move.csv")
    raw_lookup = {(r["workload"], r["trigger"], r["action"]): r for r in raw_rows}
    delta_rows = []
    for row in read_csv("copy_vs_move_delta.csv"):
        trigger = "Oracle" if row["trigger"] == "FUTURE_DEMAND" else "Persistence-300s"
        copy = raw_lookup[(row["workload"], row["trigger"], "COPY")]
        move = raw_lookup[(row["workload"], row["trigger"], "MOVE")]
        copy_saved = number(copy["saved_tokens"]); move_saved = number(move["saved_tokens"])
        copy_evictions = number(copy["ordinary_lru_evictions_evaluation"])
        move_evictions = number(move["ordinary_lru_evictions_evaluation"])
        delta_saved_pct = (move_saved - copy_saved) / copy_saved * 100
        delta_evictions_pct = (move_evictions - copy_evictions) / copy_evictions * 100
        delta_skew = number(move["skew_ratio"]) - number(copy["skew_ratio"])
        assert_close(delta_saved_pct, number(row["delta_saved_pct"]))
        assert_close(delta_evictions_pct, number(row["delta_evictions_pct"]))
        assert_close(delta_skew, number(row["delta_skew"]))
        delta_rows.append(
            {
                "workload": row["workload"], "trigger": trigger,
                "group_label": f"{WORKLOAD_TITLES[row['workload']]}\n{'Oracle' if trigger == 'Oracle' else 'P300'}",
                "copy_saved_tokens": copy_saved, "move_saved_tokens": move_saved,
                "delta_saved_pct": delta_saved_pct,
                "copy_skew": number(copy["skew_ratio"]), "move_skew": number(move["skew_ratio"]),
                "delta_skew": delta_skew,
                "copy_ordinary_evictions": copy_evictions, "move_ordinary_evictions": move_evictions,
                "delta_evictions_pct": delta_evictions_pct,
                "mean_source_release_pct": number(move["release_fraction_mean"]) * 100,
                "zero_release_actions": integer(move["zero_release_actions"]),
                "partial_release_actions": integer(move["partial_release_actions"]),
                "full_release_actions": integer(move["full_release_actions"]),
                "comparison_status": row["comparison_status"],
            }
        )
    ctx.record_check("figure8_copy_move_pairing", len(delta_rows) == 4 and all(r["comparison_status"] == "SAME_COHORT_COMPARABLE" for r in delta_rows))
    x = np.arange(4)
    labels = [r["group_label"] for r in delta_rows]
    metrics = [
        ("delta_saved_pct", "Δ Saved: MOVE − COPY (%)", True),
        ("delta_skew", "Δ skew: MOVE − COPY", True),
        ("delta_evictions_pct", "Δ ordinary LRU eviction (%)", True),
        ("mean_source_release_pct", "Mean source release fraction (%)", False),
    ]
    fig, axes = plt.subplots(2, 2, figsize=(10.3, 6.8))
    for ax, (metric, ylabel, zero), letter in zip(axes.flat, metrics, ["(a)", "(b)", "(c)", "(d)"]):
        vals = [r[metric] for r in delta_rows]
        bars = ax.bar(x, vals, color=PALETTE["MOVE"], edgecolor="#34536b", linewidth=0.5, zorder=2)
        ax.set_xticks(x, labels, fontsize=8)
        ax.set_ylabel(ylabel)
        style_axis(ax, zero_line=zero)
        panel_label(ax, letter)
        if metric in {"delta_saved_pct", "delta_evictions_pct"}:
            for bar, val in zip(bars, vals):
                ax.text(bar.get_x() + bar.get_width() / 2, val, f"{val:+.1f}%", ha="center", va="bottom" if val >= 0 else "top", fontsize=8)
    axes[1, 0].text(0.02, 0.05, "Lower is fewer ordinary evictions", transform=axes[1, 0].transAxes, fontsize=8)
    fig.suptitle("COPY vs MOVE: behavioral deltas", y=1.01, fontsize=12)
    fig.tight_layout()
    ctx.add(
        figure_id="fig08_copy_vs_move", title="COPY versus MOVE deltas",
        script="scripts/task_main/final_figures/fig08_move.py",
        input_tables=["copy_vs_move.csv", "copy_vs_move_delta.csv"], rows=delta_rows, fieldnames=list(delta_rows[0]), fig=fig,
        cohort="MAIN_25_45", notes="All deltas are MOVE minus its paired COPY case.",
    )
    ctx.record_check("figure8_percentages_recomputed_from_raw_pairs", True)

    composition_rows = []
    for row in delta_rows:
        total = row["zero_release_actions"] + row["partial_release_actions"] + row["full_release_actions"]
        composition_rows.append(
            {
                **row, "move_actions": total,
                "zero_release_pct": row["zero_release_actions"] / total * 100,
                "partial_release_pct": row["partial_release_actions"] / total * 100,
                "full_release_pct": row["full_release_actions"] / total * 100,
            }
        )
    fig, ax = plt.subplots(figsize=(8.8, 4.2))
    bottom = np.zeros(4)
    for key, label, color in [
        ("zero_release_pct", "ZERO", "#bdbdbd"),
        ("partial_release_pct", "PARTIAL", PALETTE["MOVE"]),
        ("full_release_pct", "FULL", "#2a7f62"),
    ]:
        vals = np.array([r[key] for r in composition_rows])
        ax.bar(x, vals, bottom=bottom, label=label, color=color, edgecolor="white", linewidth=0.5)
        bottom += vals
    ax.set_xticks(x, labels)
    ax.set_ylabel("MOVE actions (%)")
    ax.set_ylim(0, 100)
    ax.legend(ncol=3, loc="upper center")
    style_axis(ax)
    fig.suptitle("MOVE source-release composition", y=1.01, fontsize=12)
    fig.tight_layout()
    ctx.add(
        figure_id="fig08b_move_release_composition", title="MOVE release composition",
        script="scripts/task_main/final_figures/fig08_move.py",
        input_tables=["copy_vs_move.csv", "copy_vs_move_delta.csv"], rows=composition_rows, fieldnames=list(composition_rows[0]), fig=fig,
        cohort="MAIN_25_45", notes="ZERO/PARTIAL/FULL percentages use MOVE actions as the denominator.",
    )


def build_fig09(ctx: FigureContext) -> None:
    selected_lines = {3: "Oracle", 5: "Persistence-300s", 6: "Recency"}
    rows = []
    for row in read_csv("diagnostics_summary.csv"):
        line = integer(row["line"])
        if line not in selected_lines:
            continue
        full_unused = number(row["full_chain_unused_ratio"])
        rows.append(
            {
                "workload": row["workload"], "line": line, "strategy": selected_lines[line],
                "full_chain_used_pct": (1 - full_unused) * 100,
                "any_partial_prefix_reuse_pct": number(row["partial_reuse_any_ratio"]) * 100,
                "new_page_observed_use_pct": number(row["new_page_observed_use_ratio"]) * 100,
                "wire_redundancy_pct": number(row["wire_redundancy_ratio"]) * 100,
                "full_chain_unused_pct": full_unused * 100,
                "unused_transfer_wire_pct": number(row["unused_transfer_wire_ratio"]) * 100,
            }
        )
    metrics = [
        ("full_chain_used_pct", "Full-chain used"),
        ("any_partial_prefix_reuse_pct", "Any prefix reuse"),
        ("new_page_observed_use_pct", "New-page use"),
        ("wire_redundancy_pct", "Wire redundancy"),
    ]
    strategies = ["Oracle", "Persistence-300s", "Recency"]
    x = np.arange(len(metrics))
    width = 0.25
    fig, axes = plt.subplots(1, 2, figsize=(11.2, 4.4), sharey=True)
    for ax, workload, letter in zip(axes, ["conversation", "toolagent"], ["(a)", "(b)"]):
        lookup = {r["strategy"]: r for r in rows if r["workload"] == workload}
        for si, strategy in enumerate(strategies):
            vals = [lookup[strategy][key] for key, _ in metrics]
            ax.bar(x + (si - 1) * width, vals, width, color=PALETTE[strategy], label=strategy, zorder=2)
        ax.set_xticks(x, [label for _, label in metrics], rotation=20, ha="right")
        ax.set_ylabel("Rate (%)")
        ax.set_title(WORKLOAD_TITLES[workload])
        style_axis(ax)
        panel_label(ax, letter)
    axes[0].legend(fontsize=8)
    fig.suptitle("Why high formal waste does not mean zero partial reuse", y=1.01, fontsize=12)
    fig.tight_layout()
    ctx.add(
        figure_id="fig09_copy_efficiency", title="Copy-efficiency diagnostics",
        script="scripts/task_main/final_figures/fig09_efficiency.py",
        input_tables=["diagnostics_summary.csv"], rows=rows, fieldnames=list(rows[0]), fig=fig,
        cohort="MAIN_25_45",
        notes="Metrics have different denominators and are grouped rather than stacked; see README and audit report.",
    )


def build_appendices(ctx: FigureContext) -> None:
    persistence = _persistence_rows()
    _plot_persistence(ctx, "figA1_persistence_chain_depth", "chain_depth_mean", "Mean copied-chain depth (pages)", "Appendix: Persistence chain depth", 1)

    recency = []
    for row in read_csv("recency_sensitivity_final.csv"):
        recency.append(
            {
                "workload": row["workload"], "q": number(row["q"]),
                "positive_candidates": integer(row["positive_candidates"]),
                "shortlist_count": integer(row["shortlist_count"]), "actions": integer(row["actions"]),
                "evaluation_label": row["evaluation_label"], "canonical_case_id": row["canonical_case_id"],
            }
        )
    fig, axes = plt.subplots(1, 2, figsize=(9.4, 3.8))
    for ax, workload, letter in zip(axes, ["conversation", "toolagent"], ["(a)", "(b)"]):
        subset = sorted([r for r in recency if r["workload"] == workload], key=lambda r: r["q"])
        x = np.arange(len(subset)); labels = [f"q={r['q']:.1f}" for r in subset]
        shortlist = [r["shortlist_count"] for r in subset]; actions = [r["actions"] for r in subset]
        ax.bar(x - 0.18, shortlist, 0.36, label="Shortlist entries", color="#a99ac5")
        ax.bar(x + 0.18, actions, 0.36, label="Actions", color=PALETTE["Recency"])
        ax.set_xticks(x, labels); ax.set_ylabel("Count"); ax.set_title(WORKLOAD_TITLES[workload]); style_axis(ax); panel_label(ax, letter)
    axes[0].legend()
    fig.suptitle("Appendix: Recency shortlist changes without action changes", y=1.01, fontsize=12)
    fig.tight_layout()
    ctx.add(
        figure_id="figA2_recency_shortlist_actions", title="Recency shortlist and actions",
        script="scripts/task_main/final_figures/fig07_robustness.py",
        input_tables=["recency_sensitivity_final.csv"], rows=recency, fieldnames=list(recency[0]), fig=fig,
        cohort="MAIN_25_45", notes="q changes shortlist size while action count is unchanged.",
    )


ALL_BUILDERS = [build_fig01, build_fig02, build_fig03, build_fig04, build_fig05, build_fig06, build_fig07, build_fig08, build_fig09, build_appendices]
