from __future__ import annotations

import json
import math
import shutil
from pathlib import Path

from .common import CANONICAL_ROOT, OUTPUT_ROOT, FigureContext, canonical_gate, configure_style, read_csv
from .figures import ALL_BUILDERS


def validate_inputs(ctx: FigureContext) -> None:
    gate = canonical_gate()
    ctx.record_check("consolidation_validation_pass", gate["status"] == "PASS")
    ctx.record_check("consolidation_final_state_pass", gate["final_state"] == "FINAL_RESULTS_CONSOLIDATION_PASS")

    long_rows = read_csv("final_results_long.csv")
    ctx.record_check("canonical_rows_only_valid", all(r["validation_status"] == "PASS" for r in long_rows))
    forbidden = ("PILOT", "SMOKE", "INVALID", "SUPERSEDED")
    ctx.record_check(
        "no_pilot_smoke_invalid_superseded_sources",
        all(not any(token in (r["source_kind"] + " " + r["source_run_id"]).upper() for token in forbidden) for r in long_rows),
    )
    ctx.record_check("canonical_row_count_50", len(long_rows) == 50)


def write_readme(ctx: FigureContext) -> None:
    lines = [
        "# TaskMain Final Figures",
        "",
        "Status: **FINAL_FIGURES_PASS**.",
        "",
        "All figure data are derived only from the frozen canonical tables in `results/task_main/final_results/`. "
        "No simulator, pilot, smoke, INVALID, or superseded artifact is used. PNG files are rendered at 320 dpi; "
        "PDF and SVG are vector outputs.",
        "",
        "## Figures",
        "",
        "| Figure | Purpose | Cohort |",
        "|---|---|---|",
    ]
    for item in ctx.figures:
        lines.append(f"| `{item['figure_id']}` | {item['title']} | `{item['cohort']}` |")
    lines += [
        "",
        "## Reproduction",
        "",
        "```bash",
        "cd /root/data2/llm-kv-active-balancing",
        "python -m scripts.task_main.final_figures.build_all",
        "```",
        "",
        "Each plotted dataset is preserved in `figure_data/`. `figure_manifest.json` records the canonical input "
        "hashes and hashes of every PNG, PDF, SVG, and figure-data CSV. `figure_validation.json` records the "
        "automated audit checks.",
        "",
        "## Metric interpretation",
        "",
        "- Saved Prefill Tokens are actual longest-prefix hit tokens in the named Evaluation cohort.",
        "- WeightedSaved is a multiplier-weighted token-benefit proxy, not latency.",
        "- Skew is a random-normalized request-distribution Gini ratio, not a percentage.",
        "- Unused transfer wire requires absence of complete generation-matched chain reuse within W. It does not imply no partial-prefix reuse.",
        "- In Figure 9, full-chain use is action-weighted; any-prefix reuse is action-weighted; new-page use is page-weighted; wire redundancy is token-weighted. These rates must not be stacked or summed.",
        "- The Oracle-W figure uses only `W_SENS_10_25`. Its values must not be joined to Formal Main `[25,45)` results.",
        "- N sensitivity fixes 585 pages per Pod, so N also changes total cluster cache capacity.",
        "- Deterministic double replay validates reproducibility; it does not provide statistical replicates or confidence intervals.",
        "",
    ]
    (OUTPUT_ROOT / "README.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    canonical_gate()
    if OUTPUT_ROOT.exists():
        shutil.rmtree(OUTPUT_ROOT)
    configure_style()
    ctx = FigureContext()
    ctx.prepare()
    validate_inputs(ctx)
    for builder in ALL_BUILDERS:
        builder(ctx)

    ctx.record_check("na_not_coerced_to_zero", any(
        row["Unused_Wire_pct"].upper() == "NA" for row in read_csv("main_7line_combined.csv")
    ))
    ctx.record_check("all_figure_data_written", all((Path(ctx.output_root) / "figure_data" / f"{f['figure_id']}.csv").is_file() for f in ctx.figures))
    ctx.record_check("all_files_generated", all(
        (Path(ctx.output_root) / ext / f"{f['figure_id']}.{ext}").is_file()
        for f in ctx.figures for ext in ["png", "pdf", "svg"]
    ))
    write_readme(ctx)
    ctx.finalize()
    print(json.dumps({"status": "FINAL_FIGURES_PASS", "figure_count": len(ctx.figures), "output": str(OUTPUT_ROOT)}, indent=2))


if __name__ == "__main__":
    main()

