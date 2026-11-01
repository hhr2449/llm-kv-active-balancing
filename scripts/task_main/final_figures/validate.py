from __future__ import annotations

import csv
import json
import math
from pathlib import Path

from .common import CANONICAL_ROOT, OUTPUT_ROOT, REPO_ROOT, read_csv, sha256


def load_rows(name: str) -> list[dict[str, str]]:
    with (OUTPUT_ROOT / "figure_data" / name).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def f(value: str) -> float:
    if value.upper() == "NA":
        return math.nan
    return float(value)


def close(a: float, b: float) -> bool:
    return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9)


def main() -> None:
    checks: dict[str, bool] = {}
    canonical_validation = json.loads((CANONICAL_ROOT / "consolidation_validation.json").read_text())
    consolidation_manifest = json.loads((CANONICAL_ROOT / "consolidation_manifest.json").read_text())
    checks["consolidation_pass"] = (
        canonical_validation.get("status") == "PASS"
        and canonical_validation.get("final_state") == "FINAL_RESULTS_CONSOLIDATION_PASS"
    )
    manifest = json.loads((OUTPUT_ROOT / "figure_manifest.json").read_text())
    checks["manifest_pass"] = manifest.get("status") == "FINAL_FIGURES_PASS"
    checks["figure_count_16"] = len(manifest.get("figures", [])) == 16
    used_inputs = {table for item in manifest["figures"] for table in item["input_tables"]}
    frozen_hashes = consolidation_manifest["artifact_sha256"]
    checks["canonical_inputs_match_frozen_manifest"] = all(
        table in frozen_hashes and sha256(CANONICAL_ROOT / table) == frozen_hashes[table]
        for table in used_inputs
    )

    files_ok = True
    hashes_ok = True
    sources_ok = True
    for item in manifest["figures"]:
        for table in item["input_tables"]:
            path = CANONICAL_ROOT / table
            sources_ok &= path.is_file() and path.parent == CANONICAL_ROOT
            hashes_ok &= sha256(path) == item["input_sha256"][table]
        for key in ["png", "pdf", "svg"]:
            path = REPO_ROOT / item[key]
            files_ok &= path.is_file() and path.stat().st_size > 0
            hashes_ok &= sha256(path) == item[f"{key}_sha256"]
        data_path = REPO_ROOT / item["figure_data_csv"]
        files_ok &= data_path.is_file() and data_path.stat().st_size > 0
        hashes_ok &= sha256(data_path) == item["figure_data_sha256"]
    checks["all_inputs_canonical_root"] = sources_ok
    checks["all_artifacts_exist"] = files_ok
    checks["all_manifest_hashes_match"] = hashes_ok

    long_rows = read_csv("final_results_long.csv")
    forbidden = ("PILOT", "SMOKE", "INVALID", "SUPERSEDED")
    checks["no_forbidden_sources"] = all(
        not any(token in (r["source_kind"] + r["source_run_id"]).upper() for token in forbidden)
        for r in long_rows
    )

    fig2 = load_rows("fig02_main_7line.csv")
    checks["na_preserved_for_lines_1_2"] = all(
        r["unused_wire_pct"] == "NA" for r in fig2 if int(r["line"]) in {1, 2}
    )
    checks["fig2_saved_pct_recomputes"] = all(
        close(
            f(r["delta_saved_pct"]),
            (f(r["saved_million"]) - f(r["line2_saved_million"])) / f(r["line2_saved_million"]) * 100,
        )
        for r in fig2
    )
    checks["fig2_waste_pct_recomputes"] = all(
        (r["unused_transfer_wire_ratio_raw"] == "NA" and r["unused_wire_pct"] == "NA")
        or close(f(r["unused_wire_pct"]), f(r["unused_transfer_wire_ratio_raw"]) * 100)
        for r in fig2
    )

    fig3 = load_rows("fig03_oracle_headroom.csv")
    checks["fig3_oracle_delta_recomputes"] = all(
        close(f(r["delta_saved_pct"]), (f(r["oracle_saved"]) - f(r["baseline_saved"])) / f(r["baseline_saved"]) * 100)
        and close(f(r["delta_weighted_pct"]), (f(r["oracle_weighted"]) - f(r["baseline_weighted"])) / f(r["baseline_weighted"]) * 100)
        for r in fig3
    )
    checks["fig3_expected_oracle_delta"] = {
        r["workload"]: round(f(r["delta_saved_pct"]), 2) for r in fig3
    } == {"conversation": 56.82, "toolagent": 7.05}

    fig5 = load_rows("fig05_persistence_sensitivity.csv")
    checks["fig5_complete_grid"] = {
        (r["workload"], f(r["h_seconds"]), int(r["K"])) for r in fig5
    } == {(w, h, k) for w in ["conversation", "toolagent"] for h in [60.0, 300.0] for k in [5, 10, 20]}

    fig6 = load_rows("fig06_oracle_horizon.csv")
    checks["fig6_complete_grid"] = {f(r["W_minutes"]) for r in fig6} == {1.0, 5.0, 30.0}
    checks["fig6_single_common_cohort"] = {r["evaluation_label"] for r in fig6} == {"W_SENS_10_25"}

    fig7 = load_rows("fig07b_n_sensitivity.csv")
    checks["fig7_n_complete"] = {int(r["N"]) for r in fig7} == {2, 4}
    checks["fig7_per_pod_capacity_fixed"] = {int(r["capacity_pages_per_pod"]) for r in fig7} == {585}
    checks["fig7_total_capacity_changes"] = {int(r["total_cluster_capacity_pages"]) for r in fig7} == {1170, 2340}

    fig8 = load_rows("fig08_copy_vs_move.csv")
    checks["fig8_four_pairs"] = len(fig8) == 4 and {
        (r["workload"], r["trigger"]) for r in fig8
    } == {
        ("conversation", "Oracle"), ("conversation", "Persistence-300s"),
        ("toolagent", "Oracle"), ("toolagent", "Persistence-300s"),
    }
    checks["fig8_saved_pct_recomputes"] = all(
        close(f(r["delta_saved_pct"]), (f(r["move_saved_tokens"]) - f(r["copy_saved_tokens"])) / f(r["copy_saved_tokens"]) * 100)
        for r in fig8
    )
    checks["fig8_eviction_pct_recomputes"] = all(
        close(
            f(r["delta_evictions_pct"]),
            (f(r["move_ordinary_evictions"]) - f(r["copy_ordinary_evictions"])) / f(r["copy_ordinary_evictions"]) * 100,
        )
        for r in fig8
    )
    fig8b = load_rows("fig08b_move_release_composition.csv")
    checks["fig8_release_partition"] = all(
        int(r["move_actions"]) == int(r["zero_release_actions"]) + int(r["partial_release_actions"]) + int(r["full_release_actions"])
        and close(f(r["zero_release_pct"]) + f(r["partial_release_pct"]) + f(r["full_release_pct"]), 100.0)
        for r in fig8b
    )

    checks["all_checks_true"] = all(checks.values())
    result = {"status": "FINAL_FIGURES_PASS" if checks["all_checks_true"] else "BLOCKED", "checks": checks}
    (OUTPUT_ROOT / "independent_validation.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    if not checks["all_checks_true"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
