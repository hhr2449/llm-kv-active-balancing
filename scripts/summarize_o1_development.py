#!/usr/bin/env python3
import glob
import json
from pathlib import Path


def main():
    groups = []
    for workload in ("conversation", "toolagent"):
        for capacity in (10000, 5000):
            baseline = json.loads(Path(
                f"results/o1_development/baseline/{workload}/{capacity}/summary.json"
            ).read_text())
            runs = [json.loads(Path(path).read_text()) for path in glob.glob(
                f"results/o1_development/mapping/{workload}/{capacity}/h*.json"
            )]
            for run in runs:
                run["completion_delta_percent"] = 100 * (
                    run["completion_latency_ms"]["mean"]
                    / baseline["completion_latency_ms"]["mean"] - 1
                )
                run["token_hit_delta_percentage_points"] = 100 * (
                    run["token_hit_rate"] - baseline["cluster"]["token_hit_rate"]
                )
            pareto = [run for run in runs if not any(
                other["completion_latency_ms"]["mean"] <= run["completion_latency_ms"]["mean"]
                and other["proactive"]["wire_bytes"] <= run["proactive"]["wire_bytes"]
                and (other["completion_latency_ms"]["mean"] < run["completion_latency_ms"]["mean"]
                     or other["proactive"]["wire_bytes"] < run["proactive"]["wire_bytes"])
                for other in runs
            )]
            groups.append({
                "workload": workload, "capacity_pages": capacity,
                "baseline": {
                    "completion_latency_ms": baseline["completion_latency_ms"]["mean"],
                    "queue_time_ms": baseline["queue_time_ms"]["mean"],
                    "service_time_ms": baseline["service_time_ms"]["mean"],
                    "token_hit_rate": baseline["cluster"]["token_hit_rate"],
                },
                "best_latency_run": min(runs, key=lambda r: r["completion_latency_ms"]["mean"]),
                "worst_latency_run": max(runs, key=lambda r: r["completion_latency_ms"]["mean"]),
                "pareto_runs": sorted(pareto, key=lambda r: r["proactive"]["wire_bytes"]),
                "run_count": len(runs),
            })
    output = Path("results/o1_development/headroom_summary.json")
    output.write_text(json.dumps({"metric_scope": "DEVELOPMENT", "groups": groups},
                                 indent=2, sort_keys=True) + "\n")
    lines = ["# O1 Development-only headroom", "",
             "No O1 Evaluation metrics were read or emitted.", "",
             "| workload | capacity | best completion delta | best H/P/phase | Pareto points |",
             "|---|---:|---:|---|---:|"]
    for group in groups:
        best = group["best_latency_run"]
        lines.append(
            f"| {group['workload']} | {group['capacity_pages']} | "
            f"{best['completion_delta_percent']:.3f}% | "
            f"{best['horizon_ms']}/{best['period_ms']}/{best['phase_ms']} | "
            f"{len(group['pareto_runs'])} |"
        )
    Path("results/o1_development/headroom_summary.md").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
