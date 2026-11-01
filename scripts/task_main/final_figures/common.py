from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


REPO_ROOT = Path(__file__).resolve().parents[3]
CANONICAL_ROOT = REPO_ROOT / "results" / "task_main" / "final_results"
OUTPUT_ROOT = REPO_ROOT / "results" / "task_main" / "final_figures"

PALETTE = {
    "R_AFF": "#7f7f7f",
    "R_LEAST": "#607d8b",
    "R_REQ_KV": "#1f4e79",
    "Oracle": "#e68613",
    "Persistence-60s": "#c44e52",
    "Persistence-300s": "#2a9d55",
    "Recency": "#7b61a8",
    "Persistence+Cost": "#69ad72",
    "COPY": "#1f77b4",
    "MOVE": "#8fb7d8",
}

WORKLOAD_COLORS = {"conversation": "#3568a8", "toolagent": "#dd7c2b"}
WORKLOAD_TITLES = {"conversation": "Conversation", "toolagent": "ToolAgent"}

STRATEGY_LABELS = {
    "R_AFF": "R_AFF",
    "R_LEAST": "R_LEAST",
    "R_REQ_KV": "R_REQ_KV",
    "Future-Demand Oracle": "Oracle",
    "Persistence h=60s K=10": "Persistence-60s",
    "Persistence h=300s K=10": "Persistence-300s",
    "Recency q=0.9": "Recency",
    "Persistence + Cost-aware": "Persistence+Cost",
    "PERSISTENCE": "Persistence-300s",
    "FUTURE_DEMAND": "Oracle",
    "RECENCY": "Recency",
}

MAIN_ORDER = [
    "R_AFF",
    "R_REQ_KV",
    "Oracle",
    "Persistence-60s",
    "Persistence-300s",
    "Recency",
    "Persistence+Cost",
]

BUCKET_LABELS = {
    "B1": "<5k",
    "B2": "5–20k",
    "B3": "20–60k",
    "B4": "60–120k",
    "B5": "120–300k",
    "B6": ">300k",
}


def configure_style() -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 9,
            "axes.titlesize": 10,
            "axes.labelsize": 9,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": False,
            "legend.frameon": False,
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "savefig.facecolor": "white",
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "svg.fonttype": "none",
        }
    )


def read_csv(name: str) -> list[dict[str, str]]:
    path = CANONICAL_ROOT / name
    if path.parent != CANONICAL_ROOT or not path.is_file():
        raise FileNotFoundError(f"canonical input missing: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def number(value: Any) -> float:
    if value is None:
        return math.nan
    text = str(value).strip()
    if not text or text.upper() == "NA":
        return math.nan
    return float(text)


def integer(value: Any) -> int:
    result = number(value)
    if math.isnan(result):
        raise ValueError(f"expected integer, got {value!r}")
    return int(result)


def finite(value: float) -> bool:
    return not math.isnan(value) and math.isfinite(value)


def strategy_label(value: str) -> str:
    return STRATEGY_LABELS.get(value, value)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_csv(path: Path, rows: Sequence[dict[str, Any]], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            encoded = {}
            for key in fieldnames:
                value = row.get(key)
                if value is None or (isinstance(value, float) and math.isnan(value)):
                    encoded[key] = "NA"
                else:
                    encoded[key] = value
            writer.writerow(encoded)


def style_axis(ax: Any, *, zero_line: bool = False) -> None:
    ax.grid(axis="y", color="#d9d9d9", linewidth=0.6, alpha=0.8, zorder=0)
    ax.set_axisbelow(True)
    if zero_line:
        ax.axhline(0, color="#555555", linewidth=0.8, zorder=1)


def panel_label(ax: Any, label: str) -> None:
    ax.text(-0.12, 1.04, label, transform=ax.transAxes, weight="bold", va="bottom")


def format_signed_pct(value: float, digits: int = 1) -> str:
    return f"{value:+.{digits}f}%"


@dataclass
class FigureContext:
    output_root: Path = OUTPUT_ROOT
    figures: list[dict[str, Any]] = field(default_factory=list)
    validation_checks: dict[str, bool] = field(default_factory=dict)

    def prepare(self) -> None:
        self.output_root.mkdir(parents=True, exist_ok=True)
        for child in ["png", "pdf", "svg", "figure_data"]:
            (self.output_root / child).mkdir(parents=True, exist_ok=True)

    def add(
        self,
        *,
        figure_id: str,
        title: str,
        script: str,
        input_tables: Sequence[str],
        rows: Sequence[dict[str, Any]],
        fieldnames: Sequence[str],
        fig: Any,
        cohort: str,
        notes: str = "",
    ) -> None:
        data_path = self.output_root / "figure_data" / f"{figure_id}.csv"
        write_csv(data_path, rows, fieldnames)
        outputs: dict[str, str] = {}
        output_hashes: dict[str, str] = {}
        for extension in ["png", "pdf", "svg"]:
            path = self.output_root / extension / f"{figure_id}.{extension}"
            metadata = {"Creator": "TaskMain final figures"}
            if extension == "svg":
                metadata["Date"] = None
            if extension == "pdf":
                metadata["CreationDate"] = None
                metadata["ModDate"] = None
            fig.savefig(
                path,
                dpi=320 if extension == "png" else None,
                bbox_inches="tight",
                metadata=metadata,
            )
            outputs[extension] = str(path.relative_to(REPO_ROOT))
            output_hashes[f"{extension}_sha256"] = sha256(path)
        plt.close(fig)
        input_hashes = {name: sha256(CANONICAL_ROOT / name) for name in input_tables}
        self.figures.append(
            {
                "figure_id": figure_id,
                "title": title,
                "script": script,
                "input_tables": list(input_tables),
                "input_sha256": input_hashes,
                "figure_data_csv": str(data_path.relative_to(REPO_ROOT)),
                "figure_data_sha256": sha256(data_path),
                **outputs,
                **output_hashes,
                "cohort": cohort,
                "notes": notes,
            }
        )

    def record_check(self, name: str, value: bool) -> None:
        self.validation_checks[name] = bool(value)
        if not value:
            raise AssertionError(f"figure validation failed: {name}")

    def finalize(self) -> None:
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True, capture_output=True, check=True
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "status", "--porcelain"], cwd=REPO_ROOT, text=True, capture_output=True, check=True
            ).stdout.strip()
        )
        generated = datetime.now(timezone.utc).isoformat()
        manifest = {
            "status": "FINAL_FIGURES_PASS",
            "generation_timestamp": generated,
            "canonical_root": str(CANONICAL_ROOT.relative_to(REPO_ROOT)),
            "git_source_state": {"head": head, "dirty": dirty},
            "strategy_palette": PALETTE,
            "figures": self.figures,
        }
        manifest_path = self.output_root / "figure_manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        validation = {
            "status": "FINAL_FIGURES_PASS",
            "checks": self.validation_checks,
            "figure_count": len(self.figures),
            "all_three_formats": all(
                all((REPO_ROOT / item[key]).is_file() for key in ["png", "pdf", "svg"])
                for item in self.figures
            ),
            "generation_timestamp": generated,
        }
        (self.output_root / "figure_validation.json").write_text(
            json.dumps(validation, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )


def assert_close(actual: float, expected: float, *, tolerance: float = 1e-9) -> None:
    if not math.isclose(actual, expected, rel_tol=tolerance, abs_tol=tolerance):
        raise AssertionError(f"values differ: actual={actual}, expected={expected}")


def canonical_gate() -> dict[str, Any]:
    validation_path = CANONICAL_ROOT / "consolidation_validation.json"
    data = json.loads(validation_path.read_text(encoding="utf-8"))
    if data.get("status") != "PASS" or data.get("final_state") != "FINAL_RESULTS_CONSOLIDATION_PASS":
        raise RuntimeError("canonical consolidation is not PASS")
    return data

