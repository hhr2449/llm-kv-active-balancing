"""Rebuild nine table families from a validated matrix without replaying traces."""
import argparse
import json
from pathlib import Path
from src.simulator.task_main.reporting import write_tables

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads((args.input/"validation_manifest.json").read_text())
    if manifest["status"] != "PASS" or len(manifest["cases"]) != 14:
        raise ValueError("cannot publish invalid or incomplete matrix")
    summaries = [json.loads((args.input/case["case"]/"run_1/summary.json").read_text()) for case in manifest["cases"]]
    write_tables(args.output, summaries)
