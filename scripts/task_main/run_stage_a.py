"""Explicit single-input Stage A runner; no batch, sweep, or automatic experiment."""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

from src.simulator.task_main.config import TaskMainConfig
from src.simulator.task_main.engine import TaskMainEngine
from src.simulator.task_main.metrics import write_outputs
from src.simulator.task_main.trace import read_trace


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--trace", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    config = TaskMainConfig.from_yaml(args.config)
    if args.output.exists():
        parser.error("output must be a new directory")
    result = TaskMainEngine(config).run(read_trace(args.trace))
    result.summary["provenance"]["inputs"] = {
        "trace_sha256": hashlib.sha256(args.trace.read_bytes()).hexdigest(),
        "config_sha256": hashlib.sha256(args.config.read_bytes()).hexdigest(),
    }
    write_outputs(args.output, result)


if __name__ == "__main__":
    main()
