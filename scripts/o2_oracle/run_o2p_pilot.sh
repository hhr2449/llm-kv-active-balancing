#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
output="$repo_root/results/o2_oracle/O2P_pilot"

if [[ -e "$output" ]]; then
  echo "Refusing to overwrite existing O2-P output: $output" >&2
  exit 1
fi

cd "$repo_root"
PYTHONPATH="$repo_root${PYTHONPATH:+:$PYTHONPATH}" \
  python -m src.simulator.o2p --output "$output"

echo "O2-P Pilot completed: $output/o2p_pilot_report.md"
