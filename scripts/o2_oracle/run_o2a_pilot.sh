#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
o2p_root="$repo_root/results/o2_oracle/O2P_pilot"
output="$repo_root/results/o2_oracle/O2A_pilot"

for required in o2p_state_summary.csv o2p_branch_results.csv; do
  if [[ ! -s "$o2p_root/$required" ]]; then
    echo "Missing required O2-P input: $o2p_root/$required" >&2
    exit 1
  fi
done
if [[ -e "$output" ]]; then
  echo "Refusing to overwrite existing O2-A output: $output" >&2
  exit 1
fi

cd "$repo_root"
PYTHONPATH="$repo_root${PYTHONPATH:+:$PYTHONPATH}" \
  python -m src.simulator.o2a --o2p-root "$o2p_root" --output "$output"

echo "O2-A Pilot completed: $output/o2a_pilot_report.md"
