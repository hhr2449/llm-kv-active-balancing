#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
output="$repo_root/results/o2a_smoke_validation"

if [[ -e "$output" ]]; then
  echo "Refusing to overwrite existing smoke output: $output" >&2
  exit 1
fi

cd "$repo_root"
PYTHONPATH="$repo_root${PYTHONPATH:+:$PYTHONPATH}" python -m src.simulator.o2a_smoke \
  --config configs/o2_oracle/o2a_closed_loop_smoke.yaml \
  --trace data/mooncake/toolagent_trace.jsonl \
  --output "$output"
