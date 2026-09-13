#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
log_root="$repo_root/logs/full_o2a/toolagent"
mkdir -p "$log_root" "$repo_root/checkpoints/full_o2a/toolagent"
args=(
  --config "$repo_root/configs/o2_oracle/full_o2a_toolagent.yaml"
  --trace "$repo_root/data/mooncake/toolagent_trace.jsonl"
  --output "$repo_root/results/full_o2a/toolagent"
  --checkpoint-root "$repo_root/checkpoints/full_o2a/toolagent"
)
if [[ -n "${RESUME_CHECKPOINT:-}" ]]; then
  args+=(--resume "$RESUME_CHECKPOINT")
fi
cd "$repo_root"
PYTHONPATH="$repo_root${PYTHONPATH:+:$PYTHONPATH}" \
  python -m src.simulator.full_o2a "${args[@]}" \
  2>&1 | tee -a "$log_root/replay.log"
