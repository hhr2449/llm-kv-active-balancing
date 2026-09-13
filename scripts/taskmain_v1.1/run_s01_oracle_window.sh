#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
config_root="$repo_root/configs/taskmain_v1.1/sweeps/oracle_window"
result_root="$repo_root/results/taskmain_sweeps/S01_oracle_window"
batch_log="$result_root/batch_run.log"
experiments=(
  "S01-C-W1|Conversation|conversation_trace.jsonl|conversation_w1.yaml"
  "S01-C-W5|Conversation|conversation_trace.jsonl|conversation_w5.yaml"
  "S01-C-W30|Conversation|conversation_trace.jsonl|conversation_w30.yaml"
  "S01-T-W1|ToolAgent|toolagent_trace.jsonl|toolagent_w1.yaml"
  "S01-T-W5|ToolAgent|toolagent_trace.jsonl|toolagent_w5.yaml"
  "S01-T-W30|ToolAgent|toolagent_trace.jsonl|toolagent_w30.yaml"
)

valid_summary() {
  python - "$1" <<'PY'
import json
import sys
from pathlib import Path
path = Path(sys.argv[1])
try:
    value = json.loads(path.read_text(encoding="utf-8"))
except (OSError, ValueError):
    raise SystemExit(1)
raise SystemExit(0 if isinstance(value, dict) and "request_count" in value else 1)
PY
}

mkdir -p "$result_root"
if [[ "${ALLOW_OVERWRITE:-0}" != "1" ]]; then
  for entry in "${experiments[@]}"; do
    IFS='|' read -r experiment_id _ <<<"$entry"
    summary="$result_root/$experiment_id/summary.json"
    if [[ -f "$summary" ]] && valid_summary "$summary"; then
      echo "Existing valid result: $summary" >&2
      echo "Stop. Set ALLOW_OVERWRITE=1 to rerun the entire S01 batch." >&2
      exit 3
    fi
  done
fi

: >"$batch_log"
for entry in "${experiments[@]}"; do
  IFS='|' read -r experiment_id workload trace_name config_name <<<"$entry"
  output="$result_root/$experiment_id"
  mkdir -p "$output"
  {
    echo "START_TIME=$(date --iso-8601=seconds)"
    echo "EXPERIMENT_ID=$experiment_id"
    echo "WORKLOAD=$workload"
  } | tee -a "$batch_log"
  set +e
  python -m src.simulator.engine \
    --trace "$repo_root/data/mooncake/$trace_name" \
    --config "$config_root/$config_name" --output "$output" \
    2>&1 | tee "$output/run.log" | tee -a "$batch_log"
  exit_code=${PIPESTATUS[0]}
  set -e
  {
    echo "END_TIME=$(date --iso-8601=seconds)"
    echo "EXIT_CODE=$exit_code"
  } | tee -a "$batch_log"
  if [[ "$exit_code" -ne 0 ]]; then
    echo "FAILED_EXPERIMENT=$experiment_id" | tee -a "$batch_log" >&2
    exit "$exit_code"
  fi
done

set +e
python "$repo_root/scripts/taskmain_v1.1/summarize_s01_oracle_window.py" \
  --result-root "$result_root" 2>&1 | tee -a "$batch_log"
summary_exit=${PIPESTATUS[0]}
set -e
if [[ "$summary_exit" -ne 0 ]]; then
  echo "FAILED_EXPERIMENT=SUMMARY" | tee -a "$batch_log" >&2
  exit "$summary_exit"
fi
