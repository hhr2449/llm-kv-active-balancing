#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
trace="$repo_root/data/mooncake/conversation_trace.jsonl"
config_root="$repo_root/configs/taskmain_v1.1/conversation"
result_root="$repo_root/results/taskmain_evaluation/conversation"
batch_log="$result_root/batch_run.log"

experiments=(
  "E01_r_aff|R_AFF"
  "E02_r_req_kv_task|R_REQ_KV_TASK"
  "E03_oracle|TaskMain-Oracle"
  "E04_persist_h1_k10|Persist h=60s K=10"
  "E05_persist_h5_k10|Persist h=300s K=10"
  "E06_recency_q09|Recency q=0.9 decay=60s"
  "E07_cost_aware|Cost-aware"
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
      echo "Stop. Set ALLOW_OVERWRITE=1 to rerun the entire seven-row batch." >&2
      exit 3
    fi
  done
fi

: >"$batch_log"
for entry in "${experiments[@]}"; do
  IFS='|' read -r experiment_id policy <<<"$entry"
  config="$config_root/$experiment_id.yaml"
  output="$result_root/$experiment_id"
  run_log="$output/run.log"
  mkdir -p "$output"
  {
    echo "START_TIME=$(date --iso-8601=seconds)"
    echo "EXPERIMENT_ID=$experiment_id"
    echo "POLICY=$policy"
  } | tee -a "$batch_log"

  set +e
  python -m src.simulator.engine --trace "$trace" --config "$config" --output "$output" \
    2>&1 | tee "$run_log" | tee -a "$batch_log"
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
python "$repo_root/scripts/taskmain_v1.1/summarize_conversation_main.py" \
  --result-root "$result_root" 2>&1 | tee -a "$batch_log"
summary_exit=${PIPESTATUS[0]}
set -e
if [[ "$summary_exit" -ne 0 ]]; then
  echo "FAILED_EXPERIMENT=SUMMARY" | tee -a "$batch_log" >&2
  exit "$summary_exit"
fi
