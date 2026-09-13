#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
config_root="$repo_root/configs/taskmain_v1.1/matched"
result_root="$repo_root/results/taskmain_evaluation/matched"
batch_log="$result_root/batch_run.log"
experiments=(
  "M01_conversation_r_req_kv_v1|Conversation|conversation_trace.jsonl|R_REQ_KV_V1"
  "M02_conversation_matched_o1|Conversation|conversation_trace.jsonl|Matched-O1"
  "M03_toolagent_r_req_kv_v1|ToolAgent|toolagent_trace.jsonl|R_REQ_KV_V1"
  "M04_toolagent_matched_o1|ToolAgent|toolagent_trace.jsonl|Matched-O1"
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
      echo "Stop. Set ALLOW_OVERWRITE=1 to rerun the entire matched batch." >&2
      exit 3
    fi
  done
fi

: >"$batch_log"
for entry in "${experiments[@]}"; do
  IFS='|' read -r experiment_id workload trace_name policy <<<"$entry"
  config="$config_root/$experiment_id.yaml"
  trace="$repo_root/data/mooncake/$trace_name"
  output="$result_root/$experiment_id"
  run_log="$output/run.log"
  mkdir -p "$output"
  {
    echo "START_TIME=$(date --iso-8601=seconds)"
    echo "EXPERIMENT_ID=$experiment_id"
    echo "WORKLOAD=$workload"
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
python "$repo_root/scripts/taskmain_v1.1/summarize_matched_main.py" \
  --result-root "$result_root" 2>&1 | tee -a "$batch_log"
summary_exit=${PIPESTATUS[0]}
set -e
if [[ "$summary_exit" -ne 0 ]]; then
  echo "FAILED_EXPERIMENT=SUMMARY" | tee -a "$batch_log" >&2
  exit "$summary_exit"
fi
