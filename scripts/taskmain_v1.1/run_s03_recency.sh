#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
config_root="$repo_root/configs/taskmain_v1.1/sweeps/recency"
result_root="$repo_root/results/taskmain_sweeps/S03_recency"
batch_log="$result_root/batch_run.log"
experiments=(
  "S03-C-Q08|Conversation|conversation_trace.jsonl|conversation_q08.yaml"
  "S03-T-Q08|ToolAgent|toolagent_trace.jsonl|toolagent_q08.yaml"
)

mkdir -p "$result_root"
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
python "$repo_root/scripts/taskmain_v1.1/summarize_s03_recency.py" \
  --repo-root "$repo_root" --result-root "$result_root" 2>&1 | tee -a "$batch_log"
summary_exit=${PIPESTATUS[0]}
set -e
if [[ "$summary_exit" -ne 0 ]]; then
  echo "FAILED_EXPERIMENT=SUMMARY" | tee -a "$batch_log" >&2
  exit "$summary_exit"
fi
