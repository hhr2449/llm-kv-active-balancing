#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
config_root="$repo_root/configs/taskmain_v1.1/sweeps/persistence_h_k"
result_root="$repo_root/results/taskmain_sweeps/S02_persistence_h_k"
batch_log="$result_root/batch_run.log"
experiments=(
  "S02-C-H1-K5|Conversation|conversation_trace.jsonl|conversation_h1_k5.yaml"
  "S02-C-H1-K20|Conversation|conversation_trace.jsonl|conversation_h1_k20.yaml"
  "S02-C-H5-K5|Conversation|conversation_trace.jsonl|conversation_h5_k5.yaml"
  "S02-C-H5-K20|Conversation|conversation_trace.jsonl|conversation_h5_k20.yaml"
  "S02-T-H1-K5|ToolAgent|toolagent_trace.jsonl|toolagent_h1_k5.yaml"
  "S02-T-H1-K20|ToolAgent|toolagent_trace.jsonl|toolagent_h1_k20.yaml"
  "S02-T-H5-K5|ToolAgent|toolagent_trace.jsonl|toolagent_h5_k5.yaml"
  "S02-T-H5-K20|ToolAgent|toolagent_trace.jsonl|toolagent_h5_k20.yaml"
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
python "$repo_root/scripts/taskmain_v1.1/summarize_s02_persistence_h_k.py" \
  --repo-root "$repo_root" --result-root "$result_root" 2>&1 | tee -a "$batch_log"
summary_exit=${PIPESTATUS[0]}
set -e
if [[ "$summary_exit" -ne 0 ]]; then
  echo "FAILED_EXPERIMENT=SUMMARY" | tee -a "$batch_log" >&2
  exit "$summary_exit"
fi
