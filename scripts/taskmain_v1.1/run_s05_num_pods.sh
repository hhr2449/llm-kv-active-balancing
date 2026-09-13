#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
config_root="$repo_root/configs/taskmain_v1.1/sweeps/num_pods"
result_root="$repo_root/results/taskmain_sweeps/S05_num_pods"
batch_log="$result_root/batch_run.log"
experiments=(
  "S05-C-N2-R_AFF|Conversation|conversation_trace.jsonl|conversation_n2_r_aff.yaml"
  "S05-C-N2-R_REQ_KV_TASK|Conversation|conversation_trace.jsonl|conversation_n2_r_req_kv_task.yaml"
  "S05-C-N2-ORACLE|Conversation|conversation_trace.jsonl|conversation_n2_oracle.yaml"
  "S05-C-N2-PERSIST_H1_K10|Conversation|conversation_trace.jsonl|conversation_n2_persist_h1_k10.yaml"
  "S05-C-N2-PERSIST_H5_K10|Conversation|conversation_trace.jsonl|conversation_n2_persist_h5_k10.yaml"
  "S05-C-N2-RECENCY_Q09|Conversation|conversation_trace.jsonl|conversation_n2_recency_q09.yaml"
  "S05-C-N2-COST_AWARE|Conversation|conversation_trace.jsonl|conversation_n2_cost_aware.yaml"
  "S05-T-N2-R_AFF|ToolAgent|toolagent_trace.jsonl|toolagent_n2_r_aff.yaml"
  "S05-T-N2-R_REQ_KV_TASK|ToolAgent|toolagent_trace.jsonl|toolagent_n2_r_req_kv_task.yaml"
  "S05-T-N2-ORACLE|ToolAgent|toolagent_trace.jsonl|toolagent_n2_oracle.yaml"
  "S05-T-N2-PERSIST_H1_K10|ToolAgent|toolagent_trace.jsonl|toolagent_n2_persist_h1_k10.yaml"
  "S05-T-N2-PERSIST_H5_K10|ToolAgent|toolagent_trace.jsonl|toolagent_n2_persist_h5_k10.yaml"
  "S05-T-N2-RECENCY_Q09|ToolAgent|toolagent_trace.jsonl|toolagent_n2_recency_q09.yaml"
  "S05-T-N2-COST_AWARE|ToolAgent|toolagent_trace.jsonl|toolagent_n2_cost_aware.yaml"
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
python "$repo_root/scripts/taskmain_v1.1/summarize_s05_num_pods.py" \
  --repo-root "$repo_root" --result-root "$result_root" 2>&1 | tee -a "$batch_log"
summary_exit=${PIPESTATUS[0]}
set -e
if [[ "$summary_exit" -ne 0 ]]; then
  echo "FAILED_EXPERIMENT=SUMMARY" | tee -a "$batch_log" >&2
  exit "$summary_exit"
fi
