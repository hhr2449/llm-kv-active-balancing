# TaskMain Stage D1 sensitivity

This driver executes the frozen 32-case, 64-replay Stage D1 matrix. It writes only under
`results/task_main/stage_d1_sensitivity/run_<id>/`, emits progress every 60 seconds, and
atomically checkpoints every completed replay. Existing formal results are read-only references.

Preflight (short, safe to run interactively):

```bash
cd /root/data2/llm-kv-active-balancing
python -m scripts.task_main_d1.preflight
```

First launch in tmux (the output run directory must not already exist):

```bash
cd /root/data2/llm-kv-active-balancing
mkdir -p results/task_main/stage_d1_sensitivity
tmux new-session -d -s taskmain_d1 "bash -o pipefail -lc 'cd /root/data2/llm-kv-active-balancing && python -u -m scripts.task_main_d1.run_stage_d1 --output results/task_main/stage_d1_sensitivity/run_20260917_01 --workers 20 --progress-interval 60 --request-progress-every 250 2>&1 | tee results/task_main/stage_d1_sensitivity/run_20260917_01.console.log'"
tmux attach -t taskmain_d1
```

Monitor without attaching:

```bash
watch -n 30 cat /root/data2/llm-kv-active-balancing/results/task_main/stage_d1_sensitivity/run_20260917_01/progress.json
```

Resume after interruption. Use the same run directory and unchanged source/config files:

```bash
tmux new-session -d -s taskmain_d1 "bash -o pipefail -lc 'cd /root/data2/llm-kv-active-balancing && python -u -m scripts.task_main_d1.run_stage_d1 --output results/task_main/stage_d1_sensitivity/run_20260917_01 --workers 20 --progress-interval 60 --request-progress-every 250 --resume 2>&1 | tee -a results/task_main/stage_d1_sensitivity/run_20260917_01.console.log'"
tmux attach -t taskmain_d1
```

On PASS, the driver publishes all six required CSV/JSON families plus
`all_sensitivity_long.csv/json`, then replaces
`docs/analysis/task_main_stage_d1_sensitivity.md` with the result report.
