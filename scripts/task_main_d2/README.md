# TaskMain Stage D2 COPY vs MOVE ablation

The D2 driver executes only the four frozen MOVE cases (Oracle and Persistence
h=300s for two workloads), each twice. Formal COPY Lines 3/5 remain read-only
references. The driver writes progress every 60 seconds and atomically records a
checkpoint after every complete replay.

Short preflight and synthetic smoke:

```bash
cd /root/data2/llm-kv-active-balancing
python -m scripts.task_main_d2.preflight
python -m scripts.task_main_d2.run_smoke
```

First launch in tmux (the run directory must not already exist):

```bash
cd /root/data2/llm-kv-active-balancing
mkdir -p results/task_main/stage_d2_move_ablation
tmux new-session -d -s taskmain_d2 "bash -o pipefail -lc 'cd /root/data2/llm-kv-active-balancing && python -u -m scripts.task_main_d2.run_stage_d2 --output results/task_main/stage_d2_move_ablation/run_20260918_02 --workers 8 --progress-interval 60 --request-progress-every 250 2>&1 | tee results/task_main/stage_d2_move_ablation/run_20260918_02.console.log'"
tmux attach -t taskmain_d2
```

Monitor without attaching:

```bash
watch -n 30 cat /root/data2/llm-kv-active-balancing/results/task_main/stage_d2_move_ablation/run_20260918_02/progress.json
```

Resume after interruption, using the same directory and unchanged source/config:

```bash
tmux new-session -d -s taskmain_d2 "bash -o pipefail -lc 'cd /root/data2/llm-kv-active-balancing && python -u -m scripts.task_main_d2.run_stage_d2 --output results/task_main/stage_d2_move_ablation/run_20260918_02 --workers 8 --progress-interval 60 --request-progress-every 250 --resume 2>&1 | tee -a results/task_main/stage_d2_move_ablation/run_20260918_02.console.log'"
tmux attach -t taskmain_d2
```

On PASS the driver publishes two CSV/JSON tables and atomically replaces
`docs/analysis/task_main_stage_d2_move_ablation.md` with the final report.
