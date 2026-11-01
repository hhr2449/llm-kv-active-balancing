# Stage 1: N=16 baseline capacity scan

The 24 fixed YAML configs are in `configs/task_main/stage1_n16_capacity/`.
The original Stage 0/core source is frozen by `stage0_freeze.json`; this runner
uses Stage 0 actual-ready Saved and arrival-time Load reconciliation. No canonical
result is overwritten. No proactive policy is enabled.

## Prepare and validate

From the repository root, with a Python 3.10 environment containing requirements:

```bash
PYTHONDONTWRITEBYTECODE=1 python -m scripts.task_main_stage1.run prepare
PYTHONDONTWRITEBYTECODE=1 python -m scripts.task_main_stage1.run benchmark --workload conversation --root results/task_main/stage1_n16_capacity/benchmark_new_conversation
PYTHONDONTWRITEBYTECODE=1 python -m scripts.task_main_stage1.run benchmark --workload toolagent --root results/task_main/stage1_n16_capacity/benchmark_new_toolagent
PYTHONDONTWRITEBYTECODE=1 PYTEST_ADDOPTS='-p no:cacheprovider' python -m pytest -q tests/task_main tests/test_trace.py tests/test_cache.py
```

`--reference-engine` selects unoptimized Stage 0 for benchmark comparisons.
The optimized implementation changes only read-only admission scratch allocation
and eager summary memoization during actual unpin mutations. Full RunResult and
Stage 1 diagnostics are compared against Stage 0, not only Saved aggregates.

Pilot: `run pilot --workload WORKLOAD --root NEW_ROOT --workers 2` runs AFF@585,
REQ@585, REQ@infinity, each twice, on the first 120s of original trace. It retains
original timestamps and does not pretend that a short pilot covers the main
evaluation interval. Synthetic tests separately cover evaluation/ready boundaries.
`preflight equivalence` and `preflight receipt` produce a portable receipt matching
all source/config/test hashes. Formal runs require `--preflight`.

## Two-server formal run (after commit/push and matching HEAD)

Server A has `/root/miniconda3/bin/python`. Server B has a working project venv at
`/root/llm-kv-active-balancing/.venv/bin/python`; its default `python` is absent.
Do not depend on a noninteractive shell activating conda. Requirements must be
satisfied before launching; the baseline engine itself requires PyYAML and stdlib.

Commit only the Stage 1 additions (review `git status --short` first):

```bash
git add scripts/task_main_stage1 configs/task_main/stage1_n16_capacity tests/task_main/test_stage1_capacity.py docs/analysis/task_main_stage1_preflight.md
git commit -m "add N16 Stage 1 baseline capacity runner and validated diagnostics"
git push origin main
ansible server_b -i /root/kv-ansible/inventory.ini -m shell -a 'cd /root/llm-kv-active-balancing && git pull --ff-only'
ansible-playbook -i /root/kv-ansible/inventory.ini /root/kv-ansible/playbooks/check_repo.yml
```

Server A — Conversation only, 12 cases, two independent cases concurrently,
two deterministic replays per case:

```bash
tmux new-session -d -s stage1_conversation "bash -lc 'cd /root/data2/llm-kv-active-balancing && mkdir -p results/task_main/stage1_n16_capacity/run_20260922_01/logs && set -o pipefail && PYTHONDONTWRITEBYTECODE=1 /root/miniconda3/bin/python -u -m scripts.task_main_stage1.run run --workload conversation --workers 2 --repeat --resume --root results/task_main/stage1_n16_capacity/run_20260922_01 --preflight configs/task_main/stage1_n16_capacity/preflight.json 2>&1 | tee -a results/task_main/stage1_n16_capacity/run_20260922_01/logs/runner_conversation.log'"
```

Server B — ToolAgent only:

```bash
tmux new-session -d -s stage1_toolagent "bash -lc 'cd /root/llm-kv-active-balancing && mkdir -p results/task_main/stage1_n16_capacity/run_20260922_01/logs && set -o pipefail && PYTHONDONTWRITEBYTECODE=1 /root/llm-kv-active-balancing/.venv/bin/python -u -m scripts.task_main_stage1.run run --workload toolagent --workers 2 --repeat --resume --root results/task_main/stage1_n16_capacity/run_20260922_01 --preflight configs/task_main/stage1_n16_capacity/preflight.json 2>&1 | tee -a results/task_main/stage1_n16_capacity/run_20260922_01/logs/runner_toolagent.log'"
```

Both servers use the same relative run root and generate identical manifests from
the same committed source. A new HEAD/config/source requires a new run directory;
resume never silently mixes identities. The preflight receipt is independent of
HEAD, but checks exact source/config/tests content after commit/pull.

## Status, logs and resume

On the corresponding server, use the Python path above:

```bash
PYTHONDONTWRITEBYTECODE=1 /root/miniconda3/bin/python -m scripts.task_main_stage1.run status --root results/task_main/stage1_n16_capacity/run_20260922_01
tail -f results/task_main/stage1_n16_capacity/run_20260922_01/logs/runner_conversation.log
tail -f results/task_main/stage1_n16_capacity/run_20260922_01/logs/conversation/R_REQ_KV_TASK__1170.log
ansible all -i /root/kv-ansible/inventory.ini -m shell -a 'tmux ls 2>/dev/null || true'
```

For B substitute `.venv/bin/python`, `runner_toolagent.log`, `toolagent`.
After interruption, reuse the same tmux command with `--resume` when the old
session is no longer running. Completed cases are hash-verified and skipped.
Failed/interrupted cases restart in a fresh `.partial.UUID` directory; previous
partial evidence remains. `COMPLETE.json` is written after every output hash is
recorded, then the case directory is atomically renamed. Checkpoints distinguish
RUNNING/FAILED/COMPLETE; a stale RUNNING without a complete marker is incomplete.
A per-root advisory lock prevents concurrent batch writers. Main logs show case
progress/ETA; case logs show requests, simulation time, elapsed, ETA and peak RSS.
No case-internal snapshot is currently used: benchmark cases are below 30 minutes.

## Gather and aggregate after both workloads finish

On Server A (never rsync an entire canonical results tree):

```bash
rsync -a --partial -e 'ssh -p 30513' root@ssh-cn-xibei2.ebcloud.com:/root/llm-kv-active-balancing/results/task_main/stage1_n16_capacity/run_20260922_01/toolagent/ results/task_main/stage1_n16_capacity/run_20260922_01/toolagent/
PYTHONDONTWRITEBYTECODE=1 /root/miniconda3/bin/python -m scripts.task_main_stage1.run collect --root results/task_main/stage1_n16_capacity/run_20260922_01
```

Only matching manifests and verified formal completion markers are aggregated.
A single server has 12/24 cases and reports INCOMPLETE until both are gathered.
Outputs: `stage1_baseline_capacity.csv`, `stage1_per_pod.csv`,
`stage1_timeseries.csv`, `stage1_reactive_diagnostics.csv`, `stage1_validation.json`.
Time-series rows include JSON core/per-Pod/reactive columns; raw per-request,
transfer and occupancy records remain in each case for plotting and reanalysis.

## Metric boundaries

Request metrics use arrival cohorts; wire and copy-start use transfer start time.
`copy_ready_events` uses ready time; `copy_ready_start_cohort` is the completed
count of the start cohort. Boundary in-flight counts are left-limit states.
Pre/evaluation/post/full and 5-minute windows share one uninterrupted replay.
Occupancy is Published + Temporary; pinned is a subset, active runtime KV is
unmodeled. Infinite occupancy ratios are intentionally null; empty cohorts also
have null ratios/quantiles, never NaN/Infinity. Capacity preflight failures count
candidate-target probes, not failed requests or started actions.

Effective replica distribution covers all full pages observed in common external
inputs, including zero resident replicas, at end of run. It is not a hit-selected
hotspot set. Stage 0 did not implement hotspot coverage; no such metric is claimed.
Random Gini uses all 16 Pods, matching request counts, 200 assignments, fixed seed
20260911. Memoized equal immutable inputs give exactly the same baseline definition.
No capacity selection or causal eviction-loss interpretation is automated.
