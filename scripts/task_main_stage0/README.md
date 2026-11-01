# Stage 0 capacity audit

All commands run from the repository root. This entry profiles inputs and runs
small synthetic checks only; it does not launch Stage 1 or overwrite canonical
outputs. Existing output directories cause an error.

```bash
PYTHONDONTWRITEBYTECODE=1 python -m scripts.task_main_stage0.profile_workload \
  --workload conversation --benchmark-seconds 120 --output /tmp/conversation-benchmark-new
PYTHONDONTWRITEBYTECODE=1 python -m scripts.task_main_stage0.profile_workload \
  --workload toolagent --benchmark-seconds 120 --output /tmp/toolagent-benchmark-new
PYTHONDONTWRITEBYTECODE=1 python -m scripts.task_main_stage0.profile_workload \
  --workload conversation --output results/task_main/stage0_capacity_audit/profiles/conversation
PYTHONDONTWRITEBYTECODE=1 python -m scripts.task_main_stage0.profile_workload \
  --workload toolagent --output results/task_main/stage0_capacity_audit/profiles/toolagent
PYTHONDONTWRITEBYTECODE=1 python -m scripts.task_main_stage0.combine_profiles \
  --root results/task_main/stage0_capacity_audit
PYTHONDONTWRITEBYTECODE=1 python -m scripts.task_main_stage0.run_smoke \
  --output results/task_main/stage0_capacity_audit/smoke
PYTHONDONTWRITEBYTECODE=1 PYTEST_ADDOPTS='-p no:cacheprovider' \
  python -m pytest -q tests/task_main tests/test_trace.py tests/test_cache.py
```

Capacity studies use `CapacityStudyConfig` and `CapacityStudyEngine` from
`src.simulator.task_main.stage0`, with study version `TASK_MAIN_CAPACITY_STAGE0_V1`.
`capacity_mode: infinite` requires `capacity_pages: null`; it remains one cache per
Pod. The canonical config parser and frozen experiment matrices are unchanged.
For future study configs, `experiment_kind: capacity_study` is distinct from
canonical `formal` identity. Stage 0 does not choose subsequent study capacities.

The study corrects reactive ready-time Saved and reconciles the one original
arrival-time Load entry. Pending transfers use their guaranteed hit until ready;
no future knowledge is injected. Pure Load queries do not prune or advance state.
Every study result includes `summary.stage0_diagnostics` for all policies.

Full profiling is linear in full-page references plus the fixed window grid.
Identity is interned as `(parent_node_id, block_hash)` after the TaskMain reader
validates ancestry/depth/valid tokens. The output stores 1-second trailing-window
series, reference-count histograms and reuse-gap histograms. It does not build
all prefix tuples or a requests × all-prefixes matrix.

Canonical results must not be mixed with the new semantic revision. The original
core is preserved byte-for-byte because D1/D2 deliberately validate its frozen
source hashes. Findings and exact metric definitions are in
`results/task_main/stage0_capacity_audit/implementation_audit.md` and
`diagnostics_schema.md`.
