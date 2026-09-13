# TaskMain-v1.1 Frozen Protocol

`TASK_MAIN_PROTOCOL = "v1.1"`.

Main workloads are `conversation_trace.jsonl` and `toolagent_trace.jsonl`. The
mechanism setting uses `N=4`, `theta=2.0`, COPY actions, and the frozen absolute
Development/Warmup/Evaluation/ObservationTail split. No state resets at boundaries.

The cache capacity is 585 trace pages per Pod. This is the equivalent capacity of
an 8 GiB KV cache pool under the stated Qwen2.5-1.5B KV-bytes/token assumption and
Mooncake 512-token trace pages; it is not a reconstruction of Mooncake hardware.
Each page is 14 MiB. P2P uses 25 GB/s, 1 ms control latency, FULL_PREFIX,
FULL_PAGE_ONLY, dual-end single-flight endpoints, source pin, target
TransferTemporary, and publication only after completion.

The service model is `d0=2 ms`, `mu=10,000 tokens/s`, a simulator assumption.
The simulation is P-only: no completion KV is constructed, `output_length` creates
no unknown hashes, and reported completion latency is a prefill-oriented simulator
metric rather than online end-to-end latency.

## Main policies

- E01/E08: R_AFF.
- E02/E09: R_REQ_KV_TASK.
- E03/E10: R_AFF + T_FUTURE_DEMAND(W=300s) + COPY + TRIGGER_ONLY, named TaskMain-Oracle.
- E04/E11: R_AFF + T_PERSIST(h=60s,K=10) + COPY + TRIGGER_ONLY.
- E05/E12: R_AFF + T_PERSIST(h=300s,K=10) + COPY + TRIGGER_ONLY.
- E06/E13: R_AFF + T_RECENCY(q=0.9,decay=60s) + COPY + TRIGGER_ONLY.
- E07/E14: R_AFF + T_PERSIST(h=300s,K=10) + COPY + COST_AWARE.

TaskMain-Oracle is a Future-Demand Reference, not a global optimum. It reads only
future external demand. `R_REQ_KV_TASK != R_REQ_KV_V1`: TASK disables the cache-hit
and absolute-gap gates and admits only when `Ls > theta * Lt`, using EffectiveLoad
without division; V1 remains the frozen implementation-oriented baseline.

All proactive policies use a 1-second trigger and one action per tick. Persistence
counts external demand in `(t-h,t]`, ranks deterministically, keeps Top-K, then tries
ranks in order until the first feasible action. Recency intentionally uses external
historical request demand rather than policy hits, exponential 60-second decay, and
the q=0.9 quantile over positive eligible scores.

Cost-aware uses the task-specified input-length weights and
`V_hat/V_ref - 0.5*C(p) - 0.1*T_move/T_ref`; weights are heuristic, not milliseconds.
`C(p)` is a relative load penalty and is zero when all loads are zero. References are
estimated once from combined Development observations and frozen before Evaluation.

The proactive byte budget is q=0.05 aggregate cache-capacity turnover/minute. Tokens
start at zero and never reset at Warmup or Evaluation. Actual FULL_PREFIX wire bytes
are charged only when transfer starts. The refill rate is independent of burst size.
The protocol burst cap is exactly one Pod cache capacity in bytes:
`585 * 14 MiB = 8,587,837,440 bytes`. It is not estimated from Development data.
Consequently every capacity-legal FULL_PREFIX action (`pages <= 585`) structurally
fits the bucket after sufficient accumulation. Candidate rejection, capacity or
endpoint failure, and cancellation before transfer start are not charged.

## R_REQ_KV_TASK activity accounting

`total_requests` is partitioned exactly into `task_gate_true`,
`stayed_local_gate_false`, `stayed_local_no_transferable_prefix`, and
`stayed_local_source_equals_target`. `tickets_created` counts actual NodeGate tickets;
`transfers_started` counts endpoint/Temporary admission; `transfers_completed` counts
wire completion. `ticket_fallbacks` counts created tickets that terminate without a
completed transfer, and `ticket_timeouts` is its timeout subset. Thus completed plus
ticket fallbacks equals tickets created, and completed <= started <= created.
Gate-true can exceed tickets when the target already owns the transferable chain.
