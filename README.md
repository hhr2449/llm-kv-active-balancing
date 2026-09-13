# llm-kv-active-balancing

Current frozen baseline is `B0_IMPL_VERSION = r_req_kv_v1` (`B0 = R_REQ_KV`, FIFO reactive NodeGate,
admission-time replan, timeout, and RemotePressure accounting).

Formal Experiment Protocol v1 uses one continuous replay with half-open absolute-time
splits: Development `[0, 900000)`, Warmup `[900000, 1500000)`, Evaluation
`[1500000, 2700000)`, and ObservationTail `[2700000, trace_end]` ms. Simulator
state is never reset at a boundary; formal summaries include Evaluation requests and
the Evaluation time interval only. The frozen metric definition is `METRIC_VERSION = v1`.

The nominal four-Pod cluster is a simulator mechanism setting. It is not a topology
recovered from the Mooncake traces and must not be described as Mooncake's production
deployment size. Future topology sensitivity reserves 8- and 16-Pod cases.

Mooncake timestamps are milliseconds on an effective arrival grid of approximately
3 seconds. This is a trace property, not a frozen proactive trigger period. A future
proactive study must treat 3 seconds only as one candidate and evaluate trigger
period, phase offset, and fixed-seed bucket-smoothing sensitivity.

Kernel v3b does not implement proactive triggers, Oracle policies, or replication
without a request.

LoadGap percentiles are duration-weighted over event intervals. Running remaining
work is linear inside an interval; the diagnostic uses the interval midpoint LoadGap,
matching the convention introduced in Kernel v3a. `busy_while_other_idle_ms` is
wall-clock time, while `queued_request_ms_while_other_idle` is request × ms.
The nominal service, transfer, and gate values are development simulator assumptions,
not production-calibrated parameters. Formal outputs embed their complete provenance.

## O1 substrate: Future-Demand Reference

O1 is frozen B0 plus proactive `COPY`. Its Oracle score is only the number of future
external requests containing a candidate Prefix endpoint in `(t, t + W]`; it is not
a global optimum or an optimal scheduler and cannot read future routing, Load, Cache,
queue, eviction, or transfer outcomes. Candidates are generated only from online-seen
request endpoints and branching Prefix nodes that currently have a Published source.

The current O1 implementation is hard-gated to Development summaries. A Development
query crossing 900,000 ms raises a leakage error, and only fully observed trigger
windows run. Evaluation has not been executed. Reactive FIFO admission precedes
`TRIGGER_TICK`; background copies never enter NodeGate and start only when both
endpoints and capacity are immediately available. Started copies are non-preemptive.

Wasted-copy categories use the mutually exclusive precedence: `CENSORED`,
`DUPLICATE_WIRE`, `NO_FUTURE_DEMAND`, `TOO_LATE`, `USEFUL`,
`EVICTED_BEFORE_USE`, then `NOT_USED_AT_TARGET`. `NOT_USED_AT_TARGET` only means
that a ready proactive residency with relevant external demand was not hit at that
target by a real `SERVICE_START`; it makes no counterfactual claim that another target
would have been better. `EVICTED_BEFORE_USE` is likewise an observed generation
outcome, not a claim that the eviction was wrong. Actual use requires a normal request to
hit the still-current proactive residency generation at `SERVICE_START`. Eviction or
later reactive/service reinsertion terminates attribution to the old generation.
