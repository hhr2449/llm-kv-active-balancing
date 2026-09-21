# TaskMain v1 Formal Method Assumptions

状态：正式实验前冻结方法说明。正式 14 行尚未运行。

本文汇总正式 TaskMain v1 的执行预算和模型假设；具体状态机、策略公式、边界与验证契约以 [`task_main_v1.md`](task_main_v1.md) 为准。

## Core assumptions

- **P-only replay**：只使用 Prompt `hash_ids`；unknown completion KV 不构造，output length 仅为 metadata。
- **Topology and capacity**：N=4；每 Pod 585 logical pages；每页最多 512 Prompt tokens；每个完整 wire page 按 14 MiB 计。
- **Cross-Pod transfer**：只传 `FULL_PAGE_ONLY` Prefix。每条 transfer 独立获得 25 GB/s effective bandwidth，不建模 shared NIC 或 endpoint contention。
- **Cache**：PublishedUnique + full-wire Temporary 不超过 585 pages/Pod；只 eviction unpinned leaf，保持 Prefix closure。真实 request reuse 刷新 leaf-LRU；probe/copy/pin 不算 reuse。
- **Load proxy**：`(t-60s,t]` 内 final-assigned miss-prefill tokens；不是 GPU queue、service time 或 remaining work。theta=2。
- **Target selection**：`FEASIBLE-TARGET-FIRST`。先在 structural targets 上按当前 leaf-LRU/pins/Temporary/full-wire 规则做 non-mutating capacity preflight，再从 feasible targets 中按 `(Load, Pod ID)` 选唯一 target。Reactive overload gate 仍比较 source 与 absolute-lowest-load other Pod；gate 通过后才应用 feasible-target-first。已持有 transferable Prefix 的 reactive direct target 不需要 transfer capacity。
- **Proactive action budget**：每个 external request completion 恰好一个 proactive opportunity；每 opportunity 最多一个成功 proactive COPY。高排名 chain 失败可以继续下一 chain，首次成功后停止。
- **Evaluation**：从 t=0 连续演化状态；primary common-support Evaluation 是 arrival in `[25,45)` minutes；W=300s。
- **Oracle label**：Line 3 是 **Future-Demand Oracle Reference under the TaskMain v1 action budget**。它只完美知道 `(t,t+W]` external demand，仍受 action_cap=1、finite capacity、finite wire delay、online candidate availability、FULL_PAGE_ONLY 和 frozen target policy 限制；不是 mathematical global optimum、unlimited oracle 或 theoretical maximum achievable benefit。
- **No service model**：不使用 GPU service-time、FCFS Prefill queue、RemotePressure、NodeGate、timeout/replan、tick 或 proactive byte budget。

`action_cap=1` 是 **TaskMain v1 additional scheduling constraint / action-budget assumption**。它给每次决策明确、可复现的主动动作预算，保持 trigger 策略可比较，并避免一次 external request 触发无界复制；它不是原任务文档已经明确给出的条件，也不是生产系统硬性限制。

## Frozen seven-line matrix per workload

| Line | Routing | Proactive policy |
|---:|---|---|
| 1 | R_AFF | NONE |
| 2 | R_REQ_KV_TASK | NONE |
| 3 | R_AFF | FUTURE_DEMAND reference |
| 4 | R_AFF | PERSISTENCE, h=60s, K=10 |
| 5 | R_AFF | PERSISTENCE, h=300s, K=10 |
| 6 | R_AFF | RECENCY, q=0.9, decay=60s |
| 7 | R_AFF | PERSISTENCE_COST_AWARE, h=W=300s, K=10 |

Line 5/7 必须 execution-equivalent；Line 7 只增加真实 Cost-aware diagnostics 和严格 `Score>0` gate，不重排 Persistence shortlist。K 是 shortlist size，不是 action budget。
