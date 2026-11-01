# TaskMain v1 — Stage A / Stage B / Stage C 冻结协议

协议标识：`TASK_MAIN_V1_STAGE_A`。来源：2026-09-17 用户的 Stage A 实施指令。本文是 Stage A 的唯一实现依据；与初始 Gap Analysis 不同的已确认条款以本文为准。任何必须偏离本文的实现必须停止并报告，不自行调整协议。

历史 TaskMain-v1.1、O2、Strong Reactive、P3 和 learned-ranking 不属于本协议。保留所有历史代码、配置、测试和结果，不改写其语义。

## 1. 阶段范围与隔离

Stage A 只实现 Trace/Prefix、Cache/leaf-LRU、60s miss-token Load、R_AFF、R_REQ_KV_TASK、有限传输 ready、抽象请求完成、Prompt 发布、request/transfer/opportunity records 和最小 summary/validation。只运行合成测试及必要的共享 Trace/Cache regression tests。

独立目录：`src/simulator/task_main/`、`configs/task_main/`、`scripts/task_main/`、`tests/task_main/`。Stage B 才增加 `candidates.py` 和 `policies.py`。

Stage A 禁止 Oracle、Persistence、Recency 策略、Cost-aware 策略、proactive COPY、O2-P/O2-A/Full O2-A、Strong Reactive、P3/learned policy、GPU service_ms、FCFS Prefill queue、completion-latency 主实验、1s trigger tick、proactive byte budget、RemotePressure、NodeGate、admission timeout、transfer replan、control latency、正式 14 行和 sweep。

允许复用安全的 Trace 原语；routing、Load、request state evolution 不经过旧 `PrefillServiceModel`、`Pod.load_ms`、FCFS 或旧 engine。隔离必须由依赖检查、配置 allowlist、fail-fast 测试或 provenance 验证，不能只称“未开启旧功能”。

## 2. 输入、页与主配置

Workloads 分别为：

- `data/mooncake/conversation_trace.jsonl`
- `data/mooncake/toolagent_trace.jsonl`

P-only，只使用 Prompt `hash_ids`，不构造未知 completion KV。`output_length` 仅为 trace metadata。本地 Prefix hit 使用实际 valid tokens；每个 trace page 的最大有效长度为 512 tokens，partial tail identity 可存在本地 Cache。

主配置：N=4、每 Pod 585 logical pages、theta=2、Load window=60000ms。每个完整 512-token page 的 wire bytes 为 **14 MiB = 14 × 1024 × 1024 bytes**；这是继承的 Qwen 等效模拟假设，不是 Mooncake 原硬件实测值。

沿用独立 workload 的连续时间范围：`[0,900000)` 状态演化，`[900000,1500000)` warmup，arrival in `[1500000,2700000)` 为主 Evaluation cohort，之后为 observation tail。边界不清空 Cache、history、Load 或 in-flight。Stage A 不运行正式 trace 实验或最终主指标汇总。

## 3. 可传输 Prefix

使用 `FULL_PAGE_ONLY`。**先将逻辑 Prefix 转成完整页 Prefix，再把转换结果作为传输对象**。不允许先按含 partial tail 的长 chain 评分、执行时才截短。Stage A reactive 和未来 Stage B 必须使用相同转换原语。

```text
wire_tokens = wire_pages * 512
wire_bytes  = wire_pages * 14 MiB
```

转换后为空则没有可传输 Prefix；不得生成零页 wire 动作。

## 4. Load：recent committed miss-prefill-token workload proxy

`Load_p(t)` 为 final assignment time 落在 `(t-60000ms,t]` 内、已 final-assigned 到 Pod p 的请求的 miss prefill tokens 之和。

顺序固定：route / reactive decision -> 最终 Pod -> 最终可用 Prefix hit -> miss_tokens -> **立即入最终 Pod 的 Load** -> 必要时等待 transfer ready。

Load entry timestamp = final assignment time，通常为 arrival / transfer start。当前请求在自身 route/target decision 前不得入账；每请求只入账一次。transfer 等待时 Load 已经包含承诺的 miss tokens，不等完成后才记。

Load 不是 queue、实时 backlog、GPU 剩余服务时间或已完成工作量。窗口左边界严格排除，不用近似衰减。

## 5. R_AFF

按以下 key 选择 Pod：

1. 当前 Published Cache 最长连续 Prefix hit 降序。
2. 当前 60s Load 升序。
3. Pod ID 升序。

Temporary/future-ready copy 不可命中。lookup 不改变 LRU 或真实 reuse history。当前请求不参与自身 routing Load。

## 6. R_REQ_KV_TASK

先按 R_AFF 得 affinity source。overload gate 的比较对象保持为**其他 Pods 中绝对最低 `(Load, Pod ID)` 的 Pod**；gate 为严格乘法比较 `L_source > 2 * L_absolute_lowest_other`，不使用除法，不加 cache threshold、absolute gap、ECT、RemotePressure、timeout、NodeGate 或 replan。capacity feasibility 不改变 gate 的比较对象。

2026-09-17 用户补充确认的唯一执行顺序：

1. **先检查 gate**。若 false，final_pod=source，不转 target，不产生 wire。
2. 若 true，将 source 的本次命中转换为 FULL_PAGE_ONLY 可迁移 Prefix。
3. 若 transferable_pages=0，保留 source，不产生 wire；不得将空 Prefix 当成 target 已有可迁移 Prefix，不允许纯 Load balancing direct-target。
4. 否则对所有 other Pods 做 target feasibility：已完整持有 transferable Prefix 的 Pod 是零 transfer-capacity 的 direct feasible target；缺 Prefix 的 Pod 仅在同一无副作用 capacity preflight 成功时 feasible。
5. 从 feasible targets 中按 `(Load, Pod ID)` 选择唯一 target。direct target 不产生 wire，本次 hit 使用其当前实际本地连续 Prefix（按 valid tokens 计），不虚构 source 的 partial tail；COPY target 只 commit 已验证 plan。若 feasible targets 为空，fallback 最初 source。

因此“target 已有 Prefix”不绕过 gate；source 的 partial tail 不属于直接转 target 时要求其持有的可迁移 Prefix。2026-09-17 Stage A 修订冻结：转换为空时即使 gate=true 也必须保留 source。2026-09-17 pre-formal B 修订冻结：gate 比较对象仍是 absolute-lowest other Pod，执行 target 则是 feasible targets 中最低 `(Load, Pod ID)`。仅用于合成测试的单 Pod 拓扑没有其他 target，保留 source。

## 7. Reactive COPY：preflight -> atomic commit

Preflight 必须检查 source 完整 transferable Prefix、wire pages、target 全 wire Temporary pages、合法 leaf-LRU 可腾出的容量及保护/pin 冲突。Preflight 是确定性、无副作用的 capacity-feasibility 判断：不修改 Cache/LRU、Load、history、records，不 eviction、不产生 wire、不创建 Temporary；允许返回 immutable admission plan。只有最终选中的 target 可以 validate/commit 该 plan。

成功时原子执行必要 eviction、source Prefix pin、target Temporary reservation，final assignment=target。按 COPY 后本请求可用 Prefix 计算 miss_tokens，在 transfer start 时立即记 target Load，再开始传输。

失败时不发生 preflight eviction/Temporary/wire；fallback 到**最初** R_AFF source，按 source 当前真实 Prefix hit 计算 miss、立即记 source Load，然后在 source 完成抽象处理。记录 `REACTIVE_FALLBACK_CAPACITY` 或对应具体原因，不能丢请求。

不能先淘汰一批页，再发现无法完成而留下半执行状态。

## 8. 网络模型

采用 **no-contention independent-transfer network model**：

```text
t_ready_seconds = t_start_seconds + wire_bytes / 25e9
t_ready_ms      = t_start_ms      + wire_bytes / 25e9 * 1000
```

25 GB/s 是 **per-transfer effective bandwidth assumption**，不是共享集群总带宽。m 条并发传输可产生 m × 25 GB/s 的瞬时总速率。不同 transfer 可以同时使用同一 source/target；不建模 endpoint contention、shared NIC、FIFO transfer queue、NodeGate、queue delay、endpoint waiting 或 control latency。

## 9. Temporary、pin 与容量

每个 Pod 始终满足：

```text
PublishedUniquePages + TransferTemporaryPages <= capacity_pages
```

主配置 capacity_pages=585。没有 ActivePrivate 或 active compute state。

Source transferable Prefix 在传输期间 pin；pin/unpin 不刷新 source request-access LRU，不构成 reuse。

Target TemporaryPages=全部 wire pages；ready 前不可 lookup，但计入容量。即使 target 已有祖先，wire 和 Temporary 仍按整条 transferable Prefix 计算。不同 transfer 的 buffer 分别占容量，即使携带相同 hash。

ready 时 dedup：已有页释放对应 Temporary，missing pages 发布为新 resident，Published Cache 按 hash 去重；不得提前发布。

## 10. Cache / leaf-LRU 与真实 reuse

只 eviction 非 pinned leaf，保持 Prefix closure。真实请求实际复用的 Prefix 路径刷新 request-access LRU time。

- 新 Prompt page：抽象请求完成并写 Cache 时，初始 LRU timestamp=completion/publish time。
- 新 COPY page：ready 后发布时，初始 LRU timestamp=ready time。
- COPY 不算真实 Cache reuse，不产生 Recency event，不刷新 target 已存在 duplicate 页，不刷新 source request-access time。

Cache eviction 的 insertion order 与真实 request reuse history 明确分离。Stage A 可记录真实 reuse 证据，但不实现 Recency 策略。

## 11. Prompt Cache admission

请求成功不依赖新增 Prompt suffix 是否能全部缓存。请求完成后计算需要新增的 Prompt pages，并对合法 leaf-LRU 腾挪进行无副作用 preflight。

成功则原子 eviction+commit；失败则请求仍成功，该次无法合法容纳的新增 Prompt Cache 不写入，记录 `CACHE_ADMISSION_SKIP`。不能终止 simulator，不能留下部分 eviction。必须维持现有 Prefix 与受保护页的有效性。

## 12. 请求完成与 opportunity

无 transfer：arrival -> routing -> final assignment -> Load accounting -> abstract completion -> Prompt publish attempt -> 一个 opportunity record。

有 transfer：arrival -> route/preflight -> final assignment -> Load accounting -> transfer start/in-flight -> ready -> abstract completion -> Prompt publish attempt -> 一个 opportunity record。

每个 external request 恰好完成一次、恰好入账一次、恰好生成一个 opportunity。等待 transfer 的 arrival 不产生 opportunity。Stage A hook 只记录，不执行任何主动动作。

## 13. 同 timestamp 的精确顺序

时间 t：

1. 先处理所有 `TRANSFER_COMPLETE(t)`，发布其副本。
2. 按原 request ID 稳定顺序恢复对应 reactive requests：完成、Prompt publish attempt、唯一 opportunity。
3. 再按 request ID 稳定顺序逐个处理 arrival_time=t 的 external requests。

无 reactive wait 的 arrival 在处理下一个同 timestamp request 前，必须完成 route/assignment/Load/complete/publish/opportunity 全链；后一个 request 可看到前者已发布 Cache。有 wait 则在 start 后暂停该 request，继续处理后续 arrivals，直到 ready 才完成和产生机会。

不能把所有同 timestamp arrivals 先 route 完再统一 publish，也不能为一个 reactive request 在 arrival/ready 各生成一次机会。

### Committed-hit protection

已经 final-assigned、final hit 已确定、因 reactive transfer 等待的请求，其 committed hit Prefix 在 abstract completion 前 temporarily protected，保证已承诺的 hit 语义不被其他请求的 Prompt admission 破坏。

实现分为两个连续的保障时段：传输未 ready 时，完整 transferable Prefix 在 source 被 pin，target 全 wire Temporary 独立预留、不可 lookup；ready 发布后，在恢复本批任何请求前，先 pin 所有待恢复请求的 target committed hit paths。源 pin 在 transfer complete 释放；从发布到本批 target pin 之间不处理 arrival、Prompt admission 或 eviction。该机制保证承诺内容，不要求尚未 Published 的 target 页提前可见。

按稳定 request ID 恢复时，A 的 Prompt admission 不得淘汰尚未完成的 B 的 committed hit path；必要时 A 记录 CACHE_ADMISSION_SKIP。每个请求在同一个 completion/publish 原子处理过程结束时立即释放自身保护，随后记录 opportunity，不延续到后续事件。

保护本身不刷新 LRU/access timestamp，不产生 Recency/reuse event，不占用网络 endpoint，不引入 admission queue 或服务模型。真实请求完成时的实际 reuse 则按既有规则刷新 LRU。结束时所有 transfer pin 与 committed-hit protection 必须排空。

## 14. Stage A 记录与验证

所有记录时间字段使用 ms，metadata 显式标单位。稳定 ID、输出顺序和 Cache digest 必须可重复。

`request_records.jsonl` 至少记录：request_id、arrival_time、completion_time、routing_policy、affinity_source、final_pod、route_hit_pages/tokens、final_hit_pages/tokens、miss_tokens、load_vector_before_route、load_account_time、nullable reactive_transfer_id、reactive_fallback_reason、cache_admission_skip、split。保留 workload 和 Prompt/output metadata。

`transfer_records.jsonl` 至少记录：transfer_id、request_id、type=REACTIVE、source、target、transferable_chain、wire_pages/tokens/bytes、start_time、ready_time、temporary_pages、newly_resident_pages、duplicate_pages、status。

`opportunity_records.jsonl` 至少记录：opportunity_id、request_id、opportunity_time、workload、load_vector、Cache state digest/summary、split。

另生成最小 summary/validation，验证请求守恒、唯一 completion/Load/opportunity、最长 Prefix hit/miss tokens 守恒、transfer 完成前不可见、wire/Temporary/dedup 守恒、source pin、容量上界、无半执行 eviction、确定性和历史依赖隔离。Stage A 不要求最终 TaskMain 指标全集。

合成测试必须覆盖用户列出的 18 类：cold/self-load、最长 affinity、affinity ties、精确 60s Load、严格 reactive gate、direct target、finite COPY、COPY preflight failure、source pin、full wire Temporary/dedup、并发独立 transfer、COPY LRU/no reuse、Prompt admission skip、同 timestamp、opportunity uniqueness、P-only、deterministic replay、capacity invariant。允许小型合成拓扑/容量用于构造边界；主配置仍固定 N=4/585 pages。

## 15. Stage B 共通冻结语义与 execution equivalence

本节在 Stage A 时先行记录；Stage B 已获后续授权，具体实施范围见第17节。

1. Lines 3–7 routing=R_AFF。Oracle 是 global Future-Demand Reference：`F(c,t)` 只统计 `(t,t+W]` 包含 chain c 的未来请求，不读未来 routing、Load 或 eviction。“别的 Pod”表示当前有未完整持有该 chain 的可选 target，Oracle 不预知未来 placement。
2. Persistence 使用历史 request demand count。Recency 仅由真实 request Cache reuse event 刷新；copy/probe/cold demand 不刷新。
3. 每个 completion 一个 opportunity，每 opportunity 最多成功启动一个 proactive COPY。
4. Line 7 先按 Persistence count 得 Top-10，保持 Persistence ranking，Cost-aware 仅作 `Score>0` gate，**不按 Score 重排**。
5. Cost-aware 使用以下原始公式，不引入 V_ref/T_ref 或其他 normalization：

```text
benefit = sum_b n_hat(c,b,W) * multiplier_b
n_hat = (W/h) * n_history(c,b,h)
congestion = Load_target / max(Load), or 0 when max(Load)=0
transfer_cost = wire_bytes / 25e9  # seconds
Score = benefit - 0.5 * congestion - 0.1 * transfer_cost
```

乘子依次为 `<5k:1.3`、`5k–20k:2.5`、`20k–60k:5.3`、`60k–120k:8.9`、`120k–300k:11.3`、`>300k:15.2`，是 value multipliers，不是 ms。

主配置 W=h=300s、candidate persistence count>0，因此 benefit>=1.3，congestion penalty<=0.5；最大单 Pod wire transfer penalty 为 `0.1*(585*14*1024*1024)/25e9 = 0.03435134976 < 0.035`，所以合法 Persistence candidate 的 Score 必然>0。

**主配置下 Line 5 与 Line 7 必须 execution-equivalent / behaviorally identical。** 原始记录保留不同的 `policy=PERSISTENCE/PERSISTENCE_COST_AWARE`；Line 7 保留 benefit、congestion、transfer_cost_seconds、cost_score。这些来源和评分诊断不参与执行判等，不要求整行/整文件相等。不得为了人为区分二者而修改 multiplier、lambda、单位、references、normalization 或排序。

判等由代码中显式 `execution_projection` 完成：逐 opportunity 比较 time/request_id、是否有 action、shortlist chain 顺序、attempt/skip 执行结果；有动作则比较 chain/depth/source/target/wire pages/tokens/bytes/start/ready/status。action 顺序及数量、最终 Cache/Load/transfer state、request assignments、summary 所有非 policy-diagnostic 系统结果必须相同。配置与来源 provenance 不作为系统结果。另验证 Line 7 每个进入 gate 的合法 shortlist candidate 的 cost_score>0、cost_gate_rejected_count=0，不能伪造相同 policy 来通过测试。

## 16. Stage A 交付边界

实现与测试报告写入 `docs/analysis/task_main_stage_a_implementation.md`，包括文件、架构、复用/隔离、逐测试结果、invariants、Stage B 未实现项、歧义/blocker 和 diff summary。不得运行正式 14 行、Oracle/Persistence 实验、sweep、O2/SR/P3。完成后停止，等待下一步指令。

## 17. Stage B 冻结扩展

本节由后续 Stage B 授权生效；前文 Stage A 禁止策略实施的范围限定于 Stage A。保留 A 协议标识；B 配置使用 `TASK_MAIN_V1_STAGE_B`。不改变 Load、routing、Cache、wire timing、LRU、completion、事件顺序或 opportunity 唯一性。禁止正式14行、最终指标评价、sweep、O2/SR/P3、GPU/FCFS、endpoint contention、NodeGate、tick、byte budget。

### Candidate 与执行顺序

- 决策单位是 FULL_PAGE_ONLY chain，稳定 chain_id 使用整数 hash path tuple，按 tuple 字典序排序。identity/depth/score/wire/source/target 始终对应同一完整页 chain。
- external ARRIVAL 注册所有非空完整页 ancestors，如 `[a,b,c]` 注册 `[a]`、`[a,b]`、`[a,b,c]`；不使用 future 树结构或 endpoint/branch/leaf 子集。
- structural legality：online-seen、非 root、完整 Published source、存在缺完整 chain 的 target、目标未被 equal/deeper in-flight proactive chain 覆盖。online-seen 不等于当前可执行。
- source 为完整 holders 中最小 Pod ID。structural targets 是尚未完整持有且不被 in-flight 覆盖的其他 Pods；对每个 structural target 用当前 Published Cache、Temporary、pins、committed-hit protection、leaf-LRU 和 full-wire pages 做无副作用 capacity preflight，再从 feasible targets 中按 `(Load, Pod ID)` 唯一选择。不得加入 affinity/busy/未来/queue tie-break。
- 同 target 的 in-flight d 覆盖 c 当且仅当 c 是 d 的 Prefix（含相等）；浅 d 不阻止更深 c。过滤在 Top-K/quantile 前。
- capacity 不删除 structural chain，也不改变 chain score、ranking、Top-K 或 Recency quantile；它只决定该 chain 的 feasible target set。若 set 为空，记录 SKIP_NO_CAPACITY，继续下一个 chain。若非空，只 commit 已选 target 的预验证 plan。所有未选 target probe 不 eviction、不 reserve、不刷新 LRU、不改 Load/history/records。
- 每 completion 已有的一次 opportunity 执行 policy，最多启动一个 proactive COPY；成功后停止扫描。复制使用 A 的 source pin、全 wire Temporary、独立25GB/s、ready原子发布/dedup/LRU；不改 Load，不生成 request/opportunity/demand/reuse。
- 相同 timestamp 仍先发布全部完成 transfer（包含 proactive），再稳定恢复 reactive requests，最后逐个 arrivals；只有 external request completion 产生机会。

### History 与政策

- ExternalDemandHistory 在 ARRIVAL 用原 arrival time 注册每个完整页 ancestor，每 request/chain 一次。Persistence `(t-h,t]`，只保留 count>0；count 降序、depth 降序、chain_id 升序，Top-K=10。h 仅60s或300s，Line7固定300s；K不是 action cap。
- 独立 FutureDemandIndex 只保存 external arrival 与完整页 Prefix；Oracle `F(c,t,W)` 是 `(t,t+W]` demand count，W=300s。只对当前 structural candidates 打分，正分全部排序，无固定K；不读未来 routing/Load/Cache/eviction/transfer/result。它是 global future-demand reference，不是 future-placement oracle或全局最优上界。
- visibility_end_ms=3537000。若 t+W>visibility，Oracle opportunity 不执行决策，记 FUTURE_WINDOW_CENSORED，不把未知当0；history-only 策略不因此停止。
- ReuseHistory 在 abstract completion 按实际 hit 的 full-page ancestors 各记录一次，用 completion time。若用 tokens 表示实际 hit H，则 full-page depth 为 floor(H/512)；本地 partial tail 不注册为可搬 chain。reactive 必须 ready并完成才有 reuse；cold suffix/arrival/copy/probe/ready/duplicate commit 均不产生 reuse。
- Recency `sum exp(-(t-ti)/60000ms)`，只对 structural 且正分集合计算 q=.9。确定性 quantile：排序 x，位置 `(n-1)*.9`，相邻两点线性插值，单点返回自身。score>=threshold 入 shortlist，score/depth/id 排序；capacity不参与分位数。
- Cost-aware 严格复用 Persistence count Top10 及其顺序，只作 Score>0 gate，不按 weighted benefit 或 cost Score 重排。history bucket 为 `<5000`、`[5000,20000)`、`[20000,60000)`、`[60000,120000)`、`[120000,300000]`、`>300000`，乘子为1.3/2.5/5.3/8.9/11.3/15.2。
- benefit=`sum_b (W/h)*n_history_b*weight_b`；congestion=`Lt/max(L)`（全零则0）；transfer_cost=`wire_bytes/25e9` seconds；Score=`benefit-.5*congestion-.1*transfer_cost`。无 references、归一化或 future action value。主配置最低合法 Score>=0.76564865024>0，执行等价及诊断验收按第15节。

### 配置、审计与交付

仅允许 NONE/FUTURE_DEMAND/PERSISTENCE/RECENCY/PERSISTENCE_COST_AWARE；R_REQ_KV_TASK 只能 NONE，主动策略只能 R_AFF。NONE 保留 A 行为。配置拒绝旧 service/queue/O2/ECT/tick/budget/references/NodeGate/timeout/control 字段。

Opportunity 保留 A 字段，扩展 policy、structural/positive/shortlist/attempted count、选中 chain/source/target/transfer、final_status。成功动作记录 action/opportunity/request ID、policy、decision/start/ready、chain/depth/source/target、policy_score 与各 nullable 诊断、wire、transfer、split、status。只保存 shortlist/attempt audit 及 structural rejection 计数，不倾倒完整 candidate universe。标准执行原因：ZERO_SCORE、NO_SOURCE、NO_TARGET、INFLIGHT_COVERED、SOURCE_STALE、SKIP_NO_CAPACITY、FUTURE_WINDOW_CENSORED；额外 NOT_ATTEMPTED/NOT_ATTEMPTED_CAP_REACHED/COST_GATE_REJECTED/NONE 表示未尝试、cap已满足、gate拒绝或无策略。

Stage B 报告为 `docs/analysis/task_main_stage_b_implementation.md`；合成测试通过后可做小型 correctness smoke，不形成策略优劣结论，不运行正式14行或最终指标。完成后停止等待 Stage C。


## 18. Stage C 最终指标与验证（2026-09-17 冻结）

本节授权 metrics、validation、reporting、正式配置准备及独立 pilot；不授权正式14行，不改变 A/B 状态演化。C 配置版本为 TASK_MAIN_V1_STAGE_C。正式从0连续处理所有输入，Evaluation arrival cohort=[1500000,2700000)ms，visibility=3537000ms；不得在25min重置任何状态。Pilot 仅将 Evaluation 改为[300000,600000)ms、visibility=960000ms，处理所有 arrival<960000ms（包含完整 observation tail），排空已启动传输。其余行为参数与正式配置相同。

### 请求收益与六桶

Saved Prefill Tokens 每请求只加一次 final_hit_tokens（最长实际命中的 valid tokens，包括实际 partial tail），hit+miss=input。total_input_tokens、request_hit_count、request_count 同一 arrival cohort；token_hit_rate=saved/input，request_hit_rate=hit_count/request_count。空分母为 NA（JSON null，CSV NA），不补0。

六桶 B1<5000、B2[5000,20000)、B3[20000,60000)、B4[60000,120000)、B5[120000,300000]、B6>300000；乘子依次1.3/2.5/5.3/8.9/11.3/15.2。每桶保留 count/input/saved/hit_count/两项hit rate/weighted_saved_tokens，空桶保留。bucket count/input/saved 必须分别守恒。Multiplier-weighted Saved Tokens=sum(bucket_saved*multiplier)，使用 math.fsum 汇总各桶贡献；这是 benefit proxy，不是 TTFT、模拟延迟或实测毫秒。

### request-distribution skew ratio

按 arrival 将请求分入固定互不重叠300000ms窗口，以 final_pod 计 N 维请求数量，包括零请求 Pods；sum(counts)=window arrivals。正式四窗25–30/30–35/35–40/40–45min；pilot 仅5–10min一窗。population Gini 全零为0。

每窗口 uniform random Pod assignment，200 repetitions，seed=20260911。随机 baseline 缓存 key=(workload,start,end,N,request_count,seed,repetitions)。每个 key 使用独立 Python random.Random(seed) 从头生成200组逐请求 randrange(N) 分配；不依赖策略或调用顺序。保存 mean、P95（线性插值）、actual_gini、skew_ratio=actual/random_mean；random_mean=0 时 ratio=NA，不加epsilon。

主表分别取 actual、random_mean、random_p95、window ratios 的中位数；ratio排除NA，不取中位数之比。保存 valid_ratio_window_count、total_window_count 和全部明细。四窗不是四次独立实验，不支持显著性声明；请求分布指标不直接称计算负载不均。

### proactive wasted-copy 与 generation 证据

只选择 start_time 位于同一 Evaluation 区间的 proactive COPY。观察窗为(ready,ready+300000]；先按 ready+W<=visibility 判完整观察，超出者无论是否提前使用均 CENSORED。完整窗内，真实请求最终在 target、实际 hit 覆盖完整 copied chain，且每一页仍为 ready 时对应 residency generation，才为 USED，否则 UNUSED。ready瞬间不计使用，右端点计入。仅 future demand、其他Pod、partial hit、evict/reinsert新generation不算旧action USED。

ready 记录 chain generation vector；真实 cache reuse 记录 target/request/time/path/generation vector；这些是只读观察，不刷新LRU、不改Cache。wasted_copy_ratio=sum(UNUSED wire_tokens)/sum(USED或UNUSED wire_tokens)。整动作按 wire tokens 加权，不新增逐页 attribution。CENSORED 排除 waste 分子分母但保留实际网络成本。输出同一cohort的 proactive/observed/used/unused/censored counts、total/observed/censored wire tokens、observation_coverage=observed/total。空分母NA，Lines1/2 waste与coverage均NA。该指标解释为“W内未获整链实际复用的主动复制体量比例”，不否认部分复用价值。

### 网络与相对差值

reactive/proactive/total 分别报 successful started count、completed count、wire pages/tokens/bytes。统一按 start_time 归 PRE_EVAL[0,eval_start)、EVAL[eval_start,eval_end)、TAIL[eval_end,visibility)，另报 FULL_RUN。跨阶段 ready 不改所属阶段；这是 start-attributed cost，不意味着全部字节都在阶段墙钟时间内发送。censored cost不消失。

每workload自动以Line2为baseline：saved、weighted saved、request/token hit rate、skew、reactive/proactive/total wire bytes、transfer started count的绝对delta；hit rates另报百分点delta，不制造零/NA baseline百分比。Oracle reference delta 单独输出Line3-Line2及六桶saved/weighted差值、Line3 waste自身值（Line2为NA）；不称global optimal headroom。

### Hard gates、输出与范围

Gate A：Line5/7所有行为配置相同，允许policy、评分诊断及非行为line_id来源字段不同。执行投影比较逐请求assignment/hit、逐opportunity/shortlist/skip、COPY顺序及wire/time、evict/publish行为；另建 logical_state_projection 包含Cache/LRU、Load、candidate universe、demand/reuse histories和in-flight，排除policy/路径/provenance/评分诊断/report-only指标。execution hash及final logical state hash须相同，非诊断系统指标须相同。Line7全部合法shortlist评分分解完整、Score>0、cost_gate_rejected_count=0。

Gate B：在实际 FutureDemandIndex.count API 内记录决策上下文读取次数；分别统计 Oracle decision reads 和 history-policy decision future reads。读入完整trace/index construction不计；post-hoc observation不在决策上下文，不计。Oracle允许>=0，其他policy必须0，否则invalid。

正式及pilot各14份配置保存版本/workload/line/policy/routing/冻结参数/split/seed/repetitions/trace路径与SHA256。summary记录实际git dirty状态及工作区模块哈希，不将未提交源码冒称旧commit。生成两个workload七行主表、merged strategy×workload、baseline delta、Oracle reference delta、bucket、Gini window、transfer cost、waste共九类表，CSV/JSON均保留W/h/K/q/theta/N/action参数。

Stage C只运行独立results/task_main/stage_c_pilot/的14项pipeline pilot，各重复两次验证确定性。全部A/B及C测试、守恒、依赖隔离、Gate A/B、14项pilot须通过才可报告PASS；任一失败停止报告。数字仅验证pipeline，不调参、不形成性能结论、不运行正式14行/sweep/O2/SR/P3。交付报告docs/analysis/task_main_stage_c_implementation.md后停止，等待正式实验授权。


## 19. C1 输入职责修复（2026-09-17 用户确认）

Pilot和Formal统一分离 ReplayRequests=`arrival < visibility_end_ms` 与 OracleObservationRequests=`arrival <= visibility_end_ms`。只有前者进入ARRIVAL、Cache、Load、online candidates、demand/reuse histories、routing、transfer、request/opportunity records及系统状态演化。后者仅供FutureDemandIndex；恰好visibility_end的请求为observation-only，不能创建future-only candidate或任何请求执行状态。

查询仍为(t,t+W]，t排除、t+W包含；t+W>visibility仍为FUTURE_WINDOW_CENSORED。Pilot visibility保持960000，Formal保持3537000，不改W、端点、candidate或score。summary provenance分别保存replay/observation/boundary-only counts、两个时间规则及输入身份hash。history policies不构建或接收future index，输入加载/索引构建不计decision future reads。

修复后从头重跑全部14项pilot，每项双遍，写新目录results/task_main/stage_c_pilot/run_20260917_02/。旧INVALID manifest、C1证据和两条baseline完整保留；不得把旧结果改成PASS或复用进新矩阵。全tests、真实边界回归、Gate A/B、14项pilot均通过才可更新Stage C PASS。任一失败停止报告；无论结果如何，本阶段不运行正式14行。

## 20. Stage D2 MOVE ablation（2026-09-18 冻结）

Stage D2 只比较 A_COPY 与 A_MOVE，不改变 trigger、placement、Cache、Load、history、opportunity、wire 或正式指标语义。MOVE 定义为 **COPY-THEN-SAFE-RELEASE**：决策、source、feasible-target-first、target `(Load, Pod ID)`、FULL_PAGE_ONLY full-chain wire、Temporary、25GB/s、ready 发布及 target LRU 与 COPY 完全相同；唯一差异是在 target 成功 commit/dedup 后尝试释放 source 的安全连续 suffix。MOVE 不改变 Score/ranking、W/h/K、capacity 或 action cap。

MOVE start 后到 ready 前，source chain 保持 Published、可命中，并由本次 transfer pin 保护；pin 不刷新 LRU、不产生 reuse/history。target 只占 Temporary，ready 前不可见。完成顺序严格为 target commit/dedup并可见、解除本次 source transfer pin、按 ready 时真实状态执行 safe release、更新 MOVE records；这些步骤先于同 timestamp external arrival。target commit 失败时不释放 source，清除本次临时保护，不采用“先删后 rollback”。

start 时记录 source chain 每页 residency generation。ready 时从 chain leaf 向 root 逆序检查；页必须仍 resident、generation 相同、无其他 active pin、无 committed-hit protection且为当前 Published leaf，才可删除。删除一页后继续 parent；遇到 NOT_RESIDENT、GENERATION_CHANGED、ACTIVE_PIN、COMMITTED_PROTECTION、SHARED_OR_NONLEAF 即停止。release 是 moved chain 的最大安全连续 suffix；不删除 subtree、共享 ancestor或 moved chain 之外的页，不刷新剩余 ancestor LRU，不计入普通 leaf-LRU eviction records。全部 D 页释放为 FULL_RELEASE；释放部分为 PARTIAL_RELEASE；0页为 ZERO_RELEASE。candidate legality和target ranking不得依赖 predicted/actual releasable pages。

decision time 可只读记录 predicted_releasable_pages；最终以 ready 时 `actual_source_released_pages` 为准。记录 source generation vector、released pages/tokens/bytes、release fraction、status和stop reason，并独立汇总 move source release events/pages。MOVE 不改 Load，不产生 external demand、ReuseHistory或额外 opportunity；每 external request仍只有一个 opportunity且成功主动动作至多一个。source release带来的容量只在 ready 后生效，不能提前用于任何 preflight。

D2共同 target-side 利用指标为 `unused_transfer_wire_ratio`，沿用 Stage C `(ready,ready+W]`、target final execution、完整 chain hit及target generation一致的 USED/UNUSED/CENSORED规则。MOVE另报同值的 `unused_move_wire_ratio`；不改写正式 COPY artifact 的 `wasted_copy_ratio`。source release与ordinary eviction/churn分开报告。

实验仅包含 R_AFF、N=4、585 pages/Pod、theta=2、W=300s、25GB/s、FULL_PAGE_ONLY、action cap=1 下的 Future-Demand Oracle 与 Persistence h=300s/K=10，两个 workload 共4个 MOVE case、每项双遍。COPY直接引用冻结正式 Lines3/5，前提是逐文件源码、配置、trace、cohort及全部行为参数身份通过；D2的 COPY mode必须在 execution projection 上与冻结引擎一致。不得复用旧T2/O2/MOVE_SAFE结果，不运行其他策略、参数点、MOVE变体、granularity或predictor。
