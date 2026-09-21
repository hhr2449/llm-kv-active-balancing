# TaskMain Formal Result Diagnostics V1

日期：2026-09-17。仓库：`/root/data2/llm-kv-active-balancing`。

**状态：PASS。** 本轮只读分析最终正式结果，没有调用 simulator、修改协议/配置/策略，也没有重跑正式 14 行或运行 sweep。诊断重新读取 `run_1` 原始 records；`run_2` 由正式 manifest 的逐文件哈希一致性覆盖。共重建 12,650 条 Line 1/2 Evaluation 请求和 58,754 条 Lines 3–7 Evaluation-started proactive actions。诊断自身的守恒与身份检查全部通过。

| 身份项 | 冻结值 |
|---|---|
| formal run | `results/task_main/formal`；`formal:5ddeca8e0c768e04` |
| protocol | `TASK_MAIN_V1_STAGE_C` |
| Evaluation arrival cohort | `[1500000,2700000)` ms，即 `[25,45)` min |
| formal validation | PASS；28/28 replay、14/14 cases、双遍哈希一致、Gate A/B PASS |
| source identity | 工作区模块 SHA-256 集合 `5ddeca8e0c768e04b2813a0eedf5ec9db0363b48f8aaa183de57f800d57b3e30`；不是仅凭 Git HEAD 代表源码 |
| Git HEAD | `ec204c892bb494e54db8a0be81b5d4a775361daf` |
| Conversation trace SHA-256 | `b8cbb061a85206d729d91cdc2981f43c9e0d99209dce588d3af5f7934408b9df` |
| ToolAgent trace SHA-256 | `48a2db1a13d3bc05e6330140c64f604ba366df20d3c9e128b5c35a01c1fa5f71` |

诊断身份见 [`run_identity.json`](../../results/task_main/formal_diagnostics_v1/run_identity.json)，自检见 [`diagnostics_validation.json`](../../results/task_main/formal_diagnostics_v1/diagnostics_validation.json)，全部产物哈希见 [`diagnostics_manifest.json`](../../results/task_main/formal_diagnostics_v1/diagnostics_manifest.json)。

## 1. R_REQ_KV strong baseline 的来源

Line 2 的 Evaluation wire 为零表示 **Evaluation 内确实没有 reactive transfer start**，不是 phase attribution 把传输移走。两个 workload 的 Evaluation 请求全部走 `gate=false -> stay source`，没有 direct target、COPY、capacity fallback 或未分类路径；分类互斥且总和守恒。

| Workload | Eval requests | gate=false stay source | gate=true | Eval reactive starts/completes | Eval wire pages/tokens/bytes | Full-run starts/completes | Full-run wire pages/tokens/bytes |
|---|---:|---:|---:|---:|---:|---:|---:|
| Conversation | 4,275 | 4,275 | 0 | 0 / 0 | 0 / 0 / 0 | 7 / 7 | 7 / 3,584 / 102,760,448 |
| ToolAgent | 8,375 | 8,375 | 0 | 0 / 0 | 0 / 0 / 0 | 9 / 9 | 42 / 21,504 / 616,562,688 |

Full-run 的 16 次 COPY 全部发生在 Evaluation 前：Conversation 七次均从 `t=0` 开始；ToolAgent 六次从 `t=0`、三次从 `t=3000ms` 开始。因此 Line 2 在 Evaluation 内不再 reroute，请求级 reroute 表为空。Line 2 与 Line 1 的差别来自这些早期 COPY 已改变后续 Cache、affinity source 和请求分配的闭环演化。

| Workload | 相同 final Pod | 不同 final Pod | `delta_saved>0` | `=0` | `<0` | Line2-Line1 Saved |
|---|---:|---:|---:|---:|---:|---:|
| Conversation | 977 | 3,298 | 102 | 4,173 | 0 | +878,817 |
| ToolAgent | 2,683 | 5,692 | 91 | 8,217 | 67 | +821,625 |

这里的 per-request 对齐是整体策略结果的描述。两行各自演化 Cache 状态，不能把某一请求的差值解释成单次 reroute 的独立因果效应。明细见 [`line2_routing_decomposition.csv`](../../results/task_main/formal_diagnostics_v1/line2_routing_decomposition.csv) 和 [`line1_vs_line2_request_comparison.csv`](../../results/task_main/formal_diagnostics_v1/line1_vs_line2_request_comparison.csv)。

**Q1 答案：** zero-wire 的直接原因是 Evaluation 内 12,650 个 Line 2 请求全部 gate=false，真正没有传输启动。Line 2 的强 baseline 来自 Evaluation 前 7/9 次 reactive COPY 所造成的持续状态差异，而非 Evaluation 内的免费迁移。

## 2. WeightedSaved 的 Prompt-length 来源

`WeightedSaved` 是 Prompt bucket multiplier 加权的 token benefit proxy，不是实测 TTFT 或毫秒。相对 Line 2，主要正向贡献如下；百分比只在总 delta 为正时计算，负 bucket 仍以原始 delta 保留。

| Workload / 策略 | ΔSaved | ΔWeightedSaved | ΔSaved 最大来源 | ΔWeighted 最大来源 | Top-2 ΔWeighted 来源及合计 | 60k+ ΔWeighted | 120k+ ΔWeighted |
|---|---:|---:|---|---|---|---:|---:|
| Conversation Oracle | +1,750,228 | +9,733,907.6 | 60–120k：+607,744 | 60–120k：+5,408,921.6 | 60–120k + 20–60k：+8,331,468.8（85.6%） | +5,408,921.6 | 0 |
| Conversation Persistence300 | +325,636 | +2,391,819.6 | 20–60k：+235,520 | 60–120k：+1,257,676.8 | 60–120k + 20–60k：+2,505,932.8；低桶抵消一部分 | +1,257,676.8 | 0 |
| Conversation Recency | +137,428 | +1,164,973.2 | 60–120k：+108,032 | 60–120k：+961,484.8 | 60–120k + 20–60k：+1,148,723.2 | +961,484.8 | 0 |
| ToolAgent Oracle | +1,822,461 | +9,523,187.3 | 20–60k：+648,704 | 60–120k：+4,511,232.0 | 60–120k + 20–60k：+7,949,363.2（83.5%） | +4,511,232.0 | 0 |
| ToolAgent Persistence300 | +113,434 | +387,573.8 | 20–60k：+131,072 | 20–60k：+694,681.6 | 20–60k + 5–20k：+775,496.6；60–120k 为 −378,214.4 | −378,214.4 | 0 |
| ToolAgent Recency | +59,041 | +758,541.3 | 60–120k：+105,984 | 60–120k：+943,257.6 | 60–120k + 5–20k：+1,026,902.6；其他桶抵消 | +943,257.6 | 0 |

Conversation Oracle 的 60–120k bucket 只占 ΔSaved 的 34.7%，却占 ΔWeighted 的 55.6%；ToolAgent Oracle 对应为 27.8% 和 47.4%。这是 WeightedSaved 增长快于 Saved 的主要算术来源。当前正式 cohort 的 120k+ buckets 对这些策略相对 Line 2 的 delta 均为零，不能把增益外推到超长 Prompt。

Line 4 的总收益为负，因此报告只保留各 bucket 原始 delta，不制造“占负总量比例”。六桶、七行、两个 workload 的完整数据见 [`bucket_delta_decomposition.csv`](../../results/task_main/formal_diagnostics_v1/bucket_delta_decomposition.csv)；贡献摘要见 [`weighted_saved_contribution.csv`](../../results/task_main/formal_diagnostics_v1/weighted_saved_contribution.csv)。

**Q2 答案：** Oracle 的普通 Saved 由 5–120k 多桶共同贡献，而其 WeightedSaved 主要集中在 20–120k，尤其 60–120k。Conversation 的 Persistence300/Recency 同样由长 Prompt 主导；ToolAgent Persistence300 主要改善 20–60k，并在 60–120k 退化，ToolAgent Recency 的净 weighted gain 则主要由 60–120k 抵消其他桶损失。

## 3. 约 98% Waste 的组成

正式 Waste 定义没有改变：只看 Evaluation-started proactive action；完整 chain 在 `(ready, ready+W]` 被同 target、同 generation 的实际请求完整命中才是 `FULL_CHAIN_USED`。全部 action 的 `ready+W` 都在 visibility 内，重新验证 `CENSORED=0`。

| Workload / Line | 正式 Waste | FULL_CHAIN_USED actions | FULL_CHAIN_UNUSED actions | UNUSED 中 NO_REUSE | UNUSED 中存在 partial Prefix reuse |
|---|---:|---:|---:|---:|---:|
| Conversation L3 Oracle | 95.727% | 209 | 4,066 | 0 | 4,066（100.0%） |
| Conversation L4 Persist60 | 99.718% | 29 | 4,197 | 7（0.17%） | 4,190（99.83%） |
| Conversation L5/L7 Persist300 | 98.205% | 94 | 4,181 | 0 | 4,181（100.0%） |
| Conversation L6 Recency | 97.991% | 86 | 3,353 | 0 | 3,353（100.0%） |
| ToolAgent L3 Oracle | 96.318% | 556 | 7,807 | 268（3.43%） | 7,539（96.57%） |
| ToolAgent L4 Persist60 | 98.599% | 587 | 7,515 | 665（8.85%） | 6,850（91.15%） |
| ToolAgent L5/L7 Persist300 | 97.883% | 432 | 7,903 | 165（2.09%） | 7,738（97.91%） |
| ToolAgent L6 Recency | 97.830% | 256 | 4,873 | 331（6.79%） | 4,542（93.21%） |

这里的 partial 指 **observed chain-residency reuse**，不是 “causal saved tokens from the copy”。许多 action 只观察到根部少量 Prefix；ancestor 也可能在 copy 前已经存在。即便如此，generation ledger 还允许检查 ready 时真正新增页面的后续实际命中：new-page observed use ratio 仅为 Conversation Oracle/P60/P300/Recency 的 4.34%/0.36%/1.95%/1.82%，以及 ToolAgent 的 3.84%/1.69%/2.42%/2.29%。

Wire 守恒在每条 action 上成立：`wire_pages = newly_published_pages_at_ready + deduplicated_pages_at_ready`。按 tokens 汇总，ready 时重复传输后被 dedup 的比例如下：

| Workload | Oracle | Persist60 | Persist300 / Cost-aware | Recency |
|---|---:|---:|---:|---:|
| Conversation | 6.27% | 4.85% | 12.17% | 11.77% |
| ToolAgent | 6.73% | 3.79% | 12.66% | 10.86% |

这不是全部 Waste 的主量级：正式 Waste 约 96–99.7%，ready-time dedup 约 3.8–12.7%。当前 artifacts 没有 copy start 时逐页 target snapshot，因此 `target_preexisting_pages_at_start` 标为 `UNSUPPORTED_BY_CURRENT_ARTIFACTS`；报告只使用可靠的 ready-time dedup。逐 action 证据见 [`copy_action_diagnostics.csv`](../../results/task_main/formal_diagnostics_v1/copy_action_diagnostics.csv)，正式三分类见 [`formal_waste_classification.csv`](../../results/task_main/formal_diagnostics_v1/formal_waste_classification.csv)。

**Q3 答案：** 约 98% 的 full-chain Waste 几乎都不是“完全零 Prefix reuse”。Conversation 的 unused actions 中 99.83–100% 有 partial reuse；ToolAgent 为 91.15–97.91%。但这些 partial reuse 通常很浅，且新增页面真实使用率只有 0.36–4.34%，所以不能据此否定正式 full-chain Waste。

**Q4 答案：** 3.79–12.66% 的 wire pages 在 ready 时没有形成新发布页，而被当作已有等价页 dedup。该比例显著小于正式 Waste，说明重复 wire 是成本的一部分，却不足以单独解释约 98% Waste。

## 4. Prefix depth、wire size 与 partial reuse

长 chain 的 partial fraction 明显变小，但正式 Waste 并不随 depth 单调上升。以 fixed depth buckets 为例：

- Conversation Oracle 的 Waste 从 1–4、5–16、17–64、65–128、129–256 pages 依次为 94.6%、95.2%、94.9%、97.8%、96.0%。
- ToolAgent Oracle 对应为 97.0%、86.3%、97.3%、98.4%、97.0%；5–16 pages 是明显的高复用带。
- ToolAgent Persist60 的 5–16 pages Waste 为 81.7%，但 1–4、17–64、65–128、129–256 均为 99.7%、99.7%、100%、99.9%。
- 所有策略在 65+ pages 上几乎都达到约 98–100% Waste；同时 median observed partial fraction 通常只剩约 0.5–1.1%。

因此证据支持“很深的 chain 很难被完整复用”，不支持“每增加 depth，Waste 都显著且单调上升”。短 chain 也可能高 Waste，中等的 5–16 pages 在 ToolAgent 反而最好。完整 fixed buckets 和 quantiles 见 [`copy_depth_summary.csv`](../../results/task_main/formal_diagnostics_v1/copy_depth_summary.csv)。

Wire 成本高度集中于大动作。各策略最大的 10% actions 占总 proactive wire 的约 35.9–43.5%，其中 Persist60 为 Conversation 36.9%、ToolAgent 42.4%；这些大动作的 Waste 常在 98–100%。按 action 数等分的 wire quartiles、Top 1/5/10% 对总 wire、used wire、unused wire 的占比见 [`copy_wire_size_summary.csv`](../../results/task_main/formal_diagnostics_v1/copy_wire_size_summary.csv)。

所有策略首次 **任意 Prefix** reuse 的中位时间都约为 ready 后 3 秒，P75 多在约 6 秒，说明 shallow reuse 很快出现。完整 chain use 的中位时间更长：Conversation Oracle/P60/P300/Recency 为约 6.0/9.0/12.0/12.0 秒；ToolAgent 为约 6.0/3.0/3.0/6.0 秒。Persist60 并未表现为“成功复用发生得更晚”，其问题是大多数 chain 从未完整复用。数据见 [`time_to_use_summary.csv`](../../results/task_main/formal_diagnostics_v1/time_to_use_summary.csv)。

复用也很集中：按 full-chain reuse event 排序，Top 5% actions 在 Conversation 各策略覆盖 100%，ToolAgent 覆盖 93.5–100%；这是 full-chain-used action 本身极少的另一种表达，不是 per-action causal Saved。见 [`reuse_concentration.csv`](../../results/task_main/formal_diagnostics_v1/reuse_concentration.csv)。

**Q5 答案：** 长 chain，特别是 65+ pages，确实更难完整复用且 partial fraction 更低；但 fixed buckets 非单调，不能声称 Waste 随 depth 统计显著地单调上升。更准确的结论是存在 workload-specific 的中短链高复用带，而大链贡献了不成比例的 unused wire。

## 5. Persistence60、Persistence300、Recency 的差异与 Q6–Q7

| Workload / 策略 | Eval actions | Eval / full-run wire TB | depth mean / median | full-chain action use | formal Waste | ready dedup | Eval all-cause evicted pages | Eval admission / no-capacity skips |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Conversation P60 | 4,226 | 3.497 / 10.671 | 56.4 / 22 | 0.686% | 99.718% | 4.85% | 312,794 | 80 / 523 |
| Conversation P300 | 4,275 | 1.802 / 5.311 | 28.7 / 18 | 2.199% | 98.205% | 12.17% | 196,207 | 0 / 0 |
| Conversation Recency | 3,439 | 1.357 / 2.830 | 26.9 / 15 | 2.501% | 97.991% | 11.77% | 170,317 | 0 / 0 |
| ToolAgent P60 | 8,102 | 5.562 / 16.419 | 46.8 / 16 | 7.245% | 98.599% | 3.79% | 444,821 | 313 / 3,787 |
| ToolAgent P300 | 8,335 | 3.193 / 9.596 | 26.1 / 16 | 5.183% | 97.883% | 12.66% | 276,988 | 22 / 503 |
| ToolAgent Recency | 5,129 | 2.065 / 4.440 | 27.4 / 16 | 4.991% | 97.830% | 10.86% | 213,833 | 1 / 0 |

P60 的 action 数并不高于 P300；它主要复制了更深的 chain，使 Eval wire 增至 P300 的 1.74–1.94 倍、Recency 的 2.58–2.69 倍。它的 ready-time redundancy 反而最低，因此失败不能归因于“更高 wire redundancy”。Conversation 中 P60 同时有最低 full-chain use；ToolAgent 中 P60 的 action-level full-chain use 反而最高，但被使用的 chain 较短、未使用的深 chain 消耗大量 wire，所以 wire-token-weighted Waste 仍最差。

P60 还伴随最高的 all-cause eviction、prompt admission skip 和 candidate no-capacity skip，表现出最强的容量压力。当前 eviction ledger 只有时间、block、generation，没有 initiating operation；同一时刻 prompt admission 与 transfer 可重叠。因此下列三项均为 `UNSUPPORTED_BY_CURRENT_ARTIFACTS`：

- proactive-induced eviction events/pages；
- proactive evicted pages per action；
- future demand after proactive eviction。

不能把上表 all-cause churn 写成 proactive COPY 的因果 eviction。完整数据与限制见 [`persistence_recency_comparison.csv`](../../results/task_main/formal_diagnostics_v1/persistence_recency_comparison.csv) 和 [`cache_churn_summary.csv`](../../results/task_main/formal_diagnostics_v1/cache_churn_summary.csv)。Line 7 与 Line 5 的 execution/system results 完全相同，保持各自真实 policy metadata；诊断没有重复解释成独立现象。

**Q6 答案：** Persistence60 失败主要表现为更深 chain、约两倍于 P300 的 wire、较低的新页实际使用率，以及更强的容量压力和 all-cause churn。它不是因为比 P300 启动更多动作，也不是因为更高 ready-time redundancy；ToolAgent 也不支持“action-level future reuse 一定更低”，真正恶化的是深链 unused wire 的权重。

**Q7 答案：选择 C，两者都重要。** Oracle 相对历史策略仍取得最大的 Saved/WeightedSaved 增益，说明更好的热度预测具有直接价值；同时 Oracle 自身仍有 95.7–96.3% formal Waste，绝大多数 unused action 又观察到浅层 partial reuse，而新发布页使用率只有约 3.8–4.3%，说明整链 copy granularity 与实际复用深度明显不匹配。现有证据支持下一阶段同时研究预测与 Prefix copy granularity；其中 granularity 对降低 wire Waste 的证据更直接，预测对提高命中收益的证据更直接。本轮不据此修改策略或参数。

全部 CSV/JSON 位于 [`results/task_main/formal_diagnostics_v1/`](../../results/task_main/formal_diagnostics_v1/)。分析程序 [`analysis_program.py`](../../results/task_main/formal_diagnostics_v1/analysis_program.py) 只读取 frozen formal artifacts，便于复核本报告；它不是 simulator 入口。
