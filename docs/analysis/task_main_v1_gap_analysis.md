# TaskMain v1 Gap Analysis

审计日期：2026-09-17。审计对象：`/root/data2/llm-kv-active-balancing` 的**当前工作区文件**。唯一规范来源是本次用户贴入的 TaskMain v1 冻结协议；历史 `TaskMain-v1.1`、O1/O2、Strong Reactive、P3/P3.2 文档不构成本次协议的补充约束。

**结论：当前实现不能直接运行并宣称符合 TaskMain v1。** 虽然已有两组各七行的配置，核心仍是 FCFS Prefill simulator：服务时间影响 Load、routing、Cache 发布时间和实际命中；主动决策使用 1 秒 tick 与 byte budget；Recency、Cost-aware、candidate space 和主指标均有实质差异。不能仅替换报表或修改几个 YAML 参数解决。

本轮仅静态阅读源码、配置、测试及相关历史说明，并只读统计 trace 的时间边界与请求规模；没有调用 simulator、运行测试、执行正式/开发实验、训练模型、生成 Oracle labels 或重算历史结果。唯一新增仓库文件为本文。所有代码、配置、旧报告及旧结果保留。

## 审计范围与当前入口

HEAD 为 `ec204c892bb494e54db8a0be81b5d4a775361daf`，工作区已有未提交修改，尤其包括 `config.py`、`engine.py`、`metrics.py`、`oracle.py`、`pressure.py`、`strategies.py`。因此本文行号和结论对应工作区，不能只用 HEAD 复现。

完整审阅的主执行路径包括 `src/simulator/{config,engine,events,trace,cache,pod,routing,service,transfer,tickets,oracle,strategies,metrics,pressure,reactive_ect}.py`，以及 `step1_attribution.py` 中可能被误复用的 Gini、bucket、copy 指标。逐项检查两 workload 的全部 14 份主配置、历史 formal TaskMain 配置、主运行脚本、主汇总脚本、consolidator、TaskMain 开发校准/验证脚本和 sweep 汇总实现；对其他配置目录进行清点与配置解析。阅读相关 Cache、routing、transfer、Oracle、proactive、TaskMain、split、metrics 测试，核查其约束是否仍适用。

对 O2/P3 扩展核查入口与依赖边界，对历史 O1 routing/timing audit 核查 generation、ready-window 处理；不把这些扩展的研究目标、离线标签或模型评价搬入 TaskMain。本文不是对无关 learned-model 算法内部的重新审计。

关键调用链：

```text
scripts/taskmain_v1.1/run_{conversation,toolagent}_main.sh
  -> python -m src.simulator.engine
  -> SimulatorConfig.from_yaml + load_trace
  -> SimulatorEngine.run
     -> Pod + PrefillServiceModel + PrefixCache
     -> R_AFF / b0_arrival(R_REQ_KV_TASK)
     -> TRIGGER_TICK -> CandidateGenerator -> strategy -> COPY
     -> summarize + pressure_diagnostics + proactive action summaries
  -> summarize_{conversation,toolagent}_main.py
```

| 被审计功能 | 当前实际实现位置 |
|---|---|
| R_AFF | `routing.py:17–33`：最长 Prefix、`Pod.load_ms`、Pod ID |
| R_REQ_KV_TASK | `engine.py:533–582` arrival；`1508–1619` admission/replan；`strategies.py:94–98` gate |
| Oracle | `oracle.py:16–38` future demand index；`engine.py:892–905,947` 查询与排序 |
| Persistence | `strategies.py:26–61` 在线 external history；`engine.py:906–909,947–952` |
| Recency | `strategies.py:63–69` external-demand 衰减；`engine.py:910–913,967–974` |
| Cost-aware | `strategies.py:71–75,101–107`；`engine.py:915–965,1102–1119` |
| Candidates | `oracle.py:52–97`；`engine.py:878–925,1083–1087` |
| Cache / leaf-LRU | `cache.py:41–340` |
| proactive transfer | `engine.py:1047–1287` |
| reactive transfer | `engine.py:328–355,1345–1702`；`tickets.py`、`transfer.py` |
| 实际 hit 与 Prompt 发布 | `engine.py:1704–1751` 的 SERVICE_START / SERVICE_DONE |
| metrics / wasted copy | `metrics.py`、`pressure.py`；`engine.py:2002–2354`；两个主汇总脚本 |
| evaluation split | `config.py:13–41`、`trace.py:56`、`metrics.py:169–193` |

以上路径均相对 `src/simulator/`，脚本路径已单独标出。

只读 trace 元数据如下；这些是输入文件事实，不是实验结果：

| Workload | 请求数 | 首/末 timestamp（ms） | Evaluation 请求数 `[1500000,2700000)` | 最大 Prompt pages |
|---|---:|---:|---:|---:|
| Conversation | 12031 | 0 / 3536999 | 4275 | 247 |
| ToolAgent | 23608 | 0 / 3536999 | 8375 | 247 |

两条 trace 均包含大量同 timestamp 请求。主配置的 `visibility_end_ms=3537000` 比最后记录时间多 1 ms，不能不加说明地等同于末条请求时间；观察边界口径见 H。

## A. 可以直接复用的模块或原语

“可复用”限定到下表列出的功能，不代表整个旧模块/调用链符合 v1。

| 模块/原语 | 可复用内容 | 使用边界 |
|---|---|---|
| `TraceRequest` / `load_trace` | Mooncake 512-token page 映射、末页 valid tokens、稳定 request ID、非递减 arrival 校验、hash 的 parent/depth/valid-token 一致性检查 | 只使用 Prompt hashes；`output_tokens` 可保留元数据，不能构造 completion KV |
| `TraceRequest.hit_tokens` | 根据最长实际命中 Prefix 计算 valid tokens，末页不盲乘 512 | 必须用新抽象请求处理时的实际 hit |
| `SplitConfig` | 0/15/25/45 分钟边界、arrival 分类、半开 Evaluation cohort | 复用主 split；不迁移 S01 的另一个 split |
| `PrefixCache.lookup` | 最长连续 Prefix lookup；probe 不刷新 LRU | 非 set intersection；不暴露 Temporary |
| leaf-LRU 结构 | unpinned leaf 淘汰、子节点删除后父节点成为 leaf、确定性 heap tie-break、容量断言 | copy 新节点的 LRU timestamp 必须修正，见 C.6 |
| transfer Temporary 与 commit | 未完成副本不进入 Published Cache；完成时原子发布；source pin 不刷新 request recency | Temporary 占用规则和保护范围待 H 确认；不能连带继承 ActivePrivate/FCFS |
| `FutureDemandIndex.future_count` | 只记录未来 external arrival；精确 `(t,t+W]`，不包含 timestamp=t 请求 | candidate 必须由 online history 产生；不可改用 O2 action-value |
| `ExternalDemandHistory.persistence/window` | 每请求按包含的 Prefix 计一次；`(t-h,t]` 滑窗；按历史 Prompt 长度可计算 bucket 权重 | 仅供 Persistence / benefit；不再供 Recency |
| `task_weight` / `value_hat` | 六个乘子；`(W/h) * sum(history weights)` 与规定 benefit 等价 | 保留原始 benefit，不除 `V_ref`；bucket 边界约定见 H |
| `request_kv_task_gate` | 严格乘法比较 `Ls > theta*Lt`，避免 target=0 除零 | 传入值必须换为 60 秒 miss-token Load |
| `relative_load_penalty` | `max=0 -> 0`，否则 `0.5*Lt/max(L)` | 输出已含 0.5，不能再乘一次；换成新 Load |
| `deterministic_rank` / `quantile` | score/depth/id 排序；确定性线性插值 quantile | Recency 合法集合先修正；quantile 算法需记录 |
| `metrics.gini` | population Gini，保留所有 N 个 Pods，零总量返回 0 | 重新构建五分钟 request-count 输入及 random reference |
| generation 归因结构 | `(pod, block)` 当前 generation、ready、真实 hit、eviction 失效机制 | 补齐 W 截止、整链/部分使用口径和持久化明细；不是直接复用旧 wasted ratio |

## B. 需要修改或新增的模块

| 模块 | 必需变化 | 优先级 |
|---|---|---|
| TaskMain config / protocol validation | 建立独立 v1 入口与显式参数，禁止 service、O2、ECT、旧 Cost-aware、tick/budget 路径混入 | 阻断项 |
| 请求/event 执行层、Load、routing | 增加 60 秒已确定 miss tokens history；删除 v1 路径的 GPU 服务/FCFS/RemotePressure 因果依赖；明确逐请求完成与 proactive opportunity | 阻断项 |
| reactive Task 路径 | 使用新 Load 与规定 target；保留有限 wire ready；ready 后完成抽象请求并写 Prompt Cache | 阻断项 |
| candidate / in-flight 注册表 | 全部 online logical Prefix；按路径判定同 target 等价或更深 in-flight 覆盖 | 阻断项 |
| policy / scheduler | request 后 opportunity、cap=1、正确 Persistence/真实 reuse Recency/新 Cost-aware | 阻断项 |
| Cache / transfer | 真实 request-access LRU；统一容量与保护、失败 skip；去除 transfer 的额外 control latency | 阻断项 |
| request/action records / metrics | Saved/buckets/weighted、五分钟 random-normalized Gini、wire tokens、ready+W censored waste、分阶段成本与 baseline deltas | 阻断项 |
| batch / reports / provenance | 两 workload 分开，独立 14 行配置/输出；不复用历史结果值；明确新协议与工作区来源 | 正式运行前必需 |
| 合成测试 | 新语义断言和历史隔离检查；旧协议测试保留 | 正式运行前必需 |

这是未来实施清单，本次没有修改这些模块。隔离方案与建议文件见 F。

## C. 对照协议的差异清单

### C.1 基本配置、七行矩阵与协议入口

| 协议项 | 当前状态 | 差异/影响 |
|---|---|---|
| 两个 workload 分开各七行 | 已有 E01–E07 / E08–E14，主运行/汇总分开 | 行标签可作参考，历史结果不能作为 v1 结果；旧 consolidator 还混入 matched/sweeps |
| N=4、585 pages/Pod、512 tokens/page | 14 份主 YAML 均一致 | 可保留 |
| P-only，无 Decode/未知 completion KV | service 只计算 input misses，Cache 只写 `request.block_ids` | 已符合“不构造 completion KV”；但 Prefill service time 仍违反本轮规定 |
| A_COPY | 主 YAML 省略 action，config 默认 `COPY` | 实际是 COPY；v1 宜显式冻结并拒绝 MOVE/O2 action |
| theta=2 | YAML 同时写 `routing.theta=2` 与 `transfer.theta_simple=2` | `from_yaml` **不读取 `routing.theta`**；实际 gate 取后者。当前值正确，但配置键有误导风险 |
| W=300s、B=25 GB/s | 主 YAML `300000 ms`、`25000000000 bytes/s` | 数值正确，仍有额外 control latency 与 budget |
| lambda_load=0.5、lambda_cost=0.1 | 写死在 helper 中 | 系数相同不代表公式相同，见 C.8 |
| 独立 TaskMain v1 | 已有的是 `TASKMAIN_V1_1`；`engine.py:103–114` 只允许列举的旧 formal protocol 执行 proactive Evaluation | 新协议尚无独立执行语义或校验；改名字不能解除旧逻辑 |
| 主 formal 配置 | `configs/formal/taskmain_v1.1.yaml` 是 Development、visibility=900000，且无主 YAML 的 formal version 字段 | 不能把它当作本次正式主配置直接运行 |

七行实际映射如下，两 workload 对称：

| v1 行 | 历史配置 ID（Conversation / ToolAgent） | 当前实际 routing / strategy | 判断 |
|---|---|---|---|
| 1 | E01 / E08 | R_AFF，无 proactive | routing 的 Load 和请求处理需替换 |
| 2 | E02 / E09 | R_REQ_KV_TASK，reactive=true，无 proactive | gate 形式正确；Load、ticket、service 不符合 |
| 3 | E03 / E10 | R_AFF + FUTURE_DEMAND | Oracle count 可复用；公共 candidate/scheduler/transfer 不符合 |
| 4 | E04 / E11 | R_AFF + PERSIST h=60s K=10 | history/count 可复用；公共执行层不符合 |
| 5 | E05 / E12 | R_AFF + PERSIST h=300s K=10 | 同上 |
| 6 | E06 / E13 | R_AFF + RECENCY q=.9 decay=60s | 使用 external-demand Recency，定义错误 |
| 7 | E07 / E14 | R_AFF + COST_AWARE h=W=300s K=10 | 当前是 weighted-history 排名+旧归一化 gate，不等同规定组合 |

Line 3–7 的 `transfer.enabled=false` **仅禁用 reactive**，不会禁用 proactive wire。不存在因为该字段为 false 就“主动复制无传输成本”的实现。

### C.2 Load 与 routing：不是换单位就能修复

1. **不存在协议要求的 60 秒 miss-token Load history。** `pod.py:45–58` 的 `load_ms` 是 running remaining service + queued estimated service + committed RemotePressure；随 GPU 服务进度下降，完成后退出，而不是保留到 60 秒窗口外。也没有已实现的 TaskMain Load-window 配置。
2. **service_ms 直接影响 R_AFF。** `routing.py:25–29` 同 hit 时调用 `load_ms`，所以就算选择 R_AFF 并关闭 proactive，也没有摆脱服务模型。
3. **R_REQ_KV_TASK 的 gate 形式已正确，输入定义错误。** `strategies.py:94–98` 用乘法严格大于；`engine.py:549–566` 仍传入 service-based Load。旧 cache-hit fraction gate 和 absolute-gap gate 在 TASK 分支确实被绕过，不能误报为仍在生效。
4. **当前请求虽不参与首次 route，但之后使用的是预估服务工作。** `enqueue_ready` 在首次 route 后加入 `service.estimate(...).service_ms`（`engine.py:271–290`），实际 hit 直到 SERVICE_START 才确定。transfer ticket 更提前给 target 加 estimated-service RemotePressure（`328–355`）。这不是“最终 Pod + 实际 Prefix hit 后记录 miss_tokens”。
5. **admission replan 会移除自身 RemotePressure再判断**（`1508–1514`），避免那次 replan 的自计入；但并不能使整个 Load 符合 v1。其他请求仍看到待传请求的 service estimate。
6. **proactive source/target 与 congestion 同样使用旧 Load。** `engine.py:1082,1097–1109` 均调用 `load_ms`。必须切断全链依赖，不能只改 reactive gate。
7. **TASK target 未先排除已完整持有 chain 的 Pods。** `549–550,574–578` 先选所有 Pods 中最小 Load，然后允许已有 Prefix 的 target 直接 route。若第十二节 target 规则同时适用于 reactive，则与“target 必须尚未完整拥有”不同，适用范围见 H。

### C.3 请求处理、时序与 proactive opportunities

1. **禁止的服务模型仍是运行核心。** `service.py:21–27` 精确实现 `d0 + miss_tokens / mu`；`config.py:100–103,227–229` 强制正 service 参数；`engine.py:122–124` 无条件创建模型。将参数置零也不能得到合法 v1。
2. **抽象请求仍经 FCFS。** 到达 -> enqueue -> SERVICE_START 时查实际 hit -> SERVICE_DONE 时写 Prompt Cache（`engine.py:1704–1751`）。这会改变后续 Cache 状态、candidate、reuse history 和 Saved Tokens，不只是多输出一个旧 latency 指标。
3. **reactive ready 后没有完成抽象请求。** `1654–1699` 发布 transfer 后再次 `enqueue_ready`；仍等待 GPU，再经过 Prefill service 才写当前 Prompt。与协议第五节不符。
4. **没有“一请求完成一机会”。** `621–646` 预先排 `TRIGGER_TICK`；主 YAML 周期 1000 ms；处理请求结束没有 proactive hook。无请求也会 tick，同 timestamp 多请求共享 tick。
5. **cap=1 现有形式是 per tick。** `1047–1076` 只在成功启动后增加 `started`，失败后会继续其他候选，这个搜索方式可借鉴；不能把 per tick 的 1 视为 per external request 的 1。K=10 确为 shortlist，不是一次 10 动作。
6. **同 timestamp 确定性已有，但顺序语义不同。** `Event` 按 time/priority/event_id 排序（`events.py:28–53`），trace 文件顺序生成 request ID/event ID。所有 arrival（priority=2）早于所有 SERVICE_START（4）和 tick（5）；同 timestamp 的后续 arrival 因而看不到前一个请求尚未发布的 Prompt。新协议的逐请求处理顺序不能照搬这个 batch 语义。
7. **history/candidate 在 REQUEST_ARRIVAL 被 observe。** `1296–1301`；tick 时包含同 timestamp 所有已到达请求，即使尚未完成。没有读取未到达 future prefix 创建 candidate，但“arrival 已观察”与本协议“处理完请求后的机会”仍需对齐。
8. **Oracle 在 trace 尾部少产生机会。** `624–646` 对 FUTURE_DEMAND 只排 `t<=visibility_end-W` 的 tick，其他策略可排到 visibility end。不能把这种尾部裁剪无说明地迁移为新机会定义；未来不足应显式标记，见 H。

### C.4 Candidate 完整性、in-flight 与 target

1. **合法中间 Prefix 被遗漏，确定存在。** `CandidateGenerator.observe` 保存每一层 `_paths`，但 `online_hashes` 只返回 request endpoints、branching nodes 和 Published leaves（`oracle.py:60–76`）。例如此前只见 `[a,b,c]`，Pod 0 完整持有、其余为空，无 branch、无 in-flight，则 `[a]`、`[a,b]`、`[a,b,c]` 都满足协议；当前只枚举 `c`。这是源码推演，不是新实验结果。旧 `test_online_known_published_prefix_is_candidate_with_source` 甚至断言只返回 endpoint，需保留为历史测试并新增 v1 覆盖测试。
2. **materialize 的已有条件可复用。** `oracle.py:83–89` 检查至少一个完整 source，且未被所有 Pods 完整持有；root 不在 `_paths`；候选信息来自 online observe。不能因为这些检查正确就认定候选覆盖完整。
3. **in-flight 排除晚于排序/Top-K/quantile。** `materialize` 不检查 transfer；`engine.py:1087` 才查 `(candidate.hash_id,target)` 是否在 `proactive_inflight`，没有完整执行“等价或更深 chain 覆盖”判定，也未把 reactive running copies 纳入这个集合。
4. **single-flight 暂时掩盖一部分重复复制。** 当前 busy target 会直接排除，可能阻止向该 Pod 启动任何新 copy；这比协议 chain-target 限制更宽，且没有解决非法候选提前挤占 shortlist/quantile 的问题，不能当作正确 ancestry 去重实现。
5. **target tie-break 不符合。** `engine.py:1097–1099` 为 `(Load, -target_hit_pages, Pod ID)`；协议要求 `(Load, Pod ID)`，中间的 affinity tie-break 会改变复制位置。
6. **target 集合额外排除 endpoint busy。** `1083–1087` 且 source 也要求 endpoint free；协议未冻结双端 single-flight 模型，见 H。当前选定 target 容量失败后主配置 fanout=1，转向下一个 chain，不再试同 chain 的次低 Load Pod；需明确是否正是所需规则。
7. **logical candidate 与实际 wire chain 可能不同。** `1066–1071` 遇 partial last page 无条件减一页，candidate hash/score 仍是原 endpoint；target 是否持有、wire 和发布却按短一页 path。单 partial-page chain 会完全无法复制。

### C.5 Oracle、Persistence 与 Recency

| 项目 | 当前实现 | v1 差异 |
|---|---|---|
| Oracle 信息范围 | `FutureDemandIndex` 只有 Prefix hash -> arrival times；不含未来 route/Load/eviction | 这一点符合；O2 不是该默认分支的打分器 |
| Oracle window | 两次 `bisect_right` 给出 `(t,t+W]`；越 visibility end 抛错 | 符合窗口公式；尾部机会与边界见 C.3/H |
| Oracle 排序 | score 降序、depth 降序、hash ID 升序 | 符合排序；candidate universe 必须修复 |
| Oracle 零分 | `engine.py:919` 只保留 `score>0` | 协议未明确 F=0 是否进排序或直接 skip；不得把旧隐含 gate 当新规定 |
| Persistence | `(t-h,t]` 包含请求计数，h=60/300s、K=10，score/depth/id | 公式与排序可复用；边界与请求完成时序需明确；in-flight 合法性过滤顺序需修复 |
| Persistence 零历史 | 同样在 `score>0` 才 materialize | 旧实现额外排除零分；协议是否要求正 count 见 H |
| Recency 事件来源 | `ExternalDemandHistory.recency` 累加 external request arrivals 的指数衰减 | **不符合**。必须来自真实请求实际复用 chain 的事件；cold demand 不能刷新 |
| Recency 衰减 | `exp(-(t-arrival)/60000)` | 60s 常数可复用，但时间必须换成实际 reuse 的 `ti` |
| Recency quantile 集合 | 对有正分、source/replica 可 materialize 的 candidates 算 q=.9，然后才查 in-flight/endpoint/capacity | 不是完整的“当前合法且有正分”候选集合；至少缺 ancestry in-flight 过滤 |
| Recency copy/probe | 不调用 demand-history.observe；cache.lookup 不触碰 LRU | 不等于已有真实-reuse Recency；仍须建立独立 reuse event history |

普通 PERSIST/RECENCY/COST_AWARE 决策分支当前不调用 `future_count`，没有证据表明它们利用未来 routing/eviction 决策。但 `engine.py:78–82` 会为普通 TaskMain 也实例化全 trace future index，事后 wasted 分类会查询未来 external demand。新路径宜区分 Oracle 决策依赖与事后 observation，避免仅凭 `oracle` provenance 字段误称普通策略使用了 Oracle。

当前 `history_current_only` 配置验证只允许 R_REQ_KV / Strong Reactive，且仅 PERSIST/COST_AWARE（`config.py:151–168`），不能直接给 R_AFF 的 Line 4–7 打上该标记来取得 v1 信息隔离。

### C.6 Cache、LRU 与容量

1. **Prefix closure / unprotected leaf eviction 已实现。** `cache.py:125–169` 只淘汰未 pin leaf；lookup 不触发刷新；source `pin` 也不刷新。这些是可复用的正面发现。
2. **copy 到 target 的新节点被赋予 ready-time LRU。** `commit_transfer_temporary` 在 `cache.py:307–312` 对新节点调用 `_mark_access(child,time_ms)`。即使没有真实 request access，也给其“最近访问”时间；不符合只按真实 request access time 排序。已有节点不会被这个 commit 刷新，问题是新复制节点的初始化语义。
3. **Prompt 新页按 SERVICE_DONE 时间初始化 LRU。** `commit_active` 同样 `_mark_access`，当前 service 时间会传导到淘汰顺序；真实 hit 则在 SERVICE_START 的 `pin_and_refresh` 刷新。v1 应统一抽象真实请求访问时刻，复制页从未被 target 请求访问时如何初始化见 H。
4. **容量并非仅 Published Cache。** `memory_used_pages=resident+ActivePrivate+TransferTemporary`（`cache.py:77–78`）。transfer 按整条 transferable pages 预留 Temporary，即使部分 ancestor 已驻留；之后只把 missing pages 发布，多余 reservation 释放。两种 copy 使用同一 API，符合“统一 accounting”的方向，但是否继续全 wire staging 或按新增驻留预留不是协议已明确的事项。
5. **proactive 容量失败已有 skip，但没有规范 `SKIP_NO_CAPACITY` 事件。** `engine.py:1135–1152` 记 `skip_capacity`；计数缺少 decision/chain/target 明细，且与 no-free-capacity、endpoint 等其他原因分散。
6. **reactive 容量失败会等待/timeout，而非统一立即 skip。** `1585–1598` 容量不足时保留 ticket，随后可重试，超过 admission timeout 则 fallback（`1348–1358`）。这套旧 admission 规则未在 v1 冻结，不能擅自继承。
7. **Prompt 容量失败会终止整个 run。** `1728–1733` 抛出 `CapacityAdmissionError`，CLI 写 failure.json 后退出。真实 trace 的单 Prompt 最大仅 247 pages，不超过 585；但 transfer protection 与容量竞争仍可能让 request admission 失败。协议“容量不足 skip”的对象是否也包含 Prompt cache insertion，见 H。

### C.7 proactive/reactive transfer

1. **完成前不可见：符合。** 两条路径均只预留 Temporary，分别在 `PROACTIVE_TRANSFER_COMPLETE` / `TRANSFER_COMPLETE` 调 `commit_transfer_temporary`。没有把 future-ready 副本提前放入 Published Cache；不应重新引入即时 copy。
2. **时间公式有确定偏差。** proactive `engine.py:1158–1160`、reactive `1607–1609` 均为 `control_latency_ms + wire_bytes/B*1000`；主配置 control=1 ms。v1 只允许 `wire_bytes/B`（秒），内部 ms 换算不应多加 control 常量。
3. **带宽值及基础 Cache API 一致。** 两者均用 25e9 B/s 和同一 target Temporary/source pin 机制；proactive 的 budget 却额外限制了可启动动作。
4. **旧 byte budget 实际生效。** refill=28,626,124.8 bytes/s，burst=8,587,837,440 bytes，initial_tokens=0。`1120–1128` 不足则拒绝，`1153` 成功启动才扣费。它不是 25 GB/s 的物理传输时长，不能保留为另一个隐含策略约束。
5. **旧 endpoint/NodeGate 限制实际生效。** 每 Pod 一个 `active_transfer_id`，source/target 双端占用；有任意 queued reactive ticket 时 proactive 整体 skip（`1054–1057`）；ticket 带 5000 ms timeout、admission-time replan 和 fallback。主七行虽然没有 reactive+proactive 同时启用，Line 2 本身仍受这些 reactive 机制影响。
6. **partial-page 模式不统一可配置。** reactive 用 `transferable_prefix_pages(mode)`；proactive 无条件去掉 partial tail，即使配置 `TRACE_PAGE` 也如此。v1 未冻结 partial-page wire 与 bytes 语义，见 H；必须保证 chain identity、评分、容量、wire tokens 和发布路径一致。
7. **wire bytes/token 比例缺少本次明确授权值。** 旧配置每 page=14 MiB=14,680,064 bytes，来源为旧模型 KV 假设，不由 Mooncake hashes 推导。512-token page 与 585-page 容量本身不能唯一确定 wire bytes。该参数影响 transfer ready、Cost-aware 和成本报告，见 H。

### C.8 Cost-aware

已正确的部分：六个 `task_weight` 数值、`value_hat=(W/h)*sum(weights)` 的原始 benefit、W=h=300s、全零 Load penalty=0、最终 gate 严格 `Score>0`。

确定差异：

| 项目 | 当前 | 冻结协议 |
|---|---|---|
| benefit | `value_hat / cost_v_ref`，V_ref=2.5 | 原始 `sum_b n_hat*w_b`，不归一化 |
| congestion | service/queue/RemotePressure Load 的 target/max | 60 秒 miss-token Load 的 target/max |
| transfer term | `0.1*(1ms+wire/B*1000)/8.04643072ms` | `0.1*(wire_bytes/25e9)`，transfer_cost 单位秒 |
| 校准依赖 | `calibrate_taskmain_development.py` 合并两 workload Development 估计 V_ref/T_ref | 不允许引入这些 references，也不需要校准此公式 |
| trigger 选择与 scheduling | 用 `COST_AWARE` 单一 strategy，把 weighted `value_hat` 排序后 Top-K，再逐项 Score gate | 协议写 `T_PERSIST(h=300,K=10)+S_COST_AWARE`；当前未保留纯 Persistence count Top-K |
| final Score 排序 | 不按 final Score 重新排序，先通过 gate 的旧 benefit 排名候选获选 | 第六节“按策略 score 排序”与 Line 7 的两层组合需要明确，见 H |

最后两项不是擅自指定一种新选择方式：**当前 weighted-benefit Top-K 不是纯 persistence-count Top-K 是事实；v1 最终应采取“Persistence shortlist 后按 Score 排序”还是“保持 Persistence 排序，Score 只作 gate”需用户确认。** 不因旧代码更复杂而默认保留它。

已有 `score_gt_zero_count`、`score_le_zero_count` 等统计可作为诊断字段参考。若正确公式下没有 Score<=0 的候选，只如实报告，不调整系数、单位、normalization 或阈值。

### C.9 请求收益与 Gini 指标

1. **Saved Prefill Tokens 的聚合原语正确，取样时机错误。** `metrics.py:200–201,332` 只累加每请求最长实际 `h_used_tokens`，无重复加祖先；但实际 hit 来自 FCFS SERVICE_START，必须由新执行层重算，不可直接继承旧结果值。
2. **Request hit rate 已在两个主汇总脚本实现，engine summary 未统一提供。** 定义为 `h_used_tokens>0` 的请求比例；Token hit rate 为 total saved / total input，可复用聚合公式。
3. **主流程没有六个要求的 Prompt-bucket Saved Tokens，也没有 WeightedSavedTokens。** `task_weight` 只用于 policy benefit。`step1_attribution.py:32–39,153–190` 虽有 bucket 汇总，但分界是 2048/4096/8192/16384/32768，不能冒充本协议 buckets。
4. **主表 `gini_actual` 当前是整个 Evaluation 的 executed_miss_token_gini。** 两主汇总脚本 `build_row` 取 `summary.cluster.executed_miss_token_gini`；既不是 request count，也不是五分钟 window。
5. **engine 中另有 request_count_gini，但仅整段聚合。** `metrics.py:203`，不能直接当五分钟指标。
6. **`pressure.py:29–32` 的默认窗口为 1/3/15/30/60 秒，无 300 秒 random-normalized 主指标。** 其中按 arrival 计 request count 的思路可参考，但整个模块依赖 service intervals，不应当作 v1 Load。
7. **主汇总的 random mean / ratio 是空字符串。** 没有 random P95 / valid window count。
8. **历史确实已有 random Gini helper，但口径不匹配。** `step1_attribution.py:88–143` 随机分配每请求的 realized miss tokens，整段 Evaluation、CLI 默认 1000 repetitions（`429` 附近），不是五分钟 request counts × 200。其 seed、uniform assignment、均值为 0 时 ratio=None 的处理可参考；直接调用该模块还会导入 Full O2-A/replay 依赖，不能整模块复用。
9. **latency 仍是旧报表主要收益。** 主汇总与 consolidator 以 mean/P50/P95 completion、queue/service 及 latency deltas 为中心。v1 必须改为本协议的 token/transfer/waste/Gini 指标，并把 weighted saved 明确标为 benefit proxy，不是实测 TTFT ms。
10. **baseline deltas 不完整。** 两主汇总只提供 completion、token-hit 的部分 delta，Gini 只对 R_AFF；Saved、六 buckets、WeightedSaved、request hit、Gini random、transfer tokens/count/bytes、waste coverage 等均没有完整的 vs Line 2 输出。也不能误用历史 `R_REQ_KV_V1` / Strong Reactive 为本轮 baseline。

### C.10 Wasted-copy、residency generation 与 censored

已有 generation 路径：proactive ready 后只为 newly inserted pages 创建 generation（`engine.py:1273–1280`）；真实请求 SERVICE_START 命中对应 `(target,block)` 才记 use（`1718–1725`）；eviction 时移除当前 generation（`698–705`）。相同内容之后重新插入不会自然续接旧 generation；该基础方向符合要求。

但主口径仍不符合：

1. **use 没有 ready+W 截止。** SERVICE_START 对 generation 的 hit 一直累计到失效或整个 replay 结束，没有 `now<=ready+W` 检查。一个首次使用在 ready+W 之后的副本也能标 `actually_used=true`。
2. **censor 标记已有，分子/分母排除不完整。** `2034` 按 `complete_time+horizon>oracle_visibility_end` 标 CENSORED；但 `2116–2163` 的 useful/page-weighted fraction 用全部 actions / 全部 wire，包含 censored。主脚本只在 wasted **count** 中排除分类为 CENSORED，并没有产生要求的 observed token ratio。
3. **分类不等价于 unused。** `2035–2048` precedence 中 TOO_LATE 早于 USEFUL；即使同 generation 后来被真实请求复用，也可能仍为 TOO_LATE。不能用“classification 不等于 USEFUL/CENSORED”定义新 wasted numerator。
4. **`NO_FUTURE_DEMAND` 检查复用了 policy score。** `action.oracle_score` 在 Persistence/Recency/Cost-aware 中存的是历史 count/衰减/benefit；`2039` 将其当 future demand 条件不具统一含义。v1 的 wasted 只取真实 target use，不需要这套 future-demand precedence。
5. **旧主表是动作计数，不是 transferred-token 加权。** `proactive_scope` 输出 `unused_replica_count` 和 `wasted_copy_count`；不同长度 chain 的动作权重相同。
6. **旧 `page_weighted_wasted_wire_fraction` 也不等价。** 它是 `(wire_pages - distinct_hit_new_generation_pages)/wire_pages`，且没有上述观察窗/censor 约束。协议的 `sum(unused observed copy transferred tokens)` 究竟按整动作 used/unused 还是逐 token partial use，见 H；不能直接取这个同名近似字段。
7. **缺 observed copy count、censored copy count、observed transferred-token coverage。** 有分类计数不等于这些按同一 action cohort 正确聚合的指标。
8. **wire 和 newly resident 已分开，但缺 wire tokens 字段。** 不应以 `newly_resident_pages*512` 作成本；当前主配置 FULL_PAGE_ONLY 可从 wire pages 推出 512 倍，partial-page 最终口径仍需明确。
9. **只记录 proactive generations，reactive 没有同等 copy-use observation ledger。** 若“all observed copy”包含 reactive，须新增；若只指 proactive，Line 2 waste 应如何展示/求 delta 见 H。
10. **action log 不足以独立审计所有窗口。** summary action records 有 first_use 和 hit request IDs，但未持久化完整 per-generation ready/end/use 时间及事件顺序；后续修复须记录可重算的 observation 证据，不能只存一个最终分类。

历史 `scripts/o1_copy_routing_audit/run_audit.py:334–424` 和 `scripts/o1_timing_residency_audit/analyze.py:199–290` 已有 page lifetime、event-step、ready+W 的辅助诊断。它们绑定旧 E03/E10、旧 provenance/committed-engine replay、原始 `actually_used`，且 timing audit 明示实际 reuse 仍可取全 SERVICE_START 范围；**不是一个已接入 v1 的合规 wasted 指标实现**。可借鉴记录格式，不调用其历史实验入口或替代新指标定义。

### C.11 split、成本作用域与历史隔离

1. **连续运行与请求 cohort 的主框架符合。** 主 YAML 的 split 正确；Evaluation 模式下从 t=0 处理整 trace，没有 25 分钟清空 Cache/history/Load/in-flight 的代码。`TraceRequest.split` 按 arrival 确定，`metrics.summarize` 只选 Evaluation requests，tail 请求不混入主 Saved Tokens。
2. **普通历史策略和 Cache 从 t=0 活动，Oracle 末 W 的触发裁剪另列 C.3。** 不要将 Development 专用入口的过滤逻辑误用于主运行。
3. **engine proactive/network_cost 多数是 full replay。** `engine.py:2002–2354` 未按 evaluation 筛 actions；主脚本另用 trigger time 筛 Evaluation 成本。缺 pre-Evaluation / Evaluation / tail 三段统一输出。
4. **reactive 成本当前按 request arrival cohort，而 proactive 按 action trigger-time cohort。** `metrics.py:272–297` 对 Evaluation requests 汇总 reactive；主脚本按 `[25,45)` trigger 筛 proactive。边界跨越时两种 cohort 不相同，需要同时显式标记，不能把无标签的 total 当同一时间范围。
5. **部分诊断名义 Evaluation、实际全程。** `r_req_kv_task_activity` 为全 replay；per-Pod load/capacity peak 等部分值仍来自全程；`later_normal_eviction_count`（`engine.py:2171–2176`）甚至以 Development evictions 减全程主动 admission evictions。不能迁移为 v1 evaluation 统计。
6. **W=30min 历史 sweep 确有独立提前窗口。** `configs/taskmain_v1.1/sweeps/oracle_window/` 与 `summarize_s01_oracle_window.py` 使用 Evaluation `[900000,1500000)`，不是主 `[1500000,2700000)`。其避免 future 不足的方向可参考，但不是本轮批准的 sensitivity；当前 script 仅检查请求数量相等，不能代替完整 common-support 设计/身份核验。
7. **旧 provenance 不足以区分新的机制。** `engine.py:2382–2440` 标旧 B0/METRIC_VERSION、service、references、tick/budget；没有 TaskMain Load definition/window、copy-use definition、Gini window/repetitions 等；git_commit 也不能描述未提交修改。普通历史策略若未打 history-only flag，provenance 的 oracle policy 仍显示 Future-Demand Reference，容易误读。
8. **不要复用会写历史目录的脚本。** 旧 run 脚本写 `results/taskmain_evaluation/`，consolidator 写 `results/summary/` 和 `docs/results/overall_results.md`；`init_taskmain_docs.py` 无条件写历史实验注册页。新协议应有独立目的地，历史名称保持原样。

## D. 隐藏依赖专项结论

| 依赖 | 当前 14 行是否受影响 | 证据与处理结论 |
|---|---|---|
| `service_ms=d0+miss/mu` | **是，全部行** | `service.py`、`engine.py:122,283,1726,1737`；影响 hit 与 Cache 发布时间。v1 路径必须消除 |
| FCFS GPU queue | **是，全部行** | `Pod.queue/take_next/start_running`、SERVICE_START/DONE；不能只停止报告 latency |
| O2-P / O2-A / Full O2-A | **默认主配置没有启用其决策分支，但共享 engine 存在入口** | 默认 COPY 不进入 O2 probe/forced/closed-loop；O2 wrappers 调 engine，不是主脚本自动调用 O2。需要 v1 allowlist 隔离，不应声称已发现 O2 默认打分污染 |
| Strong Reactive | **模块无条件 import；ECT 决策分支在 14 行未启用** | `engine.py:18–20,1304–1307` 只在 `R_REACTIVE_ECT_V1` 调用；共享 service/NodeGate 是实际依赖，不能误归因为执行了 Strong Reactive |
| P3 / learned ranking | **未发现主路径调用** | 主 engine 无 learned policy enum/导入；P3/P32 独立离线模块。保持不接入，其 scope freeze 不覆盖本轮 |
| old Cost-aware | **是，Line 7** | V_ref/T_ref 与 weighted-history Top-K；必须替换定义而不是重校准 references |
| external-demand Recency | **是，Line 6** | `engine.py:911` 调 `demand_history.recency`；必须换真实 reuse history |
| 1s trigger tick | **是，Line 3–7** | 主 YAML 1000 ms + `trigger_times`；必须换请求后机会 |
| proactive byte budget | **是，Line 3–7** | `TokenBucket.can_afford/consume`；本次协议未给该约束，不应继承 |
| RemotePressure / FIFO ticket / timeout / replan | **是，Line 2** | 由旧 B0 路径继承，Load 含 service estimates；独立报告，按 H 确認未冻结的 transfer admission 细节 |
| 额外 1ms control latency | **是，有 transfer 的行** | 不符合明确的 wire/B 公式 |
| affinity target tie-break | **是，Line 3–7** | Load 同分时插入 `-target.cache.lookup`；须按协议去除 |

旧文件复杂度不是保留这些依赖的理由；未启用的历史分支也不需要删除或重命名。

## E. 需要新增或重做的 metrics

下列字段名为建议的输出契约，不改变协议公式。计数、tokens、bytes、ratio 各自保留单位，不拼成经济净收益。

| 指标组 | 必需字段/口径 |
|---|---|
| 请求记录 | workload、protocol、request_id、arrival、最终 Pod、abstract process/use time、longest_actual_hit_pages/tokens、miss_tokens、Prompt bucket；`hit+miss=input` |
| Saved | `saved_prefill_tokens=sum(actual_hit_tokens)`；每请求只加一次最长 Prefix；只统计 arrival in `[25,45)min` |
| 六 buckets | `<5k`、`5k–20k`、`20k–60k`、`60k–120k`、`120k–300k`、`>300k` 的 request_count、input_tokens、saved_tokens；空 bucket 也保留行 |
| WeightedSaved | `sum_b saved_tokens_b * multiplier_b`；名称注明 multiplier-weighted token benefit proxy，不标 ms |
| hit rates | request hit rate、token hit rate；空 cohort 的 NA 规则显式记录 |
| 五分钟 Gini | 每 window：N 维 request counts、actual Gini、uniform random 200 repetitions 的 mean/P95、actual/random ratio、seed、window start/end；random mean=0 则 ratio=NA；另报 valid window count |
| transfer | proactive/reactive/total 的启动/完成 count、实际 wire transferred_tokens、wire_bytes；成本不得换成 newly-resident tokens；按同一明确 scope 汇总 |
| 成本阶段 | pre-Evaluation `[0,25)`、Evaluation `[25,45)`、tail `[45,trace end]`；记录 start/ready、阶段及跨边界动作；同时保留全程总计用于核对 |
| copy observation | action_id、logical_chain_id/path、wire_chain/path、source、target、start、ready、ready+W、wire tokens/bytes、generation IDs、实际 use/失效事件、observed/censored 标记 |
| wasted | `unused_observed_transferred_tokens / all_observed_transferred_tokens`；CENSORED 同时排除 numerator/denominator；分母为零的显示规则需冻结 |
| coverage | observed copy count、censored copy count、observed transferred-token coverage；建议同时输出 observed/total transferred tokens 以可复核，具体 action cohort 见 H |
| baseline delta | 每 workload 内各主要指标减本轮 Line 2；六 buckets 逐 bucket delta；rate 可另显示百分点；baseline 为 0/NA 时不制造百分比或假零 |
| 决策审计 | external request/opportunity ID、候选/shortlist 数、score/rank、成功 copy 数、skip reason（含 `SKIP_NO_CAPACITY`）、in-flight 覆盖原因；用于验证每机会 cap=1 |
| Load/Recency 审计 | routing 前 N 维 Load、当前请求最终 miss-token 入账时间；reuse chain/time/event 顺序，copy/probe 不产生 reuse |

若采用固定对齐、互不重叠五分钟窗口，主 Evaluation 会对应 `[25,30)`、`[30,35)`、`[35,40)`、`[40,45)` 四窗；**这是待 H 确认的窗口实现方式，尚未替用户冻结**。无论汇总方式如何，不能只报整段 request-count Gini，也不能将 60 秒 routing Load window 当作五分钟 Gini window。

## F. 预计修改文件列表

为保留历史结果的语义，建议新增独立 TaskMain v1 路径；下表是实施建议，不是已实施的架构决定。若最终选择共享代码，也必须显式版本隔离，不能原地改掉历史协议行为。

| 建议路径 | 预期工作 |
|---|---|
| `src/simulator/task_main_v1/config.py`（新增） | 新配置结构、矩阵校验、Load/B/W/乘子/协议版本；不要求 service 参数；拒绝不允许的旧策略 |
| `src/simulator/task_main_v1/engine.py`（新增） | 无 GPU service 的请求/transfer 事件循环，逐请求机会，state 从 0 连续演化；复用已确认原语 |
| `src/simulator/task_main_v1/load.py`（新增） | 每 Pod miss tokens 滚动 history、60s 到期、post-routing 入账 |
| `src/simulator/task_main_v1/routing.py`（新增） | R_AFF、R_REQ_KV_TASK、明确 target tie-break，隔离旧 Pod.load_ms |
| `src/simulator/task_main_v1/candidates.py`（新增） | 完整 online Prefix universe 与 chain-target ancestry in-flight 判定 |
| `src/simulator/task_main_v1/policies.py`（新增） | 未来需求 Oracle / Persistence / reuse Recency / Cost-aware，明确 selector 与 scheduler |
| `src/simulator/task_main_v1/transfer.py`（新增） | 统一 reactive/proactive wire time、容量、ready publish、in-flight ledger |
| `src/simulator/cache.py`（可能有限改动）或新路径内 cache adapter | 复用 trie/leaf-LRU；独立 request-access timestamp、copy 新页初始 age、保护与容量 API；避免改变旧默认行为 |
| `src/simulator/task_main_v1/metrics.py`（新增） | 独立 request/action schema、bucket/weighted/Gini/random/wasted/phase scopes |
| `configs/task_main_v1/{conversation,toolagent}/`（新增 14 份或等价冻结矩阵） | 与旧 `configs/taskmain_v1.1/` 分开；主 Load 固定 60s，不加入 sensitivity |
| `scripts/task_main_v1/`（新增） | 两 workload 独立运行入口、汇总与所有 baseline deltas、独立结果路径、版本和输入身份校验 |
| `tests/test_task_main_v1*.py`（新增） | 新语义测试；保持旧 `test_taskmain.py` 等为历史协议测试 |
| `docs/protocol/task_main_v1.md`（未来新增） | 用户确认后记录本次协议及 H 的明确答案，不覆盖旧 v1.1 文档 |

若选择在原模块内增加 version branch，则涉及 `config.py`、`engine.py`、`pod.py`、`routing.py`、`events.py`、`cache.py`、`oracle.py`、`strategies.py`、`transfer.py`、`metrics.py`；旧 `service.py`、`tickets.py`、`pressure.py` 只保留给历史运行，不让它们影响新 routing/Load/评分/主评价。

不预计修改或删除 `o2p.py`、`o2a.py`、`full_o2a.py`、`reactive_ect.py`、P3/P31/P32、旧 configs/scripts/results。也不修改当前 IDE 打开的 `p32_correction_and_scope.md` 来重新解释其历史研究范围。

## G. 建议实施顺序与验收要点

1. **先确认 H 中会改变结果的语义。** 保存本次协议的独立版本、固定 14 行矩阵和输入身份；不要通过历史文档补齐未确认部分。
2. **先完成无 proactive 的抽象内核与两条 routing baseline。** 验证零 GPU service dependency、60s Load、自请求不参与自身 route、最终 miss 入账、同 timestamp 决定性、Prompt 发布时刻及跨 split 状态连续。
3. **完成 Cache/transfer 共用机制。** 验证 `ready=start+wire/B`，ready 前不可见、完成后发布、统一容量/保护、失败 skip 及 partial-page 已确认规则。
4. **完成全部 logical Prefix candidates 和 per-request cap。** 用直链中间节点、分支、全副本、无 source、同 target 等价/descendant in-flight、小容量等合成案例覆盖合法性；一机会最多一个成功 copy，非法高分项可继续尝试后项。
5. **依次接 Oracle、Persistence、真实-reuse Recency、Cost-aware。** 验证 Oracle 只读需求；Persistence 每请求包含计一次；Recency cold/copy/probe 不加分；quantile 在正确集合计算；Cost-aware 各项单位及纯公式一致。
6. **同步落盘 metrics 证据并实现汇总。** 验证 longest-hit 不重复计数，bucket 加和等于总 Saved，WeightedSaved 可重算；Gini 对所有 N Pods、200 次固定 seed；window ratio/NA 正确。
7. **验证 wasted 与边界。** 包含 ready+W 内外使用、ready+W=trace_end、超出 trace_end、evict/reinsert 同 hash、partial reuse、pre-Eval copy 在 Eval 被用、Eval copy 在 tail 被用、零 observed denominator 等案例；不得用服务 drain 延长可观察 trace。
8. **做历史隔离与配置检查后，再等待正式实验授权。** 旧 tests/协议行为保留；独立结果目录；两个 workload 分开。W=30min common-support、300s Load sensitivity 不加入第一轮 14 行。

本次没有执行这些测试或实施步骤。旧测试通过与否也不能替代 v1 验收，例如旧测试明确要求 external-demand Recency、reference normalization、tick/budget 和 FCFS 同 timestamp 行为。

## H. 需要用户确认的歧义（本轮未作选择）

下列问题影响实施结果，已知明确的差异仍按本次协议修复；不把禁止 service、禁止 reference normalization、请求后 cap=1 等明确条款重新变成待选择项。

| ID | 需确认事项 | 当前实现与影响 |
|---|---|---|
| H1 | **wire bytes 与 partial pages**：是否沿用每 512-token page=14 MiB？复制完整 trace page（包含 partial tail）还是仅 full pages？partial wire 按整页还是 valid tokens 计 bytes？ | 协议给容量/page tokens/B，但没有 bytes/token 假设；旧 full-page-only 会把 candidate c 截短成另一个 chain，影响 score、命中与 wire 成本 |
| H2 | **网络并发/保护/accounting**：是否保留双端 single-flight、source pin、整条 wire Temporary？busy endpoint 应跳过该 chain 还是排队？reactive 是否保留 ticket/timeout/replan，或有另一明确处理规则？ | 这些并非 `wire/B` 单独能确定；不能自动把旧 B0 admission 细节带入。proactive byte budget 不属于已给 v1 约束，不建议继承 |
| H3 | **Load history 的时间与窗口边界**：reactive 等待结束后入账时，history timestamp 用 arrival/最终 assignment/实际抽象处理时刻中的哪一个？窗口是否为 `(t-60s,t]`？ | 入账必须在最终实际 hit 已知之后是明确要求；哪一个 timestamp 作为 60s aging 基准未明确。Persistence 是否同样采用 `(t-h,t]`、当前已处理请求可进入本次机会 history，也需记录 |
| H4 | **等待中的请求与后续 arrival**：reactive 等 wire 时，继续按事件时间处理其他 arrival，还是串行等待该请求结束再处理下一个？同 timestamp 的请求完成/opportunity 与其他 ready 事件怎样固定优先级？ | 单纯“同 timestamp 固定顺序”不能唯一确定这些可见性；应冻结 request ID 顺序及 ready=arrival 等边界，不能沿用 FCFS batch |
| H5 | **Line 7 两层选择**：先 count-ranked Persistence Top-10，再用最终 Score 重排，还是在 Persistence 排序内只作 Score>0 gate？ | 当前 weighted-benefit Top-10 + gate 与 count Top-10 不同；第六节策略 score 排序与第十三节 gate 需给唯一组合解释 |
| H6 | **候选合法性与零分**：Recency quantile 的“合法”是否还含 capacity/endpoint 即时可启动，还是先满足第七节五条件？Oracle F=0、Persistence count=0 是否排除？ | Recency 正分要求明确；Oracle/Persistence 未明确正分 gate；不同过滤阶段会改变 Top-K/quantile。容量失败后是否只继续下一 chain，而不换同 chain 的次优 target，也应固定 |
| H7 | **R_AFF/source/target 细则**：R_AFF 是否继续最长 Prefix、Load、Pod ID？多 source COPY 选最低 Load 还是最低 Pod ID？“target 尚未完整持有”是否同时约束 reactive？ | 旧 proactive target 有额外 affinity tie-break，已明确不符合；但 source 选择和 reactive 已有副本直接 route 未在本次文字中完全定义 |
| H8 | **request-access LRU 初值和 Prompt admission**：从未被 target 请求访问的新副本如何排序（继承 source 的真实 access time，或 target-local never-access sentinel）？真实新 Prompt page 的访问时刻是什么？Prompt 写入容量不足时如何处理？ | 不能把 copy ready 当 request access；旧引擎遇 Prompt admission failure 会停机。需要可重复的初始 age/tie-break 和保护范围 |
| H9 | **wasted 的 used 单位与动作集合**：整 chain 完整复用才使动作 used，还是同 generation 的任意 copied page 被用就使动作 used？partial use 是整动作判定还是逐 token 判定？是否仅 proactive，还是包括 reactive（包括发起 transfer 的当前请求）？ | 决定 numerator；旧实现“任意新页 use”和 page-weighted 两种指标同时存在，不能任选。若仅 proactive，Line 2 无动作时 waste/coverage 及其 delta 需 NA 口径 |
| H10 | **成本与 observation 的 cohort**：主 wasted 与 coverage 按 Evaluation 启动动作还是全 trace 动作？coverage 是否 `observed wire tokens / 全部同 cohort wire tokens`？reactive transfer 主成本按 request arrival cohort 还是 transfer-start 阶段？ | 三阶段成本必须都能输出是明确要求，但主表选哪个 aggregate 未完全定义；被 censor 的动作仍应保留真实成本，仅从 waste 分子/分母排除 |
| H11 | **trace end 与尾部 Oracle**：观察边界采用最后 arrival=3536999ms，还是输入采样覆盖右界3537000ms？Oracle 在 t+W 超出可观察范围时是否仍产生机会并记 future-window-censored skip？ | 不能把不足未来当 0，也不能靠 drain 的模拟完成时间延长观测；W=30min 的 common-support 以后单独设计 |
| H12 | **Gini 报告聚合**：五分钟是固定非重叠窗还是滑动窗？四窗 actual/random/ratio 怎样汇总；valid window 是非空窗口还是 random mean>0 的窗口？固定 seed 具体值是否沿用20260911？ | 每窗可完整输出；主表 mean-of-ratios 与 ratio-of-means 不等价，必须冻结，200 repetitions 与 ratio zero->NA 已明确 |
| H13 | **边界及空值约定**：是否沿用 `<5000`、`[5000,20000)`、`[20000,60000)`、`[60000,120000)`、`[120000,300000]`、`>300000`？无请求/无 observed wire 的比率是否 NA？ | 旧 `task_weight` 是该 bucket 分界，不能只靠区间短标签推断所有端点；空值会影响 baseline deltas |
| H14 | **研究问题与固定矩阵的解释边界**：Line 3–7 明确为 R_AFF+proactive，Line 2 为 R_REQ_KV_TASK。是否只把差值解读为“相对 request-driven P2P baseline 的整体策略差异”？ | 固定七行没有 R_REQ_KV_TASK+proactive，因而不能隔离“同一 P2P 底座加主动复制”的因果增益。本文保留原七行，不擅自加 matched 行或改 routing，也不借用旧 O2/Strong Reactive 结果补这个结论 |

这些问题集中留在本文，等待下一步指令；本轮没有采用任何答案作为实施假设。

## 审计身份记录

为区分工作区和 HEAD，记录关键文件 SHA-256：

| 文件 | SHA-256 |
|---|---|
| `src/simulator/engine.py` | `3a7aab823eba6a2e2a90199aaf726b97786be0019853f4ec4cb856afd812d4a9` |
| `src/simulator/config.py` | `c238e7f1ecd1a4fe23c75dd794ce6b5e5314fa47c761fc24394a293e02f04612` |
| `src/simulator/metrics.py` | `76cb8629ebc896ee78a1ee1b36935834c346cf7903588c8779a6b44d2ee5ef45` |
| `src/simulator/oracle.py` | `0b3c94aaad64671b0864b7f4da6d89e8cb6fcf35c21d840683b29d8bcaaa624d` |
| `src/simulator/strategies.py` | `de1ce252571ad4b995b5d72460ffb300ccd45cca6befbb667cbfd9acc94e6e7c` |
| `data/mooncake/conversation_trace.jsonl` | `b8cbb061a85206d729d91cdc2981f43c9e0d99209dce588d3af5f7934408b9df` |
| `data/mooncake/toolagent_trace.jsonl` | `48a2db1a13d3bc05e6330140c64f604ba366df20d3c9e128b5c35a01c1fa5f71` |

状态：**Gap Analysis 完成；尚未实施 TaskMain v1；未运行实验。完成后停止，等待用户下一步指令。**
