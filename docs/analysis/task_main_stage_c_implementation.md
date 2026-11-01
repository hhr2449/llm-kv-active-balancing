# TaskMain Stage C 实施记录

日期：2026-09-17。仓库：`/root/data2/llm-kv-active-balancing`。

**当前状态：Stage C PASS，blocker=0。C1输入分离及等价性能优化完成；254项测试、优化后14项Pilot双遍、真实Gate A/B、确定性及独立artifact validation全部PASS。以下第1–17节保留首次INVALID审计记录，第18–22节记录修复、优化和被取代的运行，第23节为最终验收。** 正式14行仅创建配置和入口，没有运行。未运行sweep、O2/SR/P3；pilot数字仅用于pipeline correctness，不用于性能判断或调参。

## 1. 冻结范围与实现边界

先更新唯一协议 `docs/protocol/task_main_v1.md` 第18节，再实施C。保持A/B的routing、Load、Cache、传输、candidate、ranking、Score gate语义。新增代码负责只读观察、聚合、判等、配置身份及运行验证。Cache/load/routing/transfer/candidates/policies/proactive模块不修改；engine只增加决策读取上下文、ready-generation记录、最终历史状态导出和事后指标。history仅在FutureDemandIndex.count增加实际API计数。

## 2. Saved / bucket / WeightedSaved

正式arrival cohort=[1500000,2700000)，pilot=[300000,600000)。直接使用每请求一次final_hit_tokens，包含partial-tail valid tokens；每请求hit+miss=input。六桶边界和六个乘子精确按协议，保留空桶；request/input/saved守恒，WeightedSaved用math.fsum汇总各桶saved*multiplier。该指标为Multiplier-weighted Saved Tokens benefit proxy，不是TTFT或毫秒。

空分母JSON为null，CSV为NA。请求、wire、bytes、ratio不混成经济净收益。summary.final_metrics是C的主指标；原有顶层request_count等明确保留FULL_REPLAY范围，避免覆盖历史字段语义。

## 3. Gini与random baseline

以arrival落入固定300000ms窗口的final_pod请求数量计算population Gini，保留所有4Pods。正式四窗、pilot一窗。200次uniform randrange(N)分配，seed20260911；每cache key独立重新初始化Random(seed)，不依赖调用顺序或策略。key包含workload/start/end/N/request_count/seed/repetitions，返回不可变mean/P95。P95线性插值。零random mean产生NA ratio，无epsilon。

分别取actual/random mean/random P95/window ratio的median；ratio排除NA，不用ratio of medians。保存所有window counts、request_count、seed、repetitions、ratio和valid/total window count。名称request-distribution skew ratio；时间窗不作为独立实验，不作显著性声明。

## 4. Wasted-copy generation attribution

独立copy_observation_records记录每个proactive ready瞬间整chain的target-local generation vector，已有祖先保留其现有generation；reuse_observation_records来自既有Cache.record_reuse真实请求事件，包含target/time/request/path/generation。不为记录刷新LRU或pin，不向策略提供未来信息。

只选Evaluation-started proactive actions。先判断ready+W<=visibility，超出即CENSORED，提前used不能改变censor。完整窗口(ready,ready+W]内必须target相同、实际hit覆盖完整chain、全部对应generation相同，才USED。evict/reinsert和部分hit不能挽救旧action。分子为UNUSED完整wire tokens，分母USED+UNUSED wire tokens；censored成本保留。所有counts/tokens/coverage使用同一cohort，Line1/2及空分母为NA。无逐页收益attribution。

说明限定为“W内未获整链实际复用的主动复制体量比例”，不等同全部token没有部分价值。summary保存逐action观察判定及first-use证据，原始ledger可独立重算。

## 5. 网络成本scope

统一start-attributed；PRE_EVAL/EVAL/TAIL/FULL_RUN分别列REACTIVE/PROACTIVE/TOTAL。每类started/completed分开，wire pages/tokens/bytes来自真实传输，不使用new resident代替。跨阶段ready不迁移成本，censored仍计完整成本。每类阶段加和及reactive+proactive=total均校验。

## 6. Baseline / Oracle delta与表格

reporting.py按workload自动寻找Line2；Saved/WeightedSaved/hit rates/skew/三类wire bytes/started count输出绝对delta，request/token hit rates另报百分点。零/NA基准不造相对百分比。Oracle reference delta=Line3-Line2，包含六桶saved/weighted差值，Line3 waste单独呈现且Line2 waste为NA；不称全局最优headroom。

九类表：Conversation主表、ToolAgent主表、合并strategy×workload、baseline delta、Oracle reference delta、bucket、Gini windows、transfer cost、waste。每类CSV及JSON，所有行有W/h/K/q/theta/N/action字段，可供后续追加sweep；本阶段不运行sweep。

## 7. 正式14行配置

`configs/task_main/formal/{conversation,toolagent}/line_1.yaml`至`line_7.yaml`。每workload顺序：AFF/NONE、REQ_KV/NONE、AFF/FUTURE_DEMAND、AFF/PERSISTENCE h60、AFF/PERSISTENCE h300、AFF/RECENCY、AFF/PERSISTENCE_COST_AWARE。

参数N4、585pages、512tokens/page、14680064bytes/page、25e9B/s、theta2、Load60s、W300s、K10、q.9、decay60s、lambda .5/.1、seed20260911、random repetitions200。正式Eval25–45min、visibility3537000。配置记录完整trace路径及用户指定SHA256；未知字段和不符矩阵参数拒绝。

## 8. Gate A

static_gate_a除policy和非行为line_id外比较全部配置，包括input identity及split。显式execution_projection保留请求assignment/hit、opportunity结果/shortlist顺序、action序列/chain/source/target/time/wire/status、事件、eviction轨迹、最终state及非诊断summary。logical_state_projection另保留Cache/LRU/counters、Load和active histories、candidate universe、demand/reuse histories及request-id集合、pending/inflight/ready状态；排除policy、output path、评分、provenance和report-only内容。

Line5/7分别保留真实policy。要求execution hash、logical hash、系统metrics相同，同时Line7所有shortlist诊断完整、cost_score>0、cost_gate_rejected_count=0。任何不符invalid并停止。

## 9. Gate B

future_access.py通过ContextVar限定实际决策上下文；计数递增只在FutureDemandIndex.count API内。加载trace/构建索引不计，post-hoc不在上下文不计。分别输出oracle_decision_future_reads、history_decision_future_reads及总decision_future_reads。正常arrival/routing与proactive决策均包在上下文中；history读未来则invalid。测试通过向实际策略路径注入future API读取，确认引擎确实拒绝，不以policy名称伪造0。

## 10. Stage C tests

命令：

```text
python -m pytest -q tests/task_main tests/test_trace.py tests/test_cache.py --junitxml=/tmp/task_main_stage_c_tests.xml
238 passed in 6.01s
```

此前166项A/B及共享Trace/Cache回归继续通过；新增72项C测试，0 failed/0 skipped。新增文件test_stage_c_metrics.py与test_stage_c_gates_reporting.py，覆盖用户49类需求，包括所有bucket端点、实际partial hit、四窗arrival边界、random缓存与独立重算、median ratio/NA、整链generation/evict-reinsert、ready两端及censor优先、wire加权/phase归属、baseline与bucket Oracle delta、静态矩阵、9种行为变异检测、实际API读取注入、post-hoc隔离、原始输出读回重算、全部策略runtime import guard。

JUnit为当前会话临时证据。通过合成验收后才启动pilot。

## 11. Pilot配置与入口

`configs/task_main/pilot/{conversation,toolagent}/line_1.yaml`至`line_7.yaml`，仅experiment-kind/split/visibility与正式版不同。Warmup[0,300000)，Eval[300000,600000)，tail[600000,960000)。从空状态处理全部arrival<960000，不截到10min；已启动transfer排空。Oracle索引覆盖该16min输入，尾部不足机会按既有censor skip。

入口`python -m scripts.task_main.run_pilot`，仅写`results/task_main/stage_c_pilot/`，输出目录必须不存在。每case两次完整独立重放；每run保存记录、两类observation ledger、final_state、summary、validation和execution_evidence。summary provenance含trace与所选输入hash、原始工作区模块hash、git HEAD和dirty状态；不冒称HEAD等于实际源码。

## 12. 14项pilot验证

Conversation replay实际2890请求，Evaluation832请求；ToolAgent只读输入清点5775个pilot请求，未执行其simulator。状态如下：

| Workload | Line | 状态 |
|---|---:|---|
| Conversation | 1 | 两遍PASS，所有37个checks为true，全部输出SHA256一致 |
| Conversation | 2 | 两遍PASS，所有37个checks为true，全部输出SHA256一致 |
| Conversation | 3 | 第一遍运行中主动停止；Oracle输入右端点遗漏，INVALID，不产生可解释结果 |
| Conversation | 4 | NOT_RUN，C1阻断 |
| Conversation | 5 | NOT_RUN，C1阻断 |
| Conversation | 6 | NOT_RUN，C1阻断 |
| Conversation | 7 | NOT_RUN，C1阻断 |
| ToolAgent | 1 | NOT_RUN，C1阻断 |
| ToolAgent | 2 | NOT_RUN，C1阻断 |
| ToolAgent | 3 | NOT_RUN，C1阻断 |
| ToolAgent | 4 | NOT_RUN，C1阻断 |
| ToolAgent | 5 | NOT_RUN，C1阻断 |
| ToolAgent | 6 | NOT_RUN，C1阻断 |
| ToolAgent | 7 | NOT_RUN，C1阻断 |

完整矩阵manifest已显式标INVALID；不会输出不完整的九类pilot主表，summarize入口拒绝INVALID矩阵。已完成两行的数字仅作pipeline验证，不能推广为策略性能结论。


| Line | Eval requests | Saved tokens | Weighted Saved | Gini windows | Eval reactive starts | Eval proactive starts | Waste | Future decision reads |
|---|---:|---:|---:|---:|---:|---:|---|---:|
| 1 | 832 | 454656 | 1422950.4 | 1 | 0 | 0 | NA | 0 |
| 2 | 832 | 569771 | 1921783.9 | 1 | 0 | 0 | NA | 0 |

## 13. Line5/7 pilot equivalence

两个workload的真实pilot动态Gate A均NOT_RUN。正式/pilot静态配置gate及合成动态gate已通过，但不能替代缺失的真实pilot结果。不能宣称Line5/7 pilot execution或logical digest已经相同。Gate B仅完成的Conversation Lines1/2实际计数均0；其余真实pilot尚未验收。

## 14. Deterministic replay

要求每case两次独立重放，比较全部输出文件SHA256；实际完成Conversation Lines1/2，每项12个输出文件两遍一致，共48个文件。哈希在validation_manifest.json各case的output_sha256。其余12个case未完成，不推断确定性。Line5/7只用明确execution/logical投影，不要求不同policy的原始文件相同。

## 15. Legacy isolation与历史保留

所有pilot在加载engine前安装runtime import guard；每run保存AST依赖审计及模块hash。仅复用共享Trace及父包导出的旧config类，不执行旧engine/service/Pod/queue/O2/SR/P3。A/B测试继续通过。阶段开始保存315个既有源码/测试/配置/文档/输入文件SHA256；结束核对308个未变化，7个变化仅为协议以及新TaskMain内config/engine/equivalence/history/metrics/records。原有历史dirty文件均保持本阶段开始时的字节内容。新增40文件：28配置、4模块、5脚本、2测试、1报告。Cache/load/routing/transfer/candidates/policies/proactive未改。旧A/B smoke的128个文件全部对照各自manifest SHA256核验无变化；没有重算旧结果，没有commit。文件清单见implementation_inventory.json。

## 16. Blocker C1：Oracle visibility右端点输入遗漏

**C1_ORACLE_VISIBILITY_RIGHT_ENDPOINT，Stage C驱动接线缺陷。** `run_matrix.py`把 `arrival < visibility_end_ms` 的replay请求集传入engine；engine按既有B方式从同一请求集创建Oracle index。因此pilot在 `t=660000ms` 查询 `(660000,960000]` 时，索引缺少恰好960000ms的外部请求。

这是replay cohort与Oracle可观察输入被错误共用造成的C实现问题，不是Cost-aware公式或A/B策略逻辑冲突。合成测试验证了FutureDemandIndex的右闭查询，却没有覆盖Stage C pilot输入截取与索引构建之间的边界接线；238项测试通过不能证明这条接线正确。

只读原始trace核查如下（不是策略实验结果）：

| Workload | t=660000 arrivals | t=960000 arrivals（全部被过滤） | online-known chain [0] 正确future count | 当前截断输入的count |
|---|---:|---:|---:|---:|
| Conversation | 15 | 14 | 921 | 907 |
| ToolAgent | 25 | 22 | 834 | 821 |

ToolAgent边界22个请求中13个属于chain[0]，其余属于其他root；不能把全部22误写成chain[0]的遗漏。chain[0]在决策时已经online-known。本审计证明索引输入不满足窗口定义；Oracle行未完成，未据此声称某个具体action已经改变。

发现后对当前pilot进程发送SIGINT并确认退出，保存已有两行完整证据，将validation_manifest.json标INVALID。没有运行剩余case、正式14行或进一步实验；没有修改W、窗口端点、候选或任何参数来掩盖问题。该实现缺陷尚未修复，不能将当前pipeline视为正式运行已验收。

后续修复点：明确分离replay请求集和Oracle observation input；保持replay arrival<960000，同时为右闭future窗口提供arrival=960000的边界观测。边界请求只供Oracle demand API使用，不应提前进入arrival/candidate/history/Load状态演化。补充覆盖这一输入接线的集成回归后，再按后续指令重新验收。本文只记录修复方案，本轮按“任一项失败即停止报告”没有继续修改或重跑。

## 17. 证据与交付状态

- [pilot validation manifest](../../results/task_main/stage_c_pilot/validation_manifest.json)：INVALID；完成2/14项双遍pilot。
- [C1原始输入边界证据](../../results/task_main/stage_c_pilot/pilot_boundary_blocker.json)：两个workload、原始SHA256、边界request IDs和直接计数。
- [文件变更与保留清单](../../results/task_main/stage_c_pilot/implementation_inventory.json)。
- `source_provenance.json`保存运行时工作区身份；每个完成run保留完整配置、模块hash、实际git dirty状态和原始记录。
- `/tmp/task_main_stage_c_tests.xml`：238 PASS，新增72项，0 pytest failures/0 skips；独立真实输入边界审计发现C1，故整体不能PASS。

**Stage C未完成：blocker=1，pilot=INVALID；动态Gate A及完整14项验证尚缺。** 未运行正式14行，正式结果目录未创建；未运行sweep/O2/SR/P3，没有性能结论或调参。完成本次停止与报告，等待下一步指令。


## 18. C1修复与审计链

用户随后明确授权仅修复C1并从头重跑完整pilot。协议新增第19节：ReplayRequests arrival<visibility，OracleObservationRequests arrival<=visibility；Pilot/Formal共用同一分离函数，不改变W、visibility或查询区间。

`trace.separate_input_roles`产生两个独立输入集合；`engine.run`增加显式oracle_observation_requests参数，只有Oracle分支读取该参数并构建FutureDemandIndex。Oracle验证observation不超过visibility且内部的replay部分与实际replay完全一致。ARRIVAL事件循环始终只迭代ReplayRequests；history policies不读取传入的observation参数，不构建future index。

`run_matrix`统一为两类运行使用分离函数；只将observation传给Oracle，所有行provenance分别记录replay/observation/boundary-only counts、两个时间规则及各自SHA256。每run额外检查processed/history/Load/opportunity IDs仅来自replay，以及online candidate universe精确等于replay完整页prefixes。Oracle在决策上下文之外进行端点count回归，结果存policy diagnostics，不虚增decision_future_reads。

旧pilot的55个文件逐一对照修复开始时SHA256，保持原样；旧validation_manifest.json仍INVALID。首次完整报告副本保存在新运行目录的c1_initial_report.md。新结果目录为results/task_main/stage_c_pilot/run_20260917_02/，所有行从头运行，不复用旧Lines1/2。

## 19. C1新增测试与输入回归

新增test_c1_input_roles.py共11项实例：Pilot/Formal精确右端点；边界existing-chain计入、future-only prefix不进入Cache/candidates；四类history/NONE执行完全隔离（包含不可迭代observation哨兵）；双遍全输出确定性；两个真实trace只读回归；非法observation缺replay/超visibility拒绝。

完整命令与此前相同，JUnit仍写/tmp/task_main_stage_c_tests.xml：**249 passed in 8.35s，0 failed，0 skipped**。原238项全部继续通过。

| Workload | Replay count | Oracle observation count | Boundary-only count | chain[0] F(660000,300000) |
|---|---:|---:|---:|---:|
| Conversation | 2890 | 2904 | 14 | 921 |
| ToolAgent | 5775 | 5797 | 22 | 834 |

只读原始输入边界回归PASS，证据为新目录c1_input_regression.json。这些数字不是性能结果。边界请求包含在索引而不执行，原始trace未复制或修改。

## 20. 修复后完整pilot（优化前过程）

run_20260917_02先采用串行执行，重做了Conversation Lines1/2的双遍，在Oracle第一遍运行较久后主动中止，以调整独立replay的进程调度。其manifest标为SUPERSEDED_INCOMPLETE，并保留全部已生成证据；没有发现该次运行的correctness失败，也没有将其当作完整PASS。

矩阵驱动改用10个独立spawn进程执行28个replay jobs，每个job单独创建Engine、Cache、Load和histories；不共享模拟器状态。调度只改变墙钟执行顺序，不改变任何模拟事件时间、候选/策略/参数。driver在任一job、determinism或Gate失败时终止剩余worker并标INVALID。再次完整测试：**249 passed in 8.22s，0 failed/0 skipped**。

统一新源码版本的完整重跑目录为`results/task_main/stage_c_pilot/run_20260917_03/`，原有两次不完整运行的Line1/2均不复用。每个case的两遍全部在本次新驱动源码下重新计算并比较全部文件hash，两个workload的Gate A继续要求真实执行投影、逻辑state和系统metrics相同。

该运行当时仍在进行，尚不能宣称整体PASS；后续按用户授权停止并由第22节的优化版本取代。正式14行未运行。


## 21. 后台运行交接

用户要求程序继续运行并结束当前对话。交接时run_20260917_03仍为RUNNING，已完成7/14项双遍验证，无已报告失败；主进程PID=44896。进程继续执行剩余pilot并由驱动落盘validation_manifest.json，任一验证失败仍停止，全部完成后驱动生成九类表。进程信息保存在本次目录background_handoff.json。

用户随后授权停止该后台进程并进行行为等价优化。PID 44896及10个worker已退出；驱动因SIGINT短暂写入的INVALID已更正为SUPERSEDED_INCOMPLETE，并另存supersession.json，明确没有观察到correctness failure。该目录保留7项完整双遍及第15个单遍产物，作为优化前parity证据。当前没有宣称Stage C PASS，不自动运行正式14行。

## 22. 等价性能优化、checkpoint与新运行入口

旧实现的主要复杂度来自每个opportunity的每个candidate都重新线性扫描完整transfer历史。Conversation Line3旧运行包含1983条transfer、539565条candidate decision和1611954次future API读取，形成大量重复判断。优化后每个opportunity只遍历一次transfer ledger，将当前IN_FLIGHT proactive transfer的所有Prefix构造成`(target,chain)`集合；candidate按相同ancestry语义做O(1)查询。Persistence另为六桶维护排序时间索引，用两个`bisect_right`精确实现`(t-h,t]`；原始events仍完整保留用于final state与审计。Cache summary只在Cache version变化时重算，返回独立dict。策略公式、排序、tie-break、事件时间和records schema未修改。

新增5项优化/恢复测试，总验收为**254 passed in 8.21s，0 failed，0 skipped**。优化后的Conversation Oracle Line3真实单遍耗时360.04秒；与run_20260917_03旧结果比较，execution projection SHA、final logical state digest、落盘后final_metrics及policy diagnostics全部相同。这个单项是性能与行为parity检查，不是策略效果实验。

矩阵驱动增加两级进度：主进程每60秒在stdout及`progress.json`记录完成replay、双遍case、running/queued；worker每250个请求输出case/repeat/simulated time。`checkpoint.json`使用原子replace，每个单遍的12个输出全部完成并计算SHA256后立即登记。`--resume`仅允许相同HEAD及TaskMain源码/config SHA256，逐文件验证已完成输出并跳过；worker完成而父进程尚未来得及登记的窄窗口可从完整输出恢复，半写目录会保留并改名为partial证据后重做。最终tables也经临时目录完整生成后原子发布。

新的完整pilot目录冻结为`results/task_main/stage_c_pilot/run_20260917_04/`，尚未创建或启动。用户将通过tmux启动；首次启动与恢复命令记录在`scripts/task_main/README.md`。新run仍须完成28/28 replays、两组真实Gate A、Gate B、确定性及独立artifact validation，方可更新Stage C PASS。正式14行仍未运行。

## 23. 优化后完整Pilot最终验收

用户从tmux启动`run_20260917_04`，未使用resume。驱动在1278秒（21分18秒）完成28/28个独立replay、14/14个双遍case和九类CSV/JSON表，`progress.json`、`checkpoint.json`、`validation_manifest.json`均为PASS。每个repeat保存12个结果文件，两遍逐文件SHA256一致，共336个replay输出文件；结果目录总量约3.4GiB。所有run内validation checks均为true，包括请求/Load/opportunity守恒、容量、Temporary/pin排空、transfer、Saved/buckets/WeightedSaved、Gini、network、waste/censor、Gate B、C1输入隔离及legacy dependency isolation。

完整矩阵状态：

| Workload | Line | Policy | Eval requests | decision future reads | history future reads | Cost rejects | 双遍/validation |
|---|---:|---|---:|---:|---:|---:|---|
| Conversation | 1 | NONE | 832 | 0 | 0 | 0 | PASS |
| Conversation | 2 | NONE | 832 | 0 | 0 | 0 | PASS |
| Conversation | 3 | FUTURE_DEMAND | 832 | 1611954 | 0 | 0 | PASS |
| Conversation | 4 | PERSISTENCE h=60s | 832 | 0 | 0 | 0 | PASS |
| Conversation | 5 | PERSISTENCE h=300s | 832 | 0 | 0 | 0 | PASS |
| Conversation | 6 | RECENCY | 832 | 0 | 0 | 0 | PASS |
| Conversation | 7 | PERSISTENCE_COST_AWARE | 832 | 0 | 0 | 0 | PASS |
| ToolAgent | 1 | NONE | 1704 | 0 | 0 | 0 | PASS |
| ToolAgent | 2 | NONE | 1704 | 0 | 0 | 0 | PASS |
| ToolAgent | 3 | FUTURE_DEMAND | 1704 | 1964868 | 0 | 0 | PASS |
| ToolAgent | 4 | PERSISTENCE h=60s | 1704 | 0 | 0 | 0 | PASS |
| ToolAgent | 5 | PERSISTENCE h=300s | 1704 | 0 | 0 | 0 | PASS |
| ToolAgent | 6 | RECENCY | 1704 | 0 | 0 | 0 | PASS |
| ToolAgent | 7 | PERSISTENCE_COST_AWARE | 1704 | 0 | 0 | 0 | PASS |

C1真实回归继续成立。Conversation的Replay/Oracle observation/boundary-only分别为2890/2904/14，`F((0,),660000,300000)=921`；ToolAgent分别为5775/5797/22，count=834。两条Oracle endpoint diagnostics均为actual=expected，boundary-only请求没有进入replay状态。

真实Pilot Gate A：

| Workload | Line5/7 execution SHA256 | Line5/7 logical SHA256 | Line7 candidates | 最小Cost Score | 诊断缺失 | Gate rejects |
|---|---|---|---:|---:|---:|---:|
| Conversation | `d93ac05bc05650a787a6fa59238bfcdc68a983baed7ebb9ae2582993c34bf65b` | `5f9e14743e3505d2c50397406195a8520fc4dba4d19d587be46560cc6f7ae384` | 28882 | 2.018503871504421 | 0 | 0 |
| ToolAgent | `eed8342293d9715471de81ff360435c4e8498b07bd7eab19ec6c01c03c33e14e` | `e4079e70f7373d3bc4e4b671c66a93dccce484c976ecefd95b3c3285574237a5` | 57736 | 0.8545119604567146 | 0 | 0 |

两组Gate A的static config、distinct truthful policy、execution、logical state、所有非policy-diagnostic系统metrics和Cost diagnostics六项均PASS。Line5保留`PERSISTENCE`，Line7保留`PERSISTENCE_COST_AWARE`；未通过伪造相同policy取得一致性。所有Line7合法shortlist candidate的Score均严格大于0，公式重算容差检查PASS。

Gate B来自实际FutureDemandIndex API计数：Lines1/2及全部history策略的decision reads均为0，两个Oracle行只有oracle reads，history reads均为0；post-hoc observation不计入决策读取。九类表共18个文件，JSON行数依次为Conversation主表7、ToolAgent主表7、合并14、baseline delta14、Oracle delta2、bucket84、Gini14、transfer168、waste14。

随后执行只读独立校验：逐一验证两遍全部文件SHA256，从run_1重新加载records，重算final metrics、execution/logical digest和两组Gate A；33项顶层检查全部true，`independent_validation.json`为PASS。运行启动后冻结的43个TaskMain源码/script/config文件SHA256全部未变；首次INVALID pilot的55个文件仍与C1修复开始时一致。旧根manifest保持INVALID，run_20260917_02与run_20260917_03保持SUPERSEDED_INCOMPLETE，没有将失败或不完整运行改称PASS。

最终证据：

- `results/task_main/stage_c_pilot/run_20260917_04/validation_manifest.json`：PASS。
- 同目录`checkpoint.json`与`progress.json`：PASS；resume_count=0。
- 同目录`independent_validation.json`：PASS。
- 同目录`tables/`：九类CSV/JSON共18个文件。
- `/tmp/task_main_stage_c_optimized_tests.xml`：254 PASS，0 failed，0 skipped。

**Stage C最终状态：PASS，blocker=0。** 该结论只说明pipeline correctness与冻结协议实现通过验收；pilot指标不用于策略性能结论或调参。正式14行、sweep、O2/SR/P3均未运行。完成后停止，等待正式实验的单独授权。
