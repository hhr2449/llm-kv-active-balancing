# TaskMain Stage B 实施记录

日期：2026-09-17。仓库：`/root/data2/llm-kv-active-balancing`。

**Stage B 完成：166项测试全部通过，5项固定合成 correctness smoke 全部通过，blocker=0。** 未运行正式14行、最终指标评价、参数 sweep、O2/SR/P3，也未运行本阶段的 real-trace smoke。合成动作计数不用于策略性能结论。完成后停止，等待 Stage C。

## 1. 协议与 B1 解决

先更新 `docs/protocol/task_main_v1.md` 第15节 execution-equivalence invariant，并在第17节补入获授权的 Stage B 规则，再实施代码。此前完整 records 判等与真实 policy 元数据冲突，已按用户确认解决：执行行为相同，来源和评分诊断允许不同。不改变 Cost-aware 公式、系数、单位、排序或 gate。

Line5 保留 `PERSISTENCE`，Line7 保留 `PERSISTENCE_COST_AWARE`，后者真实保存 benefit/congestion/transfer_cost_seconds/cost_score。没有伪造相同 policy，也没有在测试中随意 drop columns。

Stage A 的 Load、routing、Cache、wire timing、LRU、completion、事件顺序、opportunity 唯一性保持原语义；Stage A 报告和既有 smoke 未改写。

## 2. 新增与修改文件

路径相对仓库根目录。

| 文件 | 工作 |
|---|---|
| `src/simulator/task_main/candidates.py`（新增） | 完整 online universe、source/target、ancestry |
| `src/simulator/task_main/history.py`（新增） | 独立 demand/reuse/future history、bucket |
| `src/simulator/task_main/policies.py`（新增） | Oracle/Persistence/Recency/Cost-aware、quantile、gate |
| `src/simulator/task_main/proactive.py`（新增） | opportunity 扫描/preflight/COPY 与审计 |
| `src/simulator/task_main/equivalence.py`（新增） | 显式 execution projection |
| `src/simulator/task_main/config.py` | Stage B version、policy allowlist、冻结参数与 routing 组合校验 |
| `src/simulator/task_main/engine.py` | ARRIVAL/完成 history hook、现有 opportunity 执行策略、proactive ready、final state digest |
| `src/simulator/task_main/transfer.py` | 共用完整 chain preflight、PROACTIVE type，保持原 timing/accounting/pin |
| `src/simulator/task_main/records.py` | opportunity/action/shortlist audit、RunResult 最终状态 |
| `src/simulator/task_main/metrics.py` | stage/type 标识、最小系统统计、新 JSONL writer |
| `tests/task_main/conftest.py`（新增） | 合成 Trace/config/cache 辅助 |
| `tests/task_main/test_candidates.py`（新增） | Candidate 测试 |
| `tests/task_main/test_policies_oracle.py`（新增） | Oracle 测试 |
| `tests/task_main/test_policies_persistence.py`（新增） | Persistence 测试 |
| `tests/task_main/test_policies_recency.py`（新增） | Recency 测试 |
| `tests/task_main/test_policies_cost_aware.py`（新增） | Cost-aware 与 Line5/7 等价测试 |
| `tests/task_main/test_proactive_execution.py`（新增） | 主动执行测试 |
| `tests/task_main/test_stage_b_isolation.py`（新增） | 配置、未来与 legacy 隔离测试 |
| `scripts/task_main/run_stage_b_smoke.py`（新增） | 固定7请求、五配置、两遍哈希的合成 smoke |
| `docs/protocol/task_main_v1.md` | Stage B 冻结规则与判等契约 |
| `docs/analysis/task_main_stage_b_implementation.md` | 本报告，替换此前 B1 预检查状态 |

`cache.py`、`load.py`、`routing.py` 未改动。没有导入旧 oracle/strategies 或旧 engine，没有创建正式14行配置。

## 3. Candidate universe 与结构合法性

每个 external ARRIVAL 注册 FULL_PAGE_ONLY 路径的所有非空 ancestors；`[a,b,c]` 产生三个 candidate，不限 endpoint/branch/leaf。partial tail 在注册前排除。chain_id 为完整 hash path tuple，按 tuple 字典序稳定 tie-break；identity/depth/score/wire/持有判断对应同一 chain，不依赖未来树结构。

source 为当前完整 Published holders 中最小 Pod ID。target 从缺完整副本、且没有 equal/deeper in-flight proactive chain 覆盖的其他 Pods 中按 `(Load, Pod ID)` 唯一选择。已完成 transfer 不再覆盖；浅 chain 不阻止更深 candidate。

无 source、所有 Pods 完整持有或缺副本 targets 全被覆盖时不进入 policy ranking。capacity 不参与 structural set、Top-K 或 quantile；没有 affinity/busy/queue tie-break。确定唯一 target 后才 preflight；失败继续下一 chain，不换同 chain 的第二 target。

## 4. 独立 histories 与策略

**FutureDemandIndex** 只保存完整 chain 到 arrival timestamps 的索引及 visibility，不保存 request 对象或 simulator state。Oracle 仅给当前 online structural candidates 查询 `(t,t+300000]`，左开右闭、每 request/chain 一次。正分全部按 count/depth/id 排序，无固定K。`t+W>3537000` 记 FUTURE_WINDOW_CENSORED，不把未知当0。这是 global demand reference，不是 future-placement oracle 或数学全局最优上界。

**ExternalDemandHistory** 在 ARRIVAL 用原时间为 full ancestors 各登记一次 demand 和 Prompt bucket。Persistence 统计 `(t-h,t]` count>0，按 count/depth/id 取 Top10，h=60s或300s。等待 reactive ready 的已到达请求已在历史中；COPY 不增加 demand。history-only 策略不因 future visibility 截止而停止。

**ReuseHistory** 只在 abstract completion 为实际 hit 覆盖的 full ancestors 各登记一次，时间为 completion。partial tail 不成为 chain。cold suffix、arrival、probe、candidate 枚举、COPY、ready 和 dedup 不产生 reuse。reactive ready 后的真实请求完成会 observe，单纯 transfer complete 不会。

Recency 为 `sum exp(-(t-ti)/60000)`。先 structural/in-flight 过滤，再在正分集合计算 q=.9：排序后位置 `(n-1)*.9`，相邻点线性插值；单点返回自身。score>=threshold 入 shortlist，按 score/depth/id 排序；capacity 不参与分位数集合。

普通策略不实例化 FutureDemandIndex，Policy 构造器也拒绝给非 Oracle 策略注入 future index。测试以 fail-fast 构造器证明 NONE/Persistence/Recency/Cost-aware 均不触碰真实未来索引。

## 5. Cost-aware 与 execution-equivalence

bucket 为 `<5000`、`[5000,20000)`、`[20000,60000)`、`[60000,120000)`、`[120000,300000]`、`>300000`，乘子为1.3/2.5/5.3/8.9/11.3/15.2。

```text
n_hat_b = (W/h) * n_history_b
benefit = sum_b n_hat_b * weight_b
congestion = Lt/max(L), or 0 when max(L)=0
transfer_cost_seconds = wire_pages * 14680064 / 25000000000
cost_score = benefit - .5*congestion - .1*transfer_cost_seconds
gate = cost_score > 0
```

Line7 固定 W=h=300s，严格保留纯 Persistence count Top10 及其顺序。完整 shortlist 计算并保存 diagnostics，但不按 benefit/Cost Score 重排，不改变 target/capacity。没有 references、归一化、校准或 future action value。

满585页的数学 lower bound 为 `1.3-.5-.1*(585*14MiB/25e9)=0.76564865024>0`。测试覆盖零 Load、congestion 范围、seconds 单位、Score=0 严格拒绝、weighted benefit 不能改变 count 排序。

`execution_projection(result)` 明确比较：逐 opportunity 的 time/request、候选与尝试计数、no-action/status、structural skip counts、shortlist rank/chain/source/target/count-score/status；动作的 chain/depth/source/target/wire/start/ready/status及顺序；全部 request/transfer/event records；完整最终 state；summary 除 `provenance`、`policy_diagnostics` 两个明确元数据分组之外的所有字段。新增系统统计会自动纳入判等。

**行为验收 PASS**：非平凡合成 integration 的 Line5/7 projection 和 final_state_digest 完全相等；人为改变一个 action target 后投影能检测不同。

**诊断验收 PASS**：两种真实 policy 保留差异；Line7 全部合法 shortlist rows 有完整分解且 Score>0；cost_gate_rejected_count=0。不要求整个原始文件或 provenance 相同。

## 6. 主动执行状态机

沿用 external completion -> Prompt attempt -> 唯一 opportunity。B 在此执行 structural candidates -> positive/shortlist -> 按序 gate/preflight -> 第一个成功 COPY -> 停扫。NONE 保留 A 行为。

容量失败记 SKIP_NO_CAPACITY，无 eviction/Temporary/wire/Load mutation，继续下一个 chain；preflight 重新确认 source，失效记 SOURCE_STALE。成功后未扫描项记 NOT_ATTEMPTED_CAP_REACHED，保留 shortlist rank/score。无 source/target、in-flight覆盖、零分、future窗口不完整分别保存明确原因。

成功动作进入原 ready heap，同一 full-wire Temporary/source pin/独立25GB/s/ready dedup。时间 t 先完成全部 transfer，再恢复 reactive requests，最后逐个 arrivals。proactive complete 只更新 transfer/action status，不完成新请求，不产生 opportunity/demand/reuse，不改 Load。COPY 新页使用 A 的 ready insertion LRU，duplicate/source 不刷新。

运行期新增断言：每 opportunity 成功<=1、action与PROACTIVE transfer一一对应、所有 action完成；保留原请求/Load/opportunity、容量、pin/Temporary drain、wire非零等断言。

## 7. Records schema

Opportunity 保留 A 的 time/workload/load/cache/split，扩展 policy、structural/positive/shortlist/attempted count、selected chain/source/target/transfer、final_status 和 structural_rejections 聚合计数。

`proactive_action_records.jsonl` 保存 action/opportunity/request ID、真实 policy、decision_time、chain/depth/source/target、policy_score、nullable Oracle/Persistence/Recency score、nullable benefit/congestion/transfer_cost_seconds/cost_score、wire pages/tokens/bytes、transfer_id、start/ready、split、status。

`candidate_decision_records.jsonl` 只保存 shortlist 的 rank/chain/source/target、score/diagnostics、selected/skip/未扫描 status，不倾倒全部结构候选；Oracle shortlist 按协议为所有正分候选。其余结构拒绝只聚合计数。

RunResult 暴露完整最终 Cache snapshots、Load vector/entries、transfer records、pending/ready state；summary 保存 final_state_digest。最小统计仅用于守恒与一致性，不是最终 Saved/Weighted/Gini/wasted-copy 主报表。

## 8. 测试与覆盖

```text
python -m pytest -q tests/task_main tests/test_trace.py tests/test_cache.py \
  --junitxml=/tmp/task_main_stage_b_tests.xml
166 passed in 2.72s
```

0 failed、0 skipped。新增 Stage B 80项，原 Stage A 70项及共享 Trace/Cache 16项继续通过；参数化实例计入总数。

| 新测试文件 | 数量 | 覆盖结果（全部 PASS） |
|---|---:|---|
| test_candidates | 10 | 中间Prefix、partial、future-only/unseen、无source/全持有、min-ID source、Load/id target、equal/deeper/shallow ancestry、已覆盖目标筛除、capacity/affinity不改变结构集合 |
| test_policies_oracle | 5 | 精确未来窗/去重、score/depth/id、正分且超过10项、未来Prefix不注册、无placement/state接口、visibility censor、partial排除 |
| test_policies_persistence | 5 | ARRIVAL/ancestors/去重、精确历史窗、count/depth/id/Top10、reactive wait已有demand未有reuse、K不是动作数、tail继续 |
| test_policies_recency | 9 | 实际full ancestors/partial、cold/copy/probe/ready不刷新、cold到hit、60s decay、4种quantile边界、in-flight先过滤、capacity不参与、排序 |
| test_policies_cost_aware | 18 | 10个bucket端点、bucket counts/W÷h、精确秒公式/零Load/congestion、3个gate边界、585页lower bound、weighted benefit不改序、Line5/7行为与诊断双验收 |
| test_proactive_execution | 7 | 容量失败续扫且不换第二target、成功停扫/cap、无额外Load/demand/reuse/opportunity、Temporary/invisible/source pin/并发、ready先于arrival、Oracle censor、stale source无副作用、记录确定性 |
| test_stage_b_isolation | 26 | 4个R_REQ+proactive拒绝、7个冻结配置错误、4个history-only未来构造/注入拒绝、全策略runtime guard、10个旧YAML字段拒绝 |

原 Source pin、leaf-LRU、duplicate commit、atomic Prompt admission、committed-hit protection、精确事件顺序等回归全部保留并通过。

## 9. 合成 smoke 与 deterministic validation

只在上述166项全部通过后运行：

```text
python -m scripts.task_main.run_stage_b_smoke \
  --output results/task_main/stage_b_smoke
```

固定7个合成请求，4 Pods/585 pages；保存输入、每项配置和两遍独立输出。汇总：[smoke_validation.json](../../results/task_main/stage_b_smoke/smoke_validation.json)。所有参数按冻结值，无调参。

| Case | Requests / opportunities | COPY actions | 两遍输出一致 | runtime checks |
|---|---|---:|---|---|
| oracle | 7 / 7 | 5 | PASS | PASS |
| persist60 | 7 / 7 | 6 | PASS | PASS |
| persist300 | 7 / 7 | 7 | PASS | PASS |
| recency | 7 / 7 | 6 | PASS | PASS |
| cost | 7 / 7 | 7 | PASS | PASS |

仅证明候选、COPY、cap、drain和记录链路工作，不比较性能。每项8个JSON/JSONL文件两遍SHA-256全部一致，执行时启用 runtime legacy import guard。

persist300/cost 共同 execution projection SHA-256：

```text
af3a818e317d5b7230d8eb00d11af452395caa6e5b5890beba848f9bd6ad07eb
```

共同 final_state_digest：

```text
7a1471155f2bf2dfce46e6ebfd32098e7bb09fee62f5ae9982cac9bc592751a2
```

Cost-aware 全部 shortlist Score>0，cost_gate_rejected_count=0，真实 policy 保持不同。

## 10. Isolation、历史保留与边界

AST dependency allowlist 覆盖新模块并记录源码SHA-256；所有政策在 fail-fast runtime guard 下执行。仅复用安全共享 Trace，父包仍导出旧 config 类但不实例化；不导入旧 engine/service/Pod/routing/oracle/strategies/RemotePressure/NodeGate/ECT/O2/P3。没有 tick/TokenBucket/timeout/replan/旧 references。

Stage A 开始前保存的279个历史文件再次SHA-256核对：279/279未变。既有Stage A smoke两遍共48个输出hash再次核对：全部未变。cache.py/load.py/routing.py 与旧 smoke provenance 中源码hash完全一致。获准扩展的其他文件hash自然变化，当前工作区不能冒称为旧smoke当时版本；旧结果保持原样。

未创建commit，未覆盖历史configs/scripts/results/tests。B1已解决，当前无Stage B blocker。最终指标全集、正式矩阵与Stage C报表尚未实施，属于本阶段边界。未运行正式14行、最终指标评价、sweep、O2/SR/P3。完成后停止，等待Stage C。
