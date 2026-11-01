# TaskMain v1 Pre-Formal Compliance Check

日期：2026-09-17。仓库：`/root/data2/llm-kv-active-balancing`。审计对象为当前工作区及最终 Stage C PASS Pilot `results/task_main/stage_c_pilot/run_20260917_04/`。

## PRE-FORMAL STATUS

**PRE_FORMAL_BLOCKED**

| Check | Result | Formal blocker |
|---|---|---|
| Empty-transferable protocol/code/test | `A_STATUS = CONSISTENT` | NO |
| NO_CAPACITY alternative feasible target | `B_STATUS = SEMANTIC_DIFFERENCE_OBSERVED`：3225 个 candidate attempts | **YES** |
| action_cap=1 implementation consistency | `C_STATUS = CONSISTENT`；真实 Pilot 的 `>1-action opportunities=0` | NO |
| action_cap=1 / 主实验假设的 Method 文档 | 冻结协议有定义；独立 formal Method/README 未完成且现有脚本 README 已过时 | NO（method docs only） |

阻断原因只有 B：当前协议和实现先按 `(Load, Pod ID)` 唯一选择 target，再做容量 preflight，失败后不尝试同一 chain 的第二个 target；最终 Pilot 中确实存在原 target 失败而其他合法 target 可成功的状态。因此“先选最低 Load target 再查容量”和“从容量可行 target 中选最低 Load”在已执行 Pilot 上并不等价。该发现不表示当前实现违背现行协议；它证明这个已冻结选择规则具有可观察的行为后果，正式实验前需要用户决定是否保持或修改语义。

本轮没有修改 simulator、协议、配置或参数，没有运行正式 14 行、完整 Pilot、sweep、O2、SR 或 P3。正式结果目录不存在。**不能进入正式 14 行，等待用户对 B 的语义作出决定。**

## 审计边界与证据身份

- 最终 Stage C Pilot 的 43 个冻结 TaskMain 源码、脚本和配置文件逐一核对 SHA-256：缺失 0、变化 0。
- 直接只读最终 Pilot 的 run 1 records；run 2 已由 Stage C 验证为逐文件确定性一致。
- 现有 artifacts 不含每个 decision time 的完整备选 target admission 状态，因此 B 使用 10 个相互隔离的单遍只读诊断重放，覆盖两个 workload 的 Lines 3–7。没有重跑双遍 Pilot 或生成策略性能表。
- 探针只在原 `preflight_chain` 返回 `REACTIVE_FALLBACK_CAPACITY` 时，对其他合法 Pod 调用同一个原始、无副作用的 `preflight_chain`。每个诊断 replay 的 execution projection、final logical state 和 final metrics 三类 digest 均与最终 PASS Pilot 完全相同。
- 完整逐事件证据位于 [`capacity_audit.json`](../../results/task_main/preformal_audit/capacity_audit.json)，SHA-256 为 `358e1d0495125eee6796deaaad29776828af9be2827956566b0a2c40b1886772`。文件内部计数、明细数和 replay parity 复核通过。
- 运行的最小相关测试：empty-prefix、主动执行、opportunity 和 isolation 相关 17 项，`17 passed, 10 deselected`。本轮没有重跑完整 254 项套件。

## A. Empty-transferable consistency

`A_STATUS = CONSISTENT`

### 当前冻结规则

```text
gate=false
    -> final_pod=source；无 wire

gate=true && transferable_pages==0
    -> final_pod=source；无 wire；不作纯 Load-balancing direct-target

gate=true && transferable_pages>0
    && target 已完整持有 transferable Prefix
    -> direct target；无 wire；hit 按 target 当前实际连续 Prefix 计算

gate=true && transferable_pages>0
    && target 未持有 transferable Prefix
    -> reactive COPY；preflight 成功到 target，失败 fallback 原 source
```

### Protocol

[`task_main_v1.md`](../protocol/task_main_v1.md) 第 3 节先把 Prefix 转为 `FULL_PAGE_ONLY` 并禁止 zero-page wire；第 6 节第 1–5 项明确规定 gate 优先、空 transferable 留在 source、非空时才进行 direct-target/COPY 分支；第 7 节规定失败回原 source，并以其当前真实 hit 计算 miss。

### Implementation

- `routing.reactive_gate` 使用严格乘法 gate；`least_other_pod` 只在其他 Pods 中按 `(Load, Pod ID)` 选择 target。
- `TaskMainEngine._reactive_assignment` 先判断 gate，再调用 `full_page_prefix`。空 tuple 直接返回 source；仅非空时检查 target ownership。direct-target 分支重新对完整 request 调用 target `cache.lookup`，没有继承 source 的 partial-tail hit。
- `TaskMainEngine._copy_or_fallback` 的 COPY 失败分支重新读取 source 实际 cache hit；成功分支只承诺 transfer chain 的完整页数。
- `IndependentTransfers.preflight_chain` 对空 path 防御性返回 `REACTIVE_FALLBACK_NO_TRANSFERABLE_PREFIX`，不会创建 Transfer、Temporary 或 wire。
- 正式 YAML 没有另一个可改变 empty-prefix 语义的字段；Line 2 使用冻结的 `R_REQ_KV_TASK`、512-token page、theta=2 和 COPY。

### Tests 与四个边界

| Case | 当前断言 | 测试证据 |
|---|---|---|
| A1 partial-tail-only、gate=false | source、保留实际 partial hit、无 wire | `test_empty_transferable_prefix_gate_false_stays_source_without_wire` |
| A2 partial-tail-only、gate=true | source、保留实际 partial hit、无 wire | `test_empty_transferable_prefix_gate_true_stays_source_without_wire` |
| A3 full pages + partial tail、target 已有 full Prefix、gate=true | direct target；target 只命中其真实 full Prefix；partial tail 计 miss | `test_gate_true_direct_target_compares_full_page_prefix_and_charges_actual_miss` |
| A4 full pages + partial tail、target 缺 Prefix、gate=true | 仅 COPY full pages；final hit 不含 source partial tail | `test_partial_tail_copy_commits_only_full_page_hit` |

补充覆盖包括 `test_gate_false_stays_source_even_when_target_owns_transferable_prefix`、`test_partial_tail_is_local_only_and_miss_committed_before_ready` 和 `test_no_full_page_fallback_has_no_zero_wire_transfer`。Stage A 实施报告第 1、5、8 节记录的最终规则与当前协议、代码和测试一致。actual local hit 与 FULL_PAGE_ONLY transferable 没有混淆，不会创建 zero-page transfer。

## B. Alternate-target capacity audit

`B_STATUS = SEMANTIC_DIFFERENCE_OBSERVED`

### 判定方法

每个原始 `SKIP_NO_CAPACITY` 时点，对除 source 和原 target 外的 Pods 逐一检查：

1. 尚未完整持有同一 chain；
2. 没有 equal/deeper in-flight proactive coverage；
3. 使用当时 Cache、pin、Temporary 和 leaf-LRU 状态；
4. 调用与原 target 完全相同的非破坏性 `IndependentTransfers.preflight_chain`；
5. 至少一个 alternate 返回有效 plan，即计一个 `alternative_target_feasible` candidate attempt。

若有多个可行 alternate，Load 差值统计选其中 `(Load, Pod ID)` 最小者，定义为 `alternate Load - originally selected target Load`。深度直方图和所有可行 alternate 均保存在诊断 artifact。`total candidate attempts` 包含真正进入 Cost gate/preflight 的记录，排除成功后标为 `NOT_ATTEMPTED_CAP_REACHED` 的候选。

### 按 workload / line 的结果

| Workload | Line | Candidate attempts | SKIP_NO_CAPACITY | Unique skip opps | Alt feasible | Unique impacted opps | Alt feasible / skips | Alt wire tokens |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Conversation | 3 | 1,992 | 9 | 2 | 0 | 0 | 0.00% | 0 |
| Conversation | 4 | 3,296 | 443 | 79 | 248 | 46 | 55.98% | 11,256,832 |
| Conversation | 5 | 2,975 | 94 | 15 | 33 | 6 | 35.11% | 1,819,136 |
| Conversation | 6 | 1,561 | 0 | 0 | 0 | 0 | NA | 0 |
| Conversation | 7 | 2,975 | 94 | 15 | 33 | 6 | 35.11% | 1,819,136 |
| ToolAgent | 3 | 6,479 | 2,573 | 55 | 519 | 20 | 20.17% | 8,140,288 |
| ToolAgent | 4 | 8,957 | 3,541 | 571 | 1,480 | 275 | 41.80% | 34,819,072 |
| ToolAgent | 5 | 7,504 | 1,883 | 251 | 456 | 70 | 24.22% | 24,894,464 |
| ToolAgent | 6 | 2,338 | 0 | 0 | 0 | 0 | NA | 0 |
| ToolAgent | 7 | 7,504 | 1,883 | 251 | 456 | 70 | 24.22% | 24,894,464 |
| **按配置行合计** |  | **45,581** | **10,520** | **1,239** | **3,225** | **493** | **30.66%** | **107,643,392** |

Line 5/7 按冻结 Gate A 是 execution-equivalent，因此两行具有相同事件；上表按用户要求分别计入，合计数也按配置行计数，不能解释为去重后的物理事件数。

### 深度与 Load 差值分布

| Workload | Line | Feasible chain depth pages min / median / P95 / max | Best feasible alternate Load delta min / median / P95 / max |
|---|---:|---|---|
| Conversation | 3 | NA | NA |
| Conversation | 4 | 2 / 63.5 / 221.65 / 235 | 1,252 / 16,995 / 64,491 / 137,445 |
| Conversation | 5 | 21 / 39 / 235 / 235 | 14,603 / 14,603 / 20,498 / 46,107 |
| Conversation | 6 | NA | NA |
| Conversation | 7 | 21 / 39 / 235 / 235 | 14,603 / 14,603 / 20,498 / 46,107 |
| ToolAgent | 3 | 2 / 18 / 73 / 161 | 584 / 10,598 / 24,720 / 54,156 |
| ToolAgent | 4 | 1 / 21 / 159 / 221 | 313 / 23,962 / 86,264 / 160,911 |
| ToolAgent | 5 | 1 / 144 / 160 / 192 | 350 / 15,761 / 120,667 / 141,256 |
| ToolAgent | 6 | NA | NA |
| ToolAgent | 7 | 1 / 144 / 160 / 192 | 350 / 15,761 / 120,667 / 141,256 |

### 可复核实例

| Workload / Line | Opportunity / request / time(ms) | Chain ID | Depth / tokens | Source | Original target / Load | Failure | Feasible alternate / Load / delta |
|---|---|---|---:|---:|---|---|---|
| Conversation 4 | 711 / 711 / 234000 | `(0,13736)` | 2 / 1,024 | 0 | 2 / 521,678 | `REACTIVE_FALLBACK_CAPACITY` | 3 / 532,903 / +11,225 |
| Conversation 5,7 | 115 / 115 / 39000 | `(0,3098,3099,3100,3101,3102,3103,3104,3105,3106,3107,3108,3109,3110,3111,3112,3113,3114,3115,3116,3117)` | 21 / 10,752 | 2 | 1 / 387,355 | `REACTIVE_FALLBACK_CAPACITY` | 3 / 433,462 / +46,107 |
| ToolAgent 3 | 2557 / 2557 / 440999 | `(0,26798)` | 2 / 1,024 | 3 | 0 / 507,061 | `REACTIVE_FALLBACK_CAPACITY` | 2 / 507,750 / +689 |
| ToolAgent 4 | 3995 / 3995 / 669000 | `(74,)` | 1 / 512 | 1 | 0 / 475,904 | `REACTIVE_FALLBACK_CAPACITY` | 2 / 510,342 / +34,438 |
| ToolAgent 5,7 | 3408 / 3408 / 585000 | `(74,)` | 1 / 512 | 0 | 1 / 509,483 | `REACTIVE_FALLBACK_CAPACITY` | 2 / 630,150 / +120,667 |

例如 Conversation Line 4 的原 target 2 当时为 resident=0、Temporary=585，无法容纳两页；alternate target 3 为 resident=406、Temporary=179，按同一 preflight 可合法 eviction 两页并成功。ToolAgent Line 3 的原 target 0 同样为 Temporary=585，而 alternate target 2 只有 534 resident pages，无需 eviction 即可接收两页。这些检查使用决策当时状态，不使用未来状态。

### 含义

当前协议第 17 节和实现彼此一致：`CandidateUniverse.materialize` 先唯一选择最低 `(Load, Pod ID)` target；`ProactiveController.execute` 对其 preflight，失败则继续下一个 chain，不试同 chain 的第二 target。合成测试 `test_capacity_skip_continues_next_chain_never_second_target` 也明确冻结这一行为。

本审计没有把另一种语义写入实现。由于实际 Pilot 已观察到差异，正式实验前必须在以下两者中由用户明确选择：保持当前“target-first”规则，或另行授权改为“capacity-feasible-target-first”并重新验收。报告不对二者作策略效果评价。

## C. Action-cap=1 consistency

`C_STATUS = CONSISTENT`

### Protocol / implementation / tests / config / validation

- 协议第 12 节规定每个 external request 恰好一个 opportunity；第 15 节第 3 项及第 17 节规定每 opportunity 最多成功启动一个 proactive COPY，成功后停止扫描；第 17 节还明确 `K=10` 不是 action cap。
- `TaskMainEngine._finish` 每次 request completion 构造一个 opportunity，并只调用一次 `ProactiveController.execute`。proactive transfer completion 仅调用 `proactive.complete`，不会调用 `_finish` 或创建新 opportunity。
- `ProactiveController.execute` 按 rank 顺序继续越过 Cost gate/preflight 失败项；第一个成功项启动 transfer、把余项标记为 `NOT_ATTEMPTED_CAP_REACHED`，随后立即 return。
- `engine.run` 的 validation 用 proactive action 的 unique opportunity IDs 验证 `proactive_action_cap`，并验证 action/transfer 守恒。基础 summary 同时验证 request/opportunity 一一对应。
- `test_capacity_skip_continues_next_chain_never_second_target` 验证高排名失败后可继续下一 chain；`test_success_stops_scanning_demand_load_reuse_opportunity_conserved` 验证首次成功后余项不再尝试；`test_proactive_ready_before_same_timestamp_arrival_does_not_create_opportunity` 验证主动 transfer ready 不产生 opportunity。Stage A 的 direct/reactive 路径也覆盖 opportunity uniqueness。
- formal configs 没有可调 `action_cap` 字段，因而 14 行不能配置出其他值；未知 `action_cap` 会被 dataclass YAML allowlist 拒绝。每行 `action=COPY`、K=10，实际 cap 由冻结控制流固定为 1。配置与实现一致，但 cap 没有作为显式 provenance/config 字段落盘。

### 最终 Pilot 实证

以下读取每个 case 的 run 1；run 2 为逐文件确定性相同。每行 `opportunity_count=request_count`。

| Workload | Line | Opportunities | Zero-action | One-action | >1-action | Max actions/opportunity |
|---|---:|---:|---:|---:|---:|---:|
| Conversation | 3 | 2,890 | 907 | 1,983 | 0 | 1 |
| Conversation | 4 | 2,890 | 37 | 2,853 | 0 | 1 |
| Conversation | 5 | 2,890 | 9 | 2,881 | 0 | 1 |
| Conversation | 6 | 2,890 | 1,329 | 1,561 | 0 | 1 |
| Conversation | 7 | 2,890 | 9 | 2,881 | 0 | 1 |
| ToolAgent | 3 | 5,775 | 1,869 | 3,906 | 0 | 1 |
| ToolAgent | 4 | 5,775 | 359 | 5,416 | 0 | 1 |
| ToolAgent | 5 | 5,775 | 154 | 5,621 | 0 | 1 |
| ToolAgent | 6 | 5,775 | 3,437 | 2,338 | 0 | 1 |
| ToolAgent | 7 | 5,775 | 154 | 5,621 | 0 | 1 |

`>1-action opportunities` 全部为 0，且 max=1。没有实现违反。

### 方法学定位与 Oracle 名称

`action_cap=1` 是 **TaskMain v1 additional scheduling constraint / action-budget assumption**，不能写成原始任务已经明确要求，也不是生产系统真实限制。其方法学作用应限定为：给每次决策明确且可复现的主动动作预算、保持 trigger 策略可比较，并防止一个 external request 触发无界或大量并发复制。

Line 3 应称为 **Future-Demand Oracle Reference under the TaskMain v1 action budget**。它有未来需求知识，但仍受 action_cap=1、有限容量、wire delay、当前 online candidate、FULL_PAGE_ONLY 和冻结 target policy 约束；不能称为 mathematical global optimum、unlimited proactive oracle 或 theoretical maximum achievable benefit。

当前协议已分别写明每 opportunity cap=1、Oracle 受当前 candidates/placement 约束且“不是全局最优上界”，语义上没有错误；但使用了 `global Future-Demand Reference`，没有在一个正式 Method 段落中直接写出 “under the TaskMain v1 action budget”。这是文档 TODO，不是 simulator blocker。

## 正式方法假设清单

| Assumption | Protocol | Formal Method / README |
|---|---|---|
| P-only replay | DOCUMENTED（第 2 节） | MISSING_FROM_METHOD |
| N=4 | DOCUMENTED（第 2 节） | MISSING_FROM_METHOD |
| 585 pages/Pod | DOCUMENTED（第 2、9 节） | MISSING_FROM_METHOD |
| 512 tokens/page | DOCUMENTED（第 2、3 节） | MISSING_FROM_METHOD |
| FULL_PAGE_ONLY cross-Pod | DOCUMENTED（第 3 节） | MISSING_FROM_METHOD |
| independent 25 GB/s per transfer | DOCUMENTED（第 8 节） | MISSING_FROM_METHOD |
| 60s miss-token Load proxy | DOCUMENTED（第 4 节） | MISSING_FROM_METHOD |
| theta=2 | DOCUMENTED（第 2、6 节） | MISSING_FROM_METHOD |
| action_cap=1 | DOCUMENTED（第 15、17 节） | MISSING_FROM_METHOD |
| leaf-LRU | DOCUMENTED（第 10 节） | MISSING_FROM_METHOD |
| `[25,45)` primary common-support Evaluation | DOCUMENTED（第 2、18 节） | MISSING_FROM_METHOD |
| Future-Demand Oracle Reference | DOCUMENTED（第 15、17 节；需同步 action-budget 名称） | MISSING_FROM_METHOD |
| W=300s | DOCUMENTED（第 17 节） | MISSING_FROM_METHOD |
| no GPU service-time / FCFS model | DOCUMENTED（第 1、17 节） | MISSING_FROM_METHOD |

仓库没有当前 TaskMain v1 的独立 formal Method/README。`scripts/task_main/README.md` 仍以 Stage A 说明开头，并错误声称尚无正式批处理、14 行配置或 Stage B 策略；后半仅记录 Stage C Pilot 的 tmux 命令。`docs/README.md` 是历史 `TaskMain-v1.1` 注册表，不是本协议的正式方法说明。正式报告前应补一个整合后的 Method 文档并同步 Oracle 名称与 action-budget 定位，但本轮按指令不修改这些文档。

## 最终结论

**PRE_FORMAL_BLOCKED**

- A：协议、实现、测试和 Stage A 报告完全一致。
- B：真实 Pilot 中观察到 3225 个同链备选 target 可行的容量失败 attempt，构成正式实验前语义决定 blocker。
- C：action_cap=1 的协议、控制流、测试、validation 和真实 Pilot 一致；没有实现 blocker。
- Method：冻结协议已记录关键假设，但当前 formal Method/README 缺失且脚本 README 过时。

没有修改已冻结行为。正式 14 行尚未运行，也不应在 B 获得明确决策前启动。
