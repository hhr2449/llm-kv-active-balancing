# TaskMain Stage A 实施记录

日期：2026-09-17。仓库：`/root/data2/llm-kv-active-balancing`。

**状态：Stage A 修订与最终验收完成；86 项测试、4 项真实输入 correctness smoke 全部通过，blocker=0。** R_AFF 和 R_REQ_KV_TASK 均经模拟器正常执行路径验证。smoke 不用于策略性能结论；未运行正式 Mooncake 14 行、Oracle/Persistence/Recency/Cost-aware、sweep、O2/SR/P3。

## 1. 协议与已确认路由顺序

先创建 `docs/protocol/task_main_v1.md`，再实现独立执行路径。用户于 2026-09-17 补充确认后，首先更新协议第 6 节，然后补齐正常执行路由：

1. source=R_AFF；target 为其他 Pods 中 `(Load, Pod ID)` 最小者。
2. 先判断严格乘法 gate `Ls > 2*Lt`；false 必须保留 source，不转 target、无 wire。
3. true 时先计算 FULL_PAGE_ONLY Prefix；为空则仍留在 source，无 wire。
4. 非空且 target 已有该 Prefix，则直接 target，按其实际本地 hit 计算 miss，无 wire。
5. 否则 preflight COPY，成功则 target、立即 Load 入账并等待 ready；失败则原 source、真实本地 hit、立即完成。

例如 source 有 `[a, partial-b]`、target 有 `[a]`，gate=true 时可直接 target；本请求只计 target 实际命中的 a，partial-b 仍计 miss。转换为空 Prefix 时即使 gate=true 也保留 source，不创建零页 wire，不做纯 Load balancing direct-target。gate=false 即使 target 已持有可迁移 Prefix 也不能转 target。

正常执行 router 的占位异常及 CLI 禁用已删除；direct-target、严格 gate、成功 COPY、容量 fallback、两条 baseline CLI 和确定性均有通过的测试。当前无 Stage A blocker。Stage B 与正式实验仍不在本阶段授权范围。

## 2. 新增文件与实际架构

全部路径相对仓库根目录。本阶段只新增或续改 TaskMain Stage A 文件，没有修改阶段开始前的历史源码、配置、文档或测试。

| 文件 | 实际内容 |
|---|---|
| `docs/protocol/task_main_v1.md` | Stage A 规范、空 Prefix 修订、committed-hit protection、仅记录的 Stage B 规则 |
| `src/simulator/task_main/__init__.py` | 新配置入口 |
| `src/simulator/task_main/config.py` | 独立严格字段 allowlist、冻结参数、arrival split |
| `src/simulator/task_main/trace.py` | 安全旧 Trace 原语适配、identity 校验、FULL_PAGE_ONLY 转换 |
| `src/simulator/task_main/cache.py` | 独立 trie/leaf-LRU、无副作用 admission plan、pin、Temporary、dedup、reuse 记录 |
| `src/simulator/task_main/load.py` | final-assignment committed miss-token history、精确 60s 过期 |
| `src/simulator/task_main/routing.py` | R_AFF、other-Pod target 排序、严格 gate 原语 |
| `src/simulator/task_main/transfer.py` | 只读 preflight、全 wire Temporary、独立带宽、ready 发布与 unpin |
| `src/simulator/task_main/engine.py` | 两条正常执行 routing baseline、arrival/ready 循环、请求完成、Prompt admission、唯一 opportunity |
| `src/simulator/task_main/records.py` | request/transfer/opportunity/event 记录 |
| `src/simulator/task_main/metrics.py` | 最小 summary、runtime invariants、JSON/JSONL writer |
| `src/simulator/task_main/isolation.py` | AST 依赖 allowlist、禁止符号检查、源码 SHA-256 provenance |
| `configs/task_main/stage_a_r_aff.yaml` | synthetic 单 baseline 配置，固定 N=4/585，非正式矩阵 |
| `configs/task_main/stage_a_r_req_kv_task.yaml` | reactive synthetic baseline 配置，同一冻结参数 |
| `scripts/task_main/run_stage_a.py` | 显式单输入入口、输入 SHA-256、新输出目录保护 |
| `scripts/task_main/run_stage_a_smoke.py` | 固定四项前五分钟 correctness smoke、每项两遍哈希验证、runtime import guard |
| `scripts/task_main/README.md` | 两条 baseline 的调用说明与阶段范围 |
| `tests/task_main/test_primitives.py` | 32 个 Cache/Load/Trace/transfer/config 合成用例 |
| `tests/task_main/test_routing.py` | 5 个 routing 原语用例 |
| `tests/task_main/test_engine_affinity.py` | 7 个 R_AFF 端到端/输出用例 |
| `tests/task_main/test_transfer_events.py` | 6 个注入 assignment 的独立事件循环用例 |
| `tests/task_main/test_engine_reactive.py` | 16 个正常执行 routing/transfer/两 baseline 确定性用例，不注入 routing 或 Load |
| `tests/task_main/test_isolation_cli.py` | 4 个静态依赖、运行期 fail-fast、双 baseline 合成 CLI 用例 |
| `docs/analysis/task_main_stage_a_implementation.md` | 本报告 |

执行路径：新 config -> 安全 Trace -> 新 Cache/Load -> R_AFF 或 R_REQ_KV_TASK -> 立即 assignment/load -> 立即完成或等待新 transfer ready -> Prompt 原子 admission -> 一次 opportunity -> 新 records/summary/validation。没有 GPU 服务过程。

同时间先完成所有 transfer 发布，再按 request ID 恢复请求，最后逐一处理 arrivals。恢复批次在处理 Prompt admission 前暂时保护各请求已承诺的 hit path，完成该请求后释放保护，以免前一个恢复请求的写入破坏后一个请求已确定的 hit。此保护不刷新 LRU、不作为 endpoint gate。请求 final hit 在 assignment 决定一次，后续其他请求的写入不使它重新决策。

传输成功按 `wire_bytes/25e9*1000` 计算 ms 时长；每条独立得到 25 GB/s。容量模型仅 PublishedUnique+各 transfer 的完整 Temporary。14 MiB/page 在 provenance 中标明为 Qwen 等效模拟假设。

## 3. 复用与历史隔离

仅复用 `src/simulator/trace.py` 的 `TraceRequest`、`load_trace` 和实际 valid-token hit 计算。新的 Cache 是独立实现，没有继承或修改旧 Cache；仍运行旧 Cache 回归来确认历史行为未受影响。

Python 导入 `src.simulator` 会执行既有 `__init__.py`，其中导出旧 config 类和 Trace 类型。没有实例化旧 config；不能将这一事实误写成“绝不导入任何旧模块”。新执行层没有导入旧 engine、Pod、service、routing、tickets、pressure、strategies、Oracle、reactive_ect 或 O2/P3 模块。

证据：

- AST allowlist 遍历新包及安全共享 Trace，检查实际 import 和被禁止的 Name/Attribute；provenance 包含模块 SHA-256 与 dependency edges。
- 独立子进程在导入前安装 fail-fast import finder；除新包和共享 Trace/父包 config 外，任何 `src.simulator.*` 旧模块导入都会失败。该进程成功执行 R_AFF、实际发生 COPY 的正常执行 R_REQ_KV_TASK，以及独立 transfer 原语。
- 新配置拒绝旧 service、proactive/tick/budget、timeout/control、Oracle 和旧 Cost-aware reference 字段；未知字段不会被静默忽略。

两条正常执行 baseline 均通过该隔离检查，依赖证据写入每次 summary provenance；provenance 也明确记录 gate 优先与 FULL_PAGE_ONLY direct/COPY 决策规则。

## 4. 实际运行的检查

```text
python -m pytest -q tests/task_main tests/test_trace.py tests/test_cache.py \
  --junitxml=/tmp/task_main_stage_a_tests.xml
86 passed in 1.54s
```

其中 Stage A 合成测试 70 个、共享 Trace 4 个、共享 Cache 12 个；最终完整检查无失败、无 skip。此前仅运行合成测试；本轮另执行前五分钟真实输入 correctness smoke，见最终验收节。保留早期注入 assignment 的隔离测试，并新增正常执行路径测试；不以注入测试替代正常执行 routing 验收。

下面列出每个新增测试函数的结果。参数化函数注明每一个参数，所有实例均 PASS。

| 测试文件 | 测试函数（省略 `test_`） | 结果 |
|---|---|---|
| primitives | load_accounts_at_assignment_and_expires_at_exact_left_boundary | PASS |
| primitives | full_page_conversion_preserves_local_partial_tail | PASS |
| primitives | copy_preflight_failure_has_no_eviction_temporary_or_wire | PASS |
| primitives | source_pin_prevents_leaf_eviction_without_refreshing_access | PASS |
| primitives | full_wire_temporary_and_dedup_new_copy_lru_no_reuse | PASS |
| primitives | concurrent_same_endpoints_have_separate_full_buffers_and_bandwidth | PASS |
| primitives | prompt_preflight_failure_does_not_partially_evict | PASS |
| primitives | leaf_lru_parent_eligibility_and_prompt_ancestor_protection | PASS |
| primitives | stale_admission_plan_rejected_before_mutation | PASS |
| primitives | config_rejects_non_stage_a_semantics | 9/9 PASS：ECT routing、ORACLE routing、theta=3、page_tokens=256、page_bytes=1、bandwidth=1、load_window=300000、旧 protocol version、bool num_pods |
| primitives | yaml_allowlist_rejects_legacy_knobs | 12/12 PASS：service、base_latency_ms、prefill_tokens_per_second、proactive、trigger_period_ms、proactive_byte_rate、cost_v_ref、cost_t_ref_ms、admission_timeout_ms、control_latency_ms、oracle、information_scope |
| primitives | trace_identity_validation_and_stable_same_timestamp_order | PASS |
| primitives | transfer_source_failure_is_nonmutating_and_early_complete_rejected | PASS |
| routing | cold_affinity_uses_lowest_pod_id | PASS |
| routing | longest_affinity_prefix_precedes_lower_load | PASS |
| routing | affinity_ties_use_load_then_pod_id_without_probe_mutation | PASS |
| routing | target_excludes_source_and_breaks_load_ties_by_id | PASS |
| routing | reactive_gate_is_strict_multiplication_and_handles_zero | PASS，原语级 |
| engine_affinity | cold_request_does_not_see_own_load_and_completes_immediately | PASS |
| engine_affinity | same_timestamp_requests_complete_and_publish_in_request_id_order | PASS |
| engine_affinity | prompt_admission_skip_keeps_request_successful_and_old_cache_unchanged | PASS |
| engine_affinity | p_only_outputs_do_not_create_completion_kv | PASS |
| engine_affinity | state_and_load_cross_evaluation_boundary_without_reset | PASS |
| engine_affinity | deterministic_records_summary_validation_and_output_roundtrip | PASS |
| engine_affinity | empty_input_is_a_valid_zero_request_run | PASS |
| transfer_events | inflight_load_visible_immediately_but_copy_and_opportunity_wait_for_ready | PASS，注入 assignment |
| transfer_events | all_transfers_publish_before_stable_resumes_then_new_arrival | PASS，注入 assignment |
| transfer_events | copy_capacity_failure_falls_back_without_losing_request_or_partial_eviction | PASS，注入 assignment |
| transfer_events | partial_tail_is_local_only_and_miss_committed_before_ready | PASS，注入 assignment |
| transfer_events | no_full_page_fallback_has_no_zero_wire_transfer | PASS，注入 assignment |
| transfer_events | reactive_event_replay_is_deterministic_under_injected_assignments | PASS，注入 assignment |
| engine_reactive | gate_false_stays_source_even_when_target_owns_transferable_prefix | 2/2 PASS：相同完整 Prefix、source 含 partial tail |
| engine_reactive | strict_gate_equality_and_one_token_above_in_production | 2/2 PASS：1024=2×512 不转；1025>2×512 COPY |
| engine_reactive | gate_true_direct_target_compares_full_page_prefix_and_charges_actual_miss | PASS：source hit600、target hit512、miss88，无 wire |
| engine_reactive | empty_transferable_prefix_gate_true_stays_source_without_wire | PASS |
| engine_reactive | empty_transferable_prefix_gate_false_stays_source_without_wire | PASS |
| engine_reactive | same_ready_committed_hit_protection_survives_earlier_prompt_admission | PASS：B 的 hit 保持、A admission skip、pin 不刷新 access/reuse、最后全部释放 |
| engine_reactive | copy_load_temporary_ready_order_and_unique_opportunity_in_production | PASS |
| engine_reactive | same_endpoint_concurrent_copy_and_capacity_fallback_in_production | PASS |
| engine_reactive | partial_tail_copy_commits_only_full_page_hit | PASS |
| engine_reactive | expired_source_load_disables_gate_at_exact_sixty_seconds | PASS |
| engine_reactive | other_target_tie_uses_pod_id_in_four_pod_production_run | PASS |
| engine_reactive | single_pod_synthetic_topology_keeps_source | PASS |
| engine_reactive | both_baselines_deterministic_from_cold_cache | 2/2 PASS：R_AFF、R_REQ_KV_TASK |
| isolation_cli | ast_dependency_allowlist_is_clean | PASS |
| isolation_cli | execution_with_legacy_imports_blocked_at_import_time | PASS |
| isolation_cli | synthetic_cli_writes_records_validation_and_input_hashes | 2/2 PASS：R_AFF、R_REQ_KV_TASK；后者产生两条真实有限时长 COPY |

共享 Trace 的 partial tail、full tail、hash length、parent consistency 四项均 PASS。共享 Cache 的 cold/publish、连续 Prefix、dedup、足够容量、leaf eviction、parent eligibility、hit refresh、probe no refresh、pin、旧 ActivePrivate、duplicate no refresh、impossible admission 十二项均 PASS。旧 ActivePrivate 用例只回归旧 Cache，并未进入新执行层。

JSONL/summary/validation 的生成和读回已由合成测试验证，合成文件位于 pytest 临时目录；真实输入 smoke 则写入独立的 `results/task_main/stage_a_smoke/`。没有写入历史结果目录，也没有把任何 smoke summary 发布为正式实验结果。JUnit 明细位于 `/tmp/task_main_stage_a_tests.xml`，为当前会话临时证据。

## 5. 18 类验收覆盖与 invariants

| 用户类别 | 当前覆盖状态 |
|---|---|
| 1 cold/self-load | R_AFF 端到端 PASS |
| 2 longest affinity | routing 原语及连续请求 PASS |
| 3 affinity tie | hit/Load/id，probe 不变异 PASS |
| 4 60s Load | 严格左边界、立即入账、等待 ready 期间可见 PASS |
| 5 reactive gate | 乘法/严格大于/zero 原语与正常执行分支组合 PASS |
| 6 reactive direct target | 正常执行 FULL_PAGE_ONLY direct-target/实际 miss/无 wire PASS |
| 7 finite COPY | 原语、注入事件循环、正常执行端到端 PASS |
| 8 copy capacity fallback | 原语无变异、注入及正常执行事件循环 fallback PASS |
| 9 source pin | transfer 期间不可淘汰、不刷新 LRU PASS |
| 10 full wire Temporary | 全量 buffer、已有祖先、ready dedup PASS |
| 11 concurrent transfers | 原语与正常执行相同双端并发、独立 buffer/带宽 PASS |
| 12 COPY LRU | ready 插入、duplicate/source 不刷新、无 reuse PASS |
| 13 Prompt admission skip | 请求成功、无半执行 eviction PASS |
| 14 same timestamp | R_AFF arrivals 逐一完成 PASS；transfer 全发布/稳定恢复/arrival 顺序的注入及正常执行测试 PASS |
| 15 opportunity uniqueness | direct R_AFF、正常执行 direct-target/reactive 等待均仅一次 PASS |
| 16 P-only | 修改 output metadata 不改变 Cache、无 completion KV PASS |
| 17 deterministic | 两条正常执行 baseline 与注入 transfer records 重放 PASS |
| 18 capacity | 每次状态修改断言、峰值检查、Temporary/pin 排空 PASS |

已实现的运行期 checks：completion ID 守恒；每请求一个 opportunity/Load entry；hit+miss=input；Load 的 Pod/tokens/time 对齐 final assignment；completion=arrival 或对应 ready；所有启动 transfer 完成；wire=Temporary=newly resident+duplicate；wire tokens/bytes 单位一致；peak<=capacity；结束 Temporary/pins=0；opportunity time=completion；依赖检查 PASS。

Cache 还逐步断言 Prefix closure、parent/child 一致、pin 非负、Temporary 正值和容量上界。Admission preflight 在 scratch state 上模拟 leaf 淘汰，失败不触碰真实状态，stale plan 在变异前拒绝。

18 类要求均有合成覆盖；通过结论限定为 Stage A 抽象执行协议，不意味着真实硬件吞吐、服务延迟或尚未实现的 Stage B 策略得到验证。

## 6. 尚未实现的 Stage B

没有 candidates/policies 模块，没有 Oracle、Persistence、真实 reuse 驱动的 Recency 策略、Cost-aware、任何主动 COPY 或最终指标全集。只在协议记录其确认语义：Lines 3–7 R_AFF、全局 future demand、count Top-10 保序+Score gate，以及 Line 5/7 预期完全一致的 consistency check。没有 V_ref/T_ref、归一化或人为区分策略的调参。

## 7. Diff 与历史保留

实施开始前保存了源码、配置、scripts、tests、docs、data 及主要根文件共 279 个预存文件的 SHA-256。完成代码与测试后逐一比较：**changed_existing=[]，279/279 未变化**，包括之前的 Gap Analysis、旧 engine/config/metrics 和原有未提交修改。既有 dirty 状态原样保留。

整个 Stage A 新增 24 个源码/配置/脚本/测试/文档文件：新包 11、配置 2、脚本/说明 3、测试 6、协议/实施报告 2。本次修订只续改这些 Stage A 路径，并在独立 smoke 目录新增 correctness 产物。新增文件未提交，因此普通 `git diff --stat` 不列出它们；`git status --short` 中原有修改不属于本阶段 diff。没有创建 commit。

旧 configs、scripts、results、tests 和历史协议未删除或改写；没有运行会重算历史结果的入口。未运行正式实验。Stage A 完成后停止，等待下一步指令；不自动进入 Stage B。

## 8. Stage A Final Validation

### 修订与保护验收

空 transferable Prefix 的旧 direct-target 行为已纠正。gate=true 但 source 只有 partial-tail local hit 时，现在 final_pod=source、保留实际 partial hit、无 wire；gate=false 同样留 source。非空情况下保留 FULL_PAGE_ONLY direct/COPY：source hit600、target 有512-token祖先时直接 target，88 tokens 仍为 miss；target 缺该祖先时只 COPY 一个完整页。

修改了 `empty_transferable_prefix_gate_true_stays_source_without_wire`，新增 gate=false 用例，以及 `same_ready_committed_hit_protection_survives_earlier_prompt_admission`。后者通过正常 routing 构造两个同 ready 请求：A 先尝试写两页 suffix，但紧容量下不能淘汰 B 的已承诺 root，因此 A admission skip；B 仍按 assignment 的一页 hit 完成。测试同时确认 pin 前后 access timestamp/order 和 reuse 数不变，所有 pin/Temporary 最终释放。没有改变保护算法，仅将 source pin/full Temporary 与 ready-batch target pin 的连续保障写入协议第 13 节。

最终 pytest：**86 passed in 1.54s，0 failed，0 skipped**，包含 70 项 Stage A 测试及 16 项共享 Trace/Cache 回归。仅在全部通过后执行以下 smoke。

### 四项 smoke 配置与产物

运行命令：

```text
python -m scripts.task_main.run_stage_a_smoke \
  --output results/task_main/stage_a_smoke
```

四项均使用新 `TaskMainEngine`，从空状态开始、arrival 范围 **`[0,300000ms)`**，不移动 timestamp、不抽样调参；只排空该 cohort 已启动的 transfer，不处理五分钟之后的新请求。统一 N=4、585 pages/Pod、512 tokens/page、14 MiB/full page、Load window=60000ms、theta=2、每 transfer 独立 25 GB/s。每项保存完整 `config.json`，两次独立重放分别写 `run_1/`、`run_2/`。

| Workload | Routing | Input=processed requests | 输出子目录 |
|---|---|---:|---|
| Conversation | R_AFF | 918 | `conversation_r_aff/` |
| Conversation | R_REQ_KV_TASK | 918 | `conversation_r_req_kv_task/` |
| ToolAgent | R_AFF | 1788 | `toolagent_r_aff/` |
| ToolAgent | R_REQ_KV_TASK | 1788 | `toolagent_r_req_kv_task/` |

子目录均相对 `results/task_main/stage_a_smoke/`。汇总证据：[smoke_validation.json](../../results/task_main/stage_a_smoke/smoke_validation.json)。完整输入 SHA-256、选择后的请求集合 SHA-256、配置、模块身份及 6 个输出文件的 SHA-256 均可审计；没有写入 `results/taskmain_evaluation/`。

### 每项 correctness

| Smoke | 请求/assignment/Load/opportunity 守恒 | hit+miss=input | 容量越界 | 最终 Temporary / pins | started=completed COPY | 零页 wire | 依赖隔离 | 确定性 |
|---|---|---|---:|---|---|---:|---|---|
| Conversation R_AFF | PASS，918 | PASS | 0 | 0 / 0 | 0=0 | 0 | PASS | PASS |
| Conversation R_REQ_KV_TASK | PASS，918 | PASS | 0 | 0 / 0 | 7=7 | 0 | PASS | PASS |
| ToolAgent R_AFF | PASS，1788 | PASS | 0 | 0 / 0 | 0=0 | 0 | PASS | PASS |
| ToolAgent R_REQ_KV_TASK | PASS，1788 | PASS | 0 | 0 / 0 | 9=9 | 0 | PASS | PASS |

每项还验证了一次 completion event、Load 入账时刻与 Pod/tokens、arrival 或 ready 完成、wire/Temporary/dedup 守恒、opportunity 在 completion 后以及 gate=true/empty 必须 stay source。每个 run 的 validation 共有 18 个 checks 全部 true。最终 pins=0 包括 source transfer pin 与 target committed-hit protection；engine pending 请求也已排空。

### Sanity statistics（不用于性能判断）

这里的 partial-tail local hit 指 routing 前 source actual hit 包含 partial tail；transferable=0 统计所有请求，包含 cold request。R_AFF 不执行 reactive gate，表中该项的 0 表示未调用。无 wire 时长度统计为 NA，不能解释为零页 transfer。

| Smoke | gate pass | direct target | COPY | fallback | CACHE_ADMISSION_SKIP | partial-tail local hit | transferable=0 | gate=true 且 transferable=0 | wire pages min/median/max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Conversation R_AFF | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 0 | NA / NA / NA |
| Conversation R_REQ_KV_TASK | 7 | 0 | 7 | 0 | 0 | 3 | 1 | 0 | 1 / 1 / 1 |
| ToolAgent R_AFF | 0 | 0 | 0 | 0 | 0 | 7 | 3 | 0 | NA / NA / NA |
| ToolAgent R_REQ_KV_TASK | 9 | 0 | 9 | 0 | 0 | 5 | 3 | 0 | 1 / 1 / 12 |

本次真实 smoke 没有出现 gate=true/transferable=0；该修正规则由合成用例非空覆盖，不能以 smoke 的零计数替代其测试。两个 reactive smoke 均自然出现 COPY，没有改变参数制造动作。direct-target/fallback 在本次 smoke 的零计数也如实保留，其行为由合成测试覆盖。

| Smoke | peak resident pages，Pod 0/1/2/3 | peak Temporary pages，Pod 0/1/2/3 |
|---|---|---|
| Conversation R_AFF | 585 / 0 / 0 / 0 | 0 / 0 / 0 / 0 |
| Conversation R_REQ_KV_TASK | 585 / 585 / 585 / 585 | 0 / 2 / 2 / 3 |
| ToolAgent R_AFF | 585 / 585 / 585 / 0 | 0 / 0 / 0 / 0 |
| ToolAgent R_REQ_KV_TASK | 585 / 585 / 585 / 585 | 12 / 12 / 12 / 2 |

两类峰值可能发生在不同时刻，不能相加判断容量；每次变异均检查同时驻留的 PublishedUnique+Temporary<=585，所有观察到的 memory peak 均不超过585。

### Deterministic replay 与 legacy isolation

每项两次从空 Cache/Load 开始的运行，其 `request_records.jsonl`、`transfer_records.jsonl`、`opportunity_records.jsonl`、`event_records.jsonl`、`summary.json`、`validation.json` 全部 SHA-256 一致。下列为两遍一致的 summary SHA-256，其余完整 hash 见汇总 JSON。

| Smoke | summary SHA-256 |
|---|---|
| Conversation R_AFF | `3cbf56f7ac0e711f31a9f2d0e2d792f63e0e28f3125b5ffee54f1bda224b5d80` |
| Conversation R_REQ_KV_TASK | `0bebd006da4f3d9536299e985b05ed1d520f656d0af3f8ed45667c4f0bd1782e` |
| ToolAgent R_AFF | `7f47f0d46a9a7d18e4da2cea48052a2f935d4f71ff240c814cf430850272c4ba` |
| ToolAgent R_REQ_KV_TASK | `4b34c7044948716321b163d92d3268fa49e2a6e30acb6403c5a7a8bbf417cdbc` |

smoke 在导入新 engine 前安装 runtime import guard，同时拒绝已经装载的非法 legacy 模块。各 run provenance 保存实际 loaded module 清单及 AST 审计；允许的历史接触仍仅为安全 Trace 和父包导出的 config 类。没有调用旧 service/Pod/FCFS/RemotePressure/NodeGate/timeout/replan/ECT/O2/P3/tick/TokenBucket/Cost-aware reference 路径。合成子进程测试另外证实 guard 会立即拒绝旧 service import。

**最终 Stage A blocker=0。** 本轮只做修订、合成验收及 correctness smoke；未运行正式 14 行，未运行 Oracle/Persistence/Recency/Cost-aware，未运行 sweep，没有旧结果重算。smoke 不比较策略优劣，不形成性能或研究结论。完成后停止，不进入 Stage B。
