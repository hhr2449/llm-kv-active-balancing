# T2 Mooncake Trace Capability Audit

> **阶段**：T2 / Step 1 — Trace Capability Audit  
> **目标**：在实现多 Pod simulator 之前，冻结 Mooncake FAST'25 Trace 的可观测信息、Prefix Hash 语义、尾块处理方式以及 simulator 的安全输入契约。  
> **审计原则**：只使用 Trace 明确提供或官方材料明确说明的信息；不能从 `output_length`、未来请求或 hash 结构中补造未知 completion KV。

---

## 1. 使用的数据版本

阶段 2 应优先使用 Mooncake FAST'25 更新版 Trace：

```text
FAST25-release/traces/conversation_trace.jsonl
FAST25-release/traces/toolagent_trace.jsonl
```

不应默认使用旧的：

```text
FAST25-release/arxiv-trace/mooncake_trace.jsonl
```

正式实验必须额外记录：

```text
repository commit / file version
local file SHA256
request count
trace start/end timestamp
```

以避免后续 Mooncake 仓库更新导致实验数据版本漂移。

当前官方 FAST'25 README 给出的 workload 概况为：

| Workload | Requests | Avg input | Avg output | Arrival |
|---|---:|---:|---:|---|
| Conversation | 12,031 | 12,035 | 343 | real timestamp |
| Tool & Agent | 23,608 | 8,596 | 182 | real timestamp |

两份真实 workload 均来自约一小时在线请求。

---

# 2. Trace 字段

每一行是一个 Request：

```json
{
  "timestamp": 27482,
  "input_length": 6955,
  "output_length": 52,
  "hash_ids": [46, 47, 48, 49]
}
```

官方字段语义：

```text
timestamp
    相对请求到达时间，单位毫秒

input_length
    Prompt token 数

output_length
    生成 token 数

hash_ids
    Prompt Prefix 的 remapped prefix block hashes
```

Raw text 和 token IDs 不公开。

因此 Mooncake 主实验不能恢复：

```text
真实 Prompt token 内容
真实 completion token 内容
completion KV hash sequence
```

---

# 3. Hash ID 的关键语义

Mooncake 后续论文对 `hash_ids` 的定义比简单“每块内容 hash”更严格：

> 每个 Prefix Hash 都包含当前 token block 以及它之前的全部 blocks，然后再映射成全局唯一 ID。

因此：

```text
hash_ids = [h0, h1, h2, ...]
```

本质上是一条 **prefix-aware cumulative hash path**。

如果两个 Request：

```text
R1 = [a, b, c, d]
R2 = [a, b, c, e]
```

那么它们共享：

```text
[a, b, c]
```

即可确认两者前 3 个 Prefix Blocks 的完整前缀相同。

因此 simulator 不应该做：

```text
set(hash_ids) intersection
```

而必须做：

```text
Longest Common Prefix(hash_ids)
```

即：

\[
LCP(r_i,r_j)
=
\max k:
h_{i,0:k}=h_{j,0:k}
\]

---

# 4. 一个重要修订：`hash_ids` 包含尾部 partial block

直接检查官方 Trace 示例可以发现：

```text
input_length = 6758
len(hash_ids) = 14

ceil(6758 / 512) = 14
floor(6758 / 512) = 13
```

另一个实际请求：

```text
input_length = 915
len(hash_ids) = 2

ceil(915 / 512) = 2
```

Tool/Agent Trace 中也能看到：

```text
input_length = 6506
hash_ids = [46, ..., 57, 64]

前 12 个 hash 对应：
12 × 512 = 6144 tokens

剩余：
6506 - 6144 = 362 tokens
```

因此最后一个 Hash 并不能简单解释成又一个“512 个有效 Prompt tokens”。

更合理的数据模型是：

```text
hash_ids:
    Trace 明确提供的 Prefix Page Identity

valid_tokens_in_block:
    该 Hash 在当前 Request 中对应的实际有效 token 数
```

对于满足：

\[
m=\lceil input\_length /512\rceil
\]

的请求：

```text
前 m-1 个 block:
    valid_tokens = 512

最后一个 block:
    valid_tokens =
    input_length - 512 × (m-1)
```

若最后恰好整除，则为 512。

---

# 5. Partial Hash 应不应该保留

## 结论：保留，但区分逻辑 token 与物理 page

以前设计中的：

> “只保留 `floor(input_length/512)` 个完整块，尾部不进入可共享 Block 集合”

需要修订。

原因：

1. 尾部 hash 是 Trace **真实提供的 identity**，不是 simulator 人造；
2. Mooncake 官方 storage benchmark 对 `request.hash_ids` 中的每个 ID 都生成一个 page access；
3. 如果两个请求的尾部 Prefix Hash 真正相同，那么 Trace 本身是在告诉我们这段 Prefix identity 相同。

因此 preprocessing 保留：

```text
all hash_ids
```

同时为每一个 Block 附带：

```text
valid_tokens
is_partial
```

### 逻辑计算量

若某 Request 实际命中 \(k\) 个 Prefix Hash：

\[
HitTokens
=
\min(input\_length,\ 512k)
\]

这样：

- 命中完整页时按 512 token；
- 如果命中到 Request 自己的最后一个 partial page，不会超过实际 `input_length`。

### 物理 Cache / Transfer

在 Trace-level 512-token page abstraction 下：

```text
1 hash_id
→ 1 physical page
```

即 partial page 仍占一个 page 的物理容量。

这样将：

```text
逻辑有效 token 数
```

和：

```text
物理 KV page 容量
```

严格分离。

---

# 6. P-only 结论继续成立

虽然 Trace 给出了：

```text
output_length
```

但没有：

```text
completion_hash_ids
```

因此不能从：

```text
output_length = 300
```

推出任何新的 KV identity。

不能：

```text
Prompt hashes
+
伪造的 output hashes
=
C_k
```

也不能使用下一条未来 Request 的新 hashes 倒推出当前 completion，然后提前发布。

Mooncake 主路径继续采用：

```text
REQUEST_ARRIVAL
    ↓
读取 Prompt hash_ids
    ↓
Prefill
    ↓
SERVICE_DONE
    ↓
发布本次 Prompt hash pages
```

即：

\[
\boxed{P\text{-only}}
\]

---

# 7. 当前可以可靠恢复什么

## 7.1 可以直接得到

```text
Request arrival order
Relative arrival time (ms)
Prompt token length
Output token length
Prompt Prefix Hash Path
512-token trace block identity
不同 Request 之间的 Prefix LCP
Prompt Prefix 的历史重复关系
Prefix Tree / Prefix DAG-like online index
```

---

## 7.2 可以基于明确假设计算

```text
每个 hash page 的 valid token 数
partial tail token 数
Trace-level page occupancy
无限历史下的潜在 Prefix reuse
Prefix demand frequency
Prefix recency
Future Prefix demand（Oracle only）
```

---

## 7.3 不能从 Mooncake Trace 恢复

```text
Raw prompt content
Token IDs
Completion content
Completion KV hashes
可靠 session_id / user_id
真实 Prefill Pod assignment
真实 Decode Pod assignment
真实 Cache capacity / Cache state
真实 eviction history
真实 request service time
真实 per-request queue time
真实 cross-Pod P2P bandwidth
真实 transfer contention
真实 model KV bytes/token
```

因此这些都必须由：

```text
simulator state
或
独立物理参数
```

提供，不能冒充 Trace 原始事实。

---

# 8. Session 不作为 Mooncake 主实验必要字段

Mooncake Hash Path 可以揭示 Prefix 继承和共享关系，但：

```text
shared prefix
!=
same session
```

同一 system prompt、工具模板或公共 context 也会产生共享 Prefix。

因此阶段 2 的主动作单位继续采用：

```text
Prefix Chain
```

而不是从 hash 自动恢复一个“伪 session”后按 session 调度。

如后续要研究 session-level prediction，应单独定义会话重建算法与误差审计。

---

# 9. 时间字段审计

官方定义：

```text
timestamp = relative arrival time in milliseconds
```

Conversation 和 Tool/Agent 均为约一小时在线 Trace。

直接检查当前 Conversation Raw Trace：

```text
first timestamp = 0 ms
last observed timestamp = 3,536,999 ms
```

对应约：

```text
58 min 57 s
```

同时，直接抽查当前 FAST'25 Raw Trace 可以看到大量：

```text
0
3000
5999
9000
...
```

这种约 3 秒粒度的 timestamp 分组。

这说明：

> **字段单位是 ms，不代表真实有效 arrival resolution 一定是 1 ms。**

正式冻结：

```text
TRIGGER_TICK
```

之前必须对两份 Trace 全量统计：

```text
unique timestamp count
same-timestamp request batch size
positive timestamp delta distribution
delta GCD / dominant intervals
```

如果全量审计确认约 3s bucketization，则 0.1s / 1s proactive tick 的含义需要重新评估。

---

# 10. Prefix Hash 的 parent consistency

因为 Mooncake 的 Hash 是 cumulative prefix hash，理论上同一个：

```text
hash_id
```

应对应稳定的：

```text
depth
parent_hash_id
```

例如：

```text
... → 46 → 47 → 48
```

若 `48` 再次出现，它的 Prefix ancestry 应保持一致。

正式 preprocessing 必须全量检查：

```text
hash_id → observed_depths
hash_id → observed_parent_ids
```

要求正常情况下：

```text
len(observed_depths) == 1
len(observed_parent_ids) <= 1
```

如果出现冲突：

- 不能直接假定整个 Global Prefix Tree 正确；
- 必须先调查 Trace remapping 语义或 parser 错误。

---

# 11. `len(hash_ids)` 与 `input_length` 的硬检查

全量 Trace 必须验证：

\[
len(hash\_ids)
=
\left\lceil
\frac{input\_length}{512}
\right\rceil
\]

至少当前官方样本和直接检查的 Raw rows 与这一关系一致。

需要输出：

```text
ceil relation passed rows
ceil relation failed rows
empty hash rows
zero/negative input rows
```

只要存在 anomaly，不能静默修正。

---

# 12. Tail Hash 的一致性检查

需要额外建立：

```text
hash_id
→ set(valid_token_count)
```

尤其检查同一个 hash 是否：

```text
一次作为 362-token partial page
另一次作为 500-token partial page
```

如果 cumulative hash 定义严格，这种情况理论上不应发生。

同时检查：

```text
同一个 hash
是否既作为 non-final full page
又作为 final partial page
```

出现时必须单独调查。

---

# 13. 建议的 simulator 输入格式

Trace Preprocessing 后，每条 Request 转成：

```text
TraceRequest {
    request_id: int

    arrival_ms: int | float

    input_tokens: int
    output_tokens: int

    block_ids: List[int]

    block_valid_tokens: List[int]
    block_is_partial: List[bool]

    num_trace_pages: int
    num_full_512_blocks: int
    tail_tokens: int
}
```

其中：

```text
block_ids
```

严格保留官方 `hash_ids` 顺序。

不重新 hash，不改变 ID，不伪造细粒度子块。

---

# 14. Cache / Prefix Tree 构建方式

不能只维护：

```text
hash_id set
```

而应按路径构建：

```text
ROOT
 └── h0
     └── h1
         └── h2
             ...
```

由于 hash 本身已经 prefix-aware，可以用：

```text
(depth, hash_id)
```

作为稳定 Prefix Node identity，并通过 parent consistency audit 验证。

每个 Prefix Node 至少记录：

```text
hash_id
depth
parent
children

first_seen_ts
last_seen_ts
access_count

valid_tokens_if_terminal?
```

注意：

> 同一个 Node 作为某个请求终点时可能对应 partial page；作为其他请求中间节点时应通过全量 audit 验证是否存在语义冲突。

---

# 15. Reuse / Hit 的安全定义

对于 Request：

```text
[h0, h1, ..., hm]
```

Pod Cache 当前已发布路径：

```text
[h0, h1, ..., hk]
```

命中只能取：

```text
从 Root 开始的连续最长 Prefix
```

即：

\[
HitPages = LCP(RequestPath, PodCache)
\]

对应：

\[
HitTokens
=
\min(
input\_tokens,\,
512\times HitPages
)
\]

物理命中容量仍按：

```text
HitPages × PageBytes
```

处理。

因此：

```text
Saved Compute Tokens
```

与：

```text
Physical KV Bytes
```

不能用同一个数字代替。

---

# 16. 当前 Trace 数据对研究设计的影响

本次 Capability Audit 对 `T2_simulator_design_final.md` 有三项明确修订。

## 修订 1：P-only 保持不变

这是正确选择。

---

## 修订 2：尾部 hash 不再丢弃

由：

```text
只保留 floor(input/512) 个完整 hash
```

改为：

```text
保留 Trace 提供的全部 hash_ids
+
记录最后一页 valid_tokens
+
物理页与逻辑 token 分开计量
```

---

## 修订 3：Trigger interval 必须等完整 timestamp audit

不能因为字段单位是：

```text
milliseconds
```

就自动采用：

```text
0.1s / 1s
```

trigger。

当前 Raw Trace 抽查已经显示明显的约 3s arrival bucket。

因此 Trigger 参数必须在全量时间分辨率审计之后冻结。

---

# 17. 正式实现前的 Hard Gate

Trace preprocessing 必须输出并通过：

```text
required_fields_present = true

timestamps_non_decreasing = true

hash_len_equals_ceil_input_over_512
    violations = 0

hash_parent_conflicts = 0

hash_depth_conflicts = 0

invalid_input_length = 0

empty_hash_on_positive_input = 0

tail_hash_valid_length_conflicts = 0
```

如果其中任意关键项失败：

> 先处理 Trace 语义，不进入 simulator policy implementation。

同时报告但不一定作为失败：

```text
same_timestamp_fraction
timestamp_delta_histogram
partial_page_fraction
repeated_hash_fraction
unique_hash_count
prefix_reuse potential
```

---

# 18. 下一步

Step 1 的逻辑数据契约现在已经明确。

接下来应做两件事：

1. 对 `conversation_trace.jsonl` 和 `toolagent_trace.jsonl` 运行附带的全量审计脚本；
2. 根据实际输出冻结：
   - timestamp resolution；
   - Trigger tick 候选；
   - Development / Warmup / Evaluation / Tail 切分；
   - 512-token page 的容量与 valid-token 处理。

完成后再进入：

\[
\boxed{\text{Step 2: Simulator Kernel Implementation}}
\]

而不是先实现任何 Oracle / Persistence 策略。

---

# 参考来源

1. Mooncake FAST'25 Trace Release  
   https://github.com/kvcache-ai/Mooncake/blob/main/FAST25-release/README.md

2. Mooncake FAST'25 Conversation Trace  
   https://raw.githubusercontent.com/kvcache-ai/Mooncake/main/FAST25-release/traces/conversation_trace.jsonl

3. Mooncake FAST'25 Tool & Agent Trace  
   https://raw.githubusercontent.com/kvcache-ai/Mooncake/main/FAST25-release/traces/toolagent_trace.jsonl

4. Mooncake: A KVCache-centric Disaggregated Architecture for LLM Serving（ACM TOS）  
   Hash ID 说明：Prefix hash 包含当前及所有前序 token blocks。  
   https://madsys.cs.tsinghua.edu.cn/publication/mooncake-a-kvcache-centric-disaggregated-architecture-for-llm-serving/ToS2025-Qin.pdf

5. Mooncake KVCache Storage Benchmark  
   官方 benchmark 将每个 `hash_id` 作为一个固定 page access 处理。  
   https://github.com/kvcache-ai/Mooncake/tree/main/benchmarks/storage_benchmark_v1
