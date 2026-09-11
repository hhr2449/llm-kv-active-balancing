# 阶段 2：多 Pod KV Cache 主动干预模拟设计（最终版）

> **研究主题**：Cache Affinity 导致的多 Pod 负载倾斜，以及 proactive KV Prefix replication 相对 request-driven P2P 的边际系统收益  
> **文档定位**：阶段 2 的最终研究设计基线。本文综合原始任务设计、P2P 实现说明、生产集群分析、此前两轮设计总结以及 Astra 两轮审阅意见，对阶段 2 的研究问题、数据契约、模拟器语义、策略定义、资源模型、评估指标和实验顺序进行统一冻结。  
> **原则**：冻结研究逻辑和实现语义；没有可靠标定依据的具体数值仍作为参数或敏感性范围，不冒充生产真实值。

---

# 1. 研究目标

阶段 2 不搭建真实的 SGLang / Mooncake 分布式推理集群，也不执行真实 KV Tensor 的跨 GPU 传输，而是在完整 workload trace 上构建一个：

- 多 Prefill Pod；
- 有限本地 KV Cache；
- Cache-Affinity Routing；
- request-driven P2P；
- proactive Prefix replication；
- 有限传输时间与端点争用；
- 请求级 Prefill 服务模型；
- closed-loop 状态演化；

的**离散事件离线 replay simulator**。

阶段 2 研究的不是底层 KV Transfer 是否能实现，而是：

> **在已经具备 request-driven P2P 能力的系统中，额外加入“请求到来之前的主动 Prefix 复制”，能否进一步降低 Prefill 完成延迟或服务压力，并在网络、显存驻留和 Cache 污染成本可接受的条件下改善多 Pod 负载分布。**

核心因果链为：

```text
未来 Prefix 需求信息
        ↓
决定是否提前复制
        ↓
选择 Prefix Chain / Source / Target
        ↓
副本在请求到达前完成
        ↓
Target 获得新的 Prefix Cache
        ↓
后续 Routing 获得更多可选 Cache holder
        ↓
可能减少 request-driven P2P 等待、排队或热点压力
        ↓
同时付出网络、容量、淘汰与错误复制成本
```

---

# 2. 核心研究问题

阶段 2 依次回答三个问题。

## 2.1 当前系统本身有什么问题

先运行基础系统：

```text
R_AFF
R_LEAST
R_REQ_KV_SIMPLE
R_REQ_KV
```

确认：

- Cache Affinity 是否造成请求分配集中；
- 请求集中是否进一步造成 Prefill 服务压力；
- request-driven P2P 能缓解多少；
- 剩余瓶颈主要来自 Cache locality、服务排队、P2P 等待还是端点争用。

---

## 2.2 Proactive replication 相对现有 P2P 是否还有额外收益

正式主比较为：

\[
\boxed{
B0 = R_{REQ\_KV}
}
\]

\[
\boxed{
O1 = R_{REQ\_KV}+T_{ORACLE}
}
\]

两组共享：

- 相同 workload；
- 相同 Pod 数；
- 相同 Cache 容量；
- 相同 service model；
- 相同 request-driven P2P；
- 相同 Transfer primitive；
- 相同端点资源与网络预算；
- 相同 fallback；
- 相同 Cache replacement。

唯一额外能力是：

> O1 允许在请求到达前，根据授权的未来 workload demand 主动建立 Prefix replica。

因此：

\[
O1-B0
\]

才是 proactive replication 在当前 P2P 底座上的**边际系统收益**。

原始任务中的：

\[
R_{AFF}
\quad vs\quad
R_{AFF}+T_{ORACLE}
\]

仍保留，但只作为**机制诊断实验**，用于验证“主动复制是否真的改变 Cache-Affinity Routing”。

---

## 2.3 如果存在空间，简单历史信息能实现多少

只有 O1 相对 B0 存在可信收益或明确可解释的局部收益空间后，才继续运行：

```text
R_REQ_KV + T_PERSIST
R_REQ_KV + T_RECENCY
```

回答：

> 不读取未来，只依赖历史频率或近期访问，能实现多少 proactive 增量。

之后再考虑：

```text
T_FREQ_DECAY
S_COST_AWARE
学习式预测器
```

---

# 3. 数据契约与 Trace 语义

## 3.1 Mooncake 主实验采用 P-only

Mooncake 主实验只使用 Trace 中能够确认的 Prompt Prefix Hash。

请求接口为：

```text
Request {
    request_id
    arrival_time

    input_length
    prompt_full_block_ids
    prompt_tail_tokens

    output_length?
    completed_block_ids?
    completion_observation_time?
}
```

其中：

- `prompt_full_block_ids`：Trace 中可以确认身份的完整 Prefix Blocks；
- `prompt_tail_tokens`：不足一个完整 Hash Block 的尾部 token 数；
- `output_length` 只描述请求，不用于伪造 completion KV；
- 只有数据真实提供 `completed_block_ids` 时才启用完整 P/C 模式。

Mooncake 主路径：

```text
REQUEST_ARRIVAL
    ↓
使用当前 Prompt Prefix 查 Cache
    ↓
Prefill 服务
    ↓
SERVICE_DONE
    ↓
发布本次已经计算且身份可确认的 Prompt KV Blocks
```

即：

\[
\boxed{\text{Mooncake main path = P-only}}
\]

不能依据 `output_length` 或未来请求反推未知 completion KV。

---

## 3.2 P/C 模式作为扩展

对于具有原始文本、能够重新 tokenize 并严格构造完整上下文的数据，例如 WildChat，可扩展：

\[
P_k,\ C_k
\]

但仍需单独定义：

- Decode 在哪里执行；
- C 何时可见；
- C 是否回流 Prefill；
- 回流是否产生传输和显存成本。

因此：

> 完整 P/C 模式不属于 Mooncake 第一轮主实验的默认语义。

---

## 3.3 Block 粒度与 Partial Tail Hash

Mooncake Trace 的 Prefix Hash 粒度为 512 tokens，但实际 Raw Trace 表明：

\[
len(hash\_ids)=\left\lceil\frac{input\_length}{512}\right\rceil
\]

至少在官方示例和直接检查的 Raw rows 中成立。因此最后一个 `hash_id` 可能对应一个不足 512 个有效 Prompt tokens 的 **partial hashed page**。

第一版 preprocessing 采用：

- simulator 的 Prefix identity 严格使用 Trace 提供的全部 `hash_ids`；
- 不能人为拆成真实 16-token 或 64-token 子块；
- 不丢弃 Trace 已经提供 identity 的最后一个 partial hash；
- 每个 hash page 额外记录 `valid_tokens`；
- 前 `m-1` 个 page 的 `valid_tokens=512`；
- 最后一个 page 的有效 token 数为：
  \[
  input\_length-512(m-1)
  \]
  若整除则为 512；
- `hash_ids` 与 `ceil(input_length/512)` 的关系必须在全量 Trace Audit 中做 hard check。

因此必须区分：

```text
Trace page identity / physical page occupancy
```

和：

```text
该 page 在当前 Request 中包含的有效 Prompt tokens
```

在当前 trace-level abstraction 下，一个 Trace hash page 按一个物理 page 进行 Cache / Transfer accounting；而计算节约 token 数必须按 `valid_tokens` 截断，不能简单用 `hash_pages × 512` 超过真实 `input_length`。

物理 KV bytes 与 Trace hash 粒度必须分开配置，不允许混用不同模型或不同 block size 的容量数字。

---

# 4. Prefix、Cache 与内存模型

## 4.1 Block 与 Prefix Chain

### Block

用于：

- Prefix match；
- Cache residency；
- Capacity；
- Transfer；
- Eviction。

### Prefix Chain

表示从 Root 到某个 Prefix Tree 节点的完整连续路径，用于：

- proactive candidate；
- future demand score；
- history score；
- replica placement。

因此：

```text
Block
= Cache / Transfer 的实际单位

Prefix Chain
= Proactive decision unit
```

---

## 4.2 Global Prefix Index

Global Prefix Index 可以离线构建，用于稳定 identity 和索引。

但在线策略只能访问：

```text
online_seen = true
```

的 Prefix。

不能利用完整未来 Trace 中的：

- future branch；
- future leaf；
- future frequency；
- future order；

提前构造普通策略 candidate。

Oracle 只能在授权的 future-demand scoring 阶段读取未来请求，不得利用未来树改变当前 candidate identity。

---

## 4.3 Pod-local Cache

每个 Pod 独立维护：

```text
published resident blocks
active private blocks
pin / reference state
transfer temporary blocks
LRU metadata
```

主版采用 Prefix-closed radix-like Cache：

> 已发布某节点，则其祖先 Prefix 也必须已发布。

这是当前 simulator 的建模选择，不宣称所有 KV Cache 系统都必须如此。

---

## 4.4 Prefix hit

Request \(r\) 在 Pod \(p\) 的命中为：

\[
H(r,p)
=
\text{Prompt 与当前已提交 Cache 的最长连续 Prefix}
\]

需要区分：

\[
H_{route}
\]

和：

\[
H_{used}
\]

- `H_route`：路由决策时看到的 Prefix hit；
- `H_used`：Request 真正 `SERVICE_START` 时重新 lookup 得到的实际命中。

最终计算节约与 Cache hit 指标使用：

\[
H_{used}
\]

因为等待期间 Cache 状态可能变化。

---

## 4.5 内存容量：最终口径

Pod 的物理内存使用定义为：

\[
\boxed{
MemoryUsed
=
PublishedUniqueBytes
+
ActivePrivateBytes
+
TransferTemporaryBytes
}
\]

其中：

### PublishedUniqueBytes

已经正式发布并可被未来 Request 命中的唯一 Cache Blocks。

如果这些已发布 Blocks 当前被 Request 或 Transfer pin：

> **只改变引用状态，不重复增加容量。**

因此 published-and-pinned block 仍然只计一份 `PublishedUniqueBytes`。

同时：

> **已发布且被 pin 的 Block 仍然可以被其他合法 Request 命中。**

pin 只表示当前不能被 eviction，不表示不可见。

### ActivePrivateBytes

尚未正式发布、当前请求计算中独占的 KV，例如：

- 新计算中的 suffix；
- 未发布尾页；
- 当前 Request 的私有运行时 KV。

这些内容尚未 commit，因此不能被其他 Request 命中。

### TransferTemporaryBytes

P2P Transfer 期间 target 独立分配的临时页。

即使传输内容与 target 当前已有 published block 重复，只要系统实际需要独立 temporary allocation，就必须在传输期间占容量。

容量约束：

\[
MemoryUsed \le C_{pod}
\]

这一公式避免把“published block 被 pin”重复记两份容量。

---

## 4.6 LRU

第一版采用 Prefix-compatible LRU：

- 只能淘汰 `unpinned leaf blocks`；
- 删除 leaf 后，新暴露的 leaf 才可继续参与 eviction；
- Request 实际复用的 Prefix 更新 access recency；
- 新计算 Block 和新复制 Block 在 commit 时按统一 MRU 规则插入；
- Copy、probe、candidate scan 不计入真实 demand hit；
- 相同 timestamp 用稳定 block identity tie-break。

---

# 5. Request-level Service 与 Load

## 5.1 ServiceTime

正式 simulator 采用 request-level virtual Prefill service model：

\[
ServiceTime(r,p)
=
f(input\_length_r,\ H_{used}(r,p))
\]

要求：

\[
f(n,n)>0
\]

即完全 Cache hit 的请求仍有非零服务成本。

第一版可以采用参数化形式：

\[
ServiceTime
=
d_0
+
\frac{MissComputeTokens}{\mu}
\]

其中：

- \(d_0\)：非零基础 Prefill/调度服务成本；
- \(\mu\)：等效 Prefill processing rate。

这两个参数优先独立标定；无法标定时明确作为 simulator assumption，并做敏感性检查。

不在当前设计文档中把某个 `μ` 或 `d0` 写成生产真实值。

---

## 5.2 Pod Load

Pod Load 定义为：

\[
\boxed{
Load_p(t)
=
EstimatedRemainingPrefillServiceTime_p(t)
}
\]

它是 Routing 使用的在线压力估计，而不是单纯 token count。

---

## 5.3 Load 的具体更新规则

为了避免重复登记和“幽灵负载”，每个 Request 维护唯一状态：

```text
ARRIVED
WAITING_TRANSFER
READY
RUNNING
DONE
FALLBACK
```

### ARRIVAL

Routing 只读取当前 Pod Load，不立即把一个尚未确定 target 的 Request 永久加入任何 Pod 的真实 GPU service backlog。

### WAITING_TRANSFER

如果 request-driven P2P 已决定目标 Pod：

- 记录该 Request 已承诺的 target；
- Routing pressure 可以记录一个单独的 `committed_remote_pressure`；
- 但 Transfer 完成前，该 Request **不消耗 target GPU service time**。

这个 committed pressure 只用于后续 routing/load estimation，不进入实际 Prefill server 的服务消耗。

### TRANSFER_COMPLETE / FALLBACK

Transfer 成功后：

```text
Request -> READY(target)
```

如果超时或失败：

```text
重新执行 fallback route
重新 lookup Cache
重新确定 target
```

旧 target 上该 Request 的 committed pressure 必须先撤销，再在新 target 上重新登记，禁止残留。

### SERVICE_START

Request 真正开始 Prefill：

1. 移除该 Request 的 committed/queued estimate；
2. 重新计算 `H_used`；
3. 用 `H_used` 重新计算真实 `ServiceTime`；
4. 把其服务时间加入 running server state。

因此 Route 时对工作量的估计只是估计，不直接决定最终实际 Prefill duration。

### REPLAN

任何 target replan：

```text
remove old commitment
→ recompute route/cache/load
→ add new commitment exactly once
```

必须保证：

> 一个 Request 在任意时刻最多对一个 Pod 形成一份 committed load contribution。

---

# 6. 离散事件模型

正式 Headroom 使用事件驱动 replay。

核心事件：

```text
REQUEST_ARRIVAL
TRANSFER_ENQUEUE
TRANSFER_ADMIT
TRANSFER_COMPLETE
TRANSFER_TIMEOUT
SERVICE_START
SERVICE_DONE
TRIGGER_TICK
```

---

## 6.1 信息权限

普通策略可见：

```text
已经到达的 Request
当前 published Cache
当前 queue/load
当前 transfer queue / in-flight transfer
历史 demand
```

不可见：

```text
尚未到达的 Request
未来 Pod state
未提交的新 KV
未知 completion KV
```

Oracle 只额外允许读取：

\[
(t,t+W]
\]

内的未来外部 Request Prefix demand。

它仍然不能直接读取未来：

```text
Routing
Load
Cache residency
eviction
transfer outcome
```

这些必须由自己的 closed-loop 分支产生。

---

## 6.2 同时刻事件顺序

同 timestamp 固定按以下阶段处理：

```text
1. SERVICE_DONE / TRANSFER_COMPLETE commit
2. TRANSFER_TIMEOUT / fallback
3. 接收本时刻所有 REQUEST_ARRIVAL
4. 推进更早到达的 request-driven transfer queue 和 ready service
5. 执行本时刻 TRIGGER_TICK
6. 推进本时刻新产生的即时状态变化至稳定
```

最后使用唯一 `event_id` 做稳定 tie-break。

---

# 7. Transfer Model

## 7.1 Instantaneous 模式

只用于：

```text
unit test
placement diagnostic
routing mechanism diagnostic
```

不用于正式 Headroom 结论。

---

## 7.2 正式有限 Transfer

Transfer ready time：

\[
t_{ready}
=
t_{admit}
+
\ell_{control}
+
\frac{WireBytes}{B_{effective}}
\]

其中：

- \(\ell_{control}\)：参数化控制开销；
- \(B_{effective}\)：参数化 P2P 带宽。

正式实验前冻结主配置与有限敏感性范围。

---

## 7.3 端点竞争与准入：最终规则

每个 Pod 同时最多参与一项 P2P。

需要区分：

```text
Request-driven P2P
Proactive P2P
```

### Request-driven P2P

Request 可以进入 FIFO transfer admission queue。

进入队列并不要求端点当前空闲。

队列记录：

```text
request_id
enqueue_time
deadline
candidate source / routing context
```

当轮到该 Request 真正尝试 admission 时，必须重新检查：

```text
当前 source 是否仍拥有 Prefix
当前 target 是否仍适合
当前 Cache hit
当前 Load
当前 target capacity
当前 endpoint availability
当前 transfer length
```

必要时允许重新选择 target。

如果超时：

```text
移除等待状态
↓
执行统一普通 fallback
↓
重新 lookup 当前 Cache
↓
重新进入普通 Routing / Service
```

### Proactive P2P

主动动作：

> **不进入一个无界后台等待队列。**

在 `TRIGGER_TICK` 时，只使用：

- 当前空闲；
- 且没有被更早 request-driven ticket 保留；

的 source / target endpoints。

不可准入则：

```text
SKIP this tick
```

下一 tick 可重新评估。

### 竞争优先级

最终冻结为：

> **更早进入的 request-driven admission ticket 优先于尚未开始的 proactive action。**

但：

> **已经开始的 proactive transfer 不可被免费抢占。**

如果主动 Transfer 已经占用某个 endpoint，后来到达的 request-driven P2P 必须真实等待，因此该阻塞成本进入 O1 的闭环结果。

---

## 7.4 Transfer Commit

Transfer 开始：

- source relevant published blocks 被 pin；
- target 分配 `TransferTemporaryBytes`；
- replica 仍不可用于 Prefix hit。

Transfer 完成：

1. 重新检查 target 已存在 published blocks；
2. duplicate temporary blocks 释放；
3. 真正新增 Blocks 转为 `PublishedUniqueBytes`；
4. source pin 释放；
5. target replica 正式可见。

因此明确区分：

```text
WireBytes
TransferTemporaryBytes
NewlyResidentBytes
```

---

## 7.5 FULL_PREFIX 与 DELTA_SUFFIX

主版：

```text
FULL_PREFIX
```

即 wire 层发送本次 Transfer 对象的完整 Prefix。

Commit 时再去重 target 已有 Blocks。

敏感性：

```text
DELTA_SUFFIX
```

只发送 target 缺失 suffix。

无论使用哪种模式：

> request-driven 与 proactive 必须共享同一种能力。

---

# 8. Routing Policies

## 8.1 R_AFF

\[
H_{\max}
=
\max_p H(r,p)
\]

先选最大 Prefix hit 的 Pod。

同命中时：

\[
p^*
=
\arg\min Load_p
\]

最终 tie 用稳定规则。

---

## 8.2 R_LEAST

\[
p^*
=
\arg\min_p Load_p
\]

完全忽略 Cache。

它是低 Load reference，不是理论最优边界。

---

## 8.3 S0 = R_REQ_KV_SIMPLE

保留旧简化版本：

```text
先 R_AFF 找 source
↓
找最低 Load target
↓
source.load > θ × target.load
↓
COPY 当前 Request 可复用 Prefix
↓
Request 去 target
```

只用于：

```text
sanity check
旧设计对齐
机制诊断
```

---

## 8.4 B0 = R_REQ_KV

正式 request-driven baseline 保留会改变行为的约束结构：

```text
cache-hit gate
relative load gap
absolute load gap
source / target 跨节点
dual-end transfer admission
FIFO request waiting
finite timeout
admission 时重新规划
transfer 完成后再进入正常 service
统一 fallback
```

实现说明中的绝对阈值数值若缺乏 Load 单位，不直接照搬到 simulator。

---

# 9. Proactive Candidate 与 Trigger

## 9.1 共享的是规则，不是 Candidate Set

闭环 replay 后，不同策略的 Cache、Load、Transfer 和已见 Prefix 状态都会不同。

因此：

> **不能要求 T_ORACLE / T_PERSIST / T_RECENCY 在整个运行过程中拥有完全相同的 candidate set。**

正确要求是：

\[
\boxed{\text{共享 Candidate Generation / Eligibility / Budget Rules}}
\]

每个策略都从**自己当前状态**生成 candidate set。

如果需要比较不同 score 在完全相同 candidate 上的排序质量，则另开：

```text
same-state shadow ranking experiment
```

不与正式 closed-loop 结果混淆。

---

## 9.2 Candidate 生成顺序

所有 proactive 策略统一：

### Step A：生成当前可见候选

候选只来自：

```text
online-seen Prompt endpoints
online-seen branching points
当前 Pod 已发布 Prefix endpoints
```

不使用未来结构。

### Step B：资格过滤

Candidate 必须：

```text
不是 Root
当前至少存在一个完整合法 source
至少存在一个尚未完整持有该 Chain 的 target
没有等价 chain-target 在途副本
能够满足 pin / reservation / endpoint 基本条件
```

### Step C：计算策略 Score

分别计算：

```text
FutureHotness
PastHotness
RecencyScore
```

### Step D：排除零需求候选

最终冻结：

- `T_ORACLE`：只保留 `F(c,t,W) > 0`；
- `T_PERSIST`：只保留 `P_h(c,t) > 0`；
- `T_RECENCY`：只保留 `R(c,t) > 0`。

全零时不触发任何动作。

### Step E：排名 / 分位

- `T_ORACLE`：按 `F` 从高到低；
- `T_PERSIST`：在资格过滤后再取 Top-K；
- `T_RECENCY`：在“资格过滤且 score>0”的候选集合上计算分位数。

### Step F：Tie-break

分数相同时使用统一稳定顺序：

```text
score
→ candidate depth / cost secondary rule（若启用必须全策略一致）
→ stable chain_id
```

不使用未来信息打破平局。

---

# 10. 深链与浅链：最终解释

不再使用：

> “同一路径上只保留最深 Chain，因为深链完全支配浅链。”

这个结论不成立。

例如复制：

```text
A-B-C
```

确实同时复制了其祖先：

```text
A-B
```

因此未来：

```text
A-B-E
```

请求仍可能从该副本中使用 `A-B`。

所以正确表述是：

> **深链包含浅链能够提供的 Prefix，但需要更多 WireBytes、TransferTemporaryBytes、PublishedBytes 和准备时间，因此不一定比浅链更划算。**

同时：

\[
F(A-B-C)
\]

只统计未来完整包含 `A-B-C` 的 Request。

它不是这次复制中：

```text
A
A-B
A-B-C
```

所有祖先块未来潜在用途的完整价值估计。

因此 `F(c)`、`P_h(c)`、`R(c)` 在第一版中只是：

> **Chain-level simple ranking score**

不是精确的 action value。

这一局限必须在结果解释中保留。

---

# 11. T_ORACLE

Oracle：

\[
F(c,t,W)
=
\sum_{t<t_r\le t+W}
\mathbf{1}[c\preceq P_r]
\]

只读取未来 Request demand。

它是：

```text
Future-Hotness / Future-Demand Reference
```

不是 globally optimal scheduler。

---

## 11.1 Oracle 的评估边界

必须区分三个边界。

### 开发边界

开发阶段 Oracle：

> 不能读取正式 evaluation 段的未来需求来调参数。

如果 development window 的末尾不足完整 \(W\)，则该末尾只能作为 observation tail，不能继续产生需要跨边界的开发 Oracle 决策。

### 评分窗口边界

Evaluation Request 的主评分窗口结束，不代表 Oracle future window 必须立即截断。

如果预留了 observation tail，则评分窗口末尾的 Oracle 可以合法读取 tail 中的未来 demand。

例如：

```text
Evaluation: [25,45)
Observation tail: [45,60)
W = 5min
```

在：

```text
t = 44min
```

Oracle 查询：

```text
(44,49]
```

是合法的，因为 49min 仍在预留 observation tail 中。

### 数据末尾

如果：

\[
t+W
\]

超过 Trace 真正结束时间：

> 不能把缺失的未来数据解释成“未来没有需求”。

这类决策：

- 要么停止触发；
- 要么标记为 censored / incomplete horizon；

不能纳入完整未来窗口的 Oracle 行为比较。

---

## 11.2 Oracle 弱结果的结论

O1 不优于 B0 时先诊断：

```text
未来热点是否存在
服务压力是否存在
被动 P2P 是否已经足够快
主动 replica 是否来得及 ready
候选深度是否产生 routing leverage
target 是否选错
Cache pollution 是否抵消收益
Transfer contention 是否抵消收益
```

完成这些诊断后，才能得出：

> 当前 workload、资源模型和当前 proactive 策略族下没有观察到足够收益。

不能推广为主动 KV replication 普遍无效。

---

# 12. T_PERSIST

历史 demand：

\[
P_h(c,t)
=
\sum_{t-h<t_r\le t}
\mathbf{1}[c\preceq P_r]
\]

统一先做 Eligibility Filter，再在正分候选中取：

\[
Top-K
\]

主候选参数：

```text
h = 1min / 5min
K = 10
```

K 是 ranking shortlist 上限，不等于每个 tick 一定执行 K 个动作。

---

# 13. T_RECENCY

历史需求衰减：

\[
R(c,t)
=
\sum_{t_i\le t}
\mathbf{1}[c\preceq P_i]
\exp\left(
-\frac{t-t_i}{\tau_d}
\right)
\]

只统计真正包含该 Chain 的历史 Request。

在：

```text
eligible
且 score > 0
```

的 candidate set 上计算分位阈值。

主候选：

```text
quantile = 0.9
```

如果集合为空：

```text
no action
```

如果所有 score 为 0：

```text
no action
```

相同分数按 stable chain_id tie-break。

具体：

```text
decay_tau
```

属于策略参数，在 development 段选择后冻结。

---

# 14. Trigger Clock

所有 proactive 策略采用固定：

\[
TRIGGER\_TICK
\]

而不是按 Request completion 次数触发。

原因：

> 不同策略由于延迟和完成数不同，不应该因此获得不同数量的主动决策机会。

具体：

```text
trigger interval
```

属于策略参数。

可以从有限候选值开发选择，但正式 evaluation 前必须冻结。

---

# 15. Action 与 Placement

第一轮只研究：

\[
A_{COPY}
\]

MOVE 后置。

---

## 15.1 Source

source 必须：

```text
完整持有 Chain
当前 Block 可 pin
Transfer endpoint 可参与 admission
```

多个 source 使用统一 deterministic rule。

---

## 15.2 Target

target 必须：

```text
不是 source
尚未完整持有该 Chain
没有等价在途副本
能够满足 temporary reservation
能够满足 endpoint admission
```

Primary placement：

```text
优先低 Load
```

是否使用：

```text
prefix overlap / missing bytes
```

作为次级 tie-break，需要全策略统一。

---

# 16. Proactive Budget：最终语义

主动策略必须同时受：

```text
action-count budget
wire-byte budget
physical endpoint budget
capacity budget
```

限制。

---

## 16.1 Action-count

配置：

```text
max_proactive_actions_per_tick
max_new_replica_per_chain_per_tick
```

具体数值在 development 段选定。

---

## 16.2 Wire-byte Budget

最终采用**Token Bucket** 语义，避免“每分钟固定窗边界”产生人为突发。

参数：

```text
proactive_byte_rate      # bytes / second
proactive_burst_bytes
```

Bucket：

\[
Tokens(t)
=
\min(
Burst,
Tokens(t_0)+Rate\cdot(t-t_0)
)
\]

### Admission

主动 Transfer 真正 `TRANSFER_ADMIT` 前，按照预计：

\[
PlannedWireBytes
\]

检查 bucket。

若不足：

```text
SKIP current proactive action
```

不建立后台无限等待。

若足够：

```text
deduct PlannedWireBytes
```

并启动 Transfer。

一旦 Transfer 已经开始：

> 已扣除预算不因后续 duplicate commit、路由改变或失败而免费返还。

如果未来实现允许 admission 后但 wire 发送前被系统取消，可以单独定义 refund；第一版不提供免费取消。

Request-driven P2P 不受 proactive token bucket 限制，但与 proactive **共享真实 endpoint 和 bandwidth 资源**。

因此预算只限制“额外主动流量”，不人为限制 baseline 的救援能力。

---

# 17. S_TRIGGER_ONLY 与 S_COST_AWARE

## 17.1 S_TRIGGER_ONLY

表示：

> Candidate 经统一排序、物理资格和预算后，不再额外使用预测收益函数过滤。

仍受：

```text
Cache capacity
endpoint contention
byte budget
action budget
inflight dedup
```

限制。

---

## 17.2 S_COST_AWARE

后置。

第一轮主实验不使用一个未标定的加权总分决定成败。

优先用：

```text
Delay
Compute
Network
Capacity
Pollution
```

多指标直接报告。

只有 proactive headroom 已经存在后，再设计统一量纲或明确 normalization 的 Cost-Aware admission。

---

# 18. 指标体系

## 18.1 主指标：模拟 Prefill 完成延迟

\[
L_r
=
t_{SERVICE\_DONE,r}
-
t_{ARRIVAL,r}
\]

报告：

```text
mean
P50
P95
P99
```

拆分：

```text
request-driven P2P admission wait
P2P transfer time
ready queue wait
Prefill service time
```

Service model 未真实标定时，明确称：

> simulated Prefill completion delay under the assumed service model

---

## 18.2 Cache / Compute

报告：

```text
H_route
H_used
Prefix hit blocks / tokens
avoided Prefill compute
local existing cache hit
reactive-P2P-assisted hit
proactive-replica hit
```

---

## 18.3 Service Pressure

报告：

```text
time-weighted backlog
queue length
busy fraction
max Pod remaining service work
```

---

## 18.4 Request Distribution / Skew

短窗 Request count：

\[
SkewRatio
=
\frac{Gini_{actual}}
{Gini_{random}}
\]

random baseline 使用：

```text
相同窗口
相同 Request 数
相同 Pod 数
固定随机规则
```

同时报告：

```text
actual Gini
random mean Gini
random P95
```

若随机 Gini 为 0，则 ratio 记 NA，不加 epsilon 伪造。

---

## 18.5 Network

报告：

```text
reactive transfer count
proactive transfer count
total transfer count

reactive wire bytes
proactive wire bytes
total wire bytes

endpoint busy time
request-driven admission wait
timeout / rejection
```

---

## 18.6 Capacity

报告：

```text
PublishedUnique byte-seconds
ActivePrivate peak bytes
TransferTemporary peak bytes
replica redundancy
evicted bytes
```

---

## 18.7 Action Effectiveness

至少记录：

```text
ReplicaCreated
ReplicaReadyBeforeArrival
EquivalentMaxHitHolderCreated
RouteChangedByReplica
ReplicaActuallyUsed
ReplicaTooLate
FutureDemandButWrongTarget
EvictedBeforeUse
```

其中：

> `RouteChangedByReplica` 只是机制诊断。

副本被使用或改变了某一次 Routing，不代表这次 Request 的全部延迟改善都由该 replica 创造。

**最终净收益仍以 B0 与 O1 的完整 closed-loop 配对结果为准。**

---

# 19. Wasted Copy

按副本实例归因：

```text
(pod, block, residency_generation, transfer_id)
```

观察区间：

\[
(ready\_time,\ ready\_time+W]
\]

只有真实 `SERVICE_START` 使用该副本时才算 consumed/useful。

区分：

```text
no future demand
future demand but routed elsewhere
replica too late
evicted before use
duplicate wire bytes
failed wire bytes
```

Observation 不完整的实例：

```text
censored
```

不能强行记为 wasted。

---

# 20. Development / Warmup / Evaluation / Tail

必须分离：

```text
Development
Warmup
Evaluation
Observation Tail
Drain
```

具体时间切分根据 Trace audit 后冻结。

原则：

### Development

用于选择：

```text
trigger interval
K
decay_tau
proactive budget
service model parameter set
```

Development Oracle 不得读取正式 Evaluation future 来调这些参数。

### Warmup

正式各策略独立从相同初始条件演化。

Warmup 产生的 proactive transfer 成本必须保留统计，不能免费预置未来热点。

### Evaluation

主结果使用共同的 evaluation Request ID 集合。

### Observation Tail

用于：

- 评分窗口末尾 Oracle 的合法 future window；
- Wasted Copy 的完整观察；
- 完成评分 Request 的延迟观测。

Evaluation 结束不等于 Oracle future horizon 立即被截断。

### Data End

若 future horizon 超出 Trace 实际结束：

```text
incomplete horizon
```

不把未知未来解释成零需求。

---

# 21. 参数分类与冻结时机

## 21.1 物理模型参数

例如：

```text
μ
d0
B_effective
control latency
KV bytes / token
Cache capacity
```

原则：

1. 优先独立标定；
2. 无法标定时明确为模型假设；
3. 做有限敏感性分析；
4. 不把候选值写成生产真实参数。

---

## 21.2 策略参数

例如：

```text
trigger interval
W
h
K
decay_tau
relative load gate
absolute load gate
action budget
proactive byte rate
burst bytes
```

在 Development 段选择。

正式 Evaluation 前冻结。

---

## 21.3 研究 / 工程接受标准

例如：

```text
延迟改善多少算有意义
允许多少额外 WireBytes
允许多少容量开销
非劣阈值
统计置信标准
```

必须在正式主结果运行前确定。

但不要求所有值必须来自生产实测才能开始写 simulator。

---

# 22. 正确性 Gate

实现后必须满足：

```text
每个 Request 恰好完成一次
SERVICE_DONE >= ARRIVAL
普通策略 future reads = 0
未 commit KV 不可 hit
published+pinned 不重复计容量
MemoryUsed 永不超过 Capacity
pinned block 不被 eviction
Prefix closure 始终成立
Transfer wire / temporary / commit 分开守恒
duplicate commit 不重复计 resident
target replan 不产生 ghost load
每个 Request 同时最多有一个 committed target
失败 / timeout 正确清理旧状态
同 seed 重放完全一致
关闭主动动作后退化到相同 baseline
```

---

# 23. 研究 Gate

正式主比较：

\[
B0=R_{REQ\_KV}
\]

\[
O1=R_{REQ\_KV}+T_{ORACLE}
\]

联合判断：

```text
simulated Prefill completion delay
service pressure
Cache reuse / avoided compute
Load skew
total wire bytes
capacity overhead
action effectiveness
```

当前不预先把某个固定：

```text
P95 +5%
P99 <2% degradation
```

写成最终科学门槛。

这些属于正式 evaluation 前冻结的接受标准。

如果 O1 结果弱，先完成机制诊断，再决定是否停止当前 proactive 策略族。

---

# 24. 最小策略表

| ID | 策略 | 用途 |
|---|---|---|
| `A0` | `R_AFF` | 纯 Cache Affinity 参考 |
| `A1` | `R_LEAST` | 纯低 Load 参考 |
| `S0` | `R_REQ_KV_SIMPLE` | 原简化 request-driven P2P，诊断 |
| `B0` | `R_REQ_KV` | 正式 request-driven P2P baseline |
| `O1` | `R_REQ_KV + T_ORACLE` | 正式 future-demand reference |
| `H1` | `R_REQ_KV + T_PERSIST(h=1,K=10)` | 历史短窗 |
| `H2` | `R_REQ_KV + T_PERSIST(h=5,K=10)` | 历史长窗 |
| `H3` | `R_REQ_KV + T_RECENCY` | 历史衰减需求 |
| `M1` | `R_AFF + T_ORACLE` | 原任务 proactive placement 机制对照 |

`H1/H2/H3` 受 O1 结果与机制诊断控制。

---

# 25. 推荐实验顺序

## Step 1：Trace Capability Audit

确认：

```text
字段
时间单位
Prompt hash 语义
完整块与 tail
父路径一致性
总时长
最大 Prompt
P-only / P-C 可恢复性
```

输出：

```text
known
unknown
recoverable
not recoverable
```

---

## Step 2：Simulator Kernel Unit Tests

人工案例验证：

```text
cold request
sequential reuse
concurrent unfinished request
full-hit request still has non-zero service
transfer incomplete not visible
published+pinned no double count
source pin
target temporary reservation
duplicate commit
capacity pressure
request transfer FIFO
proactive/request endpoint conflict
target replan clears old load
shallow replica may not change R_AFF
deterministic replay
```

---

## Step 3：基础场景资格

运行：

```text
A0 = R_AFF
A1 = R_LEAST
S0 = R_REQ_KV_SIMPLE
B0 = R_REQ_KV
```

先确认真实 Trace 是否确实产生：

```text
request concentration
service pressure
reactive P2P
endpoint contention
```

---

## Step 4：Instant Placement Diagnostic

仅小样本：

```text
R_AFF
vs
R_AFF + T_ORACLE
```

验证：

```text
candidate
→ copy
→ replica
→ routing
```

不作为正式收益结论。

---

## Step 5：正式 Oracle 主实验

有限 Transfer：

\[
\boxed{
B0
\quad vs\quad
O1
}
\]

这是阶段 2 最重要的结果。

---

## Step 6：弱结果诊断

如果 O1 弱，检查：

```text
热点不足
压力不足
被动 P2P 已经快速铺开
主动 Transfer 太慢
候选粒度缺少 routing leverage
target placement 不对
Cache pollution
endpoint contention
```

必要时做 same-state shadow / single-action counterfactual，仅用于机制诊断。

---

## Step 7：历史策略

若存在可信 proactive space：

```text
H1
H2
H3
```

所有策略使用相同：

```text
candidate generation rules
eligibility rules
trigger clock
action budget
byte budget semantics
placement
capacity
transfer model
```

但 candidate set 由各自 closed-loop 状态产生。

---

## Step 8：关键敏感性

优先检查：

```text
service model
Cache capacity
bandwidth
control latency
FULL_PREFIX vs DELTA_SUFFIX
trigger interval
R_REQ_KV gate
proactive action budget
proactive byte rate
```

主结论稳定后，再扩展：

```text
S_COST_AWARE
MOVE
two-tier cache
stale KV directory
learning-based predictor
```

---

# 26. 当前最终冻结的研究设计

已经确认：

```text
1. 阶段 2 是多 Pod、有限 Cache、离散事件 closed-loop offline replay。

2. Mooncake 主实验采用 P-only，不伪造 completion KV。

3. 正式主比较是：
   B0 = R_REQ_KV
   O1 = R_REQ_KV + T_ORACLE。

4. R_AFF + T_ORACLE 只保留为机制诊断。

5. 正式 Headroom 从第一轮就使用有限 Transfer。

6. Request-driven 与 proactive 共享 P2P endpoint / bandwidth 资源。

7. Request-driven Transfer 使用 FIFO admission；
   proactive 只抢当前未被更早 request ticket 保留的空闲端点。

8. 已开始 proactive Transfer 不可免费抢占，
   后续 request-driven P2P 必须承担真实阻塞。

9. Load 使用 request-level service model，
   Route estimate 与实际 H_used / ServiceTime 分开。

10. MemoryUsed =
    PublishedUniqueBytes
    + ActivePrivateBytes
    + TransferTemporaryBytes。

11. Published block 被 pin 不重复计容量，
    且仍然可以被合法请求命中。

12. 不再认为最深 Chain 一般性支配浅 Chain。

13. Deep Chain 包含祖先 Prefix 的复用能力，
    但额外承担传输、容量和准备时间，因此价值不一定更高。

14. Oracle / Persist / Recency 共享 Candidate Generation 和资格规则，
    而不是强制共享相同 Candidate Set。

15. Top-K / Quantile 都在资格过滤后、正分 Candidate 上计算。

16. Oracle = Future-Hotness Reference，
    不是全局最优调度器。

17. Evaluation window 末尾可合法使用预留 observation tail；
    数据末尾缺失 future 不等于 future demand=0。

18. Proactive wire-byte budget 使用 token-bucket 语义。

19. 副本机制指标只用于解释，
    净收益仍以 B0/O1 完整闭环比较为准。

20. 参数分为物理参数、策略参数和接受标准，
    按不同阶段分别标定或冻结。
```

---

# 27. 仍待 Trace Audit / 实现前填写的参数

```text
所用 Mooncake Trace 文件及 SHA256
时间单位与 Prefix hash 语义
ServiceTime f(n,h) 主形式
μ
d0
KV bytes / token
每 Pod Cache capacity
R_REQ_KV absolute Load threshold
request-driven timeout
B_effective
control latency
trigger interval
Oracle W 主值与 sensitivity
Recency decay_tau
max proactive actions / tick
proactive byte rate
proactive burst bytes
Warmup / Evaluation / Tail 切分
Gini 时间窗口和 random repeat
正式研究 / 工程接受门槛
```

这些已经属于**参数冻结问题**，不再属于研究设计逻辑漏洞。

---

# 28. 最终研究逻辑总结

阶段 2 最终验证的是：

```text
Cache Affinity
    ↓
共享 Prefix 请求持续被少数 Cache holder 吸引
    ↓
形成请求集中，并可能进一步形成 Prefill 服务压力
    ↓
当前 request-driven P2P
只能在请求已经到达后再尝试补救
    ↓
Proactive Prefix Replication
尝试利用未来需求信息，在请求到达前建立额外 Cache replica
    ↓
如果 replica 在正确 Pod、正确深度、正确时间 ready
就可能减少后续 request-driven P2P、排队和热点压力
    ↓
但同时会消耗：
network bandwidth
endpoint availability
temporary memory
resident cache capacity
并可能产生 cache pollution / wrong-target copy
```

因此最终研究问题明确为：

> **在相同 request-driven P2P 底座和相同物理资源约束下，提前复制未来会复用的 KV Prefix，是否能够产生额外、可解释且成本可接受的系统收益；如果存在这种空间，只使用历史需求的简单策略能够实现其中多少。**

至此，阶段 2 的研究设计与模拟逻辑可以视为完成冻结。

下一步进入：

\[
\boxed{\text{Mooncake Trace Capability Audit}}
\]

随后开始 simulator kernel implementation。
