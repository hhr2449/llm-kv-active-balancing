# T1 Community Survey：主动 KV 干预、Cache-Aware Routing 与多 Pod KV 调度

> 项目：`llm-kv-active-balancing`  
> 目标：调研社区中与 **Prefix Cache 亲和、负载均衡、KV 跨实例迁移/复制、主动预热、未来复用预测** 相关的工作，为后续多 Pod replay simulator 的 baseline、策略空间与实验 Gate 提供依据。  
> 调研重点：优先 2024–2025 工作；同时补充 2026 年最新工作，用于判断当前研究空白是否仍然存在。  
> 更新时间：2026-09-10

---

## 1. 调研目标

本项目关注的问题不是单纯提高 KV Cache 命中率，而是：

> Cache-aware routing 会持续把共享前缀的请求送往已有缓存的 Pod，形成  
> **Cache 更多 → 更容易被路由 → 请求更多 → Cache 更全 → 进一步吸引请求**  
> 的正反馈，从而造成多 Pod 负载倾斜。

P2P KV Transfer 提供了一种新的调度自由度：把热点 KV Cache 复制或迁移到较空闲的 Pod，使后续请求可以在不丢失 Prefix Cache 复用收益的情况下被重新分散。

因此社区调查重点回答五个问题：

1. 社区如何在 **Cache reuse 与 Load balance** 之间做路由决策；
2. KV Cache 跨实例迁移/传输已经做到什么程度；
3. 是否已有 **无请求驱动的主动 KV replication / prefetch**；
4. 是否有人使用未来复用、workflow、历史热度等信号指导 Cache 行为；
5. 哪些策略应该成为后续多 Pod simulator 的 baseline。

---

## 2. 调研范围

| 类别 | 重点 |
|---|---|
| A. Prefix-cache-aware 路由 / 调度 | Cache affinity、Prefix locality、load-aware routing、热点 Prefix 扩散 |
| B. Disaggregation 下的 KV 迁移 / 传输 | Prefill/Decode disaggregation、KV streaming、远端 Cache、P2P |
| C. KV 跨实例复制 / 主动预热 | 无请求驱动的 replication、warming、background prefetch |
| D. Cache 侧预测 / 学习式调度 | Future reuse、reuse timing、workflow-aware eviction / prefetch |

统一抽取以下信息：

```text
方案：名称 / 年份 / 系统
trigger：何时触发
粒度：请求 / prefix / chain / block / page
决策信息：Cache / workload / load / future reuse 等
动作：Route / Copy / Move / Prefetch / Evict / Reclaim
评估：workload / baseline / 指标 / 是否 closed-loop
与本项目关系：可直接作为 baseline、机制参考或研究差异
```

---

# 3. A 类：Prefix-cache-aware 路由与负载均衡

## 3.1 Preble

**方案：Preble / ICLR 2025 / Distributed LLM Serving**

### 核心问题

Preble 是与本项目最接近的基础工作之一。它明确指出，分布式 Prefix Caching 中存在两个相互冲突的目标：

- 把请求放到已有共享 Prefix 的 GPU，可以减少 Prefill；
- 持续追随 Prefix Cache，又容易导致 GPU 间计算负载不均。

因此它的核心不是最大化单一 Cache hit，而是联合优化：

```text
Prefix reuse
+
Computation load balancing
+
Cache eviction / recomputation cost
```

### Trigger

Preble 在**每个请求到达时**进行调度。

其 E2（Exploitation / Exploration）思想可以概括为：

```text
共享 Prefix 足够有价值
        ↓
优先 exploit 已有 Prefix Cache

共享 Prefix 不足以抵消负载 / 重算成本
        ↓
explore 其他 GPU
```

此外还存在负载重平衡与 Hot Prefix autoscaling：当某些 Prefix/GPU 持续过载时，会扩大该 Prefix 的可服务范围。

### 粒度

- 路由：**Request**
- Cache identity：**Prefix / Prefix Tree Node**
- 热点扩散：**Prefix/Subtree**

### 决策信息

主要包括：

- 全局 Prefix Tree；
- Prefix 在哪些 GPU 上存在；
- Prefix 历史请求量；
- GPU 历史计算负载；
- Cache eviction 后的 recomputation cost；
- 当前请求未命中部分的 Prefill cost。

### 动作

主要动作是：

```text
Route request to existing cache holder
或
Route request to another GPU and rebuild prefix there
```

需要注意：

> Preble 的 Prefix autoscaling 主要依赖**未来请求被改投到新 GPU 后建立副本**，并不是以“无请求驱动的后台 P2P KV copy”为核心机制。

这正好形成一个重要 baseline：**等待请求到达后重建副本，和提前直接复制现有 KV，二者谁更划算？**

### 评估

Preble 做的是完整 closed-loop distributed serving 实验，而不是离线 prediction evaluation。论文报告在真实 workload/request arrival pattern 下，相比已有系统：

- Average latency：约 **1.5×–14.5×** 改善；
- P99 latency：约 **2×–10×** 改善。

### 对本项目的意义

**必须作为 routing baseline。**

后续 simulator 至少应有一个 `R_PREBLE`，用于判断：

> 主动 KV replication 的收益是否只是因为我们采用了比简单 Cache Affinity 更合理的路由。

### 原始链接

- ICLR 2025 官方页面：  
  https://proceedings.iclr.cc/paper_files/paper/2025/hash/5bc342f48de8264779952fac378f96dc-Abstract-Conference.html
- arXiv：  
  https://arxiv.org/abs/2407.00023

---

## 3.2 DualMap

**方案：DualMap / 2026 preprint / Distributed LLM Serving**

> 该工作属于 2026 年最新进展，不是 T1 原计划重点年份，但与本项目问题高度重合，因此作为当前竞争方案补充。

### 核心问题

DualMap直接针对：

```text
Cache affinity
vs
Load balancing
```

它认为单映射空间下，很难同时保证共享 Prefix 聚集与负载分散。

### Trigger

每个 Prefix 通过两个独立 Hash 映射到两个候选实例：

```text
Prefix P
 ├─ Candidate A
 └─ Candidate B
```

默认优先保留 Cache Affinity；当候选节点预计无法满足 TTFT SLO 或已经过载时，再切换到另一个较轻载候选。

### 粒度

- Request routing
- Pending request rebalancing
- Prefix → candidate-pair mapping

### 决策信息

- Prefix Cache locality；
- 两个 candidate 的 Cache 状态；
- pending prefill workload；
- predicted TTFT / SLO；
- 当前节点负载。

### 动作

核心不是复制 Cache，而是：

```text
限制 Prefix 的候选映射范围
+
在两个候选之间进行 load-aware routing
+
必要时迁移 pending requests
```

### 评估

论文使用真实 workload，在相同 TTFT SLO 下报告最高约 **2.25× effective request capacity**。

### 对本项目的意义

DualMap 是非常重要的“**不主动复制 KV**”强 baseline。

如果：

```text
R_DUALMAP ≈ proactive P2P replication
```

说明更好的 routing/mapping 已经能够解决大部分负载倾斜。

只有：

```text
proactive P2P >> R_DUALMAP
```

才能说明**改变 Cache placement 本身具有额外系统价值**。

### 原始链接

- arXiv：  
  https://arxiv.org/abs/2602.06502

---

## 3.3 GORGO

**方案：GORGO / 2026 preprint / Cross-region LLM Load Balancing**

### 核心问题

GORGO 将 Cache-aware routing 扩展到跨 Region 场景，目标函数同时考虑：

```text
Cache reuse
+
Compute/load
+
Network latency
```

### Trigger / 粒度

- 每 Request 路由；
- 根据当前 Cache locality、网络延迟与计算状态选 Region/实例。

### 与本项目关系

GORGO 当前仍主要是 **reactive routing**，并不是 Hot Prefix 的后台主动复制。

它的重要意义是说明：

> 一旦 KV Cache 可以跨拓扑调度，目标函数不能只考虑 Cache hit 和 Pod load，网络成本也是必要项。

### 原始链接

- arXiv：  
  https://arxiv.org/abs/2602.11688

---

# 4. B 类：Disaggregation 下的 KV 迁移与传输

## 4.1 Llumnix

**方案：Llumnix / OSDI 2024 / Dynamic LLM Scheduling**

### Trigger

Llumnix持续监测实例的资源状态，在出现：

- Load imbalance；
- Memory fragmentation；
- Priority / SLO 调整；
- Autoscaling需求

时重新调度已有请求。

其核心 abstraction 是实例 `freeness`，通过全局 scheduler 配对较拥挤 source 和较空闲 target。

### 粒度

**Active Request + 该 Request 的完整 in-memory KV state**

### 决策信息

- GPU/KV memory usage；
- Batch size；
- Queue / virtual demand；
- Request priority；
- Instance freeness。

### 动作

```text
Load imbalance 已经发生
        ↓
选择 active request
        ↓
live migrate request + KV
        ↓
继续执行
```

### 评估

完整 closed-loop 系统实验。论文报告：

- Tail latency 可降低一个数量级；
- 高优请求最高约 **1.5×** 加速；
- 在相似尾延迟下最高约 **36% cost saving**。

### 与本项目的差异

```text
Llumnix：
问题已经发生
→ 搬 active request

本项目：
预测未来 Prefix 复用 / 热点
→ 提前复制 reusable Prefix
→ 让未来请求拥有更多可选 Pod
```

因此 Llumnix 证明的是 **live migration 数据面与 reactive load balancing 可行**，不是 proactive Prefix placement。

### 原始链接

- OSDI 2024 官方页面：  
  https://www.usenix.org/conference/osdi24/presentation/sun-biao

---

## 4.2 DistServe

**方案：DistServe / OSDI 2024**

### Trigger

Prefill 与 Decode 被分离部署，一个请求完成 Prefill 后天然需要把状态交给 Decode worker。

### 粒度

**Request-level KV state**

### 决策信息

- TTFT / TPOT SLO；
- Prefill / Decode 计算需求；
- GPU placement；
- Cluster bandwidth。

### 动作

**Request-driven KV transfer**

不是未来 Prefix 的 proactive replication。

### 评估

完整系统实验。论文报告：

- 最多支持约 **7.4×** 更多请求；
- 或支持约 **12.6×** 更严格的 SLO。

### 对本项目的意义

主要用于证明：

> KV 跨机器传输已经是成熟 serving primitive；本项目真正需要研究的是 **transfer decision**。

### 原始链接

- OSDI 2024 官方页面：  
  https://www.usenix.org/conference/osdi24/presentation/zhong-yinmin

---

## 4.3 Splitwise

**方案：Splitwise / ISCA 2024**

### Trigger

Request 从 Prompt/Prefill phase 进入 Token/Decode phase。

### 粒度

**Request state / KV**

### 决策信息

- 两阶段的 compute / memory 特征；
- Hardware capability；
- Latency / power / cost；
- Network transfer。

### 动作

在不同阶段对应的机器之间转移 request state。

### 评估

论文报告：

- 最高约 **1.4× throughput + 20% cost reduction**；
- 或在相同 power/cost 下达到约 **2.35× throughput**。

### 对本项目的意义

与 DistServe 一样，重点是证明**跨实例状态传输不是主要研究空白**。

### 原始链接

- Microsoft Research：  
  https://www.microsoft.com/en-us/research/publication/splitwise-efficient-generative-llm-inference-using-phase-splitting/

---

## 4.4 Mooncake

**方案：Mooncake / FAST 2025 / Kimi Serving**

### 核心

Mooncake 建立 KVCache-centric disaggregated architecture：

```text
Prefill / Decode disaggregation
+
Distributed KVCache
+
Transfer Engine
+
Cache-aware scheduler
```

并利用 CPU DRAM、SSD 等资源扩展 KV 生命周期。

### Trigger / 粒度

大量 KV movement 是由请求处理、Cache lookup、Prefill/Decode 协作等触发。

底层能够支持跨节点 KV 传输与分布式 Cache，但论文重点不是未来 Hot Prefix 的 proactive replication policy。

### 决策信息

- Distributed KV availability；
- Prefix Cache reuse；
- 系统负载；
- TTFT/TBT SLO；
- 存储/计算资源。

### 评估

Mooncake 是实际 Kimi serving 系统。论文公开结果表明其在长上下文场景中显著提升 serving capacity。

### 对本项目的意义

Mooncake可以视为**现实数据面背景**：

> 数据怎么高速搬，已有工程能力；我们需要补的是“什么时候主动搬、搬什么、搬到哪里”。

### 原始链接

- FAST 2025 官方页面：  
  https://www.usenix.org/conference/fast25/presentation/qin
- arXiv：  
  https://arxiv.org/abs/2407.00079
- 官方 GitHub：  
  https://github.com/kvcache-ai/Mooncake

---

## 4.5 MemServe

**方案：MemServe / 2024**

### 核心

MemServe尝试统一：

```text
Inter-request Context Caching
+
Intra-request Disaggregated Serving
```

它通过 elastic memory pool 管理跨 serving instances 的 KV，并维护全局 Prompt Tree。

### 粒度

Prompt / KV Cache object。

### 决策信息

- Global Prompt Tree；
- Context Cache locality；
- Distributed KV availability；
- Serving instance state。

### 对本项目的意义

它对我们构建：

```text
Global Prefix Tree
+
Pod-local Cache
+
Distributed state view
```

的 simulator 数据结构有直接参考价值。

### 原始链接

- arXiv：  
  https://arxiv.org/abs/2406.17565

---

## 4.6 DéjàVu

**方案：DéjàVu / ICML 2024 / KV-cache Streaming**

### 核心

DéjàVuLib 支持高效 KV Cache streaming，并被用于：

- Prompt/Token disaggregation；
- Microbatch swapping；
- State replication for fault tolerance。

### 粒度

Request KV state / streamed KV Cache。

### Trigger

主要由：

- Pipeline/resource management；
- Memory pressure；
- Fault tolerance

触发。

### 对本项目的意义

DéjàVu 证明：

> “KV 主动复制到其他机器”作为 data movement primitive 已存在。

但其 replication 目标主要是**容错**，而不是 Hot Prefix load balancing。

### 原始链接

- ICML/PMLR：  
  https://proceedings.mlr.press/v235/strati24a.html
- arXiv：  
  https://arxiv.org/abs/2403.01876

---

# 5. C 类：主动 KV 复制 / 预热

## 5.1 Vertumnus

**方案：Vertumnus / 2026 preprint / Adaptive Context Parallelism**

> 这是本次调查中与“主动跨 Worker Prefix replication”最接近的工作。论文于 2026-09-04 提交，属于非常新的 preprint。

### 核心机制

Vertumnus维护全局 Prefix Cache 管理器，并跟踪 Prefix node 的近期访问情况与副本数。

可以将其核心思想抽象成：

\[
\text{hotness per replica}
=
\frac{\text{recent access}}{\text{replica count}}
\]

当每份副本承担的近期 demand 超过阈值时：

```text
Hot Prefix
   ↓
增加 Prefix replica
```

需求下降后则回收副本，并使用不同的 replication/reclaim threshold 形成 hysteresis，避免频繁抖动。

历史访问统计还会 decay，因此其本质是：

> **Exponentially-decayed recent-frequency based proactive replication**

### 粒度

**Prefix-tree node / Prefix chain / missing blocks**

### Target selection

目标 Worker 倾向于：

1. 已经拥有更长 ancestor Prefix；
2. 有足够 Cache capacity；
3. 当前 workload 较低。

传输时只复制 target **缺失的 Prefix Blocks**，而非无条件复制整个链。

### Trigger

Periodic background scan。

论文设置中 Prefix Manager 以 seconds-scale 周期扫描并触发后台 Cache replication。

### Closed-loop 意义

复制之后，原本只有单个 holder 的 Hot Prefix 会存在于多个 Worker，因此 scheduler 能够：

```text
保持 Cache hit
+
在多个 holder 之间选择轻载 Worker
```

这正是本项目要实现的闭环。

### 对本项目的影响

非常重要：

> **“历史热度驱动的 proactive cross-worker Prefix replication”已经不能作为本项目最终创新，而应该成为强 baseline。**

因此后续应该实现：

```text
T_FREQ_DECAY
```

模拟 Vertumnus-style history-driven proactive replication。

真正值得研究的是：

> Future reuse information 能不能超过 decayed historical hotness。

### 原始链接

- arXiv：  
  https://arxiv.org/abs/2609.04774

---

## 5.2 LMCache 主动 Prefetch API

**方案：LMCache / 当前工程能力**

LMCache 当前 HTTP API 已明确支持：

```text
POST /cache/prefetches
```

用于在真实请求到达之前，把 token sequence 对应的 KV 从 L2 预热到指定节点 L1。

官方文档举出的应用场景包括：

- traffic shift；
- hot shared system prompt。

### 对本项目的意义

LMCache说明：

> **“无 Request 驱动的 Cache warming”正在成为实际 serving 系统的工程 primitive。**

但它提供的是：

```text
Action primitive
```

而不是：

```text
Decision policy
```

它没有替我们回答：

- 哪个 Prefix 应该 warm；
- 什么时候 warm；
- warm 到哪个实例；
- 是否值得占用网络和 Cache 容量。

这正是本项目策略层的问题。

### 原始链接

- 官方 HTTP API：  
  https://docs.lmcache.ai/mp/http_api.html

---

## 5.3 Pallas

**方案：Pallas / 2026 preprint / AI-RAN**

### 核心

Pallas 是一个明确以 **Proactive KV Cache Migration** 命名的系统。

它预测移动用户即将切换到哪个基站，并在 handover 前提前准备 KV：

```text
predict target
    ↓
start preparation before handover
    ↓
handover 时 target 已拥有可继续推理的 KV
```

### 决策信息

- Mobility prediction；
- Runtime telemetry；
- Predicted handover time。

### 与本项目差异

Pallas证明：

> “预测未来位置 → 主动跨节点准备 KV”这一抽象方向已经出现。

但其 future signal 是 **mobile handover**，系统目标是降低服务中断，而不是数据中心内部的 Cache Affinity load skew。

### 原始链接

- arXiv：  
  https://arxiv.org/abs/2608.16477

---

# 6. D 类：预测式 / Workflow-aware KV 管理

## 6.1 KVFlow

**方案：KVFlow / NeurIPS 2025**

### 核心问题

LRU只知道“多久以前使用过”，不知道“多久以后还会使用”。

KVFlow利用 Agent Workflow 的执行结构建立 **Agent Step Graph**，为 Agent 计算：

```text
Steps-to-Execution (STE)
```

即距离未来下一次执行还有多少 workflow step。

### 粒度

**KV Prefix Tree Node**

STE 被映射到 Cache node，用于细粒度 eviction priority。

### Trigger / 动作

KVFlow同时做两件事：

1. future-aware KV eviction；
2. 对下一步即将执行的 Agent 执行 **CPU → GPU background proactive prefetch**。

### 决策信息

- Workflow graph；
- Current execution state；
- Steps-to-Execution。

### 评估

与 SGLang hierarchical radix cache 做 closed-loop 比较：

- 大 Prompt 单 workflow：最高约 **1.83×**；
- 多并发 workflow：最高约 **2.19×**。

### 对本项目的意义

KVFlow非常重要，因为它证明：

\[
\text{future reuse timing}
\rightarrow
\text{proactive cache action}
\rightarrow
\text{closed-loop system gain}
\]

这一逻辑是成立的。

但其动作发生在：

```text
同一实例 CPU → GPU
```

而本项目关注：

```text
Pod A → Pod B
+
routing
+
load balancing
```

因此机制有明显差异。

### 原始链接

- NeurIPS 2025：  
  https://proceedings.nips.cc/paper_files/paper/2025/hash/b7971d31a7d5eb0f1eed2f8f6f368195-Abstract-Conference.html
- arXiv：  
  https://arxiv.org/abs/2507.07400

---

## 6.2 KVCache Cache in the Wild

**方案：KVCache Cache in the Wild / USENIX ATC 2025**

### 核心发现

这篇工作对大型云服务的真实 KV workload 做了系统 characterization。

关键观察包括：

- KV reuse 在不同请求之间高度不均；
- 全局 reuse time / probability 很复杂；
- 但对**具体 request category**，reuse pattern 往往更加可预测；
- Cache eviction 应该利用 workload-specific reuse distribution，而不是只依赖 LRU/LFU。

### Trigger

**Cache capacity pressure / eviction**

### 粒度

KV Cache entry / Cache management unit。

### 决策信息

- Workload/request category；
- Historical reuse time distribution；
- Reuse probability；
- Prefix position / reuse value。

### 评估

使用大型云厂商真实 traces，并在有限 Cache capacity 下评估 workload-aware eviction。

### 对本项目的意义

它给 `T_RECENCY / T_PERSIST / T_FREQ_DECAY` 提供了生产 workload 依据：

> 历史 reuse pattern 是一个合理且必须击败的 baseline，而不是随意构造的 toy heuristic。

### 原始链接

- USENIX ATC 2025：  
  https://www.usenix.org/conference/atc25/presentation/wang-jiahao

---

## 6.3 PBKV

**方案：PBKV / 2026 preprint / Prediction-Based KV-Cache Management**

### 核心

PBKV针对动态 Agent Workflow，预测未来若干步 Agent invocation，并据此估计 Cache entry 的 future reuse potential。

其思想可抽象为：

```text
Future agent invocation prediction
       ↓
Future cache reuse probability
       ↓
Eviction + Prefetch
```

### 粒度

Workflow / Agent / Cache entry。

### 关键设计：Conservative Prefetch

PBKV非常值得借鉴的一点是：

> **预测结果不能无限制创造新的系统压力。**

它对 Prefetch 设置显式的：

- Free memory budget；
- Bandwidth budget；
- Conservative resource constraints。

因此 prediction noise 较大时，也尽量避免“为了预测可能使用的 Cache，反而淘汰确定有价值的 Cache”。

### 评估

论文报告：

- Dynamic workflow 相比 LRU 最高约 **1.85× speedup**；
- Static workflow 相比 KVFlow 最高约 **1.26×**。

### 对本项目的意义

直接影响后续 `T_FUTURE`：

预测热 Prefix 后不能：

```text
预测热 → 无脑复制
```

而应该：

```text
预测热
  ↓
Target 有空闲容量？
  ↓
Transfer budget 足够？
  ↓
不会牺牲更高价值 Cache？
  ↓
COPY
```

### 原始链接

- arXiv：  
  https://arxiv.org/abs/2605.06472

---

## 6.4 CacheScout

**方案：CacheScout / 2026 preprint**

### 核心

CacheScout 不要求已知 workflow graph，而是：

> **在线学习 Agent execution transition**

然后用预测到的 future execution 同时指导：

- Cache eviction；
- Background proactive prefetch。

### 与 KVFlow / PBKV 区别

```text
KVFlow     已知 workflow graph
PBKV       预测动态 workflow
CacheScout 在线学习 transition
```

### 评估

vLLM prototype 报告：

- KV hit：+10–18 percentage points；
- Mean TTFT：降低 18%–45%；
- Peak throughput：最高 +57%。

### 对本项目的意义

说明到 2026 年：

> “future reuse prediction → proactive prefetch”本身已经不是唯一创新点。

本项目必须把研究差异放到：

```text
Cross-Pod
+
Affinity-induced load skew
+
Future-reuse-aware replication
+
Transfer / capacity cost
+
Closed-loop routing
```

### 原始链接

- arXiv：  
  https://arxiv.org/abs/2608.14624

---

# 7. 另一条竞争路线：Global KV Store

## 7.1 BanaServe

**方案：BanaServe / 2025 preprint**

### 核心问题

BanaServe明确指出：

> Prefix-cache-aware routing 会让高 Cache hit 的 Prefill node 持续吸引请求，从而造成 load skew。

它选择的解决思路并不是主动复制 Hot Prefix，而是：

```text
Global KV Cache Store
```

使 Router 不再被本地 Cache placement 绑定，从而可以执行更纯粹的 load-aware scheduling。

### 其他机制

还包括：

- Layer-level weight migration；
- Attention-level KV migration；
- Dynamic Prefill/Decode resource balancing。

### 对本项目的意义

说明解决 Cache Affinity skew 至少有两条路线：

```text
路线 A：Globalize KV
Cache placement 与 routing 解耦

路线 B：Pod-local KV + selective replication
扩大 Hot Prefix 的可选 Pod 集合
```

本项目属于路线 B。

腾讯已有高速 P2P KV Transfer，因此 selective replication 在实际工程背景下仍然合理。

### 原始链接

- arXiv：  
  https://arxiv.org/abs/2510.13223

---

# 8. 横向对照

| 工作 | Cache-aware routing | 跨实例 KV | 主动复制/预热 | Future reuse 信号 | Load-aware | 改变未来 routing |
|---|---:|---:|---:|---:|---:|---:|
| Preble | ✅ | 非核心 | Prefix autoscale | 历史 Prefix demand | ✅ | ✅ |
| Llumnix | 部分 | ✅ | ❌ | ❌ | ✅ | Active request |
| DistServe | ❌ | ✅ | ❌ | ❌ | 资源级 | ❌ |
| Splitwise | ❌ | ✅ | ❌ | ❌ | 资源级 | ❌ |
| Mooncake | ✅ | ✅ | 有底层能力 | 非核心 | ✅ | 部分 |
| MemServe | ✅ | ✅ | 部分 | locality | 部分 | ✅ |
| DéjàVu | ❌ | ✅ | ✅（容错） | ❌ | ❌ | ❌ |
| KVFlow | ❌ | CPU→GPU | ✅ | 已知 future | ❌ | ❌ |
| KVCache Wild | ❌ | ❌ | ❌ | reuse distribution | ❌ | ❌ |
| DualMap | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ |
| Vertumnus | ✅ | ✅ | ✅ | decayed historical hotness | ✅ | ✅ |
| PBKV | ❌ | memory tier | ✅ | ✅ prediction | ❌ | ❌ |
| CacheScout | ❌ | memory tier | ✅ | ✅ online prediction | ❌ | ❌ |
| Pallas | 特殊场景 | ✅ | ✅ | mobility prediction | target-aware | ✅ |
| BanaServe | ✅ | Global store | 不需要 | ❌ | ✅ | ✅ |
| **本项目** | **✅** | **✅** | **✅** | **future Prefix reuse/timing** | **✅** | **✅** |

---

# 9. 当前研究空间判断

社区工作已经逐步覆盖了：

```text
Cache-aware routing
KV migration
Hot-Prefix replication
Future-aware Cache prefetch
Global KV sharing
```

因此本项目不能再简单表述为：

> “提出主动复制 KV Cache 来改善负载均衡。”

因为 Vertumnus 等工作已经非常接近。

目前更准确的研究问题是：

> **在 Cache Affinity 导致的多 Pod 负载倾斜下，Future-reuse-aware proactive Prefix replication 能否比基于当前负载与历史热度的 replication，以更低的搬运与误复制成本获得更好的 Cache-preserving load balance？**

即：

\[
\boxed{
Future\ Reuse
+
Cross\text{-}Pod\ Replication
+
Load\ Balance
+
Transfer/Capacity\ Cost
+
Closed\text{-}loop\ Routing
}
\]

仍然是目前比较明确的交叉空间。

---

# 10. 对 T2 Simulator 的直接修改

原计划的 baseline：

```text
R_AFF
R_LEAST
R_REQ_KV
T_ORACLE
T_PERSIST
T_RECENCY
```

现在已经不足以形成强比较。

建议冻结第一版策略阶梯：

| ID | 策略 | 作用 |
|---|---|---|
| B0 | `R_AFF` | 纯 Cache Affinity，复现负载倾斜 |
| B1 | `R_LEAST` | 完全放弃 Cache locality 的负载均衡下界 |
| B2 | `R_REQ_KV` | 腾讯当前 request-driven P2P 主基线 |
| B3 | `R_PREBLE` | Cache reuse + load cost 的强 routing baseline |
| B4 | `R_DUALMAP` | 不复制 KV 的双候选 Cache/load routing |
| B5 | `T_FREQ_DECAY` | Vertumnus-style history-driven proactive replication |
| O1 | `T_ORACLE` | 完美未来信息下的 proactive replication 上限 |
| P1 | `T_FUTURE` | Future-reuse-aware proactive replication |

其中：

> `T_FREQ_DECAY` 应当成为预测方案最重要的竞争 baseline。

真正关键的比较不是：

\[
T_{FUTURE} \ vs \ R_{AFF}
\]

而是：

\[
\boxed{
T_{FUTURE} \ vs \ T_{FREQ\_DECAY}
}
\]

只有 Future information 能超过强历史热度策略，预测才具有额外系统价值。

---

# 11. 建议的三个实验 Gate

## Gate 1：Proactive replication 本身有没有 Headroom

比较：

\[
T_{ORACLE} \quad vs \quad R_{REQ\_KV}
\]

若 Oracle 相比 request-driven P2P 几乎没有净收益：

> 不继续做复杂 prediction。

这说明主动提前复制本身 headroom 不足。

---

## Gate 2：改变 Cache placement 是否优于只优化 Routing

比较：

\[
T_{ORACLE}
\]

与：

\[
R_{PREBLE}, R_{DUALMAP}
\]

如果差距很小：

> 真正应该优化的是 routing，而不是 proactive Cache placement。

---

## Gate 3：Future Prediction 是否优于历史 Hotness

只有前两个 Gate 通过后再比较：

\[
T_{FUTURE}
\quad vs \quad
T_{FREQ\_DECAY}
\]

最终不仅看 Saved Tokens，还要同时满足：

```text
Net system gain ↑
Load skew ↓
Wasted-copy ↓
Transfer bytes / useful reuse 更优
Cache hit 不明显受损
```

只有这样才能说明：

> **Future reuse information 为 proactive KV replication 提供了历史热度之外的额外系统价值。**

---

# 12. T1 阶段结论

目前社区工作的关系可以概括为：

```text
Preble
└─ Cache reuse vs Load-aware routing

DualMap
└─ Cache affinity mapping vs Load balancing

Llumnix
└─ Load imbalance 发生后的 active-request migration

Mooncake / DistServe / DéjàVu
└─ KV 跨实例传输与数据面能力

Vertumnus
└─ History-driven proactive Prefix replication

KVFlow / PBKV / CacheScout
└─ Future information → proactive Cache action

BanaServe
└─ Global KV Store，彻底解除 Routing 对 Cache placement 的依赖
```

因此 T1 给 T2 的核心结论是：

> **不要把“主动复制 Hot Prefix”本身当作研究创新。**  
> 下一阶段真正值得验证的是：在相同 Cache、负载和传输条件下，**Future reuse / reuse timing 是否能够比强历史热度策略更早、更准确地选择值得复制的 Prefix，从而获得更高的系统净收益。**

---

# 13. 原始资料索引

## 2024–2025 主体工作

1. **Preble: Efficient Distributed Prompt Scheduling for LLM Serving**  
   ICLR 2025  
   https://proceedings.iclr.cc/paper_files/paper/2025/hash/5bc342f48de8264779952fac378f96dc-Abstract-Conference.html  
   https://arxiv.org/abs/2407.00023

2. **Llumnix: Dynamic Scheduling for Large Language Model Serving**  
   OSDI 2024  
   https://www.usenix.org/conference/osdi24/presentation/sun-biao

3. **DistServe: Disaggregating Prefill and Decoding for Goodput-optimized Large Language Model Serving**  
   OSDI 2024  
   https://www.usenix.org/conference/osdi24/presentation/zhong-yinmin

4. **Splitwise: Efficient Generative LLM Inference Using Phase Splitting**  
   ISCA 2024  
   https://www.microsoft.com/en-us/research/publication/splitwise-efficient-generative-llm-inference-using-phase-splitting/

5. **Mooncake: A KVCache-centric Disaggregated Architecture for LLM Serving**  
   FAST 2025  
   https://www.usenix.org/conference/fast25/presentation/qin  
   https://arxiv.org/abs/2407.00079  
   https://github.com/kvcache-ai/Mooncake

6. **MemServe: Context Caching for Disaggregated LLM Serving with Elastic Memory Pool**  
   2024  
   https://arxiv.org/abs/2406.17565

7. **DéjàVu: KV-cache Streaming for Fast, Fault-tolerant Generative LLM Serving**  
   ICML 2024  
   https://proceedings.mlr.press/v235/strati24a.html  
   https://arxiv.org/abs/2403.01876

8. **KVFlow: Efficient Prefix Caching for Accelerating LLM-Based Multi-Agent Workflows**  
   NeurIPS 2025  
   https://proceedings.nips.cc/paper_files/paper/2025/hash/b7971d31a7d5eb0f1eed2f8f6f368195-Abstract-Conference.html  
   https://arxiv.org/abs/2507.07400

9. **KVCache Cache in the Wild: Characterizing and Optimizing KVCache Cache at a Large Cloud Provider**  
   USENIX ATC 2025  
   https://www.usenix.org/conference/atc25/presentation/wang-jiahao

10. **BanaServe: Unified KV Cache and Dynamic Module Migration for Balancing Disaggregated LLM Serving in AI Infrastructure**  
    2025 preprint  
    https://arxiv.org/abs/2510.13223

## 2026 最新进展补充

11. **DualMap: Enabling Both Cache Affinity and Load Balancing for Distributed LLM Serving**  
    https://arxiv.org/abs/2602.06502

12. **GORGO: Maximizing KV-Cache Reuse While Minimizing Network Latency in Cross-Region LLM Load Balancing**  
    https://arxiv.org/abs/2602.11688

13. **Efficient Serving for Dynamic Agent Workflows with Prediction-based KV-Cache Management (PBKV)**  
    https://arxiv.org/abs/2605.06472

14. **Learning Agent Execution for KV-Cache Management in Agentic Serving (CacheScout)**  
    https://arxiv.org/abs/2608.14624

15. **Pallas: A Proactive KV Cache Migration Framework for LLM Inference in AI-RAN**  
    https://arxiv.org/abs/2608.16477

16. **Adaptive Context Parallelism for Production LLM Serving (Vertumnus)**  
    https://arxiv.org/abs/2609.04774

17. **LMCache HTTP API — Cache Prefetch**  
    https://docs.lmcache.ai/mp/http_api.html

---

## 14. 下一步

T1 到这里已经能够支持 T2 的第一版设计。

下一步不建议继续无限扩充论文数量，而是进入：

> **T2：Multi-Pod Replay Simulator 设计**

首先冻结：

1. 请求 / Prefix Chain / Block 数据结构；
2. Pod-local Prefix Cache；
3. Closed-loop routing；
4. `R_AFF / R_LEAST / R_REQ_KV / R_PREBLE / R_DUALMAP`；
5. `T_FREQ_DECAY / T_ORACLE`；
6. Transfer cost、Cache capacity、load metric 与评估指标。

在 Oracle 与 history-driven baseline 的收益空间确认之前，不训练新的 Future predictor。
