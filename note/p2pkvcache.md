# P2P KV Transfer 代码改动说明

## 1. 文档定位

这份文档面向需要阅读、调试或继续开发 P2P KV Transfer 的同学。它描述的是代码仓库里的真实实现、模块职责、请求链路、状态机和失败语义，不是 KM 文章的改写版。

本文基于以下代码快照整理：

- 仓库：`/Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct`
- 分支：`minimal-r3-trace`
- HEAD：`5f65993 fix: prevent rank-skewed P2P scheduler fail-stop`
- 对照基线：`e4f7977 archive/original-import`
- 相对基线：40 个文件，约新增 13,934 行、删除 808 行

如果后续代码继续演进，应先确认当前分支和提交，再对照本文中的行号阅读。本文的链接都指向上述本地快照。

### 1.1 先看结论

这套改动可以拆成四层：

```text
┌─────────────────────────────────────────────────────────────┐
│ Gateway：根据 KV Events 找缓存源，选择 Prefill 目标           │
│        + 负载阈值 + 双节点公平准入 + P2P 控制请求             │
└───────────────────────────┬─────────────────────────────────┘
                            │ HTTP / JSON / headers
┌───────────────────────────▼─────────────────────────────────┐
│ Tokenizer / Scheduler：接收控制请求，异步推进 Pending 状态机    │
│        + source/target 两端按 rank 做一致性协商               │
└───────────────────────────┬─────────────────────────────────┘
                            │ Mooncake sender / receiver
┌───────────────────────────▼─────────────────────────────────┐
│ Data Plane：GPU KV 或 HiCache host buffer 的点对点传输          │
│        + 复用现有 Mooncake 注册、session、page transfer        │
└───────────────────────────┬─────────────────────────────────┘
                            │ commit / rollback / quarantine
┌───────────────────────────▼─────────────────────────────────┐
│ Cache：只有通过 rank-wide terminal fence 后才发布缓存所有权     │
└─────────────────────────────────────────────────────────────┘
```

核心原则是：P2P 失败不能影响正常推理；但是一旦数据路径已经启动，不能把“是否安全释放资源”当作普通失败处理。代码因此区分了：

- `fallback`：可以安全回退到本地重算；
- `quiesced`：数据面已经被证明停止，可以释放锁和临时资源；
- `uncertain / quarantined`：数据面状态无法证明，先返回重算结果，但保留资源并后台回收；
- `cache integrity error`：缓存所有权已经无法证明一致，触发 fail-stop，而不是继续运行并冒险污染缓存。

### 1.2 设计目标与当前实现的差异

KM 中提出的 ECT 可以写成：

```text
ECT(target) = 排队等待时间 + KV 搬运时间 + 剩余 Prefill 计算时间
```

这是选点方向，但当前快照中的 `P2pCacheAwareSelector` 没有显式计算这个公式，也没有传输带宽模型或剩余计算时间模型。当前的实际规则是：

1. 用 KV Events 在 Gateway 侧找到最长命中前缀的 source；
2. 命中率必须大于 `cache_threshold`；
3. source 和 target 必须是不同节点；
4. 只有当 source 的负载同时超过 target 的绝对阈值和相对阈值时，才触发远程 KV；
5. target 在候选节点中按当前 `load()` 选择低负载节点；
6. 若条件不满足，回到既有的 Prefill/Decode policy。

因此，读代码时应把它理解为“为 ECT 目标准备的保守 P2P 选点实现”，而不是已经完成的完整 ECT 调度器。实现位置见 [`p2p.rs`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/policies/p2p.rs:45>)。

## 2. 从一次请求开始：完整链路

下面按一次带有远程 KV 决策的 Chat 请求说明。

### 2.1 Gateway 生成精确 token 序列

KV Cache 的命中判断基于 token 序列，不是原始文本。因此 Gateway 必须尽量拿到和 Prefill worker 一致的 token IDs：

1. Chat 请求可以由上游直接携带 `input_ids`；
2. 没有 `input_ids` 时，Gateway 使用 untruncated tokenizer；
3. Completion 请求直接从 `prompt` 得到 token IDs；
4. Gateway 再根据 `BlockSizeOracle` 选择普通 hash 或 EAGLE/bigram hash。

Chat 的 `input_ids` 在 Gateway server 层通过 envelope 取出，再沿着 `RouterTrait -> RouterManager -> PDRouter` 传递。相关改动见：

- [`server.rs`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/server.rs:89>)
- [`router_manager.rs`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/routers/router_manager.rs:538>)
- [`routers/mod.rs`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/routers/mod.rs:98>)
- [`protocol.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/entrypoints/openai/protocol.py:592>)

这里不能随意截断 prompt。少一个中间 token，后续链式 block hash 就全部不同，Gateway 会看不到 worker 已经存在的缓存。

### 2.2 Gateway 根据 KV Events 找 source

worker 的缓存变化通过 ZMQ KV Events 发布，Gateway 维护一份只读的全局前缀归属视图：

```text
worker /server_info
        │
        ▼
discovery.rs：解析 kv_events 配置、page_size、dp_size、EAGLE 标志
        │
        ▼
subscriber.rs：每个 (worker_url, dp_rank) 建立一个 ZMQ SUB
        │
        ▼
wire.rs：解码 msgpack 的 BlockStored / BlockRemoved / AllBlocksCleared
        │
        ▼
index.rs：去重、检测 sequence gap/reset、投递到 pump
        │
        ▼
tree.rs：HashTree 记录每个 block chain 的 worker、rank 和 storage medium
        │
        ▼
p2p.rs：match_prefix_by_worker() 找最长命中 source
```

入口是 [`KvEventIndex`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/policies/kv_events/index.rs:66>)。worker 注册和注销由 worker workflow 接入：

- [`update_policies.rs`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/core/steps/worker/shared/update_policies.rs:107>) 在 Prefill worker 加入时调用 `index.add_worker()`；
- [`remove_from_policy_registry.rs`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/core/steps/worker/local/remove_from_policy_registry.rs:37>) 在 worker 移除时调用 `index.remove_worker()`。

KV Events 的具体处理有几个容易忽略的点：

- Gateway 先访问 worker 的 `/server_info`，拿不到可用发布配置时，只关闭该 worker 的 cache-aware 路径，不影响它加入普通 worker registry；
- 每个 DP rank 是独立的缓存持有者，树中的 ID 是 `KvWorkerId { url, dp_rank }`；
- `page_size` 和 EAGLE/bigram 模式通过进程级 `BlockSizeOracle` first-wins 建立，后续不一致的 worker 会被拒绝订阅；
- `remove_worker()` 先从 `live_workers` 删除，再取消 subscriber、清理树和 sequence cursor，避免已经排队的旧事件重新污染树；
- sequence 重复会跳过，sequence 回退或出现 gap 会先清理该 worker 的树状态，再应用当前可见事件；
- `wire.rs` 对 hash/token 数量设有上限，避免异常 msgpack 长度导致 Gateway 大量分配内存。

KV Events 的模块总览见 [`kv_events/mod.rs`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/policies/kv_events/mod.rs:1>)。

### 2.3 选 source、target 和是否触发 P2P

[`P2pCacheAwareSelector`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/policies/p2p.rs:51>) 的具体流程如下：

```text
prepare_request(token_ids)
    ├─ 读取 block_size / is_bigram
    ├─ 计算普通或 bigram block hashes
    └─ 生成 P2pPreparedRequest

match_source_prepared()
    ├─ HashTree.match_prefix_by_worker()
    ├─ 选最长命中 block chain
    ├─ 同长度时按 worker load() 打破平局
    ├─ 命中率 <= cache_threshold：无 P2P 决策
    └─ 计算 matched_tokens 和 source bootstrap address

selection_for_target_match()
    ├─ source == target 节点：无 P2P
    ├─ source_load - target_load <= balance_abs_threshold：无 P2P
    ├─ source_load <= target_load * balance_rel_threshold：无 P2P
    └─ 满足时：target 使用低负载节点，remote_kv 携带 source/target/token_ids
```

注意 `matched_tokens` 和实际传输长度不是永远相同：worker 侧还会做 page 对齐，EAGLE/bigram 还会处理逻辑 cache token 与原始 token 的边界关系。

### 2.4 双节点公平准入

Gateway 不能只锁 target。一次 P2P 同时占用 source 和 target 的数据面资源，因此 [`P2pNodeGate`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/routers/http/p2p_node_gate.rs:320>) 原子地锁住两个节点。

`acquire_best_with()` 的行为：

- 为每个等待请求登记 FIFO ticket；
- source 是固定资源，target 从当前可用候选中选择最低负载且未被占用的节点；
- 早到请求会保护自己的 source 和已选 target，避免后来的请求抢走资源；
- plan 与最终状态之间如果发生变化，释放临时 lease，保留 ticket，按 1 秒 tick 重新规划；
- 默认整个准入预算是 120 秒，超时后放弃 P2P，重新选择普通 Prefill route；
- `P2pNodeLease` 使用 RAII，正常完成、异常、取消都必须释放两个节点。

这层解决的是 Gateway 内部的“同一个节点不能同时参与多个 P2P”问题。worker 侧还有一层 pair gate 和 communicator single-flight，三者职责不同，不能混为一个锁。

### 2.5 发起 P2P 控制请求并继续 PD 流程

Gateway 在最终准入后才生成 bootstrap metadata 和 P2P payload，避免 target 重新规划后沿用旧 target 的 room。payload 发送到 target 的：

```text
POST {target_url}/experimental/p2p_kv_transfer
```

请求里包含 source、target、matched token IDs、matched token 数量、source bootstrap 地址、reason 和本次独立的 `attempt_id`。

普通 PD payload 会去掉 `remote_kv_*` 字段，P2P headers 也不会继续透传到普通 Prefill/Decode 请求。这样 Decode 不会误处理 P2P 控制字段，重试也不会重复触发传输。

这里有一个必须知道的实现细节：

> 当前 `execute_dual_dispatch()` 会先 `await execute_independent_p2p_transfer()`，然后才调用 `execute_dual_dispatch_internal()` 发送正常 PD 请求。也就是说，worker scheduler 内部的传输推进是非阻塞的，但当前 Router 的一个逻辑请求仍会等待 P2P 控制请求拿到终态后再做正常 PD dispatch。

“独立”主要表示控制请求被 shield/spawn，不能因为下游客户端断开就提前释放 source/target lease；它不等于当前代码已经把 P2P 数据传输和正常 PD HTTP dispatch 完全并行化。相关代码见 [`pd_router.rs`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/routers/http/pd_router.rs:865>)。

如果外层 HTTP retry 发生在 P2P 已经实际 dispatch 之后，`p2p_dispatched` 会阻止重试再次发送 P2P，后续 retry 只走本地重算路径。

## 3. Gateway 侧改动详解

### 3.1 KV Events：从 worker 缓存事件构建路由视图

#### `discovery.rs`

[`fetch_event_config()`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/policies/kv_events/discovery.rs:84>) 读取 worker 的 `/server_info`，识别新版 top-level `kv_events` 和 legacy `kv_events_config`。

返回的 `EventConfig` 包含：

| 字段 | 用途 |
| --- | --- |
| `host` | ZMQ 连接地址；worker 报 wildcard bind 时替换为 worker URL 的 host |
| `port_base` | DP rank 0 的端口，实际连接端口是 `port_base + dp_rank` |
| `topic` | ZMQ topic 前缀 |
| `block_size` | worker 的 page size，供 `BlockSizeOracle` 校验 |
| `dp_size` | 要建立多少个 DP rank subscriber |
| `is_bigram` | EAGLE/EAGLE3/FROZEN_KV_MTP 时使用 bigram hash |

网络错误和 5xx 会有限重试；worker 可达但没有 publisher 时返回 `Ok(None)`，这和“暂时无法发现”是两种不同状态。

#### `subscriber.rs` 与 `index.rs`

subscriber 对每个 `(worker_url, dp_rank)` 建立一个 SUB 任务，负责重连、退避和把消息放入有界 channel。`index.rs` 的 pump 是树的唯一写入口，负责：

1. 丢弃已经从 live set 移除的 worker 事件；
2. 去重同一个 sequence；
3. 处理 publisher reset、sequence regression 和 sequence gap；
4. 将 `BlockStored`、`BlockRemoved`、`AllBlocksCleared` 应用到 `HashTree`。

这样做的原因是 PUB/SUB 可能丢消息。如果发现 gap 还继续保留旧 owner，Gateway 可能产生 false-positive cache hit，把请求送到一个实际已经没有缓存的 source。代码选择清空该 worker 状态，让后续事件重新建立可见状态。

#### `hash.rs`、`tree.rs` 与 `block_size_oracle.rs`

Gateway 的 hash 必须和 Python worker 完全一致：

1. 每个 block 对 token ID 做 little-endian `u32` 编码；
2. 后一个 block 继续喂前一个 block 的完整 32-byte SHA-256 digest；
3. 对最终 digest 取前 64 bit，按 signed `i64` 发布/存储。

EAGLE 模式不是普通 token block，而是重叠 bigram：N 个原始 token 形成 N-1 个逻辑位置，因此路由时需要保留一个 raw boundary token。实现分别在：

- [`hash.rs`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/policies/kv_events/hash.rs:45>)；
- [`tree.rs`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/policies/kv_events/tree.rs:65>)；
- [`block_size_oracle.rs`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/policies/kv_events/block_size_oracle.rs:44>)。

`HashTree` 通过 `match_prefix_by_worker()` 返回每个 worker 的最长命中深度，P2P selector 再结合 worker registry 中的 URL 和负载选择 source。树还记录 `GPU`、`CPU_PINNED`、`DISK`、`EXTERNAL` 等 storage medium；P2P source 选择本身只使用 owner 深度，具体能否搬运由 worker 侧 residency probe 决定。

### 3.2 `PDRouter` 的 P2P 选点与普通 PD 选点

PDRouter 初始化时只有同时满足以下条件才创建 P2P selector：

- `enable_prefill_p2p_kv_transfer` 开启，AppContext 创建了 `KvEventIndex`；
- Prefill policy 是 `CacheAware`；
- 能够加载 untruncated tokenizer，或至少能使用注册 tokenizer fallback。

初始化代码见 [`pd_router.rs`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/routers/http/pd_router.rs:287>)。

`select_pd_pair_with_decision()` 的结果不只是 `(prefill, decode)`，还带：

```text
SelectedPdPair {
    prefill,
    decode,
    remote_kv: Option<RemoteKvDecision>,
    prepared_p2p_request: Option<P2pPreparedRequest>,
}
```

如果 P2P selector 没有足够的 token、block size、cache hit 或 bootstrap 信息，`remote_kv` 为 `None`，Prefill 直接调用原有 policy。这样 P2P 是增量能力，不会改变普通路由的默认行为。

### 3.3 `P2pNodeGate` 与 Router lease

`P2pNodeGate` 是进程级共享 coordinator，通过 `OnceLock` 保证同一 Router 进程里的多个 PDRouter 实例共享锁表。一次准入创建两个 node ownership，`P2pTransferLease` 再把 node lease、source/target、matched token 和 attempt id 绑定起来。

Router lease 必须一直持有到 worker 返回终态：

- 成功：显式 release；
- safe fallback：显式 release；
- transport uncertain：仍由 shielded task 持有到控制请求终止；
- JoinError 或异常 Drop：防御性 release，并打印 lease 未显式 settlement 的错误日志。

### 3.4 精确 token 与请求元数据隔离

为了让 source 和 target 使用同一 cache key，Gateway 做了三件事：

- Chat 支持上游 `input_ids`，避免不同侧重复套 chat template 后产生 token 差异；
- P2P 使用 untruncated tokenizer，避免长上下文在 tokenizer 层被静默截断；
- 普通 PD 请求会清理 P2P 专用 headers/body 字段，避免 metadata 泄漏到 Decode。

另外，当前 P2P decision payload 不能完整复现所有 cache namespace，因此当请求带非空 `extra_key`、`cache_salt`、`lora_id` 或 `lora_path` 时，Router 会跳过 P2P。这是安全的 cache identity 约束，不是普通请求校验失败。

## 4. Worker 侧请求入口与控制面

### 4.1 请求/响应结构

核心 wire contract 定义在 [`io_struct.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/managers/io_struct.py:1400>)。

#### `P2PKVTransferReqInput`

| 字段 | 作用 |
| --- | --- |
| `source_url` / `target_url` | P2P 两端 HTTP identity |
| `token_ids` | 用于重建 cache key 的原始 token 序列 |
| `matched_tokens` | Router 观察到的匹配长度，worker 还会 page-align |
| `request_id` | 本次 transfer attempt 的幂等/状态查询 key |
| `dry_run` / `reason` | residency、attempt status/cancel、quarantine status 等控制操作 |
| `p2p_bootstrap_room` | Mooncake sender/receiver 的 bootstrap room |
| `source_bootstrap_addr` | target 连接 source 数据面使用的地址 |
| `p2p_source_send` | `false` 表示 target pull/control，`true` 表示 source sender callback |
| `extra_key` | cache key namespace；当前 Router 对非空 namespace 默认跳过 P2P |
| `transfer_mode` | source probe 后协商出的 `device` 或 `hicache_direct` |
| `host_layout_signature(s)` | HiCache direct path 的布局兼容性证明 |

结构里仍保留 `dst_*` 等旧 pointer/session 字段，是为了 rollout 期间的 schema 兼容；当前 engine 会主动拒绝 legacy pointer payload，不会使用外部传入的裸地址作为数据面入口。

#### `P2PKVTransferReqOutput`

除了 `success`、`transferred_tokens` 和 `fallback_recompute`，需要重点看四个字段：

| 字段 | 语义 |
| --- | --- |
| `data_path_started` | source 是否已经进入可能产生异步写入的数据路径 |
| `data_path_quiesced` | source 是否已经证明 sender/backend 不再写入 |
| `attempt_phase` | `active` / `quiesced`，用于 delayed request 和恢复探测 |
| `worker_boot_id` | 标识 worker generation，辅助判断旧 attempt 是否来自旧进程 |

`data_path_*` 为 `None` 时，target 必须保守处理，不能把它当作“安全”。这个字段设计是为了兼容旧 worker，但新 worker 会尽量返回明确的 settlement。

### 4.2 TokenizerManager 的单飞与 pair gate

入口在 [`tokenizer_communicator_mixin.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/managers/tokenizer_communicator_mixin.py:431>)。

控制层有三条规则：

1. 每个 `p2p_kv_transfer_communicator` 最多允许一个实际请求在飞；
2. status、residency probe 等只读控制不能排队在一个卡住的实际传输后面，失败后由 Router 下次重试；
3. 真实 P2P 请求不能互相排队，否则容易形成 `A->B、B->C、C->A` 的控制环，重叠请求直接在 data path 启动前返回 busy fallback。

target 还会在 worker 侧申请 canonical pair gate。这个 gate 的作用是防止同一对 source/target 出现重复的 target transfer；它和 Gateway 的双节点 gate 不是同一把锁：

```text
Gateway P2pNodeGate       ：一个节点同一时刻只能参加一个 P2P
Tokenizer pair gate       ：同一 source-target pair 不能重复占用
Communicator single-flight：同一个 worker 控制 channel 只能有一个实际 call
```

控制请求通过 `_p2p_kv_transfer_communicator_call()` 做 shield：如果客户端连接断开，外层 coroutine 记录取消，但内部 transfer task 继续运行，直到产生终态后再传播 cancellation。这样不会出现“HTTP 已取消、lease 已释放、source 还在写显存”的悬空状态。

### 4.3 attempt status/cancel

每个 Router P2P 尝试生成独立 `attempt_id`，不要直接把普通请求 `rid` 当作 transfer identity。原因是同一个用户请求可能经历 retry/re-route，而 transfer 的 source/target、room 和状态必须是一组不可混淆的控制对象。

engine 保存：

- active attempt record；
- quiesced tombstone；
- worker boot id；
- source/target 角色；
- 终态和 data-path settlement。

cancel 对未知 attempt 会先写入 quiesced tombstone，阻止迟到的 real request 重新进入 data path。status/cancel/quarantine control 都会在普通 transfer admission 之前处理。

## 5. Worker 核心：异步 P2P 状态机

主实现是 [`p2p_kv_transfer.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/disaggregation/p2p_kv_transfer.py:245>)。

### 5.1 生命周期和关键对象

`PendingP2PTransfer` 是一次 transfer 的完整控制面上下文，保存 request、角色、状态、deadline、sender/receiver、target allocation、source/target future、consensus Work、layout signature、quarantine 标记和 cache lease。

关键枚举：

```text
P2PTransferMode
    DEVICE          ：GPU KV / optional model state
    HICACHE_DIRECT  ：直接使用 HiCache host buffers

P2PTransferState
    PROBING_SOURCE
    VALIDATING_SOURCE_LAYOUT
    PREPARING_TARGET
    RESERVED
    LOADING_SOURCE
    PREPARING_SOURCE
    SOURCE_SENDER_READY
    PREPARING_SEND
    STARTING_SEND
    WAIT_SOURCE
    QUIESCING
    CHECKING_TARGET
    TRANSFERRING
    CONSENSUS
    COMMIT
    FAILED
```

target 的临时显存由 `_TargetAllocation` 持有，另有明确的 ownership phase：

```text
P2P_OWNED
    ├─ 成功提交前：只有 P2P cleanup 可以释放
    ├─ transfer_to_insert()
    ▼
INSERT_OWNERSHIP_TRANSFERRED
    ├─ Cache insert 已接管，P2P cleanup 不再 free 这些页
    ├─ commit()
    ▼
INSERT_COMMITTED
```

这比单纯用一个 `success` bool 更重要：失败清理和成功提交不能同时拥有同一块内存的 free 权。

### 5.2 Scheduler 如何做到局部异步

初始化只在 Prefill + Mooncake 条件下创建 engine：

[`scheduler.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/managers/scheduler.py:1076>)。

请求处理时：

1. `handle_p2p_kv_transfer()` 调用 `engine.start_transfer()`；
2. 如果可以立即完成，直接返回 `P2PKVTransferReqOutput`；
3. 如果需要等待 HTTP、Mooncake 或 collective，创建 `PendingP2PTransfer`，注册后返回 `None`；
4. scheduler 每轮在选择普通 batch 之前调用 `progress_p2p_kv_transfers()`；
5. 完成的 output 再通过 `send_to_tokenizer` 返回给控制请求；
6. 普通 Prefill batch 不需要等待当前 transfer 完成。

普通 loop、overlap loop 和 disagg Prefill loop 都接入了 progress：

- [`scheduler.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/managers/scheduler.py:1302>)；
- [`prefill.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/disaggregation/prefill.py:464>)。

此外，`has_idle()` 会把 pending/quarantined P2P 视为 active work，避免 target 已经预留 KV slot 时被 idle maintenance 当成空闲资源进行破坏性操作。

当前 engine 强制每个 worker 最多一个 pending transfer。如果内部发现 `_pending_transfers` 数量超过 1，会抛出 `_P2PCacheIntegrityError`，因为这意味着单飞约束已经被破坏。

### 5.3 三个 collective group

多 rank 路径中，不能把所有 collective 混在同一个 Gloo group：

| group | 用途 | 为什么独立 |
| --- | --- | --- |
| normal CPU group | admission、status、cancel 等同步控制 | 外部控制请求可能随时到达 |
| progress group | 状态机中的异步 `all_reduce(MIN)` | 允许 scheduler 每轮轮询，不阻塞 batch |
| terminal group | terminal output、commit、cleanup fence | progress group 可能已经 poisoned，不能继续复用 |

`_progress_world_min()` 在 progress group 上提交 async Work。`defer_while` 允许还没有到达某个状态的 rank 暂不提交，避免把 rank-local waiting 值误当作全局结果。

一个 rank 的 `Work.is_completed()` 先变成 true，不代表所有 rank 都已经可以做 cache mutation。因此 engine 先把结果放到 `pending.terminal_output`，再通过 `_terminal_output_consensus()` 做固定宽度的 terminal reduction，校验：

- request identity；
- source/target role；
- terminal phase；
- success/fallback；
- transferred token count；
- data path started/quiesced；
- 是否仍有 progress Work 未消费。

只有 terminal fence 通过后，target 才能 commit cache；source/target 才能 cleanup；最后还要再做一次 post-cleanup fence，保证快 rank 不会提前进入普通 model collective。

### 5.4 Target 侧状态机

#### `PROBING_SOURCE`

trigger rank 通过控制线程向 source 发送 residency probe。probe 是无副作用的，只检查 source 是否有足够的 page-aligned prefix：

- 完整 GPU hit：选择 `DEVICE`；
- 完整 host-only hit、无 Mamba、布局可兼容：选择 `HICACHE_DIRECT`；
- GPU/host 混合前缀：当前 direct data plane 拒绝；
- 前缀不足或 rank 间 residency 不一致：fallback。

mode、prefix length、device/host hit length 都必须在 model-parallel ranks 之间一致。direct 模式还会收集 source 每个 rank 的 host layout signature。

实现见 [`_source_residency_probe()`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/disaggregation/p2p_kv_transfer.py:461>)。

#### `VALIDATING_SOURCE_LAYOUT`

target 根据本地 HiCache host pool 构造 probe，计算本地 compatibility signature，然后和 source 对应 rank 的 signature 比较。signature 不包含裸指针地址，包含 page size、pool 类型、layout、dtype、item length、全局 layer range 和 shard 信息。

这一步防止 source/target 虽然 token 数一致，但 host buffer 的层布局、分片方式或索引器 sidecar 不一致，最终把数据写进错误位置。

#### `PREPARING_TARGET`

根据 mode 准备目标：

- `DEVICE`：驱逐足够的 target cache，分配 GPU KV slots；如果状态类型需要，额外分配 Mamba slot；创建 Mooncake receiver；
- `HICACHE_DIRECT`：向 HiCache 申请 unpublished host pages，注册外部 host buffer，创建带 `registration_kv_args` 的 receiver。

准备阶段有 rank-wide ready consensus；任一 rank 失败，所有 rank 都回退。

#### `RESERVED`

receiver bootstrap 成功后，target 把目标 page indices 和 optional state indices 发给 source。此时目标资源已经被预留，但还没有发布到 radix cache。

#### `WAIT_SOURCE`

target 一边轮询 receiver，一边由 trigger rank 发起 source transfer control request。两边的结果并行观察：

- source safe failure 且 `data_path_quiesced=true`：释放目标 reservation，fallback；
- source transfer 尚未证明 quiesced：abort receiver，但保留 allocation、receiver 和 source future，进入 background quarantine；
- source success 且已 quiesced：进入 target receiver 检查。

这里刻意不把慢 source HTTP 当成 target scheduler 的同步等待。target 可以先看到 receiver failure；但 resource cleanup 仍要等 source 数据路径 settlement 证明。

#### `CHECKING_TARGET -> TRANSFERRING -> CONSENSUS`

source 已 quiesced 后，target 再做一次 receiver rank consensus，然后进入 transfer timeout window。receiver 成功后，trigger rank 的 source response 携带 transferred token count，其他 rank 提供 broadcast sentinel，通过 `MIN` 让所有 rank 获得同一个严格长度。

传输长度必须等于 target 预留的 page-aligned prefix length，否则即使底层 receiver 报 success，也按失败处理。

注意此时仍然不能直接调用 `tree_cache.insert()`。结果要先进入 terminal fence。

### 5.5 Source 侧状态机

source 侧从 `LOADING_SOURCE` 开始重新匹配本地 cache，而不是盲信 Router 的命中长度：

1. 重建 cache key；
2. 重新 `match_prefix()`；
3. 对实际 device/host residency 做 page alignment；
4. 校验 probe 选择的 mode 仍然成立；
5. 在 device 模式锁住 source cache node，防止传输期间被 eviction；
6. 在 direct host 模式通过 export lease 固定 host pages；
7. 创建 Mooncake sender，并等待 target metadata；
8. 构造 source page/state indices；
9. 调用 `sender.send()`；
10. 轮询直到 sender success，再返回 source quiesced result。

source 在 `PREPARING_SEND` 之前不会标记 data path started。进入 `sender.send()` 前则立即把 `data_path_started=True`、`data_path_quiesced=False`，因为从这一刻开始即使 Python 控制调用抛异常，也不能假设没有异步写入。

如果 send 或 poll 失败：

- 先调用 sender abort/clear；
- 通过 `QUIESCING` 状态等待 rank-wide backend idle proof；
- 证明 quiesced 后释放 source lock/export lease；
- 超过 control deadline 仍无法证明时，返回 `fallback_recompute=True`、`data_path_quiesced=False`，保留 source 资源进入 quarantine。

source 的关键实现入口见 [`_start_source_transfer()`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/disaggregation/p2p_kv_transfer.py:3677>) 和 [`_progress_source_transfer()`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/disaggregation/p2p_kv_transfer.py:3898>)。

## 6. 两种数据路径

### 6.1 `DEVICE`：GPU KV / optional state

这是 source 完整前缀仍在 GPU cache 中时的路径：

```text
target GPU allocator.alloc(prefix_len)
        │
        ▼
target Mooncake receiver.init()
        │  receiver.send_metadata(target pages/state)
        ▼
source cache match + lock
        │
        ▼
source Mooncake sender.init()
        │  sender.send(source pages/state)
        ▼
Mooncake batch transfer
        │
        ▼
terminal fence
        │
        ▼
target tree_cache.insert() + exact match verify
```

Mooncake 传输使用已有的 page index、layer pointer 和 batch transfer 能力。engine 没有引入一个新的 global cache store 或新的中间缓存层。

`_TargetAllocation` 保证 target 在 commit 前拥有临时页；commit 开始后 ownership 交给 cache。若 insert 发现 prefix 的一部分已经由 cache 处理，代码只释放仍由 P2P allocation 持有的 duplicate 页，避免 double free。

### 6.2 `HICACHE_DIRECT`：直接使用 HiCache host buffer

这是 source 前缀已经完整落在 HiCache host pool、GPU 没有对应前缀时的路径。流程不是“先拷到一个 global store”，而是：

```text
source HiCache host pages
        │  export lease：pin host pages
        │
        ├──────── Mooncake external registration ────────┐
        │                                                  │
        ▼                                                  ▼
source sender(source_kv_args)                 target receiver(registration_kv_args)
                                                           │
                                                           ▼
                                               target unpublished host pages
                                                           │
                                                           ▼
                                     terminal fence 后 commit 到 UnifiedRadixCache
```

#### Host layout 兼容性

HiCache direct 不是只比较总字节数。`UnifiedRadixCache._build_p2p_host_kv_args()` 会严格验证：

- tree component 必须是 `FULL`；
- 必须有且只有一个从 KV 派生的 `INDEXER` sidecar；
- KV 必须是 anchor pool；
- KV/INDEXER page size 与 tree page size 相同；
- pool layout 必须是 `layer_first`；
- 每个 layer 的 data pointer、data length、item length 合法；
- source/target 的全局 layer range 和 shard 信息一致；
- KV 与 INDEXER 必须拥有相同的全局 layer range。

实现和校验在 [`unified_radix_cache.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/mem_cache/unified_radix_cache.py:724>)。

`P2PHostExportLease` 会增加 host lock ref，保证 source 的 host pages 在传输终止前不会被 eviction；`P2PHostImportLease` 先分配未发布的 target host pages，只有 commit 成功后才把这些 pages 接到 radix tree。

#### Mooncake 外部 buffer 注册

[`register_external_kv_args()`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/disaggregation/mooncake/conn.py:485>) 为 cache-owned host buffer 做一次性注册：

- registration key 包含 layout signature 和 pointer set，重复注册同一 buffer 是幂等的；
- host buffer 仍关联到拥有它的 GPU location，例如 `cuda:{gpu_id}`，让 Mooncake 选择正确的 data NIC；
- batch register 失败时尝试 deregister 已经注册的 pointer，避免留下半注册状态；
- `send_kvcache()` 增加 `source_kv_args`，direct path 可以传 source host pointer，而不是默认使用 device KV args；
- layer-sparse state pool 中的 zero pointer/zero length 会被过滤，避免无效地址让整个 batch transfer 失败。

对应代码见 [`conn.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/disaggregation/mooncake/conn.py:883>)。

#### Host pool 的 page-granular allocator

HiSparse host pool 原本可能按 token 管理 free list；发生复用后，分配结果不一定 page-aligned、page 内也不一定 contiguous。但 direct P2P 用 `host_indices[::page_size] // page_size` 生成页号，这个前提一旦被破坏，传输数据会写入错误页。

因此 [`HiSparseHostPoolMixin`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/mem_cache/memory_pool_host.py:103>) 对 HiSparse host pool 单独使用 page-granular allocator，并维护 page bitmap；其他 host pool 不改变原有 token-granular allocator。

同时：

- `MLATokenToKVPoolHost.get_contiguous_buf_infos()` 使用 host tensor ref 的 `data_ptr()`，避免 control-plane probe 读取 accelerator tensor `.item()` 引入同步；
- `NSAIndexerPoolHost.get_contiguous_buf_infos()` 暴露 layer-first indexer host buffer、每页 stride 和容量信息。

### 6.3 Cache commit 的 duplicate/backfill 处理

direct host import 不是简单地把一段 host index 填到树里。target 可能在传输期间又出现了部分 cache path，因此 commit 前先生成 `_P2PHostCommitPlan`，把每个 segment 分类为：

- `DUPLICATE`：目标已经有 host-backed path，收到的临时 host page 可以释放；
- `BACKFILL`：目标只有 device-backed path，需要把收到的 host pointer 接到对应节点；
- suffix：目标树上还没有该路径，需要创建新 suffix node。

plan 的所有可能失败校验完成后，lease 进入 `COMMITTING`，再开始同步修改树。发布 CPU storage events 要等所有 node pointer 安装完成之后进行。`COMMITTING` 状态不能再 rollback，防止 cleanup 把树已经引用的 page 释放掉。

`commit_p2p_host_import()` 和 rollback/release 见 [`unified_radix_cache.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/mem_cache/unified_radix_cache.py:1071>)。

## 7. 成功、失败和 quarantine 语义

### 7.1 失败分类表

| 场景 | 是否已经启动 data path | 处理 | 是否影响普通推理 |
| --- | --- | --- | --- |
| token/page 对齐失败 | 否 | 立即 fallback，释放临时资源 | 否 |
| target 分配失败 | 否 | fallback | 否 |
| source 无完整 GPU/host prefix | 否 | fallback | 否 |
| source/target layout 不兼容 | 否 | fallback | 否 |
| communicator busy | 否 | preflight reject + fallback | 否 |
| Mooncake bootstrap 失败 | 通常否 | abort/clear 后 fallback | 否 |
| sender.send 后异常 | 是 | abort、quiescing；无法证明时 quarantine | 否，优先返回重算 |
| receiver timeout 且 source settlement uncertain | 可能是 | target allocation/receiver 保留在后台 | 否，返回重算 |
| progress Gloo group failure | 不确定 | P2P 暂停、资源 quarantine、切换同步 progress | 否，普通 inference 继续 |
| terminal identity/commit/cleanup fence failure | 不确定 | `_P2PCacheIntegrityError`，scheduler fail-stop | 不继续冒险运行 |

### 7.2 Quarantine 的意义

quarantine 不是永久 blacklist，而是临时的安全保留状态：

- 不释放可能仍被 backend 使用的 source cache lock 或 target allocation；
- P2P admission 暂时关闭，避免在资源状态不清楚时再启动新 transfer；
- Router 可以收到 fallback，继续本地重算；
- 后台通过 status/probe 轮询，等待 backend 给出 rank-wide idle/quiesced proof；
- 资源真正清理后再解除 `_unsafe_quarantine`。

progress collective 失败时，原 progress group 可能持有 rank-locally incomplete Work，代码不会强行 destroy/reuse。它会保留这个 group，后续用健康的 terminal group 做同步 progress fallback。这样最多退休一个有问题的 progress group，同时保持普通推理继续。

### 7.3 为什么 cache integrity error 要 fail-stop

普通 transport 失败可以重算，因为没有新的 cache ownership 被发布。但如果出现下面任一情况，继续运行可能留下“树已指向被释放内存”或“不同 rank 看到了不同 cache key”的状态：

- target commit 在某个 rank 成功、另一个 rank 失败；
- cleanup fence 无法证明所有 rank 已完成；
- terminal request identity、role 或 phase 不一致；
- commit 后 exact prefix verify 长度不一致。

因此 scheduler 对 `_P2PCacheIntegrityError` 单独 re-raise，而不是像普通异常一样吞掉并继续 batch loop。实现见 [`scheduler.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/managers/scheduler.py:3590>)。

## 8. Cache key、page 对齐和状态数据约束

### 8.1 普通 token 与 EAGLE/bigram

engine 的 cache key helper 是理解长度问题的关键：

- `_cache_key_from_raw_prefix()` 把原始 token 加上 `extra_key` 后构造 `RadixKey`；
- EAGLE 模式将 raw token 序列转成 bigram view；
- `_raw_prefix_len_for_cache_len(cache_len)` 在 EAGLE 下返回 `cache_len + 1`；
- `_transferable_len()` 对 page size、必要时 Mamba chunk size 做 floor 对齐；
- `_cache_key_for_cache_len()` 对 raw token 数、normalized key length 做一致性检查。

这些 helper 在 [`p2p_kv_transfer.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/disaggregation/p2p_kv_transfer.py:4423>)。任何新增传输路径都应该复用它们，不要重新用 `matched_tokens` 手算 cache length。

### 8.2 多状态模型

`_state_indices_for_pages()` 根据 state type 生成额外 state indices：

- `MAMBA`：使用一个 scalar Mamba index；
- `DSA` / `MINIMAX_INDEX_K`：使用与 KV page 对应的 page indices；
- 其他未支持状态：前置检查失败，回退重算。

当前 engine 的 state type admission 允许 `MAMBA`、`DSA`、`MINIMAX_INDEX_K`；但是 `HICACHE_DIRECT` 还有更严格的限制：不支持 Mamba state，也不支持 speculative draft KV。设备路径和 host direct 路径的支持范围不要混写。

### 8.3 speculative draft KV

`P2PKVTransferReqInput` 的旧 pointer 字段不能代表当前 direct host layout 的完整 ownership。Prefill 可能在主模型 KV 后追加 draft-model KV pointer，如果把这个 inherited split boundary 直接传给 Mooncake，Mooncake 会把不存在的 tail 当成 draft KV。

因此 direct host path 明确检查 `target_kv_data_ptr_count == 0`，否则返回：

```text
direct HiCache P2P does not yet transfer speculative draft KV
```

这是一条功能限制，不应通过放宽校验“先跑起来”。

## 9. 调度接线与 Decode 影响面

### 9.1 Scheduler 接线点

主要接线有四处：

1. Prefill + Mooncake 初始化 `PrefillP2PMooncakeTransferEngine`；
2. RPC handler 注册 `P2PKVTransferReqInput -> handle_p2p_kv_transfer`；
3. normal/overlap/disagg Prefill loop 每轮调用 `progress_p2p_kv_transfers()`；
4. `has_idle()` 把 pending P2P 当作 active work。

`handle_p2p_kv_transfer()` 先处理 tokenless control reason，再处理普通 transfer 参数校验。这样 quarantine/status/cancel 不会被 `matched_tokens <= 0` 之类的普通请求校验挡住。

### 9.2 Decode 为什么基本不改

P2P 的数据面和 cache ownership 都在 Prefill。Decode 只需要继续收到正常 PD 请求，以及原有的 bootstrap/Prefill DP hint；它不创建 P2P engine、不分配 P2P target cache、不参与 source/target transfer collective。

请求结构和 scheduler `Req` 增加了远程 KV metadata，是为了让控制字段能从 Gateway 传到 Prefill，而不是让 Decode 执行 P2P。普通 PD payload 在 Router 侧会去掉这些字段，进一步降低 Decode 误处理的可能。

## 10. 配置、启动和构建

### 10.1 Gateway 开关

Gateway 新增配置 `enable_prefill_p2p_kv_transfer`，默认是 `false`。CLI 定义在 [`main.rs`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/main.rs:196>)，默认参数为：

| 参数 | 默认值 | 作用 |
| --- | ---: | --- |
| `--enable-prefill-p2p-kv-transfer` | `false` | 创建 KV event index，并允许 P2P selector |
| `--prefill-policy` | 未指定时继承整体 policy | 必须是 `cache_aware` 才会创建 P2P selector |
| `--cache-threshold` | `0.3` | 最小 cache match rate |
| `--balance-abs-threshold` | `64` | source-target 的绝对负载差阈值 |
| `--balance-rel-threshold` | `1.5` | source 相对 target 的负载比阈值 |

典型 Gateway 配置片段：

```text
--enable-prefill-p2p-kv-transfer \
--prefill-policy cache_aware \
--cache-threshold 0.3 \
--balance-abs-threshold 64 \
--balance-rel-threshold 1.5
```

配置校验要求 `cache_threshold` 在 `[0, 1]`，`balance_rel_threshold >= 1.0`。相关校验在 [`validation.rs`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/config/validation.rs:148>)。

### 10.2 Worker KV Events

Prefill worker 必须配置 KV event publisher，例如：

```json
{
  "publisher": "zmq",
  "endpoint": "tcp://*:5557",
  "topic": "kv"
}
```

对应 worker 参数是 `--kv-events-config '<上述 JSON>'`。publisher 配置结构见 [`kv_events.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/disaggregation/kv_events.py:387>)。

当前改动把 KV event publisher 的启用 rank 收紧为 `pp_rank == 0 && attn_tp_rank == 0 && attn_cp_rank == 0`，避免 PP/TP/CP 维度重复发布同一份 cache event；DP rank 仍通过 publisher endpoint 的端口偏移区分。helper 在 [`scheduler_rank_utils.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/managers/scheduler_rank_utils.py:1>)。

### 10.3 Worker image

[`Dockerfile.p800-p2p`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/Dockerfile.p800-p2p:1>) 的策略不是重新安装整套 Python 依赖，而是：

- 以已经验证过的 P2P runtime image 为 base；
- 将本次修改过的 Python 模块覆盖到 site-packages；
- 保留现有 Mooncake、torch、KLX/xpytorch 等运行时依赖；
- 默认打开 `SGLANG_P2P_TRACE=1`，便于生产灰度阶段定位 rank/状态机问题。

构建必须从仓库根目录执行，因为 Dockerfile 的 `COPY python/...` 路径依赖 build context。不要为了“方便”升级或替换 KLX 的自定义 torch 栈。

Gateway 因为新增 untruncated tokenizer 加载，`sgl-model-gateway/Cargo.toml` 增加了 `tokenizers = "0.22.0"` 依赖。

## 11. 调试手册

### 11.1 推荐排查顺序

遇到“命中了但没搬过去”时，按下面顺序看，不要一开始就盯 Mooncake：

1. Gateway 是否订阅了 source worker 的 KV Events；
2. `BlockSizeOracle` 是否建立，是否出现 page size/bigram mismatch；
3. Gateway 是否拿到了精确 token IDs；
4. `P2pCacheAwareSelector` 是否找到 source、bootstrap 是否存在、load threshold 是否满足；
5. `P2pNodeGate` 是否因 source/target 被占用或 admission timeout 放弃；
6. target 是否在 residency probe 中选择了正确 mode；
7. source 重新 match 后是否仍有完整 page-aligned prefix；
8. target allocation/host import lease 是否建立；
9. sender/receiver 是否完成 bootstrap、metadata、send/poll；
10. terminal fence、cache commit 和 exact verify 是否通过。

### 11.2 关键日志

engine 内置 JSON trace 由 `SGLANG_P2P_TRACE` 控制，输出前缀是 `P2P_TRACE`，包括 request id、role、state、phase sequence、consensus label、engine rank、TP/CP/PP/DP rank 和 pending count。

建议搜索：

```text
P2P_TRACE
p2p_transfer_fallback
p2p_progress_collective_quarantined
p2p_source_enter_quiescing
p2p_target_allocation_quarantined_background
p2p_target_cache_integrity_error
p2p_control_busy_preflight_rejected
p2p_pair_gate_busy
Independent P2P transfer control request started
```

同一个 `request_id` 要把 Gateway、target scheduler、source scheduler 的日志串起来看。若是重试问题，还要看 `attempt_id` 是否只 dispatch 过一次。

### 11.3 常见现象与代码位置

| 现象 | 首先检查 |
| --- | --- |
| Gateway 永远选 min-load，不选 cache owner | `KvEventIndex` 是否订阅成功；`BlockSizeOracle`；hash/bigram；`cache_threshold` |
| source 有缓存但返回 `no page-aligned transferable prefix` | `matched_tokens`、page size、EAGLE boundary、Mamba chunk 对齐 |
| direct host 始终 fallback | FULL+NSA shape、layer_first、page size、host layout signature、draft KV/Mamba 限制 |
| transfer 失败后显存不释放 | `data_path_started/quiesced`、quarantine 状态、source/target cleanup 日志 |
| 同一节点上的多个 transfer 互相等待 | Gateway `P2pNodeGate`、worker pair gate、communicator busy 日志 |
| 多 rank 偶现卡死或 rank skew | progress/terminal group 日志、`P2P_TRACE` 的 consensus label 和 phase sequence |
| cache commit 后命中长度不对 | `target_commit`、`_register_target_prefix()` exact verify、EAGLE raw/cache length |

## 12. 测试地图

当前测试按“状态机、数据面、协议、Gateway”分层：

| 测试文件 | 覆盖内容 |
| --- | --- |
| [`test_p2p_async_scheduler.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/test/registered/unit/disaggregation/test_p2p_async_scheduler.py:1>) | scheduler loop、pending progress、非阻塞推进、single-flight |
| [`test_p2p_progress_protocol.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/test/registered/unit/disaggregation/test_p2p_progress_protocol.py:1>) | progress/terminal collective、rank order、quarantine/fail-stop |
| [`test_p2p_hicache_direct_state_machine.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/test/registered/unit/disaggregation/test_p2p_hicache_direct_state_machine.py:1>) | direct host mode 的 source/target 状态机和 layout negotiation |
| [`test_unified_p2p_host_transfer.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/test/registered/unit/mem_cache/test_unified_p2p_host_transfer.py:1>) | host export/import lease、duplicate/backfill、rollback/commit |
| [`test_mooncake_kv_args_override.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/test/registered/unit/disaggregation/test_mooncake_kv_args_override.py:1>) | external KV args、source pointer override、layer/state mapping |
| [`test_p2p_kv_transfer.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/test/registered/unit/disaggregation/test_p2p_kv_transfer.py:1>) | 基础 P2P request/response、mode、alignment、失败回退 |
| [`test_remote_kv_headers.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/test/registered/unit/entrypoints/openai/test_remote_kv_headers.py:1>) | header/body metadata 一致性、冲突时关闭 remote KV |
| [`test_p2p_kv_transfer_endpoint.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/test/registered/unit/entrypoints/test_p2p_kv_transfer_endpoint.py:1>) | endpoint 基本接线 |
| Gateway `p2p.rs` tests | source match、bigram、阈值和 URL alias |
| Gateway `p2p_node_gate.rs` tests | FIFO、公平、反向 pair、取消、超时、lease release、并发竞争 |
| Gateway `pd_router.rs` tests | tokenization、retry、P2P dispatch、namespace skip、metadata cleanup |

建议的局部测试命令：

```bash
cd /Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct

python -m pytest -q \
  test/registered/unit/disaggregation/test_p2p_async_scheduler.py \
  test/registered/unit/disaggregation/test_p2p_progress_protocol.py \
  test/registered/unit/disaggregation/test_p2p_hicache_direct_state_machine.py

python -m pytest -q \
  test/registered/unit/mem_cache/test_unified_p2p_host_transfer.py \
  test/registered/unit/disaggregation/test_mooncake_kv_args_override.py \
  test/registered/unit/entrypoints/openai/test_remote_kv_headers.py

cd sgl-model-gateway
cargo test p2p --lib
cargo test kv_events --lib
```

这些是按模块分组的建议入口；是否能在本机直接运行取决于 CUDA/Mooncake、Python 依赖和 Rust workspace 环境，不能把命令可执行等同于生产数据面验证。

## 13. 代码变更清单

相对 `e4f7977` 的 40 个文件可以按下面几组理解：

### Worker 控制面与调度

- `python/sglang/srt/disaggregation/p2p_kv_transfer.py`
- `python/sglang/srt/disaggregation/prefill.py`
- `python/sglang/srt/managers/scheduler.py`
- `python/sglang/srt/managers/scheduler_rank_utils.py`
- `python/sglang/srt/managers/tokenizer_communicator_mixin.py`
- `python/sglang/srt/managers/tokenizer_manager.py`
- `python/sglang/srt/managers/io_struct.py`
- `python/sglang/srt/managers/schedule_batch.py`

### Mooncake 与 cache 数据面

- `python/sglang/srt/disaggregation/mooncake/conn.py`
- `python/sglang/srt/mem_cache/unified_radix_cache.py`
- `python/sglang/srt/mem_cache/memory_pool_host.py`
- `python/sglang/srt/mem_cache/swa_radix_cache.py`

### OpenAI/PD 请求元数据

- `python/sglang/srt/entrypoints/openai/protocol.py`
- `python/sglang/srt/entrypoints/openai/remote_kv.py`
- `python/sglang/srt/entrypoints/openai/serving_chat.py`
- `python/sglang/srt/entrypoints/openai/serving_completions.py`

### Gateway KV Events、选点和 PD Router

- `sgl-model-gateway/src/policies/kv_events/index.rs`
- `sgl-model-gateway/src/policies/kv_events/tree.rs`
- `sgl-model-gateway/src/policies/mod.rs`
- `sgl-model-gateway/src/policies/p2p.rs`
- `sgl-model-gateway/src/routers/http/p2p_node_gate.rs`
- `sgl-model-gateway/src/routers/http/pd_router.rs`
- `sgl-model-gateway/src/routers/http/mod.rs`
- `sgl-model-gateway/src/routers/mod.rs`
- `sgl-model-gateway/src/routers/router_manager.rs`
- `sgl-model-gateway/src/server.rs`
- `sgl-model-gateway/Cargo.toml`

KV Events 的 `discovery.rs`、`subscriber.rs`、`wire.rs`、`hash.rs`、`block_size_oracle.rs` 等模块在基线中已经存在；本次重点扩展的是 index/tree 与 P2P selector 接线。阅读时要区分“本次改动新增/强化的能力”和“被复用的既有 publisher 协议”。

### 构建与测试

- `Dockerfile.p800-p2p`
- `test/registered/unit/disaggregation/test_p2p_async_scheduler.py`
- `test/registered/unit/disaggregation/test_p2p_hicache_direct_state_machine.py`
- `test/registered/unit/disaggregation/test_p2p_progress_protocol.py`
- `test/registered/unit/disaggregation/test_p2p_kv_transfer.py`
- `test/registered/unit/disaggregation/test_mooncake_kv_args_override.py`
- `test/registered/unit/mem_cache/test_unified_p2p_host_transfer.py`
- `test/registered/unit/entrypoints/openai/test_remote_kv_headers.py`
- `test/registered/unit/entrypoints/test_p2p_kv_transfer_endpoint.py`
- `test/registered/unit/managers/test_io_struct.py`
- `test/registered/unit/managers/test_scheduler_kv_event_publisher_rank.py`
- `test/registered/openai_server/basic/test_protocol.py`
- `test/registered/openai_server/basic/test_serving_chat.py`

## 14. 新同学推荐阅读顺序

如果目标是“能改代码”，建议按下面顺序阅读：

1. 先看 [`p2p.rs`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/policies/p2p.rs:45>)，明确 Gateway 到底何时产生 remote decision；
2. 再看 [`pd_router.rs`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/sgl-model-gateway/src/routers/http/pd_router.rs:865>)，理解 node gate、attempt id、retry 和正常 PD dispatch 的关系；
3. 看 [`io_struct.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/managers/io_struct.py:1400>) 和 [`remote_kv.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/entrypoints/openai/remote_kv.py:74>)，掌握 HTTP 到 worker 的字段契约；
4. 看 [`scheduler.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/managers/scheduler.py:3383>)，理解请求如何进入 engine、如何返回 completion；
5. 通读 [`p2p_kv_transfer.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/disaggregation/p2p_kv_transfer.py:97>) 的 enum、`start_transfer()`、`progress_transfers()`、source/target progress；
6. 再看 [`unified_radix_cache.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/mem_cache/unified_radix_cache.py:724>)，理解 host lease 和 cache ownership；
7. 最后看 [`conn.py`](</Users/k1he/Documents/推理优化/xsgl-p2p-kv-transfer-hicache-direct/python/sglang/srt/disaggregation/mooncake/conn.py:485>) 与相关 tests，补齐 pointer registration、page mapping 和 backend 行为。

最适合用来验证阅读理解的场景是：先构造一个 safe pre-send failure，再构造一个 sender.send 之后的 uncertain failure，观察二者为什么都返回重算，但清理路径完全不同。

## 15. 后续开发时必须守住的约束

新增或修改 P2P 代码时，至少检查以下事项：

- 是否仍然保持 source/target 同一 model、KV layout、TP/CP/PP layout 和 page size；
- 是否复用了 `_cache_key_for_cache_len()`，没有绕过 EAGLE/bigram 边界处理；
- 是否在真正 `sender.send()` 前后正确维护 `data_path_started/quiesced`；
- 是否所有 rank 都以同一顺序进入 progress/terminal collective；
- 是否把 cache mutation 放在 terminal fence 之后；
- 是否给每个新资源定义明确 owner，避免 P2P cleanup 和 Cache cleanup double free；
- 是否对 uncertain 状态采用 quarantine，而不是直接 free；
- 是否把新的控制 reason 放在 scheduler generic validation 之前；
- 是否让 Router retry、worker attempt status、worker boot generation 仍能区分一次 transfer attempt；
- 是否为新状态添加 unit test，尤其是 rank skew、取消、超时、重复 transfer 和 allocator rollback。

这套实现最容易被破坏的地方不是 Mooncake API 本身，而是“某个 rank 比其他 rank 多走了一步”以及“某个组件提前释放了仍由另一个组件使用的资源”。后续代码评审应优先检查这两类问题。
