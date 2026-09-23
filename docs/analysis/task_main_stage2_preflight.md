# Stage 2 实施与预检

范围：N=16，24 个主动策略容量 case +4 个 reactive 时段消融 case。每个新增正式 case 一次 replay，Stage 1 的 24 个 baseline 直接复用。**本轮只完成准备，不启动正式长实验，也不宣称 Stage 2 Results PASS。**

## 冻结与实现

- C_low=585、C_ref=2340；完整容量轴 585/1170/2340/infinite。Oracle W=300s、Persistence h=300s/K=10、Recency q=.9/decay=60s，仅 COPY、AFF 主动底座。
- 独立 Stage 2 配置、引擎、runner、汇总器和测试；Stage 0/1 与 canonical 源码/配置/报告保持原内容。
- 25 分钟按 external arrival time 切换 C/D，直到相同 trace endpoint=3537000ms；ready-before-arrival、ready batch pin、Stage 0 actual Saved/Load 修正不变。跨边界旧 transfer 正常完成，无 reset。
- 增量 holder/inflight/时间窗索引，精确 SHA256 cache 摘要分块，候选诊断无损 gzip 分段保存。单 closed-loop 顺序不变，workload trace/immutable metadata 在多 case worker 间共享。
- 单次 replay 导出后设置原子 COMPLETE；持有相同身份的 COMPLETE 永不重跑。每 600s 在完整时间戳批次后保存 case 内 checkpoint，包含所有闭环状态，支持 `--resume`。
- COPY 300s 定义保持 `(ready, ready+300s]`；完整观察未用与 right-censored、still-resident/lifecycle unresolved 独立。page/action/byte 分母和 new-page observed use 单独报告。
- 分阶段输出 total/reactive/proactive Wire；六策略差值相对同容量 REQ。Reactive 四组 contrast 保留正负，不归因于单独 COPY/cache placement。

## 验证交付

自动化证据位于 `results/task_main/stage2_active_capacity/preflight/`：

- `pytest.xml`：完整 TaskMain、Stage 0/1、共享 Trace/Cache 回归及 Stage 2 合成测试；要求 failed=0、skipped=0。
- `synthetic_benchmark.json`：三策略 ×有限/无限容量六组合成对照，逐项请求/transfer/opportunity/candidate/action/event、final state、final metrics 精确一致。
- `preflight.json`：绑定源码/配置/测试 SHA256 的 portable PASS receipt；两服务器共用该文件。

首次合成性能检查局部加速约 1.3–2.9 倍。正式 workload 更大，finite preflight、长 shortlist、诊断写盘仍有开销；该倍数不是正式耗时承诺，也不是内存上限。两服务器各 10 CPU/100GiB 容器配额，启动示例使用 8 workers，runner 默认 4。

## 最终预检结果

- **STAGE2_PREFLIGHT_PASS**；398 passed、0 failed、0 skipped，20.23s。
- 新增 32 项 Stage 2 测试；6 组合成对照的全部执行记录、状态与指标精确一致。
- 本次合成对照速度约 1.27–2.94 倍；未执行正式 workload 样本作为 benchmark。
- 238 个既有 source/config/test/doc 文件 SHA256 未变，105 个 canonical artifact SHA256 全部匹配。
- 两服务器 Python 均为 3.10.16，Ansible ping 均成功；既有 Stage 1 HEAD 均为 `7a539f058c2f0f8a6c343c4a5c5e6e90b195aade`。
- 已生成 28 份冻结参数 YAML 和 Stage 1 artifact/source hash 引用；新提交的最终 HEAD 由各服务器正式 manifest 记录。

## 正式启动边界

完成代码 commit/push、B 的 ff-only 同步和同 HEAD 检查后，只生成 28-case 配置快照与 manifest，不启动 tmux 长实验。当前不存在 28 个正式 Stage 2 结果；运行完成后 collect 才生成 48 行容量比较、8 个 A/B/C/D 比较以及正式 Results 报告。

全部 HEAD 检查、同步、双服务器 tmux、status、日志、resume 和汇总命令见 [Stage 2 运行说明](../../scripts/task_main_stage2/README.md)。
