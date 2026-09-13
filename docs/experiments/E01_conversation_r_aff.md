# E01 Conversation — R_AFF

> Status: completed  
> Protocol: TaskMain-v1.1  
> Run start: 2026-09-11T22:55:27+08:00  
> Git commit: `54f3c5e6d1105f96e57c2ccc2dc8b26f3835089f`  
> Config: `configs/taskmain_v1.1/conversation/E01_r_aff.yaml`  
> Result: `results/taskmain_evaluation/conversation/E01_r_aff`

## 配置与检查

Mooncake Conversation；R_AFF；4 Pods；每 Pod 585 pages；512 tokens/page；14 MiB/page；无 transfer。Replay 从 t=0 开始，仅统计 Evaluation `[1500000,2700000)` ms 的 4,275 个请求。Simulator exit code 0；capacity admission failures 0。该策略不读取 future、不使用 transfer/budget；随机 Gini 基线未运行，故为 N/A。

## 核心结果

| 指标 | 结果 |
|---|---:|
| Mean / P50 / P95 completion (ms) | 5366.478 / 4689.400 / 11850.030 |
| Mean queue / service (ms) | 4322.787 / 1043.692 |
| Request / token hit rate | 100.000% / 6.525% |
| Saved prefill tokens | 3,108,577 |
| Executed-miss-token Gini | 0.002926 |
| Reactive / proactive transfers | 0 / 0 |
| Total wire bytes | 0 |
| Evictions / fallbacks | 88,919 / 0 |

本行即 R_AFF 基线。
