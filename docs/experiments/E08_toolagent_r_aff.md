# E08 ToolAgent — R_AFF

> Status: completed  
> Run start: 2026-09-11T23:33:26+08:00  
> Protocol / commit: TaskMain-v1.1 / `54f3c5e6d1105f96e57c2ccc2dc8b26f3835089f`  
> Config: `configs/taskmain_v1.1/toolagent/E08_r_aff.yaml`  
> Result: `results/taskmain_evaluation/toolagent/E08_r_aff`

## 配置与检查

ToolAgent；R_AFF；4 Pods；每 Pod 585 pages；无 transfer。Replay 从 t=0 开始，仅统计 Evaluation `[1500000,2700000)` ms 的 8,375 个请求。Exit code 0，capacity failures 0；随机 Gini 基线未运行，故为 N/A。

## 核心结果

| 指标 | 结果 |
|---|---:|
| Mean / P50 / P95 completion (ms) | 3663.111 / 3138.500 / 9457.290 |
| Mean queue / service (ms) | 3148.536 / 514.574 |
| Request / token hit rate | 99.916% / 37.620% |
| Saved prefill tokens | 25,888,830 |
| Executed-miss-token Gini | 0.003878 |
| Reactive / proactive transfers | 0 / 0 |
| Total wire bytes | 0 |
| Evictions / fallbacks | 88,257 / 0 |

本行即 R_AFF 基线。
