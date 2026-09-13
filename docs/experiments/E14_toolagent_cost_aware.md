# E14 ToolAgent — Cost-aware

> Status: completed  
> Run start: 2026-09-11T23:43:44+08:00  
> Protocol / commit: TaskMain-v1.1 / `54f3c5e6d1105f96e57c2ccc2dc8b26f3835089f`  
> Config: `configs/taskmain_v1.1/toolagent/E14_cost_aware.yaml`  
> Result: `results/taskmain_evaluation/toolagent/E14_cost_aware`

## 配置与检查

R_AFF + Persist(history=300 s,K=10) + COPY + COST_AWARE；冻结 V_ref/T_ref、score 与 budget。Evaluation 共 8,375 个请求。Exit code 0；capacity failures、future leakage、action exceeds bucket cap 均为 0。随机 Gini 为 N/A。

## 核心结果

| 指标 | 结果 |
|---|---:|
| Mean / P50 / P95 completion (ms) | 3528.098 / 2980.400 / 9325.920 |
| Mean queue / service (ms) | 3013.261 / 514.837 |
| Request / token hit rate | 99.904% / 37.588% |
| Saved prefill tokens | 25,866,834 |
| Executed-miss-token Gini | 0.002416 |
| Reactive / proactive transfers | 0 / 527 |
| Proactive / total wire bytes | 34,043,068,416 / 34,043,068,416 |
| Evictions / fallbacks | 90,004 / 0 |
| Unused replicas / wasted copies | 508 / 508 |

相对 R_AFF：mean completion −3.686%，token hit −0.032 pp，Gini −0.001462；相对 E09 mean completion −0.167%。
