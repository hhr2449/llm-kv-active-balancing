# E13 ToolAgent — Recency q=0.9

> Status: completed  
> Run start: 2026-09-11T23:38:33+08:00  
> Protocol / commit: TaskMain-v1.1 / `54f3c5e6d1105f96e57c2ccc2dc8b26f3835089f`  
> Config: `configs/taskmain_v1.1/toolagent/E13_recency_q09.yaml`  
> Result: `results/taskmain_evaluation/toolagent/E13_recency_q09`

## 配置与检查

R_AFF + T_RECENCY + COPY + TRIGGER_ONLY，quantile=0.9、decay=60 s，冻结 budget。Evaluation 共 8,375 个请求。Exit code 0；capacity failures、future leakage、action exceeds bucket cap 均为 0。随机 Gini 为 N/A。

## 核心结果

| 指标 | 结果 |
|---|---:|
| Mean / P50 / P95 completion (ms) | 3549.039 / 3016.100 / 9384.050 |
| Mean queue / service (ms) | 3033.976 / 515.063 |
| Request / token hit rate | 99.904% / 37.560% |
| Saved prefill tokens | 25,847,890 |
| Executed-miss-token Gini | 0.000947 |
| Reactive / proactive transfers | 0 / 1,075 |
| Proactive / total wire bytes | 34,336,669,696 / 34,336,669,696 |
| Evictions / fallbacks | 89,465 / 0 |
| Unused replicas / wasted copies | 905 / 1,059 |

相对 R_AFF：mean completion −3.114%，token hit −0.059 pp，Gini −0.002931；相对 E09 mean completion +0.426%。
