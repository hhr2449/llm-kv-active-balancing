# E11 ToolAgent — Persist h=60s K=10

> Status: completed  
> Run start: 2026-09-11T23:34:41+08:00  
> Protocol / commit: TaskMain-v1.1 / `54f3c5e6d1105f96e57c2ccc2dc8b26f3835089f`  
> Config: `configs/taskmain_v1.1/toolagent/E11_persist_h1_k10.yaml`  
> Result: `results/taskmain_evaluation/toolagent/E11_persist_h1_k10`

## 配置与检查

R_AFF + T_PERSIST + COPY + TRIGGER_ONLY，history=60 s、K=10，冻结 budget。Evaluation 共 8,375 个请求。Exit code 0；capacity failures、future leakage、action exceeds bucket cap 均为 0。随机 Gini 为 N/A。

## 核心结果

| 指标 | 结果 |
|---|---:|
| Mean / P50 / P95 completion (ms) | 3510.835 / 2960.900 / 9328.490 |
| Mean queue / service (ms) | 2996.340 / 514.495 |
| Request / token hit rate | 99.928% / 37.630% |
| Saved prefill tokens | 25,895,486 |
| Executed-miss-token Gini | 0.001677 |
| Reactive / proactive transfers | 0 / 519 |
| Proactive / total wire bytes | 34,615,590,912 / 34,615,590,912 |
| Evictions / fallbacks | 89,891 / 0 |
| Unused replicas / wasted copies | 472 / 492 |

相对 R_AFF：mean completion −4.157%，token hit +0.010 pp，Gini −0.002200；相对 E09 mean completion −0.655%。
