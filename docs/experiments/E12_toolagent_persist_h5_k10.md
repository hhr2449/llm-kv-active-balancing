# E12 ToolAgent — Persist h=300s K=10

> Status: completed  
> Run start: 2026-09-11T23:36:25+08:00  
> Protocol / commit: TaskMain-v1.1 / `54f3c5e6d1105f96e57c2ccc2dc8b26f3835089f`  
> Config: `configs/taskmain_v1.1/toolagent/E12_persist_h5_k10.yaml`  
> Result: `results/taskmain_evaluation/toolagent/E12_persist_h5_k10`

## 配置与检查

R_AFF + T_PERSIST + COPY + TRIGGER_ONLY，history=300 s、K=10，冻结 budget。Evaluation 共 8,375 个请求。Exit code 0；capacity failures、future leakage、action exceeds bucket cap 均为 0。随机 Gini 为 N/A。

## 核心结果

| 指标 | 结果 |
|---|---:|
| Mean / P50 / P95 completion (ms) | 3530.374 / 2987.500 / 9364.420 |
| Mean queue / service (ms) | 3015.397 / 514.978 |
| Request / token hit rate | 99.881% / 37.571% |
| Saved prefill tokens | 25,855,038 |
| Executed-miss-token Gini | 0.001510 |
| Reactive / proactive transfers | 0 / 638 |
| Proactive / total wire bytes | 34,351,349,760 / 34,351,349,760 |
| Evictions / fallbacks | 89,796 / 0 |
| Unused replicas / wasted copies | 610 / 610 |

相对 R_AFF：mean completion −3.624%，token hit −0.049 pp，Gini −0.002368；相对 E09 mean completion −0.103%。
