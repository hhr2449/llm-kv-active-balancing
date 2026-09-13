# E07 Conversation — Cost-aware

> Status: completed  
> Protocol: TaskMain-v1.1  
> Run start: 2026-09-11T23:01:35+08:00  
> Git commit: `54f3c5e6d1105f96e57c2ccc2dc8b26f3835089f`  
> Config: `configs/taskmain_v1.1/conversation/E07_cost_aware.yaml`  
> Result: `results/taskmain_evaluation/conversation/E07_cost_aware`

## 配置与检查

R_AFF + T_PERSIST(history=300 s,K=10) + COPY + COST_AWARE；V_ref=2.5、T_ref=8.04643072 ms，使用冻结 score/budget。仅统计 Evaluation 的 4,275 个请求。Exit code 0；capacity failures 0；future leakage 0；action exceeds bucket cap 0。随机 Gini 为 N/A。

## 核心结果

| 指标 | 结果 |
|---|---:|
| Mean / P50 / P95 completion (ms) | 5315.385 / 4631.000 / 11856.630 |
| Mean queue / service (ms) | 4272.828 / 1042.557 |
| Request / token hit rate | 100.000% / 6.627% |
| Saved prefill tokens | 3,157,094 |
| Executed-miss-token Gini | 0.002064 |
| Reactive / proactive transfers | 0 / 434 |
| Proactive / total wire bytes | 34,336,669,696 / 34,336,669,696 |
| Evictions / fallbacks | 90,665 / 0 |
| Unused replicas / wasted copies | 421 / 421 |

相对 R_AFF：mean completion −0.952%，token hit +0.102 pp，Gini −0.000862；相对 E02 mean completion −0.817%。七行中本策略 mean/P50 completion 最低、token hit rate 最高；这是固定配置下的机械对照。
