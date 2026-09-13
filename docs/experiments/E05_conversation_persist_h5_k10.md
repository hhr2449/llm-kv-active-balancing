# E05 Conversation — Persist h=300s K=10

> Status: completed  
> Protocol: TaskMain-v1.1  
> Run start: 2026-09-11T22:57:36+08:00  
> Git commit: `54f3c5e6d1105f96e57c2ccc2dc8b26f3835089f`  
> Config: `configs/taskmain_v1.1/conversation/E05_persist_h5_k10.yaml`  
> Result: `results/taskmain_evaluation/conversation/E05_persist_h5_k10`

## 配置与检查

R_AFF + T_PERSIST + COPY + TRIGGER_ONLY，history=300 s、K=10，冻结 proactive budget。仅统计 Evaluation 的 4,275 个请求。Exit code 0；capacity failures 0；future leakage 0；action exceeds bucket cap 0。随机 Gini 为 N/A。

## 核心结果

| 指标 | 结果 |
|---|---:|
| Mean / P50 / P95 completion (ms) | 5372.790 / 4720.700 / 11846.660 |
| Mean queue / service (ms) | 4330.138 / 1042.653 |
| Request / token hit rate | 100.000% / 6.618% |
| Saved prefill tokens | 3,152,998 |
| Executed-miss-token Gini | 0.001433 |
| Reactive / proactive transfers | 0 / 609 |
| Proactive / total wire bytes | 34,292,629,504 / 34,292,629,504 |
| Evictions / fallbacks | 90,261 / 0 |
| Unused replicas / wasted copies | 578 / 578 |

相对 R_AFF：mean completion +0.118%，token hit +0.093 pp，Gini −0.001492；相对 E02 mean completion +0.254%。
