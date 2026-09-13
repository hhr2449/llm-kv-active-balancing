# E06 Conversation — Recency q=0.9

> Status: completed  
> Protocol: TaskMain-v1.1  
> Run start: 2026-09-11T22:58:59+08:00  
> Git commit: `54f3c5e6d1105f96e57c2ccc2dc8b26f3835089f`  
> Config: `configs/taskmain_v1.1/conversation/E06_recency_q09.yaml`  
> Result: `results/taskmain_evaluation/conversation/E06_recency_q09`

## 配置与检查

R_AFF + T_RECENCY + COPY + TRIGGER_ONLY，selection quantile=0.9、decay=60 s，冻结 proactive budget。仅统计 Evaluation 的 4,275 个请求。Exit code 0；capacity failures 0；future leakage 0；action exceeds bucket cap 0。随机 Gini 为 N/A。

## 核心结果

| 指标 | 结果 |
|---|---:|
| Mean / P50 / P95 completion (ms) | 5409.830 / 4733.500 / 11974.320 |
| Mean queue / service (ms) | 4365.740 / 1044.090 |
| Request / token hit rate | 100.000% / 6.489% |
| Saved prefill tokens | 3,091,558 |
| Executed-miss-token Gini | 0.005653 |
| Reactive / proactive transfers | 0 / 989 |
| Proactive / total wire bytes | 34,380,709,888 / 34,380,709,888 |
| Evictions / fallbacks | 90,138 / 0 |
| Unused replicas / wasted copies | 870 / 972 |

相对 R_AFF：mean completion +0.808%，token hit −0.036 pp，Gini +0.002728；相对 E02 mean completion +0.945%。
