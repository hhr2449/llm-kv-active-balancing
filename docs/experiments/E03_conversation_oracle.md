# E03 Conversation — TaskMain-Oracle

> Status: completed  
> Protocol: TaskMain-v1.1  
> Run start: 2026-09-11T22:55:40+08:00  
> Git commit: `54f3c5e6d1105f96e57c2ccc2dc8b26f3835089f`  
> Config: `configs/taskmain_v1.1/conversation/E03_oracle.yaml`  
> Result: `results/taskmain_evaluation/conversation/E03_oracle`

## 配置与检查

R_AFF + T_FUTURE_DEMAND + COPY + TRIGGER_ONLY，W=300 s；1 s trigger、每 tick 最多 1 action、冻结 byte budget。仅统计 Evaluation 的 4,275 个请求；Oracle 可读取 ObservationTail。Exit code 0；capacity failures 0；future reads outside allowed trace/protocol 0；action exceeds bucket cap 0。随机 Gini 为 N/A。

## 核心结果

| 指标 | 结果 |
|---|---:|
| Mean / P50 / P95 completion (ms) | 5369.784 / 4687.300 / 11915.080 |
| Mean queue / service (ms) | 4327.206 / 1042.578 |
| Request / token hit rate | 100.000% / 6.625% |
| Saved prefill tokens | 3,156,193 |
| Executed-miss-token Gini | 0.002407 |
| Reactive / proactive transfers | 0 / 1,147 |
| Proactive / total wire bytes | 34,351,349,760 / 34,351,349,760 |
| Evictions / fallbacks | 90,077 / 0 |
| Unused replicas / wasted copies | 1,053 / 1,065 |

相对 R_AFF：mean completion +0.062%，token hit +0.100 pp，Gini −0.000518；相对 E02 mean completion +0.198%。
