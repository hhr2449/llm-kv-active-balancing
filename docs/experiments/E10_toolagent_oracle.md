# E10 ToolAgent — TaskMain-Oracle

> Status: completed  
> Run start: 2026-09-11T23:33:44+08:00  
> Protocol / commit: TaskMain-v1.1 / `54f3c5e6d1105f96e57c2ccc2dc8b26f3835089f`  
> Config: `configs/taskmain_v1.1/toolagent/E10_oracle.yaml`  
> Result: `results/taskmain_evaluation/toolagent/E10_oracle`

## 配置与检查

R_AFF + T_FUTURE_DEMAND + COPY + TRIGGER_ONLY，W=300 s，冻结 budget。Evaluation 共 8,375 个请求。Exit code 0；capacity failures、future reads outside allowed trace/protocol、action exceeds bucket cap 均为 0。随机 Gini 为 N/A。

## 核心结果

| 指标 | 结果 |
|---|---:|
| Mean / P50 / P95 completion (ms) | 3567.369 / 3007.400 / 9408.440 |
| Mean queue / service (ms) | 3053.167 / 514.202 |
| Request / token hit rate | 99.964% / 37.665% |
| Saved prefill tokens | 25,919,992 |
| Executed-miss-token Gini | 0.003397 |
| Reactive / proactive transfers | 0 / 1,155 |
| Proactive / total wire bytes | 34,351,349,760 / 34,351,349,760 |
| Evictions / fallbacks | 89,428 / 0 |
| Unused replicas / wasted copies | 1,057 / 1,066 |

相对 R_AFF：mean completion −2.614%，token hit +0.045 pp，Gini −0.000481；相对 E09 mean completion +0.944%。
