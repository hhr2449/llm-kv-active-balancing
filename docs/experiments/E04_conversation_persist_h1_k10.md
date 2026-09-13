# E04 Conversation — Persist h=60s K=10

> Status: completed  
> Protocol: TaskMain-v1.1  
> Run start: 2026-09-11T22:56:25+08:00  
> Git commit: `54f3c5e6d1105f96e57c2ccc2dc8b26f3835089f`  
> Config: `configs/taskmain_v1.1/conversation/E04_persist_h1_k10.yaml`  
> Result: `results/taskmain_evaluation/conversation/E04_persist_h1_k10`

## 配置与检查

R_AFF + T_PERSIST + COPY + TRIGGER_ONLY，history=60 s、K=10，冻结 proactive budget。仅统计 Evaluation 的 4,275 个请求。Exit code 0；capacity failures 0；future leakage 0；action exceeds bucket cap 0。随机 Gini 为 N/A。

## 核心结果

| 指标 | 结果 |
|---|---:|
| Mean / P50 / P95 completion (ms) | 5365.088 / 4717.700 / 11907.570 |
| Mean queue / service (ms) | 4322.016 / 1043.072 |
| Request / token hit rate | 100.000% / 6.581% |
| Saved prefill tokens | 3,135,078 |
| Executed-miss-token Gini | 0.002492 |
| Reactive / proactive transfers | 0 / 526 |
| Proactive / total wire bytes | 34,395,389,952 / 34,395,389,952 |
| Evictions / fallbacks | 90,628 / 0 |
| Unused replicas / wasted copies | 477 / 508 |

相对 R_AFF：mean completion −0.026%，token hit +0.056 pp，Gini −0.000434；相对 E02 mean completion +0.110%。
