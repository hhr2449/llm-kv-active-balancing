# E02 Conversation — R_REQ_KV_TASK

> Status: completed  
> Protocol: TaskMain-v1.1  
> Run start: 2026-09-11T22:55:33+08:00  
> Git commit: `54f3c5e6d1105f96e57c2ccc2dc8b26f3835089f`  
> Config: `configs/taskmain_v1.1/conversation/E02_r_req_kv_task.yaml`  
> Result: `results/taskmain_evaluation/conversation/E02_r_req_kv_task`

## 配置与检查

共享配置；R_REQ_KV_TASK，theta=2，FULL_PREFIX/FULL_PAGE_ONLY reactive transfer。仅统计 Evaluation 的 4,275 个请求。Simulator exit code 0；capacity admission failures 0；Evaluation 内 24 tickets = 10 completed + 14 fallbacks，timeout 0。随机 Gini 基线为 N/A。

## 核心结果

| 指标 | 结果 |
|---|---:|
| Mean / P50 / P95 completion (ms) | 5359.187 / 4687.900 / 11840.430 |
| Mean queue / service (ms) | 4316.027 / 1043.127 |
| Request / token hit rate | 100.000% / 6.576% |
| Saved prefill tokens | 3,132,732 |
| Executed-miss-token Gini | 0.003995 |
| Reactive / proactive transfers | 10 / 0 |
| Reactive / total wire bytes | 3,347,054,592 / 3,347,054,592 |
| Evictions / fallbacks | 89,175 / 14 |

相对 R_AFF：mean completion −0.136%，token hit +0.051 pp，Gini +0.001070。
