# E09 ToolAgent — R_REQ_KV_TASK

> Status: completed  
> Run start: 2026-09-11T23:33:35+08:00  
> Protocol / commit: TaskMain-v1.1 / `54f3c5e6d1105f96e57c2ccc2dc8b26f3835089f`  
> Config: `configs/taskmain_v1.1/toolagent/E09_r_req_kv_task.yaml`  
> Result: `results/taskmain_evaluation/toolagent/E09_r_req_kv_task`

## 配置与检查

R_REQ_KV_TASK，theta=2，FULL_PREFIX/FULL_PAGE_ONLY reactive transfer。Evaluation 共 8,375 个请求。Exit code 0，capacity failures 0；Evaluation 内 58 tickets = 6 completed + 52 fallbacks，timeout 0。随机 Gini 为 N/A。

## 核心结果

| 指标 | 结果 |
|---|---:|
| Mean / P50 / P95 completion (ms) | 3533.997 / 2980.000 / 9305.010 |
| Mean queue / service (ms) | 3019.347 / 514.641 |
| Request / token hit rate | 99.928% / 37.612% |
| Saved prefill tokens | 25,883,198 |
| Executed-miss-token Gini | 0.000437 |
| Reactive / proactive transfers | 6 / 0 |
| Reactive / total wire bytes | 1,482,686,464 / 1,482,686,464 |
| Evictions / fallbacks | 88,368 / 52 |

相对 R_AFF：mean completion −3.525%，token hit −0.008 pp，Gini −0.003440。
