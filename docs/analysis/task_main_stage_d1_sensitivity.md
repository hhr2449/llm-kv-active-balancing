# TaskMain Stage D1 Sensitivity

**状态：PASS。** 32 个固定 sensitivity cases 均完成双遍 replay；64/64 replay validation 与 determinism PASS。该阶段是 one-factor robustness，不是参数搜索，未实现 MOVE。

结果目录：`results/task_main/stage_d1_sensitivity/run_20260917_01`。正式默认点引用 `formal:5ddeca8e0c768e04`；所有引用均通过冻结源码、配置、trace 与 cohort 身份检查。

N sensitivity 保持 585 pages/Pod，因此 N=2 总容量 1,170 pages，N=4 为 2,340 pages；不能解释为只改变 Pod 数的纯因果效应。Oracle W 的三个点统一使用 `[10,25)` min，Waste observation horizon 分别为 1/5/30 min，不能把不同 W 的 Waste 当作同尺度预测准确率。

Line 7 Cost-aware 不在 sweep 中重复；正式 Line 5/7 execution-equivalence 与 truthful policy metadata 继续作为冻结 ablation 结论，未调整 lambda。

## Q1. R_LEAST 与两个 baseline

| Workload | Case | Saved | WeightedSaved | Token hit | Skew | Eval wire B | Full wire B | Waste |
|---|---|---|---|---|---|---|---|---|
| conversation | r_aff_reference | 2,201,477 | 5450182.500 | 0.046 | 28.950 | 0 | 0 | NA |
| conversation | r_req_kv_reference | 3,080,294 | 9492657.400 | 0.065 | 1.890 | 0 | 102,760,448 | NA |
| conversation | rleast | 2,413,743 | 6640150.700 | 0.051 | 1.542 | 0 | 0 | NA |
| toolagent | r_aff_reference | 25,041,672 | 58636146.000 | 0.364 | 22.032 | 0 | 0 | NA |
| toolagent | r_req_kv_reference | 25,863,297 | 63001485.300 | 0.376 | 9.053 | 0 | 616,562,688 | NA |
| toolagent | rleast | 25,198,231 | 59805721.900 | 0.366 | 1.976 | 0 | 0 | NA |

- **conversation：** Saved 最高为 `r_req_kv_reference`（3,080,294）；Skew 最低为 `rleast`（1.542）。R_LEAST 是纯 load-first、R_AFF 是 cache-first、R_REQ_KV 保持 cache affinity 并只在 gate 通过时响应式平衡。
- **toolagent：** Saved 最高为 `r_req_kv_reference`（25,863,297）；Skew 最低为 `rleast`（1.976）。R_LEAST 是纯 load-first、R_AFF 是 cache-first、R_REQ_KV 保持 cache affinity 并只在 gate 通过时响应式平衡。

## Q2. R_REQ_KV theta sensitivity

| Workload | Case | Saved | WeightedSaved | Token hit | Skew | Eval wire B | Full wire B | Waste |
|---|---|---|---|---|---|---|---|---|
| conversation | theta_1p5 | 3,138,074 | 10065813.400 | 0.066 | 0.878 | 0 | 176,160,768 | NA |
| conversation | theta_2_reference | 3,080,294 | 9492657.400 | 0.065 | 1.890 | 0 | 102,760,448 | NA |
| conversation | theta_3 | 3,080,294 | 9492657.400 | 0.065 | 1.890 | 0 | 102,760,448 | NA |
| toolagent | theta_1p5 | 25,876,451 | 62630089.900 | 0.376 | 7.045 | 0 | 411,041,792 | NA |
| toolagent | theta_2_reference | 25,863,297 | 63001485.300 | 0.376 | 9.053 | 0 | 616,562,688 | NA |
| toolagent | theta_3 | 25,863,297 | 63001485.300 | 0.376 | 9.053 | 0 | 616,562,688 | NA |

- **conversation：** θ=1.5: Saved=3,138,074, PRE/EVAL gate=7/0, PRE/EVAL reactive COPY=7/0; θ=2: Saved=3,080,294, PRE/EVAL gate=7/0, PRE/EVAL reactive COPY=7/0; θ=3.0: Saved=3,080,294, PRE/EVAL gate=7/0, PRE/EVAL reactive COPY=7/0。比较基准固定为 θ=2。
- **toolagent：** θ=1.5: Saved=25,876,451, PRE/EVAL gate=9/0, PRE/EVAL reactive COPY=9/0; θ=2: Saved=25,863,297, PRE/EVAL gate=9/0, PRE/EVAL reactive COPY=9/0; θ=3.0: Saved=25,863,297, PRE/EVAL gate=9/0, PRE/EVAL reactive COPY=9/0。比较基准固定为 θ=2。

## Q3–Q4. Persistence K sensitivity

| Workload | Case | Saved | WeightedSaved | Token hit | Skew | Eval wire B | Full wire B | Waste |
|---|---|---|---|---|---|---|---|---|
| conversation | persist_h60_k5 | 2,875,498 | 10251811.400 | 0.060 | 1.136 | 3,495,205,797,888 | 10,605,421,395,968 | 0.995 |
| conversation | persist_h60_k10_reference | 2,604,650 | 8004080.200 | 0.055 | 1.590 | 3,496,644,444,160 | 10,671,364,243,456 | 0.997 |
| conversation | persist_h60_k20 | 2,660,458 | 8472509.000 | 0.056 | 0.984 | 3,407,888,777,216 | 10,447,463,907,328 | 0.998 |
| conversation | persist_h300_k5 | 3,405,930 | 11884477.000 | 0.071 | 1.498 | 1,801,654,894,592 | 5,311,247,155,200 | 0.982 |
| conversation | persist_h300_k10_reference | 3,405,930 | 11884477.000 | 0.071 | 1.498 | 1,801,654,894,592 | 5,311,247,155,200 | 0.982 |
| conversation | persist_h300_k20 | 3,405,930 | 11884477.000 | 0.071 | 1.498 | 1,801,654,894,592 | 5,311,247,155,200 | 0.982 |
| toolagent | persist_h60_k5 | 25,421,109 | 61288036.900 | 0.369 | 2.706 | 5,370,084,851,712 | 16,097,512,259,584 | 0.985 |
| toolagent | persist_h60_k10_reference | 25,356,766 | 61008957.400 | 0.368 | 2.910 | 5,561,894,567,936 | 16,418,829,500,416 | 0.986 |
| toolagent | persist_h60_k20 | 25,425,081 | 61548027.700 | 0.369 | 2.069 | 5,590,359,212,032 | 16,545,180,811,264 | 0.985 |
| toolagent | persist_h300_k5 | 26,125,107 | 64516325.500 | 0.380 | 2.552 | 3,190,021,947,392 | 9,554,651,774,976 | 0.977 |
| toolagent | persist_h300_k10_reference | 25,976,731 | 63389059.100 | 0.377 | 2.724 | 3,192,840,519,680 | 9,595,946,795,008 | 0.979 |
| toolagent | persist_h300_k20 | 26,062,718 | 63021031.000 | 0.379 | 2.377 | 2,994,101,813,248 | 9,356,382,830,592 | 0.978 |

- **conversation, h=60s：** K=5/10/20 的 Saved 为 2,875,498/2,604,650/2,660,458，Eval wire 为 3,495,205,797,888/3,496,644,444,160/3,407,888,777,216，Waste 为 0.995/0.997/0.998；同一 h 内只相对 K=10 解读。
- **conversation, h=300s：** K=5/10/20 的 Saved 为 3,405,930/3,405,930/3,405,930，Eval wire 为 1,801,654,894,592/1,801,654,894,592/1,801,654,894,592，Waste 为 0.982/0.982/0.982；同一 h 内只相对 K=10 解读。
  - h=60 的 Saved 在 K=5/10/20 是否均低于 h=300：`True`。这项逐 K 对照用于判断 h 与 K 的相对作用，不用于选择最优 K。
- **toolagent, h=60s：** K=5/10/20 的 Saved 为 25,421,109/25,356,766/25,425,081，Eval wire 为 5,370,084,851,712/5,561,894,567,936/5,590,359,212,032，Waste 为 0.985/0.986/0.985；同一 h 内只相对 K=10 解读。
- **toolagent, h=300s：** K=5/10/20 的 Saved 为 26,125,107/25,976,731/26,062,718，Eval wire 为 3,190,021,947,392/3,192,840,519,680/2,994,101,813,248，Waste 为 0.977/0.979/0.978；同一 h 内只相对 K=10 解读。
  - h=60 的 Saved 在 K=5/10/20 是否均低于 h=300：`True`。这项逐 K 对照用于判断 h 与 K 的相对作用，不用于选择最优 K。

## Q5. Recency q sensitivity

| Workload | Case | Saved | WeightedSaved | Token hit | Skew | Eval wire B | Full wire B | Waste |
|---|---|---|---|---|---|---|---|---|
| conversation | recency_q0p8 | 3,217,722 | 10657630.600 | 0.068 | 1.801 | 1,357,406,797,824 | 2,830,375,059,456 | 0.980 |
| conversation | recency_q0p9_reference | 3,217,722 | 10657630.600 | 0.068 | 1.801 | 1,357,406,797,824 | 2,830,375,059,456 | 0.980 |
| toolagent | recency_q0p8 | 25,922,338 | 63760026.600 | 0.377 | 2.413 | 2,064,971,202,560 | 4,440,205,557,760 | 0.978 |
| toolagent | recency_q0p9_reference | 25,922,338 | 63760026.600 | 0.377 | 2.413 | 2,064,971,202,560 | 4,440,205,557,760 | 0.978 |

- **conversation：** q=.8 相对 q=.9：ΔSaved=0，ΔEval wire=0，Δproactive actions=0，Δnew-page use ratio=0.000。positive/shortlist counts 为 307,425/145,237 对 307,425/106,969。
- **toolagent：** q=.8 相对 q=.9：ΔSaved=0，ΔEval wire=0，Δproactive actions=0，Δnew-page use ratio=0.000。positive/shortlist counts 为 312,822/139,853 对 312,822/109,808。

## Q6. Oracle W common-support sensitivity

| Workload | Case | Saved | WeightedSaved | Token hit | Skew | Eval wire B | Full wire B | Waste |
|---|---|---|---|---|---|---|---|---|
| conversation | oracle_w1m | 4,378,860 | 16643378.800 | 0.118 | 2.553 | 1,012,072,972,288 | 4,249,306,005,504 | 0.915 |
| conversation | oracle_w5m | 3,389,164 | 12284569.200 | 0.091 | 1.092 | 1,231,128,887,296 | 4,910,584,168,448 | 0.952 |
| conversation | oracle_w30m | 2,625,536 | 9639731.200 | 0.071 | 0.655 | 1,462,119,694,336 | 2,808,457,723,904 | 0.977 |
| toolagent | oracle_w1m | 20,789,996 | 55049164.400 | 0.404 | 2.523 | 1,212,940,288,000 | 5,155,462,316,032 | 0.915 |
| toolagent | oracle_w5m | 19,834,880 | 51447398.400 | 0.385 | 2.885 | 2,353,874,862,080 | 8,727,738,449,920 | 0.959 |
| toolagent | oracle_w30m | 18,933,248 | 47989299.200 | 0.368 | 2.375 | 2,702,864,023,552 | 4,999,515,996,160 | 0.972 |

- **conversation：** W=1/5/30min 的 Saved 为 4,378,860/3,389,164/2,625,536，WeightedSaved 为 16643378.800/12284569.200/9639731.200，Eval wire 为 1,012,072,972,288/1,231,128,887,296/1,462,119,694,336。三个点均来自同一 `[10,25)` cohort。
- **toolagent：** W=1/5/30min 的 Saved 为 20,789,996/19,834,880/18,933,248，WeightedSaved 为 55049164.400/51447398.400/47989299.200，Eval wire 为 1,212,940,288,000/2,353,874,862,080/2,702,864,023,552。三个点均来自同一 `[10,25)` cohort。

## Q7. N sensitivity

| Workload | Case | Saved | WeightedSaved | Token hit | Skew | Eval wire B | Full wire B | Waste |
|---|---|---|---|---|---|---|---|---|
| conversation | n2_aff | 2,201,477 | 5450182.500 | 0.046 | 39.272 | 0 | 0 | NA |
| conversation | n2_oracle | 4,074,373 | 15340896.100 | 0.086 | 0.766 | 1,744,666,886,144 | 4,859,512,225,792 | 0.967 |
| conversation | n2_persist300 | 3,037,802 | 9673507.400 | 0.064 | 1.197 | 1,786,020,626,432 | 5,537,364,180,992 | 0.987 |
| conversation | n2_recency | 2,482,474 | 6291843.400 | 0.052 | 0.793 | 322,447,605,760 | 775,988,183,040 | 0.978 |
| conversation | n2_reqkv | 2,457,823 | 6757915.500 | 0.052 | 1.006 | 0 | 44,040,192 | NA |
| conversation | n4_aff_reference | 2,201,477 | 5450182.500 | 0.046 | 28.950 | 0 | 0 | NA |
| conversation | n4_oracle_reference | 4,830,522 | 19226565.000 | 0.101 | 1.489 | 1,842,744,393,728 | 4,910,584,168,448 | 0.957 |
| conversation | n4_persist300_reference | 3,405,930 | 11884477.000 | 0.071 | 1.498 | 1,801,654,894,592 | 5,311,247,155,200 | 0.982 |
| conversation | n4_recency_reference | 3,217,722 | 10657630.600 | 0.068 | 1.801 | 1,357,406,797,824 | 2,830,375,059,456 | 0.980 |
| conversation | n4_reqkv_reference | 3,080,294 | 9492657.400 | 0.065 | 1.890 | 0 | 102,760,448 | NA |
| toolagent | n2_aff | 25,010,894 | 58575614.600 | 0.363 | 4.174 | 0 | 0 | NA |
| toolagent | n2_oracle | 27,132,382 | 71000330.200 | 0.394 | 2.294 | 2,765,386,416,128 | 7,327,788,826,624 | 0.963 |
| toolagent | n2_persist300 | 25,787,081 | 63018070.900 | 0.375 | 2.763 | 3,041,944,141,824 | 8,933,039,144,960 | 0.978 |
| toolagent | n2_recency | 25,294,148 | 59602557.200 | 0.368 | 2.290 | 492,207,865,856 | 1,056,127,844,352 | 0.981 |
| toolagent | n2_reqkv | 25,289,028 | 60284643.600 | 0.367 | 27.108 | 0 | 205,520,896 | NA |
| toolagent | n4_aff_reference | 25,041,672 | 58636146.000 | 0.364 | 22.032 | 0 | 0 | NA |
| toolagent | n4_oracle_reference | 27,685,758 | 72524672.600 | 0.402 | 2.313 | 3,228,836,036,608 | 8,727,738,449,920 | 0.963 |
| toolagent | n4_persist300_reference | 25,976,731 | 63389059.100 | 0.377 | 2.724 | 3,192,840,519,680 | 9,595,946,795,008 | 0.979 |
| toolagent | n4_recency_reference | 25,922,338 | 63760026.600 | 0.377 | 2.413 | 2,064,971,202,560 | 4,440,205,557,760 | 0.978 |
| toolagent | n4_reqkv_reference | 25,863,297 | 63001485.300 | 0.376 | 9.053 | 0 | 616,562,688 | NA |

- **conversation：** AFF: ΔSaved(N2-N4)=0, ΔSkew=10.322; REQ_KV: ΔSaved(N2-N4)=-622,471, ΔSkew=-0.884; Oracle: ΔSaved(N2-N4)=-756,149, ΔSkew=-0.724; P300: ΔSaved(N2-N4)=-368,128, ΔSkew=-0.301; Recency: ΔSaved(N2-N4)=-735,248, ΔSkew=-1.008。该差异同时包含总 Cache 容量变化。
- **toolagent：** AFF: ΔSaved(N2-N4)=-30,778, ΔSkew=-17.858; REQ_KV: ΔSaved(N2-N4)=-574,269, ΔSkew=18.055; Oracle: ΔSaved(N2-N4)=-553,376, ΔSkew=-0.019; P300: ΔSaved(N2-N4)=-189,650, ΔSkew=0.039; Recency: ΔSaved(N2-N4)=-628,190, ΔSkew=-0.124。该差异同时包含总 Cache 容量变化。

## Q8. Formal Main 核心结论的方向检查

- `True` — conversation: theta范围内R_REQ_KV Saved均不低于R_AFF
- `True` — conversation: N=2 oracle Saved相对R_REQ_KV方向为非负
- `True` — conversation: N=2 persist300 Saved相对R_REQ_KV方向为非负
- `True` — conversation: N=2 recency Saved相对R_REQ_KV方向为非负
- `True` — conversation: N=4 oracle Saved相对R_REQ_KV方向为非负
- `True` — conversation: N=4 persist300 Saved相对R_REQ_KV方向为非负
- `True` — conversation: N=4 recency Saved相对R_REQ_KV方向为非负
- `True` — toolagent: theta范围内R_REQ_KV Saved均不低于R_AFF
- `True` — toolagent: N=2 oracle Saved相对R_REQ_KV方向为非负
- `True` — toolagent: N=2 persist300 Saved相对R_REQ_KV方向为非负
- `True` — toolagent: N=2 recency Saved相对R_REQ_KV方向为非负
- `True` — toolagent: N=4 oracle Saved相对R_REQ_KV方向为非负
- `True` — toolagent: N=4 persist300 Saved相对R_REQ_KV方向为非负
- `True` — toolagent: N=4 recency Saved相对R_REQ_KV方向为非负

上述预先声明的方向检查通过 14/14 项。未通过项是 sensitivity 发现，不会触发补点、调参或修改默认配置。
完整结论数据位于同目录六张 family 表和 `all_sensitivity_long.csv/json`。

运行期间继续满足 C1 right-closed future input separation、Gate B、legacy dependency isolation、finite capacity 与正式主指标定义。完成后停止；Stage D2 MOVE 未启动。
