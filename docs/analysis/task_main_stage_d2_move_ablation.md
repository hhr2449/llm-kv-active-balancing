# TaskMain Stage D2 MOVE Ablation

**状态：PASS。** 四个固定 MOVE case 均完成双遍 replay；8/8 validation 与 determinism PASS。正式 COPY Lines3/5 以只读方式引用，provenance gate PASS。

## 1. 为什么需要细化 MOVE

原任务只规定 MOVE 在 source 释放，没有冻结释放时刻、共享祖先、transfer 期间可见性、pin、generation 或失败原子性。D2 因此将可执行语义冻结为 COPY-THEN-SAFE-RELEASE；没有复用旧 T2/O2/MOVE_SAFE 结果。

## 2. TaskMain MOVE protocol

MOVE 与 COPY 共用 routing、candidate、ranking、source、feasible-target-first、target、full-chain wire、Temporary、bandwidth、capacity 和 action cap。source 在 ready 前保持 Published 且可命中；target commit 成功后才解除本次 source transfer pin并尝试释放。target commit失败时source不变。

## 3. Safe source release

release 从 moved chain 的末页向 root 检查，只删除仍resident、generation一致、无其他active pin、无committed protection且为leaf的最大连续suffix。遇到共享/非leaf、pin、保护、generation变化或不resident立即停止。release不刷新LRU，也不计入普通LRU eviction。

## 4. COPY regression 与 provenance

正式 COPY reference=`formal:5ddeca8e0c768e04`；manifest SHA-256=`7499b1e08fe0f4b7a8bbd72c83b120c16ebfc6a647d797095ca469c056a4debf`，source provenance SHA-256=`25ddfae1be89148d8f30a39b03fd0b4764b0703041d4a7be4e00d5b69f4eb318`。D2 COPY mode 的冻结fixture execution projection回归通过；四组trace/cohort/参数/target-selection gate均PASS。

## 5. MOVE correctness 与确定性

全测试覆盖full/partial/zero release、共享祖先、deeper descendant、active pin、committed protection、generation变化、in-flight可见性、target commit失败、同timestamp顺序、COPY regression、Load/history/opportunity守恒。正式D2每case双遍逐文件SHA-256一致，capacity、Temporary/pin drain、Gate B、C1 right endpoint及legacy isolation均PASS。

## 6. 四组 COPY vs MOVE 结果

| Workload | Trigger | Action | Saved | Weighted | Token hit | Skew | Eval wire B | Full wire B | Unused wire | Eval actions | Released pages | Release mean | Zero/Partial/Full |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| conversation | FUTURE_DEMAND | COPY | 4,830,522 | 19226565.0000 | 0.1014 | 1.4894 | 1,842,744,393,728 | 4,910,584,168,448 | 0.9573 | 4,275 | NA | NA | NA/NA/NA |
| conversation | FUTURE_DEMAND | MOVE | 4,444,895 | 16509928.3000 | 0.0933 | 1.4966 | 1,546,192,420,864 | 4,617,731,571,712 | 0.9615 | 4,275 | 66,292 | 0.5874 | 1,413/2,862/0 |
| conversation | PERSISTENCE | COPY | 3,405,930 | 11884477.0000 | 0.0715 | 1.4984 | 1,801,654,894,592 | 5,311,247,155,200 | 0.9820 | 4,275 | NA | NA | NA/NA/NA |
| conversation | PERSISTENCE | MOVE | 3,429,865 | 12111221.3000 | 0.0720 | 1.7212 | 1,762,782,085,120 | 5,148,533,325,824 | 0.9867 | 4,275 | 67,401 | 0.5101 | 1,812/2,463/0 |
| toolagent | FUTURE_DEMAND | COPY | 27,685,758 | 72524672.6000 | 0.4023 | 2.3126 | 3,228,836,036,608 | 8,727,738,449,920 | 0.9632 | 8,363 | NA | NA | NA/NA/NA |
| toolagent | FUTURE_DEMAND | MOVE | 27,601,095 | 72209725.9000 | 0.4011 | 1.8020 | 3,234,076,819,456 | 8,926,653,317,120 | 0.9682 | 8,375 | 124,223 | 0.5086 | 3,552/4,655/168 |
| toolagent | PERSISTENCE | COPY | 25,976,731 | 63389059.1000 | 0.3775 | 2.7238 | 3,192,840,519,680 | 9,595,946,795,008 | 0.9788 | 8,335 | NA | NA | NA/NA/NA |
| toolagent | PERSISTENCE | MOVE | 26,056,499 | 64092901.5000 | 0.3786 | 2.9804 | 3,281,860,427,776 | 9,790,736,564,224 | 0.9827 | 8,365 | 114,048 | 0.4506 | 4,049/4,174/142 |

## 7. Source release 分布

| Workload | Trigger | MOVE actions | Released pages | Released bytes | Mean | Median | P75 | P90 | P95 | Zero | Partial | Full |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| conversation | FUTURE_DEMAND | 4,275 | 66,292 | 973,170,802,688 | 0.5874 | 0.8333 | 0.9524 | 0.9719 | 0.9836 | 1,413 | 2,862 | 0 |
| conversation | PERSISTENCE | 4,275 | 67,401 | 989,450,993,664 | 0.5101 | 0.7500 | 0.9524 | 0.9737 | 0.9841 | 1,812 | 2,463 | 0 |
| toolagent | FUTURE_DEMAND | 8,375 | 124,223 | 1,823,601,590,272 | 0.5086 | 0.7500 | 0.9524 | 0.9796 | 0.9905 | 3,552 | 4,655 | 168 |
| toolagent | PERSISTENCE | 8,365 | 114,048 | 1,674,231,939,072 | 0.4506 | 0.5000 | 0.9500 | 0.9773 | 0.9895 | 4,049 | 4,174 | 142 |

## 8. Capacity 与 ordinary churn

Source release未并入下表ordinary LRU eviction/turnover。

| Workload | Trigger | Action | Eval eviction | Full eviction | Eval turnover pages | Full turnover pages | Eval admission skip | Full admission skip | Eval no-capacity skips | Full no-capacity skips |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| conversation | FUTURE_DEMAND | COPY | 203,075 | 565,349 | 203,075 | 565,349 | 1 | 6 | 0 | 9 |
| conversation | FUTURE_DEMAND | MOVE | 118,537 | 355,995 | 118,537 | 355,995 | 0 | 0 | 0 | 0 |
| conversation | PERSISTENCE | COPY | 196,207 | 571,086 | 196,207 | 571,086 | 0 | 12 | 0 | 20 |
| conversation | PERSISTENCE | MOVE | 124,474 | 374,206 | 124,474 | 374,206 | 1 | 12 | 0 | 15 |
| toolagent | FUTURE_DEMAND | COPY | 288,700 | 791,798 | 288,700 | 791,798 | 12 | 58 | 229 | 3,154 |
| toolagent | FUTURE_DEMAND | MOVE | 168,930 | 482,947 | 168,930 | 482,947 | 3 | 22 | 0 | 109 |
| toolagent | PERSISTENCE | COPY | 276,988 | 809,061 | 276,988 | 809,061 | 22 | 85 | 503 | 1,615 |
| toolagent | PERSISTENCE | MOVE | 167,871 | 492,387 | 167,871 | 492,387 | 7 | 49 | 124 | 1,198 |

## 9. Saved、WeightedSaved、Skew 与 wire 差值

- **conversation / FUTURE_DEMAND：** ΔSaved=-385,627，ΔWeighted=-2716636.7000，ΔSkew=0.0072，ΔEval wire=-296,551,972,864 B，Δordinary eviction=-84,538，Δadmission skip=-1。
- **conversation / PERSISTENCE：** ΔSaved=23,935，ΔWeighted=226744.3000，ΔSkew=0.2228，ΔEval wire=-38,872,809,472 B，Δordinary eviction=-71,733，Δadmission skip=1。
- **toolagent / FUTURE_DEMAND：** ΔSaved=-84,663，ΔWeighted=-314946.7000，ΔSkew=-0.5105，ΔEval wire=5,240,782,848 B，Δordinary eviction=-119,770，Δadmission skip=-9。
- **toolagent / PERSISTENCE：** ΔSaved=79,768，ΔWeighted=703842.4000，ΔSkew=0.2565，ΔEval wire=89,019,908,096 B，Δordinary eviction=-109,117，Δadmission skip=-15。

## 10. Target-side unused transfer wire

COPY 的正式 wasted-copy ratio在本表映射为共同的unused_transfer_wire_ratio；MOVE按同一target、完整chain hit、generation匹配与censor规则计算unused_move_wire_ratio。它不度量source release，也不把partial reuse归为完整使用。

## 11. Workload 差异

Conversation 与 ToolAgent 分别报告，不合并为单一总体效应。重点结合release fraction、ordinary churn、Saved/WeightedSaved、Skew、wire与unused ratio判断；闭环action sequence可在MOVE释放source后合法分叉，因此不强制总动作数或总wire相同。

## 12. 结论边界

该结果只回答：在相同 trigger/placement 与 TaskMain safe-release 语义下，不保留source独占suffix副本的影响。共享ancestor、active pin和committed protection可使MOVE只释放部分或零页；不能外推为所有KV迁移系统或完整整链迁移。

Stage D2完成后停止；未启动granularity、predictor、参数搜索或Stage D3。
