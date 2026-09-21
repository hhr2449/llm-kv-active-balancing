# TaskMain Final Figures 审计报告

日期：2026-09-18。仓库：`/root/data2/llm-kv-active-balancing`。

**状态：FINAL_FIGURES_PASS。** 本轮只读取冻结目录 `results/task_main/final_results/`，未调用 simulator，未读取 Formal/D1/D2 raw results、pilot、smoke、INVALID 或 `SUPERSEDED_INCOMPLETE` 产物，未修改 canonical 数据。`consolidation_validation.json` 在绘图前为 `PASS`，`final_state=FINAL_RESULTS_CONSOLIDATION_PASS`；所有实际使用的 canonical 文件 SHA-256 均与 `consolidation_manifest.json` 冻结身份一致。

共生成 16 个 figure IDs，包括 9 组核心图、协议要求的 companion 图和 2 张附录图。每个 figure ID 均有 320 dpi PNG、PDF、SVG 和可复现的 figure-data CSV，共 48 个图文件。完整路径、输入表及全部 SHA-256 记录在 `results/task_main/final_figures/figure_manifest.json`。

## 统一绘图口径

- 图内文字为英文，使用 Matplotlib 与 DejaVu Sans；没有 seaborn、3D、饼图、双 Y 轴、置信区间或统计显著性标记。
- Conversation 与 ToolAgent 从不求平均；需要并列时固定 Conversation 在左、ToolAgent 在右。
- 策略颜色在所有图中固定。COPY/MOVE、两个 workload 及 Persistence 的历史窗口也使用一致的视觉语义。
- 百分比由 canonical raw values 在构建时重算。Figure 2 的 Saved delta 与 unused-wire percentage、Figure 3 的 Oracle delta、Figure 8 的 Saved/eviction delta 均有重算断言。
- CSV 的 `NA` 保留为 `NA`。例如 Figure 2 中 Lines 1/2 的 proactive waste 缺失值不绘制为 0。
- `MAIN_25_45` 与 `W_SENS_10_25` 不连接、不作 delta。Oracle-W 图只使用统一的 `W_SENS_10_25` cohort。
- 双遍 deterministic replay 只证明可复现性，不被用作统计重复；所有图均无虚构 error bars、confidence intervals 或 p-values。

## 逐图审计

| Figure | 回答的问题 | canonical 输入与坐标 | Cohort | 关键读法 | 不能得出的结论 |
|---|---|---|---|---|---|
| `fig01_baseline_tradeoff` | 三种 baseline routing 的 reuse/balance trade-off 是什么？ | `baseline_comparison.csv`；x=skew ratio，y=Saved Tokens (M) | `MAIN_25_45` | R_AFF 的 skew 最大；R_LEAST 最均衡；R_REQ_KV 在两个 workload 上取得更高 Saved。 | 不能把平面上的任一点称为跨指标“全局最优”。 |
| `fig02_main_7line` | Formal Main 的 benefit、balance、wire 与 copy efficiency 如何同时变化？ | `main_7line_combined.csv`、`final_results_long.csv`；2×2 独立单位 panels | `MAIN_25_45` | Oracle benefit 最大但 wire 很高；P60 为负收益；P300 与 Recency 为较温和正收益；Line 7 与 Line 5 执行一致。 | 不能把 unused wire 当成“完全无价值”，也不能把四种单位合成单一效用分数。 |
| `fig03_oracle_headroom` | 已知未来需求相对 R_REQ_KV 有多少参考 headroom？ | `oracle_headroom.csv`；R_REQ_KV/Oracle grouped bars，分别绘制 Saved 与 WeightedSaved | `MAIN_25_45` | Saved delta 从 raw token 重算为 Conversation +56.82%、ToolAgent +7.05%；WeightedSaved delta 分别为 +102.54%、+15.12%。 | Oracle 是 future-demand reference，不是理论最优，也不是可在线部署策略。 |
| `fig04_prompt_bucket_contribution` | WeightedSaved delta 来自哪些 prompt buckets？ | `prompt_bucket_final.csv`；x=六个完整 buckets，y=ΔWeightedSaved vs Line 2 | `MAIN_25_45` | 正贡献总体集中在 20–120k。60–120k 是多数 workload/策略组合的最大贡献桶；ToolAgent P300 例外，其 60–120k 为负、20–60k 为正。 | 不能选择性隐藏负桶，也不能声称所有策略的主要贡献都必然来自同一桶。 |
| `fig04b_prompt_bucket_saved_delta` | 未加权 Saved delta 的 bucket 构成是什么？ | 同上；y=ΔSaved Tokens vs Line 2 | `MAIN_25_45` | Oracle 在 Conversation 的最大贡献为 60–120k，在 ToolAgent 为 20–60k；P300/Recency 的负桶被完整保留。 | WeightedSaved 与 Saved 的桶排序不能相互替代。 |
| `fig05_persistence_sensitivity` | h 与 K 对 Persistence benefit 的影响如何？ | `persistence_sensitivity_final.csv`；x=K，y=Saved Tokens (M)，两条 h 曲线 | `MAIN_25_45` | h=300s 与 h=60s 的差异明显大于 K=5/10/20 的局部变化。 | 不依据单个最高点宣称 K=5 或其他 K 为普遍最优。 |
| `fig05b_persistence_wire` | Persistence 参数如何改变 Evaluation wire？ | 同上；y=Evaluation wire (TB) | `MAIN_25_45` | h=60s 在两个 workload 上都使用更多 wire；K 的影响较小。 | 不能把较少 wire 单独解释为更好策略。 |
| `fig05c_persistence_waste` | Persistence 参数如何改变 unused-wire rate？ | 同上；y=Unused transfer wire (%) | `MAIN_25_45` | 所有组合仍处在很高的 unused-wire 区域；h=300s 通常略低。 | 不能将该比率解释为完全没有 partial reuse。 |
| `fig06_oracle_horizon` | 相同 Evaluation cohort 下，W=1/5/30 min 如何影响 Oracle？ | `oracle_w_sensitivity_final.csv`；真实数值 x=1/5/30，分别绘制 Saved、wire、skew | **`W_SENS_10_25`** | 两个 workload 上 W=1min Saved 最高；更长未来视野没有带来更高 Saved。 | 不与 Formal Main `[25,45)` 的 W=5 直接连线或作 delta；图中也不把不同 W 的 observation horizon 当作相同预测准确率标尺。 |
| `fig07a_theta_sensitivity` | R_REQ_KV 对 θ 是否稳健？ | `theta_sensitivity_final.csv`；x=1.5/2/3，y=Saved 或 skew | `MAIN_25_45` | θ=2 与 θ=3 在两个 workload 上相同；θ=1.5 改变 benefit/balance，Conversation 更明显。 | 不能从三点得出连续参数的最优阈值。 |
| `fig07b_n_sensitivity` | N=2/4 下结果如何变化？ | `n_sensitivity_final.csv`；五策略 grouped bars，分别绘制 Saved 与 skew | `MAIN_25_45` | 大多数策略在 N=2 下 Saved 下降；R_AFF Conversation Saved 相同但 skew 更差。 | **每 Pod 容量固定为 585 pages，N 同时改变总 Cache 容量；不能解释为纯 Pod-count 因果效应。** |
| `fig08_copy_vs_move` | MOVE 相对配对 COPY 改变了什么？ | `copy_vs_move.csv`、`copy_vs_move_delta.csv`；四组 MOVE−COPY delta | `MAIN_25_45` | MOVE 在四组都减少 ordinary LRU eviction（约 36.6%–41.6%），但 Saved 与 wire 的方向不一致。 | 不能声称 MOVE 全面优于 COPY。 |
| `fig08b_move_release_composition` | MOVE 动作实际释放 source 的构成是什么？ | 同上；ZERO/PARTIAL/FULL 100% stacked bars | `MAIN_25_45` | source release 主要是 partial；Conversation 两组没有 full release，ToolAgent 只有少量 full release。 | 释放比例不是 target-side reuse benefit，也不是 Saved attribution。 |
| `fig09_copy_efficiency` | 为什么 formal waste 接近 98%，但又不能说复制完全无价值？ | `diagnostics_summary.csv`；full-chain used、any-prefix reuse、new-page use、wire redundancy 分组展示 | `MAIN_25_45` | any-prefix reuse 很常见（约 93.5%–100%），但 full-chain used 仅约 2.2%–6.6%，new-page observed use 仅约 1.8%–4.3%。 | 四个指标分母不同，不能堆叠、相加或互相替代；any-prefix reuse 不证明整个 wire 成本有效。 |
| `figA1_persistence_chain_depth` | Persistence 的 h/K 如何改变复制深度？ | `persistence_sensitivity_final.csv`；x=K，y=mean chain depth | `MAIN_25_45` | h=60s 的 chain 明显更深，与其高 wire/高 waste 现象一致。 | 这是关联诊断，不是复制深度对 Saved 的单变量因果估计。 |
| `figA2_recency_shortlist_actions` | q=.8/.9 是否改变 shortlist 与实际动作？ | `recency_sensitivity_final.csv`；shortlist entries 与 action count | `MAIN_25_45` | q=.8 产生更大 shortlist，但两个 q 的 action count 相同。 | 不能由 action count 相同推断所有诊断元数据或计算成本相同。 |

## Figure 9 分母说明

Figure 9 中四个百分比必须分别解释：

- `Full-chain used %`：以 observed proactive actions 为分母，等于 `1-full_chain_unused_ratio`。
- `Any prefix reuse %`：以 observed proactive actions 为分母，只要求至少有 generation-matched Prefix reuse。
- `New-page observed use %`：以 proactive 新发布页面为分母。
- `Wire redundancy %`：以 transferred tokens 为分母，表示 ready 时已经存在于 target 的比例；它不是因果收益。

因此，Conversation Oracle 可以同时出现 100% 的 any-prefix reuse、4.89% 的 full-chain used、4.34% 的 new-page use 和 6.27% 的 wire redundancy。这些事实不矛盾，并说明“部分 Prefix 经常被碰到”与“整链复制成本得到充分利用”是不同问题。

## 自动与独立验证

构建期 `figure_validation.json` 和独立只读 `independent_validation.json` 均为 `FINAL_FIGURES_PASS`。独立审计逐项确认：

1. 所有输入文件都位于 canonical `final_results` 根目录，且 SHA-256 与冻结 consolidation manifest 一致。
2. 50 条 canonical long rows 全部 `validation_status=PASS`，不存在 pilot、smoke、INVALID 或 superseded source。
3. Figure 2 的 Saved delta 和 waste percentage、Figure 3 的 Saved/Weighted delta、Figure 8 的 Saved/eviction percentage 均可从 raw values 重算。
4. Figure 3 的 Oracle Saved delta 精确重算为 56.82% 与 7.05%。
5. Persistence 完整包含 `h={60,300}`、`K={5,10,20}`；Oracle-W 完整包含 `W={1,5,30}` 且 cohort 唯一为 `W_SENS_10_25`。
6. N 图只有 `N={2,4}`，每 Pod 容量恒为 585 pages，总容量按 1170/2340 pages 改变。
7. COPY/MOVE 四组配对完整，MOVE release 的 ZERO/PARTIAL/FULL 数量守恒且百分比合计为 100%。
8. Line 1/2 的 waste `NA` 在 figure data 中仍为 `NA`，没有转换为 0。
9. 16 个 figure-data CSV、16 个 PNG、16 个 PDF、16 个 SVG 全部存在且逐文件 hash 与 manifest 一致。
10. 所有 PNG 的写入分辨率约为 320 dpi，满足不低于 300 dpi 的要求；视觉总览检查未发现裁剪、不可辨识标签或单位混轴。

## 交付物

- `results/task_main/final_figures/png/`：Markdown/PPT 图。
- `results/task_main/final_figures/pdf/`：论文与报告矢量图。
- `results/task_main/final_figures/svg/`：后续矢量编辑文件。
- `results/task_main/final_figures/figure_data/`：每张图实际使用的全部行和派生字段。
- `results/task_main/final_figures/figure_manifest.json`：输入、figure data 和三种输出的 SHA-256。
- `results/task_main/final_figures/figure_validation.json`：构建期检查。
- `results/task_main/final_figures/independent_validation.json`：从落盘产物重新读取后的独立检查。
- `results/task_main/final_figures/README.md`：复现命令和指标解释。

**FINAL_FIGURES_PASS。** 本轮到此停止；未撰写最终完整项目报告，未新增实验或策略，等待 Final Report 阶段指令。
