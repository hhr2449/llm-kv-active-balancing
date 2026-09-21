# Stage A 单输入入口

从仓库根目录用 `python -m scripts.task_main.run_stage_a --config <yaml> --trace <jsonl> --output <新目录>` 调用。
输出 request、transfer、opportunity、event JSONL 和 summary/validation JSON；输出目录必须不存在。

R_AFF 与 R_REQ_KV_TASK 均可端到端运行。Reactive 先判断 gate；成立后按 FULL_PAGE_ONLY Prefix 判断直接转 target 或 COPY，失败 fallback source。
`configs/task_main/stage_a_r_aff.yaml` 与 `stage_a_r_req_kv_task.yaml` 是 synthetic 配置，不是正式 workload 实验注册。
Stage A 最终验收允许单测通过后运行 correctness smoke：

```text
python -m scripts.task_main.run_stage_a_smoke --output results/task_main/stage_a_smoke
```

固定读取 Conversation/ToolAgent 的 `[0,300000ms)`，分别执行两条 baseline，每项重放两遍并比较全部 records/summary/validation SHA-256。输出目录必须全新；任一 invariant 或确定性检查失败立即终止。不调参数、不作策略性能比较。运行时安装 legacy import fail-fast guard。

## Stage C优化后Pilot（tmux）

从仓库根目录首次启动；该命令只运行pilot，不会进入formal：

```bash
tmux new-session -d -s taskmain-stage-c-opt "bash -lc 'set -o pipefail; cd /root/data2/llm-kv-active-balancing && python -u -m scripts.task_main.run_pilot --output results/task_main/stage_c_pilot/run_20260917_04 --workers 10 --progress-interval 60 --request-progress-every 250 2>&1 | tee results/task_main/stage_c_pilot/run_20260917_04.console.log'"
```

查看控制台和机器可读进度：

```bash
tmux attach -t taskmain-stage-c-opt
tail -f results/task_main/stage_c_pilot/run_20260917_04.console.log
cat results/task_main/stage_c_pilot/run_20260917_04/progress.json
```

每个完整单遍在`checkpoint.json`中按12个文件SHA256登记。若进程中断且源码、配置没有变化，用新tmux session恢复；已验证的单遍不会重算：

```bash
tmux new-session -d -s taskmain-stage-c-opt-resume "bash -lc 'set -o pipefail; cd /root/data2/llm-kv-active-balancing && python -u -m scripts.task_main.run_pilot --resume --output results/task_main/stage_c_pilot/run_20260917_04 --workers 10 --progress-interval 60 --request-progress-every 250 2>&1 | tee -a results/task_main/stage_c_pilot/run_20260917_04.console.log'"
```

不要在同一目录同时启动首次运行和恢复运行。`validation_manifest.json=PASS`之后仍需执行独立artifact validation；不要据此自动启动正式14行。

## 当前正式方法与执行入口

正式 TaskMain v1 方法假设集中记录在 `docs/protocol/task_main_v1_method.md`，完整状态机契约位于 `docs/protocol/task_main_v1.md`。正式两 workload × 七行配置位于 `configs/task_main/formal/`，入口为 `python -m scripts.task_main.run_formal --formal-authorized`。该入口只可在单独明确授权后使用；Stage C Pilot 或 pre-formal PASS 不会自动启动正式实验。

当前 target 规则为 `FEASIBLE-TARGET-FIRST`：capacity probe 无副作用，只有选中 target commit plan；reactive gate 仍使用 absolute-lowest-load other Pod，empty-transferable 仍保留 source。每 external request completion 一个 opportunity，每 opportunity 最多成功启动一个 proactive COPY。
