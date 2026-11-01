# TaskMain Git Finalize

提交前检查：PASS。本文在提交前生成；实际 commit SHA 和 push 核查结果由本轮终端与最终回复给出，文档不在提交后回填。

- branch：`main`
- previous HEAD：`ec204c892bb494e54db8a0be81b5d4a775361daf`
- final commit SHA：`TO_BE_FILLED_AFTER_COMMIT`
- commit message：`finalize TaskMain v1 and remove legacy experiments`
- staged counts（关闭 rename 推断，含本文）：新增 **176**，修改 **0**，删除 **186**。
- pytest：**303 passed / 0 failed / 0 skipped / 0 errors**，13.14 秒，exit code=0。
- final artifacts：**105/105 存在且 SHA256 匹配**，仅验证，未暂存。
- dependency smoke：**PASS**；69 个当前模块导入成功，64 个配置输入路径及 15 个静态文件路径存在；TaskMain / D1 / D2 audit 全部 PASS。
- push target：`origin/main`（`git@github.com:hhr2449/llm-kv-active-balancing.git`），使用普通 push。
- 提交前远端 `main` 指向 previous HEAD；push 结果待提交后核查。

本次仅纳入当前正式 TaskMain source/configs/scripts/tests/protocol/analysis docs、两份最终 cleanup report、本文和已经确认的 legacy 删除。未暂存 results/data/checkpoints、归档或生成缓存。staged 最大文件为 `scripts/task_main_final/consolidate.py`，79,877 bytes；无超过 20 MiB 的文件。

以下 6 个历史 dirty shared files 保留工作区修改，未暂存、未恢复、未改写：

- `src/simulator/config.py`
- `src/simulator/engine.py`
- `src/simulator/metrics.py`
- `src/simulator/oracle.py`
- `src/simulator/pressure.py`
- `src/simulator/strategies.py`

验证在当前工作区执行；这些历史 dirty 文件不属于本次提交内容。没有重新运行正式实验、生成最终图表或修改 TaskMain 协议。
