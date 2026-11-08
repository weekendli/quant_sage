# QuantSage 独立验收 — 2026-11-08

**结论：当前 Step 02–05 ready 子集暂不通过最终验收；发现 2 项 P1 正确性缺陷和 1 项 P2 命名契约缺陷。现有测试及真实 A/B/C 主路径通过，但不能覆盖这些反例。完整 Step 01–13 也尚未完成，不能宣称全步骤通过。**

验收代码版本：`5c5dd17407448cea5225af9caf48443c713a40e3`。环境：Python 3.12.3、Ruff 0.16.1，使用既有 `/data/src/research/.venv/bin/python`。开始时工作树有既有未跟踪文件 `CFFEX`，本次未修改它。本次只新增验收材料，未修改实现、冻结行情、原研究数据库或既有完成状态。

## 1. 需求范围与验收口径

依据 `docs/ARCHITECTURE.md` 的“当前实施范围”、`docs/WORKFLOW_STAGES.md`、`docs/COMMON_CONTRACTS.md`、各阶段 SPEC/VERIFY，以及 `docs/BUILD_ORDER_AND_ACCEPTANCE.md`。

- 当前必需闭环：Study → FeatureSet → Run → FeatureResult → Review → 下一 Run；执行 Step 02–05 所选组合与依赖。
- 完整框架路线图：Events / Regimes / Phenomena / Hypotheses / Cube / Falsification / Planner。它们不作为本轮 ready 子集的隐含前置，但必须与“所有步骤已完成”区分。
- 目录支持、任务成功、结果有效、科学结论成立分别判断；`COMPLETE` 不等于数值契约正确。

| 阶段 | 本次判断 | 证据及边界 |
|---|---|---|
| 01 管理/引擎 | 已验证主路径通过 | 现有管理、冻结、计划、执行、RPC、缓存/恢复测试通过；真实 A/B/C 通过 |
| 02 数据源 | 消费路径通过；保留历史交付限制 | 当前冻结源可校验、注册、消费；未重跑 79 个月接入 |
| 03 primitives | 选定计算路径通过 | 现有 primitive 测试及真实所选 Run 通过；不等于所有目录项均完成真实数值验收 |
| 04 业务窗口 | **不通过** | 下述 F1/F2/F3；另有 F07/F08 聚合语义待明确 |
| 05 统计 | 已验证选定 trailing 主路径；不能全量签收 | catch22/R 后端测试、真实统计产物通过；上游窗口缺陷影响下游可靠性；segment-summary 仍未交付 |
| 06–12 | 未实现，无法验收 | 状态表为 NOT_STARTED；当前源码没有相应领域算法交付 |
| 13 集成 | 当前 A/B/C 路径通过，完整闭环未通过 | 尚无 Event→Planner 完整 DAG；VERIFY 中相关要求仍未满足 |

## 2. 必须修复的验收发现

### F1 · P1：已知行情缺口没有使受影响业务窗口失效

**需求：** `docs/steps/04_business_features/SPEC.md:148` 要求影响依赖的已知缺口使窗口 NA，重新积累完整 L 后恢复；`docs/IMPLEMENTATION_DESIGN.md` §6.4 明确隔离事件属于可追溯缺口，不能当作普通无更新。

**位置：** `quantsage_discovery/research/business_windows.py:252` 的 `_rolling_mean` 只根据保留的有效快照求均值，没有检查缺口；`quantsage_discovery/research/execution.py:838` 的业务分支没有把源 gap 事件传入窗口算子。Step 03 虽记录 gap lineage，后续数值没有据此失效。

**真实反例：** 冻结源 `cffex_treasury_l2_full79@2`，合约 `TS2606`，交易日 `2026-06-12`，上午 session。`01:34:00.800 UTC` 的记录因 `INVALID_L1_BOOK` 被隔离；`01:34:11.800 UTC` 的 1m OBI 窗口仍输出 `-0.7142857142857143`，`invalid_reason=null`。缺口位于该窗口内并直接影响盘口依赖。复现中读到 7 条 primitive 记录、2 条相关隔离事件，发现 1 个此类非空窗口。

- gap event：`767c1fecbf12259a834995654f3bb6753b04e8d5806f261a40824530a4c01592`
- 隔离记录：`5a17031a699f184f651d4eafcefdd65f58329739c48fd2eaa55b07164b5ad2f5`
- gap manifest hash：`sha256:1f9f7105ab2ab0e807f55d40d0dc65487af6a360970e4c9295cf985c02cb8333`

**影响：** 会把受源质量问题影响的窗口报告为有效，并可继续进入统计计算。现有 A/B/C 使用的单日样例不能证明此边界正确。

**修复验收标准：** 按受影响字段传播已知缺口到业务和统计窗口；给出机器可读原因，完整 L/W 恢复前保持失效。加入上述真实记录反例及有效快照不可估、普通无更新的区分测试；更改实现版本，避免继续复用旧错误结果。

### F2 · P1：以输出范围起点代替可用历史边界，产生错误 NA

**需求：** `docs/COMMON_CONTRACTS.md` 和 Step 04 时间契约要求按同 session 的授权历史计算窗口；Run 输出范围与依赖读取范围分别定义。planner 已展开历史，execution 也已读取，不能因输出起点改变就抛弃它。

**位置：** `quantsage_discovery/research/business_windows.py:266`，判断 `event_time - lookback < partition.start`。同类判断还出现在 `_flow_windows`、`_volume_depth_window`、`_mid_move_window`。`partition.start` 是输出范围起点，不是真实 session 起点。

**手算反例：** 同 session 输入秒数 `[0,20,40,60,80]`，值 `[0.8,0,0.4,0.6,-0.2]`，回看 1m，窗口 `(t-L,t]`。两次计算输入完全相同，只有输出起点从 0s 改成 60s。

| 共同输出时刻 | 宽范围正确值 | 缩窄范围实际值 |
|---|---:|---|
| 60s | 0.3333333333333334 | NA / HISTORY_INSUFFICIENT |
| 80s | 0.2666666666666668 | NA / HISTORY_INSUFFICIENT |

**影响：** 从 session 中途启动 Run，或统计依赖展开业务结果时，前 L 区间可被错误标为历史不足；相同输入和共同时间点出现不一致结果。

**修复验收标准：** 显式传递真实 session/history 边界，用输出范围只筛选发布时刻。为四类业务窗口增加起点变化的共同值不变性测试，并验证无授权历史时仍正确 NA；变更实现身份以使旧缓存失效。

### F3 · P2：目录名称把 d-space 写成 s-space

**需求：** `docs/steps/04_business_features/SPEC.md:130` 明确沿用原规格名称，例如 `obi__d0__h3s`。

**位置：** `quantsage_discovery/research/business_windows.py:194` 用 `key[0]` 构造 space 前缀，`space` 因而变成 `s`。

**反例：** `business_catalog_entries()` 的首个 F02 名称应为 `obi__d0__h1s`，实际为 `obi__s0__h1s`。432 项数量及唯一性测试均可通过，但没有断言正确名称。

**修复验收标准：** 按原规格生成名称，对 d0/d1/d2/d3/d5 及 size 组合逐项校验；若这些名称已被外部消费，需要说明兼容策略。

## 3. 本次实际运行的验证

| 检查 | 结果 |
|---|---|
| 全量 pytest | **314 passed，66.93s，无 skip** |
| 核心代码、tests/examples/scripts Ruff | 通过 |
| 新增复现脚本 Ruff | 通过 |
| `ruff check --no-cache .` | 未通过：参考脚本 `docs/references/inspect_l2_sample.py:31`、`:116` 两处 RUF046；不影响上述数值缺陷判断 |
| 主 CLI、research、onboarding help | 通过 |
| 通用示例 validate / plan | 命令通过，但 `stage_count=0`、空 DAG；不计为完整管线通过 |
| 全新工作区真实 A/B/C | A/B/C 全部 COMPLETE，Review/coverage/比较及 artifact 校验通过 |
| 显式空选择 | NO_WORK，0 work units |
| 选中待明确 F07 | BLOCKED / PENDING_SEMANTICS，0 结果 |
| 独立语义探针 | **F1/F2/F3 三个要求均失败**；脚本输出 `passed=false`，脚本正常完成不表示要求通过 |

首次受限沙箱测试为 `307 passed / 7 failed`；7 个失败都是 RPC owner 无法建立本地 Unix socket。允许本地 socket 后完整重跑得到上述 314/314 结果，没有将环境限制误报为实现缺陷。

主要命令（cwd 为仓库根目录）：

```bash
python -m pytest -q -o cache_dir=/tmp/quantsage-acceptance-20261108/pytest-cache
ruff check --no-cache quantsage_discovery tests examples scripts
ruff check --no-cache .
python -m quantsage_discovery.cli --help
python -m quantsage_discovery.cli research --help
python -m quantsage_discovery.onboarding --help
python -m quantsage_discovery.cli validate --config docs/examples/quant_discovery.yaml
python -m quantsage_discovery.cli plan --config docs/examples/quant_discovery.yaml
PYTHONPATH=. python docs/acceptance/2026-11-08/probe_semantics.py
```

## 4. 隔离真实数据验证

工作区 `/tmp/quantsage-acceptance-20261108` 使用新 DuckDB 和新产物目录，运行结束已优雅关闭 owner。底层行情、gap、calendar、split 及 hash 沿用冻结源；为在新库正常注册首个版本，创建只改变 source id/version 和对应 manifest identity 的别名 `acceptance_cffex_full79@1`。未修改原源文件或原库；源内容 hash 保持 `sha256:73acc246fc198e1a32f3184a445e075af347954cba5f98769e8322e51299a949`。

原历史 v1 source 无法通过现行 gap schema 校验，因此没有通过降级校验导入它。别名通过当前 `sources.register` 校验后，冷启动计算并验证复用。原 `@2` 另用于 F1 的直接真实数据反例。

```bash
python -m quantsage_discovery.cli research owner serve \
  --database /tmp/quantsage-acceptance-20261108/registry.duckdb \
  --socket /tmp/quantsage-acceptance-20261108/rpc/owner.sock
python -m quantsage_discovery.cli research sources register \
  --database /tmp/quantsage-acceptance-20261108/registry.duckdb \
  --socket /tmp/quantsage-acceptance-20261108/rpc/owner.sock \
  --input /tmp/quantsage-acceptance-20261108/source.json
python -m examples.research_end_to_end \
  --database /tmp/quantsage-acceptance-20261108/registry.duckdb \
  --socket /tmp/quantsage-acceptance-20261108/rpc/owner.sock \
  --source-ref acceptance_cffex_full79@1 --run-prefix acceptance-20261108 \
  --report /tmp/quantsage-acceptance-20261108/real-abc.json
```

| Run | ID | 实测状态 |
|---|---|---|
| A | `run_531c180d72b348a895f996fdec73e7fb` | COMPLETE；冷启动小范围 |
| B | `run_768e68ee902549889daad8d32e1e4f97` | COMPLETE；CACHED=4、SUCCEEDED=6、SKIPPED=2 |
| C | `run_d177b9b9067d42538fc0867f8785bafa` | COMPLETE；CACHED=8、SUCCEEDED=2、SKIPPED=4 |
| pending | `run_4af1c013422c4069ab649d35714709f1` | BLOCKED / PENDING_SEMANTICS |

范围是 T2603 / 2026-01-05 上午扩至同日下午，再增加 spectral entropy；**此例不是跨交易日扩展测试**。现有 planner/cache 测试覆盖分区扩展；本次未新增多日真实实验。

C 关联的 10 个 requested FeatureResult 均通过 fresh artifact verification，且各有非空有效值。两个业务特征的上午有效数为 `12195/12255`、`12136/12255`，下午为 `11959/12018`、`11905/12018`；catch22 和新增 spectral entropy 各为上午 `25/115`、下午 `25/130`。其余窗口保留 NA 原因，不将执行完成解释为全部数值有效。全部真实结果仍为 `research_only`。

## 5. 未覆盖与保留项

- 不重新接入或全量重哈希 79 个月原始行情；本次真实读取为有界分区，历史全量接入证据见 Step 02 RESULTS。
- 未重跑 30GB 性能/峰值内存基准，不据此宣称性能验收通过。现有 benchmark 脚本与分区策略仅作静态核对。
- 用户 tick/unit metadata 缺失，tick-dependent 全量真实数值验证未完成；未编造 metadata。
- F07/F08 聚合、retrospective segment-summary、Step 06–12 完整研究闭环未交付；后者属于当前范围之外的路线图。
- 未验证独立 final holdout 科学推断或交易价值；当前结果为研究描述。
- README 中“stops at Step 02”、WORKFLOW_STAGES 中旧 NOT_STARTED 段落以及 AGENTS 中“只有文档”的表述已过时，需要与实际状态对齐。

## 6. 复验入口与证据

修复 F1/F2/F3 后，先复跑 [probe_semantics.py](probe_semantics.py)；三项必须 `passed=true`，再跑全量测试及冷启动 A/B/C。补充修复后的具体真实边界结果，不能仅引用旧 314 项测试通过。

本目录保存 [semantic-probes.json](semantic-probes.json)、[evidence.json](evidence.json)、[real-abc.json](real-abc.json)、[pytest.txt](pytest.txt) 和 [ruff.txt](ruff.txt)。`evidence.json` 包含按结果记录的有效计数、health、source/input refs、artifact 哈希及本次环境信息。完整临时计算产物保留在上述 `/tmp` 工作区，其长期可访问性不作保证；此处摘要可持久审阅，真实数据复现仍依赖机器上的冻结行情。
