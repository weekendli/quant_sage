# QuantSage · Codex Stepwise Implementation Package

这是 **Markdown 工程规格**（没有实现代码），目的是让 Codex 按模块逐个实现和验证约 30GB 不规则期货行情上的 Data Understanding → Hypothesis Discovery → Falsification 流程。

## 首次使用

1. 将本目录整体复制到代码仓库，确保 `AGENTS.md` 在仓库可见。
2. 从 [STEP_INDEX.md](STEP_INDEX.md) 打开一个模块（推荐先 Step 01）。
3. 向 Codex 提交该模块的 `IMPLEMENT.md` 指令；它会阅读 `SPEC.md` 和 `VERIFY.md`，只实现该模块。
4. 验证 `VERIFY.md` 每条要求并记录真实命令、结果与 artifact；只有通过后推进下一步。
5. 遇到上游尚未完成，可用遵守 `COMMON_CONTRACTS.md` 的 fixture artifacts **独立验证模块逻辑**，但不可声称全链路集成通过。

## 全局文档

- [STEP_INDEX.md](STEP_INDEX.md)：每个阶段的独立实现、验证与规格入口
- [AGENTS.md](AGENTS.md)：Codex 约束，分步提交
- [ARCHITECTURE.md](ARCHITECTURE.md)：项目意图、技术范围
- [COMMON_CONTRACTS.md](COMMON_CONTRACTS.md)：schema / 时间 / 可用性 / 防泄漏契约
- [WORKFLOW_STAGES.md](WORKFLOW_STAGES.md)：DAG、stage 间数据流
- [BUILD_ORDER_AND_ACCEPTANCE.md](BUILD_ORDER_AND_ACCEPTANCE.md)：整体里程碑及全流程验收
- [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md)：每次完成后准确更新
- [examples/quant_discovery.yaml](examples/quant_discovery.yaml)：目标配置示例，**非已运行生产配置**

## 数据字段参考

Parquet 行情文件仅保存在本地，不纳入 Git。复现样本核验前，需在本地准备 `docs/references/part-0.parquet`；文件 SHA-256 以 schema JSON 和核验结果中的记录为准。

- [国债期货 L2 Schema](references/gov_bond_l2.schema.md)：Word 62 个业务字段与实际样例 66 列的映射、类型与使用限制。
- [机读 Schema](references/gov_bond_l2.schema.json)：字段顺序、物理类型、nullable、来源及文件 SHA-256。
- [part-0.parquet](references/part-0.parquet) 是 3,178,989 行的逐条五档 L2 快照，附带 4 个导入元数据字段；[样本核验结果](references/part-0.profile.json) 保存 复现脚本、统计与 hash。配置示例按实际全大写字段映射，仍需确认日历等语义。

- [国债期货 POC 接入交付清单](steps/02_data_onboarding/DELIVERY.md)：复用源 schema，定义下游入口、行情/隔离表和 Marketdata 日历快照。

## 最小验证流程

对于每个模块：先跑该模块的 unit tests，再跑使用小型 Parquet fixture 的 stage integration test；如涉 timely data，再跑 prefix-invariance / session / stale quote / holdout tests。精确命令由 Codex 在实现后给出，**本包不假装已有任何测试或 CLI 实现**。

推荐先完成：01 工作流引擎 → 02 数据接入 → 03 Primitives → 04 基准统计。接着 05 Feature Grammar → 06 Event → 07 Regime → 08 Phenomenon → 09 Hypothesis → 10 Cube → 11 Falsification → 12 Planner → 13 Integration。
