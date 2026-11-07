# QuantSage · Implementation and Stepwise Documentation

本仓库包含 Python 工作流实现和分步工程文档，用于在不规则金融时序数据上开展可复现研究。Step 01 工作流引擎、Step 02 国债期货 L2 接入、02-A 冻结源/gap 适配以及 03-A 基础 primitives 已实现并验证；下一步为 03-B，完整 Step 03 及后续仍未完成。真实基础结果见 [Step 03 RESULTS](steps/03_research_dimensions_and_primitives/RESULTS.md)。

## 首次使用

当前 Study / FeatureSet / Run 与 Step 02–05 闭环按 [分步执行规格](EXECUTION_SPEC.md) 的最新执行记录接续；01-A～01-G、02-A 与 03-A 已完成，不从头重做。每次只执行一个增量，配套阅读 [计算技术方案与执行验收手册](COMPUTATION_PLAN.md)。该规格区分已有阶段交付与本次管理/计算扩展，不把历史 COMPLETE 当作新扩展完成。

1. 在仓库根目录阅读 [AGENTS.md](../AGENTS.md) 和 [implementation status](IMPLEMENTATION_STATUS.md)。
2. 从 [STEP_INDEX.md](STEP_INDEX.md) 选择一个尚未实现的模块；Step 01 和 Step 02 已完成。
3. 按对应模块的 `SPEC.md`、`IMPLEMENT.md` 和 `VERIFY.md` 实施与验收。
4. 记录实际命令、结果和 artifacts；仅在验收通过后推进后续步骤。
5. 后续阶段可使用符合 [COMMON_CONTRACTS.md](COMMON_CONTRACTS.md) 的 fixtures 验证模块逻辑，不得以 fixtures 代替其要求的真实集成验收。

## 全局文档

- [分步执行规格](EXECUTION_SPEC.md)：需求约束、14 个顺序增量、逐项验收、A/B/C 交付与可复制执行提示。
- [计算技术方案与执行验收手册](COMPUTATION_PLAN.md)：当前实现接续点、Step 02–05 计算规则、接口演进、数值验收和 subagent 工作单。
- [实施设计](IMPLEMENTATION_DESIGN.md)：DuckDB 存储、配置解析、范围依赖、执行恢复、接口及分阶段验收。

- [领域模型关键字段](domain.detail.md)：四个核心模型与三个辅助记录的字段、版本、状态和引用约束。

- [领域模型科学评审](DOMAIN_MODEL_REVIEW.md)：后续科学验证阶段的参考，非当前工具前置要求。
- [领域术语](../CONTEXT.md) 与 [领域模型及关系](DOMAIN_MODEL.md)：以 Study / Feature / FeatureSet / Run 管理目标、组合配置、渐进执行和评估，为发现模块准备结果。
- [STEP_INDEX.md](STEP_INDEX.md)：每个阶段的独立实现、验证与规格入口
- [AGENTS.md](AGENTS.md)：Codex 约束，分步提交
- [ARCHITECTURE.md](ARCHITECTURE.md)：项目意图、技术范围
- [COMMON_CONTRACTS.md](COMMON_CONTRACTS.md)：schema / 时间 / 可用性 / 防泄漏契约
- [WORKFLOW_STAGES.md](WORKFLOW_STAGES.md)：DAG、stage 间数据流
- [BUILD_ORDER_AND_ACCEPTANCE.md](BUILD_ORDER_AND_ACCEPTANCE.md)：整体里程碑及全流程验收
- [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md)：每次完成后准确更新
- [STEP_IMPLEMENTATION_TRACKER.md](STEP_IMPLEMENTATION_TRACKER.md)：阶段完成情况与证据索引
- [examples/quant_discovery.yaml](examples/quant_discovery.yaml)：Step 02–05 管理模型配置示例，**非当前 CLI schema 或已运行生产配置**

## Step 03–05 的职责

- [Step 03 Primitives](steps/03_research_dimensions_and_primitives/SPEC.md)：可复用基础序列与有效性/血缘；私有中间节点不必全部落盘。
- [Step 04 Business Features](steps/04_business_features/SPEC.md)：按业务公式计算 lookback windows，保留各 family 合法参数组合；原始 [L2 spec](references/gov_bond_l2_feature_spec.md) 不变。
- [Step 05 Statistical Features](steps/05_statistical_features/SPEC.md)：在选定业务序列或 primitives 上计算所选 catch22/tsfeatures，交付质量、coverage 与 FeatureResult；Atlas/条件 baseline 延期，供后续发现模块按需消费。

业务 lookback、统计 profile window 与未来 impact horizon 分开；上述三步仍为 NOT_STARTED，本次文档调整不代表实现完成。

## 数据字段参考

- [数据位置与已核验访问方式](data.md)：国债期货 L2 Parquet、交易日历、Python / DuckDB 与 LAN API 访问示例。

Parquet 行情文件仅保存在本地，不纳入 Git。复现样本核验前，需从已核验数据位置准备本地文件 `docs/references/part-0.parquet`；文件 SHA-256 以 schema JSON 和核验结果中的记录为准。

- [国债期货 L2 Schema](references/gov_bond_l2.schema.md)：Word 62 个业务字段与实际样例 66 列的映射、类型与使用限制。
- [机读 Schema](references/gov_bond_l2.schema.json)：字段顺序、物理类型、nullable、来源及文件 SHA-256。
- 本地 `part-0.parquet` 样本是 3,178,989 行的逐条五档 L2 快照，附带 4 个导入元数据字段；[样本核验结果](references/part-0.profile.json) 保存复现脚本、统计与 hash。配置示例按实际全大写字段映射；本次完成的日历与会话证据和全量接入限制见 Step 02 [结果](steps/02_data_onboarding/RESULTS.md)。

- [国债期货 POC 接入交付清单](steps/02_data_onboarding/DELIVERY.md)：复用源 schema，定义下游入口、行情/隔离表和冻结日历/会话规则。
- [Step 02 实施结果](steps/02_data_onboarding/RESULTS.md)：全 79 个月的产物、独立 consumer 验收、复现配置和研究限制。

## 最小验证流程

对于每个未完成模块：先跑该模块的 unit tests，再跑 stage integration tests；若模块读取时间序列，还需验证排序、会话、可用时间和防泄漏约束。Step 01/02 的真实测试及 CLI/全量验收证据已分别记录在实现文档中；未实现阶段不得标记为已测试。

Step 01 工作流引擎和 Step 02 数据接入的历史交付已完成。当前按执行 spec 接续管理扩展与 Step 02–05 计算闭环；新扩展和后续阶段的状态分别以最新执行记录为准，本文不作为验收通过证据。
