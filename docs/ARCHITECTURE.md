# Step 00 — Codex Master Brief

## 你要构建什么

实现 Python 项目 `quantsage_discovery`：一个可恢复、可扩展、支持 30GB+ 不规则金融时间序列的经验规律发现框架。

**产品闭环：**

`Raw Dataset → Data Passport → Primitives → Business Features / Time Windows → Statistical Features / Atlas / Baselines → Events & Regimes → Phenomena → Hypotheses → Discovery Cube → Falsification → Knowledge Registry / Next Experiment`

不要把它误实现为传统技术指标批量生产器，或只输出回测收益的 AutoML 平台。首先建设 Market Observatory，在没有未来收益标签的情况下发现有解释价值的统计现象；随后才引入预测目标做条件搜索和验证。

## 当前实施范围

优先交付 Step 02–05 的 Study / Feature / FeatureSet / Run 数据与特征探索闭环；字段见 [domain.detail.md](domain.detail.md)，执行范围见 [WORKFLOW_STAGES.md](WORKFLOW_STAGES.md)。Step 02 复用已冻结 onboarding 数据；Step 03–05 按显式组合计算和复用 FeatureResult，人工/agent 追加 Review 后推进。以下完整产品闭环是后续路线图，完整 Atlas、条件 baseline、events/regimes/phenomena 不作为当前画像 Run 的前置。实现仍逐个 stage 验收。

## 技术默认

- Python 3.11+；优先 `polars` lazy/streaming、`duckdb` 查询 Parquet、`pyarrow` 处理 schema；`numpy/scipy/statsmodels/scikit-learn` 用于统计；`ruptures` 可作为可选变化点组件。
- 数据以 Parquet 按真实合约/交易日分区存储，不要求一次性读入 30GB。元数据、运行与结果索引统一使用 DuckDB；算法产物保存为 Parquet + JSON/YAML manifest。单 owner 进程及恢复设计见 [IMPLEMENTATION_DESIGN.md](IMPLEMENTATION_DESIGN.md)。
- 工作流引擎 v1 使用本地 DAG + 进程内/进程池执行，**不必先部署 Airflow/Prefect/K8s**。预留执行器接口，但不要引入不必要基础设施。
- CLI 使用 `typer` 或 `argparse`；结构化配置 YAML + Pydantic schema；测试使用 pytest；日志 JSON/结构化日志。
- 代码应有明确包边界，`core/` 不依赖任何具体期货合约；市场特定逻辑通过 schema mapping、calendar 与 adapters 注入。

## 需要实现的主要能力

1. **Workflow Engine**：有向无环任务、缓存、幂等、重跑、检查点、失败恢复、血缘、任务状态。
2. **Dataset Onboarding**：原始合约/行情接入、字段映射、交易时段、质量检查、契约完整性审计。
3. **Research Dimensions & Primitives**：自动识别与人工审定的变量族、时间尺度、跨合约依赖；按时间基准构造基础可观测量。
4. **Business Features / Time Windows**：从 primitives 计算固定业务公式、合法 space/size/lookback 组合与时间窗口，注册、校验和持久化；typed AST 可作为内部表达，不依赖统计阶段。
5. **Statistical Features / Atlas / Baselines**：对选定业务特征及 primitives 计算原单位描述、catch22/tsfeatures、正常条件分布、异常度和参考对照；区分 trailing 与事后 summary。
6. **Event Detector**：极端、预测残差、背离、变化点事件，去重、冷却、可审计。
7. **Regime Detector**：规则状态、聚类/HMM 可选、线上过滤概率和状态稳定性评估。
8. **Phenomenon Discovery**：发现并登记值得解释的现象，保留来源和反例。
9. **Hypothesis Generators**：六种生成方法，输出竞争解释和事前反证规则。
10. **Discovery Cube**：预算化选择 F×C×H 候选单元，非暴力穷举。
11. **Falsification**：多重检验、区块重采样、错位/安慰剂检验、Walk-forward、独立 Holdout。
12. **Research Planner**：优先执行能排除竞争解释的实验，生成结构化研究摘要。

## 全局不可妥协的规则

- **Point-in-time**：每条特征、状态、标签都要有 `event_time` 和 `available_time` 语义；生产研究结果不得看未来。
- **Time semantics**：时钟时间、事件时间、成交量/活跃度时间是不同轴；不规则更新不能直接按“过去100行 = 过去100秒”。
- **Sessions**：交易日、日内间歇、夜盘（如适用）、合约切换不能被隐式连续处理；模型必须明确会话边界。
- **Snapshot ≠ trade log**：仅凭 LOB 快照无法准确重建逐笔成交、撤单和真实订单流；只能计算可识别的快照代理量。
- **No leakage**：拟合标准化器、分位阈值、regime 模型、特征选择、假设修正不能看 final holdout。
- **Auditability**：每个被保留的现象/假设都能追溯到输入字段、参数、数据区间、版本、代码提交、统计证据和失败的测试。
- **Honest results**：不虚构数据统计结果、不将示例数值当结论；不把显著性等同于可交易性。

## 推荐包结构（可在实现时合理调整）

```text
quantsage_discovery/
  cli.py
  config.py
  core/{types,workflow,artifacts,registry,calendar,split}.py
  ingest/{readers,schema,quality,align}.py
  primitives/{catalog,builders}.py
  features/{catalog,windows,compiler}.py
  atlas/{fingerprint,baselines,dependencies}.py
  detectors/{events,regimes,phenomena}.py
  hypotheses/{generators,models}.py
  discovery/{cube,search}.py
  validation/{tests,bootstrap,multiple_testing}.py
  planner/{experiments,ranking}.py
  reports/{dataset_passport,research_summary}.py
  tests/{unit,integration,leakage,synthetic}/
examples/quant_discovery.yaml
```

## Codex 工作方式

每次只按对应步骤的验收标准提交一个可运行增量。先阅读 `COMMON_CONTRACTS.md` 和 `WORKFLOW_STAGES.md`，再实现当前 Step；不能只交付接口壳。每次结束提供：文件变化、执行命令、测试结果、已知限制、下一步依赖；维护 `IMPLEMENTATION_STATUS.md`。

**开始执行的建议 Prompt：**

> 阅读 `README.md`、`ARCHITECTURE.md`、`COMMON_CONTRACTS.md` 与 `BUILD_ORDER_AND_ACCEPTANCE.md`。先只实现 Milestone M0/M1（项目基础、数据读取与质量、工作流最小闭环），确保 CLI 可以跑通合成 Parquet 数据，执行 pytest 并报告结果。随后逐步实现其他 Step，不要一次性生成不可运行的大量代码。

## 完工定义

运行完整管线后，用户至少可以列出：Data Passport、基本时间结构、Primitives、Statistical Fingerprints、Conditional Baselines、Feature Registry、Event/Regime Registry、Phenomenon Cards、Hypothesis Cards、Cube Experiments、Falsification Results 与推荐后续实验；每项可从 manifest 追踪和重现。
