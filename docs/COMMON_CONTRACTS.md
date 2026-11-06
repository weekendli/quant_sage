# Step 14 — Common Contracts / Non-negotiable Semantics

## 目的

所有独立模块必须对**时间、字段、数据集拆分、产物引用和统计独立性**使用相同术语。本文件是实现的共同约束；模块文档冲突时，应先澄清，不得偷偷改变契约。

## 原始数据字段与标准化字段

以 [L2 Schema](references/gov_bond_l2.schema.md) 及其 [JSON](references/gov_bond_l2.schema.json) 为字段依据。本文件的 snake_case 名称属于标准化层，不能假定源文件已有同名列。Word 描述 62 个业务字段；当前 `part-0.parquet` 为 3,178,989 行、66 列的逐条 L2 快照，包含全部五档盘口和 4 个导入元数据字段。业务列实际为全大写；类型、精度及 nullable 以 Parquet 为准。`TRADINGTIME` 无时区，`UNIX` 用于事件时间校验；`source_row` 是来源行号，`ingest_run_id` 是上游导入批次，均不能当作交易所序号或逐条 `available_time`。样本核验脚本与结果见 [profile](references/part-0.profile.json)。

## A. 时间契约

| 字段 | 含义 | 用途 |
|---|---|---|
| `event_time` | 源市场事件时间 | 原始行情排序与时序统计 |
| `ingested_time` | 本平台实际接收/写入时间（如果有） | 数据延迟与回放 |
| `available_time` | **研究/模型当时最早可以知道该值的时间** | 防泄漏 point-in-time joins |
| `decision_time` | 某候选信号包含的全部输入均已可用的时刻 | Horizon 起点 |
| `detected_time` | 检测器实际可以宣布一个事件/状态的时刻 | 事件可交易性 |
| `trade_date` | 交易所语义交易日 | Partition、session、split |
| `session_id` | 连续交易会话标识 | 禁止不合理跨会话窗口 |

所有标准化时间存储为有时区语义的 timestamp（推荐 Arrow UTC 存储、展示时明确交易所时区）。**输入没有 `available_time` 时，不可无证据地断言它等于 `event_time`**；按配置选 `research_only` 或使用明确延迟假设并标记风险。

跨源 join：要求双方在 decision_time 之前可用，使用 as-of、staleness 与缺失标志；不可用后见之明配对。

## B. 数据拆分与训练契约

```yaml
split_id: default_v1
method: chronological
train: {start: '2024-01-01', end: '2024-06-30'}
development: {start: '2024-07-01', end: '2024-09-30'}
final_holdout: {start: '2024-10-01', end: '2024-12-31'}
label_max_horizon: 30m
purge_and_embargo: enabled
```

以上日期为示例；实际数据跨度未知，不能照搬。Train 可用于拟合基准/特征；Discovery 允许探索但全部测试计数；Development 允许有限选择；Holdout 只能评估冻结 candidate。区间语义和使用权限在 SplitManifest 中定义，purge 长度须覆盖依赖与标签窗口及事件跨界影响。

## C. ArtifactManifest（必需）

```json
{
  "artifact_id": "art_<content_hash>",
  "artifact_type": "feature_table",
  "schema_version": "1.0",
  "uri": "file:///.../features/instrument=T/date=.../part.parquet",
  "content_hash": "sha256:...",
  "run_id": "run_...",
  "stage_name": "features",
  "stage_version": "1.0",
  "config_hash": "sha256:...",
  "code_revision": "gitsha-or-version",
  "input_artifact_ids": ["art_..."],
  "data_snapshot_id": "snapshot_...",
  "split_id": "default_v1",
  "time_min": "...",
  "time_max": "...",
  "row_count": 0,
  "quality_flags": []
}
```

所有 ID 是稳定引用，未实现时不能在报告里填假数据；大数据产物 URI 必须可访问且内容 hash 可验证。数值表存 Parquet，目标管理数据库为 DuckDB；不要把 30GB 数值表存入元数据 JSON/blob。数据库并发约束见 [实施设计](IMPLEMENTATION_DESIGN.md)。

Step 02–05 领域产物按 `(exchange, instrument_id, trade_date)` 分区，session 独立约束窗口。用户补充的行情 metadata（含历史 tick 规则）按精确版本/hash 引用，进入源绑定和计算身份，不改写冻结行情。成交量未验证及累计量/增量对账不一致仅警告，真实缺口、reset 和非法值仍按各指标契约处理。默认 trailing 不拼接缺口两端；缺少行情的推荐机制见实施设计 §6.4。

## D. Registry / Experiment Ledger

以下是后续完整研究领域的逻辑登记类别，不要求 Step 02–05 提前创建独立实体表。当前用 Study/Feature/FeatureSet/Run 与 DataSource/FeatureResult/Review，复用既有 artifact/registry；按阶段需要映射这些类别：`datasets`, `runs`, `stage_runs`, `artifacts`, `primitives`, `features`, `events`, `regimes`, `phenomena`, `hypotheses`, `cube_cells`, `experiments`, `results`, `knowledge_edges`。

其中每个候选应有 `source_refs`, `schema_version`, `config`, `created_in_run`, `available_from`, `status`, `notes`。`experiment` 还保存 `registered_before_test`, `search_family_id`, `split_id`, `time_range`, `n_trials_in_family`。

## E. Sample Units / Statistical Contracts

- 本次 L2 输入的原始观察单位 `quote snapshot observation`（是否仅在变化时发布仍待确认）；事件研究单位 `event episode`；更稳定的独立性评价单位通常是 `trading day / independent time block`。
- 所有指标注明分母；bootstrap/block design 要以依赖结构选择，而不是按行情逐行重抽样。
- 任意模型输出不应只保存 point estimate；至少在实验报告中提供估计的不确定性、有效样本数、基准差异和搜索次数。
- 测试之前先定义要测量的 outcome 和 effect；如果执行后改动，创建新版本，不隐式覆盖。

## F. Contract Roll / TF-T-TL

- 默认按**真实合约**分别计算，展期/主力合约选择作为单独 transformation，使用当时可获得的数据。
- 不同久期国债期货不可直接把价差称为套利价差；cross-contract feature 需记录缩放选择和理由。
- 缺现券、收益率曲线和 Repo 时，禁用需要这些外部输入的现金期货基差/持有成本估算。

## G. 错误语义

`MISSING_FIELD | INSUFFICIENT_HISTORY | INVALID_QUOTE | STALE_DATA | CROSS_SESSION | UNKNOWN_AVAILABILITY | LEAKAGE_RISK | NO_CONTROL_MATCH | INSUFFICIENT_BLOCKS | TEST_BUDGET_EXCEEDED | HOLDOUT_ALREADY_USED`。

Error code 与 warning 必须机读并可计数；不能用空值掩盖不可用原因。

## H. 版本与可复现

固定 random seed、算法版本、输入数据 snapshot/hash、时区、日历、Python/库版本、配置。明确浮点顺序/并行可能造成的容许误差；算法不确定性与数值不确定性分开记录。

## 验收

全流程的每个产物都通过 schema 验证；任何 Feature/Event/Regime/Hypothesis 的 lineage 可以追到原始数据 snapshot；`available_time` 漏检在 CI 中应使测试失败。
