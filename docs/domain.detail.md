# 领域模型关键字段

实现方案见 [IMPLEMENTATION_DESIGN.md](IMPLEMENTATION_DESIGN.md)：统一 DuckDB 元数据/运行/结果索引，Parquet 存数值。


配套 [简化领域模型](DOMAIN_MODEL.md)。四个核心对象：Study、Feature、FeatureSet、Run；三个辅助记录：DataSource、FeatureResult、Review。本文为目标字段契约，非现有 Python/数据库 schema，也不是当前 CLI 可直接读取的格式。

## 通用约定

- `ref` 是精确版本引用，例如 `liquidity_profile@2`，禁止在冻结 Run 中引用 `latest`。Feature 的名称/alias 不决定计算身份。
- 时间使用带时区 timestamp；范围为 `[start, end)`，按真实合约和 session 表达。`scope` 是内嵌结构，不再建独立领域模型。
- `actor` 为 `{kind: human|agent, id: ...}`。所有记录有 `schema_version`、`created_at`、`created_by`。
- Study 用 `revision` 做乐观并发控制并保留历史；Feature、FeatureSet、DataSource 版本发布后内容不可改。Run 的配置/计划冻结，状态/attempts 追加更新；FeatureResult 内容不可改，健康检查可追加；Review 只追加。
- 表中“条件”表示对应 kind/功能启用时必填；“可选”可省略。大清单/数组可放 artifact，以带 hash 的引用读取，不复制进数据库。

## 1. Study：目标与研究边界

| 字段 | 类型 / 必需性 | 含义 |
|---|---|---|
| study_id | string / 必填 | 稳定研究 ID |
| revision | integer / 必填 | 每次编辑递增；更新需提交 expected_revision |
| name、objective | string / 必填 | 名称与研究目标，说明希望观察什么 |
| status | enum / 必填 | active / paused / closed；不删除历史 |
| source_refs | ref[] / 必填 | 允许使用的数据源版本 |
| feature_set_refs | ref[] / 必填，可空 | 已登记候选组合集的明确版本，可包含暂未运行的集合 |
| backlog | object[] / 必填，可空 | `{feature_set_ref, item_id, priority, state, reason}`；state=active/parked/retired |
| default_feature_set_ref | ref / 可选 | 新 Run 的默认配置集，必须在登记清单中 |
| default_scope | scope / 可选 | 默认合约、输出时间区间及 session 过滤 |
| limits | object / 必填 | allowed_steps、允许的数据范围、资源预算；未配置的访问不隐式授权 |
| evaluation_guide | object / 可选 | 人工/agent 的关注点；评分时带规则版本和指标定义 |
| notes、tags | string / string[] / 可选 | 尚未形成可执行组合的想法、分类 |
| updated_at、change_reason | timestamp / string / 必填 | 本 revision 的时间与修改原因 |

Study 的 feature_set_refs 是配置集目录；backlog 是有限待探索条目子集。指标组合加入待办需引用已冻结 FeatureSet 的 item；新版本不自动替换旧条目，可明确迁移。跨集合的同一定义在覆盖查询中去重，保留各条目显示名。纯想法先放 notes，不纳入“未跑”计数。

## 2. Feature：具体计算定义

| 字段 | 类型 / 必需性 | 含义 |
|---|---|---|
| feature_id、version | string / 必填 | 稳定定义 ID 与不可变版本 |
| name、description | string / 必填 | 可读名称与业务/统计含义 |
| step、kind | integer / enum / 必填 | 3=primitive、4=business、5=statistical，映射必须一致 |
| family | string / 必填 | 公式族或统计方法族，如 OBI、catch22 |
| formula_ref | versioned ref / 必填 | 数学公式及端点、聚合规则的确定版本 |
| implementation | object / 必填 | 执行器 key、算法/后端版本；实际代码指纹另随 Run 记录 |
| parameters | object / 必填，可空 | 当前具体组合的参数；不允许以参数数组代表隐式笛卡尔积 |
| input_feature_refs | ref[] / 必填，可空 | 精确上游定义；必须无循环 |
| source_requirements | object / 条件 | 源字段/类型/单位要求；绑定数据版本由 Run 完成 |
| time_policy | object / 必填 | lookback/profile_window、窗口端点、trailing/segment_summary、session/availability 规则 |
| sampling | object / 条件 | clock-time/event-time、interval、evaluation_stride、as-of/max_age、缺口/重采样规则 |
| validity | object / 必填 | min_samples、缺失/零分母/stale 处理、支持的数据条件及 reason codes |
| output_schema、unit | object / string / 必填 | 输出类型/命名/单位，统计量可为 dimensionless |
| definition_hash | string / 必填，生成 | 规范化语义、参数、依赖、算法和时间规则的指纹 |
| status、reason | enum / string | ready / pending_semantics / retired；非 ready 时记录原因 |

parameters 按 kind 校验：primitive 为基础量参数；business 为 family 合法的 space/size 等；statistical 为 metric 与其参数。`lookback` 属业务窗口 L，`profile_window` 属统计窗口 W；未来 `impact_horizon` 不属于 Step 03–05 输入配置。`size` 必须带业务含义和单位，如档位数与目标数量不可仅用裸数字互换。不适用的 size 可省略；需要该参数的公式不能用 null 规避校验。

Feature 不包含数据日期、执行状态或结果路径。相同定义可以在不同来源、范围中多次求值。执行实现/源数据不同即使公式不变，也不能仅靠 definition_hash 复用数值。

## 3. FeatureSet：要跑哪些组合

| 字段 | 类型 / 必需性 | 含义 |
|---|---|---|
| feature_set_id、version | string / 必填 | 配置集稳定 ID 与版本 |
| name、description | string / 必填 | 该组组合的用途 |
| parent_ref | ref / 可选 | 从哪个旧配置集扩展/分支而来 |
| status | enum / 必填 | draft / frozen / retired；Run 只引用 frozen 版本 |
| defaults | object / 可选 | 仅限明确列出的采样/有效性等默认值；冻结时逐项显式化 |
| items | object[] / 必填，可空 | 显式组合条目，结构见下表 |
| resolved_items | object[] / 冻结时必填 | 每个 item/输出映射到具体 feature refs；metric set 已展开 |
| config_hash | string / 冻结时必填 | 规范化配置及解析结果指纹；不是整个数值缓存键 |
| change_reason、frozen_at | string / timestamp / 条件 | 版本变化理由、冻结时刻 |

每个 `items[]` 的关键字段：

| 字段 | 规则 |
|---|---|
| item_id、alias | 集合内稳定唯一 item_id；alias 仅供显示 |
| feature_ref **或** recipe | 二选一：引用现成具体 Feature，或按版本化公式模板生成具体定义 |
| recipe.template_ref、step | 精确模板版本和 Step；模板是 provider 配置资源，不增加核心领域模型 |
| recipe.parameters | 显式 space、`size: {value, unit}`、其他合法公式参数 |
| recipe.lookback | Step 04 的业务回看 L（需要时必填） |
| recipe.input | Step 05 的 input_feature_ref 或本集合上游 item/output 引用，二选一；禁止歧义和循环 |
| recipe.profile_window、sampling、validity | Step 05 的 W、采样、最小样本/缺失规则 |
| recipe.metrics **或** metric_set_ref | 明确统计方法版本列表，或版本化命名集，二选一；只在此输入/窗口下展开 |

同一统计 recipe 选三项 metrics，解析为三个具体 Feature，并用稳定的 output_id（含 metric_ref）定位；下游若引用该 recipe 必须指定输出，不能把三列当一列。若希望按单项统计分别排优先级，可直接拆成三个 items。冻结时去重相同定义但保留 item→feature 映射。

FeatureSet 保存 **features × 合法 size × L/W × 指定 statistics 的显式选择**，不保存数据区间、不记录执行状态。它可以跨 Study/Run 复用。改组合产生新版本；只扩大日期继续引用原版本。

## 4. Run：对数据范围执行配置集

| 字段 | 类型 / 必需性 | 含义 |
|---|---|---|
| run_id | string / 必填 | 本次运行 ID |
| study_id、study_revision | reference / 必填 | 使用哪个研究及其当时配置 |
| feature_set_ref、feature_set_hash | ref / hash / 必填 | 唯一冻结配置集版本及内容校验 |
| selected_item_ids | string[] / 可选 | 缺省选整个集合；显式空数组代表无工作，绝不解释为全选 |
| source_refs、split_ref | refs / 必填 | 实际绑定的数据快照及访问/训练边界 |
| scope | object / 必填 | instruments、start/end、可选 session_filter；指输出范围 |
| allowed_steps | integer[] / 必填 | 允许执行的 Step 3/4/5；展示层映射实际 workflow stage |
| missing_dependencies | enum / 必填 | block / compute_within_allowed_steps |
| reuse_policy | enum / 必填 | verified_only；复用须校验结果身份与覆盖 |
| budget | object / 必填 | 具体的计算/分区/并发上限，不得超过 Study 限制 |
| parent_run_id | ref / 可选 | 探索来源或对比基准，非缓存唯一来源 |
| adopted_review_refs | ref[] / 可选 | 本次配置采用哪些评估建议 |
| request_key、request_hash | string / 必填 | 在 Study 内幂等；相同 key+hash 返回原 run，同 key 不同内容拒绝 |
| config_snapshot、config_hash | object / hash / 必填 | 包含所有生效默认值、精确 refs 和选项的冻结副本 |
| plan、plan_hash | object / hash / 执行前必填 | 依赖闭包、分区、历史支持范围、预期缓存和阻断原因 |
| code_revision、environment_ref、seed | string / ref / integer | 代码和环境必填；随机计算 seed 必填 |
| status、summary | enum / object / 派生 | NO_WORK / NOT_STARTED / IN_PROGRESS / COMPLETE / PARTIAL / FAILED / BLOCKED / SKIPPED；计数与原因 |
| item_executions | object[] / 必填，可空 | 下表的执行明细，可由引擎记录关联查询 |
| started_at、finished_at | timestamp / 可选 | 实际运行时间 |

`item_executions[]` 不是新领域模型：

| 字段 | 含义 |
|---|---|
| work_id、item_refs、feature_ref | 调度 ID、映射的配置条目和具体指标版本；共享依赖可对应多个条目 |
| role | requested / dependency_only；not_selected 从集合对比派生，不造任务 |
| partition_scope、required_history | 本任务输出范围及实际所需历史 |
| state | PENDING / RUNNING / SUCCEEDED / CACHED / FAILED / BLOCKED / SKIPPED |
| result_refs、cache_source_run_id | 产生/复用的结果；缓存来源是实际生产者 |
| attempts | 每次 attempt_id、开始/结束、state、error_code/message、日志引用；追加保留 |

plan 中缓存只是执行前预测，实际使用前再次验证 artifact；失效则在允许范围补算或阻断，记录原因，不悄悄扩大执行边界。Run 总状态按计划内工作汇总，COMPLETE 不要求每个数值有效；quality 单独展示。计划为空是 NO_WORK。仍有运行中的任务是 IN_PROGRESS；终态成功与失败/阻断混合为 PARTIAL；全部成功或验证缓存才是 COMPLETE；均未成功时按失败优先、再阻断、再全跳过给状态并保留明细。

## 5. DataSource：数据身份与访问入口

| 字段 | 类型 / 必需性 | 含义 |
|---|---|---|
| source_id、version、name | string / 必填 | 可读身份与不可变版本 |
| manifest_ref、content_hash | ref / hash / 必填 | 数据文件集合及其内容身份，不只记录目录名 |
| schema_ref、format | ref / string / 必填 | 字段类型、单位和文件格式 |
| instruments、coverage | list / object / 必填 | 真实合约、已知区间/分区/缺口；不以 min/max 假装连续覆盖 |
| time_rules | object / 必填 | 时区、event_time、available_time 的字段或假设 |
| calendar_ref、session_policy_ref | ref / 必填 | 固定交易日历和会话语义 |
| market_metadata_ref | ref / 条件 | 用户提供的行情 metadata 版本/hash；tick 规则、单位、生效日期和来源；依赖该信息的计算必填 |
| gap_manifest_ref | ref / 可选 | 已知缺口、隔离事件及连续性证据；缺失时不能声称覆盖已验证 |
| availability_mode | enum / 必填 | observed / assumed_latency / research_only；假设模式附具体延迟规则 |
| quality_ref | ref / 可选 | 接入质量报告 |
| access_policy_ref | ref / 必填 | 允许的用途与数据范围，Run 另绑定精确 split |

数据访问凭据不写入这些对象；URI/manifest 引用由已有数据访问设施解析。源集合或行情 metadata 变化发布新版本；补充 metadata 可继续引用原始冻结行情，不改写 onboarding 文件。Run 固定实际 metadata 依赖，计算指纹包含其版本/hash。结果按 exchange/instrument_id/trade_date 分区，session 另作窗口边界。

## 6. FeatureResult：结果、质量和血缘

| 字段 | 类型 / 必需性 | 含义 |
|---|---|---|
| result_id | string / 必填 | 不可变逻辑结果 ID |
| feature_ref、definition_hash | ref / hash / 必填 | 具体指标语义身份 |
| source_refs、split_ref | refs / 必填 | 实际数据版本及授权边界 |
| scope、processed_scope | object / 必填 | 此结果声明的输出范围与实际处理范围 |
| input_result_refs | ref[] / 必填，可空 | 上游数值结果；源依赖由 source refs 表达 |
| fit_refs | object[] / 条件 | 若有学习变换：fit artifact、训练范围/cutoff、available_time |
| producer_run_id、producer_work_id | ref / 必填 | 实际产生结果的执行；后续复用不改变生产者 |
| artifacts | object[] / 必填，非空 | artifact_ref、hash、schema、列/行绑定；支持分区文件或共享宽表 |
| computation_fingerprint | hash / 必填 | 定义、源、精确输入、范围、实现/环境、fit/seed 和时间规则的计算身份 |
| coverage | object / 必填 | processed/observed/valid/missing 范围或可查询 manifest；缺口与原因 |
| quality | object / 必填 | n_total、n_valid、NA reason counts、flags；`0 <= n_valid <= n_total` |
| time_metadata | object / 必填 | 时间列映射、session、sampling、窗口、available_time、trailing/summary |
| health_checks | object[] / 追加 | checked_at、available/missing/invalid、原因；是验证时刻的健康状态 |

即使有效值为零，也发布含 schema/原因的有效产物；无产物不能标为成功。初版一个 result 对应一个具体 feature/精确分区即可。Run 的 result_refs 指向这里，Study coverage 从这些索引及运行记录派生；不在三个位置维护相互矛盾的完成标志。

## 7. Review：人工与 agent 的推进记录

| 字段 | 类型 / 必需性 | 含义 |
|---|---|---|
| review_id、study_id | string / ref / 必填 | 评估身份与研究归属 |
| actor、created_at | object / timestamp / 必填 | 谁在何时评估，沿用公共字段 created_by 表达 actor |
| run_refs | ref[] / 必填，非空 | 被评价或比较的运行 |
| result_refs | ref[] / 必填，可空 | 具体证据；失败运行评估允许无结果，但引用错误/日志 |
| target_item_refs | ref[] / 可选 | 指向 FeatureSet item/output，明确评价哪项组合 |
| scope | object / 必填 | 本次判断适用的范围；不得将一周意见隐式外推至一月 |
| observations | string / structured[] / 必填 | 看到的覆盖、质量、结构或异常，附证据引用 |
| decision | enum / 必填 | keep / investigate / park；无 Review 时才派生 unreviewed |
| rationale、limitations | string / 必填 | 为什么这样判断、哪些问题尚未解决 |
| scores | object[] / 可选 | metric_ref/version、value、适用范围与证据；不默认单一优化目标 |
| next_action | object / 可选 | 扩大 scope、修改 FeatureSet 或重试的建议配置；不是执行授权 |
| supersedes_review_id | ref / 可选 | 修正旧判断时新建 Review，保留原文 |

多个 Review 可以意见不同；列表保留作者和证据，不用“最后一条覆盖所有意见”。Study 可指定自己的采用规则；实际采用由新 Run.adopted_review_refs 记录，Review 本身不回写执行状态。

## 8. 一条完整引用链

```text
Study S@revision3
  ├─ DataSource L2@1
  ├─ FeatureSet liquidity@2（冻结；items 是显式组合）
  │    └─ resolved_items → Feature OBI@1、Feature catch22_CO_f1ecac@2
  └─ Run R1（L2@1 + liquidity@2 + 一周范围）
       ├─ item_executions → FeatureResult A、B
       └─ Review V1（引用 R1/A/B，建议扩大时间）
            └─ Run R2（同配置集 + 一月范围，adopted_review_refs=[V1]）
                 └─ 复用 A/B，仅产生缺失分区结果
```

改 size、L/W 或 statistic：发布 FeatureSet 新版本，解析到相应新 Feature；改日期：只创建新 Run。两者都可能复用匹配的旧结果。任意时刻用 Study + FeatureSet/items + DataSource + scope 查询已跑/未跑/有结果，无需逐个翻运行日志。
