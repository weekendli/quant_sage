# Study / FeatureSet / Run 分步执行规格

版本：`research_execution_v1`。日期：2026-11-07。状态：**连续分步实施中：01-A～01-G、02-A、03-A～03-B、04-A、05-A 已完成；下一增量 05-B**。

本文把需求和实施设计拆成可逐项交付的工程增量，范围为 Step 01 管理/执行扩展和 Step 02–05 数据/特征闭环。创建本文不表示任何增量已实现，也不启动代码实现、依赖安装或全量数据重跑。

## 1. 依据与使用方式

必须配套阅读：

- [计算技术方案与执行验收手册](COMPUTATION_PLAN.md)：承接当前代码的模块接缝、计算方法、逐增量验收门、数值用例和 subagent 工作单。
- [领域模型](DOMAIN_MODEL.md)与[字段契约](domain.detail.md)：对象、字段、版本、状态和引用。
- [实施设计](IMPLEMENTATION_DESIGN.md)：存储、并发、解析、规划、发布与恢复。
- [工作流](WORKFLOW_STAGES.md)、[共同契约](COMMON_CONTRACTS.md)与[架构](ARCHITECTURE.md)：数据流、时间、split 和范围边界。
- [执行约束](AGENTS.md)、[实现状态](IMPLEMENTATION_STATUS.md)与[阶段索引](STEP_INDEX.md)：仓库规则、已有证据和各阶段 SPEC/IMPLEMENT/VERIFY。
- [原始 L2 公式](references/gov_bond_l2_feature_spec.md)：只读公式依据；本文不重新定义公式。

按第 4 节顺序一次选一个增量。一个增量可跨多次会话或多个小提交，但不能同时实施其他 stage。各 Step 目录仍是该阶段算法和验证的依据，本文补充增量边界与依赖，不替代阶段验收。

用户已授权连续执行直到本规格完成：每个增量通过验收后，主代理自动推进下一项，无需再次确认。单个子代理仍只实施指定增量；真实 metadata 等外部输入不足时记录受影响的验收项，继续完成不依赖它的工作，不能将受阻验收标为通过。

本会话已经确认的规则以第 3 节为准。发现与旧文档或代码不一致时，修正当前增量涉及的契约和实现，并记录原因；涉及尚未决定的公式时保留 pending，不自行补公式。不得仅因旧阶段标记 COMPLETE 就假定本次扩展也已完成。

## 2. 交付目标与范围

人工与 agent 共用一个循环：

```text
注册冻结数据源与用户行情 metadata
  → 创建 Study、登记有限候选
  → 编辑并冻结 FeatureSet
  → plan / submit 所选组合与数据范围
  → 执行或复用 Step 03–05
  → 查询结果、覆盖和待办
  → 追加 Review
  → 新建 Run 扩日期或扩指标
```

交付七个对象的可追溯实现：Study、Feature、FeatureSet、Run、DataSource、FeatureResult、Review。复用现有 Registry、WorkflowEngine、ArtifactStore 和显式条目引擎的公共行为，不要求七套独立服务。

本轮不实现完整 Atlas、条件 baseline、异常模型、events/regimes/phenomena、科学实验平台、未来收益标签、任意公式执行、分布式调度或外网服务。不要为这些能力预建空模块。

Step 02 已有冻结交付：复用入口和 manifests，不修改其历史状态/报告/文件。当前泛型批次原型仅证明合成执行行为；真实 Step 03–05、完整 Study/FeatureSet/Review 与长期 coverage 仍须逐项交付。

## 3. 必须遵守的需求

| ID | 规则 | 主要验收位置 |
|---|---|---|
| R01 | 元数据、运行、结果索引用 DuckDB；数值用 Parquet；只有 owner 写活动库，worker 不直写 | 01-A、01-E、01-G |
| R02 | 精确版本 refs、严格 schema、不可变发布、revision 冲突检查；保留完整来源 | 01-B、01-C、02-A |
| R03 | 仅执行选中 item 及授权依赖；缺省全选、显式空数组 NO_WORK；不先全算再过滤 | 01-C、01-D、03–05 |
| R04 | 预算不能覆盖全部已选目标/依赖时提交前拒绝，不按预算选择任意子集；运行失败允许 PARTIAL | 01-D、01-E |
| R05 | 用户提供行情 metadata，含历史 tick、单位、适用范围和来源；版本/hash 入源绑定和计算身份；补充不重做接入 | 01-B、02-A、03-A |
| R06 | 成交量未验证及累计量/增量对账不一致仅警告，不单独 BLOCKED/隔离/强制 NA；TRADEVOLUME 不再差分 | 02-A、03-B、04-A、05-A |
| R07 | 深档无效按指标依赖传播；有效 L1 保留；不得用缺档后的容量重新定义 q | 03-B、04-A |
| R08 | F09/F11/F12 端点取同 session 内 `event_time <= t-L` 的最近有效记录；最大端点 age 显式冻结，超限 NA | 01-D、04-A |
| R09 | 按 `(exchange, instrument_id, trade_date)` 分区；时间存 UTC；窗口不能借午休/隔夜历史 | 01-D、02-A、03–05 |
| R10 | 默认 trailing 窗口遇 NA/stale/相关缺口输出 NA；禁止 dropna 拼接；子段摘要仅显式启用并单独标识 | 05-A、05-B |
| R11 | 按实施设计 §6.4 的推荐 `gap_policy_v1` 实现默认机制，并保留策略版本和阈值；无更新不自动等于丢包/零成交 | 02-A、03-B、04-A、05-A |
| R12 | available_time 未知保持 research_only；禁止伪造 event_time 即 available_time；L/W 与未来 H 分离 | 全部领域增量 |
| R13 | 定义身份与配置集版本分离；精确分区复用校验内容/输入/实现/metadata/split；结果保留实际生产者 | 01-C 至 01-F、验收 A/B/C |
| R14 | 未选、未执行、失败、BLOCKED、缺文件、全 NA、零观测和部分覆盖分别可查；Review 不代替执行状态 | 01-E、01-F、验收 |
| R15 | catch22 固定 0.4.5；tsfeatures 首次实施锁定精确 R 版本、源码和依赖，Python 移植须 golden 对照；升级新身份 | 05-B |
| R16 | F07/F08 窗口聚合仍 pending；不默认均值、不复制快照值填 8 个窗口；不阻塞无依赖 ready 子集 | 01-C、04-A、验收 |

具体 metadata、gap manifest、FeatureResult 和 Run 字段以字段契约及实施设计为准。metadata 更新可保守使相关源绑定缓存失效，初版不要求跨源版本证明数值等价。

阈值由具体配置提供：max_quote_age、max_feature_age、max_endpoint_age、观察间隔阈值、min_samples 和资源预算。冻结前显式化，缺必要值返回配置错误；本文不捏造经验最优数值。常规指标不可估与源缺口分开，例如 F06 不可完成快照继续按原公式有效均值规则处理。

## 4. 执行顺序与状态

ID 是本文的工程增量编号，不是新增 workflow stage。初始状态为待执行；当前进度与实际证据见第 8 节。

| 顺序 | 增量 | 所属 Step | 依赖 | 交付里程碑 |
|---|---|---|---|---|
| 1 | 01-A DuckDB Registry 与嵌入 owner | 01 | 现有引擎 | 持久化与锁可运行 |
| 2 | 01-B Study / Source 管理 | 01 | 01-A | 可创建、版本化、查询 |
| 3 | 01-C FeatureSet 冻结与定义身份 | 01 | 01-B | 显式配置可解析 |
| 4 | 01-D Run 规划与幂等登记 | 01 | 01-C | 可预览并冻结计划 |
| 5 | 01-E 执行、发布和恢复 | 01 | 01-D | 合成 provider 可执行/恢复 |
| 6 | 01-F Coverage / Results / Review | 01 | 01-E | 合成 A/B/C 工具闭环 |
| 7 | 01-G 本地 RPC 与完整 CLI | 01 | 01-F | 运行中可供多客户端查询 |
| 8 | 02-A 冻结行情与 metadata / gap 适配 | 02 | 01-G、既有 Step 02 交付 | 真实源可供后续消费 |
| 9 | 03-A 基础 primitives | 03 | 02-A | 真实行情基础序列 |
| 10 | 03-B L2 快照与连续性依赖 | 03 | 03-A | Step 03 稳定交接 |
| 11 | 04-A 业务窗口与组合 | 04 | 03-B | ready 业务子集可执行 |
| 12 | 05-A 采样、窗口与描述统计 | 05 | 04-A；primitive 路径仅依赖 03-B | 统计视图可验证 |
| 13 | 05-B catch22 / tsfeatures | 05 | 05-A | 所选统计项可运行 |
| 14 | E2E-A/B/C 最终验收 | 当前闭环验收 | 上述全部 | 真实小范围证据 |

01-A–01-G 先用已校验序列化 DataSource/FeatureResult fixtures 与算术 provider，不提前实现 L2 算法。E2E 是本范围的验收任务，不等于实施完整 Step 13。不能因 03-A 或 05-A 通过就把相应整个 stage 标为 COMPLETE。

## 5. 各增量执行规格

### 01-A：DuckDB Registry 与嵌入 owner

**输入/阅读：** 实施设计 §2–4；Step 01 SPEC/IMPLEMENT/VERIFY；现有 Registry/WorkflowEngine/ArtifactStore 及引擎测试。

**实现：**

1. 提供 DuckDB Registry 存储和当前需要的完整 DDL/schema 版本记录，保留现有 Registry 公共行为及旧 CLI/合成示例；不引入历史库导入任务。
2. workspace 锁与嵌入 owner 统一持有活动连接；所有写入经过同一入口，使用显式事务。事件顺序用显式序号，不依赖物理行序。
3. 创建、查询、状态转换、缓存/失效、artifact lineage 在新临时 workspace 上可实际运行。后续对象的表随增量增加，不预建空服务。

**输出：** 可运行的持久化增量、schema 初始化/升级测试、精确 DuckDB 环境记录、引擎回归证据。

**验收：** 新库创建/重开；冲突/失败事务无部分写入；第二 writer 返回 WORKSPACE_BUSY；现有引擎状态、缓存、resume、lineage 公共语义回归通过。使用临时目录，不修改冻结产物。

### 01-B：Study / DataSource 管理

**输入/阅读：** 01-A；领域模型、字段契约的 Study/DataSource；实施设计 §4、§9。

**实现：**

1. 严格 schema 和 ResearchService/repository 用例，实现 sources.register、studies.create/update/get；当步提供最小 CLI 或可复现 Python 调用入口。
2. 校验 manifest/hash、日历/session/availability/split 引用；支持用户 metadata 的版本/hash 和 tick 规则校验。先用序列化通用 fixtures，真实 onboarding 字段映射留给 02-A。
3. Study revision 乐观并发、actor/reason/history、有限 backlog、source refs、范围和预算限制；关闭/暂停限制新执行，查询保留。
4. DataSource 不可变版本，metadata 补充创建新版本但沿用行情文件；凭据不落入对象。

**输出：** Study/Source 持久化和序列化契约、可复现管理调用。

**验收：** 创建/更新/重开查询；重复身份冲突；过期 revision 拒绝且不丢更新；metadata 非正 tick/范围重叠/覆盖不足被拒绝；metadata 补充不改原 artifacts；无 available_time 不被补造。

### 01-C：Feature / FeatureSet 解析与冻结

**输入/阅读：** 01-B；字段契约 Feature/FeatureSet；实施设计 §5；各领域阶段的合法参数域，仅实现解析通用能力。

**实现：**

1. 版本化 provider/template/metric-set 资源注册；仅允许已登记实现，不执行任意表达式或动态用户代码。
2. item 的 feature_ref/recipe 二选一；metrics/metric_set_ref 二选一；校验单位、kind/step、窗口、精确版本及依赖 DAG。
3. 默认值规范化后生成具体 Feature、definition_hash、resolved_items、稳定 output_id；共享定义去重但保留条目映射。
4. 草稿 revision 检查；冻结事务一次写入定义/集合/映射；frozen 不可原地编辑。pending 可登记和冻结但不可执行。

**输出：** 可保存、冻结、读取、列举定义的服务/最小入口与冻结 manifest。

**验收：** 非法组合/未知字段/环/歧义拒绝；一个统计 recipe 的多输出可精确定位；同值比例/时长规范化；alias/集合换版不改变未变定义身份；公式/采样/版本变化改变身份；冻结失败不留下半套记录。

### 01-D：Run 规划、预算与幂等登记

**输入/阅读：** 01-C；实施设计 §6、§7.1；字段契约 Run；现有 BatchEngine 与 `_make_jobs`。

**实现：**

1. runs.plan/submit/get：冻结 Study revision、source/split、FeatureSet、selection、配置与范围化 DAG。plan 不产生数值。
2. 以 trade_date_v1 分区，递归传播输出范围和历史读取范围；考虑 L/W/端点 age、session、授权、allowed_steps 与 missing_dependencies。
3. 明确 requested/dependency_only、work_key、依赖边、cache candidate、blocked reason；禁止无条件附加实验 validation。
4. 预算覆盖全部所选任务与必需依赖，否则 BUDGET_EXCEEDED；不创建可派发的部分 Run。
5. `(study_id, request_key)` 原子幂等：同 hash 同 Run，不同 hash 冲突；原子登记 Run/计划/初始任务后才允许派发，避免引擎再次插入。expected_plan_hash 不匹配返回 PLAN_CHANGED。

**输出：** 不可变计划、摘要、幂等 Run 记录、可执行依赖图入口。

**验收：** 省略选择/空选择/未知选择；未选 pending 不进任务；选中 pending 及其下游阻断而无关 ready 可继续；禁止步骤缺依赖则 BLOCKED；预算拒绝无派发副作用；并发同 key 只建一个 Run；权限无法通过新 Run 扩大；历史不只裁成输出范围。

### 01-E：执行、发布、失败续跑与恢复

**输入/阅读：** 01-D；实施设计 §7；ArtifactStore、WorkflowEngine、ItemStage。

**实现：**

1. 执行冻结依赖图，worker 只读授权输入并写独立 staging；owner 记录 attempts、事件及逐项状态。以算术 provider 交付真实序列化结果，不创建 L2 假实现。
2. 结果自检、owner schema/hash/依赖校验、durable receipt、原子目录发布、单事务提交 FeatureResult/链接/成功事件。
3. 恢复 receipt 与索引；重复 receipt 幂等；非法/孤立 receipt 保留隔离证据。CACHED 使用前校验，不创建虚假生产者。
4. 同配置 resume 保留历史 attempts，只重试未完成或结果失效任务；未登记或无授权的文件不能自动接纳。

**输出：** 实际 artifacts、FeatureResult、逐项状态/日志、崩溃恢复实现与合成执行示例。

**验收：** 成功/失败/全 NA/零行/空选择；发布前、发布后未提交 DB、DB 提交后分别注入故障；重复恢复无双结果；提交后文件丢失追加 health 并可修复；Run 已登记未派发可恢复；全部成功或验证缓存才 COMPLETE，混合终态 PARTIAL。

### 01-F：结果、覆盖、Review 与比较

**输入/阅读：** 01-E；实施设计 §8–9；字段契约 FeatureResult/Review。

**实现：**

1. results.list/read、studies.coverage、reviews.add/list、runs.compare，服务与最小调用入口同步可用。
2. 精确 refs/列白名单/scope/limit 查询 manifest 指定 Parquet；分页、truncated 和空结果语义明确。
3. coverage 针对有限 backlog 和精确源/定义/范围派生；processed/observed/valid/available 分开；区间并集去重，不按 min/max 假定连续。
4. health 默认内容校验；快速查询返回 checked_at 与未重新验证标记。失败历史、尚未执行和需要修复分别返回。
5. Review 追加、允许意见不同；新 Run 记录 adopted_review_refs，不自动改权限、预算或冻结集合。比较共同/新增日期、定义和质量。

**输出：** 合成 A/B/C 工具闭环，带未选候选、待办、结果 refs 与评估证据。

**验收：** 三个候选只选两个，第三项可查未跑；后续成功保留早期失败；全 NA 不自动重算；文件丢失归修复项；跨 Run 引用同一结果不重复计数；Review 修正保留原文；不同 scope 的比较明确适用范围。

### 01-G：本地 owner RPC 与完整 CLI

**输入/阅读：** 01-F；实施设计 §4.2、§9；现有 CLI。

**实现：**

1. 本地 Unix socket owner 调用同一 ResearchService；private socket 权限，仅接受本地用户，不提供任意 SQL。
2. `quantsage research ...` 覆盖设计 §9 全部管理方法；保持既有 `quantsage run --config ...` 和合成示例公共行为。
3. JSON envelope 为 `schema_version,ok,data,error`，错误含 code/message/details/retryable；统一分页、limit、错误码和退出码。
4. 运行中查询不由另一个进程直开活动库；大结果查询有行数/时间/内存上限，不长期占用元数据写队列。

**输出：** 可复现启动/停止/请求示例、CLI 帮助、错误协议和完整管理演示。

**验收：** 执行期间多客户端查询；断连/重试幂等；workspace 锁冲突；查询限制；owner 中断后恢复；旧 CLI 回归。完成后记录 Step 01 扩展实际状态，不覆盖早期交付证据。

### 02-A：真实冻结数据源、metadata 与缺口适配

**输入/阅读：** 01-G；Step 02 SPEC/IMPLEMENT/VERIFY/DELIVERY/RESULTS；用户行情 metadata；既有 onboarding.json、外层 manifest、冻结日历/session/split。

**实现：**

1. 将真实 onboarding 入口映射为 DataSource；验证 entry、manifests、内容身份、覆盖与限制；不重新接入 79 个月行情。
2. metadata 校验并独立冻结引用；源文件不变，DataSource 新版本标识补充。用户 metadata 缺失时只阻断所需字段，不能假造 tick。
3. 从已有覆盖报告和隔离 refs 派生 gap manifest，按合约/交易日/session 记录缺口点/区间、原因、字段、来源和确定性；不能把休市或单纯无更新写成已知丢包。
4. 保留可获得的 reset/源区间起点证据；未知起点不替换为上一条 accepted quote。不能证明连续时保留 unknown 并使相关连续性窗口失效。
5. 区分成交量/成交额对账命中计数，统一为警告；schema 非法、真实 reset、缺字段与缺口仍独立处理。

**输出：** 真实 DataSource ref、用户 metadata ref、派生 gap/coverage refs、消费者示例和适配验证报告。

**验收：** 一个真实合约/交易日可以只凭 refs 读取；元数据新版本不改冻结文件 hash；隔离记录可定位且不回灌有效行情；原始区间 lineage 可追溯；缺 metadata、破损 manifest、零有效记录有明确诊断；现有 consumer 验收不被改写为新适配已通过。

### 03-A：基础 primitives 与真实源消费

**输入/阅读：** 02-A；Step 03 SPEC/IMPLEMENT/VERIFY；原始公式 §2；真实冻结标准化行情。

**实现：** selected definitions/provider 接口；用户 tick 历史映射；mid、spread_ticks、一档深度/imbalance、quote_interarrival、源时间/可用性与 lineage。只实现当前所选依赖，不一次生成全部目录数值。

**输出：** primitive Feature 定义和 FeatureResult、按交易日分区的 Parquet、质量摘要、可查询源映射。

**验收：** 手算不规则快照；历史 tick 切换和最长前缀；零分母/未知 tick；跨 session 不串联；metadata/hash 变化不错误复用；追加未来不改前缀；真实 Step 02 小范围集成。03-A 通过时仍不得标记整个 Step 03 完成。

### 03-B：L2 快照 primitives、独立掩码与连续性

**输入/阅读：** 03-A；Step 03 完整 L2 分解与 VERIFY；02-A gap manifest。

**实现：**

1. 实现 tick-distance space、五档容量/depth_ref/逐快照 q、OBI/WOBI/coverage/capped capacity、VWAP/fill/walk、累计买卖量及 reset/native interval-volume 输入。
2. 注册完整 50 个命名单快照描述目录，执行仅展开所选依赖；共享私有节点，不把所有逐档消耗数组分别持久化。
3. 各指标独立掩码；无效深档不伤及有效 L1，未知档不当零，不能外推 L6 或重定义 q。
4. 输出 Step 04 所需终端序列与 gap/reset/端点源证据，警告不升级成无效值；不计算业务 L 窗口。

**输出：** 稳定的 Step 03 公共产物契约和小范围真实结果。

**验收：** Step 03 VERIFY 全部适用项；gapped book、q25/q50/q100、部分末档成交、双侧不足、符号与零分母；reset 后累计量回升仍可识别；原生 TRADEVOLUME 不变；缺口 lineage；完整阶段回归和真实上游集成后才更新 Step 03 状态。

### 04-A：业务窗口与窗口结果组合

**输入/阅读：** 03-B；Step 04 SPEC/IMPLEMENT/VERIFY；只读原公式。

**实现：**

1. 注册 432 个合法名称/参数目录，保留 F07/F08 的 48 个 pending 定义；只执行所选 ready 组合。
2. 实现有效快照均值、区间求和、端点匹配/差、reset/缺口检查、比值及 F09–F12 窗口组合；严格遵守先求比值再均值等计算顺序。
3. 端点 `<= t-L`，冻结 max_endpoint_age，保存各依赖命中记录。窗口 `(t-L,t]` 不跨 session；已知相关缺口失效，重新积累完整 L 后恢复。
4. F06 只对双侧可完成快照平均并保存有效比例；不把单指标不可估误判成源缺口。成交量对账警告原样传递。
5. 输出 FeatureResult、有效性与 n_total/n_valid，支持仅 Step 04 执行及复用已有 primitive refs，不反向依赖 Step 05。

**输出：** 规范业务定义目录、ready 子集结果、pending 原因与窗口审计证据。

**验收：** Step 04 VERIFY；432 名称计数正确但不冒称全数数值交付；手算不规则窗口、精确等时/age 超限、reset/缺口、均值顺序和逐快照 q；不同 Run scope 的共同时间点一致；源/定义变化不误缓存。仅可声称 ready 子集完成；完整 pack 数值交付必须先明确 F07/F08 聚合。

### 05-A：采样、完整窗口与基础描述

**输入/阅读：** 04-A 或所选 primitive 的 03-B；Step 05 SPEC §3–5、§9、IMPLEMENT/VERIFY；源 coverage 与 gap refs。

**实现：**

1. 版本化 input view 白名单，按实际选择绑定业务结果或 primitive refs；不在本步重造 Step 04 公式。
2. 同 session 的 clock 网格/backward as-of、event index、max age、区间量端点分桶与源跨度记录；成交量/计数不 forward fill。
3. 网格、stride 和固定块对齐 session 起点，event 块对齐稳定观察顺序，不能从 Run 起点重新计数。
4. 默认 trailing 完整 W 且不含 NA/stale/相关缺口；单个缺失仍保留评估行并输出 NA/原因。segment_summary/显式子段摘要分别命名和记录实际范围，段内不可提前消费。
5. 必需质量统计及所选原单位描述；记录 L/W/stride/采样、底层依赖与 availability；不因选一个统计量自动执行全描述集。

**输出：** 可复现输入视图、统计 Feature/FeatureResult、描述统计与质量产物。

**验收：** 30m 窗口中间缺 1s 为 NA，完整历史恢复后有效；子段不代替完整 W；午休/隔夜/缺口不拼接；无更新不随意补成交量零；clock/event 含义可区分；改变 Run 起点不改共同评估值；primitive-only 无 Step 04 依赖；真实上游小范围集成。

### 05-B：catch22 与固定 tsfeatures

**输入/阅读：** 05-A；Step 05 SPEC §6–7、§9–10 与 VERIFY。

**实现：**

1. catch22 固定 `pycatch22==0.4.5`，按 canonical long name 提供 22 项；完整集和显式子集均可选择，子集不先全算再过滤。
2. 首次实施选定一个精确 R tsfeatures 版本、源码 revision 与依赖锁，交付 SPEC 的 8 个候选；Python 移植提供同输入 golden fixtures 和误差约定。固定后不得自动升级。
3. 每项 Feature 绑定 metric/参数/后端/时间视图/validity；保留短样本、常量、近常量、数值失败的具体原因，不填 0 冒充有效。
4. 发布 fingerprint long Parquet、质量与 lineage，输出单位和输入单位分开；不新增 Atlas/baseline 占位输出。

**输出：** 精确版本锁、golden 产物/生成方法、所选统计结果与成本记录。

**验收：** Step 05 VERIFY 全部适用项；22+8 身份/参数映射和 golden 对照；ARCH R²、谱熵、crossing ties、分块缩放一致；未选 metric 不调用；完整基线/自定义子集标签明确；未来追加不改已结束窗口；真实覆盖/NA/耗时与合成敏感性分别报告。

## 6. 最终 A/B/C 与故障验收

在上述增量通过后，仅对已验证源中明确授权的真实小范围运行；不重跑全量接入。使用用户提供的真实 metadata；未提供时不得把示例 metadata 当生产事实。先核对源覆盖、split、session 和预算；research_only 允许描述性计算，不进行拟合或假设检验。

| 场景 | 操作 | 必须观察到的结果 |
|---|---|---|
| A | 有限 backlog 登记至少三项；小范围仅选两个 ready 业务组合及少量统计项 | 所选项和依赖完成，未选项可查未执行，refs/质量/覆盖齐全 |
| Review A | 查询 A 的数值/NA/覆盖后追加人工或 agent Review | 判断有证据、明确 scope，保留 actor；不改执行状态 |
| B | 同 frozen FeatureSet，扩大到包含 A 完整交易日分区的范围，记录 parent/adopted refs | 匹配旧分区验证后复用，仅补缺失；实际生产者不变；共同/新增范围可比 |
| C | 同范围冻结 FeatureSet 新版本，新增少量 tsfeatures | 未变业务/统计定义可复用，仅执行新增定义及缺失依赖 |
| 单步 | 仅 Step 04 或仅 Step 05；另测 primitive-only | 已有合格依赖可引用，禁止补跑且缺依赖则 BLOCKED |
| 无工作/不可估 | 空选择、全 NA、有缺口证据的零行 | NO_WORK 或合法质量产物；不伪造有效值，不混同未执行 |
| 失败/修复 | 注入失败、同配置 resume、损坏/移走测试结果副本 | attempts 保留，健康与修复可查，不删除历史证据 |
| 时间/身份 | 改采样、tick metadata、算法版本；改变 Run 起点/追加未来 | 变身份不误复用；共同评估值和历史前缀保持一致 |
| pending | 未选 F07/F08、选中 F07/F08 | 前者不阻塞，后者及其依赖消费者明确 BLOCKED |

故障只注入新建测试 workspace 或可恢复副本，不破坏真实冻结数据。生成实际 Run/FeatureSet/Review/result refs、输入版本/hash、所执行组合、缓存/失败/NA/覆盖、耗时/资源及复现命令的验收报告。F07/F08 仍 pending 时，报告写“ready 子集闭环验收”，不能写“完整 432 项数值完成”。

## 7. 验证与完成规则

每个增量先验收自己的公共行为，再运行受影响回归。算法阶段须有手算或固定参考数值断言、序列化 fixture 集成、真实上游消费、空/失败路径、lineage 和适用时间契约测试；只检查文件存在不算验收。

仓库当前可用的回归入口示例：

```bash
python -m pytest -q tests/test_workflow_engine.py tests/test_research_batches.py
python -m pytest -q tests/test_onboarding*.py
python -m ruff check quantsage_discovery scripts tests
```

新增量的测试文件及 research CLI 尚待实现，交付时记录真实存在且实际运行的命令，不把上述示例当通过记录。阶段结束运行其全部相关测试；最终运行全套 pytest/Ruff 和本范围真实小范围验收。环境缺工具时如实记录，不声称已通过，不为本文安装依赖。

完成一个增量须同时满足：可调用的实际行为、schema/产物/错误路径、该节验收证据、适用回归通过、完成报告和状态同步。记录实际代码/环境版本、数据 refs、资源限制及未覆盖项。不得沿用旧测试数量或以新文档冒充运行结果。

02-A 是既有阶段的后续适配扩展；01-A–01-G 是引擎管理扩展。分别登记增量状态，不篡改原 Step 01/02 的历史完成记录。Step 03–05 仅在对应全部必需增量和该阶段 VERIFY 通过后更新状态；全量数值 pack 与 ready 子集支持分开说明。

## 8. 执行记录与单步提示

实施时在当前 Step 的 RESULTS.md 追加独立小节，记录增量 ID、日期、输入 refs、变更、实际测试命令/结果、产物/报告路径、限制与下一可执行 ID；同步 [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) 和 [实施跟踪](STEP_IMPLEMENTATION_TRACKER.md)。不要覆盖既有冻结证据。

进度从“待执行”进入“进行中”，证据齐备后“完成”；依赖不足时记录具体阻断原因。本文创建时所有增量均为待执行，历史 Step 01/02 完成不等于本表增量完成。

| 增量 | 当前状态 | 验收证据 |
|---|---|---|
| 01-A | COMPLETE，2026-11-06 | [DuckDB 增量结果](steps/01_workflow_engine/RESULTS.md#01-a-duckdb-registry-与嵌入-owner2026-11-06)；13 项 Registry 测试及全套 136 项通过，Ruff 通过 |
| 01-B | COMPLETE，2026-11-06 | [管理增量结果](steps/01_workflow_engine/RESULTS.md#01-b-study--datasource-管理2026-11-06)；30 项管理测试、全套 166 项及模块入口 smoke 通过 |
| 01-C | COMPLETE，2026-11-06 | [冻结增量结果](steps/01_workflow_engine/RESULTS.md#01-c-feature--featureset-冻结2026-11-06)；20 项冻结测试、全套 186 项及 Ruff 通过 |
| 01-D | COMPLETE，2026-11-07 | [规划增量结果](steps/01_workflow_engine/RESULTS.md#01-d-run-规划与幂等登记2026-11-07)；17 项规划测试、全套 203 项及 Ruff 通过 |
| 01-E | COMPLETE，2026-11-07 | [执行增量结果](steps/01_workflow_engine/RESULTS.md#01-e-执行发布与恢复2026-11-07)；18 项执行测试、全套 221 项、Ruff 及独立进程示例通过 |
| 01-F | COMPLETE，2026-11-07 | [查询增量结果](steps/01_workflow_engine/RESULTS.md#01-f-结果覆盖review-与比较2026-11-07)；9 项查询测试、全套 230 项、Ruff 和独立进程 inspection 示例通过 |
| 01-G | COMPLETE，2026-11-07 | [RPC/CLI 增量结果](steps/01_workflow_engine/RESULTS.md#01-g-本地-owner-rpc-与-cli2026-11-07)；8 项 RPC 验收、全套 241 项及 Ruff 通过 |
| 02-A | COMPLETE，2026-11-07 | [冻结源与 gap 适配结果](steps/02_data_onboarding/RESULTS.md#02-a-frozen-source-metadata-and-gap-adaptation--2026-11-07)；真实 DataSource v2、16,636 分区、49,800 session 区间、354,319 gap 事件；全套 247 项测试及 Ruff 通过 |
| 03-A | COMPLETE，2026-11-07 | [基础 primitives 结果](steps/03_research_dimensions_and_primitives/RESULTS.md#03-a-basic-primitives-over-the-real-frozen-source--2026-11-07)；真实 Run 18 个结果、18 个数值 artifact、18 个 audit artifact；全套 262 项测试与 Ruff 通过 |
| 03-B | COMPLETE，2026-11-07 | [完整快照目录结果](steps/03_research_dimensions_and_primitives/RESULTS.md#03-b-complete-snapshot-catalog-and-terminal-inputs--2026-11-07)；50 名称、54 个真实结果/审计；全套 291 项测试与 Ruff 通过 |
| 04-A | COMPLETE，2026-11-07 | [ready business 子集结果](steps/04_business_features/RESULTS.md#04-a-ready-business-window-subset--2026-11-07)；432 名称/48 pending、48 个真实工作、全套 296 项测试及 Ruff 通过 |
| 05-A | COMPLETE，2026-11-07 | [primitive-only 采样/描述结果](steps/05_statistical_features/RESULTS.md#05-a-primitive-only-sampling-and-descriptions--2026-11-07)；真实 session 起点、输入白名单、mean/std Run、全套 302 项测试与 Ruff 通过 |
| 05-B、E2E | NOT_STARTED | 按依赖顺序自动推进 |

可复制以下提示，每次只替换一个 ID：

> 阅读仓库 AGENTS.md、docs/AGENTS.md、docs/EXECUTION_SPEC.md、docs/IMPLEMENTATION_DESIGN.md、docs/COMMON_CONTRACTS.md，以及增量 `<ID>` 所属阶段的 SPEC/IMPLEMENT/VERIFY。核对上游真实完成证据，仅实现 `<ID>` 的可运行增量，不提前实现其他 stage。复用既有接口，按 schema 序列化并验证产物；没有真实上游时用合规 fixture 开发，但不冒充真实集成完成。遵守本规格已确认的 metadata、警告、缺口、端点、交易日分区和固定版本规则；不猜测 F07/F08 聚合。执行对应验收与回归，记录实际命令/结果/产物/限制并同步状态，然后停止在本增量边界。

下一执行 ID：**05-B**。F07/F08 的聚合规则仍为唯一明确保留的业务公式待决项；最大 age/间隔阈值和真实 metadata 是执行配置输入，tsfeatures 精确版本锁是 05-B 交付项。用户真实 metadata 路径待提供，格式及校验/换版命令见 [模板说明](../configs/templates/README.md)；缺失时 tick 相关计算保持阻断，其余真实源路径可继续实施。
