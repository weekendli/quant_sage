# Step 01–05 计算技术方案与执行验收手册

版本：`computation_plan_v1`。编写日期：2026-11-07。用途：承接当前实现，供主代理及 subagents 按 [EXECUTION_SPEC](EXECUTION_SPEC.md) 逐增量实现、交接和验收。

本文是**实施方案，不是完成报告**。已实现状态以执行记录、提交和测试证据为准；下面标为“拟新增”的接口、文件与测试均须实际实现后才能使用。

## 1. 依据、优先级与接续点

需求依据为 [领域模型](DOMAIN_MODEL.md)、[字段契约](domain.detail.md)、[实施设计](IMPLEMENTATION_DESIGN.md)、[共同契约](COMMON_CONTRACTS.md)、[架构](ARCHITECTURE.md)，拆分与顺序以 [EXECUTION_SPEC](EXECUTION_SPEC.md) 为准。算法必须同时满足 Step 03–05 的 SPEC/IMPLEMENT/VERIFY 和只读的 [L2 原始公式](references/gov_bond_l2_feature_spec.md)。用户已确认规则见执行 spec 的 R01–R16；旧文档中的历史未知状态不能覆盖这些确认，也不能被悄悄改写。

遇到冲突时：先核对用户确认和共同契约；确定是旧实现限制则在当前增量修正并记录；确属公式未决则保留 pending，不代填公式。本文补足计算方法和工程交接，不扩大到 Step 06–13、Atlas、拟合、未来标签或科学显著性验证。

| 接续位置 | 编写时核实的事实 | 接手动作 |
|---|---|---|
| 01-A～01-F | 已提交；本地 HEAD 为 `c3aa1aa`；此前全量 230 项测试通过 | 复用既有服务、规划、发布、恢复、查询与 Review，不重新搭建 |
| 01-G | RPC/CLI、migration 8、资源护栏及全套测试已收尾 | 复用 owner/CLI，不回退到进程内绕过 owner 的实现 |
| 02-A | 已完成真实冻结源适配、metadata 模板/校验、session 区间、gap/reset/raw-interval 证据及 v2 注册 | 后续使用 `cffex_treasury_l2_full79@2`；真实 tick/单位 metadata 仍待用户提供 |
| 03-A | 已完成七项基础模板、真实 L2 provider、逐行 validity/audit 发布和真实单日三 session 执行 | 03-B 复用该 dispatch；`spread_ticks` 仍待真实 tick metadata |
| 03-B | 已完成 50 名称目录、五档空间/容量/VWAP/连续性终端输入和真实 d0 子集执行 | 04-A 可消费 mid/depth_ref/q/累计量/原生成交量；d1+与 F06–F08 仍待 metadata |
| 04-A | 已完成 432 名称/48 pending、多输入窗口 AST、端点/reset/审计和真实 ready 子集 | 05-A 消费 business refs；完整 432 数值仍受 pending/tick metadata 限制 |
| 05-A | 已完成固定 session 网格、完整 trailing W、基础描述/质量、primitive-only 与真实 mean/std | 05-B 增加 catch22/tsfeatures，不修改 05-A 窗口身份 |
| 05-B | 已完成 catch22 0.4.5、tsfeatures 1.1.1/R 4.4.3 锁、真实 goldens、22+8 目录与真实 selected Run | E2E 使用真实业务 refs 验证 A/B/C 与复用 |
| E2E A/B/C | 已完成真实 ready-subset A→Review→B→C、跨 Run 复用、单定义扩展、NO_WORK/pending 与故障恢复证据 | 用户 metadata/F07-F08 和 Step 06–12 保持外部/范围限制 |
| 冻结 Step 02 | 已有 79 个月接入和独立 consumer 证据 | 只读复用入口及 manifests；不重跑接入，不改冻结报告/文件 |
| 用户行情 metadata | 用户稍后提供实际文件，目前要求给好格式 | 先交模板、校验器及不依赖缺失字段的计算；真实 tick 验收待用户文件，不套用历史参考表充当用户输入 |

这是时间点快照；下一代理先运行 `git status --short`、`git log -8 --oneline`，阅读 spec 第 8 节和对应 RESULTS，确认代码是否已前进。`CFFEX` 是未跟踪原始数据，不纳入代码/文档提交。当前有其他代理修改的文件时，先确认归属，避免覆盖。

## 2. 计算路径与代码接缝

```text
冻结 onboarding/manifest/calendar/session/split + 用户 metadata
    → 02-A DataSource + gap/reset/interval evidence
    → 03-A/B 按选择展开的 primitive DAG
    → 04-A 业务窗口及组合（ready 子集）
    → 05-A 输入视图/采样/统计窗口 → 05-B 指定形态指标
          ↑ 可直接来自 03-B 的 primitive-only 路径
    → ArtifactStore staging/receipt/publish → owner 验证并提交
    → FeatureResult → Results/Coverage/Review → A/B/C
```

### 2.1 复用现有管理与执行层

| 现有代码 | 后续职责与修改边界 |
|---|---|
| `research/service.py`、`repository.py` | 复用 Study/Source/FeatureSet/Run 用例；新增领域输入仍通过 schema 校验 |
| `research/catalog.py`、`feature_schemas.py` | 注册精确 provider/template/metric 版本、合法参数域和依赖；领域 recipe 按角色解析多个输入 |
| `research/planner.py` | 复用选择、授权、lookback、显式 DAG、幂等登记、预算；随领域算子补足时间支持传播 |
| `research/execution.py` | 复用 attempts、缓存验证、receipt、原子发布、恢复；扩展受信任 provider 调度和输入读取 |
| `research/providers.py` | 当前仅合成算术 provider；保留测试用途，不将其改名冒充领域计算 |
| `research/queries.py` | 复用数值读取、coverage、Review 和比较；显式支持新审计产物版本 |
| `research/rpc.py`、`research_cli.py` | 复用同一 ResearchService 的管理入口；客户端不能直开活动 DuckDB |

### 2.2 领域模块的拟新增边界

建议按当前增量新增 `research/onboarding_adapter.py`、`research/l2_primitives.py`、`research/business_windows.py`、`research/statistical_views.py`、`research/statistical_metrics.py`，或放入现有等效领域目录。路径由实现代理依仓库结构确定，交付时记录实际位置；不要现在创建空文件。

领域算子只返回待发布的数值、质量和审计数据，不自行提交 Registry、不直接创建成功状态、不绕过 ArtifactStore。provider key 使用代码内白名单；RPC 不接受动态 import、表达式执行或任意 SQL。

**必须修正的真实接缝：** 当前 executor 对每个输入只接受一个数值 artifact，并把多个输入表拼接后交给合成 transform；这不能表达 mid/depth/累计量等不同角色。03-A 起增加受信任 dispatch，输入按 `feature_ref + role + scope` 保留，不用一个 concat 表混淆不同输入。04-A 前完成多输入依赖解析及逐输入时间对齐；禁止让下游直接读 raw 重造已登记上游公式。若多 artifact 表示同一输入的分片，只在验证同一语义和范围后合并；同时间重复观察按稳定源身份处理，不按 timestamp 无条件去重。

建议逻辑调用契约如下，**是拟实现协议，不是当前可调用 SDK**：

```text
ProviderContext = frozen_feature + partition/output_scope + read_scope
                + source/metadata/split refs + gap_policy + runtime fingerprint
ProviderInputs  = {role: [verified result/artifact refs and bounded readers]}
ProviderOutput  = value batches + row-quality/audit batches + quality summary
```

03-A 的最小 provider 接入须从实际 onboarding refs 读取白名单行情列；不允许把真实 L2 文件当成已经拥有 `value` 的合成六列表。后续复用该 reader 和公共发布路径。

## 3. 数据、时间、身份与产物

### 3.1 读取与历史支持

- 物理输出分区为 `(exchange, instrument_id, trade_date)`；任务内可按 session 拆分，session 是连续交易边界。所有时间存 UTC，交易日和 session 使用冻结交易所规则，不能从 UTC 日期猜交易日。
- Run 输出 scope 为 `[a,b)`；业务窗口为 `(t-L,t]`；统计窗口为 `(t-W,t]`。只发布输出 scope 内评估点，允许读取经过授权的历史支持。
- 由每个算子的直接依赖递归传播历史，而非每层重复加全部 raw lookback。例如业务 L、统计 W、过去端点最大 age A 的保守源支持可到 `a-W-L-A`；具体还取决于采样变换、差分前驱及依赖图。每条边分别保存 direct input scope，最终 raw scope 取实际闭包。
- 历史扩展仍检查 Study/split 读取权限。没有足够同 session 历史时输出 NA；不得跨午休、隔夜、合约或未授权范围补齐。禁止读取 holdout 来确定阈值。
- 输入稳定顺序沿用上游排序/重复处理证据。没有交易所序号时保留限制，不宣称可恢复真实逐笔顺序。
- known available_time 按实际贡献输入与计算模式传播；未知任一必要输入则保留未知和 `research_only`。不能用 event_time、文件生成时间或模拟延迟冒充实证 available_time。回顾段摘要不在段内广播。

### 3.2 数值表与审计产物演进

现有 `feature_values_v1` 固定六列：UTC `event_time`、可空 UTC `available_time`、`instrument_id`、`trade_date`、`session_id`、可空 float64 `value`。现有校验器严格检查列集合；不能直接加列而仍称 v1。

推荐保留 v1 数值表给现有 results.read 使用，另注册版本化质量/审计 artifact，作为同一 FeatureResult 的绑定。03-A 先冻结最小 schema，再由 03-B/04-A/05-A 显式升级。配套审计至少能定位：源观察键、对应输出行/稳定 occurrence、is_valid/invalid_reason、warning flags、gap/reset refs、端点命中时间/ID、原生区间起止及确定性、n_total/n_valid、实际窗口边界、采样 age。重复时间不能只用 timestamp 关联；若无法在 v1 中可靠对应 occurrence，必须设计 v2 数值 schema 并同步发布/恢复/查询校验，不能做隐含行序 join。

审计允许按依赖和输出类型拆表，不要求每行复制全部 lineage。实现必须保存大表 refs/hash，不把逐行审计塞进 DuckDB JSON。SchemaRegistry、receipt/owner 验证、cache health、results 与 coverage 的必要修改必须在当前增量内打通；保留旧产物可读性。不能只写 sidecar 而不纳入 manifest/hash/恢复验证。

### 3.3 身份与复用

Feature 身份包含公式/参数、输入定义、单位、L/W/stride、采样原点、有效性/gap/age 策略、实现及 backend 精确版本。alias、FeatureSet 换版和用户选择排列不改变同一数值定义。

计算指纹再绑定 DataSource/metadata/split 精确版本、输入结果及内容 hash、授权 scope、实际输出/历史范围、代码和依赖指纹。结果保留真实 producer_run_id，消费 Run 只建链接。metadata 补充生成新 DataSource 版本；初版可保守失效，不能仅因 raw 文件未变就沿用旧 tick 结果。

扩大 Run 只能复用精确匹配且重新验证健康的已完成分区；不把局部分区 min/max 当全覆盖。先保持当前精确范围缓存契约，不引入未经证明的任意范围切片复用。全 NA/零行的合法结果也是已处理结果，不自动重算；文件缺失是 health/修复问题。

## 4. 02-A：冻结接入、metadata 与缺口

### 4.1 适配步骤

1. 从 Step 02 [RESULTS](steps/02_data_onboarding/RESULTS.md) 的真实 onboarding entry 与 outer manifest 开始；验证 schema、引用链、文件身份、calendar/session/split 与覆盖限制。不要依赖某次 Run 的临时目录或猜测路径。
2. 建立源分区索引，只在授权小范围扫描行情；流式校验大文件，避免把所有行情/所有分区放进一个 Arrow/Python 对象。完整内容验证的成本与复用依据要记录，不能跳过大文件 hash。
3. 生成版本化 DataSource 及 adapter manifest；引用原冻结行情，不重写 onboarding.json。保存原产物身份和当前适配实现身份，二者不能混写。
4. metadata 新版本只补规则/单位等引用，不重新接入原始行情。缺必要字段只阻断其消费者；例如 mid 和价格单位 spread 不必因缺 tick 全部停止。
5. 从已有覆盖、隔离和源顺序证据派生 gap/reset/native interval 审计；隔离行只能提供证据，不能重新作为有效数值行情。

### 4.2 用户 metadata 格式交付

实现代理交付**与现有 `market_metadata_ref` 校验契约一致、可通过验证的 JSON 模板、字段说明和校验命令**；模板中的示例数值只作 fixture，真实验收不能使用。字段设计至少表达 schema/version、提供者/来源、产品或合约规则、交易日生效区间、正 tick_size、价格/数量单位。若现有 schema 字段名不同，使用现有名或显式迁移，不能在此另造一个无法注册的格式。

规则要求：最长前缀 `TL,TF,TS,T`，重叠/歧义拒绝，生效边界明确、历史覆盖可查，缺匹配不回退当前 tick。覆盖验证针对声明可计算的范围；允许明确的 metadata 缺失状态，而不是伪造全覆盖。模板所在路径和实际 CLI/Python 注册示例必须写入 02-A RESULTS。

### 4.3 `gap_policy_v1` 的可计算表示

gap manifest 记录源/合约/交易日/session、点或区间、reason、受影响字段/依赖、源行或报告 refs、certainty；native interval start 和 reset evidence 单独保留。区间端点、点事件命中规则须显式定义并测试，与 `(t-L,t]` 和 `[a,b)` 区分。

| 情况 | 数据处理 | 下游规则 |
|---|---|---|
| 休市/午休 | 由 session/calendar 表示 | 不当丢包，不借另一 session 历史 |
| 无更新/发布机制未知 | 保留 unknown，按冻结 age/观察间隔策略判断 | 不能直接标 known packet loss，也不能制造零成交 |
| 已知隔离/缺字段/缺失区间 | 保存证据和字段影响 | 只使相关依赖失效；book as-of 不穿过相关失效记录 |
| 超限观察间隔 | 保存实测间隔及阈值版本，区别于已证实源丢失 | 按策略使相关连续性窗口无效 |
| 累计量 reset 或隔离期间连续性未知 | 保留 reset/unknown 事件，即使后来累计值回升 | 不仅看窗口两端；相关 flow 窗口 NA |
| 成交量未验证/对账不一致 | 原值透传，volume/amount 分别计数和警告 | 警告本身不 BLOCKED、不隔离、不强制 NA |
| 结果文件丢失 | 追加 health 并建立修复待办 | 不是行情 gap，不删除历史结果 |

原生 TRADEVOLUME 区间起点无证据时保持未知，不能使用上一条 accepted quote 冒充。源连续性未知和 warning-only 成交量语义不同；前者可使依赖该连续性的细粒度视图不可用，后者不能单独使其失效。

## 5. 03-A/B：按依赖计算 primitives

### 5.1 基础量、空间与 size

令价格 `b_i,a_i`、数量 `B_i,A_i`、tick `τ`；使用当前快照：

```text
mid=(b1+a1)/2                    spread_ticks=(a1-b1)/τ
δB_i=(b1-b_i)/τ                 δA_i=(a_i-a1)/τ
DB(d)=Σ B_i·1[δB_i≤d]          DA(d)=Σ A_i·1[δA_i≤d]
DB5=Σ(i=1..5)B_i               DA5=Σ(i=1..5)A_i
depth_ref=(DB5+DA5)/2           q_u(α)=α·depth_ref_u
```

`D={0,1,2,3,5}` ticks，`α={0.25,0.50,1.00}`。d 表示距 touch 的 tick 空间，不是档数；DB5 不等于 DB(d5)。未知深档不能当 0，不能用残缺五档重新定义 q。

03-A 实现 tick 绑定、mid、spread、一档深度/imbalance、quote_interarrival 及必要时间/源映射；03-B 完成以下目录。全部定义可注册，执行仅展开选中项及依赖。

| 快照 family | 公式/依赖 | 命名数 |
|---|---|---:|
| F01 | spread_ticks | 1 |
| F02 | `(DB-DA)/(DB+DA)` | 5 |
| F03 | `WB=Σ B_i·1[δB_i≤d]/(1+δB_i)`，WA 同理；`(WB-WA)/(WB+WA)` | 5 |
| F04 | `min(min(1,DB/q),min(1,DA/q))` | 15 |
| F05 | `CB=min(DB,q),CA=min(DA,q)`；`(CB-CA)/(CB+CA)` | 15 |
| F06 | 双侧完整可成交时 `(VWAPbuy-VWAPsell)/τ` | 3 |
| F07 | `(dist_buy-dist_sell)/(1+dist_buy+dist_sell)` | 3 |
| F08 | `cost_buy=(VWAPbuy-mid)/τ`、`cost_sell=(mid-VWAPsell)/τ`；两成本差/和 | 3 |
| 合计 | 不含私有共享节点，不展开业务 horizon | 50 |

walk 逐档消耗 `x_i=min(size_i,max(0,q-Σ_{j<i}x_j))`，最后一档可部分消耗；完整覆盖才给 VWAP，距离取达到 q 的首个完整成交档位。禁止外推 L6/复制 L5/隐藏流动性。q100 不保证弱侧能完成；这类不可估有独立原因。

### 5.2 独立掩码与连续性

每个公开 primitive 保存自己的依赖与有效掩码。有效 L1 不因深档异常全部失效；空间求和必须证明可能入选档位有效，不能跳过未知档；完整五档容量及目标 q 按完整性要求判断。零分母/零 q/未知 tick/不完整可见深度分别 NA，不能夹 epsilon；只有 F12 的原定义允许其指定 epsilon。

active_buy/sell_cumulative 透传累计字段并保存每个 reset/连续性断点，trade_volume_interval 透传 TRADEVOLUME；不二次差分。quote_interarrival 在同 session 稳定观察序内计算，并保留 source adjacency 证明的限制。

共享私有计算可在一个 work 内重用，不必每个档位中间数组持久化；但 Step 04 所需终端输入、mask、源时间、reset 和 gap 证据必须通过已发布 refs 可读。

## 6. 04-A：业务 L 窗口

合法 L 为 `1s,3s,1m,5m,10m,15m,30m,60m`。原始文件将其命名为 horizon；实现中明确映射为业务 lookback L，与统计 W、未来 H 分开。

| Family | `(t-L,t]` 计算 | 定义数 |
|---|---|---:|
| F01/F02/F03 | 对各快照 spread/OBI/WOBI 取有效算术均值 | 8/40/40 |
| F04/F05 | 对各快照当前 q 下的 coverage/capacity imbalance 取有效均值 | 120/120 |
| F06 | 只对双侧完整可完成的快照 size_spread 取均值，保存总数/可完成数/比例 | 24 |
| F07/F08 | 聚合未决，登记 `pending_semantics`，不发布窗口数值 | 24/24 |
| F09 | `B=Cbuy(t)-Cbuy(start)`，S 同理；`(B-S)/(B+S)` | 8 |
| F10 | `V=Σ trade_volume_interval`，`Dbar=valid_mean(depth_ref)`；`V/Dbar` | 8 |
| F11 | `M=(mid(t)-mid(start))/tick(t)` | 8 |
| F12 | `NF=B-S`；`sign(NF)·M/(abs(NF)/Dbar+epsilon)`；NF=0 为 NA | 8 |

合计 432 个合法定义，其中 384 个 ready、48 个 pending；ready 是语义状态，不保证有输入或每行有效。不得把“目录齐全”写成“432 项数值完成”。

### 6.1 端点、均值与有效性

累计量/mid 分别在同 session 查找 `event_time <= t-L` 的最近有效观察，包含等时；各输入 mask 不同，命中记录可不同。`age=(t-L)-matched_time` 必须不超过冻结 max_endpoint_age；未配置拒绝相关定义执行。保存各端点时间、源键、age 和原因；不得 nearest、未来匹配或用 `>=t-L` 替代。

实现可用有序双指针、前缀和、reset/gap 索引、逐输入 backward as-of；浮点累加与容差需固定。输入窗口不按 Run 起点重新初始化；先读取充分历史再裁输出。每个快照使用其自己的 q，不用 q_t 回算整个历史。

计算顺序固定：F02/F03/F05 是均值(比值)；F09 是端点差后求比；F10 是量之和/平均深度；F12 是窗口组合。不能统一套 rolling mean。snapshot mean 按快照等权，不改时间加权。

相关源缺口/reset/超限间隔影响依赖窗口时 NA，重新积累完整 L 后恢复。F06 个别快照无法完成不是源缺口，仍按有效均值规则；空有效集合/历史不足为 NA。成交量警告保留，不单独改变数值有效性。

## 7. 05-A/B：输入视图、统计窗口和固定后端

### 7.1 采样与评估顺序

1. 输入视图白名单绑定一个明确业务/primitive feature 与单位、变换、time_basis、允许 metrics；不生成全空间笛卡尔积。primitive-only 不引入 Step 04。
2. clock 网格、stride、固定块均锚定冻结 session 起点。默认研究配置为 1s 输入、30m trailing W、60s 评估 stride；这些是项目配置，须写入定义，不是后端自动默认。
3. book/业务值按 backward as-of 取 `event_time<=g`，验证 max_quote_age/max_feature_age 及相关无效记录/缺口；不能跳过最近无效值而沿用更老有效值，也不能跨缺口 carry。
4. volume 按真实记录端点落入 `(g-Δ,g]` 求和，保存原区间跨度/端点归属近似；不 forward fill、不假造比例分配。源覆盖不能证明零时记 NA。观察 count=0 只是未观察到快照，不等于真实订单数为零。
5. event_index 使用 session 内稳定有效观察序；lag 是事件数。event 分块从 session 序列原点计数，不从 Run.scope.start 计数。
6. 保留固定评估行。默认完整 trailing W 内任一 NA/stale/相关缺口，该时刻统计 NA，不 dropna 拼接；恢复须满足完整 W 的有效输入及上游 L。子段摘要只在显式开启时另发独立 segment_summary，不能回填父窗口。
7. 默认形态门槛连续有限样本 n≥256，单指标更严限制优先；常量/近常量按各指标可估条件处理，基础描述仍可有效，不添加噪声/填 0。

质量统计必需，原单位描述量显式选择。完整 session summary 是回顾产物；尾块/子段是否允许必须冻结，默认 trailing 不回退到 partial。

在发布数值前，算子/metric 注册还必须冻结适用的数值约定：ddof、quantile 插值、ACF 分母/lag、PACF 方法、近常量判定、最小样本数、累加精度以及 F12 epsilon 的值和单位。已有 SPEC 或锁定源码规定的直接沿用；未规定的项目描述量在实现时作出技术选择并入版本和 fixture，不依赖环境默认值。阈值不是全数据估计出的“最优”值；用于真实运行的 age/间隔等必要配置缺失时明确报错，不使用测试值补齐。

### 7.2 形态后端

catch22 使用 `pycatch22==0.4.5` 的 canonical long names，22 个指标 ID 按 [Step 05 SPEC §6](steps/05_statistical_features/SPEC.md#6-catch22-基线22-项完整保留) 原样登记。完整基线与 custom_subset 分开标记；选一项只调用相应函数，禁止先调用全集再丢列。

tsfeatures 指 R 包，首次实施锁定精确包版本、源码 revision/hash、R 与依赖版本，不能以 Python 同名包自动等价。本文不先选未验证的精确 R 版本；05-B 的交付必须作出并记录该技术选择，无需用户再次决定。8 个指标如下：

| metric ID | 数值契约 |
|---|---|
| `tsf.x_acf1` | ACF lag1 |
| `tsf.x_acf10` | 1～10 阶 ACF 平方和 |
| `tsf.x_pacf5` | 1～5 阶 PACF 平方和 |
| `tsf.arch_r2_l12` | demean=TRUE、lags=12 的平方序列回归 R²；不是 nR²/p-value |
| `tsf.spectral_entropy` | 固定 R 实现的 AR/Burg 谱估计、平滑及归一化；不能替换 histogram/Welch 熵 |
| `tsf.stability_w60` | 固定源码标准化和 60 点块均值方差 |
| `tsf.lumpiness_w60` | 固定源码标准化和 60 点块方差的方差 |
| `tsf.crossing_points` | 相邻 `x<=median` 布尔侧别改变次数；ties 归低侧 |

Python 移植须先取得**实际固定 R 参考运行**的 golden：记录输入文件/hash、生成脚本、backend/依赖锁、参考输出与状态。按指标冻结 atol/rtol（比较式 `abs(actual-ref)<=atol+rtol*abs(ref)`），离散次数精确相等；容差必须有数值依据，不能跑不过就放宽。覆盖随机固定 seed、周期、AR、异方差、ties、短/常量/近常量和失败样本；参考不可估不能换 0。

后端不可用时交付锁定/适配/测试进展并保留阻断项，不伪造 golden。不要自动安装系统 R；优先项目隔离环境，系统变更遵守运维规则。算法输入单位、输出单位、lag 单位、内部标准化、数值失败原因及耗时必须入审计。库升级产生新实现身份。

## 8. 按 spec 的实现包与硬验收门

以下测试文件名是建议；实现时使用真实文件/selector 记录，不把建议命令当已通过证据。每个增量均包含：schema/序列化、成功、失败、空/全 NA、lineage、时间边界和受影响回归。

| 增量 | 必须交付的实现包 | 阻止进入下一增量的验收门 |
|---|---|---|
| 01-G 收尾 | 核对已有 RPC/CLI、资源限制与测试，补记录/提交 | 多客户端查询不阻塞于计算；断连幂等；SIGKILL receipt 恢复；旧 CLI；最新全套 pytest/Ruff |
| 02-A | adapter、metadata 模板/校验、gap/reset/native interval manifest、真实小范围 reader 示例 | 单个真实合约/交易日凭 refs 可读；原 hash 不变；缺 metadata/坏 manifest/零有效记录诊断；隔离 refs 可追溯；不依赖 tick 的路径可运行 |
| 03-A | 领域 dispatch/源 reader、基础 primitives、质量 schema/发布验证 | 5～10 行手算；tick 生效边界/最长前缀/未知；session 间隔；metadata 变身份；未来前缀不变；真实 Step 02 消费 |
| 03-B | 50 目录、独立 mask、walk、reset/interval 证据 | family 数 `1,5,5,15,15,3,3,3`；不规则价距；q25/50/100；部分末档；坏深档保留 L1；不重定义 q；原生量不差分；[全部适用 VERIFY](steps/03_research_dimensions_and_primitives/VERIFY.md) |
| 04-A | 多输入 recipe/时间对齐、窗口算子、432 目录/ready 子集 | family 数 `8,40,40,120,120,24,24,24,8,8,8,8`；均值顺序；等时端点/age；中途 reset 回升；缺口恢复；F06 比例；pending 选择；[适用 VERIFY](steps/04_business_features/VERIFY.md) |
| 05-A | 输入白名单、稳定网格/stride、区间分桶、完整 W/质量和显式描述 | 30m 中缺 1s 保留行并 NA；恢复完整历史；不跨 lunch/session；Run 起点变共同值不变；primitive-only 无04；不虚构零量 |
| 05-B | 后端锁、22+8 注册、按选择调用、实际 golden、long 结果/审计 | 原 R golden 对照；ARCH R²/谱熵/ties/块缩放；未选函数 spy 零调用；常量/失败原因；未来追加不改过去；[全部适用 VERIFY](steps/05_statistical_features/VERIFY.md) |
| E2E A/B/C | 真实授权小范围完整工具闭环、故障测试 workspace、验收报告 | 第 10 节所有已具备前提的场景通过；真实 metadata 缺失的场景单独记待验，不冒称全 spec 完成 |

每个单元内先冻结最小公共契约，再实现 reader/算子/发布/查询接通，最后验收；不要跨 stage 同时写实现。02-A 提供真实上游后，后续不能只跑合成测试而省略真实集成。

## 9. 数值验收用例与不变量

测试应从独立手算/参考结果生成预期，不能复制实现函数来算 expected。fixtures 序列化为实际 Parquet/manifests，并在 fresh workspace 经过公共服务发布和读取。

| 测试 ID | 最小构造 | 必须断言 |
|---|---|---|
| N01 | L1 bid=100、ask=102，fixture tick=1，数量6/2 | mid=101、spread=2、L1 imbalance=0.5；这是合成规则，非真实 tick |
| N02 | 两快照空间深度 B/A 分别9/1与1/1，等快照权 | OBI 均值=(0.8+0)/2=0.4；不得得到聚合比8/12 |
| N03 | buy ask价101/103、量2/4，q=3 | VWAP=(2×101+1×103)/3；只消耗第二档1单位 |
| N04 | 中间深档无效、L1有效；另造跳价盘口 | L1 mid/spread有效；完整五档q依赖无效；tick空间和档数不同 |
| N05 | `t-L` 恰有有效记录、其前后各一条 | 取等时；删等时取之前；仅之后无合法端点；age等于阈值允许、超过无效 |
| N06 | 窗口累计量100→2(reset)→120 | 相关flow无效，即使端点差为正；无关spread不受该reset污染 |
| N07 | F06 一条可完成、一条容量不足 | 结果为可完成值，n_total=2/n_valid=1；不因该不可估快照制造源gap |
| N08 | 固定1s网格30m窗口中间一格NA | 评估行保留、指标NA；不把两端拼接；子段不冒充父窗口 |
| N09 | 累计量/增量 warning-only，数值/连续性其余合法 | 不独立BLOCKED/NA，保留各警告计数；再加真实缺口时相关值才失效 |
| N10 | source区间跨多个bin，起点已知/未知两版本 | 端点归属审计保留跨度；未知不能假设上一accepted为起点，不能拆比例假成交 |
| N11 | metadata/采样/版本改变与仅alias改变 | 前者新定义或计算身份不误复用；后者未变定义不被集合身份隔离 |
| N12 | 完整session计算vs改变Run起点/扩大日期/追加未来 | 共同评估点value、mask、端点与输入规则一致；比较覆盖实际共同范围 |
| N13 | 选一metric、选空集合、选pending+无关ready | 未选函数零调用；NO_WORK；pending局部BLOCKED且ready可完成 |
| N14 | 统计窗口常量/ties/近常量/短样本 | 原单位描述保留；形态按项不可估原因；crossing精确计数；不加噪声/填0 |

另验 UTC/session/交易日、授权范围、input refs/hash 和多输入错位拒绝。浮点值按预先声明的精度比较，状态/计数/refs/离散次数精确比较。gap 恢复边界同时测试恰落在左开边界和右端命中的情况，不能只测窗口中部。

## 10. 最终闭环、资源和证据

### 10.1 A/B/C 与故障

- A：至少三个有限候选，只选择两个 ready 业务组合及少量统计项；核对实际 provider 调用、依赖 ledger、未选状态、数值与 NA/覆盖后追加有 scope/evidence 的 Review。
- B：同冻结集合扩大到含 A 完整交易日的范围；验证旧分区 hash/health 后复用，只补缺失；producer 不变，parent/adopted refs 不扩大权限。
- C：同范围新 FeatureSet 增加少量 tsfeatures；未变定义复用，只调用新增指标及缺失依赖。比较列明共同/新增定义和日期。
- 分别验证仅 Step 04、仅 Step 05、primitive-only、禁止补依赖时 BLOCKED、空选择 NO_WORK、全 NA/零观测成功处理。processed/observed/valid/current available 分开，不按文件存在推断计算完成。
- 在测试 workspace 注入发布前、receipt 后、DB 提交后中断，以及坏/丢文件副本；resume 保留 attempts，恢复无双结果，修复发布新结果不覆盖原证据。多客户端查询使用真实 owner/CLI。

真实小范围选择必须记录合约/日期/session、源refs、split授权、metadata版本及预算。缺真实 metadata 时继续完成不依赖字段的路径，tick相关真实集成和完整 A/B/C保留待验；fixture tick不算替代。F07/F08未决时最终可交付“ready 子集闭环”，不能声称完整432项数值。

### 10.2 资源约束

目前 01-G 代码护栏为 planner 50,000 work units/20,000 partitions、coverage 20,000 cells；effective budget取Study/request与服务上限最小值并冻结。请求8MiB、响应16MiB；RPC metadata page≤100、数值≤500行/100refs，查询20s；具体以当次 owner.status/实现为准。超限明确拒绝，不偷偷挑子集执行。新增依赖也计预算，建图前预检且扩展时逐节点检查。

这些不是操作系统硬内存隔离。当前无法兑现的 CPU/内存预算须按既有契约显式拒绝；不能把队列/行数上限描述为RSS硬限制。领域 reader 按列投影、分区过滤和 bounded batches 迭代，窗口只保留必要 session/history状态；避免对全源 `read_table().to_pylist()`。记录真实 wall time、peak RSS/测量方法、扫描行数/字节、输出大小和缓存命中。小范围验收不外推全量性能。

### 10.3 每个增量的证据包

在对应 Step RESULTS 追加：增量ID、git/runtime/后端锁、实际源与metadata/split refs、选择与依赖、产物refs/hash、精确测试/CLI命令、实际结果、NA/警告/缺口/覆盖、资源和限制。同步 EXECUTION_SPEC 第8节、IMPLEMENTATION_STATUS 和 STEP_IMPLEMENTATION_TRACKER；不覆盖原Step02历史证据。

建议验收报告机读字段：`increment_id, code_revision, source_refs, metadata_refs, feature_set_ref, run_refs, review_refs, artifact_refs, commands, assertions, failures, blocked_checks, resource_measurements, next_increment`。实际 schema 在实现报告工具时确定，不预建空报告系统。

命令使用当时可用 Python 环境。当前可用入口为 `/data/src/research/.venv/bin/python -m pytest ...` 与 `-m ruff check quantsage_discovery scripts tests examples`；不在外部共享环境安装新增包。各增量先定向测试，受影响回归通过后运行规定检查；最终全套pytest/Ruff加真实小范围验收。Unix socket测试如被沙箱限制，按已有授权与运维规则使用允许的运行方式并如实记录。

## 11. Subagent 工作单与主代理合并规则

按用户要求使用 GPT-6 Luna、xhigh。一次只实施一个 spec 增量，可在该增量内拆为实现、验收与只读审查；不同stage的实现不并行。共享目录内分配文件归属，同一个源文件只有一个写入代理。

可复制工作单：

```text
增量：<EXECUTION_SPEC 中唯一 ID>
基线：<HEAD、工作区已有改动及文件归属、上一步 RESULTS>
必读：AGENTS + COMMON_CONTRACTS + EXECUTION_SPEC 对应节
      + COMPUTATION_PLAN 对应节 + 当前 Step SPEC/IMPLEMENT/VERIFY
目标：<公共行为、输入/输出 schema、实际 provider 调用路径>
允许修改：<明确路径列表>；其他代理文件和冻结行情只读
计算规则：<公式/窗口/端点/gap/availability/身份的具体条款>
验收：<第8/9节测试ID、当前Step VERIFY、真实集成范围>
返回：实际变更、命令/结果、refs/hash、限制、未完成项、下一交接
禁止：跨stage预建、填补pending、伪造metadata/golden/available_time、
      全算后过滤、跳过真实集成却标完成、改冻结产物、覆盖他人改动
```

主代理负责依赖/契约冲突、集成与最终状态；测试代理独立验证公开入口和序列化产物，不能为迎合实现修改预期公式。读审查核对需求R01–R16、模块边界和未测风险。source freeze后再运行依赖代码指纹的plan/execute/cache测试：当前指纹包含package源码，测试期间修改源码会合法触发PLAN_CHANGED，不能删除该保护让测试变绿。

接续顺序为：01-G → 02-A → 03-A → 03-B → 04-A → 05-A → 05-B → E2E（ready-subset 完成）。每步只在自身验收证据齐备后推进；外部输入不足的验收明确保留，不把待验写成通过。02-A 的真实 metadata 补充是后续 DataSource 版本操作，不重做行情接入。
