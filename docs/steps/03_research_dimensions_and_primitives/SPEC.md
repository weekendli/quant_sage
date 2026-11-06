# Step 03 — Research Dimensions & Primitive Factory

## 当前收敛范围：Step 02–05 数据与特征闭环

本步只按 Run 已解析的 FeatureSet 依赖闭包计算基础序列。输入是 Step 02 的冻结数据源 refs、所选 primitive 定义及输出/历史范围；输出为 Feature(kind=primitive) 对应的 FeatureResult、有效性和 lineage。维度目录是数据可用性说明，不要求自动研究规划、全空间扫描或把每个内部节点都持久化。

L2 公式目录保留完整合法域，实际只算选中项及必要依赖。当前重点为已选 L2 组合需要的快照量、累计量/reset 与时间信息；跨合约因子、activity bars 和自动维度发现列为后续扩展，不作为此最小闭环的交付门槛。

统一管理字段见 [domain.detail.md](../../domain.detail.md)，当前调度边界见 [WORKFLOW_STAGES.md](../../WORKFLOW_STAGES.md)。旧 Registry 名称表示 Feature/FeatureResult 的分类视图，不要求新增独立领域仓库。Study、FeatureSet、Run、Review 的通用管理由 Step 01 承担，领域步骤只实现本步算子与结果契约。


## 研究维度不是先凭空写假设

从 `DatasetPassport` 中推导**可研究什么、不可研究什么**，形成 `ResearchDimensionCatalog`。维度是研究视角，不直接等于未来收益标签。

| Dimension | 数据证据 | 可研究问题 |
|---|---|---|
| Price Dynamics | bid/ask/last、tick size | 价格变化、惯性/反转、价格停滞 |
| Liquidity | 各档 bid/ask size、spread | 深度稀薄、流动性骤降、补充速度代理 |
| Trading Activity | 可靠成交字段、报价更新间隔 | 活跃度、成交量冲击、更新强度 |
| Volatility | price/mid sequence | 波动聚集、尾部冲击、尺度依赖 |
| Cross-Instrument | 有同步的 TF/T/TL | 相对价格创新、领先滞后、共同因子 |
| Temporal Structure | timestamp/session/calendar | 开盘/收盘效应、会话内形态 |
| Structural Complexity | 足够长的时序 | entropy、motif、change point |

每个维度存 `required_fields`, `supported_time_bases`, `constraints`, `enabled/disabled_reason`, `candidate_primitives`。不要因为用户想分析成交订单流就虚构不存在的逐笔数据。

## 本数据源的可用性

参见 [L2 Schema](../../references/gov_bond_l2.schema.md)。当前 `part-0.parquet` 包含逐条 `TRADINGTIME`、五档盘口和区间/累计成交字段，可经 Step 02（workflow Stage 01）质量检查后构造盘口、报价间隔与窗口价格等研究视图。功能仍受日历、tick size、有效报价、成交量语义与可用性约束；缺少逐条 `available_time` 时保留 `availability_unverified`。`ORDERRATE` 是五档委比，不能冒充一档 OBI；快照不提供真实逐笔订单或撤单。

## Primitive 可用目录（按依赖选择，非全量执行要求）

| Primitive | 定义 | 必需数据 / 限制 |
|---|---|---|
| `mid` | `(bid_price_1 + ask_price_1)/2` | 正常有效盘口 |
| `spread_ticks` | `(ask_price_1 - bid_price_1)/tick_size` | tick size 已知 |
| `depth_bid_1`, `depth_ask_1` | 最优档可见量 | 快照深度 |
| `depth_imbalance_1` | `(B-A)/(B+A)` | B=bid_size_1，A=ask_size_1；B+A>0，否则 null |
| `microprice_proxy` | `(ask_price_1*B + bid_price_1*A)/(A+B)` | 为加权价代理，并非真实成交价 |
| `mid_return` | mid 的对数/差分变化 | 非正价格禁止对数；保留 tick-based 备选 |
| `quote_interarrival` | 相邻行情更新的时间差 | 先按源+合约排序、处理重复 |
| `quote_update_count` | 给定窗口内更新计数 | 窗口派生视图，归 Step 04/05 显式配置；Step 03 提供时间/有效观察输入 |
| `volume_increment` | 相邻累计量差或原生增量 | 原始 TradeVolume 已为区间量；TotalVolume 才做差分，处理日/会话边界 |
| `relative_price_move` | 经 tick/风险尺度合理归一的两合约变化差 | 两边都在 staleness 容忍范围内 |

`order_flow_imbalance` 只在输入足以支持明确 OFI 定义时启用；在一般 L1 快照中可输出 `snapshot_ofi_proxy`，但命名、文档和限制必须清楚。无法获得真实撤单和成交方向时不得宣称还原真实订单流。

## 国债 L2 feature pack：本步骤负责的拆解

本节将 [原始 L2 feature spec](../../references/gov_bond_l2_feature_spec.md) 的公共量和单快照公式分配到 **Step 03**；原文及公式保持不变。**Step 03 = workflow Stage 02 `primitives`**，不是 workflow Stage 03 features。最终带 horizon 的 factors 由 [Step 04](../04_business_features/SPEC.md) 负责。

### 静态规则与输入契约

- 复用 Step 02 的标准化快照、质量标记、真实合约身份与连续 `session_id`，不在本步另造交易日历或覆盖上游判定。
- tick size 从用户补充的行情 metadata 精确版本读取，按原规格 §2 的 `SYMBOL + TRADINGDATE` 历史映射校验；最长前缀顺序 `TL,TF,TS,T`。记录 metadata hash、来源和适用日期；未知产品/日期不能套当前 tick。可补充 metadata 而不重做 Step 02 行情接入，不把历史 Passport 的未知状态改写为已知。
- 原始行情输入仅使用原规格白名单。`TRADEVOLUME` 原生区间量直接透传为 `trade_volume_interval`，**禁止再次差分**；`BUYVOL/SELLVOL` 作为累计主动成交代理量保留，不能宣称为真实订单流。
- 复用排序/去重契约并保留行血缘；不规则时间戳、同时间多条快照不得无规则合并。当前快照失效时输出 null 和原因，不以之前值冒充当前值。

### 公共量与领域 primitive 目录

下表名称是本步骤的规范化 primitive ID 建议；Registry 必须记录与源字段、原文公式的映射。`d ∈ {0,1,2,3,5}`，`q ∈ {25,50,100}` 对应 `α ∈ {0.25,0.50,1.00}`。本步骤没有 8 个 horizon 的展开。

| Primitive / 参数 | 原规格位置 | 计算责任 |
|---|---|---|
| `tick_size`, `mid`, `spread_ticks` | §2、§4.1–4.2 | 历史 tick 映射、mid、touch spread |
| `tick_distance_bid/ask(level)` | §4.3 | 每档相对各自 touch 的 tick 距离；保留档位有效性 |
| `space_depth_bid/ask(d)` | §4.4 | 按 tick distance 选档求深度；不能用“前 d 档”替代 |
| `visible_depth_bid/ask_5`, `depth_ref`, `target_size(q)` | §3.2 | 五档双侧深度、双侧均值容量、逐快照相对 size |
| `obi_snapshot(d)` | F02 | 单快照双侧空间深度比值 |
| `weighted_depth_bid/ask(d)`, `wobi_snapshot(d)` | F03 | 距离权重 `1/(1+δ)`、加权深度与比值 |
| `coverage_bid/ask(d,q)`, `min_coverage_snapshot(d,q)` | F04 | 对各侧覆盖率先截到 1，再取较弱侧 |
| `capped_capacity_bid/ask(d,q)`, `capacity_imb_snapshot(d,q)` | F05 | 双侧容量分别截到当前目标 size，再求 imbalance |
| `can_fill_buy/sell(q)`, `vwap_buy/sell(q)` | F06 | 按当前五档逐档消耗，完整覆盖才计算 VWAP |
| `size_spread_snapshot(q)` | F06 | 同一快照双侧 VWAP 差 / tick |
| `fill_distance_buy/sell(q)`, `fill_dist_asym_snapshot(q)` | F07 | 最小完整成交档位的 tick 距离及原定义不对称比值 |
| `walk_cost_buy/sell(q)`, `walk_cost_asym_snapshot(q)` | F08 | 相对 mid 的双侧 tick 成本及原定义不对称比值 |
| `active_buy_cumulative`, `active_sell_cumulative`, reset 标记 | F09/F12 输入 | 累计值透传；按相邻观察记录 reset 与边界，不在此计算 horizon flow |
| `trade_volume_interval` | F10 输入 | 原生区间成交量透传；无效/缺失不能静默归零 |

该目录包含公开 primitives 和内部计算依赖，不要求所有条目都成为独立 artifact。公开序列按业务解释与下游复用需求登记（如 mid、空间深度、OBI、VWAP、原生成交量及质量信息）；权重、逐档消耗量、截断容量等可作为内部节点。中间依赖可以作为共享 AST 节点缓存；须保证 Step 04 所需的终端 primitives、有效性和 lineage 可持久化访问，不要求把每个档位消耗数组都写成冗余表。`target_size(q)` 按当前快照的 `depth_ref` 计算，不使用历史均值或未来容量。

### 公式分解：公共量

下面仅重述原规格中已有公式，不引入 horizon。设买/卖价格为 `b_i/a_i`、数量为 `B_i/A_i`、tick 为 `τ`；所有量均取当前快照 t，省略 t 下标。深度求和只使用源质量契约允许的有效档位。

```text
mid = (b_1 + a_1) / 2
spread_ticks = (a_1 - b_1) / τ
δB_i = (b_1 - b_i) / τ
δA_i = (a_i - a_1) / τ
DB(d) = Σ_i B_i * 1[δB_i <= d]
DA(d) = Σ_i A_i * 1[δA_i <= d]
DB5 = Σ_i B_i
DA5 = Σ_i A_i
depth_ref = (DB5 + DA5) / 2
q(α) = α * depth_ref
```

`DB5/DA5` 是五档可见容量，与 `DB(d5)/DA(d5)` 不同：第五档可能超出 5 ticks。`d0` 按原公式纳入所有距 touch 为零的有效档；若输入允许同价重复档，应记录该情况，不能无条件用第一档数量替代求和。

### 公式分解：F01–F08 单快照描述

| 原 family | 单快照 primitive 公式 | 合法参数 / 命名定义数 |
|---|---|---|
| F01 | `spread_ticks` 如上 | 无 d/q / 1 |
| F02 | `obi_snapshot(d)=(DB(d)-DA(d))/(DB(d)+DA(d))` | D / 5 |
| F03 | `WB(d)=Σ_i B_i*1[δB_i<=d]/(1+δB_i)`，`WA(d)` 同理；`wobi_snapshot(d)=(WB-WA)/(WB+WA)` | D / 5 |
| F04 | `coverageB=min(1,DB(d)/q)`，`coverageA=min(1,DA(d)/q)`；`min_coverage_snapshot(d,q)=min(coverageB,coverageA)` | D × Q / 15 |
| F05 | `CB=min(DB(d),q)`，`CA=min(DA(d),q)`；`capacity_imb_snapshot(d,q)=(CB-CA)/(CB+CA)` | D × Q / 15 |
| F06 | `size_spread_snapshot(q)=(vwap_buy(q)-vwap_sell(q))/τ` | Q / 3 |
| F07 | `fill_dist_asym_snapshot(q)=(dist_buy-dist_sell)/(1+dist_buy+dist_sell)` | Q / 3 |
| F08 | `cost_buy=(vwap_buy-mid)/τ`，`cost_sell=(mid-vwap_sell)/τ`；`walk_cost_asym_snapshot(q)=(cost_buy-cost_sell)/(cost_buy+cost_sell)` | Q / 3 |

`D={0,1,2,3,5}`，`Q={25,50,100}`。合计为 **50 个命名单快照描述**，不含共享输入和私有中间节点，也不宣称它们统计独立。语义重复可复用计算，但保留业务引用；不得将本表自动展开成 D × Q 的全局乘积。

双侧执行的共享依赖定义为：

```text
x_buy_i = min(A_i, max(0, q - Σ_{j<i} x_buy_j))
x_sell_i = min(B_i, max(0, q - Σ_{j<i} x_sell_j))
can_fill_buy = 五档有效 ask 容量完整覆盖 q
can_fill_sell = 五档有效 bid 容量完整覆盖 q
vwap_buy = Σ_i a_i*x_buy_i / q     # 仅完整覆盖且 q>0 时
vwap_sell = Σ_i b_i*x_sell_i / q   # 仅完整覆盖且 q>0 时
j_buy = min{j: Σ_{i<=j} A_i >= q}
j_sell = min{j: Σ_{i<=j} B_i >= q}
dist_buy = (a_j_buy - a_1) / τ
dist_sell = (b_1 - b_j_sell) / τ
```

正的 fill/walk asymmetry 表示向 ask 买入侧需要更深穿透/更高成本；负值对应 bid 卖出侧。`q100` 是双侧平均容量的 100%，不保证较弱侧也能覆盖，因此其 NA 可由定义自然产生。

### F09–F12 的输入边界

这四个 family 不要求本步骤输出同名单快照比值。交付以下输入即可保持原公式：

- F09：`active_buy_cumulative=BUYVOL`、`active_sell_cumulative=SELLVOL`、session/reset/有效性标记。
- F10：`trade_volume_interval=TRADEVOLUME`、`depth_ref`；成交区间端点及源行引用仍保留。
- F11：`mid`、`tick_size`、源观察时间/可用时间/质量状态。
- F12：复用 F09–F11 的输入；窗口净 flow、平均深度和价格变化在 Step 04 构造。

窗口端点差不是单快照量；不能只保存逐区间归一化 flow imbalance 后丢弃累计量，以免无法恢复窗口成交权重。

### 数据有效性与边界

沿用原规格 §9：有效 touch、非负数量、价格层级单调、零值/sentinel 档及更深档的 walk 限制；禁止外推 L6+、重复 L5 或假定隐藏流动性。双侧五档不能完整覆盖目标 size 时，相应完整执行指标为 NA，原因 `insufficient_visible_depth`。

零分母、零目标规模、未知 tick 和无效依赖分别输出 null + reason，不夹加 epsilon 改写 F02/F05/F08 的比值。F12 的 epsilon 属于 Step 04 的原定义。各 primitive 的有效掩码独立保存：walk 不可用不意味着 spread/OBI 必须同时失效。

累计字段 reset 标记必须保留，即使之后累计值回升到重置前的水平，下游仍能发现窗口内有 reset。所有时间字段遵守 COMMON_CONTRACTS；无证据的 available_time 不得等同于 event_time。

`DEPTH_BOOK_INVALID` 保留为上游警告，执行按具体依赖生成掩码：L1 有效时 mid、spread 和一档深度可用；空间深度须验证所有可能纳入空间的档位，不能静默跳过未知档；依赖完整五档的 depth_ref/target_size/VWAP 等按各自完整性要求失效，不以残缺容量重定义 q。成交量未验证和对账不一致仅传递警告，不单独使 primitive 失效。

推荐复用 [gap_policy_v1](../../IMPLEMENTATION_DESIGN.md#64-缺少行情的推荐处理机制)：从上游隔离/覆盖 refs 关联缺口及 reset 证据；不把上一条保留记录当作原生成交区间起点，不跨缺口推断累计量连续，不插值或补零。

### 交付边界

输出 `PrimitiveRegistry`、按真实合约/交易日分区的 primitive Parquet、tick 映射引用、依赖/单位/有效性契约及 summary。注册定义与实际有效样本数分开记录。

本步骤不计算最终 `__h1s...__h60m` factors、不拟合 Atlas baseline，也不输出 absorption / fragility 等市场状态结论。Step 04 计算业务时间窗口特征，随后 Step 05 对选定业务特征及 primitives 计算统计特征；条件基线为可选后续扩展。

## 时间轴能力（当前按显式需求启用）

1. `clock_time`: 10s/30s/1m/5m 的实际时长窗口。
2. `event_time_index`: 最近 N 次**有效行情更新**；不能把按行的计算误写为秒。
3. `activity_time`: 当可信成交量支持时的成交量 bar；否则提供 quote-count bar 并明确这不是 volume bar。

Primitive 的计算均遵守 session boundary。对过去窗口统计，窗口右端包括当前已知数据，左端排除未来；某时间窗没有有效观测则输出 null/quality flag，不强制制造零变化。

## Factory 的算法

1. 从 passport 校验所选定义所需字段/维度，为不可用项写原因，不自动生成研究任务。
2. 由 Schema Adapter 创建 Primitive AST/算子依赖图。
3. 按 Run 的已选定义和分区计算，并输出 FeatureResult 关联的 long 或 wide Parquet；保留 `event_time/available_time`。
4. 对同一 primitive 输出 summary：missingness、覆盖率、分布、极值、单位、有效样本数。
5. Registry 保存 `primitive_id, definition, input_fields, unit, time_basis, availability_rule, implementation_version, data_hash`。

## 验收

- 手算 5–10 行不规则行情的 mid、spread、OBI、interarrival，与程序一致。
- 跨 session 的 previous-value 不串联；累计成交量 reset 被正确处理；缺可用时间时明确警告。
- 当没有成交量和三档订单簿时，只生成有依据的 primitives；`disabled_reason` 可查。
- 后续启用跨合约扩展时，再验收 TF/T/TL 的 quote_age/staleness；不启用时不虚构结果。

**前置**：02；**下一步**：04。
