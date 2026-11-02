# Step 03 — Research Dimensions & Primitive Factory

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

参见 [L2 Schema](../../references/gov_bond_l2.schema.md)。当前 `part-0.parquet` 包含逐条 `TRADINGTIME`、五档盘口和区间/累计成交字段，可经 Stage 02 质量检查后构造盘口、报价间隔与窗口价格等研究视图。功能仍受日历、tick size、有效报价、成交量语义与可用性约束；缺少逐条 `available_time` 时保留 `availability_unverified`。`ORDERRATE` 是五档委比，不能冒充一档 OBI；快照不提供真实逐笔订单或撤单。

## Primitive 目录（MVP）

| Primitive | 定义 | 必需数据 / 限制 |
|---|---|---|
| `mid` | `(bid_price_1 + ask_price_1)/2` | 正常有效盘口 |
| `spread_ticks` | `(ask_price_1 - bid_price_1)/tick_size` | tick size 已知 |
| `depth_bid_1`, `depth_ask_1` | 最优档可见量 | 快照深度 |
| `depth_imbalance_1` | `(B-A)/(B+A)` | B=bid_size_1，A=ask_size_1；B+A>0，否则 null |
| `microprice_proxy` | `(ask_price_1*B + bid_price_1*A)/(A+B)` | 为加权价代理，并非真实成交价 |
| `mid_return` | mid 的对数/差分变化 | 非正价格禁止对数；保留 tick-based 备选 |
| `quote_interarrival` | 相邻行情更新的时间差 | 先按源+合约排序、处理重复 |
| `quote_update_count` | 给定窗口内更新计数 | 需区分事件/时钟时间 |
| `volume_increment` | 相邻累计量差或原生增量 | 原始 TradeVolume 已为区间量；TotalVolume 才做差分，处理日/会话边界 |
| `relative_price_move` | 经 tick/风险尺度合理归一的两合约变化差 | 两边都在 staleness 容忍范围内 |

`order_flow_imbalance` 只在输入足以支持明确 OFI 定义时启用；在一般 L1 快照中可输出 `snapshot_ofi_proxy`，但命名、文档和限制必须清楚。无法获得真实撤单和成交方向时不得宣称还原真实订单流。

## 三种时间轴

1. `clock_time`: 10s/30s/1m/5m 的实际时长窗口。
2. `event_time_index`: 最近 N 次**有效行情更新**；不能把按行的计算误写为秒。
3. `activity_time`: 当可信成交量支持时的成交量 bar；否则提供 quote-count bar 并明确这不是 volume bar。

Primitive 的计算均遵守 session boundary。对过去窗口统计，窗口右端包括当前已知数据，左端排除未来；某时间窗没有有效观测则输出 null/quality flag，不强制制造零变化。

## Factory 的算法

1. 从 passport 推断可以启用的维度，并为未启用维度写原因。
2. 由 Schema Adapter 创建 Primitive AST/算子依赖图。
3. 分合约/交易日计算，并输出 long 或 wide Parquet；保留 `event_time/available_time`。
4. 对同一 primitive 输出 summary：missingness、覆盖率、分布、极值、单位、有效样本数。
5. Registry 保存 `primitive_id, definition, input_fields, unit, time_basis, availability_rule, implementation_version, data_hash`。

## 验收

- 手算 5–10 行不规则行情的 mid、spread、OBI、interarrival，与程序一致。
- 跨 session 的 previous-value 不串联；累计成交量 reset 被正确处理；缺可用时间时明确警告。
- 当没有成交量和三档订单簿时，只生成有依据的 primitives；`disabled_reason` 可查。
- 对 TF/T/TL 的异步报价测试 quote_age/staleness；不可用时输出 null 及原因，不输出貌似可信的数。

**前置**：02；**下一步**：04。
