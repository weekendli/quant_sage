# Step 02 — Data Onboarding & Quality

## 目的

让系统**正确理解输入到底是什么数据**。输入约 30GB Parquet/CSV 原始国债期货价格与行情快照，也允许其他市场通过适配器接入。

## 输入 / 输出

- 输入：多文件 Parquet/CSV；字段 mapping；exchange/calendar/contract metadata；用户声明的语义信息。
- 输出：标准化行情 Parquet（按 `instrument/trade_date` 分区）、`DatasetPassport`、`QualityReport`、`data_snapshot_id`、异常隔离表。

## 标准必需/可选字段

| 名称 | 说明 | 要求 |
|---|---|---|
| `event_time` | 交易所/源事件发生时刻 | 必需；带明确时区 |
| `available_time` | 本系统最早可获取时刻 | 有则保存；没有则显式 `unknown` |
| `instrument_id` | 原始具体合约 | 必需 |
| `trade_date`, `session_id` | 交易日、交易会话 | 必需，由日历明确计算 |
| `bid_price_1`, `ask_price_1` | 一档报价 | MVP 必需（若有订单簿） |
| `bid_size_1`, `ask_size_1` | 一档可见量 | MVP 必需（若有订单簿） |
| `last_price`, `volume`, `turnover`, `open_interest` | 交易相关字段 | 可选，需明确累计/增量 |
| `source_sequence`, `exchange`, `source` | 数据源元信息 | 可选但推荐 |

不符合 MVP LOB schema 的纯成交行情可走单独 `trade_only` adapter，不应伪造 bid/ask。

## 必须检查的语义与质量

1. 文件与字段：类型、单位、tick size、合约乘数、缺失值标识、币种、编码。
2. 时间：重复时间、排序错误、非交易时段、日切换、时区、夏令时（如适用）、事件时间与入库时间偏移。
3. 行情：负数价格/数量、交叉盘口、无效价差、重复快照、异常跳变、静态盘口长时间未更新。
4. 成交量：`volume` 是每笔增量还是 session/day 累计；累计量在新会话清零时不能当负成交量。
5. 缺口：仅在“有变化才上报”模式下，不可以把没有新帧当作没有交易；保留更新间隔和 `staleness`。
6. 合约：保留原始具体月份合约，连续合约只能作为额外研究视图；展期规则禁止用未来成交量挑选主力。
7. 可用性：`available_time` 未知时，对精细跨源领先滞后因果解释标记 `availability_unverified`。

## 质量输出

`QualityReport` 按合约与交易日记录 `row_count, duplicate_rate, invalid_quote_rate, null_rate, time_gap_quantiles, crossed_book_rate, time_reorder_count, stale_quote_segments, quarantined_rows`；每个质量指标有具体公式及分母。

Quality Gates：阻断型错误（schema/时区/严重时间混乱）；隔离型错误（不合理价格/交叉盘口）；警告型问题（个别缺口/长尾、未提供成交字段）。阈值均可配置，不使用未经验证的固定硬编码。

## 时间同步与 As-of

- 原始流是 **change-driven**，不先重采样为固定帧并向前填充覆盖原始信息。
- 当需要 T/TF/TL 跨合约计算，使用按 `event_time` 的 as-of join，并记录 `quote_age` 和是否超出 `max_staleness`；不可无限 forward fill。
- 可提供 clock-time grid 的研究视图；每个 tick 的最新值必须标记多久前出现，以及该值当时是否已可获得。
- 不可在交易日间、停盘区间、跨合约展期时隐式向前填充。

## 验收

- 构建合成输入覆盖乱序、同时间戳重复、跨会话、累计成交量重置、缺行情、交叉盘口、缺少可用时间。
- 所有清洗规则有计数和例子，原始数据不原地修改；错误记录可以追踪到文件和行/序列。
- 读取 30GB 数据不要求一次装入内存，分区处理/streaming 策略明确；提供性能 smoke-test 脚本。
- 标准化数据可被后续 Stage 使用，且 `DatasetPassport` 能解释没有哪些字段、因此哪些分析能力不可用。

**前置**：01；**下一步**：03。
