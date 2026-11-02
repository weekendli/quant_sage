# Step 02 — Data Onboarding & Quality

## 目的

让系统**正确理解输入到底是什么数据**。输入约 30GB Parquet/CSV 国债期货 L2 行情快照，也允许其他市场通过适配器接入。

## 输入 / 输出

- 输入：多文件 Parquet/CSV；字段 mapping；exchange/calendar/contract metadata；用户声明的语义信息。
- 输出：标准化行情 Parquet（按 `instrument_id/trade_date` 分区）、`DatasetPassport`、`QualityReport`、`data_snapshot_id`、异常隔离表。

## POC 交付与日历来源

POC 的下游使用场景、文件清单、入口字段和发布条件以 [DELIVERY.md](DELIVERY.md) 为准。复用已有 [源 schema JSON](../../references/gov_bond_l2.schema.json)，只规定确定性字段映射及必要派生列，不另建一份业务字段字典。DatasetPassport 和 QualityReport 在 POC 中内嵌于 onboarding.json，数据与冻结日历通过 ArtifactManifest 引用。

交易日历从用户提供的 Marketdata LAN API 获取：`https://192.168.1.102/marketdata-api/`，表为 `marketdata.reference.calendars`。先发现并确认适用于 CFFEX 国债期货的 calendar_id，再获取数据区间日历，保存 release、查询参数和结果 hash。示例 CN_STOCK 不作为本数据源的默认日历。API 说明尚未定义日内会话表，日内会话需来自经确认的 API 表或独立版本化规则；交易日与日内时段分别验证。

## 本数据源字段依据

先读取 [L2 Schema](../../references/gov_bond_l2.schema.md)、[机读版本](../../references/gov_bond_l2.schema.json)，并对照 [Word 字段说明](../../references/国债期货%20L2.docx) 与 [实际样本](../../references/part-0.parquet)。本次样本为 **3,178,989 行、66 列、26 个 row group 的逐条 L2 快照**，包含 Word 的全部 62 个业务字段和 4 个导入元数据字段。此前 240 行、35 列汇总表的描述已失效；当前 adapter 为 `l2_snapshots`。每行是行情观测，不是逐笔成交或委托记录。

实际源业务字段使用全大写，Word 的驼峰名称和 OCR 拼写不能直接用作物理列名。映射须显式声明、校验类型与冲突，不通过猜测自动纠正任意输入列名。

| 标准字段 | 样本实际字段 | 语义 |
|---|---|---|
| `instrument_id`, `exchange` | `SYMBOL`, `MARKET` | 具体合约、市场 |
| `trade_date`, `event_time` | `TRADINGDATE`, `TRADINGTIME` | 交易归属日与源事件时间 |
| `bid_price_1..5`, `ask_price_1..5` | `BUYPRICE01..05`, `SELLPRICE01..05` | 五档报价 |
| `bid_size_1..5`, `ask_size_1..5` | `BUYVOLUME01..05`, `SELLVOLUME01..05` | 五档可见量 |
| `last_price`, `open_interest` | `LASTPRICE`, `TOTALPOSITION` | 最新成交价、持仓存量 |
| `volume`, `turnover` | `TOTALVOLUME`, `TOTALAMOUNT` | 日内累计成交量/额 |
| `volume_increment`, `turnover_increment` | `TRADEVOLUME`, `TRADEAMOUNT` | 相邻观测区间量/额，不重复差分 |
| `source_event_unix_ms` | `UNIX` | 用于事件时间交叉核验，不是接收时间 |
| 原名保留 | `product`, `source_file`, `source_row`, `ingest_run_id` | 品种、源文件、源行号、上游导入批次 |

`TRADINGDATE` 为 `date32[day]`；`TRADINGTIME` 为无时区 `timestamp[us]`，Parquet 标记 `isAdjustedToUTC=false`；`UNIX` 为 `int64`，`SECURITYID` 为 string。价格为 decimal128(9,3)，数量/金额/比例保留各自 decimal 精度，详见完整 schema。nullable=true 与实测 null 是不同概念。

`available_time`、逐条 `ingested_time`、交易所 `source_sequence` 没有源字段；`session_id` 由确认的交易日历和日内会话规则共同派生。必须保留 `availability_unverified`，允许显式 `research_only` 模式。`ingest_run_id` 不能解析成每条行情的可用时刻，`source_row` 只能做来源定位和稳定回放顺序，不能赋予交易所序号含义。标准化及隔离记录都保留原 Parquet 文件 hash、row group/行位置，以及上述上游血缘字段。

`CONTINUESIGN` 仅为额外标签，不能覆盖 `SYMBOL`；源文件名也不能覆盖记录中的 `SYMBOL` / `product`。同合约同时间可以来自不同连续合约源文件，重复处理必须比较业务内容并保留来源。POC 不合并去重：无可靠交易所序号的重复事件键组全部隔离，报告重复或冲突；不能任取最后一条。

## 样本实测基线

[样本核验结果](../../references/part-0.profile.json) 记录全文件统计、复现脚本、工具版本及输入 hash。这里只描述该单文件，不将其当作全量约 30GB 数据的完整性结论。实测结果和待实现的验收检查分开，不能据此宣布本阶段已实现。

| 项目 | 全文件实测结果 |
|---|---|
| 交易日与合约 | 2026-01-05 至 2026-01-30，20 个交易日；T/TF/TL/TS，各含 2603、2606、2609，共 12 个合约 |
| 事件时间范围 | 本地无时区值 2026-01-05 09:29:00.400000 至 2026-01-30 15:24:14.200000 |
| 缺失值 | 66 列实测 null 数均为 0；不等同于不存在零值或哨兵值 |
| 事件时间核验 | 全部记录满足 `epoch_ms(TRADINGTIME 按无时区墙钟计算) - UNIX = 28,800,000`；支持 Asia/Shanghai 本地时间解释；事件日期与 TRADINGDATE 不一致数为 0 |
| 重复键 | `(MARKET, SYMBOL, TRADINGDATE, TRADINGTIME)` 重复组/额外行均为 0；`(ingest_run_id, source_file, source_row)` 重复组/额外行均为 0 |
| 一档盘口 | 买价大于卖价、锁盘、非正买卖价、负一档数量的记录数均为 0；未据此认证其余档位 |
| 成交与委比 | TRADEVOLUME 或 TOTALVOLUME 为负的记录数为 0；ORDERRATE 范围为 -0.9446 至 0.9811，超出 [-1,1] 的记录数为 0；未核验全部计算公式 |
| 日终字段零值 | SETTLEPRICE 为零 3,178,749 行；CLOSEPRICE 为零 3,178,509 行 |
| 物理顺序与间隔 | 按上游批次、源文件、市场、合约、交易日分组，物理输入中时间倒退/相邻同时间计数均为 0；正间隔 P50/P90/P99 为 0.5/1.5/13.5 秒，含日内休市，不能用作会话内 staleness 阈值 |
| 上游血缘 | 单一 source_file 为 `TFL2_2023_202607/TFL2_TAQ_202601.txt`，却包含全部四品种；source_row 范围 1 至 3,178,989，批次为 `20261101T164334Z-a29d3957` |

以上计数以原文件行作为观察单位，未清洗或去重；零计数仅适用于列出的条件。未核验完整业务内容重复/冲突、累计量与增量对账、全五档有效性或已确认日历下的交易时段。样本中存在 09:29 和 15:24 的记录，不能仅按常规连续交易时段截断；需查明集合竞价、盘后状态或源发布规则后分类。

逐条快照已经具备盘口计算所需字段；通过报价、时间和会话检查后可向后续 Stage 提供 mid、spread、深度与快照代理特征。交易日历已有 Marketdata API 来源，仍需确认所选 calendar_id 和日内会话规则；tick size、合约乘数、金额单位、零值/哨兵规则和发布机制分别记录确认状态。对 `SETTLEPRICE` / `CLOSEPRICE` 等零值，不可直接视为有效价格或据此隔离整行正常盘口；日终字段是否盘中已知需单独核实。

## 标准必需/可选字段

| 名称 | 说明 | 要求 |
|---|---|---|
| `event_time` | 交易所/源事件发生时刻 | 必需；带明确时区 |
| `available_time` | 本系统最早可获取时刻 | 有则保存；没有则为 null，Passport 标记 unknown |
| `instrument_id` | 原始具体合约 | 必需 |
| `trade_date`, `session_id` | 交易日、交易会话 | 必需；交易日保留源声明并经日历核验，会话由日历及版本化日内会话规则计算 |
| `bid_price_1`, `ask_price_1` | 一档报价 | MVP 必需（若有订单簿） |
| `bid_size_1`, `ask_size_1` | 一档可见量 | MVP 必需（若有订单簿） |
| `last_price`, `volume`, `turnover`, `open_interest` | 交易相关字段 | 可选，需明确累计/增量 |
| `source_sequence`, `exchange`, `source` | 数据源元信息 | 可选但推荐 |

不符合 MVP LOB schema 的纯成交行情可走单独 `trade_only` adapter，不应伪造 bid/ask。

## 必须检查的语义与质量

1. 文件与字段：类型、单位、tick size、合约乘数、缺失值标识、币种、编码。
2. 时间：重复时间、排序错误、非交易时段、日切换、时区、夏令时（如适用）、事件时间与入库时间偏移。
3. 行情：负数价格/数量、交叉盘口、无效价差、重复快照、异常跳变、静态盘口长时间未更新。
4. 成交量：`volume` 是每笔增量还是 session/day 累计；累计量在日切换或经源规则确认的会话边界清零时不能当负成交量；午间休市不自动推断清零。
5. 缺口：仅在“有变化才上报”模式下，不可以把没有新帧当作没有交易；保留更新间隔和 `staleness`。
6. 合约：保留原始具体月份合约，连续合约只能作为额外研究视图；展期规则禁止用未来成交量挑选主力。
7. 可用性：`available_time` 未知时，对精细跨源领先滞后因果解释标记 `availability_unverified`。

## 质量输出

`QualityReport` 按来源、合约与交易日记录 `row_count, duplicate_rate, invalid_quote_rate, null_rate, time_gap_quantiles, crossed_book_rate, time_reorder_count, stale_quote_segments, quarantined_rows`；每个质量指标有具体公式及分母。重复事件键定义为 `(exchange, instrument_id, trade_date, event_time)`；额外行数为各重复组 `n-1` 之和，除以总行数得到事件键重复率，不能等同于完整业务快照重复率。交叉盘口比例以 bid/ask 均有效的记录为分母；不可用的指标输出 null 与原因，不能用零冒充通过。时间倒退需先按输入物理位置审计，再显式排序；时间间隔统计须限定来源、合约、交易日和会话，跨源/跨会话间隔不混入 staleness 分布。

Quality Gates：阻断型错误（schema/时区/严重时间混乱）；隔离型错误（不合理价格/交叉盘口）；警告型问题（个别缺口/长尾、未提供成交字段）。阈值均可配置，不使用未经验证的固定硬编码。

## 时间同步与 As-of

- 更新机制尚未由样例和 Word 证实；应在 Passport 记录为 unknown，待原始流验证是否 **change-driven**；不先重采样为固定帧并向前填充覆盖原始信息。
- 当需要 T/TF/TL 跨合约计算，使用按 `event_time` 的 as-of join，并记录 `quote_age` 和是否超出 `max_staleness`；不可无限 forward fill。
- 可提供 clock-time grid 的研究视图；每个 tick 的最新值必须标记多久前出现，以及该值当时是否已可获得。
- 不可在交易日间、停盘区间、跨合约展期时隐式向前填充。

## 验收

- 构建合成输入覆盖乱序、同时间戳重复、跨会话、累计成交量重置、缺行情、交叉盘口、缺少可用时间。
- 所有清洗规则有计数和例子，原始数据不原地修改；错误记录可以追踪到文件和行/序列。
- 读取 30GB 数据不要求一次装入内存，分区处理/streaming 策略明确；提供性能 smoke-test 脚本。
- 标准化数据可被后续 Stage 使用，且 `DatasetPassport` 能解释没有哪些字段、因此哪些分析能力不可用。

**前置**：01；**下一步**：03。
