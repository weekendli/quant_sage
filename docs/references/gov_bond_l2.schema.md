# 国债期货 L2 字段与样例 Schema

依据：[字段说明](国债期货%20L2.docx)、[实际样例](part-0.parquet)。机读版本：[gov_bond_l2.schema.json](gov_bond_l2.schema.json)。

## 数据层级与核验范围

`part-0.parquet` 是 **3,178,989 行、66 列、26 个 row group 的逐条 L2 快照文件**：前 62 列对应 Word 字段，后 4 列为上游导入元数据。它替代此前文档引用的 240 行、35 列汇总样例；当前接入按逐条快照处理，不再要求 summary adapter。快照不等于逐笔成交或订单日志。

字段名称、顺序、类型和 nullable 来自实际 Parquet。全部字段 nullable=true 仅表示允许空值；实测空值另见[样本核验结果](part-0.profile.json)。物理名称为全大写业务字段及小写导入元数据，不能直接用 Word 的驼峰拼写读取。Word 中 `Se11`、`。` 等文字错误已映射到样本中的实际名称，原文仍保存在 JSON 中。

## 样例实际字段

| 字段 | Arrow 类型 | nullable | Word 对应字段 | 含义 |
|---|---|---|---|---|
| `TRADINGDATE` | `date32[day]` | true | `TradingDate` | 交易归属日期 |
| `TRADINGTIME` | `timestamp[us]` | true | `TradingTime` | 交易发生时间 |
| `SYMBOL` | `string` | true | `Symbol` | 合约代码 |
| `OPENPRICE` | `decimal128(9, 3)` | true | `OpenPrice` | 今开盘价 |
| `LASTPRICE` | `decimal128(9, 3)` | true | `LastPrice` | 最新成交价 |
| `HIGHPRICE` | `decimal128(9, 3)` | true | `HighPrice` | 日内最高价 |
| `LOWPRICE` | `decimal128(9, 3)` | true | `LowPrice` | 日内最低价 |
| `SETTLEPRICE` | `decimal128(9, 3)` | true | `SettlePrice` | 结算价 |
| `PRESETTLEPRICE` | `decimal128(9, 3)` | true | `PreSettlePrice` | 昨结算价 |
| `CLOSEPRICE` | `decimal128(9, 3)` | true | `ClosePrice` | 今收盘价 |
| `PRECLOSEPRICE` | `decimal128(9, 3)` | true | `PreClosePrice` | 昨收盘价 |
| `TRADEVOLUME` | `decimal128(19, 0)` | true | `TradeVolume` | 相邻观测间成交量 |
| `TOTALVOLUME` | `decimal128(19, 2)` | true | `TotalVolume` | 日内累计成交量 |
| `TRADEAMOUNT` | `decimal128(19, 3)` | true | `TradeAmount` | 相邻观测间成交金额 |
| `TOTALAMOUNT` | `decimal128(19, 3)` | true | `TotalAmount` | 日内累计成交金额 |
| `PRETOTALPOSITION` | `decimal128(19, 2)` | true | `PreTotalPosition` | 昨持仓量 |
| `TOTALPOSITION` | `decimal128(19, 2)` | true | `TotalPosition` | 当前持仓量 |
| `PREPOSITIONCHANGE` | `decimal128(19, 2)` | true | `PrePositionChange` | 当前持仓量减昨持仓量 |
| `PRICEUPLIMIT` | `decimal128(9, 3)` | true | `PriceUpLimit` | 涨停价 |
| `PRICEDOWNLIMIT` | `decimal128(9, 3)` | true | `PriceDownLimit` | 跌停价 |
| `BUYORSELL` | `string` | true | `BuyOrSell` | 推导的买卖方向 |
| `OPENCLOSE` | `string` | true | `OpenClose` | 推导的开平仓性质 |
| `BUYPRICE01` | `decimal128(9, 3)` | true | `BuyPrice01` | 买价第1档 |
| `BUYPRICE02` | `decimal128(9, 3)` | true | `BuyPrice02` | 买价第2档 |
| `BUYPRICE03` | `decimal128(9, 3)` | true | `BuyPrice03` | 买价第3档 |
| `BUYPRICE04` | `decimal128(9, 3)` | true | `BuyPrice04` | 买价第4档 |
| `BUYPRICE05` | `decimal128(9, 3)` | true | `BuyPrice05` | 买价第5档 |
| `SELLPRICE01` | `decimal128(9, 3)` | true | `SellPrice01` | 卖价第1档 |
| `SELLPRICE02` | `decimal128(9, 3)` | true | `SellPrice02` | 卖价第2档 |
| `SELLPRICE03` | `decimal128(9, 3)` | true | `SellPrice03` | 卖价第3档 |
| `SELLPRICE04` | `decimal128(9, 3)` | true | `SellPrice04` | 卖价第4档 |
| `SELLPRICE05` | `decimal128(9, 3)` | true | `SellPrice05` | 卖价第5档 |
| `BUYVOLUME01` | `decimal128(19, 2)` | true | `BuyVolume01` | 买量第1档 |
| `BUYVOLUME02` | `decimal128(19, 2)` | true | `BuyVolume02` | 买量第2档 |
| `BUYVOLUME03` | `decimal128(19, 2)` | true | `BuyVolume03` | 买量第3档 |
| `BUYVOLUME04` | `decimal128(19, 2)` | true | `BuyVolume04` | 买量第4档 |
| `BUYVOLUME05` | `decimal128(19, 2)` | true | `BuyVolume05` | 买量第5档 |
| `SELLVOLUME01` | `decimal128(19, 2)` | true | `SellVolume01` | 卖量第1档 |
| `SELLVOLUME02` | `decimal128(19, 2)` | true | `SellVolume02` | 卖量第2档 |
| `SELLVOLUME03` | `decimal128(19, 2)` | true | `SellVolume03` | 卖量第3档 |
| `SELLVOLUME04` | `decimal128(19, 2)` | true | `SellVolume04` | 卖量第4档 |
| `SELLVOLUME05` | `decimal128(19, 2)` | true | `SellVolume05` | 卖量第5档 |
| `DELTA` | `decimal128(9, 4)` | true | `Delta` | 预留今虚实度 |
| `PREDELTA` | `decimal128(9, 4)` | true | `PreDelta` | 预留昨虚实度 |
| `SETTLEGROUPID` | `string` | true | `SettleGroupID` | 结算组代码 |
| `SETTLEID` | `decimal128(9, 0)` | true | `SettleID` | 结算编号 |
| `CHANGE` | `decimal128(9, 3)` | true | `Change` | 最新价减昨结算价 |
| `CHANGERATIO` | `decimal128(9, 4)` | true | `ChangeRatio` | 涨跌幅（2017年8月起为比率） |
| `CONTINUESIGN` | `string` | true | `ContinueSign` | 连续合约代码 |
| `MARKET` | `string` | true | `Market` | 市场 |
| `UNIX` | `int64` | true | `UNIX` | 事件时间对应的 Unix 毫秒 |
| `POSITIONCHANGE` | `decimal128(19, 2)` | true | `PositionChange` | 相邻观测持仓差，当日首条为0 |
| `CONTINUESIGNNAME` | `string` | true | `ContinueSignName` | 连续合约名称 |
| `SECURITYID` | `string` | true | `SecurityID` | 证券统一内部编码 |
| `SHORTNAME` | `string` | true | `ShortName` | 证券简称 |
| `AVERAGEPRICE` | `decimal128(9, 3)` | true | `AveragePrice` | 成交均价，无成交时使用昨收盘价 |
| `ORDERRATE` | `decimal128(9, 4)` | true | `OrderRate` | 五档委比 |
| `ORDERDIFF` | `decimal128(19, 2)` | true | `OrderDiff` | 五档买量合计减卖量合计 |
| `AMPLITUDE` | `decimal128(9, 4)` | true | `Amplitude` | 日内高低价差除昨结算价 |
| `VOLRATE` | `decimal128(9, 4)` | true | `VolRate` | 相对前五日分钟均量的量比 |
| `SELLVOL` | `decimal128(19, 2)` | true | `SellVOL` | 主动卖累计成交量 |
| `BUYVOL` | `decimal128(19, 2)` | true | `BuyVOL` | 主动买累计成交量 |
| `product` | `string` | true | 导入元数据 | 品种标签；核对 SYMBOL 前缀，不用 source_file 文件名推断合约 |
| `source_file` | `string` | true | 导入元数据 | 上游源文件标识；只用于血缘，不表示合约或交易所时间 |
| `source_row` | `int64` | true | 导入元数据 | 上游源行号；不是交易所序号，起算方式待确认 |
| `ingest_run_id` | `string` | true | 导入元数据 | 上游导入批次标识；不是逐条 ingested_time 或 available_time |


## 接入与计算约束

- `TRADINGDATE` 已是 `date32[day]`，不是 Word 中的 yyyymmdd 整数。`TRADINGTIME` 是 `timestamp[us]`，Parquet 标记 `isAdjustedToUTC=false`，没有时区；按明确的源时区解释后转换为 UTC，不可直接标注为 UTC。`UNIX` 是 `int64`，用毫秒 epoch 与事件时间交叉校验，不当作接收时间。具体样本结果见 Step 02。
- `SECURITYID` 实际为 string，应保留字符串身份；`UNIX` 实际为 int64，不用 Word decimal 声明覆盖。价格为 decimal128(9,3)，委比等为 decimal128(9,4)，数量和金额按表内精度保留，禁止无说明转成 float 或截断。
- `available_time`、逐条 `ingested_time`、交易所 `source_sequence` 均未提供。`ingest_run_id` 即使带日期字符串也不证明逐条接收时间；`source_row` 可稳定定位源记录，但不能证明跨文件的市场事件顺序。
- `SYMBOL` 是具体合约；`CONTINUESIGN` / `CONTINUESIGNNAME` 是额外连续合约标签。`product` 可与 SYMBOL 前缀核对；源文件名可能与记录中的品种不同，不得据此覆盖身份或删除记录。
- `TRADEVOLUME` / `TRADEAMOUNT` 是相邻观测区间量；`TOTALVOLUME` / `TOTALAMOUNT` 是日内累计量。`TOTALPOSITION` 是持仓存量，`PREPOSITIONCHANGE` 相对昨持仓，`POSITIONCHANGE` 是相邻观测仓差。区间量不重复差分，累计量重置和源文件边界需要单独核验。
- `BUYPRICE01..05` / `SELLPRICE01..05` 映射为 `bid_price_1..5` / `ask_price_1..5`，数量对应 `BUYVOLUME01..05` / `SELLVOLUME01..05`。字段具备支持盘口研究的条件，仍需有效报价、日历、tick size 和可用性检查。
- `BUYORSELL` / `OPENCLOSE` 是供应商推导字段，非真实订单方向；2017 年 8 月前后编码有变化。`CHANGERATIO` 同期从百分数口径改为比率；`ORDERRATE` 是五档委比，不能当作一档 OBI。
- `DELTA` / `PREDELTA` 为预留字段；`SETTLEPRICE` / `CLOSEPRICE` 可为零，不能直接当作有效价格或盘中已知的日终信息。零值/哨兵值、金额单位、合约乘数和 tick size 均须明确配置。

## 证据与限制

[part-0.profile.json](part-0.profile.json) 保存全文件扫描的 复现脚本、结果、工具版本及样本 SHA-256。JSON schema 保存两个输入文件 SHA-256。统计只描述所提供的单个文件，不证明全量约 30GB 数据质量、交易日完整性、发布机制或生产可用性；Step 02 尚未实现。原始 Word 与 Parquet 不修改。
