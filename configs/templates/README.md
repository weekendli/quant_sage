# 用户行情 metadata

复制 [market_metadata.template.json](market_metadata.template.json) 到自己的文件，替换所有 `<...>` 待填项后提供文件路径。模板仅说明格式，不能直接用作真实数据输入。

| 字段 | 填写规则 |
|---|---|
| metadata_id | 稳定名称；修改规则时保持名称并递增 version |
| version | 从 1 开始的正整数；已登记版本不可覆盖 |
| provided_by | 提供者；kind 为 human，id 为用户选择的标识 |
| source | 规则来源或说明；可以写原始规格文件路径及章节 |
| units.price_unit / size_unit / turnover_unit | 行情价格、盘口数量、成交额的显式单位；不确定时不要用模板占位符注册 |
| tick_rules[].product | 品种前缀：T、TF、TS、TL；仅保留本次使用的品种 |
| tick_rules[].instrument_id | 可选；提供时该合约规则优先于同品种规则，合约自身的历史区间仍必须完整 |
| valid_from | 生效交易日，ISO 日期 YYYY-MM-DD，包含当天 |
| valid_to | 失效交易日，不包含当天；null 表示无结束日 |
| tick_size | 正十进制数字字符串，例如 `"0.005"`；示例不代表用户已提供的实际值 |
| unit | `quoted_price_unit`，表示 tick 使用行情报价价格的单位 |

同一品种有历史变更时增加多条规则，按 `[valid_from, valid_to)` 衔接，不允许日期重叠。规则只需覆盖实际使用的数据日期；未覆盖日期不能沿用当前 tick。系统按最长前缀识别品种，TF/TS/TL 不会被误识别成 T。

metadata 文件作为独立内容计算 hash，并通过 `market_metadata_ref` 绑定到 DataSource 版本；无需手填 hash，也不修改冻结行情或重新接入。尚未提供 tick 时，可以先登记研究用数据源，但依赖 tick 的指标会明确受限。

先做独立格式校验：

```bash
/data/src/research/.venv/bin/python -m quantsage_discovery.cli \
  validate-market-metadata --input /path/to/market-metadata.json
```

DataSources 版本化适配命令：

```bash
/data/src/research/.venv/bin/python -m quantsage_discovery.cli \
  adapt-onboarding-source --entry <onboarding.json> \
  --entry-manifest <manifest.json> --split-manifest <split_manifest.json> \
  --output <new-version-directory> --source-id cffex_treasury_l2_full79 \
  --version 2 --metadata /path/to/market-metadata.json
```
