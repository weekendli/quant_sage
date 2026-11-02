# Step 02 国债期货数据 POC 交付契约

## 范围与入口

本阶段接入的是 T、TF、TL、TS 等国债期货具体合约的 L2 行情快照。源字段定义直接复用 [gov_bond_l2.schema.json](../../references/gov_bond_l2.schema.json)，不再维护第二份业务字段字典。该 JSON 已定义源列、类型和语义；本文件补充标准化映射、派生字段、日历依赖和下游交接约定。

Step 03 只接收一个已成功发布的 `onboarding.json` 引用，通过其中的 artifact refs 找到数据与元数据；不重新解析原始文件，不访问实时日历 API。POC 暂不产生训练特征、mid/spread/OBI、成交量 bar 或跨合约宽表，这些由下游按本契约计算。

## 下游使用场景与必要交付

| 下游场景 | 必要数据与规则 | 缺少条件时的行为 |
|---|---|---|
| mid、spread、盘口深度、一档 OBI、microprice proxy | 五档价格和数量、真实合约身份、报价有效性 | 无效一档盘口隔离；缺 tick size 只禁用 spread_ticks，仍可计算价格单位的 spread |
| 报价间隔、时钟窗口、事件计数窗口、窗口收益 | UTC event_time、trade_date、session_id、稳定排序 | 窗口按具体合约和会话重置；不能跨午间休市或合约切换取前值 |
| 成交活跃度与成交量 bar | 原生区间量、累计量、增量可信度检查 | 未通过增量一致性检查时禁用成交量 bar，保留原字段及原因；不反复差分原生增量 |
| T/TF/TL/TS 异步对齐 | 各合约独立时序、会话边界、max_staleness | 下游自行 as-of 对齐并输出 quote_age；研究模式对齐不宣称已验证的可交易领先滞后 |
| Atlas 与后续统计研究 | 数据范围、质量摘要、单位、可用性限制、稳定 snapshot 引用 | tick size、乘数或单位未知时禁用依赖它们的计算；split 沿用上游 SplitManifest，不在 onboarding 内拟合或划分 |

## 必须交付的文件

以下是目标布局，不表示这些文件已经生成。

```text
onboard/<data_snapshot_id>/<config_hash>/
  onboarding.json
  quotes/instrument_id=<contract>/trade_date=<YYYY-MM-DD>/part-*.parquet
  quarantine/part-*.parquet
  calendar.parquet
  session_rules.json
```

| 文件 | 内容与责任 |
|---|---|
| `onboarding.json` | 唯一入口；内嵌最小 DatasetPassport、QualityReport、分区索引和各文件的 ArtifactManifest。POC 不要求另写重复的 Passport/Quality 文件 |
| `quotes/...` | 有效记录的标准化长表，逐条快照，不重采样、不隐式填充；按实际合约和交易日分区 |
| `quarantine/...` | 未进入 quotes 的记录，保留来源与原因；无异常时仍生成有固定 schema 的零行 Parquet |
| `calendar.parquet` | Marketdata API 返回并冻结的所选交易日历；字段至少为 calendar_id:string、trade_date:date32，键为两者组合 |
| `session_rules.json` | 用于当前数据区间和品种的日内会话规则、时区、有效期、来源与版本；交易日列表不能替代它 |

入口文件自身的 ArtifactManifest 由工作流在成功发布后记录，不放入入口文件自身。每个 Parquet 文件都在 ArtifactManifest 中记录 schema_version、URI、content_hash、row_count 和时间范围。分区索引记录 exchange、instrument_id、trade_date、row_count、event_time_min/max 和文件引用；行数及分区列须与实际文件一致。POC 限定单一 exchange=CFFEX，其他市场另配适配器。

## 行情表复用源 schema 的方式

从已有 JSON 的 `sample.fields` 按以下确定性规则推导输出 schema，不另手写 62 个业务字段的类型：

1. 使用 [SPEC 映射表](SPEC.md#本数据源字段依据)重命名列，覆盖五档价格和数量、身份、时间、累计/区间成交量金额、最新价和持仓；对应 decimal 精度不变。`TRADINGTIME` 转换为 UTC `event_time`，`UNIX` 重命名为 `source_event_unix_ms`。
2. 未映射的业务列保持原名和类型；例如 CONTINUESIGN、SECURITYID、SETTLEPRICE。`product`、`source_file`、`source_row`、`ingest_run_id` 原样保留。源 schema_ref、hash 和有效映射完整写入入口文件，任何名称碰撞都阻断发布。
3. 增加下表中的派生列。quotes 必需字段不得为空；源 nullable 声明不等于接入后的质量要求。此 POC 要求五档业务列存在，逐行深档位缺失/无效则保留并用质量标记限制深度计算，不伪造报价。

| 新增列 | Arrow 类型与空值 | 约定 |
|---|---|---|
| `record_id` | string，非空 | 固定为 sha256(raw_file_sha256 + ":" + 十进制 raw_row_index)，UTF-8 编码；重跑稳定，不当交易所序号 |
| `raw_file_sha256` | string，非空 | 输入 Parquet 的内容 hash；区别于上游 source_file |
| `raw_row_index` | int64，非空 | 文件内零起算绝对行号；与文件 hash 组合可定位原记录 |
| `session_id` | string，非空 | 由 calendar_id、trade_date、会话规则中的 segment_id 组成；不同连续会话使用不同 ID |
| `available_time` | timestamp[us, UTC]，可空 | 本次源没有此字段，统一为 null；未知状态在 Passport 记录，不在时间列写字符串 unknown |
| `quality_flags` | list<string>，非空 | 无标记时为空列表；至少支持深档位无效、日终字段未验证、成交增量未验证三类标记，规则编号随配置冻结 |

`event_time` 固定为 timestamp[us, UTC]；trade_date 保留源交易归属日，不能用 UTC 日期覆盖。分区内按 `(event_time, raw_file_sha256, raw_row_index)` 排序，先审计物理输入中的乱序，再进行排序。原始事件时间可通过未修改的输入文件和来源引用核对。

未确认发布时间的 SETTLEPRICE、CLOSEPRICE 等日终字段仅作为源附加信息保留；不进入默认盘中特征输入白名单。缺少 available_time 时统一为 research_only + availability_unverified，不用 event_time 或 ingest_run_id 补造。

## Marketdata 日历依赖

服务地址为 `https://192.168.1.102/marketdata-api/`。使用用户提供的受信任 LAN 服务，客户端通过 CA 文件验证 HTTPS；CA 路径是运行配置，不硬编码服务器本地路径。公共证书的服务端分发源为 `/home/jianl/ops/marketdata-api/server-ca.crt`。仓库当前没有该 CA 或 client.py，本次仅定义接入约定，未实测 API。

日历获取流程：

1. 调用 GET health，保存返回的 release；通过 POST query 执行只读 SELECT。先列出 `marketdata.reference.calendars` 中实际存在的 calendar_id，确认适用于 CFFEX 国债期货的 ID，不照搬示例 CN_STOCK 或猜测存在 CFFEX。
2. 按 calendar_id 与输入交易日期上下界参数化查询，稳定排序。至少覆盖源 TRADINGDATE 的完整区间；若会话规则涉及相邻自然日，额外覆盖相应日期。
3. JSON 响应的 rows 是按 columns 排列的位置数组，必须按列名和返回类型解析；不能假设 rows 是字典。日期按 ISO 解析，decimal 字符串保持精度。
4. `truncated=true` 时禁止发布。缩小日期范围或按键分页，直到全部分片不截断、无重叠重复；所有分片和 health 的 release 必须一致，否则重新获取。空响应不能自动视为完整覆盖，需核验所选 calendar_id 和查询范围。
5. 固化去重并排序后的 calendar.parquet，记录查询 SQL、params、请求区间、release、抓取时间、行数与 hash；只有这些快照参与下游重放。API 不可用时可复用引用明确、覆盖完整且 hash 已验证的既有快照，不悄悄使用本机工作日规则。

查询模板，calendar_id 参数必须来自实际发现与确认：

```sql
SELECT DISTINCT calendar_id
FROM marketdata.reference.calendars
ORDER BY calendar_id
```

```sql
SELECT calendar_id, trade_date
FROM marketdata.reference.calendars
WHERE calendar_id = ? AND trade_date >= ? AND trade_date <= ?
ORDER BY calendar_id, trade_date
```

用户提供的 API 说明只证明交易日表可查询，没有提供日内时段表的名称和字段。如果 API 中可发现适用的会话表，则查询并固化其结果；否则使用明确来源、经确认并随交付保存的 session_rules.json。规则至少包含 version、timezone、source_refs，以及各产品/有效日期区间对应的 segment_id、起止本地时间、边界包含方式、自然日与交易日关系。09:29、15:24 等样本记录必须有明确分类，不能凭样本最早/最晚时间反推交易时段。每条保留记录只匹配一个会话；未知会话行进入隔离，全部无匹配或规则缺失则阻断交付。

## onboarding.json 的最小字段

| 字段 | 类型及约定 |
|---|---|
| `schema_version`, `stage_name` | string；入口版本与固定 stage 名 onboard |
| `run_id`, `stage_run_id`, `config_hash`, `code_revision`, `split_id` | 工作流提供的稳定引用；不填虚构 ID，独立验证使用显式 fixture manifest |
| `data_snapshot_id` | 沿用 bootstrap 的原始输入快照 ID；输出版本由输入 snapshot、配置 hash、stage 版本、冻结日历/会话规则 hash 和产物 hash 区分 |
| `status` | READY / READY_WITH_WARNINGS；失败不发布此成功入口，失败诊断由 stage run 保存 |
| `inputs` | 输入 ArtifactRef 列表、源 schema_ref/hash、有效 mapping、source_timezone、calendar 查询与快照来源、session rules 引用 |
| `passport` | asset_class=government_bond_futures、exchange、products、instruments、日期范围、分区数、行数、字段语义引用、availability_status、research_only、未确认事项 |
| `passport.instrument_metadata` | 按 exchange+instrument_id 记录 tick_size、contract_multiplier、price_unit、turnover_unit；值允许 null，每项保留来源或 unknown 原因，禁止猜测单位和乘数 |
| `passport.input_constraints` | 哪些字段可进入盘中特征、哪些仅供审计；增量可信度、深档位有效性等条件供 Step 03 决定能力启用与禁用 |
| `quality_report` | input_rows、accepted_rows、quarantined_rows，按规则/分区的结果列表；每项有 rule_id、scope、outcome、命中数、分母、阈值（可空）和示例来源引用 |
| `partitions` | 标准化行情分区索引；时间范围、行数与对应 manifest 引用 |
| `artifact_manifests` | quotes、quarantine、calendar、session_rules 文件的公共 ArtifactManifest；包含原始输入血缘，并共享 data_snapshot_id；不包含入口文件自身以避免 hash 循环 |
| `warnings` | 机读 code 与说明；至少表达 UNKNOWN_AVAILABILITY 和未确认元数据 |

质量规则 outcome 使用 PASS / WARN / FAIL / NOT_EVALUATED / NOT_APPLICABLE。未检查与不适用不得写成命中数为零的 PASS。QualityReport 的无效记录数按行去重，规则命中数允许一行命中多条规则，两者不混为分母。

## POC 发布与记录对账

- POC 不合并重复记录。没有可靠交易所序号的重复事件键 `(exchange, instrument_id, trade_date, event_time)`，组内全部行隔离并标记重复或冲突，避免任取一条；含真实序号的其他适配器另定契约。以后引入合并去重时再增加多来源映射表。
- quarantine 固定包含原始 66 列、record_id、raw_file_sha256、raw_row_index、reason_codes:list<string>。每条输入只写入 quotes 或 quarantine 之一，每行可包含多个原因，满足 `input_rows = accepted_rows + quarantined_rows`。
- 缺必要列、类型不兼容、时区无法解释、日历依赖/会话规则无效、hash/行数对账失败属于阻断错误，不发布成功入口。逐行无效时间、非交易日、无法分类会话和无效一档盘口进入隔离；无可用记录时保存诊断并阻断下游。
- POC 默认允许存在已隔离坏行，以 READY_WITH_WARNINGS 发布剩余有效记录，同时公开隔离比例；若配置了更严格比例阈值则执行该阈值。缺可用时间允许 research_only；若日后关闭该模式则必须阻断。
- 文件先写临时位置，校验 schema、hash、行数、会话和来源后，最后原子发布 onboarding.json。失败和未完成目录不能供 Step 03 使用。空输入记录为显式 skipped/empty 诊断，不伪造 READY 数据集。

## 最小交接验收

由 Step 02 的消费者 fixture 只读取 onboarding.json 及其引用文件，验证：可以选取一个合约和交易日，按 event_time 读取盘口并手算 mid、价格单位 spread、一档 OBI 与会话内间隔；午间休市和跨合约不串接；未知 tick size 时禁用 spread_ticks；未知 available_time 保持研究模式。此 fixture 验证交付可消费，不在本阶段实现 Step 03 的特征工厂。

真实样本还应核验 12 个合约和 20 个源交易日的分区覆盖、每条记录去向与隔离原因。数据源出现 240 个合约日组合不代表必然发布 240 个有效分区，实际保留分区取决于日历和质量检查；任何减少必须可解释。
