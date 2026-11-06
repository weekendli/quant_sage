# Step 05 — Statistical Features & Quality

## 当前收敛范围：Step 02–05 数据与特征闭环

本步消费选定的 Step 04 业务结果或 Step 03 primitive 结果，按 FeatureSet 的 input + W + sampling + metrics 生成 Feature(kind=statistical) 与 FeatureResult。只依赖实际选中输入；primitive-only 配置不要求先运行 Step 04。保留 catch22 22 项及精选 tsfeatures 8 项的完整定义/对照目录，每次仅执行被明确选择的合法统计量。

当前必需交付为统计数值、采样/时间语义、质量/coverage 和可复用结果索引。完整 Atlas、条件正常分布拟合、异常分数、依赖图、null/control、事件/状态发现均为后续扩展，不作为本步核心验收前置；详细数学资料保留供显式扩展使用。

统一管理字段见 [domain.detail.md](../../domain.detail.md)，当前调度边界见 [WORKFLOW_STAGES.md](../../WORKFLOW_STAGES.md)。旧 Registry 名称表示 Feature/FeatureResult 的分类视图，不要求新增独立领域仓库。Study、FeatureSet、Run、Review 的通用管理由 Step 01 承担，领域步骤只实现本步算子与结果契约。


> 规格版本：`statistical_profile_v3`。用于 workflow 的 Stage 04 `statistical_atlas`，本文件是 Step 05 的完整规格，实施与验收见 [IMPLEMENT](IMPLEMENT.md) 和 [VERIFY](VERIFY.md)。本文定义待实现的指标与执行契约，不代表已经运行或验证的统计结果。

## 1. 目标与边界

为国债期货不规则 L2 快照建立无未来收益标签的统计画像，回答：分布是否偏斜/厚尾、序列是否持续或反复、冲击是否聚集、局部均值与波动是否稳定，以及统计结构是否随交易时段改变。

采用三层结构：

1. **基础描述层**：保留原始单位、分布、零值与覆盖率。
2. **catch22 基线层**：完整保留 22 项经典统计指纹，描述时间序列形态。
3. **tsfeatures 精选扩展层**：默认候选增加 8 项，补足直接可解释的线性依赖、条件异方差、谱复杂度和局部稳定性。

这里的 baseline 指用于比较扩展价值的固定特征集；与后续可选扩展拟合的条件正常分布 `P0(X|context)` 是两件事。

本文是研究设计选择，不声称 catch22 或某个扩展在当前市场数据上已证明最优。catch22 来源于跨数据集分类任务的筛选，不是金融市场专用指标，也不保证 22 项相互独立。[S1][S2]

### 1.1 输入、输出与职责

- 输入：Step 04 的 FeatureRegistry/feature Parquet、Step 03 的 PrimitiveRegistry/选定 primitive Parquet、冻结的 SplitManifest、日历和采样配置。按输入白名单选择，不要求 432 个业务指标全部进入统计展开。
- 必需输出：具体统计 Feature 定义、`StatisticalFingerprintTable` 与 FeatureResult 索引，附采样、有效性、coverage、可用时间和血缘。StatisticalFeatureRegistry 仅为统一 Feature 目录的统计视图。
- 延期扩展：完整 Atlas、BaselineModels/BaselineManifest、DependencyGraph、AnomalyCandidateTable；未选择或未实现不产生占位产物。事件与状态模型由后续步骤负责。

### 1.2 与 events / regimes 的交付关系

Step 06/07 可联合消费 Step 04 原单位业务特征、Step 05 统计向量（条件 baseline 仅在后续扩展实际交付时可用），按可用时间 join；不要只给 catch22 标准化形态、丢掉容量和成本的绝对水平。

本步骤不训练事件/状态模型，也不需要未来 impact 标签。对业务 lookback、输入或冗余的研究建议只能形成下一轮冻结配置，不反向修改本次 Step 04 产物。

## 2. 与 workflow 和业务 features 的关系

```text
Step 03 primitives（workflow Stage 02）
       ↓
Step 04 business features（workflow Stage 03 features）
  └─ 按各 family 定义计算业务 lookback L
       ↓ 选定业务序列；也允许选定 primitives 作为对照输入
Step 05 statistical features（workflow Stage 04 statistical_atlas）
  ├─ 原单位描述 + catch22 + 精选 tsfeatures
  ├─ trailing profile window W / retrospective summary
  └─ FeatureResult + quality/coverage + lineage
       ↓
Step 06 events / Step 07 regimes
```

- [L2 feature spec](../../references/gov_bond_l2_feature_spec.md) 定义业务指标；本文件定义对这些指标序列及选定 primitives 的统计描述，不改写其公式。
- 选业务输入时依赖对应 Step 04 结果；选 primitive 时可直接依赖 Step 03，不要求运行 Step 04，也不要求 432 个 factors 全部进入统计计算；只对已实现、语义明确且列入白名单的输入执行。
- `source_factor_horizon`/业务 lookback L、`profile_window` W、未来 impact horizon H 独立记录。前两者回看，H 只出现在后续实验 outcome 中。
- 底层依赖可延伸到 t-W-L，并需计入输入的端点匹配 age；不足历史或跨 session/缺口的输入不可静默拼接。
- 不反向依赖本步骤来计算固定业务 features。统计发现只能提出下一轮冻结配置，不能在同次运行中倒灌公式或窗口。
- v3 收敛执行与产物职责，保留 catch22 0.4.5 及精选 tsfeatures 数学定义。配置规范版本与 FeatureSet 业务版本分开；数值复用按具体定义/输入/实现身份验证，不因整个配置集换版一律失效。

## 3. 输入序列：按业务含义选择，不做完整笛卡尔积

每个输入视图必须登记 `source_kind / source_feature_id或source_primitive_id / transform / unit / time_basis / admissible_metrics / rationale`。下表是初始白名单；缺少上游依赖时跳过并说明，不在 Step 05 暗中补造上游业务公式。

| 输入族 | 首选画像序列 | 默认时间轴 | 关注点 / 限制 |
|---|---|---|---|
| 业务窗口序列（主输入） | 已注册且有效的 `spread_ticks__h1m`、`obi__d0__h3s`、`active_flow_imb__h3s`、`volume_to_depth__h1m`、`mid_move_ticks__h1m` 等显式选择项 | clock，1s 采样 | 先由 Step 04 计算，再对其历史序列画像；不启用原定义尚待明确的数值输出 |
| 价格动态 | 相邻有效网格 midpoint 的 tick 差 | clock，1s | 价格停滞、短期相关、波动聚集；不跨缺口作差 |
| 价格水平 | `mid` 原值 | 原单位描述；形态扩展关闭 | 用于价格范围核验；不把水平趋势直接解释为收益可预测性 |
| 价差 | 单快照 `spread_ticks` 的 as-of 视图 | clock，1s | 宽度、持续性、离散化；常量窗口合法但形态量可能不可估 |
| 深度 | `depth_bid/ask(d)`、`D_ref` | clock，1s | 主画像先启用 `d0,d5`；其他 space 需业务理由 |
| 盘口方向 | 单快照 `OBI(d)` | clock，1s | 主画像先启用 `d0,d5`；有界序列不自动 log 或差分 |
| size 容量 / 成本 | 单快照 min coverage、capacity imbalance、size spread、walk cost asymmetry | clock，1s；条件启用 | 首轮只选业务已登记的组合，如 `d1/q50` coverage、`q100` walk cost；可完成性必须保留 |
| 成交活动 | 每个 1s bin 的 `sum(TRADEVOLUME)` | clock，1s | 区间量求和，不再次差分；空 bin 的 0 需要覆盖证据 |
| 主动成交代理 | 同一 session 内累计 `BUYVOL/SELLVOL` 的有效相邻增量之差，再按 bin 求和 | clock，1s | reset / 边界增量不可归零；与真实逐笔订单流区分 |
| 报价活动 | bin 内有效报价观察数 `quote_update_count` | clock，1s | 是快照发布活动，不声称为订单到达次数 |
| 报价间隔 | `quote_interarrival` | quote event index | 观察间隔分布与事件索引依赖；不能把 lag 直接换成秒 |

业务窗口序列为主输入；下列单快照 primitive 视图用于背景与对照。默认深度用原值；如长尾严重，可显式增加 `log1p(depth)` 视图。log 只适用于非负且单位固定的输入，不能覆盖原值，也不对所有序列自动增加 log/diff 分支。跨合约比较原单位画像时分产品、真实合约；不将换月序列直接拼接。

合法组合由白名单决定：`allowed(input_view, metric, profile_window, time_basis)`。不允许自动生成所有输入 × 所有 space × 所有 size × 所有窗口 × 所有统计量。每段输入的形态指标最多为 **22 + 8 = 30 个默认候选**；实际数量依适用性与质量门槛减少，基础描述和条件扩展另计。

逐项选择与执行证据遵循 [Step 01 的配置/ledger 契约](../01_workflow_engine/IMPLEMENT.md)：每个 statistical item 指定唯一 source、profile window、采样/评估步长，以及 `metrics` 或命名 `metric_set`。显式列举只运行被选统计量；`catch22_classic_22` 表示完整经典基线，`catch22_plus_selected_tsfeatures_30` 表示完整 22+8 候选集。自定义子集标记 `custom_subset`，不得报告为完整基线。精确 metric 参数、库版本、源输入和窗口策略进入语义 ID/缓存键，按定义与请求分区分别记录执行与数据有效性。

## 4. 不规则采样、窗口和缺失契约

### 4.1 时间轴

**clock_time 主视图**：在同一真实合约、交易日、连续 session 内建立 1s 网格。

- 已计算业务 features 的采样也使用明确的 backward as-of、max_feature_age 与有效性政策；不重算其原窗口，不将无效值填成有效，不以源 timestamp 冒充 available_time。
- book state 用 backward as-of；候选快照的 `event_time ≤ grid_time`，在线用途还必须满足可用时间约束。禁止 nearest join、未来插值或 backfill。
- `max_quote_age` 必须由配置明确给定，结合 onboarding 的覆盖情况在训练期确定并冻结；未声明则该视图不可运行。没有新快照不自动等价于盘口仍然可信。
- 不对成交量/计数做 book-state 式 forward fill。区间成交量以其记录端点落入 `(g-1s,g]` 分桶；区间可能跨 bin，保留端点归属近似和真实区间跨度。无法验证可靠归属时禁用该细粒度视图，不能按比例虚构成交时间。
- 成交零值仅在数据源覆盖可信、没有 reset/缺口影响时成立；未知记 NA。计数 0 只表示该 bin 未观察到快照，保留 feed coverage 状态。

成交量未验证及累计量/增量对账不一致仅保留警告，不单独禁用成交活动视图或强制 NA；区间起点缺失、真实缺口、reset 和非法值仍执行各自有效性规则。推荐的缺口证据与恢复机制见 [实施设计 §6.4](../../IMPLEMENTATION_DESIGN.md#64-缺少行情的推荐处理机制)。

**quote_event_index 补充视图**：按上游确定的有效观察顺序运行；1 lag 表示相邻观察，不是 1 秒。原始行序号不是交易所序号；同 timestamp 的处理遵守上游去重/排序契约并记录策略。

catch22 与 tsfeatures 接收的是有序数值序列，本身不会恢复不规则时间。所有 lag、run length、频率必须结合时间轴解释；不将 event-index 谱峰称为实际秒数周期。

### 4.2 分段策略

| 配置 | 默认用途 | 规则 |
|---|---|---|
| `clock_1s_30m_trailing` | 在线 events/regimes 的统计输入 | 过去 30m 的 1s 输入序列，每 60s 评估一次；完整历史后发布 |
| `clock_1s_30m` | 回顾画像、日内对照 | 从连续 session 起点对齐，30m 非重叠块 |
| `clock_1s_session` | 整个 session 背景 | 独立摘要，不能当作开盘时已知特征 |
| `event_2048` | 报价间隔等事件轴补充 | 同 session 内每 2048 个有效观察分块 |

默认只为显式选定的业务序列启用 trailing 配置；segment/session/event 配置按用途另选。这些是本项目的初始研究配置，不是两个库规定的最优窗口。`1s/3s` 业务 horizon 不意味着只取 1 或 3 个样本计算 catch22。其他粒度/窗口经训练期预算审查后显式增加，不与全部画像配置全交叉。

- 输入业务 lookback L 与统计窗口 W 均需完整；重叠输入与 rolling profile 导致的依赖必须用不同步长/非重叠对照检查，不能自动视为市场持续性。
- 默认形态画像要求连续有限样本 `n ≥ 256`；这是保守运行门槛，不是精度保证。更严格的单指标条件优先。
- 默认 trailing 窗口内出现 NA/stale/已知相关缺口时，该评估时刻的统计值为 NA 并附原因，保留父窗口和覆盖率；不允许 `dropna()` 后把两端拼接，也不以子段值代替完整 W 的值。只有显式开启子段画像时，才将足够长的连续子段作为独立 segment_summary 发布，带独立 `segment_id`、父窗口引用和实际起止时间；不足者记 `INSUFFICIENT_HISTORY`。
- segment/event 模式的尾部不足 30m/2048 残块可保留基础描述，形态结果标记 `partial_window`；只在满足门槛且被显式允许时计算，不与完整窗口无条件混比。默认 trailing 不启用残块或子段回退，须恢复完整 W 有效历史及上游 L 后再输出有效统计值。
- 午休、收盘、合约切换和未授权范围均断开；split 边界按读取/拟合权限处理，允许评价窗口读取已授权且当时可用的过去历史；长区间可汇总窗口画像的分布，不拼接窗口原始序列计算 ACF/频谱。
- 对 `constant` / `near_constant` 输入，基础描述仍输出；形态量按每项定义处理，不添加噪声、不将失败填成 0。

### 4.3 计算模式、发布与底层历史依赖

- `trailing`：只读取 `(t-W,t]` 已可用的输入值，按显式 evaluation stride 输出统计向量 `Z_t`，供 events/regimes 使用。
- `segment_summary`：30m 非重叠块或完整 session 的回顾摘要，记录区间结束后的发布时间；完整 session 摘要标记 retrospective_only，不向段内广播。

输出按 `(exchange, instrument_id, trade_date)` 分区。clock 网格、evaluation stride 与固定块均以冻结的 session 起点对齐，event 块以 session 内稳定观察顺序对齐；不得从 Run.scope.start 重新起算，扩大或裁剪 Run 范围不改变共同评估时刻的数值。窗口及 as-of 不能跨 session 或已知相关缺口。

业务 lookback `L`、统计窗口 `W`、实验未来 impact horizon `H` 分别记录。对于 `F_t=phi(X_(t-L,t])`，统计 `Z_t=stats({F_u:t-W<u<=t})` 的底层依赖可延伸到 `t-W-L`，还需计入端点匹配 age。不能以一个 horizon 字段代替全部时间语义。

初始在线候选配置为 1s 输入采样、30m trailing window、60s evaluation stride；这只是研究默认值，需经训练数据覆盖/成本验证。仅当所选业务特征已有完整 lookback、统计窗口及样本门槛时发布，不跨 session 借历史。采样只改变统计输入视图，不重算或改写 Step 04 原公式。

保持输入特征的有效性与 source available_time，按明确 backward as-of/age policy 采样；禁止把不可用指标 forward fill 为有效值。统计结果可用时间不早于全部依赖和窗口结束；未知源可用时间仍标记 research_only。

重叠业务窗口和统计窗口都会引入序列相关。记录 `L,W,stride,sampling_interval`，用不同步长/非重叠对照识别算子造成的平滑，不将其自动解释为市场持续性，也不按窗口行数充当独立样本数。

## 5. 基础描述层：质量必需，统计项显式选择

catch22 主要描述标准化后的动态形态，对绝对位置和尺度不敏感，因此保留原单位输入 refs 和质量信息；位置/尺度等描述统计作为 FeatureSet 中明确的 metrics 或命名描述集执行，不因选择某项 catch22 自动计算本表全部统计。[S2][S3]

| 组 | 字段 / 公式（首行为质量信息，其余按选择执行） |
|---|---|
| 规模与质量 | `n_total,n_valid,n_missing,n_unique,coverage_ratio,max_gap,stale_ratio`；分别说明父窗口与实际连续段分母 |
| 位置与尺度 | `mean`、样本 `std(ddof=1)`、`median`、`MAD=median(abs(x-median(x)))`、`IQR=q75-q25` |
| 分位数 | `q01,q05,q25,q75,q95,q99,min,max`；quantile interpolation 固定并版本化 |
| 离散与符号 | `zero_ratio`；有方向含义时增加 `positive_ratio,negative_ratio`；`unchanged_ratio=sum(x[i]=x[i-1])/(n-1)` |
| 波动与尾部 | 仅对明确的价格变化视图增加 `realized_variance=sum(r_tick²)`、`skewness`、`excess_kurtosis`；后两者明确采用的有限样本校正，极短/常量样本 NA |

不对 spread 等非负量机械输出多空比例；价格不变不等于 feed 中断，必须结合 quote age/计数。原始尺度统计不替代条件 baseline。

## 6. catch22 基线：22 项完整保留

### 6.1 版本选择

本规格明确冻结 **classic `pycatch22==0.4.5`** 的公式及下面的 canonical long names；完整集可调用 `catch22_all(x, catch24=False, short_names=False)`；自定义子集调用对应单项函数或可证明等价的选择执行适配器，不先全算再过滤。按名字匹配结果，不靠输出位置或短名称。[S3]

这不是“自动使用最新版”的约定：当前 upstream main 的 v1.0 接口说明多项定义已改变，且新增不可估值的 masking 行为。迁移需要新 `feature_set_version`、显式名称/定义映射与数值对照；不能把新版别名视为公式不变。[S4]

下面的解释为研究阅读标签；精确算法以锁定的源码为准。选择完整 catch22 metric_set 时计算整个向量作为公平基线，不因结果与收益无关就事后删项。显式自定义 metrics 可选择其子集，但须记录选择配置与子集标签，不能计为完整基线。不可估项保留定义与失败原因。

| ID | canonical name（0.4.5） | 画像含义 |
|---|---|---|
| C01 | `DN_HistogramMode_5` | 粗分箱众数 |
| C02 | `DN_HistogramMode_10` | 细分箱众数 |
| C03 | `CO_f1ecac` | ACF 衰减尺度 |
| C04 | `CO_FirstMin_ac` | ACF 首个局部低点 |
| C05 | `CO_HistogramAMI_even_2_5` | lag-2 非线性依赖 |
| C06 | `CO_trev_1_num` | 时间方向不对称 |
| C07 | `MD_hrv_classic_pnn40` | 标准化增量的大幅变化比例 |
| C08 | `SB_BinaryStats_mean_longstretch1` | 均值上方最长持续段 |
| C09 | `SB_TransitionMatrix_3ac_sumdiagcov` | 符号转移结构 |
| C10 | `PD_PeriodicityWang_th0_01` | 周期结构摘要 |
| C11 | `CO_Embed2_Dist_tau_d_expfit_meandiff` | 延迟嵌入距离形态 |
| C12 | `IN_AutoMutualInfoStats_40_gaussian_fmmi` | 互信息时间尺度 |
| C13 | `FC_LocalSimple_mean1_tauresrat` | 差分前后相关尺度比 |
| C14 | `DN_OutlierInclude_p_001_mdrmd` | 正向极端值出现时序 |
| C15 | `DN_OutlierInclude_n_001_mdrmd` | 负向极端值出现时序 |
| C16 | `SP_Summaries_welch_rect_area_5_1` | 低频功率占比 |
| C17 | `SB_BinaryStats_diff_longstretch0` | 非上升变化的最长持续段 |
| C18 | `SB_MotifThree_quantile_hh` | 符号对熵 |
| C19 | `SC_FluctAnal_2_rsrangefit_50_1_logi_prop_r1` | R/S 尺度交叉摘要 |
| C20 | `SC_FluctAnal_2_dfa_50_1_2_logi_prop_r1` | DFA 尺度交叉摘要 |
| C21 | `SP_Summaries_welch_rect_centroid` | 频谱位置摘要 |
| C22 | `FC_LocalSimple_mean3_stderr` | 简单局部预测误差 |

名称与数量依据锁定源码，概念分类参考官方说明。[S1][S3] C07 的历史命名不代表金融数据上的固定 40ms/40tick 阈值；C19/C20 不是直接的 Hurst 指数；C14/C15 也不是尾部概率或收益极值。

## 7. tsfeatures 精选扩展：8 项默认候选

这里的 `tsfeatures` 特指 **Rob Hyndman 等维护的 R 包**；Python 同名包不自动视为数值等价。首次实施时选定并固定一个精确 R 包版本、依赖和源码 revision，保存版本锁及 golden fixtures；不自动升级。具体版本锁属于实施交付项，不再作为需求待决项。Python 移植必须通过同输入 golden fixtures 对照，升级时使用新实现身份并重新验证。[S5]

### 7.1 特征目录

| ID | 本项目统计量 ID | 官方函数 / 输出 | 定义与补充价值 | 适用条件 |
|---|---|---|---|---|
| T01 | `tsf.x_acf1` | `acf_features` / `x_acf1` | `ρ(1)`；提供直接可读的一阶相关方向 | 有效非恒定序列 |
| T02 | `tsf.x_acf10` | `acf_features` / `x_acf10` | `Σ[k=1..10]ρ(k)²`；短期线性依赖总量 | 固定 10 lag |
| T03 | `tsf.x_pacf5` | `pacf_features` / `x_pacf5` | `Σ[k=1..5]π(k)²`；控制中间 lag 后的线性依赖 | PACF 求解有效 |
| T04 | `tsf.arch_r2_l12` | `arch_stat(x,lags=12,demean=TRUE)` / `ARCH.LM` | 去均值序列平方对其 12 阶滞后回归的 `R²`；波动聚集代理 | 默认用于价格变化、净 flow 增量；其他输入需显式理由 |
| T05 | `tsf.spectral_entropy` | `entropy` / `entropy` | AR 谱估计的归一化谱熵；补充 C18 的符号对熵 | 有限、非恒定；趋势主导时标记解释限制 |
| T06 | `tsf.stability_w60` | `stability(x,width=60)` | 标准化序列各非重叠块均值的方差 | 默认 clock 1s；至少 4 个完整 60 点块 |
| T07 | `tsf.lumpiness_w60` | `lumpiness(x,width=60)` | 标准化序列各非重叠块方差的方差 | 同上，关注波动非均匀性 |
| T08 | `tsf.crossing_points` | `crossing_points` | 穿越样本中位数的次数；补充最长持续段摘要 | 比较时同时保存 `n`；使用官方相等值归侧规则 |

ACF/PACF 是与 catch22 有一定冗余但解释性较强的刻度，不宣称相互独立。其输出与 lag 定义见官方文档。[S6][S7]

`arch_stat` 虽输出名为 `ARCH.LM`，返回的是 **R²，不是 nR²，也不是 p-value**；不能据其单值宣布通过 ARCH 显著性检验。价格变化不是 AR 残差，均值依赖仍可能影响解释，若要预白化需另登记变换。[S8]

谱熵沿用官方 AR/Burg 谱估计、平滑和归一化实现；不能以 histogram entropy、sample entropy 或任意 Welch FFT entropy 替代同名结果。[S9]

T06/T07 内部执行标准化，其值不保留原单位；官方定义是块均值/块方差的离散程度。宽度固定 60 **点**，仅在 1s 网格上对应 60s，不能沿用到 event 轴再称 60s。具体分块端点、尾块处理和方差约定须遵照锁定源码并用 fixtures 对照，不凭上述概念公式重写后声称数值等价。原单位变化幅度看第 5 节。[S10]

T08 的中位数相等值归入 `x≤median` 一侧，统计相邻布尔侧别改变次数。跨不同样本长度比较可另外存 `crossing_rate=crossing_points/(n-1)`，该值标记为项目派生量，不计为第九项 tsfeatures 原生指标。[S11]

### 7.2 条件扩展：默认关闭，不自动增加总量

| 扩展 | 何时值得启用 | 约束 / 不默认开启的原因 |
|---|---|---|
| `diff1_acf1,diff1_acf10,diff1_pacf5` | 深度、spread 水平变化的依赖 | 先注册原序列与差分含义；不对已有 return/flow 增量自动再差分 |
| `flat_spots` | 研究盘口长期停留于狭窄区间 | 官方是值域十等宽箱内的最长运行长度，不是价格完全不动；与 C08/C17 有重叠。[S12] |
| `heterogeneity` 的 `arch_acf,arch_r2,garch_acf,garch_r2` | 初始 ARCH 画像提示持续异方差，需要比较 AR/GARCH 处理后的剩余结构 | 涉及拟合、模型选择和收敛；单独预算与版本，不能用原序列平方 ACF 冒充拟合后结果。[S13] |
| `stl_features` 中 `trend,seasonal_strength,spike,e_acf1` | 已有明确等间隔周期且拥有足够多个周期的专门研究 | 季节项名可能随周期扩展；绑定实际 API。单个 session 不强设日周期；跨日主画像先用 session bucket baseline，不拼午休/隔夜做 STL。[S14] |
| `unitroot_kpss` | 判断水平序列是否需要单独趋势视图 | 统计量不是 p-value；结构突变、离散性会影响解读，不作为自动过滤闸门 |

不默认加入完整 `autocorr_features`、全部差分/二阶差分、全部 STL 或 GARCH 输出；先验证覆盖率、增量解释价值和成本。

## 8. 当前计算流程与延期扩展

1. 读取 Run 冻结的 FeatureSet 解析结果、源/输入 refs 与 split，只检查实际选中定义的依赖。
2. 按真实合约/session 与请求分区构造视图；历史支持可以超出输出范围，但必须获授权且在当时可用。
3. 仅执行所选基础描述/catch22/tsfeatures；n_total/n_valid、NA reasons 与覆盖是所有结果必需质量信息，不因未选描述指标而省略。
4. 发布数值表、FeatureResult 和质量摘要，按请求范围记录成功/失败/缓存。人工或 agent 的意见另存 Review，不自动修改 FeatureSet。

第 8.1 节供 Review/扩展比较参考，8.2–8.5 为后续分析能力及启用时的约束，不是当前必交付算子或产物。

catch22 内部标准化、样本中位数 crossing、谱估计等使用当前**已完成画像段**，属于该段描述，最早在段结束且依赖全部可用后发布。完整 session 描述标记 `retrospective_only`；不能把段末信息向段内广播。缺逐条 `available_time` 时按共同契约保留 `availability_unverified/research_only`，不假设等于 event time。

完整段的双侧拟合/平滑只用于回顾画像；在线事件/状态输入使用显式 trailing 模式，按窗口末端及全部依赖可用时间发布。baseline 的 fit/transform 仅用允许的过去数据，禁止任何未来 smoothing。

### 8.1 增量价值评价

- 固定比较组 `B0=基础描述+catch22`、`B1=B0+8项适用扩展`，保存精确启用项；不能让 B0/B1 使用不同时间段后直接比较。
- 检查跨日/时段的覆盖率、分布稳定性、Spearman 冗余和计算成本；高相关只标记冗余候选，不事后改写冻结基线。
- 用合成白噪声、AR、波动聚集、局部均值/方差突变检查可解释敏感性；本任务不以未来收益 IC/回测收益筛指标。
- 不对每条 quote 当作独立样本给显著性。跨日比较以交易日或适当独立 block 估计不确定性；不足时记录 `INSUFFICIENT_BLOCKS`。
- 30m 与 session 摘要相互重叠，不能合并计算独立样本数；探索依赖记录候选数量和多重比较范围。

### 8.2 Atlas 研究方向（延期）

| 统计方向 | 基础方法 | 产出 |
|---|---|---|
| Marginal Distribution | 分位数、偏度、尾部、零变动比例 | distribution fingerprint |
| Temporal Dependence | ACF/PACF、持续时间、间隔统计 | lag/time-scale map |
| Volatility Structure | 不同窗口 RV、vol clustering | volatility atlas |
| Session Seasonality | 同交易时段内的分位数/强度 | conditional seasonality |
| Cross-variable | 稳健相关、lead-lag、partial dependence 探索 | dependency candidates |
| Cross-instrument | as-of alignment、共同变化与残差 | cross-contract atlas |
| Change & Motif | 在线/离线变化点、重复子序列（可选） | pattern candidates |

### 8.3 条件正常背景（延期）

对 Primitive、业务 Feature 或选定统计 Feature `X`，先在训练区间用当时可观测的 Context `Z` 拟合：

`P0(X_t | instrument, session_bucket, activity_bucket, recent_volatility, ...)`

后续 Baseline 扩展：分组中位数、MAD、分位数（用训练样本拟合），小样本组回退至父级分组；支持平滑/收缩。生成 `expected`, `scale`, `conditional_quantile`, `tail_probability` 或其可估计子集。针对长尾和零膨胀优先条件分位而不是默认正态分布。

异常示意：`robust_score=(X - conditional_median)/max(conditional_MAD * 1.4826, eps)`。这一值只是偏离正常背景的指标，不等于显著的可交易信号。

### 8.4 Null / Control 世界（后续实验参考）

- **Descriptive baseline**：同合约/同交易时段的通常分布，用于定义异常。
- **Conditional control**：在相近波动/活跃度/时段背景中的非事件观测，用于后续实验对照。
- **Statistical null**：保持时序依赖结构的 block/circular permutation、shift 等，用于估计随机结果的分布。

这三者用途不同，不能混写为一个 `null_model`。

### 8.5 防止信息泄漏

- baseline、阈值、标准化器在训练子样本 fit；在随后样本 transform。若需要自适应更新，采用滚动过去窗口，绝不包含当前未来结果。
- 高波动 regime 若定义用未来30分钟 realized volatility，在当时不可用，不能做可交易条件；可以作为事后描述，但标记 `retrospective_only`。
- 相关、lead-lag 只产生候选依赖，不直接证明因果；异步采样可能导致伪领先。
- 跨训练/验证时间折按 split 重建 baseline，避免拿整个训练+测试期的分位点定义异常。

## 9. 输出与可复现契约

持久化 `StatisticalFingerprintTable`（long Parquet）、FeatureResult 与输入视图定义；Atlas/BaselineManifest 只在以后启用相应扩展时提供；遵守 [COMMON_CONTRACTS](../../COMMON_CONTRACTS.md)。

| 字段组 | 必需内容 |
|---|---|
| 身份 | `run_id,feature_set_version,profile_id,segment_id,parent_window_id,input_view_id,source_kind,source_feature_id,source_primitive_id` |
| 范围 | `symbol,trade_date,session_id,split_id,split_role,window_start,window_end` |
| 采样 | `time_basis,grid_interval,event_count,profile_mode,evaluation_stride,profile_window,source_factor_horizon,raw_dependency_start,transform,unit,max_quote_age,max_feature_age`；不适用则 null |
| 指标 | `metric_id,library_name,library_metric_name,library_version,source_revision,parameters,value,output_unit` |
| 有效性 | `is_valid,invalid_reason,quality_flags,n_total,n_valid,n_unique,coverage_ratio,parent_coverage_ratio` |
| 时效 / 血缘 | `event_time_end,available_time,availability_status,retrospective_only,input_artifact_ids,config_hash,code_revision` |

`output_unit` 不等于输入 `unit`：ACF 无量纲、stability 为标准化尺度、crossing 为次数、部分 catch22 输出为样本尺度。只有算法明确给出样本 lag 时才允许额外换算 clock seconds，并保留原始值。

指标 ID 示例（字符串仅示意，不是实测）：

```text
input_view_id = obi__d0__h3s__clock_1s
profile_id    = clock_1s_30m_trailing
metric_id     = c22.CO_f1ecac
```

结果身份包含具体 feature_ref/definition_hash、source/input refs、scope、采样/窗口及实现指纹。FeatureSet ref/version 记录选择来源，但不作为阻止同一定义跨配置集复用的唯一键；result_id 与 producer_run_id 指向实际产物，消费 run 单独引用。space/size 可以保留在输入元数据里，但不凭表结构自动补齐组合。

失败类型至少覆盖：`MISSING_FIELD,INSUFFICIENT_HISTORY,STALE_DATA,CROSS_SESSION,CONSTANT_INPUT,NUMERICAL_FAILURE,MODEL_FIT_FAILED,UNSUPPORTED_COMBINATION`。项目扩展 reason 必须映射到共同错误类别；缺可用时间是明确质量状态。NaN/Inf、库警告或模型不收敛不能悄悄转成有效 0。

实现须记录版本锁、源码 revision、数值依赖、随机种子（若使用）、标准化/quantile/ACF/PACF 约定，以及抽样/预算策略。不得仅写 `latest`。当前主分支链接用于核对语义，不代替运行时 revision 锁。

## 10. 实施与验收

本步骤按 [IMPLEMENT.md](IMPLEMENT.md) 实施，逐项验收标准集中于 [VERIFY.md](VERIFY.md)。包含 22+8 指标身份/数值对照、输入白名单、trailing 可用性、session/缺失边界、成本/覆盖率证据；baseline 训练隔离仅在后续拟合扩展启用时验收。所有验收均为待实施要求，不能将本规格视为完成记录。

## 11. 官方依据与选择说明

查阅日期：2026-11-04。库定义以官方资料为依据；窗口、输入白名单、样本门槛、8 项扩展和分层策略是本项目针对当前目标提出的设计。

- **[S1]** [catch22 官方特征说明](https://time-series-features.gitbook.io/catch22/information-about-catch22/feature-descriptions/feature-overview-table)：概念分类与非独立性；精确经典名称以 S3 为准。
- **[S2]** [Lubba et al. (2019), catch22 原始论文](https://doi.org/10.1007/s10618-019-00647-x)：通用特征集的研究背景。
- **[S3]** [pycatch22 v0.4.5 源码](https://github.com/DynamicsAndNeuralSystems/pycatch22/blob/v0.4.5/src/pycatch22/catch22.py) 与 [官方 0.4.5 发布说明](https://pypi.org/project/pycatch22/0.4.5/)：本规格冻结的 API、名称和尺度说明。
- **[S4]** [pycatch22 upstream API 源码](https://github.com/DynamicsAndNeuralSystems/pycatch22/blob/main/src/pycatch22/catch22.py)：新版定义迁移与 masking 约定。
- **[S5]** [R tsfeatures 官方仓库](https://github.com/robjhyndman/tsfeatures)：实现身份与版本依据。
- **[S6]** [acf_features](https://pkg.robjhyndman.com/tsfeatures/reference/acf_features.html)。
- **[S7]** [pacf_features](https://pkg.robjhyndman.com/tsfeatures/reference/pacf_features.html)。
- **[S8]** [arch_stat](https://pkg.robjhyndman.com/tsfeatures/reference/arch_stat.html)。
- **[S9]** [entropy 定义](https://pkg.robjhyndman.com/tsfeatures/reference/entropy.html) 与 [AR 谱熵源码](https://github.com/robjhyndman/tsfeatures/blob/master/R/entropy.R)。
- **[S10]** [stability / lumpiness](https://pkg.robjhyndman.com/tsfeatures/reference/lumpiness.html) 与 [分块及标准化源码](https://github.com/robjhyndman/tsfeatures/blob/master/R/features.R)。
- **[S11]** [crossing_points](https://pkg.robjhyndman.com/tsfeatures/reference/crossing_points.html)：ties 细节见 S10 源码。
- **[S12]** [flat_spots](https://pkg.robjhyndman.com/tsfeatures/reference/flat_spots.html)。
- **[S13]** [tsfeatures 官方参考手册](https://robjhyndman.r-universe.dev/tsfeatures/doc/manual.html)：heterogeneity 的拟合前后定义。
- **[S14]** [stl_features 官方源码](https://github.com/robjhyndman/tsfeatures/blob/master/R/multipleseasonal.R)。

**前置**：按所选输入为 03 或 04；**下一步**：06、07（可并行）。
