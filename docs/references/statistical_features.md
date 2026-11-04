# Stage 03 — Statistical Profiling Feature Set

> 规格版本：`statistical_profile_v1`。用于 workflow 的 Stage 03 `statistical_atlas`，对应实施目录 [Step 04](../steps/04_statistical_atlas_and_baselines/SPEC.md)。本文定义待实现的指标与执行契约，不代表已经运行或验证的统计结果。

## 1. 目标与边界

为国债期货不规则 L2 快照建立无未来收益标签的统计画像，回答：分布是否偏斜/厚尾、序列是否持续或反复、冲击是否聚集、局部均值与波动是否稳定，以及统计结构是否随交易时段改变。

采用三层结构：

1. **基础描述层**：保留原始单位、分布、零值与覆盖率。
2. **catch22 基线层**：完整保留 22 项经典统计指纹，描述时间序列形态。
3. **tsfeatures 精选扩展层**：默认候选增加 8 项，补足直接可解释的线性依赖、条件异方差、谱复杂度和局部稳定性。

这里的 baseline 指用于比较扩展价值的固定特征集；与 Stage 03 拟合的条件正常分布 `P0(X|context)` 是两件事。

本文是研究设计选择，不声称 catch22 或某个扩展在当前市场数据上已证明最优。catch22 来源于跨数据集分类任务的筛选，不是金融市场专用指标，也不保证 22 项相互独立。[S1][S2]

## 2. 与 workflow 和业务 features 的关系

```text
Stage 02 primitives
  ├─ 基础盘口、成交活动、价格变化序列
  └─ 单快照 OBI / capacity / VWAP 等可复用序列（若已实现）
       ↓ 按明确的输入视图与时间分段
Stage 03 statistical_atlas
  ├─ 原单位描述统计
  ├─ catch22 + 精选 tsfeatures
  ├─ 时段画像、训练期条件 baseline
  └─ 候选依赖与时间尺度证据
       ↓
Stage 04 features：业务指标计算与 Atlas 引导的候选生成
```

- [L2 feature spec](gov_bond_l2_feature_spec.md) 的 Family × Space/Size × Horizon 是业务指标定义；本文件的统计量是对一条输入序列、一段时间的摘要，两者不能混为一层。
- Stage 03 首次运行只依赖 Stage 02 已有 primitives，不要求先生成 Stage 04 的 432 个 factors。
- Stage 04 产物若需自身画像，可通过显式后续任务复用同一 profiling 服务；记录新输入血缘，不能形成 `03 → 04 → 03` 的调度环。
- 下文 `profile_window` 是画像样本区间，业务 factor 的 `horizon` 是该输入值自身的回看长度，两者独立记录。
- 不修改原有业务公式；采样视图也是独立 transformation，不得将画像用的等间隔采样替换原规格的 snapshot-average 公式。

## 3. 输入序列：按业务含义选择，不做完整笛卡尔积

每个输入视图必须登记 `source_primitive_id / transform / unit / time_basis / admissible_metrics / rationale`。下表是初始白名单；缺少上游依赖时跳过并说明，不在 Stage 03 暗中补造上游指标。

| 输入族 | 首选画像序列 | 默认时间轴 | 关注点 / 限制 |
|---|---|---|---|
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

默认深度用原值；如长尾严重，可显式增加 `log1p(depth)` 视图。log 只适用于非负且单位固定的输入，不能覆盖原值，也不对所有序列自动增加 log/diff 分支。跨合约比较原单位画像时分产品、真实合约；不将换月序列直接拼接。

合法组合由白名单决定：`allowed(input_view, metric, profile_window, time_basis)`。不允许自动生成所有输入 × 所有 space × 所有 size × 所有窗口 × 所有统计量。每段输入的形态指标最多为 **22 + 8 = 30 个默认候选**；实际数量依适用性与质量门槛减少，基础描述和条件扩展另计。

## 4. 不规则采样、窗口和缺失契约

### 4.1 时间轴

**clock_time 主视图**：在同一真实合约、交易日、连续 session 内建立 1s 网格。

- book state 用 backward as-of；候选快照的 `event_time ≤ grid_time`，在线用途还必须满足可用时间约束。禁止 nearest join、未来插值或 backfill。
- `max_quote_age` 必须由配置明确给定，结合 onboarding 的覆盖情况在训练期确定并冻结；未声明则该视图不可运行。没有新快照不自动等价于盘口仍然可信。
- 不对成交量/计数做 book-state 式 forward fill。区间成交量以其记录端点落入 `(g-1s,g]` 分桶；区间可能跨 bin，保留端点归属近似和真实区间跨度。无法验证可靠归属时禁用该细粒度视图，不能按比例虚构成交时间。
- 成交零值仅在数据源覆盖可信、没有 reset/缺口影响时成立；未知记 NA。计数 0 只表示该 bin 未观察到快照，保留 feed coverage 状态。

**quote_event_index 补充视图**：按上游确定的有效观察顺序运行；1 lag 表示相邻观察，不是 1 秒。原始行序号不是交易所序号；同 timestamp 的处理遵守上游去重/排序契约并记录策略。

catch22 与 tsfeatures 接收的是有序数值序列，本身不会恢复不规则时间。所有 lag、run length、频率必须结合时间轴解释；不将 event-index 谱峰称为实际秒数周期。

### 4.2 分段策略

| 配置 | 默认用途 | 规则 |
|---|---|---|
| `clock_1s_30m` | 主画像、日内差异 | 从连续 session 起点对齐，30m 非重叠块 |
| `clock_1s_session` | 整个 session 背景 | 独立摘要，不能当作开盘时已知特征 |
| `event_2048` | 报价间隔等事件轴补充 | 同 session 内每 2048 个有效观察分块 |

这些是本项目的初始研究配置，不是两个库规定的最优窗口。`1s/3s` 业务 horizon 不意味着只取 1 或 3 个样本计算 catch22。其他粒度/窗口经训练期预算审查后显式增加，不与三个默认配置全交叉。

- 默认形态画像要求连续有限样本 `n ≥ 256`；这是保守运行门槛，不是精度保证。更严格的单指标条件优先。
- 任何 NA/stale 区间切断连续段，不允许 `dropna()` 后把两端拼接。足够长的子段有独立 `segment_id` 与父窗口引用；不足者记 `INSUFFICIENT_HISTORY`。窗口覆盖率始终按父窗口记录。
- 尾部不足 30m/2048 的残块可保留基础描述，形态结果标记 `partial_window`；只在满足门槛且被显式允许时计算，不与完整窗口无条件混比。
- 午休、收盘、合约切换、split 边界均断开；长区间可汇总窗口画像的分布，不拼接窗口原始序列计算 ACF/频谱。
- 对 `constant` / `near_constant` 输入，基础描述仍输出；形态量按每项定义处理，不添加噪声、不将失败填成 0。

## 5. 基础描述层：必须保留的解释坐标

catch22 主要描述标准化后的动态形态，对绝对位置和尺度不敏感，因此必须另保留原单位描述；不将其冒充额外 catch22 项。[S2][S3]

| 组 | 必需字段 / 公式 |
|---|---|
| 规模与质量 | `n_total,n_valid,n_missing,n_unique,coverage_ratio,max_gap,stale_ratio`；分别说明父窗口与实际连续段分母 |
| 位置与尺度 | `mean`、样本 `std(ddof=1)`、`median`、`MAD=median(abs(x-median(x)))`、`IQR=q75-q25` |
| 分位数 | `q01,q05,q25,q75,q95,q99,min,max`；quantile interpolation 固定并版本化 |
| 离散与符号 | `zero_ratio`；有方向含义时增加 `positive_ratio,negative_ratio`；`unchanged_ratio=sum(x[i]=x[i-1])/(n-1)` |
| 波动与尾部 | 仅对明确的价格变化视图增加 `realized_variance=sum(r_tick²)`、`skewness`、`excess_kurtosis`；后两者明确采用的有限样本校正，极短/常量样本 NA |

不对 spread 等非负量机械输出多空比例；价格不变不等于 feed 中断，必须结合 quote age/计数。原始尺度统计不替代条件 baseline。

## 6. catch22 基线：22 项完整保留

### 6.1 版本选择

本规格明确冻结 **classic `pycatch22==0.4.5`** 的公式及下面的 canonical long names；调用 `catch22_all(x, catch24=False, short_names=False)`，按名字匹配结果，不靠输出位置或短名称。[S3]

这不是“自动使用最新版”的约定：当前 upstream main 的 v1.0 接口说明多项定义已改变，且新增不可估值的 masking 行为。迁移需要新 `feature_set_version`、显式名称/定义映射与数值对照；不能把新版别名视为公式不变。[S4]

下面的解释为研究阅读标签；精确算法以锁定的源码为准。计算整个向量是公平基线，不因结果与收益无关就事后删项。不可估项保留定义与失败原因。

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

这里的 `tsfeatures` 特指 **Rob Hyndman 等维护的 R 包**；Python 同名包不自动视为数值等价。实现时锁定 R 包版本、依赖和源码 revision；Python 移植必须通过同输入 golden fixtures 对照。[S5]

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

## 8. 计算与 baseline 策略

1. 读取冻结的 SplitManifest、PrimitiveRegistry 和输入 artifact；按白名单解析依赖。
2. 逐真实合约 × 交易日 × session 处理，按第 4 节生成视图与连续段；不把 30GB 数据整体载入内存。
3. 先输出基础描述，再运行锁定的 catch22，最后按适用性运行 8 项 tsfeatures；失败逐项保留，不丢整条输入。
4. 保存分段原值统计和形态指纹；汇总为各交易时段、合约和训练区间的分布、分位数、有效天数与置信区间。
5. 对确有意义的统计量或 primitive 拟合 `P0(X|instrument,session_bucket,activity,recent_volatility)`；组内样本不足回退父组，并记录规则。不是每个指纹必须拟合所有 context。
6. 基线、分位点、输入视图选择、冗余筛选和参数选择只用允许的训练/探索区间，最终 holdout 不参与选择；development 权限遵守 SplitManifest。

catch22 内部标准化、样本中位数 crossing、谱估计等使用当前**已完成画像段**，属于该段描述，最早在段结束且依赖全部可用后发布。完整 session 描述标记 `retrospective_only`；不能把段末信息向段内广播。缺逐条 `available_time` 时按共同契约保留 `availability_unverified/research_only`，不假设等于 event time。

完整段的双侧拟合/平滑只用于回顾画像；若将相同统计量转成在线事件输入，另定义 trailing window、发布时间和只用过去的 fit/transform 契约。

### 8.1 增量价值评价

- 固定比较组 `B0=基础描述+catch22`、`B1=B0+8项适用扩展`，保存精确启用项；不能让 B0/B1 使用不同时间段后直接比较。
- 检查跨日/时段的覆盖率、分布稳定性、Spearman 冗余和计算成本；高相关只标记冗余候选，不事后改写冻结基线。
- 用合成白噪声、AR、波动聚集、局部均值/方差突变检查可解释敏感性；本任务不以未来收益 IC/回测收益筛指标。
- 不对每条 quote 当作独立样本给显著性。跨日比较以交易日或适当独立 block 估计不确定性；不足时记录 `INSUFFICIENT_BLOCKS`。
- 30m 与 session 摘要相互重叠，不能合并计算独立样本数；探索依赖记录候选数量和多重比较范围。

## 9. 输出与可复现契约

建议持久化 `StatisticalFingerprintTable`（long Parquet），连同 Atlas、BaselineManifest 及输入视图定义；遵守 [COMMON_CONTRACTS](../COMMON_CONTRACTS.md)。

| 字段组 | 必需内容 |
|---|---|
| 身份 | `run_id,feature_set_version,profile_id,segment_id,parent_window_id,input_view_id,source_primitive_id` |
| 范围 | `symbol,trade_date,session_id,split_id,split_role,window_start,window_end` |
| 采样 | `time_basis,grid_interval,event_count,profile_window,source_factor_horizon,transform,unit,max_quote_age`；不适用则 null |
| 指标 | `metric_id,library_name,library_metric_name,library_version,source_revision,parameters,value,output_unit` |
| 有效性 | `is_valid,invalid_reason,quality_flags,n_total,n_valid,n_unique,coverage_ratio,parent_coverage_ratio` |
| 时效 / 血缘 | `event_time_end,available_time,availability_status,retrospective_only,input_artifact_ids,config_hash,code_revision` |

`output_unit` 不等于输入 `unit`：ACF 无量纲、stability 为标准化尺度、crossing 为次数、部分 catch22 输出为样本尺度。只有算法明确给出样本 lag 时才允许额外换算 clock seconds，并保留原始值。

指标 ID 示例（字符串仅示意，不是实测）：

```text
input_view_id = obi__d0__clock_1s
profile_id    = clock_1s_30m
metric_id     = c22.CO_f1ecac
```

`input_view_id + segment_id + metric_id + parameters + feature_set_version` 共同确定一条结果。space/size 可以保留在输入元数据里，但不凭表结构自动补齐组合。

失败类型至少覆盖：`MISSING_FIELD,INSUFFICIENT_HISTORY,STALE_DATA,CROSS_SESSION,CONSTANT_INPUT,NUMERICAL_FAILURE,MODEL_FIT_FAILED,UNSUPPORTED_COMBINATION`。项目扩展 reason 必须映射到共同错误类别；缺可用时间是明确质量状态。NaN/Inf、库警告或模型不收敛不能悄悄转成有效 0。

实现须记录版本锁、源码 revision、数值依赖、随机种子（若使用）、标准化/quantile/ACF/PACF 约定，以及抽样/预算策略。不得仅写 `latest`。当前主分支链接用于核对语义，不代替运行时 revision 锁。

## 10. 实现验收清单（待实施）

- [ ] catch22 以 0.4.5 long-name 清单精确匹配 22 项；不依赖 short names 或列表顺序。
- [ ] 8 项 tsfeatures 候选的参数、适用输入和对应官方输出有显式映射；Python 移植与固定版本 R fixtures 数值对照。
- [ ] 对数值稳定性、极短/常量/近常量、重尾、NA、stale、reset 和异步时间建立 fixtures；不可估状态不得被 masking 掩盖。
- [ ] 不跨午休、隔夜、真实合约、split 或 NA 缺口连接序列；事件轴与 clock 轴在可控例子中产生不同且正确的解释。
- [ ] ARCH 返回 R²；谱熵算法、crossing ties、stability/lumpiness 的缩放/分块语义与参考实现一致。
- [ ] 上游只有 primitives 时可独立运行；432 个业务 factors 不成为前置依赖。
- [ ] 非法 space/size/metric 组合不生成；跳过原因、候选数、实际运行数和耗时可查。
- [ ] 添加未来数据不改变已结束窗口的结果；训练 baseline 与输入选择不受 holdout 改变影响。
- [ ] session 摘要不会被标成 session 开始时可用；没有可用时间证据时保留研究用途标记。
- [ ] 合成敏感性和真实数据覆盖率各自记录，不将合成测试等同于市场发现；完成相应验证后再更新实施状态。

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
