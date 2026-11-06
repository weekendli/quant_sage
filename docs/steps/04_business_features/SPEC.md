# Step 04 — Business Features & Time Windows

## 当前收敛范围：Step 02–05 数据与特征闭环

本步消费 Step 03 结果和冻结 FeatureSet 中选中的业务组合，生成 Feature(kind=business) 与 FeatureResult。只实现确定业务公式、合法 space/size/回看 L、窗口端点/reset/有效性；不要求自动创造公式或运行全部 432 个组合。

432 是目录合法名称数，不是每次必须计算的数量。未选中的 pending_semantics 不阻塞已选 ready 子集；选中的未明确公式阻断对应任务并报告原因，不改变公式。只有声称“完整 pack 数值交付”时才要求所有相关语义明确。

统一管理字段见 [domain.detail.md](../../domain.detail.md)，当前调度边界见 [WORKFLOW_STAGES.md](../../WORKFLOW_STAGES.md)。旧 Registry 名称表示 Feature/FeatureResult 的分类视图，不要求新增独立领域仓库。Study、FeatureSet、Run、Review 的通用管理由 Step 01 承担，领域步骤只实现本步算子与结果契约。


## 目标

从 Step 03 primitives 计算预定义业务特征及其 lookback windows，保持原公式、适用参数和质量语义。**Feature 是对当下/过去的描述，不能含任何未来标签。** 本步骤直接依赖 primitives，不依赖尚未执行的 Step 05 统计特征/Atlas/baseline；输出供 Step 05 及后续 events/regimes 使用。

## 特征表达与执行

```text
Feature := Primitive
         | unary_op(Feature, Window)
         | binary_op(Feature, Feature)
         | cross_instrument_op(Feature, InstrumentSet, Tolerance)

TimeBasis := clock_time | quote_event_count | volume_time
Window    := (TimeBasis, positive_value)
```

可用结构化公式/typed AST 表达依赖；Grammar 是内部实现选择，不要求自动创造候选。固定 pack 的节点覆盖 primitive 引用、有效样本均值、窗口求和、历史端点差、reset 检查和安全比值/组合即可。所有 node 具有输出类型、单位、所需窗口、可用时间延迟、缺失处理规则。

## 运算和类型约束

| 运算族 | 举例 | 约束 |
|---|---|---|
| Difference / Rate | `delta(depth_bid, 30s)` | 不能跨 session 或数据缺口误连 |
| Snapshot aggregation | `mean(obi_snapshot(d), 1m)` | 使用该指标的有效快照数，不按名义时长补零 |
| Interval sum | `sum(trade_volume_interval, 1m)` | 原生区间量，不再次差分 |
| Endpoint difference | `mid(t)-mid(start)` | 过去端点定位、时效及源引用明确 |
| Window composition | `sum(volume,h)/mean(depth_ref,h)` | 保留先聚合后组合的顺序、单位和有效性 |

分布分位、ACF、熵、条件标准化和基于模型的残差归 Step 05；未来业务扩展若需要 slope/duration 等算子，应单独声明，不是本固定 pack 的额外实施要求。

**示例 Feature DSL：**

```yaml
id: bid_depth_depletion_30s
expr:
  op: relative_change
  input: {primitive: depth_bid_1}
  window: {basis: clock_time, size: 30s}
semantics:
  availability: at_current_observation
  on_session_boundary: reset
  missing: null
```

## 定义、注册与预算

配置遵循 [Step 01 显式 items 与执行记录契约](../01_workflow_engine/IMPLEMENT.md)。每个 business item 独立声明合法的 family/space/size/lookback 元组；模块为引擎暴露 catalog 和类型化依赖，引擎按选中输出反向解析 Step 03 所需计算。每个分区的执行状态与有效样本统计分别上报，不先全量生成 432 个数值再过滤。

1. 优先读取冻结 FeatureSet 的业务条目及解析 manifest，只展开选中元组并校验 family 合法域；输入、单位、时间轴和窗口必须可解析。
2. 保存 `feature_id, formula/AST, description, unit, lookback, availability_rule, validity_rule, dependencies, code_hash`，公共子表达式共享计算。
3. 固定公式的计算不进行 baseline/PCA/条件分位等拟合。需要这些统计变换时由 Step 05 执行并登记模型版本。
4. 预算不足以覆盖全部已选目标及必要依赖时，提交前返回 BUDGET_EXCEEDED，由调用方缩小选择后重新提交；首版不按预算跳过已选项。运行中失败可产生 PARTIAL，保留已完成结果与失败记录。语义去重可以共享计算，但保留原业务名称和 alias。每个最终指标不必拥有同名的单快照 primitive；F09–F12 使用窗口算子后组合。
5. 将来如需探索新公式，使用独立、版本化的研究配置；Step 05 的统计证据只能建议下一轮配置，不在同一 DAG 中反向修改本步骤。

## 国债 L2 feature pack：本步骤负责的拆解

公式依据为只读的 [原始 L2 feature spec](../../references/gov_bond_l2_feature_spec.md)。**Step 04 = workflow Stage 03 `features`**；消费 [Step 03 的 L2 primitives](../03_research_dimensions_and_primitives/SPEC.md)，不重复定义 tick/depth/VWAP 公式。

### 预定义 pack 与自动生成分开

`gov_bond_l2_market_state` 是固定业务定义 pack；本步骤负责公式表达、合法组合展开、校验与执行。未来的自由组合生成属于可选研究扩展，不能成为固定 pack 的前置要求，也不能静默改变其公式、窗口或参数域。

```text
D = {0,1,2,3,5}
Q = {25,50,100}
H = {1s,3s,1m,5m,10m,15m,30m,60m}
```

合法空间为各 family 参数域的并集 `union_f ({f} × allowed_parameters_f)`，而非全局 Family × D × Q × H。维度不适用表示参数不存在，输出元数据可为 null；不能为 spread 添加 d/q，或为 VWAP 成本添加 d。完整 pack 声明 432 个定义；显式业务子集须保留 selection manifest 与每项未执行原因，不能冒称完整交付。预算不足在提交前报错，不能静默改变定义或执行子集。

### 按 family 的执行映射

`mean_h(x)` 表示按原规格有效快照掩码，在窗口内对单快照值取算术均值；`mean_h` 的分母是该输入实际有效快照数，不是名义秒数。表中 `B_h/S_h` 是累计主动买/卖量的窗口端点差，`Dbar_h=mean_h(depth_ref)`，`M_h=(mid_t-mid_start)/tick_t`。

| Family / 最终名称 | Step 03 输入 | Step 04 运算（保持原定义） | 参数域 / 数量 |
|---|---|---|---|
| F01 `spread_ticks` | `spread_ticks` | `mean_h(spread_ticks)` | H / 8 |
| F02 `obi` | `obi_snapshot(d)` | `mean_h(obi_snapshot(d))` | D × H / 40 |
| F03 `wobi` | `wobi_snapshot(d)` | `mean_h(wobi_snapshot(d))` | D × H / 40 |
| F04 `min_coverage` | `min_coverage_snapshot(d,q)` | `mean_h(min_coverage_snapshot(d,q))` | D × Q × H / 120 |
| F05 `capacity_imb` | `capacity_imb_snapshot(d,q)` | `mean_h(capacity_imb_snapshot(d,q))` | D × Q × H / 120 |
| F06 `size_spread` | 双侧 fill 掩码、`size_spread_snapshot(q)` | 仅对双侧完整覆盖的有效快照取均值 | Q × H / 24 |
| F07 `fill_dist_asym` | `fill_dist_asym_snapshot(q)` | horizon 聚合语义待确认，见下文 | Q × H / 24 |
| F08 `walk_cost_asym` | `walk_cost_asym_snapshot(q)` | horizon 聚合语义待确认，见下文 | Q × H / 24 |
| F09 `active_flow_imb` | 累计主动买/卖量及 reset 标记 | `(B_h-S_h)/(B_h+S_h)` | H / 8 |
| F10 `volume_to_depth` | `trade_volume_interval`, `depth_ref` | `sum_h(trade_volume_interval)/Dbar_h` | H / 8 |
| F11 `mid_move_ticks` | `mid`, `tick_size` | `M_h`，起点取原规格指定的过去有效快照 | H / 8 |
| F12 `flow_impact_eff` | 与 F09/F10/F11 共享依赖 | `sign(NF_h)*M_h/(abs(NF_h)/Dbar_h+epsilon)`，`NF_h=B_h-S_h` | H / 8 |
| **合计** | | 定义数，不保证每个快照有 432 个有效值 | **432** |

本 pack 具有三种分解，不能一律实现成同名 primitive 的窗口均值：

- **F01–F06：44 个单快照描述 × 8 windows = 352 个指标**，均值聚合已由原文明确，F06 有额外双侧可完成掩码。
- **F07/F08：6 个单快照描述，声明 48 个窗口组合**，时间聚合仍待原定义语义明确，不默认补均值。
- **F09–F12：32 个窗口组合**，由原始累计量/区间量、mid、depth_ref 等 primitives 经过窗口运算再组合，不要求 4 个同名单快照效率/比值。

总计 `352 + 48 + 32 = 432` 个业务定义；共同窗口算子及计算节点可以复用，公开 artifact 只需承载复用序列、最终值和必要审计证据。

### F09–F12 的窗口级组合

令 `start` 为冻结端点策略命中的同 session 历史观察，窗口集合仍为 `(t-h,t]`：

```text
B_h = active_buy_cumulative(t) - active_buy_cumulative(start)
S_h = active_sell_cumulative(t) - active_sell_cumulative(start)
V_h = sum_{u in W_h(t)} trade_volume_interval(u)
Dbar_h = valid_mean_{u in W_h(t)} depth_ref(u)
M_h = (mid(t) - mid(start)) / tick_size(t)
NF_h = B_h - S_h

F09 = (B_h - S_h) / (B_h + S_h)
F10 = V_h / Dbar_h
F11 = M_h
F12 = sign(NF_h) * M_h / (abs(NF_h)/Dbar_h + epsilon)
```

累计量和 mid 使用统一 backward as-of 规则：各自在同 session 内取满足 `event_time <= t-L` 的最近有效观察；因有效掩码不同，实际命中的记录可以不同，须保存各自源 ID/时间。`max_endpoint_age=(t-L)-matched_time` 的上限须显式配置并冻结，超限输出 NA；未配置则拒绝相关定义执行。端点无效、历史不足、窗口内 reset/相关缺口、零分母或无有效深度都按依赖规则传播 NA；仅 F12 使用原公式中的 epsilon。

命名严格沿原规格 §7，例如 `obi__d0__h3s`、`min_coverage__d3__q50__h10m`。参数保存数值/语义枚举，序列化时只加一次 `d/q/h` 前缀；不得输出 `dd0/qq50`。

### 计算顺序不可交换

- F02/F03/F05 必须先逐快照求比值再平均，不能改成窗口聚合深度之比。
- F04/F05/F06/F07/F08 每个窗口内快照 u 使用自己的 `q_u=alpha*depth_ref_u`；不能用当前 `q_t` 重新评价整个窗口，也不能用平均盘口先算 VWAP。
- F09 先求累计端点差，再求比值；不能平均逐区间 imbalance。
- F10 是窗口成交量之和除以窗口平均深度，不是逐快照 volume/depth 的均值。
- F12 使用窗口净 flow、窗口平均深度和过去 mid move 组合；不是逐快照效率的均值，也不是未来价格响应。`NF_h=0` 为 NA；epsilon 明确版本化，不能用于掩盖深度为零或其他无效依赖。

### 窗口、有效性与发布

窗口为同一真实合约、交易日、连续 session 的 `(t-h,t]`。历史长度不足 h 输出 NA；不跨午休/隔夜补历史、不按行数冒充时长、不重采样后改变 snapshot-average 权重。空有效集合输出 NA，不补零。

可共享的 AST 节点至少包括 `SessionWindow`、`ValidRollingMean`、`RollingSum`、`BackwardAsOfAtWindowStart`、`CumulativeWindowDelta`、`WindowResetCheck`、`SafeRatio`；名称可适配现有实现，但输出类型、参数、掩码和可用时间必须可序列化。mid 和累计量端点查找须保留命中的源观察时间/ID；禁止 nearest/未来 join。

窗口内累计量 reset 导致相关 flow factors 为 NA；不能只比较端点来漏掉中途重置。F06 有效均值必须同时输出总快照数、实际可完成快照数与比例，避免 NA 筛选后丢失可见深度不足的信息。

成交量未验证及对账不一致仅警告，保留原生区间量并传递原因/计数，不单独使 F09/F10/F12 失效。缺少行情推荐按 [gap_policy_v1](../../IMPLEMENTATION_DESIGN.md#64-缺少行情的推荐处理机制) 处理：影响依赖的已知缺口/超限观察间隔使窗口 NA，重新积累完整 L 后恢复；F06 的不可完成快照仍按原公式有效均值处理，不等同于源行情缺口。输出按 exchange/instrument_id/trade_date 分区。

输出原规格 §8 的 factor 表字段，并按共同契约增加 `session_id,event_time,available_time,availability_status,feature_id,input_artifact_ids` 和有效样本/窗口信息。语义待明确与输入数据不足是不同状态，不能用同一个 NA 原因混淆。

### 补充执行语义与待明确事项

原始参考文档保持只读；用户确认的执行规则在此版本化，剩余待决项明确保留：

1. **F07/F08** 给出单快照公式和 horizon 列表，却没有显式的时间聚合公式。Step 03 单快照结果可实现；Step 04 为其 48 个组合登记 `definition_status=pending_semantics`，在聚合规则明确并版本化前不得以窗口均值冒充原定义，也不得重复同一快照值填充 8 个 horizons。
2. **F09/F11/F12 已确认**：历史端点使用 `<= t-L` 的最近有效观察，包含等时值；累计量与 mid 采用相同匹配规则并分别保存命中记录。最大端点 age 作为必填配置冻结，禁止依赖引擎默认值。

固定 pack 的完整数值交付门槛包括上述语义全部明确；定义清单 432 项不表示本次已消除所有歧义或已完成实现。明确后的执行语义写入独立版本化配置/决策记录及 registry，不修改原始参考文档。

Step 05 在本步骤之后，接收选定业务特征以及 Step 03 primitives，计算所选 catch22/tsfeatures 统计特征；条件 baseline 是后续可选扩展。固定 pack 数值不依赖这些统计结果；不建立统计阶段返回本步骤的运行依赖。

## 编译与执行

- AST 编译成 Polars LazyFrame expressions 或 DuckDB SQL；对复杂窗口可走 batch UDF，但必须列明内存/时间复杂度。
- 数值安全：除零得 null+flag，不产生无限值；数值尺度、overflow、nan 有规则。
- 算法计算粒度：优先按合约×交易日分区，可配置 lookback 边界；跨分区 state 显式传递，不隐式丢历史。
- `row_count` 与 `availability_time` 不可因全量 join 静默变化；输出 schema check。

## 验收

- AST 可序列化、反序列化、hash 去重；非法组合（例如 volume_time 但没有可靠 volume）报可解释错误。
- 人工小数据分别验证 clock/event window 的数值不同且正确。
- 特征计算不改变原始观察顺序，不跨不允许的 session，所有 null 传播一致。
- 未来验证数据无法影响训练期 fit 的任何参数；禁用负 lag、centered rolling、未来 frame joins。

**前置**：03；**下一步**：05。
