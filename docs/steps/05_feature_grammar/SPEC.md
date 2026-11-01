# Step 05 — Feature Grammar: typed feature-generation language

## 目标

不靠 LLM 随意拼公式，而是从 Primitives 和 Statistical Atlas 中，通过有类型、可控复杂度的语法生成可复现 Feature。**Feature 是对当下/过去的描述，不能含任何未来标签。**

## 语法设计

```text
Feature := Primitive
         | unary_op(Feature, Window)
         | binary_op(Feature, Feature)
         | cross_instrument_op(Feature, InstrumentSet, Tolerance)

TimeBasis := clock_time | quote_event_count | volume_time
Window    := (TimeBasis, positive_value)
```

推荐 AST node：`PrimitiveRef`, `Lag`, `Delta`, `Return`, `RollingMean`, `RollingStd`, `RollingQuantile`, `Ewma`, `Slope`, `Duration`, `CountAbove`, `Ratio`, `Interaction`, `ConditionalRank`, `CrossResidual`。所有 node 具有输出类型、单位、所需窗口、可用时间延迟、缺失处理规则。

## 运算和类型约束

| 运算族 | 举例 | 约束 |
|---|---|---|
| Difference / Rate | `delta(depth_bid, 30s)` | 不能跨 session 或数据缺口误连 |
| Level / Rank | `past_quantile(spread, 5m)` | 只能使用历史；不足样本返回缺失 |
| Trend | `slope(mid, 100 updates)` | 回归窗口按 event time index |
| Persistence | `duration(OBI>0.4)` | streak reset 与缺失规则明确 |
| Interaction | `OBI * high_activity_score` | 单位、零值、相关冗余要检查 |
| Cross-Instrument | `residual(T, TF)` | 使用过去 fit 的尺度、as-of、报价时效 |
| Entropy / Motif | `entropy(quote_arrivals, N)` | 最小窗口数量、算法成本预算 |

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

## 候选生成策略

1. 由 `ResearchDimensionCatalog` 选择可用 primitive，按维度启用 grammar operators。
2. 根据 Atlas 的 `anomaly/ACF/relationship/time_scale` 给窗口排序，而不是无脑乘积。
3. 限制 AST 深度（例如≤3）、不同窗口/参数数、最大 feature budget；生成 `semantic_hash` 去重，并基于数据计算相关冗余。
4. 每个 Feature 都写 registry：`feature_id, AST, description, required_fields, unit, time_basis, min_history, availability_rule, dependencies, code_hash`。
5. `fit` 与 `transform` 必须区分：条件分位、z-score、projection、PCA 等统计参数只能在过去训练数据 fit。

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

**前置**：04；**下一步**：06、07（可并行）。
