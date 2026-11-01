# Step 10 — Discovery Cube（Features × Conditions × Horizons）

## 目标

把 Hypothesis 转为有预算、有精确时间语义的候选实验网格。**Cube 是假设测试空间，不是一次性穷举全部组合的 Alpha 工厂。**

## 三个轴

- **F: Features / Event-based signals**。可为数值变量、符号事件、跨合约 residual、motif score；有 Registry 引用。
- **C: Conditions / Regimes**。可为合约/交易时段/流动性/波动/活跃度/在线状态，必须在实验起点已知；不能由未来 outcome 定义。
- **H: Horizons**。如 30s、1m、5m、30m，也可按 100 updates 等 event-time horizon，但不同时间基准不可无标注混用。

## Candidate Cell schema

```yaml
cube_cell_id: CUBE-000812
hypothesis_id: H-00042
feature_ref: bid_depth_depletion_30s
condition_expr: 'low_liquidity AND active_session'
horizon: {basis: clock_time, size: 5m}
outcome: mid_return
sample_unit: event_episode
control_definition: matched_non_events
metric: difference_in_conditional_mean
fit_split: discovery
status: planned
```

## Horizon 与 Label 生成

- `decision_time` 为所有参与 feature/condition 都已可用的**最晚**时间，而不是最早行情事件时间。
- clock-time outcome 在 `decision_time + horizon` 通过明确的 as-of policy 取价（如最晚不超过阈值的前值），并记录观察偏离目标时间的 `horizon_age`；若无有效价格则删失/缺失，而不是直接用将来第一帧无条件补价。
- 可配置严格的 `at_or_after` 估值规则，但必须保证其解释和真实可执行性：等待将来的行情意味着结束时刻晚于 horizon，不允许用来冒充准时可执行价格。
- 会话结束前无法完整观察 horizon 时默认 `censored`，不能自动延伸到隔夜。
- 事件 episode 的结果标签避免重叠；若保留重叠样本则对其相关性作处理。

## 搜索策略

1. **Seed**：来自 Phenomenon 的少量主 feature、受支持的 condition 和统计指纹显示的 horizons。
2. **Cheap Filter**：样本可用率、事件独立天数、极端稀疏、规则重复、时间信息泄漏检查、效果不确定性上限。
3. **Coarse Test**：在 Discovery split 估计 effect、区块置信区间、样本基数、简单对照；记录**所有**尝试。
4. **Expand**：仅在现象有依据的情况下扩展相邻 horizons、相近 conditions、低复杂度交互（beam search）。
5. **Stop**：遵守全局/每 hypothesis 预算、family-wise 试验记录、最小有效样本和最大语法复杂度。
6. **Promote**：满足探索门槛的候选冻结定义后进入独立验证；不能因验证失败反复调参并继续把同一 holdout 称为独立样本。

## 评价指标

一条 cell 不只存 IC：必须支持 effect size、direction、基线差、置信区间、n_events、n_days、coverage、turnover（适用时）、search_family_size、失效条件和计算成本。Outcome 若为未来收益，可以另存回测，但不取代统计检验。

## Example

现象：`depth drop but stable mid`。首批 Cells：深度下降强度 × 高/低流动性 × 1m/5m/30m。先比较非事件配对对照，再按结果决定是否扩展 quote activity 维度。不应立刻遍历200个交互特征。

## 验收

- 合成事件的 1m/5m horizon 计算有显式时序测试、跨 session 截断与 stale 标签检查。
- 同一个 feature 在不同 H 上以唯一 cell id 保存，所有失败和放弃的搜索也计数。
- 将测试集标签任意改写不改变 Discovery 阶段生成的候选列表。
- Search budget 达到上限可稳定终止，重跑同样 seed/config 输出一致。

**前置**：09；**下一步**：11。
