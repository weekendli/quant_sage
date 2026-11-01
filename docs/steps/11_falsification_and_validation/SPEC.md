# Step 11 — Falsification & Independent Validation

## 目标

为每条候选假设执行**事先登记的反证实验**。输出不只是 p-value 或 backtest Sharpe，而是何种证据支持或削弱了该现象，以及是否能外推到独立时段。

## 数据分割（必须先定）

- 按**时间连续区块**划分 `discovery`, `development`, `final_holdout`，先于特征选择和测试冻结，保存 `SplitManifest`。
- 训练模型/分位基准/因子投影仅在相应的过去训练窗拟合；可使用 walk-forward 更新，但更新策略也需预定义。
- 相邻样本的未来标签窗口重叠时需 purge；必要时 embargo；划分时尊重事件 episode 和交易日。
- `final_holdout` 仅用于冻结候选最终验证一次；反复修改假设后不能继续作为真正未触碰测试集。

## 三层 Falsification Tests

| 类型 | 核心检查 | 实现方式 |
|---|---|---|
| Statistical | 差异能否被时序相关噪声解释？ | 按交易日或事件块 bootstrap；保留依赖结构的 surrogate/shift；区间估计 |
| Structural | 是否是时段、波动、活跃度或行情质量的伪像？ | matched controls、covariate stratification、placebo timestamps、stale quote exclusion |
| Independent | 不同日期/合约/状态下还能重复吗？ | walk-forward、独立 holdout、cross-contract robustness |

## 样本与独立性

高频 tick ≠ 独立 N。默认报告：`n_rows`, `n_event_episodes`, `n_unique_days`, `n_independent_blocks_est`，并说明各估计和区块选择。重叠 horizon 事件可使用 cluster-robust uncertainty，不能把它们当独立样本做朴素 t-test。

## 多重检验

- Hypothesis 归属 `search_family_id`（同一个现象、相关 feature 与 horizons 的搜索族）。
- Discovery/Development 的所有候选与失败尝试进入 Experiment Ledger；需要报告 family-wise tests、至少一种 FDR 控制（如 BH，需声明依赖假设）或适合相关检验的 resampling 方案。
- 若使用 permutation/max-statistic，应在保持时间块结构的前提下构造 null；简单随机打乱每行通常不合法。
- 不把搜索后的最高 Sharpe 当作无偏估计；可附 deflated Sharpe 等诊断，但它们不能替代真实样本外检验。

## 反证结果状态

`SUPPORTED_IN_SCOPE | INCONCLUSIVE | FALSIFIED_BY_TEST | FAILED_REPLICATION | INVALID_DATA | NOT_TESTABLE`。

`SUPPORTED_IN_SCOPE` 表示特定数据、条件、测量和验证范围内未被推翻，**不等于经济机制已得到证明，也不等于可交易**。

## 可交易性是独立测试

对于收益类 outcome，增加 tick size、手续费、滑点、成交可行性、延迟、流动性和风险预算的情景分析。在没有逐笔订单与队列数据时，填单概率不能伪装成精确可验证值。

## 输出 Falsification Report

至少包括：hypothesis id/version、事前测试、所有 control、sample counts、effect/CI、同假设搜索次数、经修正统计量、跨时期结果、失败原因、未验证限制、traceable artifacts。

## 验收

- 在注入真实可重复信号的合成数据上，检测到规定效应；在无信号但强自相关的数据上，拒绝用逐行独立检验夸大显著性。
- 改变 `final_holdout` 前阶段参数与候选列表完全不变；holdout 使用有审计记录。
- 数据质量/时间泄漏 test 失败可直接 `INVALID_DATA`，不得继续输出被支持结论。
- 同一结果通过 run id、固定配置、固定 split 与代码版本重现。

**前置**：10；**下一步**：12。
