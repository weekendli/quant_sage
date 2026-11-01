# Step 04 — Statistical Atlas & Baseline / Null World

## 目标

第一轮回答 **“这个数据世界正常情况下是什么样”**。先发现数据结构，不以未来收益预测为必要输入。Atlas 的输出必须保留统计量的条件、样本数、样本期与置信程度，不得只给一个全样本 ACF 值。

## 输入 / 输出

- 输入：标准化数据、Primitive Registry、交易日历、冻结的训练/探索时间区间。
- 输出：`StatisticalAtlas`、`BaselineModels`、`DependencyGraph`、`AnomalyCandidateTable`；附证据表和采样/估计参数。

## 研究清单

| 统计方向 | 基础方法 | 产出 |
|---|---|---|
| Marginal Distribution | 分位数、偏度、尾部、零变动比例 | distribution fingerprint |
| Temporal Dependence | ACF/PACF、持续时间、间隔统计 | lag/time-scale map |
| Volatility Structure | 不同窗口 RV、vol clustering | volatility atlas |
| Session Seasonality | 同交易时段内的分位数/强度 | conditional seasonality |
| Cross-variable | 稳健相关、lead-lag、partial dependence 探索 | dependency candidates |
| Cross-instrument | as-of alignment、共同变化与残差 | cross-contract atlas |
| Change & Motif | 在线/离线变化点、重复子序列（可选） | pattern candidates |

L1 使用稳健简单统计，L2 才使用 tsfresh/catch22、Matrix Profile、非线性依赖等扩展；输出应将“统计描述”“预测发现”标注为不同证据层次。

## 构建正常世界（Baseline）

对 Primitive 或 Feature `X`，先在训练区间用当时可观测的 Context `Z` 拟合：

`P0(X_t | instrument, session_bucket, activity_bucket, recent_volatility, ...)`

MVP Baseline：分组中位数、MAD、分位数（用训练样本拟合），小样本组回退至父级分组；支持平滑/收缩。生成 `expected`, `scale`, `conditional_quantile`, `tail_probability` 或其可估计子集。针对长尾和零膨胀优先条件分位而不是默认正态分布。

异常示意：`robust_score=(X - conditional_median)/max(conditional_MAD * 1.4826, eps)`。这一值只是偏离正常背景的指标，不等于显著的可交易信号。

## Null / Control 世界必须区分

- **Descriptive baseline**：同合约/同交易时段的通常分布，用于定义异常。
- **Conditional control**：在相近波动/活跃度/时段背景中的非事件观测，用于后续实验对照。
- **Statistical null**：保持时序依赖结构的 block/circular permutation、shift 等，用于估计随机结果的分布。

这三者用途不同，不能混写为一个 `null_model`。

## 防止信息泄漏

- baseline、阈值、标准化器在训练子样本 fit；在随后样本 transform。若需要自适应更新，采用滚动过去窗口，绝不包含当前未来结果。
- 高波动 regime 若定义用未来30分钟 realized volatility，在当时不可用，不能做可交易条件；可以作为事后描述，但标记 `retrospective_only`。
- 相关、lead-lag 只产生候选依赖，不直接证明因果；异步采样可能导致伪领先。
- 跨训练/验证时间折按 split 重建 baseline，避免拿整个训练+测试期的分位点定义异常。

## 验收

- 能输出至少 price, spread, depth, quote intensity 的分布及日内时间特性；无字段时生成明确跳过理由。
- 构造人工日内季节性数据：全局异常但时段条件下正常的样本应被降低误报。
- 在完全独立的验证天数上应用固定 baseline；更改验证集不影响 train-fitted 统计量。
- 每项 Atlas 图表/指标均有来源分区、sample count、算法参数，支持 artifact explain。

**前置**：03；**下一步**：05。
