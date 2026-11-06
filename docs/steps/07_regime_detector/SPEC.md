# Step 07 — Regime Detector

## 输入来源与时间契约

Step 04 提供业务 features，Step 05 提供选定序列的 trailing catch22/tsfeatures 及条件 baseline；联合使用原单位水平与统计结构，按真实可用时间对齐。完整 session 的回顾画像不得作为该 session 内的在线输入。分别记录业务 lookback L、统计窗口 W 与采样/评估步长，避免把重叠窗口造成的相关性当独立证据；此处不使用未来 impact H。

Events / regimes 默认并行；只有显式声明时，过去已检测事件的 trailing 摘要才作为 regimes 的额外输入，不能使用事件最终峰值、最终持续时长或未来状态。

## 目标

识别具有**时间持续性**的市场环境（而非单次 Event），并区分在线可用状态与事后解释状态。先做可解释 L1，再增加数据驱动 L2。

## 三层检测器

| 层级 | 实现 | 用途与限制 |
|---|---|---|
| L1 可解释状态 | 基于训练期条件分位数的 low/high volatility、liquidity、activity | 最容易检验并解释 |
| L2 统计状态 | k-means / GMM（先行）、HMM（可选） | 捕捉多变量联合状态；模型参数只在过去 fit |
| L3 状态转移 | online CUSUM / change-point evidence | 发现状态发生变化；离线检测仅描述过去 |

不要把一个简单二元阈值随意称为“发现了经济体制”，Regime 是研究上的统计状态候选。

## 输入与状态表示

使用过去已可获得的 Feature 向量（如 realized vol、spread、depth、quote intensity），输出：

`regime_id, instrument_id, event_time, available_time, state_id, state_name, probability(optional), model_version, observation_count, online_flag, quality_flags`。

对于 HMM：实时可用的应是 `P(S_t | X_≤t)`（filtered probability），**不能**使用 `P(S_t | X_1:T)` 的 future-smoothed probability 来生成在线交易条件。

## 关键设计点

- L1 阈值按合约/交易时段 fit；样本过少回退父级；状态切换可以使用迟滞与最短停留时长。
- L2 的 K 值做最小模型选择，初版可只做2–4种候选；不能用 test performance 决定最终状态数。
- L2 的 state 0/1/2 标签本质无语义；跨重训用特征中心或匹配算法对齐状态，而非默认编号相同。
- 评估状态占用率、持续时间、状态转移矩阵、跨时期稳定性，识别过短碎片化状态。
- 将 retrospective-only 状态与 point-in-time 状态分开命名/存储，避免用事后信息做 Cube 条件。

## 组合 Regime

先保持状态可组合：`volatility=high`、`liquidity=low`、`activity=high`。仅在统计证据充分时生成复合标签 `high_activity_low_liquidity`；限制组合复杂度及最小独立交易日/事件数。

## 验收

- 人工序列含两个波动/流动性分布，L1 能识别合理状态变化，边界处不因轻微噪声反复切换。
- 在线状态只依赖当前及过去；在前缀固定、未来追加的数据上结果完全一致。
- HMM 可选模块缺失不能阻断 L1；模型重训后 state 映射与版本可查。
- Regime summary 包括可用性、持续性和样本覆盖率；不可为验证集重新拟合阈值。

**前置**：04、05；**下一步**：08。
