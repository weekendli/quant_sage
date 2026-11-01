# Step 09 — Hypothesis Generation（六类生成器）

## 目标

将已登记的 Phenomena 变为**可以被拒绝、比较和复现的实验假设**。不接受只有一句“某变量可能有预测力”的自然语言想法；每个候选必须包含 observable、条件、结果度量、反证条件与证据来源。

## 六种生成机制

| Generator | 输入证据 | 候选方向 | 最小反证 |
|---|---|---|---|
| G1 Rare / Weak Signal | tail、surprise、微弱条件均值偏移 | 异常之后的分布变化 | 同背景随机事件对照、尾部稳健性 |
| G2 Sparse Interaction | 两个单变量效应弱、联合状态异常 | `F1×F2` 的增量解释 | 单变量基准、条件匹配、多重测试 |
| G3 Low-dim Projection | 共同因子/相关结构和 residual | 公共运动去除后的残差有结构 | 与未投影模型比较；投影仅 train fit |
| G4 Local / Multiscale | ACF/motif/event duration 的局部峰 | 效应只在某时间尺度出现 | 临近窗口/尺度对照，窗口多重性 |
| G5 Conditional / Subgroup | regime 下不同的条件效应 | 规律被 C 调节或方向反转 | 控制时段/活跃度/波动、异质性检验 |
| G6 Nonlinear / Symbolic | 非线性依赖、阈值、低维表示 | 存在可简化的非线性关系 | 与线性/分段模型对照、复杂度惩罚 |

## Hypothesis schema

每个 Hypothesis Card 至少包含：

```yaml
hypothesis_id: H-00042
source_phenomenon_ids: [P-00042]
generator: G5
question: 'Does liquidity state modify the response to depth depletion?'
observables:
  feature_refs: [bid_depth_depletion_30s]
  event_refs: [depth_depletion_event]
  condition_refs: [low_liquidity]
outcome:
  target: mid_return
  horizons: [1m, 5m, 30m]
  direction: two_sided
competing_explanations:
  - continuation_after_depletion
  - absorption_and_reversion
  - session_activity_confound
controls: [session_bucket, recent_volatility]
falsifiers:
  - disappears_in_matched_controls
  - only_exists_with_stale_quotes
  - fails_untouched_holdout
status: proposed
```

上例只是一张**待检验**卡片，`direction: two_sided` 防止结果出来后再选方向。Outcome 不限收益：波动率变化、价差恢复时间、事件到达率等也是合法 outcome。

## Generator 的操作步骤

1. 读取 Phenomenon Card 与可用数据字典，选择适用 Generator。
2. 自动形成至少两个竞争解释：目标机制 + 统计/数据伪像或替代机制。
3. 将解释翻译成可观测、可反证的预测，并选择最简 controls；标注无法识别的因果结论。
4. 输出要测量的 effect、样本单位、候选条件/时间尺度和最小样本需求。
5. 在实验执行前冻结 `hypothesis_id`, `hypothesis_version`, `proposed_at`, `discovery_dataset_id`, `search_family_id`。
6. 引擎根据实验预算而非语言流畅程度做候选排序。

## LLM 的边界

- LLM 可以把 ObservableFacts 组织成假设、提出竞争机制、推荐下一实验；统计值、样本数、显著性必须来自分析工具。
- 所有生成结果通过 JSON Schema / Pydantic 验证，只引用 Registry 中存在的 feature/event/regime/outcome。
- 没有 LLM 时提供 rule-based generator，仍可从典型现象自动形成假设，以便本地测试。
- 因果解释若不可识别，统一标记 `mechanism_speculative`。

## 验收

- 六类 generator 都能在给定合成 ObservableFacts 时输出结构化、可运行的 hypothesis，不要求每类都产生真实阳性。
- 输入缺少 required fields 时生成明确失败/skip；不存在的 feature id 无法通过 schema 校验。
- 一张假设至少有竞争解释、对照、falsifier、目标和版本；可从 Registry 回溯来源。
- 负对照/安慰剂不能事后根据有利结果挑选；不同搜索尝试在 ledger 中留下计数。

**前置**：08；**下一步**：10。
