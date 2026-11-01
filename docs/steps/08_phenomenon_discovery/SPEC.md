# Step 08 — Phenomenon Discovery

## 目标

建立真正的 Market Observatory 核心：**把一堆统计值和事件转化为“值得解释的现象”**。Phenomenon 是可检索的观察与待解决问题，不等同于已确认机制或交易策略。

## 六类现象探测器

| Detector | 输入信号 | 典型现象 |
|---|---|---|
| Tail / Anomaly | baseline tail probability | 某交易时段极端深度骤降 |
| Recurrence / Motif | event episodes、局部形态 | 类似多阶段盘口变化反复出现 |
| Divergence | 两变量 conditional dependence | 深度下降但价格不动 |
| Conditional Contrast | feature×regime summaries | 某变量在高/低波动时表现不同 |
| Residual Structure | 已解释部分的残差 | TF/T 共同移动后存在异常价格创新 |
| Transition | change-point / state transitions | 低波动→高活跃状态跳变 |

第一版可实现 Tail、Divergence、Conditional Contrast、Residual；Motif/Nonlinear 可以通过插件逐步加入。

## 流程

1. 从 Atlas/Features/Events/Regimes 构造 `ObservableFacts`（事实、计算方法、样本期、置信区间、缺失、对照基准）。
2. 检测候选现象，生成最短描述：`what, where, when, compared_with_what`。
3. 对同源/相近时间/高度相关的候选进行语义 + 统计去重，避免重复计数。
4. 打分依据：事件独立天数、跨样本稳定性、背景解释后剩余差异、数据质量与检测成本；不能仅按最小 p-value 排名。
5. 注册 Phenomenon Card，附候选解释与**尚不知道什么**；不在这一阶段宣称预测结论。

## 推荐 Schema

```yaml
phenomenon_id: P-00042
kind: divergence
statement: 'Bid depth contracts while mid stays near unchanged'
observables: [bid_depth_change_30s, mid_return_30s]
context: {instrument_family: T, time_basis: clock_time}
evidence:
  sample_window: discovery_2024H1
  n_episodes: null  # 运行后填真实值
  benchmark_id: base_depth_by_session
  summary_artifact_id: artifact_...
open_questions:
  - delayed response or true absorption?
  - confounded by time of day?
provenance: {feature_refs: [], event_refs: []}
status: observed_unverified
```

卡片应保存机器可读事实和自然语言摘要；LLM 只负责在事实基础上表达问题，不得杜撰统计数值。

## 数据质量和反例

每个现象应自动生成：相邻无事件样本、相似背景但未触发样本、可能的误报机制（stale quote、跨 session、错位同步、合约切换）、最小可检测效应或估计不确定性。

## 验收

- 合成数据包含“深度下降但 mid 不变”插入片段，系统应识别对应 Divergence，且给出触发区间/基准。
- 随机白噪声数据不应因为重复扫描就被总结为“确认的市场机制”；只能保留未验证候选与搜索数量。
- 同一个事件经多个 Feature 命中时去重/关联，能够解释为何合并。
- Phenomenon Card 在没有任何未来收益标签的条件下可生成。

**前置**：04、05、06、07；**下一步**：09。
