# Step 12 — Research Planner & Knowledge Registry

## 目标

把一次测试结束转化为下一次更有信息价值的实验，实现发现—反证—修正的研究循环，而不是对同一个数据盲目重复搜索。

## Research Planner

输入：Phenomenon、Hypothesis、Falsification Result、可用字段、资源预算、历史失败记录。输出：有理由的 `NextExperiment` 队列。

可先采用启发式排序：

`priority = expected_discrimination_between_competing_explanations × evidence_quality × sample_feasibility / estimated_cost`

所有分项均须明示其估计规则；**该 priority 只是调度启发，不是某假设为真的概率**。以后可选 Bayesian Expected Information Gain，但 MVP 不必实现精确的信息论优化。

## 举例

观察：OBI 在高波动状态表现不同。竞争解释可能是：真实的状态调节、仅由时段结构导致、仅由流动性共同驱动、时间对齐伪像、搜索偶然性。

最有判别力的下一实验可以是“在同一个交易时段+相似深度的样本中比较高低波动条件”。如果结果失效，记录“效应主要由深度/时段解释”，不继续以相同假设修改参数在 holdout 上刷分。

## Knowledge Objects

| 对象 | 内容 | 状态 |
|---|---|---|
| `ObservableFact` | 直接计算事实及其估计不确定性 | measured |
| `Phenomenon` | 有背景的可重复观察与问题 | observed / unresolved |
| `Hypothesis` | 竞争解释、预定预测与反证计划 | proposed / frozen |
| `Experiment` | cell、control、split、估计方法 | queued / completed |
| `FalsificationResult` | 支持/失败/无效与证据 | audited |
| `ResearchQuestion` | 下一项有区分力的实验 | planned / retired |

关系：`DERIVED_FROM`, `TESTS`, `CONTRADICTS`, `REFINES`, `REPLICATES`, `SAME_FAMILY_AS`。MVP 可用 SQLite 关系表或 JSONL，不需先上图数据库。

## 研究循环的边界

- 失败结果不能删除；同样假设在多次测试中若更改条件，应派生新版本，并保留原版本与搜索开销。
- 有反证证据应主动生成竞争解释，不能由 LLM 只写“仍有潜力”。
- 不把数据集覆盖范围之外的因果结论推广为确认知识。
- Planner 不直接修改 `final_holdout` split，也不得静默重开已消耗的独立测试。

## 输出与交互

CLI 或报告至少支持：`quantsage research questions`, `quantsage research show <id>`, `quantsage research lineage <id>`、按 instrument / phenomenon / status 过滤。总结最有价值的 5–10 条现象、它们的主要反证风险和下一实验。

## 验收

- 使用伪合成的两条竞争解释产生不同的后续实验，并能解释为何优先。
- 一条被反证的假设不被清除，只从候选升级队列移除；产生新的 REFINE 版本时有血缘。
- 完整的从原始字段 → feature → event → phenomenon → hypothesis → cube → test 路径可查询。

**前置**：11；最终回到 08 或 09 形成新一轮探索，不能修改既往已冻结实验。
