# Codex Implementation Task — Step 05: Statistical Features & Quality

## 当前收敛范围：Step 02–05 数据与特征闭环

本步消费选定的 Step 04 业务结果或 Step 03 primitive 结果，按 FeatureSet 的 input + W + sampling + metrics 生成 Feature(kind=statistical) 与 FeatureResult。只依赖实际选中输入；primitive-only 配置不要求先运行 Step 04。保留 catch22 22 项及精选 tsfeatures 8 项的完整定义/对照目录，每次仅执行被明确选择的合法统计量。

当前必需交付为统计数值、采样/时间语义、质量/coverage 和可复用结果索引。完整 Atlas、条件正常分布拟合、异常分数、依赖图、null/control、事件/状态发现均为后续扩展，不作为本步核心验收前置；详细数学资料保留供显式扩展使用。

统一管理字段见 [domain.detail.md](../../domain.detail.md)，当前调度边界见 [WORKFLOW_STAGES.md](../../WORKFLOW_STAGES.md)。旧 Registry 名称表示 Feature/FeatureResult 的分类视图，不要求新增独立领域仓库。Study、FeatureSet、Run、Review 的通用管理由 Step 01 承担，领域步骤只实现本步算子与结果契约。


**Milestone:** M3<br>
**Upstream dependencies:** selected Step 03 or Step 04 results<br>
**Scope:** implement only this stage, not downstream algorithms.

## Required reading

1. [`../../AGENTS.md`](../../AGENTS.md) — repository-wide rules.
2. [`../../COMMON_CONTRACTS.md`](../../COMMON_CONTRACTS.md) — strict data/time/provenance contract.
3. [`SPEC.md`](SPEC.md) — complete stage design: input whitelist (§3), sampling/modes (§4), summaries (§5), catch22 (§6), tsfeatures (§7), deferred Atlas/baselines (§8), schema/provenance (§9) and primary sources (§11). No separate feature-reference document is required.
4. [`VERIFY.md`](VERIFY.md) — this stage's verifiable exit gate.
5. [`../04_business_features/SPEC.md`](../04_business_features/SPEC.md) — upstream business-feature/window definitions.

## What to build

1. Provide the specified basic summaries, classic catch22 baseline (22) and eight applicable tsfeatures extensions. Execute only explicit statistical items/metrics or their selected named metric_set; a complete baseline request includes all corresponding definitions, while custom subsets are labeled. Pin reference versions and validate ports with golden fixtures.
2. Resolve frozen FeatureSet input/window/metric selections to concrete feature definitions.
3. Produce selected statistics, units, valid/total counts, NA reasons and coverage.
4. Defer conditional baselines, anomaly models, dependency graphs and null/control methods.
5. Emit the statistical view of the Feature catalog, fingerprint artifacts and FeatureResult indexes with exact inputs, sampling/windows and availability lineage. No placeholder Atlas/baseline outputs.
6. Support separate trailing/evaluation-stride and retrospective segment-summary modes. Track business lookback L, statistical window W and raw dependency extent; never use future impact H as an input.
7. Limit input/metric/window combinations explicitly and document overlapping-window dependence; do not build event/regime models in this step.

## Input contract

Use the [Step 01 explicit-item configuration and execution ledger](../01_workflow_engine/IMPLEMENT.md). Each statistical item binds one source and its own windows/metrics; expose per-metric canonical definitions, resolved versions/parameters, dependencies and partition scope. Report selected-but-pending, cached, completed, failed/blocked/skipped and invalid-data counts independently; no source×all-statistics Cartesian expansion.

Use the precise inputs declared in `SPEC.md`. Load upstream artifacts via manifest references. When a prerequisite stage is not yet implemented, **test with serialized fixture artifacts that obey the contract**, never with fake production success or untracked in-memory mocks.

## Required outputs

Implement the stage's declared persistable output artifacts with stable IDs, `schema_version`, input hashes, `available_time` semantics where applicable, status and quality flags. Integrate with the common stage interface; artifact file formats can follow `SPEC.md` and shared contracts.

## Codex prompt (paste into Codex)

> Read AGENTS.md, COMMON_CONTRACTS.md, steps/05_statistical_features/SPEC.md, steps/05_statistical_features/IMPLEMENT.md and steps/05_statistical_features/VERIFY.md. Implement **only Step 05: Statistical Features & Quality** and dependencies strictly required to make its documented contract runnable. Create actual code and synthetic fixtures; add unit tests, an integration test and time-leakage or quality tests as applicable. Do not leave algorithmic stubs. Run tests, report exact command and results, update IMPLEMENTATION_STATUS.md with the true status, and stop before implementing the next stage.

## Exit criterion

Stage implementation is not complete until every item in [`VERIFY.md`](VERIFY.md) is checked by real automated tests or a documented reproducible CLI run; no manufactured test claims.
