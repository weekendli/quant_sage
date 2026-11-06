# Codex Implementation Task — Step 04: Business Features & Time Windows

## 当前收敛范围：Step 02–05 数据与特征闭环

本步消费 Step 03 结果和冻结 FeatureSet 中选中的业务组合，生成 Feature(kind=business) 与 FeatureResult。只实现确定业务公式、合法 space/size/回看 L、窗口端点/reset/有效性；不要求自动创造公式或运行全部 432 个组合。

432 是目录合法名称数，不是每次必须计算的数量。未选中的 pending_semantics 不阻塞已选 ready 子集；选中的未明确公式阻断对应任务并报告原因，不改变公式。只有声称“完整 pack 数值交付”时才要求所有相关语义明确。

统一管理字段见 [domain.detail.md](../../domain.detail.md)，当前调度边界见 [WORKFLOW_STAGES.md](../../WORKFLOW_STAGES.md)。旧 Registry 名称表示 Feature/FeatureResult 的分类视图，不要求新增独立领域仓库。Study、FeatureSet、Run、Review 的通用管理由 Step 01 承担，领域步骤只实现本步算子与结果契约。


**Milestone:** M2<br>
**Upstream dependencies:** Step 03<br>
**Scope:** implement only this stage, not downstream algorithms.

## Required reading

1. [`../../AGENTS.md`](../../AGENTS.md) — repository-wide rules.
2. [`../../COMMON_CONTRACTS.md`](../../COMMON_CONTRACTS.md) — strict data/time/provenance contract.
3. [`SPEC.md`](SPEC.md) — full stage design.
4. [`VERIFY.md`](VERIFY.md) — this stage's verifiable exit gate.
5. [`../../references/gov_bond_l2_feature_spec.md`](../../references/gov_bond_l2_feature_spec.md) and [`../03_research_dimensions_and_primitives/SPEC.md`](../03_research_dimensions_and_primitives/SPEC.md) — read-only source formulas and upstream L2 primitive contract.

## What to build

1. Implement registered business formulas with canonical definitions/hash; a typed AST may serve as the internal execution representation. No automatic formula-generation system is required.
2. Provide the required valid rolling mean, interval sum, past endpoint difference, reset checks and window-result composition operators. Entropy, statistical profiling and fitted transforms belong to Step 05; broad automatic grammar generation is optional future work.
3. Check units, time basis, lookback requirements, field availability and costs.
4. Compile to bounded Polars/DuckDB lazy queries; cache subexpressions.
5. Register feature definition and persist features with availability lineage.
6. Implement the fixed L2 pack using the per-family mapping in SPEC. Declare exactly 432 legal definitions for the full pack; distinguish pending definitions, disabled inputs, explicitly selected subsets and successful numerical execution. Do not globally cross family/space/size/horizon or let Atlas silently rewrite fixed definitions.
7. Reuse snapshot primitives and shared session-window/endpoint/reset AST nodes. Preserve mean-of-ratios versus ratio-of-aggregates ordering, snapshot-relative size, valid sample counts and original naming.
8. Keep F07/F08 aggregation explicitly pending. For cumulative/price endpoints use the latest valid same-session observation at or before t-L, with explicit frozen max_endpoint_age and recorded source identities. Complete-pack numerical acceptance requires aggregation semantics resolved; selected ready-subset acceptance does not require unrelated pending definitions.

## Input contract

Consume frozen FeatureSet business selections (the existing `business_features.items` form is a compatibility adapter) through the [Step 01 explicit selection and item ledger](../01_workflow_engine/IMPLEMENT.md). Validate per-family tuple legality, resolve aliases to canonical formulas, declare dependency closure and partition coverage, and report each requested definition separately. Do not implement a global family×space×size×lookback product or treat stage success as every feature having executed.

Use the precise inputs declared in `SPEC.md`. Load upstream artifacts via manifest references. When a prerequisite stage is not yet implemented, **test with serialized fixture artifacts that obey the contract**, never with fake production success or untracked in-memory mocks.

## Required outputs

Implement the stage's declared persistable output artifacts with stable IDs, `schema_version`, input hashes, `available_time` semantics where applicable, status and quality flags. Integrate with the common stage interface; artifact file formats can follow `SPEC.md` and shared contracts.

## Codex prompt (paste into Codex)

> Read AGENTS.md, COMMON_CONTRACTS.md, steps/04_business_features/SPEC.md, steps/04_business_features/IMPLEMENT.md and steps/04_business_features/VERIFY.md. Implement **only Step 04: Business Features & Time Windows** and dependencies strictly required to make its documented contract runnable. Create actual code and synthetic fixtures; add unit tests, an integration test and time-leakage or quality tests as applicable. Do not leave algorithmic stubs. Run tests, report exact command and results, update IMPLEMENTATION_STATUS.md with the true status, and stop before implementing the next stage.

## Exit criterion

Stage implementation is not complete until every item in [`VERIFY.md`](VERIFY.md) is checked by real automated tests or a documented reproducible CLI run; no manufactured test claims.
