# Codex Implementation Task — Step 03: Research Dimensions & Primitives

## 当前收敛范围：Step 02–05 数据与特征闭环

本步只按 Run 已解析的 FeatureSet 依赖闭包计算基础序列。输入是 Step 02 的冻结数据源 refs、所选 primitive 定义及输出/历史范围；输出为 Feature(kind=primitive) 对应的 FeatureResult、有效性和 lineage。维度目录是数据可用性说明，不要求自动研究规划、全空间扫描或把每个内部节点都持久化。

L2 公式目录保留完整合法域，实际只算选中项及必要依赖。当前重点为已选 L2 组合需要的快照量、累计量/reset 与时间信息；跨合约因子、activity bars 和自动维度发现列为后续扩展，不作为此最小闭环的交付门槛。

统一管理字段见 [domain.detail.md](../../domain.detail.md)，当前调度边界见 [WORKFLOW_STAGES.md](../../WORKFLOW_STAGES.md)。旧 Registry 名称表示 Feature/FeatureResult 的分类视图，不要求新增独立领域仓库。Study、FeatureSet、Run、Review 的通用管理由 Step 01 承担，领域步骤只实现本步算子与结果契约。


**Milestone:** M2  
**Upstream dependencies:** Step 02  
**Scope:** implement only this stage, not downstream algorithms.

## Required reading

1. [`../../AGENTS.md`](../../AGENTS.md) — repository-wide rules.
2. [`../../COMMON_CONTRACTS.md`](../../COMMON_CONTRACTS.md) — strict data/time/provenance contract.
3. [`SPEC.md`](SPEC.md) — full stage design.
4. [`VERIFY.md`](VERIFY.md) — this stage's verifiable exit gate.
5. [`../../references/gov_bond_l2_feature_spec.md`](../../references/gov_bond_l2_feature_spec.md) — read-only source formulas; use the L2 decomposition in this step's SPEC.

## What to build

1. Validate the selected definitions against DatasetPassport capabilities; record unsupported reasons. Automatic research-dimension discovery is deferred.
2. Create typed primitive definitions, lineage and availability metadata.
3. Compute mid, spread_ticks, OBI, quote interarrival, volume_increment where supported.
4. Preserve observation-time/quote-index semantics for selected L2 inputs. Activity bars and cross-contract analysis are deferred extensions.
5. Persist primitive partitions and queryable registry.
6. Implement the L2 primitive catalog in SPEC: historical tick mapping, tick-distance spaces, relative target sizes, snapshot OBI/WOBI/coverage/capacity and shared five-level VWAP/fill/walk calculations. Register public primitive formula references, parameter domains and per-primitive validity masks; keep weights, level consumption and other private dependencies as shared internal nodes unless independent reuse justifies publication.
7. Preserve cumulative buy/sell fields and intra-session reset markers, and pass through native interval TRADEVOLUME unchanged. Expose serialized dependencies for Step 04; do not compute its horizon grid or depend on the Atlas/feature output.

## Input contract

Integrate with the [Step 01 explicit-item planning/ledger contract](../01_workflow_engine/IMPLEMENT.md): expose public primitive definitions and private dependencies for requested business/statistical outputs. Compute only the selected dependency closure, deduplicate matching shared nodes, and report per-scope completion/validity; do not persist an entire primitive parameter grid by default. The generic engine prototype is partial; Study/FeatureSet integration and this real primitive stage remain pending.

Use the precise inputs declared in `SPEC.md`. Load upstream artifacts via manifest references. When a prerequisite stage is not yet implemented, **test with serialized fixture artifacts that obey the contract**, never with fake production success or untracked in-memory mocks.

## Required outputs

Implement the stage's declared persistable output artifacts with stable IDs, `schema_version`, input hashes, `available_time` semantics where applicable, status and quality flags. Integrate with the common stage interface; artifact file formats can follow `SPEC.md` and shared contracts.

## Codex prompt (paste into Codex)

> Read AGENTS.md, COMMON_CONTRACTS.md, steps/03_research_dimensions_and_primitives/SPEC.md, steps/03_research_dimensions_and_primitives/IMPLEMENT.md and steps/03_research_dimensions_and_primitives/VERIFY.md. Implement **only Step 03: Research Dimensions & Primitives** and dependencies strictly required to make its documented contract runnable. Create actual code and synthetic fixtures; add unit tests, an integration test and time-leakage or quality tests as applicable. Do not leave algorithmic stubs. Run tests, report exact command and results, update IMPLEMENTATION_STATUS.md with the true status, and stop before implementing the next stage.

## Exit criterion

Stage implementation is not complete until every item in [`VERIFY.md`](VERIFY.md) is checked by real automated tests or a documented reproducible CLI run; no manufactured test claims.
