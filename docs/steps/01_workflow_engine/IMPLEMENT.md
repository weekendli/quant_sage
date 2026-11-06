# Codex Implementation Task — Step 01: Workflow Engine

**Milestone:** M1  
**Upstream dependencies:** no upstream implementation (only shared contracts)  
**Scope:** implement only this stage, not downstream algorithms.

**Current increment:** a generic catalog-backed batch path is implemented and verified with arithmetic fixtures; the expanded contract below is only partially delivered. See [RESEARCH_BATCH_RESULTS.md](RESEARCH_BATCH_RESULTS.md) for supported CLI/provider behavior and explicit gaps. Domain boundaries and relationships are specified in [DOMAIN_MODEL.md](../../DOMAIN_MODEL.md).

## Required reading

1. [`../../AGENTS.md`](../../AGENTS.md) — repository-wide rules.
2. [`../../COMMON_CONTRACTS.md`](../../COMMON_CONTRACTS.md) — strict data/time/provenance contract.
3. [`SPEC.md`](SPEC.md) — full stage design.
4. [`VERIFY.md`](VERIFY.md) — this stage's verifiable exit gate.
5. [`../../DOMAIN_MODEL.md`](../../DOMAIN_MODEL.md) and [`../../../CONTEXT.md`](../../../CONTEXT.md) — target domain vocabulary, entity relationships and explicit gaps in the current generic batch prototype.

## What to build

1. Define Stage, RunContext, StageResult, ArtifactRef and status transitions.
2. Implement DAG validation, deterministic topological scheduling and error propagation.
3. Persist manifest before exposing an artifact, with content hash and atomic publication.
4. Implement cache keys, resume, partial partition replay and failure classification.
5. Provide validate, plan, run, status, artifacts, explain CLI commands.
6. Implement the explicit feature-selection and item-level execution contract below: normalize requested business/statistical combinations, resolve only their dependency closure, freeze a plan, and persist every selected output's execution/coverage evidence. This extension is a new requirement; existing stage-level engine verification does not prove it implemented.

## Explicit combinations: configuration contract to implement

The engine owns planning, identities, scheduling and evidence. Step 03/04/05 plugins own domain catalogs, formula legality, metric/version resolution and computation. Validate this interface using synthetic catalog/worker plugins; do not implement financial formulas in the engine.

```yaml
feature_selection:
  schema_version: explicit_items_v1
  mode: explicit
  unknown_fields: error
  catalog_inventory: selected_and_registered

business_features:
  pack: gov_bond_l2_market_state
  items:
    - id: obi_touch_3s
      family: obi
      space: d0
      lookback: 3s
    - id: coverage_near_q50_1m
      family: min_coverage
      space: d1
      size: q50
      lookback: 1m
    - id: size_spread_q100_5m
      family: size_spread
      size: q100
      lookback: 5m

statistical_features:
  feature_set_version: statistical_profile_v2
  items:
    - id: obi_structure
      source: {kind: business_feature, ref: obi_touch_3s}
      mode: trailing
      profile_window: 30m
      sampling_interval: 1s
      evaluation_stride: 60s
      max_feature_age: 5s
      min_samples: 256
      metrics: [c22.CO_f1ecac, tsf.x_acf1]
    - id: coverage_structure
      source: {kind: business_feature, ref: coverage_near_q50_1m}
      mode: trailing
      profile_window: 15m
      sampling_interval: 1s
      evaluation_stride: 30s
      max_feature_age: 5s
      min_samples: 256
      metrics: [tsf.stability_w60, tsf.lumpiness_w60]
```

This selects exactly **3 business definitions and 4 statistical definitions**, plus required internal dependencies. Do not compute the full 432 business pack or apply every statistical metric to each source. The sample age/window policies are illustrative, not validated market settings.

### Selection and validation rules

1. Each item is an explicit tuple; reject list-valued family/space/size/lookback/window coordinates rather than implicitly multiplying or zipping arrays. Enumerate desired tuples as separate items. A statistical `metrics` list intentionally requests those metrics for that one source/window only.
2. For statistics, require exactly one of `metrics` or a named `metric_set`. Support `metric_set: catch22_classic_22` and `metric_set: catch22_plus_selected_tsfeatures_30`; expand their versioned member IDs into individual planned outputs. An explicit subset is labeled `custom_subset`, never a complete catch22 baseline. Parameters such as lags/width and backend versions resolve via the metric catalog and enter the semantic hash.
3. In explicit mode, omitted/empty items mean no requested outputs of that kind, never all. A referenced business output must be explicitly declared or resolve to a registered, versioned artifact covering the requested scope; do not infer wildcard features from an unresolved alias. Primitive sources use `{kind: primitive, ref: <registered primitive ID>}` and resolve only declared parameters/dependencies.
4. Reject duplicate aliases, unknown refs/metrics/fields, cycles and illegal family dimensions (e.g. size on OBI, space on size spread). Registered definitions with unresolved formula semantics may appear as `BLOCKED` with a reason in the plan; strict execution rejects the affected request before numerical work. Never silently choose a missing formula.
5. Keep dataset request scope (instrument/date/time bounds and split permissions), business lookback L, statistical window W, evaluation stride, sampling interval and future outcome H separate. Resolve L+W and endpoint-age history requirements per item without silently crossing sessions or forbidden split boundaries. Emit unavailable coverage reasons rather than extending authorized scope.
6. An explicit full-pack preset, if provided, must be separately named and expanded into a frozen item list; no default full-pack fallback. Record exclusions/reasons. Budget checks run after definition expansion and dependency deduplication, before numerical execution; refusal cannot silently drop outputs. Report requested-output and dependency-computation budgets separately.
7. Normalize aliases to canonical definitions. Plan only the union of selected dependencies; shared depth/VWAP/window computations execute once per matching scope. Unselected registry entries are inventory rows, not numerical tasks; private subexpressions need not become public artifacts.
8. In explicit mode, legacy global `lookback_windows/source_features/profile_window` selectors must not multiply or override item coordinates. Reject conflicting legacy selectors; generic research window suggestions are metadata for future experiments, not implicit requests. Parser acceptance of arbitrary YAML keys is not validation or implementation of this schema.

### Planning interface and persisted plan

Add a typed planning interface alongside Stage execution (exact class names may follow existing architecture):

- `WorkDefinition`: canonical ID, kind (`primitive/business/statistical`), formula or metric/version/parameters, typed input refs, time/validity policy and dependency IDs.
- `WorkRequest`: output alias, definition ref, requested instrument/time/split scope, selection reason and required history.
- `WorkPartition`: normalized definition + requested output scope/partition + exact input artifact refs; declares output validation and expected coverage.
- `WorkPlan`: schema/plan hash, run/snapshot/split refs, selected output expansion, dependency graph, registry inventory and deterministic execution order.

Persist the immutable WorkPlan and all selected partition rows as `PENDING` before dispatch. A plan rejected during validation must retain a diagnostic report; a plan that validates does not imply computation succeeded. Planned feature definitions can be emitted as long-format rows within a shared Parquet artifact; map each work item to its row selector/coverage and hash, not to a mandatory separate file.

## Item-level execution and coverage ledger

Stage-level status alone cannot answer which features ran. Extend the ledger (or equivalent serialized tables) with item requests, partition states, attempts and verified output refs; use the existing stage/partition machinery where possible.

| Dimension | Required representation |
|---|---|
| Selection | `REQUESTED`, `DEPENDENCY_ONLY`, `NOT_SELECTED`; unselected entries have no execution state and do not count as pending work |
| Partition execution | `PENDING`, `RUNNING`, `SUCCEEDED`, `CACHED`, `FAILED`, `BLOCKED`, `SKIPPED` |
| Result validity | row/sample counts, valid/invalid counts, invalid-reason counts, requested/processed/missing intervals and session coverage; independent of execution state |
| Definition-level summary | `NO_WORK`, `NOT_STARTED`, `IN_PROGRESS`, `COMPLETE`, `PARTIAL`, `FAILED`, `BLOCKED`, `SKIPPED`; derive deterministically from the frozen partition set |

Persist at least `run_id,plan_id,stage_run_id,request_id,canonical_definition_id,alias,kind,selection_state,partition_id,instrument_id,requested_time_range,split_id,state,reason_code,attempt_id,started_at,finished_at,input_hashes,semantic_hash,artifact_refs,coverage,n_total,n_valid,invalid_reason_counts,cache_source_run_id`.

- Only after output schema/hash/coverage validation and atomic manifest publication may a partition become `SUCCEEDED`; an all-NA but correctly computed result may succeed with `n_valid=0` and explicit quality reasons. A missing artifact is not equivalent to a valid empty result.
- `CACHED` means a validated prior result is reused, not newly executed. A previously successful feature on one day does not establish completion on another day, input snapshot or definition version.
- Empty selections yield `NO_WORK`; selected partitions all pending yield `NOT_STARTED`; any running yield `IN_PROGRESS`; all requested partitions succeeded/cached with complete requested processing coverage yield `COMPLETE`. A mixture of complete and unfinished/failed/skipped partitions is `PARTIAL`; if none completed, explicit failed, blocked, skipped states take precedence over pending in that order. Preserve underlying counts and reasons rather than presenting only the summary.
- `SKIPPED`/`BLOCKED` require an explicit reason (operator selection, unavailable prerequisite, pending semantics, budget, or coverage policy). Cancellation/interruption is recorded in attempt history; interrupted RUNNING work returns to recoverable pending on resume, not success. Selection filtering itself is `NOT_SELECTED`, not failed/skipped execution.
- Display “executed this run”, “reused”, “not yet executed”, “failed/blocked/skipped” separately. Do not claim every selected feature executed just because its containing stage is green. Stage success requires all required items validated; explicit partial-run policies must be visible in the stage report and cannot masquerade as complete pack delivery.

## Fine-grained cache, resume and status

1. Item cache keys include canonical semantics/versions, time/validity policies, normalized output scope, exact input snapshot/artifact hashes, split/fit scope and code revision. Aliases/list order are not mathematical inputs; adding one independent request should not invalidate already reusable work. A changed dependency invalidates only affected items and descendants. Backend limitation to coarser artifact chunks must be reported, not hidden as precise recomputation.
2. Resume reconciles ledger rows with committed manifests. Validate cached partitions and schedule only missing/invalid/failed recoverable work; never duplicate a published result, overwrite history or use a stale artifact outside its coverage. Expand the requested date range into a new plan, reusing exactly matching prior partitions while leaving old runs immutable.
3. Expose the plan and execution ledger through `plan/status/artifacts/explain` (add item/scope/state filters and machine-readable output). A selected feature must be traceable from config alias to definition, dependency closure, attempts and output rows. Inventory reports show the union of configured and registered finite catalog entries, with `NOT_SELECTED` entries labeled; do not generate an unbounded universe of hypothetical combinations.
4. Required status columns: alias/definition/kind, selection, requested scope, completed/total partitions, processed coverage, execution summary, valid-row counts, cache evidence and remaining-work reasons. Report counts for requested business/statistical outputs, dependency-only nodes and unselected inventory separately.
5. Implement and test this generic planning/ledger capability using synthetic item workers. Bind domain validation/computation in Steps 03–05 when available; historical Step 01/02 success remains valid evidence for their original scope and is not evidence for this new extension.

## Research batches: select data, compute combinations, validate, expand

The primary interactive research unit is a **batch experiment**, not a mandatory full end-to-end run. A user selects a bounded data scope, a few feature combinations and a validation protocol, inspects evidence, then creates a new batch with a longer time range and/or more feature definitions. A batch may cross multiple steps, but must not execute every stage or every registered feature by default.

The following specifies the target batch configuration. The generic runtime subset now requires a registered `workflow.item_provider`, verified source refs and its supported protocol; arbitrary artifact bindings and the complete domain model remain pending. It accompanies the explicit `business_features.items` / `statistical_features.items` definitions above; selected IDs resolve to those definitions or, when that binding adapter is implemented, verified registered artifacts.

```yaml
research_batch:
  schema_version: research_batch_v1
  id: depth_pressure_pilot
  parent_batch_ref: null
  data_scope:
    snapshot_ref: <frozen_snapshot_id>
    split_ref: <authorized_split_id>
    instruments: [TF2603]
    output_interval:
      start: '2026-01-05T09:30:00+08:00'
      end: '2026-01-10T00:00:00+08:00'
  targets:
    business_features: [obi_touch_3s, coverage_near_q50_1m]
    statistical_features: [obi_structure]
  execution:
    allowed_stages: [primitives, features, statistical_atlas]
    missing_dependencies: compute_within_allowed_stages
    reuse_verified_artifacts: true
  validation:
    mode: quality
    protocol_ref: <versioned_quality_protocol>
```

Dates/product are illustrative scope only; the planner checks actual inventory, sessions and split permissions. Time bounds are half-open. Validated source refs and feature definitions are required; placeholder text is not executable configuration. For market-pattern experiments, select `validation.mode: exploratory` or `confirmatory` with an explicit experiment/protocol ref and authorize the required experiment/validation stages; do not reinterpret the example's quality check as evidence of an empirical effect.

### Batch execution contract

1. Freeze batch data scope, feature/statistical tuples, validation protocol and exact input/definition refs into a BatchManifest bound to an immutable WorkPlan/run. `targets` are the requested outputs; other definitions in a reusable config/catalog are available inventory, not automatically requested work. A selected statistical source is dependency-only unless independently targeted. Without research_batch, explicit-items mode retains its existing all-declared-items request semantics.
2. Resolve only dependencies for these targets and the requested validation. The engine treats validation protocols as typed work with input refs, authorized stages, versions and output validators. Domain validation workers remain owned by their respective steps; do not implement effect estimation or market semantics in the generic engine.
3. `allowed_stages` is a hard boundary. Resolve prerequisites from verified artifacts first; compute missing dependencies only inside authorized stages. Support `missing_dependencies: block` for a standalone later-step batch. Missing out-of-bound dependencies yield BLOCKED with a precise missing scope, never an automatic replay from onboarding. Separate `read_interval` (required lookback/profile history and, if explicitly requested, realized outcome support) from `output_interval`; never silently widen the evaluated sample or cross session/split permissions.
4. Permit independent primitive-only, business-feature-only, statistical-only and feature-plus-validation batches. Event/regime construction is optional unless the selected experiment actually references those artifacts. A pure feature experiment must not acquire an event/regime dependency merely because the full workflow diagram includes those stages; plugin work-level dependencies must declare this distinction.
5. Validation levels are distinct: `quality` checks numerical correctness, coverage, validity, lineage and time contracts; `exploratory` evaluates an explicitly defined pattern/effect with controls, sample units and uncertainty, recording every search attempt; `confirmatory` tests a frozen candidate using untouched authorized evaluation data. The last two additionally declare outcome/impact H, decision-time alignment, censoring, fit/evaluation scopes and protocol version. Neither a successful compute nor quality PASS proves a market pattern.
6. Each batch emits a report with requested/resolved/processed data scope, selected feature tuples, execution/cache/remaining-work states, validity counts, validation status/protocol refs, effects/uncertainty where applicable, and limitations. Validation work has its own pending/running/succeeded/failed/blocked status plus domain verdict (quality PASS/FAIL or supported/inconclusive/falsified as applicable); infrastructure success cannot substitute for a scientific verdict.
7. Expansion creates a new batch/run with `parent_batch_ref`, never mutates the pilot. Record `added_time_scope`, `added/removed/changed_definition_ids`, changed protocols and newly accessed evaluation data. Support time-only expansion, feature-only expansion, both, or a different authorized data slice. Report the parent/current common scope separately from added scope so changing sample composition cannot masquerade as a stronger effect.
8. Reuse matching verified per-item partitions across batches, preserving old attempts/artifacts and reporting cache source. Added dates/features compute only their missing dependency closure; time-history overlap and shared primitives are reusable when identities match. An expanded aggregate validation report generally must be recomputed for its new sample and search history even if underlying features are cached. Changed fit windows invalidate fitted baselines/models and downstream work that depends on them; do not promise every old statistical result remains valid after refitting.
9. Maintain a persistent research inventory spanning batches: canonical definition, processed/available data coverage, protocol/evaluation coverage, execution attempts, cache provenance and uncomputed gaps. Reused aliases must resolve to versions, not silently bind to a newer similarly named feature. Queries distinguish current-batch completion from cumulative historical coverage; there is no global unqualified 'feature finished' flag.
10. Expanded exploration remains exploration. Record inspected periods, tried feature combinations and validation attempts; a period already examined cannot be relabeled untouched holdout when expanding. Confirmatory candidate changes require new versions and genuinely authorized independent data under the split policy. A batch can stop and return its report at any boundary without running discovery/planner stages.

### Required iterative demonstration

- Batch A: one bounded data interval, two business tuples, one selected statistical item, then quality or an explicit exploratory validation protocol.
- Batch B: same definitions, additional dates; reuse verified A inputs, compute missing new scope, and rerun the expanded validation summary.
- Batch C: same data scope as B, one extra feature/statistical combination; reuse existing features and primitives, compute new dependencies, and record the added experiment/search count.
- At each boundary, inspect evidence before creating the next batch. The engine must support pausing after a batch without scheduling later research stages, and comparing batches without overwriting prior results.

## Stage input contract

Use the precise inputs declared in `SPEC.md`. Load upstream artifacts via manifest references. When a prerequisite stage is not yet implemented, **test with serialized fixture artifacts that obey the contract**, never with fake production success or untracked in-memory mocks.

## Required outputs

Implement the stage's declared persistable output artifacts with stable IDs, `schema_version`, input hashes, `available_time` semantics where applicable, status and quality flags. Integrate with the common stage interface; artifact file formats can follow `SPEC.md` and shared contracts.

For the explicit-items extension, also persist the immutable WorkPlan, selection/inventory manifest, partition/attempt ledger and item-level completion/coverage report. Full extension acceptance requires the additional cases in VERIFY.md; documentation changes alone do not mark this capability implemented.

## Codex prompt (paste into Codex)

> Read AGENTS.md, COMMON_CONTRACTS.md, steps/01_workflow_engine/SPEC.md, steps/01_workflow_engine/IMPLEMENT.md and steps/01_workflow_engine/VERIFY.md. Implement **only Step 01: Workflow Engine** and dependencies strictly required to make its documented contract runnable. Create actual code and synthetic fixtures; add unit tests, an integration test and time-leakage or quality tests as applicable. Do not leave algorithmic stubs. Run tests, report exact command and results, update IMPLEMENTATION_STATUS.md with the true status, and stop before implementing the next stage.

## Exit criterion

Stage implementation is not complete until every item in [`VERIFY.md`](VERIFY.md) is checked by real automated tests or a documented reproducible CLI run; no manufactured test claims.


## Current scope: Study / Feature / FeatureSet / Run tools

The [simplified domain model](../../DOMAIN_MODEL.md) is the current product boundary. Implement four management entry points (Study, Feature, FeatureSet, Run) with lightweight source references, FeatureResult indexes and append-only Reviews. A research batch is a frozen Run configuration, not a required independent repository. Preserve existing `research_batch_v1` and provider compatibility; no schema migration is implied by this documentation update.

Next Step 01 increment should provide:

1. Study goal, source references, revisioned candidate backlog, defaults and budgets. FeatureSet owns versioned explicit combinations, frozen metric-set expansion and resolved Feature references. Runs bind one frozen set and optional selected item IDs; an empty selection means no work. Unregistered theoretical combinations are not pending work.
2. Run configuration/plan, exact FeatureSet/feature/source versions, allowed steps, dependency closure, per-item attempts and result references. Same-config resume preserves history; changed scope/features creates a new run. Idempotent submission keys prevent duplicate runs from agent retries.
3. Study coverage across runs for explicit feature/source/scope filters. Distinguish not selected, never executed, failed, processed coverage, missing/invalid artifacts, available results and all-NA outputs. Derive remaining work from this query, not from a global feature flag.
4. Reviews with actor, evidence refs, observations, keep/investigate/park and next-run suggestions; compare common/new ranges and definitions. Use the same interfaces for humans and RSI autoresearch. Record study revisions to prevent concurrent overwrites.
5. Result consumer contract: source/definition/code identity, time/availability, sessions, sampling/windows, quality, coverage and lineage. Data arrays remain in artifacts. Events/regimes/phenomena consume explicit result refs later.

Reuse the current registry/artifact store and internal work/attempt mechanism. Validate this management loop with the synthetic provider before real Step 03–05 algorithms. Detailed scientific experiment entities and downstream discovery algorithms are deferred; they are not prerequisites for this tool increment. Earlier batch terminology in this guide describes configuration/execution semantics, not a separate mandatory domain layer. No new runtime capability is delivered by this design change.

Field contracts for all seven models are in [domain.detail.md](../../domain.detail.md). They are target interfaces; the current research_batch_v1 adapter remains unchanged.

## Implementation design and database decision

Follow [IMPLEMENTATION_DESIGN.md](../../IMPLEMENTATION_DESIGN.md) for the incremental implementation order, DuckDB storage/SQLite import, single-owner access, atomic submission, explicit history-support dependencies, result publication recovery and service interfaces. New metadata and runtime writes belong to DuckDB; existing SQLite files and frozen artifact identities are preserved during verified import. This replaces any earlier SQLite-as-target recommendation, but does not claim the migration has run.
