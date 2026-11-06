# Acceptance / Verification — Step 01: Workflow Engine

**Important:** These are **tests to create and run**, not claims that tests or code already exist.

## Minimal test fixture

A synthetic no-op DAG producing small test artifacts; no market data needed.

## Required acceptance checks

- [ ] Cycle / missing dependency triggers an explicit validation error.
- [ ] Same snapshot, config, code and inputs produce a cache hit with no duplicate outputs.
- [ ] Changed stage config invalidates the affected stage and descendants only.
- [ ] Killed partition followed by resume creates one committed artifact per partition.
- [ ] Failed artifact validation never publishes a SUCCEEDED manifest.

## Explicit selection / item ledger extension — pending acceptance

These are additional requirements beyond previously verified Step 01 behavior; test with synthetic catalogs/workers before connecting future domain stages.

- [ ] The IMPLEMENT.md YAML example resolves to exactly 3 requested business outputs and 4 requested statistical outputs, with no 432-pack or source×metric×window cross product.
- [ ] Omitted/empty selections produce NO_WORK, never all; primitive/statistical source refs, duplicate aliases, illegal dimensions, unknown fields/metrics and dependency cycles receive deterministic validation diagnostics.
- [ ] A metric_set expands only for its selected source/scope into versioned members; explicit metrics produce a labeled subset; ambiguous metrics+metric_set or list-valued tuple coordinates are rejected.
- [ ] Dependencies are resolved before execution; shared computations run once, dependency-only nodes and unselected registered inventory are distinguishable, and no unselected full-grid outputs execute.
- [ ] A persisted plan shows PENDING before dispatch; a rejected plan retains diagnostics. Pending formula semantics cannot silently receive numerical defaults.
- [ ] Kill a selected item's partition before publication, resume, and verify exactly one committed result plus attempt history; stale RUNNING state is reconciled rather than reported complete.
- [ ] For a feature requested across two days, one successful day and one failed/pending day yields PARTIAL with exact remaining scope. A prior result outside current snapshot/scope never counts as completion.
- [ ] SUCCEEDED/CACHED require validated artifact hashes, row selectors and processed scope; all-NA output can succeed with n_valid=0, whereas missing/invalid artifacts cannot.
- [ ] Add an independent item, reorder aliases, or extend dates in a new plan: matching item partitions reuse valid cache without rewriting earlier plans; semantic/input/split/code changes invalidate the affected dependency closure.
- [ ] Multiple selected metrics may share a Parquet artifact while each item retains independent traceability and coverage; the engine does not require a file per feature.
- [ ] NOT_SELECTED, PENDING, CACHED, FAILED, BLOCKED and SKIPPED are separately queryable; summary precedence and partial-run policy match IMPLEMENT.md, and stage success alone never marks every feature complete.
- [ ] Status/explain machine-readable reports reconcile requested outputs, actual attempts, reused items, dependency nodes, validity counts and missing scope; finite catalog inventory is labeled explicitly.
- [ ] Generic extension tests require no L2/catch22 implementation; when Steps 03–05 are implemented, add serialized-artifact integration tests before claiming end-to-end domain support.

## Iterative research batch extension — pending acceptance

- [ ] A bounded data scope + two selected business tuples + one statistical item + a declared validation protocol runs as one batch; unselected config/catalog entries and unrelated downstream stages do not execute.
- [ ] Targets override inventory selection only inside research_batch mode, and statistical source dependencies are resolved without implicitly selecting all items.
- [ ] Statistical-only and feature-only batches bind verified upstream artifacts across runs; missing unauthorized-stage dependencies are BLOCKED without replaying onboarding or unrelated stages.
- [ ] Read support for L/W/endpoints/outcomes is tracked separately from evaluated output scope, with no silent session/split/holdout violation or widened evaluation interval.
- [ ] Batch B expands only dates from A, reuses matching item partitions, computes missing scope and recomputes the expanded validation summary where required; parent results stay immutable.
- [ ] Batch C adds only one feature combination to B, reuses existing data/dependencies, and increments experiment/search records without re-executing all previously requested features.
- [ ] Combined time/feature expansion, changed protocols and fitted-model refits record their deltas and invalidate exactly the affected evidence under declared cache granularity.
- [ ] Quality validation and exploratory/confirmatory inference have separate protocols/verdicts; a green compute/quality check never becomes scientific support. A feature experiment has no event/regime dependency unless its declared inputs require one.
- [ ] Batch reports compare common versus added scope and show current-batch versus cumulative coverage, execution/cache gaps, validity, validation verdicts and limitations.
- [ ] Repeated inspection/selection is recorded; previously inspected exploration data cannot be reclassified as untouched confirmatory holdout, and the pipeline can stop after every batch.

## Required evidence in Codex completion report

- [ ] Exact command(s) actually run (including `pytest` selectors and CLI smoke test where applicable).
- [ ] Tests have passing assertions on actual serialized artifacts; no no-op implementations.
- [ ] Schema and provenance IDs verified, with sample artifact paths or manifest examples.
- [ ] Deterministic behavior under a pinned seed/config and recorded upstream snapshot.
- [ ] Error/empty-input paths explicitly tested and reported.
- [ ] Performance/resource characteristics or limits documented (streaming for data-heavy stages).
- [ ] `IMPLEMENTATION_STATUS.md` updated only after confirmation.

## Handoff / done definition

A downstream step may depend on this module **only after** its public artifact contract is stable and the above tests pass. Missing prerequisites may be represented by validated fixture artifacts while developing this module, but final integration must use real upstream outputs.
