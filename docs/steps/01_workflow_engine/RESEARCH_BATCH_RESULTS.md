# Research batch engine increment — 2026-11-06

Status: **PARTIAL implementation of the expanded Step 01 contract**. The generic item/batch execution path is runnable and tested; real Step 03–05 computations and the complete [target domain model](../../DOMAIN_MODEL.md) are not implemented.

## Implemented

- `core/research.py` resolves explicit business/statistical items through a catalog provider, expands only requested metrics, deduplicates dependencies and rejects invalid/ambiguous selection.
- Each batch binds verified snapshot/split artifacts, an authorized interval and instruments, explicit targets, allowed stages and a validation protocol. A provider must authorize domain read/session/split rules; the engine does not infer them.
- Plans are stored in the run graph before execution. Items use existing stage/partition transactions, cache keys, content/schema verification and resume. Source refs participate in cache identity and lineage.
- Item results account for processed scope and total/valid rows. State events retain attempt changes. Status separates requested/dependency/unselected inventory, per-partition execution, cached evidence and validation verdicts.
- Parent batches report new/common scope and new/changed definitions. Expanding time reuses matching old partitions; adding definitions reuses shared work. Aggregate quality validation is recalculated when its targets/scope change.
- Existing `validate/plan/run/status/artifacts/explain` support registered batch providers. `status` adds `--item`, `--item-kind`, `--item-state` filters for research runs. Legacy stage-plugin workflows retain their existing behavior.

## Verification

```bash
python -m pytest -q
python -m ruff check quantsage_discovery scripts tests
python -m tests.research_demo
```

Result: **123 tests passed**, including **21 batch tests**; Ruff passed. Batch tests cover exact arithmetic output, explicit selection, date/feature expansion, cache-only execution boundaries, empty selection, invalid dimensions/metrics/aliases, scope/budget/cycle rejection, pending definitions, failure/resume history, empty valid results, corrupt/missing artifacts and CLI plan/run/status/resume.

The final demonstration artifacts/configs/plans/reports are in `/tmp/quantsage-research-demo-ljya71jl`:

| Synthetic batch | New work units | Reused units | Change |
|---|---:|---:|---|
| A | 5 | 0 | One date partition, one business output and two arithmetic statistics, plus shared input and quality check |
| B | 5 | 4 | Add one date partition; rerun aggregate quality summary |
| C | 3 | 8 | Add one business definition across both dates; rerun quality summary |

All three completed. These are arithmetic fixtures, **not L2 features, catch22 results, trading effects or independent scientific validation**. The demonstration seeds its own source artifacts and never reads or changes production market data.

## Use from the repository root

```bash
python -m tests.research_demo
```

It prints a new temporary output directory containing `A.yaml`, `B.yaml`, `C.yaml` and each batch's plan/run JSON. To inspect the recorded demonstration above:

```bash
python -m quantsage_discovery.cli status \
  --config /tmp/quantsage-research-demo-ljya71jl/C.yaml \
  --run-id C --item long --item-kind business
```

Use a new empty output directory for each demo; do not reuse immutable run IDs with changed configuration.

## Provider seam

`workflow.item_provider` imports a zero-argument catalog factory. Its methods are:

- `resolve(kind, parameters) -> WorkDefinition` and `get(key) -> WorkDefinition`: resolve canonical, versioned semantics and dependencies; reject domain-invalid combinations.
- `metric_sets`: named lists of versioned metric IDs.
- `authorize(scope, validation, source_refs) -> dict`: verify source inventory, sessions, exact historical/outcome support and split/protocol permissions; return explicit authorization evidence. Statistical implementations must include all fitting/algorithm parameters in definitions.
- `register_validators(store)`: idempotently register schema/content validators for source and output types.
- `execute(definition, ctx, inputs) -> StageResult`: compute only the requested scope, publish transaction-owned artifacts, and report `n_total`, `n_valid`, `processed_scope=complete`; validation workers additionally emit a separate domain verdict.

Worker context exposes its resolved definition/scope, not arbitrary batch aliases; algorithmic dependencies belong in the definition and exact input refs. The example provider is `tests.research_provider.ArithmeticCatalog` and deliberately rejects confirmatory/exploratory protocols it cannot implement.

## Remaining scope

- True L2/catch22/domain protocols and their numerical/session/availability verification remain Steps 03–05 and later validation work.
- Work units currently use fixed UTC day slices plus exact partial boundary times; these are storage/execution slices, not inferred exchange sessions. Changing a partial boundary prevents exact-scope cache reuse for that slice.
- Each item currently publishes its own artifact; sharing a physical table with row-selector bindings and arbitrary interval stitching is not implemented.
- The current simplified target still lacks Study/backlog management, cross-run FeatureResult/coverage queries and Reviews. A separate Batch repository is no longer required. Full holdout governance and experiment/effect reports are deferred to later scientific validation work.
- Frozen plans are durable in registry run graphs. Invalid configurations return CLI diagnostics; persisted rejected-plan reports are not yet implemented. Catalog requests currently require a registered business source alias or primitive definition; arbitrary imported feature-definition bindings need a further adapter.
- These limitations mean the new extension's entire acceptance checklist is not marked complete. The previous Step 01/02 evidence and frozen production outputs remain separate.
