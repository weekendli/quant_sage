# Codex Implementation Task — Step 03: Research Dimensions & Primitives

**Milestone:** M2  
**Upstream dependencies:** Step 02  
**Scope:** implement only this stage, not downstream algorithms.

## Required reading

1. [`../../AGENTS.md`](../../AGENTS.md) — repository-wide rules.
2. [`../../COMMON_CONTRACTS.md`](../../COMMON_CONTRACTS.md) — strict data/time/provenance contract.
3. [`SPEC.md`](SPEC.md) — full stage design.
4. [`VERIFY.md`](VERIFY.md) — this stage's verifiable exit gate.

## What to build

1. Infer enabled/disabled research dimensions from the DatasetPassport.
2. Create typed primitive definitions, lineage and availability metadata.
3. Compute mid, spread_ticks, OBI, quote interarrival, volume_increment where supported.
4. Support clock-time, quote-event-time and activity-time with named units.
5. Persist primitive partitions and queryable registry.

## Input contract

Use the precise inputs declared in `SPEC.md`. Load upstream artifacts via manifest references. When a prerequisite stage is not yet implemented, **test with serialized fixture artifacts that obey the contract**, never with fake production success or untracked in-memory mocks.

## Required outputs

Implement the stage's declared persistable output artifacts with stable IDs, `schema_version`, input hashes, `available_time` semantics where applicable, status and quality flags. Integrate with the common stage interface; artifact file formats can follow `SPEC.md` and shared contracts.

## Codex prompt (paste into Codex)

> Read AGENTS.md, COMMON_CONTRACTS.md, steps/03_research_dimensions_and_primitives/SPEC.md, steps/03_research_dimensions_and_primitives/IMPLEMENT.md and steps/03_research_dimensions_and_primitives/VERIFY.md. Implement **only Step 03: Research Dimensions & Primitives** and dependencies strictly required to make its documented contract runnable. Create actual code and synthetic fixtures; add unit tests, an integration test and time-leakage or quality tests as applicable. Do not leave algorithmic stubs. Run tests, report exact command and results, update IMPLEMENTATION_STATUS.md with the true status, and stop before implementing the next stage.

## Exit criterion

Stage implementation is not complete until every item in [`VERIFY.md`](VERIFY.md) is checked by real automated tests or a documented reproducible CLI run; no manufactured test claims.
