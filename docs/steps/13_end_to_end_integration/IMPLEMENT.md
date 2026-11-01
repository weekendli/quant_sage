# Codex Implementation Task — Step 13: End-to-End Integration

**Milestone:** M7  
**Upstream dependencies:** Step 01, Step 02, Step 03, Step 04, Step 05, Step 06, Step 07, Step 08, Step 09, Step 10, Step 11, Step 12  
**Scope:** implement only this stage, not downstream algorithms.

## Required reading

1. [`../../AGENTS.md`](../../AGENTS.md) — repository-wide rules.
2. [`../../COMMON_CONTRACTS.md`](../../COMMON_CONTRACTS.md) — strict data/time/provenance contract.
3. [`SPEC.md`](SPEC.md) — full stage design.
4. [`VERIFY.md`](VERIFY.md) — this stage's verifiable exit gate.

## What to build

1. Wire all stages into observatory, hypothesis, validation and report-only modes.
2. Connect manifests, stage dependencies, configuration inheritance and lineage.
3. Export consolidated report with evidence, uncertainty, limitations and next experiment.
4. Demonstrate cache, interrupted resume and dependency-based invalidation end-to-end.
5. Add a deterministic synthetic data E2E smoke test and runtime budget checklist.

## Input contract

Use the precise inputs declared in `SPEC.md`. Load upstream artifacts via manifest references. When a prerequisite stage is not yet implemented, **test with serialized fixture artifacts that obey the contract**, never with fake production success or untracked in-memory mocks.

## Required outputs

Implement the stage's declared persistable output artifacts with stable IDs, `schema_version`, input hashes, `available_time` semantics where applicable, status and quality flags. Integrate with the common stage interface; artifact file formats can follow `SPEC.md` and shared contracts.

## Codex prompt (paste into Codex)

> Read AGENTS.md, COMMON_CONTRACTS.md, steps/13_end_to_end_integration/SPEC.md, steps/13_end_to_end_integration/IMPLEMENT.md and steps/13_end_to_end_integration/VERIFY.md. Implement **only Step 13: End-to-End Integration** and dependencies strictly required to make its documented contract runnable. Create actual code and synthetic fixtures; add unit tests, an integration test and time-leakage or quality tests as applicable. Do not leave algorithmic stubs. Run tests, report exact command and results, update IMPLEMENTATION_STATUS.md with the true status, and stop before implementing the next stage.

## Exit criterion

Stage implementation is not complete until every item in [`VERIFY.md`](VERIFY.md) is checked by real automated tests or a documented reproducible CLI run; no manufactured test claims.
