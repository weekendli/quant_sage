# Codex Implementation Task — Step 10: Discovery Cube

**Milestone:** M6  
**Upstream dependencies:** Step 04, Step 05, Step 06, Step 07, Step 09
**Scope:** implement only this stage, not downstream algorithms.

## Required reading

1. [`../../AGENTS.md`](../../AGENTS.md) — repository-wide rules.
2. [`../../COMMON_CONTRACTS.md`](../../COMMON_CONTRACTS.md) — strict data/time/provenance contract.
3. [`SPEC.md`](SPEC.md) — full stage design.
4. [`VERIFY.md`](VERIFY.md) — this stage's verifiable exit gate.

## What to build

1. Define feature × condition × horizon cell registry and search-family accounting.
2. Derive outcome labels strictly after decision_time and within allowed session rules.
3. Implement budget-aware candidate proposal and staged exploration/selection.
4. Build matched conditional and unconditioned comparison queries.
5. Track every attempted cell, effect definition, searched threshold and trial count.

## Input contract

Use the precise inputs declared in `SPEC.md`. Load upstream artifacts via manifest references. When a prerequisite stage is not yet implemented, **test with serialized fixture artifacts that obey the contract**, never with fake production success or untracked in-memory mocks.

## Required outputs

Implement the stage's declared persistable output artifacts with stable IDs, `schema_version`, input hashes, `available_time` semantics where applicable, status and quality flags. Integrate with the common stage interface; artifact file formats can follow `SPEC.md` and shared contracts.

## Codex prompt (paste into Codex)

> Read AGENTS.md, COMMON_CONTRACTS.md, steps/10_discovery_cube/SPEC.md, steps/10_discovery_cube/IMPLEMENT.md and steps/10_discovery_cube/VERIFY.md. Implement **only Step 10: Discovery Cube** and dependencies strictly required to make its documented contract runnable. Create actual code and synthetic fixtures; add unit tests, an integration test and time-leakage or quality tests as applicable. Do not leave algorithmic stubs. Run tests, report exact command and results, update IMPLEMENTATION_STATUS.md with the true status, and stop before implementing the next stage.

## Exit criterion

Stage implementation is not complete until every item in [`VERIFY.md`](VERIFY.md) is checked by real automated tests or a documented reproducible CLI run; no manufactured test claims.
