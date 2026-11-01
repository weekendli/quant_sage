# Codex Implementation Task — Step 01: Workflow Engine

**Milestone:** M1  
**Upstream dependencies:** no upstream implementation (only shared contracts)  
**Scope:** implement only this stage, not downstream algorithms.

## Required reading

1. [`../../AGENTS.md`](../../AGENTS.md) — repository-wide rules.
2. [`../../COMMON_CONTRACTS.md`](../../COMMON_CONTRACTS.md) — strict data/time/provenance contract.
3. [`SPEC.md`](SPEC.md) — full stage design.
4. [`VERIFY.md`](VERIFY.md) — this stage's verifiable exit gate.

## What to build

1. Define Stage, RunContext, StageResult, ArtifactRef and status transitions.
2. Implement DAG validation, deterministic topological scheduling and error propagation.
3. Persist manifest before exposing an artifact, with content hash and atomic publication.
4. Implement cache keys, resume, partial partition replay and failure classification.
5. Provide validate, plan, run, status, artifacts, explain CLI commands.

## Input contract

Use the precise inputs declared in `SPEC.md`. Load upstream artifacts via manifest references. When a prerequisite stage is not yet implemented, **test with serialized fixture artifacts that obey the contract**, never with fake production success or untracked in-memory mocks.

## Required outputs

Implement the stage's declared persistable output artifacts with stable IDs, `schema_version`, input hashes, `available_time` semantics where applicable, status and quality flags. Integrate with the common stage interface; artifact file formats can follow `SPEC.md` and shared contracts.

## Codex prompt (paste into Codex)

> Read AGENTS.md, COMMON_CONTRACTS.md, steps/01_workflow_engine/SPEC.md, steps/01_workflow_engine/IMPLEMENT.md and steps/01_workflow_engine/VERIFY.md. Implement **only Step 01: Workflow Engine** and dependencies strictly required to make its documented contract runnable. Create actual code and synthetic fixtures; add unit tests, an integration test and time-leakage or quality tests as applicable. Do not leave algorithmic stubs. Run tests, report exact command and results, update IMPLEMENTATION_STATUS.md with the true status, and stop before implementing the next stage.

## Exit criterion

Stage implementation is not complete until every item in [`VERIFY.md`](VERIFY.md) is checked by real automated tests or a documented reproducible CLI run; no manufactured test claims.
