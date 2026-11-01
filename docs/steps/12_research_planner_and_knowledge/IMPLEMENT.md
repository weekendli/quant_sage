# Codex Implementation Task — Step 12: Research Planner & Knowledge

**Milestone:** M7  
**Upstream dependencies:** Step 08, Step 09, Step 11  
**Scope:** implement only this stage, not downstream algorithms.

## Required reading

1. [`../../AGENTS.md`](../../AGENTS.md) — repository-wide rules.
2. [`../../COMMON_CONTRACTS.md`](../../COMMON_CONTRACTS.md) — strict data/time/provenance contract.
3. [`SPEC.md`](SPEC.md) — full stage design.
4. [`VERIFY.md`](VERIFY.md) — this stage's verifiable exit gate.

## What to build

1. Persist verified/failed/inconclusive knowledge objects and lineage edges.
2. Suggest next discriminating tests based on competing explanations and cost/coverage.
3. Enforce planner budget and immutable experiment record.
4. Rank tests on stated expected information value proxy, not invented confidence.
5. Provide knowledge query, provenance trace and next-experiment API.

## Input contract

Use the precise inputs declared in `SPEC.md`. Load upstream artifacts via manifest references. When a prerequisite stage is not yet implemented, **test with serialized fixture artifacts that obey the contract**, never with fake production success or untracked in-memory mocks.

## Required outputs

Implement the stage's declared persistable output artifacts with stable IDs, `schema_version`, input hashes, `available_time` semantics where applicable, status and quality flags. Integrate with the common stage interface; artifact file formats can follow `SPEC.md` and shared contracts.

## Codex prompt (paste into Codex)

> Read AGENTS.md, COMMON_CONTRACTS.md, steps/12_research_planner_and_knowledge/SPEC.md, steps/12_research_planner_and_knowledge/IMPLEMENT.md and steps/12_research_planner_and_knowledge/VERIFY.md. Implement **only Step 12: Research Planner & Knowledge** and dependencies strictly required to make its documented contract runnable. Create actual code and synthetic fixtures; add unit tests, an integration test and time-leakage or quality tests as applicable. Do not leave algorithmic stubs. Run tests, report exact command and results, update IMPLEMENTATION_STATUS.md with the true status, and stop before implementing the next stage.

## Exit criterion

Stage implementation is not complete until every item in [`VERIFY.md`](VERIFY.md) is checked by real automated tests or a documented reproducible CLI run; no manufactured test claims.
