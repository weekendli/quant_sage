# Codex Implementation Task — Step 06: Event Detector

**Milestone:** M4  
**Upstream dependencies:** Step 04, Step 05  
**Scope:** implement only this stage, not downstream algorithms.

## Required reading

1. [`../../AGENTS.md`](../../AGENTS.md) — repository-wide rules.
2. [`../../COMMON_CONTRACTS.md`](../../COMMON_CONTRACTS.md) — strict data/time/provenance contract.
3. [`SPEC.md`](SPEC.md) — full stage design.
4. [`VERIFY.md`](VERIFY.md) — this stage's verifiable exit gate.

## What to build

1. Implement threshold extreme, baseline surprise, cross-series divergence, change detectors.
2. Define event identity, thresholds, detected_time, available_time and reason.
3. Merge overlapping triggers into deterministic episodes with cooldown/rearm policy.
4. Produce event registry and event instances with source feature pointers.
5. Calculate event quality stats independent of future return labels.

## Input contract

Use the precise inputs declared in `SPEC.md`. Load upstream artifacts via manifest references. When a prerequisite stage is not yet implemented, **test with serialized fixture artifacts that obey the contract**, never with fake production success or untracked in-memory mocks.

## Required outputs

Implement the stage's declared persistable output artifacts with stable IDs, `schema_version`, input hashes, `available_time` semantics where applicable, status and quality flags. Integrate with the common stage interface; artifact file formats can follow `SPEC.md` and shared contracts.

## Codex prompt (paste into Codex)

> Read AGENTS.md, COMMON_CONTRACTS.md, steps/06_event_detector/SPEC.md, steps/06_event_detector/IMPLEMENT.md and steps/06_event_detector/VERIFY.md. Implement **only Step 06: Event Detector** and dependencies strictly required to make its documented contract runnable. Create actual code and synthetic fixtures; add unit tests, an integration test and time-leakage or quality tests as applicable. Do not leave algorithmic stubs. Run tests, report exact command and results, update IMPLEMENTATION_STATUS.md with the true status, and stop before implementing the next stage.

## Exit criterion

Stage implementation is not complete until every item in [`VERIFY.md`](VERIFY.md) is checked by real automated tests or a documented reproducible CLI run; no manufactured test claims.
