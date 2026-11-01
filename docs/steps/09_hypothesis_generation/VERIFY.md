# Acceptance / Verification — Step 09: Hypothesis Generation

**Important:** These are **tests to create and run**, not claims that tests or code already exist.

## Minimal test fixture

A tiny fixture registry of three synthetic PhenomenonCards (no raw 30GB required).

## Required acceptance checks

- [ ] All six generators can be called with deterministic fixture PhenomenonCards.
- [ ] Each emitted hypothesis has falsifiable claim, outcome, control and failure criterion.
- [ ] Ill-typed/unsupported input feature is rejected or annotated unusable.
- [ ] Rerun with same seed and registry snapshot reproduces candidate identities.
- [ ] No empirical significance or Sharpe value appears before evaluation.

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
