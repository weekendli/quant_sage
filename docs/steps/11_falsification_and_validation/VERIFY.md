# Acceptance / Verification — Step 11: Falsification & Validation

**Important:** These are **tests to create and run**, not claims that tests or code already exist.

## Minimal test fixture

S1 + S3 + S7 + S8; multiple-testing null, shifted regimes, temporal split boundary.

## Required acceptance checks

- [ ] S1 null universe does not report spurious confirmed signal after family correction.
- [ ] Overlapping horizon windows do not leak train observations into final holdout.
- [ ] Second read of used final holdout for newly tuned candidate is rejected.
- [ ] Inference reports estimates, interval, block/sample counts and failure reasons.
- [ ] Injected S3 effect can be detected in discovery while reversed holdout fails verification.

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
