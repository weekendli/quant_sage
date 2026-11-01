# Acceptance / Verification — Step 04: Statistical Atlas & Baselines

**Important:** These are **tests to create and run**, not claims that tests or code already exist.

## Minimal test fixture

S1 + S4; seasonal null data with injected shocks and asynchronous instruments.

## Required acceptance checks

- [ ] Injected known seasonality is visible to baseline but is not misreported as anomaly.
- [ ] A shifted extreme observation gets a larger conditional surprise score.
- [ ] Fitting sees only training data; adding future holdout observations does not change fitted baseline.
- [ ] Asynchronous instrument joins reject stale counterparts and report coverage.
- [ ] Small-sample bins return explicit insufficient-history flags.

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
