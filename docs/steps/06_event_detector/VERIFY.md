# Acceptance / Verification — Step 06: Event Detector

**Important:** These are **tests to create and run**, not claims that tests or code already exist.

## Minimal test fixture

S1 + S2 + S4 + S6; known shock timestamps and repeated trigger sequences.

## Required acceptance checks

- [ ] Injected extreme depth shock produces an episode with expected detected_time.
- [ ] Multiple neighboring triggers form one episode under declared dedup policy.
- [ ] Thresholds fitted from training do not drift due to future holdout.
- [ ] Asynchronous data with stale cross-series quotes does not trigger a valid divergence.
- [ ] No injected shock in null data does not force nonempty event results.

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
