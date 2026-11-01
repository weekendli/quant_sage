# Acceptance / Verification — Step 07: Regime Detector

**Important:** These are **tests to create and run**, not claims that tests or code already exist.

## Minimal test fixture

S1 + S3 + S6; piecewise market states and append-only future segment.

## Required acceptance checks

- [ ] Known low/high volatility segments are detected with declared latency.
- [ ] Future append does not revise previously emitted online regime state.
- [ ] Offline smoothed regime is never tagged tradable/online.
- [ ] Quote staleness is explicitly included or prevents regime decisions.
- [ ] Short ambiguous sequences return unknown state rather than fabricated confidence.

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
