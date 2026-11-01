# Acceptance / Verification — Step 03: Research Dimensions & Primitives

**Important:** These are **tests to create and run**, not claims that tests or code already exist.

## Minimal test fixture

S0 + S5; 8 manually calculable quote snapshots and missing-field variant.

## Required acceptance checks

- [ ] Hand-calculated 8-row fixture matches mid, spread and OBI exactly.
- [ ] Zero denominator for imbalance is null/flag, never +/- infinity.
- [ ] A primitive never reaches into a previous exchange session unless specified.
- [ ] Trade-only input disables LOB dimensions with a reason rather than fabricating data.
- [ ] Append future rows does not alter previously computed prefix primitives.

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
