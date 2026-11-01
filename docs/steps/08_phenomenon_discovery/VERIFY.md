# Acceptance / Verification — Step 08: Phenomenon Discovery

**Important:** These are **tests to create and run**, not claims that tests or code already exist.

## Minimal test fixture

S1 + S2; both injected and null conditions, with matched non-event controls.

## Required acceptance checks

- [ ] Injected depth-depletion-and-unchanged-price pattern yields a structured phenomenon.
- [ ] Pure seasonality S1 does not become a spurious independent phenomenon.
- [ ] Negative controls and duplicate event windows are accounted for.
- [ ] Changing future labels cannot change the emitted observation-only registry.
- [ ] No qualifying phenomenon returns empty registry with documented reason.

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
