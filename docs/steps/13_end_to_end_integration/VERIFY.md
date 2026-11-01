# Acceptance / Verification — Step 13: End-to-End Integration

**Important:** These are **tests to create and run**, not claims that tests or code already exist.

## Minimal test fixture

S0–S8 suite, with one representative small synthetic pipeline fixture.

## Required acceptance checks

- [ ] One CLI command reaches DataPassport → Primitive → Atlas → Event/Regime → Phenomenon.
- [ ] Same run can extend to Hypothesis → Cube → Falsification → Planner on fixture.
- [ ] Report-only mode does not refit or rerun upstream inference.
- [ ] Fault injection followed by resume produces no duplicate published results.
- [ ] Result can be traced by IDs back through all stage inputs to source snapshot.

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
