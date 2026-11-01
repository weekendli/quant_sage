# Acceptance / Verification — Step 10: Discovery Cube

**Important:** These are **tests to create and run**, not claims that tests or code already exist.

## Minimal test fixture

S0 + S3 + S8; small dataset with known future outcome and condition labels.

## Required acceptance checks

- [ ] Decision-time label offset and horizon boundaries are correct on hand-labeled fixture.
- [ ] Empty/insufficient coverage cell returns explicit status not fake significance.
- [ ] Cell duplication/variant changes do not erase prior search trials.
- [ ] Budget and max-cell thresholds cap exploratory search deterministically.
- [ ] Modifying final holdout cannot change which cells were selected for final verification.

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
