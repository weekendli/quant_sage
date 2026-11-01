# Acceptance / Verification — Step 12: Research Planner & Knowledge

**Important:** These are **tests to create and run**, not claims that tests or code already exist.

## Minimal test fixture

Fixture experiment ledger with one supported, one falsified and one inconclusive record.

## Required acceptance checks

- [ ] Failed/inconclusive experiments stay discoverable with falsification reason.
- [ ] Planner can propose at least two discriminating tests for synthetic competing hypotheses.
- [ ] Same evidence and seed produce stable rankings with recorded decision basis.
- [ ] Planner cannot mutate past hypothesis/holdout evidence in place.
- [ ] Missing measurements are explicit; planner never invents results.

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
