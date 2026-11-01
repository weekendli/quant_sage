# Acceptance / Verification — Step 05: Feature Grammar

**Important:** These are **tests to create and run**, not claims that tests or code already exist.

## Minimal test fixture

S0 + S6; compare explicit hand-computed rolling-window values and prefix invariance.

## Required acceptance checks

- [ ] Equivalent ASTs canonicalize to same feature ID; unequal units are rejected.
- [ ] Rolling clock 30s differs correctly from rolling 30 updates in irregular fixture.
- [ ] Every computed feature depends only on rows available at decision time.
- [ ] Window never crosses session boundary silently.
- [ ] Grammar refuses candidate explosion beyond configured budget and logs skipped items.

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
